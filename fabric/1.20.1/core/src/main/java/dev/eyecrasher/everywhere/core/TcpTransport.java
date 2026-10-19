package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Direct, pair-authenticated TCP. Length-bounded encrypted frames, bounded queues and handshakes. */
final class TcpTransport implements AutoCloseable {
    @FunctionalInterface interface SharedPortBinder {
        void bind(SocketChannel channel,int port) throws IOException;
    }
    static final class SharedPortUnavailableException extends BindException {
        SharedPortUnavailableException(Throwable cause) {
            super("TCP shared listener port unavailable");
            if(cause!=null)initCause(cause);
        }
    }
    record Diagnostics(long attempts, long established, long failures, String lastFailure, boolean connecting, boolean ready) {}
    private static final class Statistics {
        final LongAdder attempts = new LongAdder(), established = new LongAdder(), failures = new LongAdder();
        volatile String lastFailure = "";
    }
    interface Handler {
        PeerCrypto crypto(UUID session);
        boolean permits(UUID session,InetAddress source);
        void received(Wire.Frame frame,byte[] plain);
    }
    private final UUID self;
    private final Handler handler;
    private final ServerSocketChannel server;
    private final int localPort;
    private final SharedPortBinder sharedPortBinder;
    private final Object sharedBindLock=new Object();
    private volatile BindException sharedPortFailure;
    private final ConcurrentMap<UUID,Connection> connections=new ConcurrentHashMap<>();
    private final Set<Socket> handshakes=ConcurrentHashMap.newKeySet();
    private final Set<Socket> opened=ConcurrentHashMap.newKeySet();
    private final Set<UUID> connecting=ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<UUID,Statistics> statistics=new ConcurrentHashMap<>();
    private final Map<UUID,Long> retry=new HashMap<>();
    private final Map<UUID,Integer> cursor=new HashMap<>();
    private final RateLimit acceptBudget=new RateLimit(8,8),connectBudget=new RateLimit(4,8);
    private final AtomicBoolean closed=new AtomicBoolean();
    private final ScheduledExecutorService deadlines=Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t=new Thread(r,"everywhere-tcp-deadlines"); t.setDaemon(true); return t;
    });
    TcpTransport(UUID self,int preferredPort,Handler handler) throws IOException {
        this(self,preferredPort,handler,(channel,port) -> channel.bind(new InetSocketAddress(port)));
    }
    // Bind injection reproduces operating-system port-sharing refusal without changing global JVM options.
    TcpTransport(UUID self,int preferredPort,Handler handler,SharedPortBinder sharedPortBinder) throws IOException {
        this.self=self; this.handler=handler; this.sharedPortBinder=Objects.requireNonNull(sharedPortBinder);
        ServerSocketChannel bound=listener(preferredPort);
        server=bound;
        localPort=bound.socket().getLocalPort();
        RuntimeCompat.start("everywhere-tcp-accept", this::accept);
    }
    private static ServerSocketChannel listener(int preferred) throws IOException {
        ServerSocketChannel result=ServerSocketChannel.open();
        try {
            result.setOption(StandardSocketOptions.SO_REUSEADDR,true);
            if(result.supportedOptions().contains(StandardSocketOptions.SO_REUSEPORT))result.setOption(StandardSocketOptions.SO_REUSEPORT,true);
            try { result.bind(new InetSocketAddress(Math.max(0,preferred)),16); }
            catch(BindException e) { result.close(); result=ServerSocketChannel.open(); result.setOption(StandardSocketOptions.SO_REUSEADDR,true); result.bind(new InetSocketAddress(0),16); }
            return result;
        } catch(IOException|RuntimeException e) { result.close(); throw e; }
    }
    int port() { return localPort; }
    boolean sharedPortAvailable() { return sharedPortFailure==null; }
    BindException sharedPortFailure() { return sharedPortFailure; }
    boolean isClosed() { return closed.get(); }
    boolean ready(UUID peer) { Connection c=connections.get(peer); return c!=null && c.alive(System.nanoTime()); }
    Diagnostics diagnostics(UUID peer) {
        Statistics stats=statistics.get(peer);
        return new Diagnostics(stats==null?0:stats.attempts.sum(),stats==null?0:stats.established.sum(),
                stats==null?0:stats.failures.sum(),stats==null?"":stats.lastFailure,connecting.contains(peer),ready(peer));
    }
    boolean send(UUID peer,byte[] frame) {
        Connection c=connections.get(peer);
        if(c==null || !c.alive(System.nanoTime()))return false;
        if(c.queue.offer(frame))return true;
        c.close(); return false;
    }
    void retain(Set<UUID> peers) {
        connections.forEach((id,c) -> { if(!peers.contains(id))c.close(); });
        statistics.keySet().retainAll(peers);
    }
    synchronized void tick(long now,Map<UUID,List<InetSocketAddress>> targets,Predicate<UUID> udpReady) {
        if(closed.get())return;
        opened.removeIf(Socket::isClosed);
        retry.keySet().retainAll(targets.keySet()); cursor.keySet().retainAll(targets.keySet());
        statistics.keySet().retainAll(targets.keySet());
        connections.forEach((id,c) -> {
            if(!targets.containsKey(id) || !c.alive(now)) { c.close(); return; }
            if(now-c.lastPing>TimeUnit.SECONDS.toNanos(c.nonce==null?1:2)) {
                c.lastPing=now; c.nonce=Wire.random(16);
                PeerCrypto crypto=handler.crypto(id);
                if(crypto==null || !c.queue.offer(crypto.seal(Wire.PING,c.nonce).encode()))c.close();
            }
        });
        for(var item:targets.entrySet()) {
            UUID id=item.getKey(); var endpoints=item.getValue();
            if(closed.get() || endpoints.isEmpty() || ready(id) || udpReady.test(id) || connecting.contains(id)
                    || handshakes.size()>=8 || now<retry.getOrDefault(id,0L) || !connectBudget.take())continue;
            int index=cursor.merge(id,1,Integer::sum)-1;
            InetSocketAddress endpoint=endpoints.get(Math.floorMod(index,endpoints.size()));
            retry.put(id,now+TimeUnit.SECONDS.toNanos(index<6?1:10));
            connecting.add(id);
            RuntimeCompat.start("everywhere-tcp-connect", () -> {
                try { connect(id,endpoint); }
                finally { connecting.remove(id); }
            });
        }
    }
    Socket openSocket(InetSocketAddress endpoint,boolean requireSharedPort) throws IOException {
        SocketChannel channel=boundSocket(requireSharedPort);
        try {
            Socket socket=channel.socket(); socket.setTcpNoDelay(true); socket.setSoTimeout(1500); opened.add(socket);
            if(closed.get())throw new SocketException("Closed");
            socket.connect(endpoint,900); return socket;
        } catch(IOException|RuntimeException e) { channel.close(); throw e; }
    }
    private SocketChannel boundSocket(boolean requireSharedPort) throws IOException {
        // A listener's port cannot be reused on every OS/JVM. A bind refusal is local capability,
        // not a failed relay: remember it for this listener, while keeping ordinary TCP available.
        synchronized(sharedBindLock) {
            if(closed.get())throw new SocketException("Closed");
            if(sharedPortFailure==null) {
                SocketChannel channel=SocketChannel.open();
                try {
                    channel.setOption(StandardSocketOptions.SO_REUSEADDR,true);
                    if(channel.supportedOptions().contains(StandardSocketOptions.SO_REUSEPORT))channel.setOption(StandardSocketOptions.SO_REUSEPORT,true);
                    try {
                        sharedPortBinder.bind(channel,port());
                        if(channel.socket().getLocalPort()!=port())throw new BindException("TCP listener port not shared");
                        return channel;
                    } catch(BindException unavailable) { sharedPortFailure=unavailable; }
                } catch(IOException|RuntimeException error) { channel.close(); throw error; }
                channel.close();
            }
            if(requireSharedPort)throw new SharedPortUnavailableException(sharedPortFailure);
        }
        SocketChannel channel=SocketChannel.open();
        try { channel.bind(new InetSocketAddress(0)); return channel; }
        catch(IOException|RuntimeException error) { channel.close(); throw error; }
    }
    ScheduledFuture<?> deadline(Socket socket,long milliseconds) {
        return deadlines.schedule(() -> closeSocket(socket),milliseconds,TimeUnit.MILLISECONDS);
    }
    private void connect(UUID peer,InetSocketAddress endpoint) {
        if(closed.get() || handler.crypto(peer)==null || !handler.permits(peer,endpoint.getAddress()))return;
        Statistics stats=statistics.computeIfAbsent(peer,ignored -> new Statistics()); stats.attempts.increment();
        Socket socket=null;
        try {
            socket=openSocket(endpoint,false);
            handshake(socket,peer);
        } catch(Exception error) {
            stats.failures.increment();
            stats.lastFailure=error instanceof SocketTimeoutException?"timeout":error instanceof NoRouteToHostException?"no-route"
                    :socket!=null?"handshake-failed":error instanceof ConnectException?"connect-failed":"socket-unavailable";
            closeSocket(socket);
        }
    }
    private void accept() {
        while(!closed.get())try {
            Socket socket=server.accept().socket(); socket.setTcpNoDelay(true); socket.setSoTimeout(1500);
            if(!acceptBudget.take() || handshakes.size()>=8) { socket.close(); continue; }
            RuntimeCompat.start("everywhere-tcp-handshake", () -> {
                try { handshake(socket,null); } catch(Exception ignored) { closeSocket(socket); }
            });
        } catch(IOException ignored) { if(closed.get())return; }
    }
    private void handshake(Socket socket,UUID expected) throws Exception {
        if(closed.get() || handshakes.size()>=8) { socket.close(); return; }
        handshakes.add(socket);
        var timeout=deadlines.schedule(() -> closeSocket(socket),1600,TimeUnit.MILLISECONDS);
        boolean accepted=false;
        try {
            DataInputStream input=new DataInputStream(socket.getInputStream());
            DataOutputStream output=new DataOutputStream(socket.getOutputStream());
            byte[] nonce=Wire.random(16);
            if(expected!=null) {
                PeerCrypto crypto=handler.crypto(expected); if(crypto==null)throw new IOException("Peer left");
                write(output,crypto.seal(Wire.PING,nonce).encode());
            }
            Wire.Frame first=read(input);
            UUID peer=first.from();
            if(!first.to().equals(self) || expected!=null && !expected.equals(peer)
                    || !handler.permits(peer,socket.getInetAddress()))throw new IOException("TCP peer binding");
            PeerCrypto crypto=handler.crypto(peer); if(crypto==null)throw new IOException("Unknown peer");
            byte[] plain=crypto.open(first);
            if(plain==null || plain.length!=16)throw new IOException("TCP authentication");
            if(first.kind()==Wire.PING) {
                write(output,crypto.seal(Wire.PONG,plain).encode());
                if(expected==null)write(output,crypto.seal(Wire.PING,nonce).encode());
                // Two active opens can become a single TCP connection. Both sides send
                // PING first; accept that order as well as the usual active/passive one.
                Wire.Frame proof=read(input); byte[] answer=crypto.open(proof);
                if(proof.kind()!=Wire.PONG || answer==null || !Arrays.equals(nonce,answer))throw new IOException("TCP fresh proof");
            } else if(expected!=null && first.kind()==Wire.PONG && Arrays.equals(nonce,plain)) {
                Wire.Frame challenge=read(input); byte[] fresh=crypto.open(challenge);
                if(challenge.kind()!=Wire.PING || fresh==null || fresh.length!=16)throw new IOException("TCP mutual proof");
                write(output,crypto.seal(Wire.PONG,fresh).encode());
            } else throw new IOException("TCP authentication order");
            socket.setSoTimeout(5000);
            Connection connection=new Connection(peer,expected==null?peer:self,socket,input,output);
            synchronized(this) {
                Connection old=connections.get(peer);
                if(closed.get() || old!=null && old.alive(System.nanoTime()) && old.initiator.compareTo(connection.initiator)<=0)return;
                connections.put(peer,connection); if(old!=null)old.close();
                Statistics stats=statistics.computeIfAbsent(peer,ignored -> new Statistics());
                stats.established.increment(); stats.lastFailure="";
            }
            accepted=true; timeout.cancel(false); handshakes.remove(socket);
            RuntimeCompat.start("everywhere-tcp-write", connection::writeLoop);
            RuntimeCompat.start("everywhere-tcp-read", connection::readLoop);
        } finally {
            timeout.cancel(false); handshakes.remove(socket);
            if(!accepted)socket.close();
        }
    }
    private final class Connection {
        final UUID peer,initiator;
        final Socket socket;
        final DataInputStream input;
        final DataOutputStream output;
        final BlockingQueue<byte[]> queue=new ArrayBlockingQueue<>(32);
        final RateLimit receive=new RateLimit(100,128);
        volatile long pong=System.nanoTime(),lastPing;
        volatile byte[] nonce;
        Connection(UUID peer,UUID initiator,Socket socket,DataInputStream input,DataOutputStream output) {
            this.peer=peer; this.initiator=initiator; this.socket=socket; this.input=input; this.output=output;
        }
        boolean alive(long now) { return !socket.isClosed() && now-pong<TimeUnit.SECONDS.toNanos(4); }
        void readLoop() {
            try {
                while(!closed.get() && !socket.isClosed()) {
                    Wire.Frame frame=read(input); PeerCrypto crypto=handler.crypto(peer);
                    if(crypto==null || !frame.from().equals(peer) || !receive.take())break;
                    byte[] plain=crypto.open(frame); if(plain==null)break;
                    if(frame.kind()==Wire.PING && plain.length==16) {
                        if(!queue.offer(crypto.seal(Wire.PONG,plain).encode()))break;
                    } else if(frame.kind()==Wire.PONG && plain.length==16) {
                        if(Arrays.equals(plain,nonce)) { pong=System.nanoTime(); nonce=null; }
                    }
                    else if(frame.kind()==Wire.VOICE || frame.kind()==Wire.END)handler.received(frame,plain);
                    else break;
                }
            } catch(IOException|RuntimeException ignored) {}
            finally { close(); }
        }
        void writeLoop() {
            try {
                while(!closed.get() && !socket.isClosed()) {
                    byte[] frame=queue.poll(1,TimeUnit.SECONDS);
                    if(frame!=null)write(output,frame);
                }
            } catch(IOException|InterruptedException ignored) {}
            finally { close(); }
        }
        void close() { connections.remove(peer,this); closeSocket(socket); queue.clear(); }
    }
    private static Wire.Frame read(DataInputStream input) throws IOException {
        int length=input.readUnsignedShort();
        if(length<47 || length>Wire.MAX_FRAME)throw new IOException("TCP frame size");
        byte[] bytes=input.readNBytes(length); if(bytes.length!=length)throw new EOFException();
        try { return Wire.decode(bytes); } catch(IllegalArgumentException e) { throw new IOException("TCP frame",e); }
    }
    private static void write(DataOutputStream output,byte[] frame) throws IOException {
        output.writeShort(frame.length); output.write(frame); output.flush();
    }
    private static void closeSocket(Socket socket) { if(socket!=null)try { socket.close(); } catch(IOException ignored) {} }
    @Override public void close() {
        if(!closed.compareAndSet(false,true))return;
        try { server.close(); } catch(IOException ignored) {}
        handshakes.forEach(TcpTransport::closeSocket); connections.values().forEach(Connection::close);
        opened.forEach(TcpTransport::closeSocket); opened.clear();
        deadlines.shutdownNow();
    }
}
