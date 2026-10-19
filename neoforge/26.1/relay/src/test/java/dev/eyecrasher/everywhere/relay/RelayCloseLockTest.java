package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.Wire;
import org.java_websocket.*;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.HandshakeImpl1Client;
import java.lang.reflect.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Reproduces transport-monitor -> relay and expire -> slow-peer close ordering. */
public final class RelayCloseLockTest {
    private static final Class<?> SESSION;
    private static final Constructor<?> CONSTRUCTOR;
    private static final Method FINISH;
    static {
        try {
            SESSION=Class.forName(RelayServer.class.getName()+"$Session");
            CONSTRUCTOR=SESSION.getDeclaredConstructor(WebSocket.class);CONSTRUCTOR.setAccessible(true);
            FINISH=RelayServer.class.getDeclaredMethod("finishJoin",SESSION,boolean.class);FINISH.setAccessible(true);
        } catch(ReflectiveOperationException error) { throw new ExceptionInInitializerError(error); }
    }
    private static final class Socket extends WebSocketImpl {
        final RelayServer relay;
        final CountDownLatch closeAttempt=new CountDownLatch(1),closeCompleted=new CountDownLatch(1);
        final AtomicInteger closes=new AtomicInteger();
        final AtomicBoolean closedUnderRelayLock=new AtomicBoolean();
        volatile boolean open=true;
        volatile String reason;
        Socket(RelayServer relay) { super(relay,new Draft_6455());this.relay=relay; }
        @Override public boolean isOpen() { return open; }
        @Override public InetSocketAddress getRemoteSocketAddress() { return new InetSocketAddress("127.0.0.1",1); }
        @Override public void send(String text) {}
        @Override public void close(int code,String reason) {
            closedUnderRelayLock.set(Thread.holdsLock(relay));closeAttempt.countDown();
            synchronized(this) { open=false;this.reason=reason;closes.incrementAndGet();closeCompleted.countDown(); }
        }
    }
    public static void main(String[] args) throws Exception {
        verifyExpireAndCallback();verifyBoundedCloseBacklog();verifyRejectedAdmission();
        System.out.println("Relay close locking: expiry/slow-peer callback inversion, responsive health, coalesced close and outside-lock rejection passed");
    }
    private static void verifyExpireAndCallback() throws Exception {
        RelayServer relay=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,0);
        boolean completed=false;
        ExecutorService tasks=Executors.newCachedThreadPool(task -> { Thread thread=new Thread(task,"close-lock-test");thread.setDaemon(true);return thread; });
        try {
            Socket expired=new Socket(relay),slow=new Socket(relay);
            Object first=actor(relay,expired,1),second=actor(relay,slow,2);
            set(first,"nearby",Set.of(new UUID(0,2)));set(second,"nearby",Set.of(new UUID(0,1)));
            Field indexes=RelayServer.class.getDeclaredField("nearbyIndexes");indexes.setAccessible(true);
            @SuppressWarnings("unchecked") NearbyIndex<Object> index=((Map<String,NearbyIndex<Object>>)indexes.get(relay)).get("f".repeat(64));
            index.replace(first,Set.of(),Set.of(new UUID(0,2)));index.replace(second,Set.of(),Set.of(new UUID(0,1)));
            long now=System.nanoTime();set(first,"lastControl",now-TimeUnit.SECONDS.toNanos(31));set(second,"lastControl",now);
            slow.outQueue.add(ByteBuffer.allocate(65536));
            CountDownLatch transportLocked=new CountDownLatch(1);
            Future<?> callback=tasks.submit(() -> {
                synchronized(slow) {
                    transportLocked.countDown();await(slow.closeAttempt,"slow close attempt");
                    // WebSocketImpl.closeConnection calls this while holding its monitor.
                    relay.onClose(slow,1000,"transport callback",true);
                }
            });
            await(transportLocked,"transport monitor ownership");
            Future<?> expiry=tasks.submit(() -> relay.expire(now));
            expiry.get(2,TimeUnit.SECONDS);callback.get(2,TimeUnit.SECONDS);
            tasks.submit(relay::health).get(2,TimeUnit.SECONDS);
            await(slow.closeCompleted,"slow close completion");await(expired.closeCompleted,"heartbeat timeout close");
            require(!slow.closedUnderRelayLock.get() && !expired.closedUnderRelayLock.get(),"Transport close acquired under relay monitor");
            require("Slow connection".equals(slow.reason) && "Voice heartbeat timed out".equals(expired.reason),"Close reasons changed");
            require(slow.closes.get()==1,"Repeated close request not coalesced");
            require(relay.health().get("connections").getAsInt()==0,"Deadlock fix retained expired sessions");
            completed=true;
        } finally {
            tasks.shutdownNow();
            // A regression must fail in bounded time instead of hanging its own cleanup on the deadlock.
            if(completed)relay.close();
        }
    }
    @SuppressWarnings("unchecked")
    private static void verifyBoundedCloseBacklog() throws Exception {
        RelayServer relay=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,0);
        CountDownLatch release=new CountDownLatch(1),held=new CountDownLatch(1);
        Socket first=new Socket(relay);actor(relay,first,1);
        Thread holder=new Thread(() -> { synchronized(first) { held.countDown();await(release,"release backlog transport monitor"); } },"backlog-lock-test");
        holder.setDaemon(true);holder.start();await(held,"backlog transport monitor ownership");
        try {
            List<Socket> sockets=new ArrayList<>();sockets.add(first);Socket second=new Socket(relay);actor(relay,second,2);sockets.add(second);
            Field admittedField=RelayServer.class.getDeclaredField("sockets");admittedField.setAccessible(true);
            Map<WebSocket,Object> admitted=(Map<WebSocket,Object>)admittedField.get(relay);
            for(int n=0;n<16;n++) { Socket pending=new Socket(relay);admitted.put(pending,CONSTRUCTOR.newInstance(pending));sockets.add(pending); }
            Method close=RelayServer.class.getDeclaredMethod("requestClose",WebSocket.class,int.class,String.class);close.setAccessible(true);
            for(Socket socket:sockets) { close.invoke(relay,socket,1008,"Invalid control message");close.invoke(relay,socket,1008,"Join timeout"); }
            await(first.closeAttempt,"blocked close worker");
            Field pendingField=RelayServer.class.getDeclaredField("pendingCloses");pendingField.setAccessible(true);
            synchronized(relay) { require(((Map<?,?>)pendingField.get(relay)).size()==18,"Close requests exceeded socket bound or duplicated"); }
            relay.expire(System.nanoTime()+TimeUnit.SECONDS.toNanos(16));
            Socket newcomer=new Socket(relay);HandshakeImpl1Client handshake=new HandshakeImpl1Client();handshake.setResourceDescriptor("/voice");
            relay.onOpen(newcomer,handshake);
            require("Connection limit".equals(newcomer.reason) && !newcomer.closedUnderRelayLock.get(),"Detached close backlog did not bound new admission");
            release.countDown();holder.join(2000);
            // Shutdown must drain existing requests without taking either monitor in reverse order.
            relay.close();
            for(Socket socket:sockets)require(socket.closes.get()==1 && "Invalid control message".equals(socket.reason),"Shutdown lost, duplicated or reordered a pending close reason");
            Field closerField=RelayServer.class.getDeclaredField("closer");closerField.setAccessible(true);
            require(!((Thread)closerField.get(relay)).isAlive(),"Close worker retained after shutdown drain");
        } finally { release.countDown();holder.join(2000);relay.close(); }
    }
    private static void verifyRejectedAdmission() throws Exception {
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            Socket socket=new Socket(relay);HandshakeImpl1Client handshake=new HandshakeImpl1Client();handshake.setResourceDescriptor("/invalid");
            relay.onOpen(socket,handshake);
            require(!socket.closedUnderRelayLock.get() && "Connection limit".equals(socket.reason),"Admission rejection closes under relay monitor");
        }
    }
    @SuppressWarnings("unchecked")
    private static Object actor(RelayServer relay,Socket socket,int number) throws Exception {
        Object session=CONSTRUCTOR.newInstance(socket);UUID player=new UUID(0,number);
        set(session,"player",player);set(session,"account",player);set(session,"name","Close"+number);
        set(session,"room","f".repeat(64));set(session,"dimension","minecraft:overworld");set(session,"publicKey","test-key");
        Field sockets=RelayServer.class.getDeclaredField("sockets");sockets.setAccessible(true);
        synchronized(relay) { ((Map<WebSocket,Object>)sockets.get(relay)).put(socket,session);FINISH.invoke(relay,session,false); }
        return session;
    }
    private static void set(Object session,String name,Object value) throws Exception {
        Field field=SESSION.getDeclaredField(name);field.setAccessible(true);field.set(session,value);
    }
    private static void await(CountDownLatch latch,String detail) {
        try { if(!latch.await(2,TimeUnit.SECONDS))throw new AssertionError("Timed out: "+detail); }
        catch(InterruptedException error) { Thread.currentThread().interrupt();throw new AssertionError(detail,error); }
    }
    private static void require(boolean condition,String message) { if(!condition)throw new AssertionError(message); }
}
