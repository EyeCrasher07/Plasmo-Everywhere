package dev.eyecrasher.everywhere.core;

import dev.eyecrasher.everywhere.relay.RelayServer;
import com.google.gson.*;
import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Real UDP sockets emulate independent NAT mappings and filtering, without changing host networking. */
public final class NatTraversalTest {
    private enum Mode { OPEN, RESTRICTED, SYMMETRIC }
    private record Packet(byte[] bytes, SocketAddress source) {}
    private static final class NatSocket extends DatagramSocket {
        private final Mode mode;
        private final Set<SocketAddress> services;
        private final Map<SocketAddress,Mapping> mappings = new HashMap<>();
        private final BlockingQueue<Packet> incoming = new LinkedBlockingQueue<>(256);
        volatile boolean peersBlocked, closed;
        private final class Mapping {
            final DatagramSocket socket = new DatagramSocket(new InetSocketAddress("127.0.0.1",0));
            final Set<SocketAddress> allowed = ConcurrentHashMap.newKeySet();
            Mapping() throws SocketException {
                Thread reader = new Thread(() -> {
                    byte[] buffer = new byte[1500];
                    while (!closed) try {
                        DatagramPacket packet = new DatagramPacket(buffer,buffer.length); socket.receive(packet);
                        SocketAddress source = packet.getSocketAddress();
                        if (peersBlocked && !services.contains(source)) continue;
                        if (mode != Mode.OPEN && !allowed.contains(source)) continue;
                        incoming.offer(new Packet(Arrays.copyOf(buffer,packet.getLength()),source));
                    } catch (IOException e) { return; }
                },"nat-test-mapping"); reader.setDaemon(true); reader.start();
            }
        }
        NatSocket(Mode mode, Set<SocketAddress> services) throws SocketException {
            super((SocketAddress)null); this.mode=mode; this.services=services;
        }
        @Override public synchronized void send(DatagramPacket packet) throws IOException {
            if (closed) throw new SocketException("Closed");
            SocketAddress target=packet.getSocketAddress();
            SocketAddress key=mode==Mode.SYMMETRIC?target:new InetSocketAddress("127.0.0.1",0);
            Mapping mapping=mappings.get(key);
            if(mapping==null) { mapping=new Mapping(); mappings.put(key,mapping); }
            mapping.allowed.add(target);
            if (!peersBlocked || services.contains(target)) mapping.socket.send(packet);
        }
        @Override public void receive(DatagramPacket packet) throws IOException {
            try {
                while (!closed) {
                    Packet input=incoming.poll(200,TimeUnit.MILLISECONDS); if(input==null)continue;
                    System.arraycopy(input.bytes,0,packet.getData(),packet.getOffset(),Math.min(packet.getLength(),input.bytes.length));
                    packet.setLength(Math.min(packet.getLength(),input.bytes.length)); packet.setSocketAddress(input.source); return;
                }
                throw new SocketException("Closed");
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SocketException("Interrupted"); }
        }
        @Override public synchronized void close() { closed=true; mappings.values().forEach(m -> m.socket.close()); super.close(); }
        @Override public boolean isClosed() { return closed; }
        synchronized InetSocketAddress publicEndpoint(SocketAddress service) {
            SocketAddress key=mode==Mode.SYMMETRIC?service:new InetSocketAddress("127.0.0.1",0);
            return (InetSocketAddress)mappings.get(key).socket.getLocalSocketAddress();
        }
    }
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer,long sequence,int distance,byte[] opus,boolean end) { if(!end)audio.add(opus); }
    }
    private static final class Ipv6Socket extends DatagramSocket {
        private final InetSocketAddress relay;
        Ipv6Socket(InetSocketAddress relay) throws SocketException { super(); this.relay=relay; }
        @Override public void send(DatagramPacket packet) throws IOException {
            var target=(InetSocketAddress)packet.getSocketAddress();
            if(target.getAddress() instanceof Inet4Address && !target.equals(relay))return;
            super.send(packet);
        }
    }
    public static void main(String[] args) throws Exception {
        verify(Mode.OPEN,Mode.SYMMETRIC,true,true);
        verify(Mode.RESTRICTED,Mode.RESTRICTED,true,false);
        verify(Mode.SYMMETRIC,Mode.SYMMETRIC,false,false);
        verifyIpv6();
        System.out.println("NAT integration checks passed");
    }
    private static void verifyIpv6() throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,0,null)) {
            server.start(); await(() -> server.getPort()>0,"IPv6 test start");
            URI uri=URI.create("ws://127.0.0.1:"+server.getPort()+"/voice");
            var relay=new InetSocketAddress("127.0.0.1",server.udpPort());
            UUID a=Wire.offlineUuid("Ipv6Actor"),b=Wire.offlineUuid("Ipv6Friend"); Sink sa=new Sink(),sb=new Sink();
            try(Ipv6Socket ua=new Ipv6Socket(relay); Ipv6Socket ub=new Ipv6Socket(relay);
                VoiceClient ca=client(uri,a,"Ipv6Actor",sa,List.of(),ua); VoiceClient cb=client(uri,b,"Ipv6Friend",sb,List.of(),ub)) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS);
                ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.hasUdp() && cb.hasUdp() && ca.diagnostics().mappedServers()==1 && cb.diagnostics().mappedServers()==1,"IPv6 test discovery");
                Thread.sleep(300);
                publish(ca,List.of(new InetSocketAddress(InetAddress.getByName("::1"),ua.getLocalPort())));
                publish(cb,List.of(new InetSocketAddress(InetAddress.getByName("::1"),ub.getLocalPort())));
                await(() -> direct(ca) && direct(cb),"IPv6 direct path with IPv4 peer traffic blocked");
                deliver(ca,cb,sa,sb,a,b,"IPv6 direct path with IPv4 unavailable");
                publish(ca,List.of(new InetSocketAddress("8.8.8.8",12345)));
                // The server must discard a client-supplied IPv4 address other than its observed source.
                await(() -> cb.diagnostics().candidates()==1,"Unobserved IPv4 candidate was forwarded");
            }
        }
    }
    private static void publish(VoiceClient client,List<InetSocketAddress> candidates) throws Exception {
        JsonObject message=Wire.message("candidates"); JsonArray array=new JsonArray(); candidates.forEach(e -> array.add(Endpoints.write(e)));
        message.add("candidates",array);
        var send=VoiceClient.class.getDeclaredMethod("send",JsonObject.class); send.setAccessible(true); send.invoke(client,message);
    }
    private static void verify(Mode a,Mode b,boolean expectDirect,boolean delayed) throws Exception {
        try (RelayServer relay=new RelayServer("127.0.0.1",0,0,null);
             RelayServer second=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start(); second.start(); await(() -> relay.getPort()>0 && second.getPort()>0,"NAT relay start");
            URI uri=URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice");
            URI alternative=URI.create("ws://127.0.0.1:"+second.getPort()+"/voice");
            SocketAddress main=new InetSocketAddress("127.0.0.1",relay.udpPort());
            Set<SocketAddress> services=Set.of(main,new InetSocketAddress("127.0.0.1",second.udpPort()));
            try (NatSocket na=new NatSocket(a,services); NatSocket nb=new NatSocket(b,services)) {
                na.peersBlocked=delayed; nb.peersBlocked=delayed;
                UUID pa=Wire.offlineUuid("NatActor"), pb=Wire.offlineUuid("NatFriend");
                Sink sa=new Sink(), sb=new Sink();
                List<NetworkDirectory.Probe> probes=List.of(new NetworkDirectory.Probe(alternative,second.udpPort()));
                try (VoiceClient ca=client(uri,pa,"NatActor",sa,probes,na); VoiceClient cb=client(uri,pb,"NatFriend",sb,probes,nb)) {
                    ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS);
                    ca.nearby(Set.of(pb)); cb.nearby(Set.of(pa));
                    await(() -> ca.peers().size()==1 && cb.peers().size()==1 && ca.hasUdp() && cb.hasUdp(),"NAT discovery / relay bind");
                    await(() -> ca.diagnostics().mappedServers()==2 && cb.diagnostics().mappedServers()==2,"Two STUN observations on same sockets");
                    if (b==Mode.SYMMETRIC && !cb.diagnostics().varyingMapping()) throw new AssertionError("Destination-specific NAT mappings missed");
                    if (b==Mode.SYMMETRIC) await(() -> ca.diagnostics().candidates()>=2,"Extra mapped candidate exchanged");
                    UUID originalA=ca.sessionId(),originalB=cb.sessionId();
                    if (delayed) {
                        deliver(ca,cb,sa,sb,pa,pb,"Relay while direct path is unavailable");
                        int before=ca.diagnostics().candidates();
                        // Allow inbound unknown-port packets to reach A, but keep B blocked.
                        na.peersBlocked=false;
                        try (DatagramSocket attacker=new DatagramSocket(new InetSocketAddress("127.0.0.1",0))) {
                            byte[] forged=new Wire.Frame(Wire.PING,cb.sessionId(),ca.sessionId(),0,Wire.random(32)).encode();
                            attacker.send(new DatagramPacket(forged,forged.length,na.publicEndpoint(main)));
                        }
                        Thread.sleep(250);
                        if(ca.diagnostics().candidates()!=before || direct(ca))
                            throw new AssertionError("Unauthenticated port learned: before="+before+", after="+ca.diagnostics()+", direct="+direct(ca));
                        nb.peersBlocked=false;
                    }
                    if (expectDirect) {
                        await(() -> direct(ca) && direct(cb),"NAT direct path: "+a+" / "+b);
                        deliver(ca,cb,sa,sb,pa,pb,"Direct through "+a+" / "+b);
                        if (delayed) {
                            na.peersBlocked=true; nb.peersBlocked=true;
                            await(() -> !direct(ca) && !direct(cb),"Direct path expiry");
                            deliver(ca,cb,sa,sb,pa,pb,"Relay after direct loss");
                            na.peersBlocked=false; nb.peersBlocked=false;
                            await(() -> direct(ca) && direct(cb),"Automatic retry after direct loss");
                            deliver(ca,cb,sa,sb,pa,pb,"Direct restored without reconnect");
                        }
                    } else {
                        Thread.sleep(2500);
                        if(direct(ca) || direct(cb))throw new AssertionError("Restricted symmetric NAT falsely reported direct");
                        deliver(ca,cb,sa,sb,pa,pb,"Relay for symmetric / symmetric NAT");
                    }
                    if(!originalA.equals(ca.sessionId()) || !originalB.equals(cb.sessionId()))throw new AssertionError("Path change recreated session");
                }
            }
        }
    }
    private static VoiceClient client(URI uri,UUID id,String name,Sink sink,List<NetworkDirectory.Probe> probes,DatagramSocket udp) throws Exception {
        return new VoiceClient(uri,null,new VoiceClient.Identity(id,name,id,name,"d".repeat(64),"minecraft:overworld"),
                true,true,challenge -> CompletableFuture.completedFuture(false),sink,probes,udp);
    }
    private static boolean direct(VoiceClient client) { return client.peers().stream().anyMatch(VoiceClient.PeerInfo::direct); }
    private static void deliver(VoiceClient ca,VoiceClient cb,Sink sa,Sink sb,UUID pa,UUID pb,String label) throws Exception {
        byte[] payload={42,15,7};
        ca.sendAudio(Set.of(pb),0,16,payload,false); cb.sendAudio(Set.of(pa),0,16,payload,false);
        if(!Arrays.equals(sa.audio.poll(3,TimeUnit.SECONDS),payload) || !Arrays.equals(sb.audio.poll(3,TimeUnit.SECONDS),payload))
            throw new AssertionError(label);
        System.out.println(label+": bidirectional encrypted audio passed");
    }
    private static void await(BooleanSupplier condition,String label) throws Exception {
        long until=System.nanoTime()+Duration.ofSeconds(12).toNanos();
        while(!condition.getAsBoolean()) { if(System.nanoTime()>until)throw new AssertionError(label); Thread.sleep(25); }
    }
}
