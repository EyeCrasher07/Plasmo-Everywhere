package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.*;
import java.net.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

public final class RelayTest {
    private static String ROOM = "a".repeat(64);
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio = new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo p, long seq, int distance, byte[] bytes, boolean end) { if (!end) audio.add(bytes); }
    }
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            if(args.length>2) {
                if(!args[2].matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Test room");
                ROOM=args[2];
            }
            URI publicRelay = URI.create(args[0]);
            List<NetworkDirectory.Probe> probes=List.of(new NetworkDirectory.Probe(publicRelay,24454));
            if(args.length>1 && !args[1].isBlank()) {
                var route=NetworkDirectory.lookup(URI.create(args[1]),ROOM).get(8,TimeUnit.SECONDS);
                publicRelay=route.control(); if(!route.probes().isEmpty())probes=route.probes();
                System.out.println("Public HTTPS directory selected a voice node");
            }
            runPair(publicRelay, false, false, "Public TLS WebSocket fallback");
            Thread.sleep(2100);
            runPair(publicRelay, false, true, "Public authenticated UDP relay");
            verifyStun(probes);
            System.out.println("Public relay checks passed"); return;
        }
        UdpSocketBufferTest.main(new String[0]);
        RelayCloseLockTest.main(new String[0]);
        RelayServer server = new RelayServer("127.0.0.1", 0, 0, null); server.start();
        try {
            await(() -> server.getPort() > 0, "Server start");
            runPair(server, false, false, "WSS fallback equivalent (loopback WS)");
            // New connections are deliberately rate limited; different tests use the same loopback IP.
            Thread.sleep(1100);
            runPair(server, false, true, "Authenticated UDP relay");
            Thread.sleep(2100);
            runPair(server, true, true, "Direct UDP with encrypted keepalives");
        } finally { server.close(); }
        verifyIsolation();
        verifyCapacity();
        RelayTopologyTest.main(new String[0]);
        UdpLeaseTest.main(new String[0]);
        WebSocketBoundsTest.main(new String[0]);
        NatTraversalTest.main(new String[0]);
        AutomaticRoutesTest.main(new String[0]);
        PriorityTest.main(new String[0]);
        SessionTest.main(new String[0]);
        DirectoryTest.main(new String[0]);
        System.out.println("Relay integration checks passed");
    }
    private static void verifyStun(List<NetworkDirectory.Probe> probes) throws Exception {
        Set<InetSocketAddress> observed=new HashSet<>();
        try(DatagramSocket socket=new DatagramSocket()) {
            socket.setSoTimeout(3000);
            for(var probe:probes) {
                var target=new InetSocketAddress(probe.control().getHost(),probe.udpPort());
                byte[] nonce=Wire.random(12),request=Stun.request(nonce);
                socket.send(new DatagramPacket(request,request.length,target));
                byte[] buffer=new byte[129]; DatagramPacket packet=new DatagramPacket(buffer,buffer.length); socket.receive(packet);
                if(!packet.getSocketAddress().equals(target))throw new AssertionError("STUN response source");
                observed.add(Stun.mapped(Arrays.copyOf(buffer,packet.getLength()),nonce));
            }
        }
        System.out.println("Public STUN: "+probes.size()+" trusted nodes replied; mapping "+(observed.size()==1?"stable":"varies across destinations"));
    }
    private static void runPair(RelayServer relay, boolean direct, boolean udp, String label) throws Exception {
        runPair(URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice"), direct, udp, label);
    }
    private static void runPair(URI uri, boolean direct, boolean udp, String label) throws Exception {
        UUID a = Wire.offlineUuid("EverywhereActor"), b = Wire.offlineUuid("EverywhereFriend");
        Sink sa = new Sink(), sb = new Sink();
        try (VoiceClient ca = client(uri,a,"EverywhereActor",direct,udp,sa); VoiceClient cb = client(uri,b,"EverywhereFriend",direct,udp,sb)) {
            ca.connect().get(10, TimeUnit.SECONDS); cb.connect().get(10, TimeUnit.SECONDS);
            ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
            await(() -> ca.peers().size()==1 && cb.peers().size()==1, "Automatic discovery");
            if (udp) await(() -> ca.hasUdp() && cb.hasUdp(), "UDP return-routability");
            if (direct) await(() -> ca.peers().stream().allMatch(VoiceClient.PeerInfo::direct) && cb.peers().stream().allMatch(VoiceClient.PeerInfo::direct), "Direct path");
            byte[] payload = {42,27,5}; ca.sendAudio(Set.of(b),0,16,payload,false);
            if (!Arrays.equals(sb.audio.poll(5,TimeUnit.SECONDS),payload)) throw new AssertionError("Audio delivery: "+label);
            cb.sendAudio(Set.of(a),0,16,payload,false);
            if (!Arrays.equals(sa.audio.poll(5,TimeUnit.SECONDS),payload)) throw new AssertionError("Reverse audio");
            PeerCrypto before=peerCrypto(ca);
            byte[] plain=java.nio.ByteBuffer.allocate(10+payload.length).putLong(77).putShort((short)16).put(payload).array();
            Wire.Frame recorded=before.seal(Wire.VOICE,plain);
            replayThroughControl(ca,recorded.encode());
            if(!Arrays.equals(sb.audio.poll(5,TimeUnit.SECONDS),payload))throw new AssertionError("Recorded frame delivery");
            if (direct && udp) {
                // Simulate a transport disappearing while control stays connected.
                var field=VoiceClient.class.getDeclaredField("udp"); field.setAccessible(true);
                ((DatagramSocket)field.get(ca)).close(); ((DatagramSocket)field.get(cb)).close();
                Thread.sleep(500);
                ca.sendAudio(Set.of(b),1,16,payload,false);
                if (!Arrays.equals(sb.audio.poll(5,TimeUnit.SECONDS),payload)) throw new AssertionError("Automatic WebSocket fallback after UDP loss");
                System.out.println("UDP disappeared during session: automatic WebSocket fallback passed");
            }
            cb.nearby(Set.of()); await(() -> ca.peers().isEmpty(), "Revoke discovery");
            ca.sendAudio(Set.of(b),1,16,payload,false);
            if (sb.audio.poll(250,TimeUnit.MILLISECONDS)!=null) throw new AssertionError("Revoked listener received audio");
            cb.nearby(Set.of(a)); await(() -> ca.peers().size()==1 && cb.peers().size()==1,"Rediscovery");
            if(peerCrypto(ca).seal(Wire.VOICE,plain).sequence()<=recorded.sequence())throw new AssertionError("AES-GCM nonce reused after rediscovery");
            replayThroughControl(ca,recorded.encode());
            if(sb.audio.poll(300,TimeUnit.MILLISECONDS)!=null)throw new AssertionError("Replay accepted after rediscovery");
            ca.sendAudio(Set.of(b),2,16,payload,false);
            if(!Arrays.equals(sb.audio.poll(5,TimeUnit.SECONDS),payload))throw new AssertionError("Audio did not resume after rediscovery");
            System.out.println(label+": discovery, bidirectional delivery, revocation, rediscovery and replay protection passed");
        }
    }
    private static PeerCrypto peerCrypto(VoiceClient client) throws Exception {
        var field=VoiceClient.class.getDeclaredField("peers"); field.setAccessible(true);
        Object peer=((Map<?,?>)field.get(client)).values().iterator().next();
        var crypto=peer.getClass().getDeclaredField("crypto"); crypto.setAccessible(true); return (PeerCrypto)crypto.get(peer);
    }
    private static void replayThroughControl(VoiceClient client,byte[] frame) throws Exception {
        var send=VoiceClient.class.getDeclaredMethod("sendBinary",byte[].class); send.setAccessible(true); send.invoke(client,(Object)frame);
    }
    private static VoiceClient client(URI uri, UUID player, String name, boolean direct, boolean udp, Sink sink) throws Exception {
        return client(uri, player, name, direct, udp, sink, ROOM, "minecraft:overworld");
    }
    private static VoiceClient client(URI uri, UUID player, String name, boolean direct, boolean udp, Sink sink, String room, String dimension) throws Exception {
        return new VoiceClient(uri,null,new VoiceClient.Identity(player,name,player,name,room,dimension), direct, udp,
                challenge -> CompletableFuture.completedFuture(false),sink);
    }
    private static void verifyIsolation() throws Exception {
        try (RelayServer server = new RelayServer("127.0.0.1",0,0,null)) {
            server.start(); await(() -> server.getPort()>0,"Isolation server start");
            URI uri=URI.create("ws://127.0.0.1:"+server.getPort()+"/voice");
            UUID a=Wire.offlineUuid("EverywhereActor"), b=Wire.offlineUuid("EverywhereFriend"), c=Wire.offlineUuid("NetherFriend");
            Sink sa=new Sink(), sb=new Sink(), sc=new Sink(), sd=new Sink();
            try (VoiceClient ca=client(uri,a,"EverywhereActor",false,false,sa);
                 VoiceClient cb=client(uri,b,"EverywhereFriend",false,false,sb,"b".repeat(64),"minecraft:overworld");
                 VoiceClient cc=client(uri,c,"NetherFriend",false,false,sc,ROOM,"minecraft:the_nether");
                 VoiceClient cd=client(uri,b,"EverywhereFriend",false,false,sd)) {
                for (VoiceClient client : List.of(ca,cb,cc,cd))client.connect().get(10,TimeUnit.SECONDS);
                ca.nearby(Set.of(b,c)); cb.nearby(Set.of(a)); cc.nearby(Set.of(a)); cd.nearby(Set.of(a));
                await(() -> ca.peers().size()==1 && cd.peers().size()==1,"Room discovery");
                if(!cb.peers().isEmpty() || !cc.peers().isEmpty())throw new AssertionError("Discovery leaked across server/dimension");
                byte[] payload={71,5}; ca.sendAudio(Set.of(b,c),0,32,payload,false);
                if(!Arrays.equals(sd.audio.poll(3,TimeUnit.SECONDS),payload))throw new AssertionError("Same-server delivery");
                if(sb.audio.poll(250,TimeUnit.MILLISECONDS)!=null || sc.audio.poll(250,TimeUnit.MILLISECONDS)!=null)
                    throw new AssertionError("Audio leaked across server/dimension");
                System.out.println("Separate Minecraft servers and dimensions: discovery and audio isolation passed");
            }
        }
    }
    private static void verifyCapacity() throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,0)) {
            server.start(); await(() -> server.getPort()>0,"Capacity server start");
            URI uri=URI.create("ws://127.0.0.1:"+server.getPort()+"/voice");
            UUID a=Wire.offlineUuid("EverywhereActor"),b=Wire.offlineUuid("EverywhereFriend"),c=Wire.offlineUuid("LimitTest");
            Sink sa=new Sink(),sb=new Sink();
            try(VoiceClient ca=client(uri,a,"EverywhereActor",false,false,sa);
                VoiceClient cb=client(uri,b,"EverywhereFriend",false,false,sb);
                VoiceClient cc=client(uri,c,"LimitTest",false,false,new Sink())) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS);
                try { cc.connect().get(5,TimeUnit.SECONDS); throw new AssertionError("Configured capacity was ignored"); }
                catch(ExecutionException expected) {}
                ca.nearby(Set.of(b));cb.nearby(Set.of(a)); await(() -> ca.peers().size()==1,"Peers at capacity");
                byte[] payload={5,11};ca.sendAudio(Set.of(b),0,16,payload,false);
                if(!Arrays.equals(sb.audio.poll(3,TimeUnit.SECONDS),payload))throw new AssertionError("Existing clients disrupted at capacity");
                System.out.println("Configured capacity: extra session rejected, existing voice unaffected");
            }
        }
    }
    private static void await(BooleanSupplier condition, String label) throws Exception {
        long until=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) { if(System.nanoTime()>until)throw new AssertionError(label); Thread.sleep(25); }
    }
}
