package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.java_websocket.handshake.ClientHandshake;

/** Actual sockets and encrypted clients on three independent localhost relays. */
public final class FederationTest {
    private static final String ROOM="d".repeat(64),DIMENSION="minecraft:overworld";
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        final java.util.concurrent.atomic.AtomicInteger ends=new java.util.concurrent.atomic.AtomicInteger();
        @Override public void audio(VoiceClient.PeerInfo peer,long seq,int distance,byte[] opus,boolean end) { if(end)ends.incrementAndGet();else audio.add(opus); }
    }
    private static final class Cluster implements AutoCloseable {
        final RelayServer[] nodes=new RelayServer[3];final String key=HexFormat.of().formatHex(Wire.random(32));
        Cluster(boolean startMesh) throws Exception {
            for(int n=0;n<nodes.length;n++) { nodes[n]=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,32,0,
                    (challenge,name,account) -> CompletableFuture.completedFuture(true));nodes[n].start(); }
            await(() -> Arrays.stream(nodes).allMatch(node -> node.getPort()>0),"relays start");
            if(startMesh)mesh();
        }
        void mesh() throws Exception {
            for(int n=0;n<nodes.length;n++) {
                Map<String,URI> peers=new HashMap<>();for(int p=0;p<nodes.length;p++)if(p!=n)peers.put("node"+p,uri(p,"/federation"));
                nodes[n].enableFederation(new Federation.Config("local-test","node"+n,key,peers));
            }
            await(() -> Arrays.stream(nodes).allMatch(node -> node.health().getAsJsonObject("federation").get("ready").getAsBoolean()),"mesh synchronization");
        }
        URI uri(int node,String path) { return URI.create("ws://127.0.0.1:"+nodes[node].getPort()+path); }
        @Override public void close() { for(RelayServer node:nodes)if(node!=null)node.close(); }
    }
    private static VoiceClient client(Cluster cluster,int node,String name,boolean direct,boolean udp,Sink sink,String room,String dimension) throws Exception {
        UUID id=Wire.offlineUuid(name);
        return new VoiceClient(cluster.uri(node,"/voice"),null,new VoiceClient.Identity(id,name,id,name,room,dimension),direct,udp,
                challenge -> CompletableFuture.completedFuture(false),sink);
    }
    private static void await(BooleanSupplier condition,String message) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while(!condition.getAsBoolean()) { if(System.nanoTime()>deadline)throw new AssertionError(message);Thread.sleep(25); }
    }
    private static void audio(VoiceClient from,String to,Sink sink,int seq) throws Exception {
        byte[] expected={41,3,(byte)seq};from.sendAudio(Set.of(Wire.offlineUuid(to)),seq,32,expected,false);
        if(!Arrays.equals(expected,sink.audio.poll(4,TimeUnit.SECONDS)))throw new AssertionError("Cross-node voice delivery "+seq);
    }
    private static void pair(boolean direct,boolean udp,boolean lateMesh) throws Exception {
        try(Cluster cluster=new Cluster(!lateMesh)) {
            Sink sa=new Sink(),sb=new Sink(),sc=new Sink(),sd=new Sink();
            try(VoiceClient a=client(cluster,0,"FederationA",direct,udp,sa,ROOM,DIMENSION);
                VoiceClient b=client(cluster,1,"FederationB",direct,udp,sb,ROOM,DIMENSION);
                VoiceClient c=client(cluster,2,"FederationC",direct,udp,sc,"c".repeat(64),DIMENSION);
                VoiceClient d=client(cluster,2,"FederationD",direct,udp,sd,ROOM,"minecraft:the_nether")) {
                for(VoiceClient client:List.of(a,b,c,d))client.connect().get(5,TimeUnit.SECONDS);
                a.nearby(Set.of(Wire.offlineUuid("FederationB"),Wire.offlineUuid("FederationC"),Wire.offlineUuid("FederationD")));
                b.nearby(Set.of(Wire.offlineUuid("FederationA")));c.nearby(Set.of(Wire.offlineUuid("FederationA")));d.nearby(Set.of(Wire.offlineUuid("FederationA")));
                if(lateMesh)cluster.mesh();
                await(() -> a.peers().size()==1 && b.peers().size()==1,"mutual cross-node discovery");
                if(!c.peers().isEmpty() || !d.peers().isEmpty())throw new AssertionError("Server or dimension isolation failed");
                if(udp)await(() -> a.hasUdp() && b.hasUdp(),"UDP binding");
                if(direct)await(() -> a.networkStats().udpDirect()>0 && b.networkStats().udpDirect()>0,"cross-node P2P route");
                audio(a,"FederationB",sb,1);audio(b,"FederationA",sa,2);
                a.sendAudio(Set.of(Wire.offlineUuid("FederationB")),3,32,new byte[0],true);
                await(() -> sb.ends.get()==1,"cross-node end of speech");
                if(cluster.nodes[0].federationFrame("node2",new Wire.Frame(Wire.VOICE,b.sessionId(),a.sessionId(),0,new byte[]{1}),new byte[]{1}))
                    throw new AssertionError("A node forged another node's media source");
                if(sc.audio.poll(100,TimeUnit.MILLISECONDS)!=null || sd.audio.poll(100,TimeUnit.MILLISECONDS)!=null)
                    throw new AssertionError("Cross-room audio leaked");
                long forwarded=Arrays.stream(cluster.nodes).mapToLong(node -> node.health().getAsJsonObject("federation").get("sentFrames").getAsLong()).sum();
                // UDP speech uses P2P; its reliable end marker may still use the control connection.
                if(direct && (a.networkStats().relayedVoicePackets()!=0 || b.networkStats().relayedVoicePackets()!=0 || forwarded>1))
                    throw new AssertionError("P2P audio unnecessarily crossed VPS links");
                if(!direct && forwarded<2)throw new AssertionError("Fallback bypassed tested inter-node forwarding");
                b.nearby(Set.of());await(() -> a.peers().isEmpty(),"withdraw remote proximity");
                b.nearby(Set.of(Wire.offlineUuid("FederationA")));await(() -> a.peers().size()==1,"restore remote proximity");
                // Authentication failures must not consume normal voice slots or disturb existing sessions.
                CountDownLatch denied=new CountDownLatch(1);
                WebSocketClient rogue=new WebSocketClient(cluster.uri(2,"/federation"),Map.of("Authorization","Bearer "+"0".repeat(64),
                        "X-Everywhere-Node","node0","X-Everywhere-Network","local-test")) {
                    @Override public void onOpen(ServerHandshake handshake) {}
                    @Override public void onMessage(String text) {}
                    @Override public void onClose(int code,String reason,boolean remote) { if(code==1008)denied.countDown(); }
                    @Override public void onError(Exception error) {}
                };
                rogue.connect();if(!denied.await(5,TimeUnit.SECONDS))throw new AssertionError("Unauthenticated node accepted");rogue.close();
                int oldPort=cluster.nodes[1].getPort();cluster.nodes[1].close();await(() -> a.peers().isEmpty(),"failed node removes remote peers");
                RelayServer replacement=new RelayServer("127.0.0.1",oldPort,"127.0.0.1",0,null,false,32,0);
                cluster.nodes[1]=replacement;
                replacement.enableFederation(new Federation.Config("local-test","node1",cluster.key,
                        Map.of("node0",cluster.uri(0,"/federation"),"node2",cluster.uri(2,"/federation"))));replacement.start();
                try {
                    await(() -> Arrays.stream(cluster.nodes).allMatch(node -> node.health().getAsJsonObject("federation").get("ready").getAsBoolean()),"mesh recovers after restart");
                } catch(AssertionError failure) {
                    for(int n=0;n<cluster.nodes.length;n++)System.err.println("Recovery node "+n+": "+cluster.nodes[n].health());
                    throw failure;
                }
                Sink recovered=new Sink();
                try(VoiceClient afterRestart=client(cluster,1,"FederationB",direct,udp,recovered,ROOM,DIMENSION)) {
                    afterRestart.connect().get(5,TimeUnit.SECONDS);afterRestart.nearby(Set.of(Wire.offlineUuid("FederationA")));
                    await(() -> a.peers().size()==1 && afterRestart.peers().size()==1,"discovery after node restart");
                    audio(a,"FederationB",recovered,5);audio(afterRestart,"FederationA",sa,6);
                }
                System.out.println("Federation PASS direct="+direct+" udp="+udp+" lateSnapshot="+lateMesh+" isolation=true auth=true disconnect=true restart=true endOfSpeech=true");
            }
        }
    }
    public static void main(String[] args) throws Exception {
        pair(false,false,false);pair(false,true,true);pair(true,true,false);
        identityPriority();
        stalledSynchronization();
        RegionalDirectoryTest.verify();
        try { new Federation.Config("test","a","0".repeat(64),Map.of("b",URI.create("ws://example.com/federation")));throw new AssertionError("Public plaintext allowed"); }
        catch(IllegalArgumentException expected) {}
        System.out.println("Federation integration checks passed");
    }
    private static void stalledSynchronization() throws Exception {
        java.util.concurrent.atomic.AtomicInteger connections=new java.util.concurrent.atomic.AtomicInteger();
        RelayServer node=new RelayServer("127.0.0.1",0,0,null);
        var peer=new org.java_websocket.server.WebSocketServer(new java.net.InetSocketAddress("127.0.0.1",0)) {
            @Override public void onOpen(org.java_websocket.WebSocket socket,ClientHandshake handshake) {
                if(connections.incrementAndGet()>1)socket.send(Wire.GSON.toJson(Wire.message("sync")));
            }
            @Override public void onClose(org.java_websocket.WebSocket socket,int code,String reason,boolean remote) {}
            @Override public void onMessage(org.java_websocket.WebSocket socket,String text) {}
            @Override public void onError(org.java_websocket.WebSocket socket,Exception error) {}
            @Override public void onStart() {}
        };
        try {
            node.start();peer.start();await(() -> node.getPort()>0 && peer.getPort()>0,"Stalled-sync fixture starts");
            String key=HexFormat.of().formatHex(Wire.random(32));
            node.enableFederation(new Federation.Config("stalled-test","a",key,Map.of("b",URI.create("ws://127.0.0.1:"+peer.getPort()+"/federation"))));
            await(() -> connections.get()>0,"Intentionally stalled first synchronization");
            if(node.health().getAsJsonObject("federation").get("ready").getAsBoolean())throw new AssertionError("Missing synchronization reported ready");
            await(() -> connections.get()>=2 && node.health().getAsJsonObject("federation").get("ready").getAsBoolean(),"Stalled open link automatically reconnects");
            System.out.println("Federation synchronization deadline passed: an open unsynchronized connection recovers automatically");
        } finally { node.close();peer.stop(1000); }
    }
    private static void identityPriority() throws Exception {
        try(Cluster cluster=new Cluster(true)) {
            String name="IdentityTest";UUID player=Wire.offlineUuid(name);Sink sink=new Sink();
            try(VoiceClient original=client(cluster,0,name,false,false,sink,ROOM,DIMENSION)) {
                original.connect().get(5,TimeUnit.SECONDS);
                await(() -> cluster.nodes[1].health().getAsJsonObject("federation").get("remoteSessions").getAsInt()==1,"remote identity announced");
                try(VoiceClient duplicate=client(cluster,1,name,false,false,new Sink(),ROOM,DIMENSION)) {
                    try { duplicate.connect().get(5,TimeUnit.SECONDS);throw new AssertionError("Unverified cross-node duplicate admitted"); }
                    catch(ExecutionException expected) {}
                }
                try(VoiceClient verified=new VoiceClient(cluster.uri(1,"/voice"),null,new VoiceClient.Identity(player,name,player,name,ROOM,DIMENSION),false,false,
                            challenge -> CompletableFuture.completedFuture(true),new Sink());
                    VoiceClient differentRoom=client(cluster,2,name,false,false,new Sink(),"a".repeat(64),DIMENSION)) {
                    verified.connect().get(5,TimeUnit.SECONDS);differentRoom.connect().get(5,TimeUnit.SECONDS);
                    await(() -> !original.isReady(),"verified owner reclaims identity across nodes");
                    if(!verified.isReady() || !differentRoom.isReady())throw new AssertionError("Identity priority crossed room boundaries");
                }
            }
        }
        System.out.println("Federation identity priority passed: duplicate rejection, verified ownership and room isolation");
    }
}
