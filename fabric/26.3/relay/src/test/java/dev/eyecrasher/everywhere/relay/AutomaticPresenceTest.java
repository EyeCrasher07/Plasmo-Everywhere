package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

public final class AutomaticPresenceTest {
    private static final String DIM="minecraft:overworld",WORLD=WorldPresence.fingerprint(91,DIM);
    private static final class Player implements AutoCloseable,VoiceClient.Listener {
        final UUID id;final VoiceClient client;final boolean direct;final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        final ScheduledExecutorService clock=Executors.newSingleThreadScheduledExecutor();
        volatile WorldPresence.Snapshot observations;
        Player(URI uri,String name,String room,boolean direct,boolean udp) throws Exception {
            this.direct=direct;id=Wire.offlineUuid(name);client=new VoiceClient(uri,null,new VoiceClient.Identity(id,name,id,name,room,DIM),direct,udp,
                    challenge -> CompletableFuture.completedFuture(false),this);
            client.connect().get(5,TimeUnit.SECONDS);
            clock.scheduleAtFixedRate(() -> { var value=observations;if(value!=null)client.nearby(value.nearby().keySet(),value); },500,500,TimeUnit.MILLISECONDS);
        }
        void observe(WorldPresence.Snapshot value) { observations=value;client.nearby(value==null?Set.of():value.nearby().keySet(),value); }
        @Override public void audio(VoiceClient.PeerInfo peer,long sequence,int distance,byte[] opus,boolean end) { if(!end)audio.add(opus); }
        @Override public void close() { clock.shutdownNow();client.close(); }
    }
    private static WorldPresence.Snapshot snapshot(Player player,int entity,UUID other,int otherEntity,String world) {
        double x=entity==1?0:3,otherX=otherEntity==1?0:3;
        return new WorldPresence.Snapshot(player.id,DIM,world,100,new WorldPresence.Point(entity,x,80,0),Map.of(other,new WorldPresence.Point(otherEntity,otherX,80,0)));
    }
    private static void await(BooleanSupplier test,String message) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while(!test.getAsBoolean()) { if(System.nanoTime()>deadline)throw new AssertionError(message);Thread.sleep(20); }
    }
    private static void audio(Player from,Player to,int sequence) throws Exception {
        if(from.direct)await(() -> from.client.networkStats().udpDirect()>0 && to.client.networkStats().udpDirect()>0,"P2P recovers after discovery changes");
        byte[] expected={17,(byte)sequence,35};from.client.sendAudio(Set.of(to.id),sequence,32,expected,false);
        if(!Arrays.equals(expected,to.audio.poll(4,TimeUnit.SECONDS)))throw new AssertionError("Cross-address audio "+sequence);
    }
    private static void indexSemantics() {
        PresenceIndex index=new PresenceIndex();UUID a=UUID.randomUUID(),b=UUID.randomUUID(),sa=UUID.randomUUID(),sb=UUID.randomUUID(),copy=UUID.randomUUID();
        var pa=new WorldPresence.Point(1,0,0,0);var pb=new WorldPresence.Point(2,1,0,0);
        var va=new WorldPresence.Snapshot(a,DIM,WORLD,100,pa,Map.of(b,pb));var vb=new WorldPresence.Snapshot(b,DIM,WORLD,100,pb,Map.of(a,pa));
        index.put(sa,va,100);index.put(sb,vb,100);
        if(!sb.equals(index.match(sa,b,200)) || !sa.equals(index.match(sb,a,200)))throw new AssertionError("Unique mutual presence");
        index.put(copy,va,100);
        if(index.match(sa,b,200)!=null || index.match(sb,a,200)!=null)throw new AssertionError("Repeated UUID must be ambiguous in both directions");
        index.put(copy,va,100,true);
        if(index.match(sa,b,200)!=null || !copy.equals(index.match(sb,a,200)) || !sb.equals(index.match(copy,b,200)))
            throw new AssertionError("Verified identity priority must apply in both directions");
        index.remove(copy);if(!sb.equals(index.match(sa,b,200)))throw new AssertionError("Removing ambiguity must recover");
        if(index.match(sa,b,TimeUnit.SECONDS.toNanos(11))!=null || index.expired(TimeUnit.SECONDS.toNanos(11)).size()!=2)
            throw new AssertionError("Presence expiry");
        index.remove(sa);index.remove(sb);if(!index.expired(Long.MAX_VALUE).isEmpty())throw new AssertionError("Presence cleanup");
    }
    private static void pair(boolean mesh,boolean direct,boolean udp) throws Exception {
        RelayServer a=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,16,0);
        RelayServer b=mesh?new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,16,0):a;
        try {
            a.start();if(mesh)b.start();await(() -> a.getPort()>0 && b.getPort()>0,"Local relays start");
            URI ua=URI.create("ws://127.0.0.1:"+a.getPort()+"/voice"),ub=URI.create("ws://127.0.0.1:"+b.getPort()+"/voice");
            if(mesh) {
                String key=HexFormat.of().formatHex(Wire.random(32));
                a.enableFederation(new Federation.Config("presence-test","node-a",key,Map.of("node-b",URI.create(ub.toString().replace("/voice","/federation")))));
                b.enableFederation(new Federation.Config("presence-test","node-b",key,Map.of("node-a",URI.create(ua.toString().replace("/voice","/federation")))));
                await(() -> a.health().getAsJsonObject("federation").get("ready").getAsBoolean() && b.health().getAsJsonObject("federation").get("ready").getAsBoolean(),"Federation ready");
            }
            try(Player alice=new Player(ua,"PresenceA","a".repeat(64),direct,udp);Player bob=new Player(ub,"PresenceB","b".repeat(64),direct,udp)) {
                alice.client.nearby(Set.of(bob.id));bob.client.nearby(Set.of(alice.id));Thread.sleep(250);
                if(!alice.client.peers().isEmpty() || !bob.client.peers().isEmpty())throw new AssertionError("UUID lists alone must not bridge addresses");
                var va=snapshot(alice,1,bob.id,2,WORLD);var vb=snapshot(bob,2,alice.id,1,WORLD);
                alice.observe(va);bob.observe(vb);
                await(() -> alice.client.peers().size()==1 && bob.client.peers().size()==1,"Automatic cross-address discovery");
                if(direct)await(() -> alice.client.networkStats().udpDirect()>0 && bob.client.networkStats().udpDirect()>0,"Automatic cross-address P2P");
                if(udp)await(() -> alice.client.hasUdp() && bob.client.hasUdp(),"UDP bound");
                audio(alice,bob,1);audio(bob,alice,2);
                if(a.health().get("crossAddressPeerLinks").getAsInt()==0)throw new AssertionError("Cross-address links not observable");
                bob.observe(snapshot(bob,2,alice.id,1,WorldPresence.fingerprint(92,DIM)));
                await(() -> alice.client.peers().isEmpty() && bob.client.peers().isEmpty(),"Different world revokes links");
                bob.observe(vb);await(() -> alice.client.peers().size()==1 && bob.client.peers().size()==1,"World recovery");
                try(Player replica=new Player(ua,"PresenceA","c".repeat(64),direct,udp)) {
                    replica.observe(va);
                    await(() -> alice.client.peers().isEmpty() && bob.client.peers().isEmpty() && replica.client.peers().isEmpty(),"Ambiguous repeated UUID must not connect");
                    replica.observe(null);await(() -> alice.client.peers().size()==1 && bob.client.peers().size()==1,"Ambiguity withdrawal recovers");
                }
                bob.observe(null);await(() -> alice.client.peers().isEmpty() && bob.client.peers().isEmpty(),"World/nearby withdrawal revokes links");
                bob.observe(vb);await(() -> alice.client.peers().size()==1 && bob.client.peers().size()==1,"Proximity recovery");audio(alice,bob,3);
                if(!mesh) {
                    bob.clock.shutdownNow();
                    await(() -> alice.client.peers().isEmpty() && bob.client.peers().isEmpty(),"Stale presence expires while voice connection stays alive");
                    if(!alice.client.isReady() || !bob.client.isReady())throw new AssertionError("Presence expiry closed voice control");
                }
                if(direct && (alice.client.networkStats().relayedVoicePackets()!=0 || bob.client.networkStats().relayedVoicePackets()!=0))
                    throw new AssertionError("Automatic discovery replaced P2P with relay audio");
                System.out.println("Automatic presence PASS mesh="+mesh+" direct="+direct+" udp="+udp+" worldIsolation=true ambiguity=true withdrawal=true");
            }
            await(() -> a.health().get("connections").getAsInt()==0 && b.health().get("connections").getAsInt()==0,"Session cleanup");
        } finally { a.close();if(mesh)b.close(); }
    }
    public static void main(String[] args) throws Exception {
        indexSemantics();pair(false,false,false);pair(true,false,true);pair(true,true,true);
    }
}
