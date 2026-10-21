package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import java.net.URI;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Persistence, day boundaries, rejection/resumption and existing-client audio compatibility. */
public final class UsageStatisticsTest {
    private static final class Time extends Clock {
        Instant now=Instant.parse("2026-10-20T23:59:50Z");
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now; }
    }
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer,long sequence,int distance,byte[] data,boolean end) {
            if(!end)audio.add(data);
        }
    }
    public static void main(String[] args) throws Exception {
        Path temp=Files.createTempDirectory("everywhere-usage-test-");
        try {
            verifyCounters(temp.resolve("history.json"));
            verifyCorruptHistory(temp.resolve("corrupt.json"));
            verifyWriteFailure(temp.resolve("broken"));
            verifyRelay(temp.resolve("relay.json"));
        } finally {
            try(var paths=Files.walk(temp)) {
                for(Path path:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(path);
            }
        }
        System.out.println("Usage statistics: persistent aggregates, UTC rollover/retention, corrupt history preservation, failed-save recovery, rejected joins, proven reconnect and bidirectional audio passed");
    }
    private static void verifyCounters(Path file) throws Exception {
        Time time=new Time();
        try(UsageStatistics stats=new UsageStatistics(file,time,false)) {
            stats.joined(true,false,1); stats.joined(false,false,2);
            require(n(stats.snapshot(),"sessionsStarted")==2 && n(stats.snapshot(),"peakOnline")==2,"Initial joins/peak");
            time.now=time.now.plusSeconds(20);
            JsonObject midnight=stats.snapshot();
            require(n(midnight.getAsJsonObject("today"),"sessionsStarted")==0 && n(midnight.getAsJsonObject("today"),"peakOnline")==2,"Midnight active peak");
            stats.left(20_000,1); stats.joined(false,true,2); stats.left(10_000,1); stats.left(30_000,0);
            stats.persist();
            JsonObject json=stats.snapshot();
            require(n(json,"sessionsStarted")==3 && n(json,"resumedSessions")==1 && n(json,"completedSessions")==3,"Lifecycle counters");
            require(n(json,"connectedMillis")==60_000 && n(json,"longestSessionMillis")==30_000,"Completed session durations");
            require(n(json,"verifiedSessions")==1 && n(json,"unverifiedSessions")==2,"Verification aggregates");
            require(json.get("persistent").getAsBoolean(),"Healthy persistence");
        }
        try(UsageStatistics restored=new UsageStatistics(file,time,false)) {
            require(n(restored.snapshot(),"sessionsStarted")==3 && n(restored.snapshot(),"connectedMillis")==60_000,"Restart retained totals");
            for(int day=0;day<50;day++) {
                time.now=time.now.plusSeconds(86400); restored.joined(false,false,1); restored.left(1000,0);
            }
            require(restored.snapshot().getAsJsonArray("days").size()==UsageStatistics.RETAIN_DAYS,"Bounded daily history");
            time.now=time.now.minusSeconds(100*86400L);
            require(restored.snapshot().getAsJsonArray("days").size()<=UsageStatistics.RETAIN_DAYS,"Backwards clock history bound");
        }
        String data=Files.readString(file);
        for(String forbidden:List.of("player","uuid","room","accountName","udpKey","sessionProof","127.0.0.1"))
            require(!data.contains(forbidden),"Identity material in statistics file");
    }
    private static void verifyCorruptHistory(Path file) throws Exception {
        Files.writeString(file,"{\"schema\":1,\"broken\":true}");
        String before=Files.readString(file);
        try(UsageStatistics stats=new UsageStatistics(file,new Time(),false)) {
            stats.joined(false,false,1); stats.left(1000,0); stats.persist();
            require(!stats.snapshot().get("persistent").getAsBoolean(),"Corrupt file presented as durable");
        }
        require(Files.readString(file).equals(before),"Corrupt history overwritten");
    }
    private static void verifyWriteFailure(Path parent) throws Exception {
        Files.writeString(parent,"blocks a directory");
        UsageStatistics stats=new UsageStatistics(parent.resolve("usage.json"),new Time(),false);
        try {
            stats.joined(false,false,1); stats.left(1000,0); stats.persist();
            require(!stats.snapshot().get("persistent").getAsBoolean(),"Save failure hidden");
            Files.delete(parent); Files.createDirectory(parent); stats.persist();
            require(stats.snapshot().get("persistent").getAsBoolean(),"Save failure failed to recover");
            require(n(stats.snapshot(),"sessionsStarted")==1,"Save failure lost in-memory count");
        } finally { stats.close(); }
    }
    private static void verifyRelay(Path file) throws Exception {
        RelayServer relay=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,4,0,
                (challenge,name,account)->CompletableFuture.completedFuture(true));
        relay.enableUsage(file);
        try(relay) {
            relay.start(); await(()->relay.getPort()>0,"Relay startup");
            URI uri=URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice");
            Sink one=new Sink(),two=new Sink(),resumedSink=new Sink();
            try(VoiceClient actor=client(uri,"UsageActor",true,one,null);
                VoiceClient friend=client(uri,"UsageFriend",false,two,null)) {
                actor.connect().get(5,TimeUnit.SECONDS); friend.connect().get(5,TimeUnit.SECONDS);
                require(n(usage(relay),"sessionsStarted")==2,"Successful joins not counted once");
                try(VoiceClient duplicate=client(uri,"UsageFriend",false,new Sink(),null)) {
                    try { duplicate.connect().get(5,TimeUnit.SECONDS); throw new AssertionError("Duplicate unexpectedly joined"); }
                    catch(ExecutionException expected) {}
                }
                require(n(usage(relay),"sessionsStarted")==2,"Rejected join counted as session");
                try(VoiceClient resumed=client(uri,"UsageFriend",false,resumedSink,friend.resumption())) {
                    resumed.connect().get(5,TimeUnit.SECONDS);
                    require(n(usage(relay),"resumedSessions")==1 && n(usage(relay),"sessionsStarted")==3,"Proof-verified reconnect");
                    require(n(usage(relay),"peakOnline")==2 && relay.health().get("connections").getAsInt()==2,"Reconnect inflated concurrency peak");
                    actor.nearby(Set.of(Wire.offlineUuid("UsageFriend"))); resumed.nearby(Set.of(Wire.offlineUuid("UsageActor")));
                    await(()->actor.peers().size()==1 && resumed.peers().size()==1,"Peer discovery");
                    byte[] sound={7,42,9}; actor.sendAudio(Set.of(Wire.offlineUuid("UsageFriend")),0,16,sound,false);
                    require(Arrays.equals(resumedSink.audio.poll(5,TimeUnit.SECONDS),sound),"Forward audio with stats");
                    resumed.sendAudio(Set.of(Wire.offlineUuid("UsageActor")),0,16,sound,false);
                    require(Arrays.equals(one.audio.poll(5,TimeUnit.SECONDS),sound),"Reverse audio with stats");
                }
            }
            await(()->relay.health().get("connections").getAsInt()==0,"Session cleanup");
            require(n(usage(relay),"completedSessions")==3 && n(usage(relay),"connectedMillis")>0,"Completed durations counted once");
        }
        JsonObject saved=JsonParser.parseString(Files.readString(file)).getAsJsonObject();
        require(n(saved,"sessionsStarted")==3 && n(saved,"completedSessions")==3,"Shutdown failed to persist");
    }
    private static VoiceClient client(URI uri,String name,boolean verified,Sink sink,VoiceClient.Resumption resumption) throws Exception {
        UUID player=Wire.offlineUuid(name);
        return new VoiceClient(uri,null,new VoiceClient.Identity(player,name,player,name,"b".repeat(64),"minecraft:overworld"),false,false,
                challenge->CompletableFuture.completedFuture(verified),sink,List.of(),resumption);
    }
    private static JsonObject usage(RelayServer relay) { return relay.health().getAsJsonObject("usage"); }
    private static long n(JsonObject object,String name) { return object.get(name).getAsLong(); }
    private static void require(boolean condition,String message) { if(!condition)throw new AssertionError(message); }
    private static void await(BooleanSupplier condition,String name) throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(!condition.getAsBoolean()) { if(System.nanoTime()>end)throw new AssertionError(name); Thread.sleep(20); }
    }
}
