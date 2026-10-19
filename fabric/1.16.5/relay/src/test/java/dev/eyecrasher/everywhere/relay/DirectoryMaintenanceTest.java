package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.*;
import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Lifecycle tests use a controlled clock; node snapshots additionally exercise real admitted sessions. */
public final class DirectoryMaintenanceTest {
    private static final String A="{\"id\":\"a\",\"control\":\"wss://a.example/voice\",\"weight\":1}";
    private static final String B="{\"id\":\"b\",\"control\":\"wss://b.example/voice\",\"weight\":1}";
    public static void main(String[] ignored) throws Exception {
        Path dir=Files.createTempDirectory("everywhere-directory-lifecycle-");
        try {
            AtomicLong now=new AtomicLong(1_000_000); Path config=dir.resolve("nodes.json"),state=dir.resolve("routes.json");
            Files.writeString(config,"{\"nodes\":["+A+","+B+"]}");
            RoutingDirectory routing=new RoutingDirectory(config,state,true,now::get,1000);
            unavailable(() -> routing.route(room(1)),"Unknown nodes accepted a new group");
            observe(routing,"a",true,2,2,Set.of(),now); observe(routing,"b",true,64,0,Set.of(),now);
            var first=routing.route(room(1)); require(node(first).equals("b"),"New room selected a full node");
            long lease=first.get("validUntil").getAsLong();
            require(lease==now.get()+RoutingDirectory.LEASE_MILLIS,"Bounded route reservation");
            observe(routing,"b",false,64,0,Set.of(),now);
            require(node(routing.route(room(1))).equals("b"),"Failure silently migrated an existing group");
            observe(routing,"a",true,2,0,Set.of(),now);
            require(node(routing.route(room(2))).equals("a"),"New room ignored node health");
            now.addAndGet(RoutingDirectory.REPORT_MILLIS+1);
            unavailable(() -> routing.route(room(3)),"Stale health report accepted a new group");
            require(routing.collect()==0 && routing.assignmentCount()==2,"Unknown health evicted assignments");
            observe(routing,"a",true,2,0,Set.of(),now); observe(routing,"b",true,64,0,Set.of(),now);
            require(routing.collect()==0,"GC broke a still-advertised mirror lease");
            now.set(lease+1); observe(routing,"a",true,2,0,Set.of(),now); observe(routing,"b",true,64,0,Set.of(room(1)),now);
            require(routing.collect()==1 && routing.assignmentCount()==1,"GC did not preserve only the active room");
            // Activity refreshes lastSeen even without new /route calls during a long game.
            now.addAndGet(500); observe(routing,"a",true,2,0,Set.of(),now); observe(routing,"b",true,64,0,Set.of(),now);
            require(routing.collect()==0,"A just-ended long game was aged from its original lookup");
            now.addAndGet(501); observe(routing,"a",true,2,0,Set.of(),now); observe(routing,"b",true,64,0,Set.of(),now);
            require(routing.collect()==1 && routing.assignmentCount()==0,"Idle room leaked indefinitely");
            RoutingDirectory restored=new RoutingDirectory(config,state,true,now::get,1000);
            require(restored.assignmentCount()==0,"GC was not durable across restart");

            // Legacy flat state is accepted and upgraded in place only on a successful durable response.
            Files.writeString(state,"{\""+room(4)+"\":\"a\"}");
            RoutingDirectory legacy=new RoutingDirectory(config,state,false,now::get,1000);
            require(node(legacy.route(room(4))).equals("a"),"Legacy room affinity changed");
            require(Wire.parse(Files.readString(state)).get("version").getAsInt()==2,"Legacy routes not migrated");

            // A publish must fail atomically if the assignment cannot be made durable.
            Path blocked=dir.resolve("blocked"),badState=blocked.resolve("routes.json");
            RoutingDirectory broken=new RoutingDirectory(config,badState,false,now::get,1000);
            Files.writeString(blocked,"not a directory");
            unavailable(() -> broken.route(room(5)),"Undurable assignment was published");
            require(broken.assignmentCount()==0,"Failed persistence left an assignment in memory");
            verifyLargeNodeReports();
            verifyNodeSnapshot();
            verifyRemoteReports(dir);
            System.out.println("Directory maintenance: fresh health/capacity, active-room GC, reserved leases, restart and persistence rollback passed");
        } finally { remove(dir); }
    }
    private static void verifyLargeNodeReports() {
        com.google.gson.JsonObject report=new com.google.gson.JsonObject();
        report.addProperty("status","ok"); report.addProperty("capacity",1152); report.addProperty("connections",1000);
        com.google.gson.JsonArray rooms=new com.google.gson.JsonArray();
        for(int n=0;n<1000;n++)rooms.add(room(10000+n)); report.add("activeRooms",rooms);
        byte[] encoded=Wire.GSON.toJson(report).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        require(encoded.length>Wire.MAX_TEXT,"Large report fixture must exceed ordinary player-message bound");
        var decoded=DirectoryNodeMonitor.parse(DirectoryNodeMonitor.parseBody(encoded),1_000_000);
        require(decoded.capacity()==1152 && decoded.connections()==1000 && decoded.activeRooms().size()==1000,"Thousand-client node report was rejected");
        report.addProperty("capacity",RelayServer.MAX_SESSIONS+1);
        try { DirectoryNodeMonitor.parse(report,1_000_000); throw new AssertionError("Unbounded node capacity accepted"); }
        catch(IllegalArgumentException expected) {}
        report.addProperty("capacity",1152); report.addProperty("connections",1153);
        try { DirectoryNodeMonitor.parse(report,1_000_000); throw new AssertionError("Over-capacity report accepted"); }
        catch(IllegalArgumentException expected) {}
    }
    private static void verifyNodeSnapshot() throws Exception {
        String key="1".repeat(64),room=room(6); UUID player=Wire.offlineUuid("DirectoryGuest");
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null);
            HealthServer health=new HealthServer(relay,0,null,key);
            HttpClient http=HttpClient.newHttpClient()) {
            relay.start(); long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(relay.getPort()==0) { if(System.nanoTime()>deadline)throw new AssertionError("Relay startup"); Thread.sleep(10); }
            URI endpoint=URI.create("http://127.0.0.1:"+health.port()+"/directory-node");
            var bodies=HttpResponse.BodyHandlers.ofString();
            require(http.send(HttpRequest.newBuilder(endpoint).GET().build(),bodies).statusCode()==403,"Private room list exposed without key");
            require(http.send(HttpRequest.newBuilder(endpoint).header("Authorization","Bearer "+"2".repeat(64)).GET().build(),bodies).statusCode()==403,"Wrong directory key accepted");
            require(http.send(HttpRequest.newBuilder(URI.create(endpoint+"?room=x")).header("Authorization","Bearer "+key).GET().build(),bodies).statusCode()==404,"Private endpoint accepted query");
            try(VoiceClient voice=new VoiceClient(URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice"),null,
                    new VoiceClient.Identity(player,"DirectoryGuest",player,"DirectoryGuest",room,"minecraft:overworld"),
                    false,false,c -> CompletableFuture.completedFuture(false),new VoiceClient.Listener() {})) {
                voice.connect().get(5,TimeUnit.SECONDS);
                var response=http.send(HttpRequest.newBuilder(endpoint).header("Authorization","Bearer "+key).GET().build(),bodies);
                var snapshot=Wire.parse(response.body()); require(response.statusCode()==200,"Authenticated node report unavailable");
                require(snapshot.get("connections").getAsInt()==1 && snapshot.getAsJsonArray("activeRooms").size()==1
                        && snapshot.getAsJsonArray("activeRooms").get(0).getAsString().equals(room),"Node report omitted an admitted room");
                var publicHealth=http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+health.port()+"/health")).GET().build(),bodies);
                require(!publicHealth.body().contains(room) && !publicHealth.body().contains(key),"Public health leaked routing internals");
                require(response.headers().firstValue("Cache-Control").orElse("").equals("no-store"),"Private node reports may be cached");
            }
        }
    }
    private static void verifyRemoteReports(Path dir) throws Exception {
        String key="3".repeat(64); Path config=dir.resolve("monitor-nodes.json");
        try(RelayServer local=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,1,0);
            RelayServer remote=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,0);
            HealthServer localHealth=new HealthServer(local,0,null,key);
            HealthServer remoteHealth=new HealthServer(remote,0,null,key)) {
            local.start(); remote.start(); long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(local.getPort()==0 || remote.getPort()==0) {
                if(System.nanoTime()>deadline)throw new AssertionError("Monitor relay startup"); Thread.sleep(10);
            }
            Files.writeString(config,"{\"nodes\":[{\"id\":\"a\",\"control\":\"ws://127.0.0.1:"+localHealth.port()+"/voice\"},"
                    +"{\"id\":\"b\",\"control\":\"ws://127.0.0.1:"+remoteHealth.port()+"/voice\"}]}");
            RoutingDirectory routing=new RoutingDirectory(config,dir.resolve("monitor-routes.json"),true);
            try(VoiceClient owner=guest(local,"LocalSlot",room(20));
                DirectoryNodeMonitor monitor=new DirectoryNodeMonitor(routing,local,"a",key,false)) {
                owner.connect().get(5,TimeUnit.SECONDS); monitor.refresh();
                require(node(routing.route(room(21))).equals("b"),"Authenticated remote availability was not used");
                try(VoiceClient one=guest(remote,"RemoteSlotA",room(22)); VoiceClient two=guest(remote,"RemoteSlotB",room(22))) {
                    one.connect().get(5,TimeUnit.SECONDS); two.connect().get(5,TimeUnit.SECONDS); monitor.refresh();
                    unavailable(() -> routing.route(room(23)),"Monitor routed a new group into full real relays");
                    require(node(routing.route(room(21))).equals("b"),"Remote full report moved an existing group");
                }
            }
            // A wrong server-only key cannot turn an unauthenticated response into a fresh observation.
            RoutingDirectory wrongKey=new RoutingDirectory(config,dir.resolve("wrong-key-routes.json"),true);
            try(DirectoryNodeMonitor monitor=new DirectoryNodeMonitor(wrongKey,local,"a","4".repeat(64),false)) {
                monitor.refresh();
                // Local node remains healthy, so force it to drain and leave only the unauthorized remote.
                wrongKey.observe("a",new RoutingDirectory.Report(true,1,1,Set.of(),System.currentTimeMillis()));
                unavailable(() -> wrongKey.route(room(24)),"Unauthorized remote node report was accepted");
            }
        }
    }
    private static VoiceClient guest(RelayServer relay,String name,String room) throws Exception {
        UUID id=Wire.offlineUuid(name);
        return new VoiceClient(URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice"),null,
                new VoiceClient.Identity(id,name,id,name,room,"minecraft:overworld"),false,false,
                c -> CompletableFuture.completedFuture(false),new VoiceClient.Listener() {});
    }
    private static void observe(RoutingDirectory r,String id,boolean healthy,int capacity,int connections,Set<String> rooms,AtomicLong now) {
        r.observe(id,new RoutingDirectory.Report(healthy,capacity,connections,rooms,now.get()));
    }
    private static String node(com.google.gson.JsonObject route) { return route.get("node").getAsString(); }
    private static String room(int n) { return String.format("%064x",n); }
    @FunctionalInterface private interface Throwing { void run() throws Exception; }
    private static void unavailable(Throwing action,String message) throws Exception {
        try { action.run(); throw new AssertionError(message); } catch(IOException expected) {}
    }
    private static void require(boolean ok,String message) { if(!ok)throw new AssertionError(message); }
    static void remove(Path dir) throws Exception {
        try(var paths=Files.walk(dir)) { for(Path path:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(path); }
    }
}
