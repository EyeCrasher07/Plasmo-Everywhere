package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Public lookups prove demand, not an admitted voice session or an indefinite room lease. */
public final class DirectoryAbuseTest {
    private static final String ROOM="d".repeat(64),ACTIVE="c".repeat(64);
    public static void main(String[] args) throws Exception {
        Path dir=Files.createTempDirectory("everywhere-directory-abuse-");
        try {
            Path config=dir.resolve("nodes.json"),state=dir.resolve("routes.json");
            Files.writeString(config,"{\"nodes\":[{\"id\":\"a\",\"control\":\"wss://a.example/voice\"}]}");
            AtomicLong now=new AtomicLong(1_000_000);
            RoutingDirectory routing=new RoutingDirectory(config,state,true,now::get,RoutingDirectory.IDLE_MILLIS);
            observe(routing,now,Set.of()); routing.route(ROOM); long created=now.get();
            for(int n=1;n<=9;n++) {
                now.set(created+n*30_000); observe(routing,now,Set.of()); routing.route(ROOM);
            }
            now.set(created+RoutingDirectory.PENDING_MILLIS-1); observe(routing,now,Set.of());
            long lastLease=routing.route(ROOM).get("validUntil").getAsLong();
            JsonObject item=saved(state,ROOM);
            require(!item.get("observed").getAsBoolean() && item.get("lastSeen").getAsLong()==created,
                    "Public lookups forged session activity");
            now.addAndGet(2); observe(routing,now,Set.of());
            require(routing.route(ROOM).get("validUntil").getAsLong()==lastLease,"Expired pending lookup extended a durable lease");
            require(routing.collect()==0,"Pending cleanup violated an advertised mirror reservation");
            RoutingDirectory restored=new RoutingDirectory(config,state,true,now::get,RoutingDirectory.IDLE_MILLIS);
            observe(restored,now,Set.of());
            require(restored.route(ROOM).get("validUntil").getAsLong()==lastLease,"Restart lost pending provenance or lease");
            now.set(lastLease+RoutingDirectory.REPORT_MILLIS+1); // Missing reports are not proof that a room never connected.
            require(restored.collect()==0 && restored.assignmentCount()==1,"Stale node reports permitted unsafe room retirement");
            observe(restored,now,Set.of());
            require(restored.collect()==1 && restored.assignmentCount()==0,"Never-admitted room retained a 24-hour slot");

            // Only an authenticated node observation promotes a pending assignment to normal retention.
            restored.route(ACTIVE); long admitted=now.get(); observe(restored,now,Set.of(ACTIVE));
            now.addAndGet(RoutingDirectory.PENDING_MILLIS+1); observe(restored,now,Set.of(ACTIVE));
            long ended=now.get(); restored.route(ACTIVE);
            require(saved(state,ACTIVE).get("observed").getAsBoolean(),"Node activity did not promote pending room");
            now.addAndGet(1); observe(restored,now,Set.of());
            require(restored.collect()==0,"Recently admitted room received pending-room retention");
            now.set(ended+RoutingDirectory.IDLE_MILLIS-1); observe(restored,now,Set.of());
            require(restored.collect()==0,"Active room retired before normal retention");
            now.addAndGet(2); observe(restored,now,Set.of());
            require(restored.collect()==1,"Admitted but ended room did not expire");

            // Safe retirement in the /route path also uses a new creation time and allocation budget.
            restored.route(ROOM); long firstCreation=now.get();
            now.addAndGet(RoutingDirectory.PENDING_MILLIS+1); observe(restored,now,Set.of());
            restored.route(ROOM);
            require(saved(state,ROOM).get("lastSeen").getAsLong()>firstCreation,"Expired public reservation renewed without reallocation");
            // Legacy v2 state without provenance retains its existing affinity and normal lifetime.
            JsonObject old=new JsonObject(); old.addProperty("version",2); JsonObject assignments=new JsonObject();
            JsonObject legacy=new JsonObject(); legacy.addProperty("node","a"); legacy.addProperty("lastSeen",now.get());
            legacy.addProperty("reservedUntil",0); assignments.add(ACTIVE,legacy); old.add("assignments",assignments);
            Files.writeString(state,old.toString());
            RoutingDirectory compatible=new RoutingDirectory(config,state,true,now::get,RoutingDirectory.IDLE_MILLIS);
            now.addAndGet(RoutingDirectory.PENDING_MILLIS+1); observe(compatible,now,Set.of());
            require(compatible.collect()==0 && compatible.route(ACTIVE).get("node").getAsString().equals("a"),
                    "State-v2 compatibility discarded an established affinity");
            System.out.println("Directory abuse: unauthenticated lookup expiry, nonrenewable pending leases, observed activity, outage/restart and v2 compatibility passed");
        } finally { DirectoryMaintenanceTest.remove(dir); }
    }
    private static JsonObject saved(Path state,String room) throws Exception {
        return JsonParser.parseString(Files.readString(state)).getAsJsonObject().getAsJsonObject("assignments").getAsJsonObject(room);
    }
    private static void observe(RoutingDirectory routing,AtomicLong now,Set<String> active) {
        routing.observe("a",new RoutingDirectory.Report(true,1152,active.size(),active,now.get()));
    }
    private static void require(boolean condition,String reason) { if(!condition)throw new AssertionError(reason); }
}
