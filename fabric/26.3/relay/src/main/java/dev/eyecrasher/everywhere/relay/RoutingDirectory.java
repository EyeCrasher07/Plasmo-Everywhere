package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.LongSupplier;

/** Single-writer persistent room affinity. An unavailable node never silently splits an active room. */
final class RoutingDirectory implements DirectoryRoutes {
    static final int MAX_ROOMS=16384;
    static final long IDLE_MILLIS=24*60*60*1000L, PENDING_MILLIS=5*60*1000L, REPORT_MILLIS=30_000, LEASE_MILLIS=60_000;
    record Node(String id,URI control,int weight,int udpPort,boolean enabled) {}
    record Report(boolean healthy,int capacity,int connections,Set<String> activeRooms,long checkedAt,String network,boolean federationReady,Set<String> federationPeers) {
        Report(boolean healthy,int capacity,int connections,Set<String> activeRooms,long checkedAt) {
            this(healthy,capacity,connections,activeRooms,checkedAt,"",false,Set.of());
        }
        Report {
            if(capacity<1 || capacity>RelayServer.MAX_SESSIONS || connections<0 || connections>capacity || activeRooms.size()>RelayServer.MAX_SESSIONS)
                throw new IllegalArgumentException("Invalid node report");
            activeRooms=Set.copyOf(activeRooms);
            if(activeRooms.stream().anyMatch(room -> !room.matches("[a-f0-9]{64}")))
                throw new IllegalArgumentException("Invalid active room");
            if(network==null || !network.isEmpty() && !network.matches("[a-z0-9_-]{1,64}") || federationReady && network.isEmpty())
                throw new IllegalArgumentException("Invalid federation report");
            federationPeers=Set.copyOf(federationPeers);
            if(federationPeers.size()>=Federation.MAX_NODES || federationPeers.stream().anyMatch(node -> !node.matches("[a-z0-9_-]{1,32}")))
                throw new IllegalArgumentException("Invalid federation peers");
        }
    }
    private record Assignment(String node,long lastSeen,long reservedUntil,boolean observed) {}
    private final Path config,state;
    private final LongSupplier clock;
    private final long idleMillis;
    private final boolean monitored;
    private final Map<String,Assignment> assignments=new HashMap<>();
    private final Map<String,Report> reports=new HashMap<>();
    private final RateLimit allocations=new RateLimit(2,10);
    private Map<String,Node> nodes=Map.of();
    private FileTime modified;
    private long lastPersisted;

    RoutingDirectory(Path config,Path state) throws Exception {
        this(config,state,false,System::currentTimeMillis,IDLE_MILLIS);
    }
    RoutingDirectory(Path config,Path state,boolean monitored) throws Exception {
        this(config,state,monitored,System::currentTimeMillis,IDLE_MILLIS);
    }
    RoutingDirectory(Path config,Path state,boolean monitored,LongSupplier clock,long idleMillis) throws Exception {
        if(idleMillis<1)throw new IllegalArgumentException("Idle interval");
        this.config=config; this.state=state; this.monitored=monitored; this.clock=clock; this.idleMillis=idleMillis;
        lastPersisted=clock.getAsLong();
        reload();
        if(!Files.exists(state))return;
        if(Files.size(state)>4*1024*1024)throw new IOException("Routing state too large");
        JsonObject json=JsonParser.parseString(Files.readString(state)).getAsJsonObject();
        boolean versioned=json.has("version");
        if(versioned && json.get("version").getAsInt()!=2)throw new IOException("Routing state version");
        JsonObject items=versioned?json.getAsJsonObject("assignments"):json;
        if(items.size()>MAX_ROOMS)throw new IOException("Routing capacity");
        long now=clock.getAsLong();
        for(var item:items.entrySet()) {
            if(!item.getKey().matches("[a-f0-9]{64}"))throw new IOException("Invalid routing state");
            String id; long seen,reserved; boolean observed=true;
            if(versioned) {
                JsonObject a=item.getValue().getAsJsonObject(); id=a.get("node").getAsString();
                seen=a.get("lastSeen").getAsLong(); reserved=a.get("reservedUntil").getAsLong();
                // Older state has no proof provenance: retain its established affinity conservatively.
                if(a.has("observed"))observed=a.get("observed").getAsBoolean();
                if(seen<0 || reserved<0 || reserved>now+IDLE_MILLIS)throw new IOException("Invalid routing timestamps");
            } else { id=item.getValue().getAsString(); seen=now; reserved=0; }
            if(!id.matches("[a-z0-9_-]{1,32}"))throw new IOException("Invalid node id");
            assignments.put(item.getKey(),new Assignment(id,seen,reserved,observed));
        }
    }

    synchronized List<Node> nodes() throws Exception { reload(); return List.copyOf(nodes.values()); }
    synchronized void observe(String id,Report report) {
        if(!nodes.containsKey(id))return;
        reports.put(id,report);
        if(!report.healthy)return;
        // Long games do not repeatedly resolve /route. An authenticated live-session report renews inactivity.
        for(String room:report.activeRooms) {
            Assignment a=assignments.get(room);
            if(a!=null && a.node.equals(id))assignments.put(room,new Assignment(a.node,Math.max(a.lastSeen,report.checkedAt),a.reservedUntil,true));
        }
    }
    synchronized int assignmentCount() { return assignments.size(); }
    synchronized int collect() throws Exception {
        reload(); long now=clock.getAsLong();
        // A missing heartbeat is not proof that its players left. Retain rooms until all nodes can be checked.
        if(monitored && !allReportsFresh(now))return 0;
        Map<String,Assignment> removed=new HashMap<>();
        for(var entry:assignments.entrySet()) {
            Assignment a=entry.getValue();
            if(now-a.lastSeen<inactiveMillis(a) || now<a.reservedUntil || active(entry.getKey(),now))continue;
            // Explicitly retire a node with enabled=false before removing it from configuration.
            // Without a report from a removed node we cannot establish whether its rooms are still active.
            if(monitored && !nodes.containsKey(a.node))continue;
            removed.put(entry.getKey(),a);
        }
        if(removed.isEmpty()) {
            // Bound persistence of renewed activity so a restart cannot age a long game by hours.
            if(now-lastPersisted>=5*60*1000L)persist();
            return 0;
        }
        removed.keySet().forEach(assignments::remove);
        try { persist(); }
        catch(IOException failure) { assignments.putAll(removed); throw failure; }
        return removed.size();
    }
    @Override public synchronized JsonObject route(String room) throws Exception {
        if(!room.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Invalid room");
        reload(); long now=clock.getAsLong(); Assignment previous=assignments.get(room);
        Assignment original=previous;
        Node node=previous==null?null:nodes.get(previous.node);
        if(previous!=null && node==null) {
            // Configuration removal is not a safe failover signal while a room may still be active.
            throw new IOException("Assigned node was removed; retire it before removing configuration");
        }
        if(previous!=null && monitored && now-previous.lastSeen>=inactiveMillis(previous)
                && allReportsFresh(now) && !active(room,now)) {
            // Public lookups cannot keep an unused reservation alive indefinitely. Honor the
            // last durable lease before safely reallocating; only node reports prove activity.
            if(now<previous.reservedUntil)return candidates(response(node,nodes.values(),previous.reservedUntil),node,now);
            previous=null; node=null;
        }
        if(node==null) {
            if(assignments.size()>=MAX_ROOMS)collect();
            if(assignments.size()>=MAX_ROOMS || !allocations.take())throw new IOException("Routing capacity");
            node=choose(room,now);
        }
        // A cached secondary may serve the assignment only within this durable reservation.
        // Never relocate or collect the room before the advertised reservation expires.
        long reserved=Math.max(previous==null?0:previous.reservedUntil,now+LEASE_MILLIS);
        long seen=previous==null || !monitored?now:previous.lastSeen;
        boolean observed=previous==null?!monitored:previous.observed;
        Assignment next=new Assignment(node.id,seen,reserved,observed);
        boolean save=previous==null || previous.reservedUntil-now<LEASE_MILLIS/2;
        assignments.put(room,next);
        if(save) {
            try { persist(); }
            catch(Exception e) { if(original==null)assignments.remove(room); else assignments.put(room,original); throw e; }
        } else {
            // Only advertise what is already durable if the process restarts immediately after this response.
            next=new Assignment(node.id,seen,previous.reservedUntil,observed); assignments.put(room,next);
        }
        return candidates(response(node,nodes.values(),next.reservedUntil),node,now);
    }
    private JsonObject candidates(JsonObject response,Node assigned,long now) {
        Report home=reports.get(assigned.id);
        // Legacy clients retain room affinity. New clients may choose only peers of the same ready mesh.
        if(!monitored || home==null || home.network.isEmpty())return response;
        if(nodes.values().stream().anyMatch(node -> {
            Report report=reports.get(node.id);
            return node.enabled && fresh(report,now) && report.healthy && !home.network.equals(report.network);
        }))return response;
        Set<String> required=new HashSet<>();
        nodes.values().forEach(node -> {
            Report report=reports.get(node.id);
            if(node.enabled && fresh(report,now) && report.healthy && home.network.equals(report.network))required.add(node.id);
        });
        JsonArray choices=new JsonArray();
        nodes.values().stream().sorted(Comparator.comparing(Node::id)).forEach(node -> {
            Report report=reports.get(node.id);
            if(!node.enabled || !fresh(report,now) || !report.healthy
                    || !home.network.equals(report.network) || report.connections>=report.capacity)return;
            if(required.stream().anyMatch(id -> !id.equals(node.id) && !report.federationPeers.contains(id)))return;
            JsonObject value=new JsonObject();value.addProperty("id",node.id);value.addProperty("control",node.control.toString());
            value.addProperty("udpPort",node.udpPort);choices.add(value);
        });
        if(!choices.isEmpty()) { response.addProperty("federation",1);response.add("nodes",choices); }
        return response;
    }
    static JsonObject response(Node node,Collection<Node> nodes,long validUntil) {
        JsonObject j=new JsonObject(); j.addProperty("protocol",1); j.addProperty("node",node.id);
        j.addProperty("control",node.control.toString()); j.addProperty("validUntil",validUntil);
        JsonArray probes=new JsonArray(); List<Node> sources=new ArrayList<>(); sources.add(node);
        nodes.stream().sorted(Comparator.comparing(Node::id)).filter(n -> !n.id.equals(node.id)).forEach(sources::add);
        Set<String> hosts=new HashSet<>();
        for(Node source:sources) {
            if(probes.size()>=4)break;
            if(!hosts.add(source.control.getHost()))continue;
            JsonObject p=new JsonObject(); p.addProperty("control",source.control.toString());
            p.addProperty("udpPort",source.udpPort); probes.add(p);
        }
        j.add("probes",probes); return j;
    }
    private Node choose(String room,long now) throws Exception {
        Node best=null; double bestScore=Double.POSITIVE_INFINITY;
        for(Node node:nodes.values()) {
            Report report=reports.get(node.id);
            if(!node.enabled || (monitored && (!fresh(report,now) || !report.healthy || report.connections>=report.capacity)))continue;
            byte[] digest=MessageDigest.getInstance("SHA-256").digest((room+"/"+node.id).getBytes(StandardCharsets.UTF_8));
            double uniform=((ByteBuffer.wrap(digest).getLong()>>>11)+1.0)/9007199254740993.0;
            double score=-Math.log(uniform)/node.weight;
            if(score<bestScore || (score==bestScore && (best==null || node.id.compareTo(best.id)<0))) {
                best=node; bestScore=score;
            }
        }
        if(best==null)throw new IOException("No available voice node");
        return best;
    }
    private boolean fresh(Report report,long now) { return report!=null && report.checkedAt<=now && now-report.checkedAt<=REPORT_MILLIS; }
    private long inactiveMillis(Assignment assignment) {
        return monitored && !assignment.observed?Math.min(idleMillis,PENDING_MILLIS):idleMillis;
    }
    private boolean allReportsFresh(long now) { return nodes.keySet().stream().allMatch(id -> fresh(reports.get(id),now) && reports.get(id).healthy); }
    private boolean active(String room,long now) {
        return reports.values().stream().anyMatch(r -> fresh(r,now) && r.activeRooms.contains(room));
    }
    private void reload() throws Exception {
        FileTime current=Files.getLastModifiedTime(config);
        if(current.equals(modified))return;
        try {
            if(Files.size(config)>65536)throw new IOException("Node config too large");
            var list=Wire.parse(Files.readString(config)).getAsJsonArray("nodes");
            if(list.isEmpty() || list.size()>16)throw new IOException("Expected 1 to 16 nodes");
            Map<String,Node> next=new TreeMap<>();
            for(var item:list) {
                var j=item.getAsJsonObject(); String id=Wire.string(j,"id",32);
                if(!id.matches("[a-z0-9_-]{1,32}"))throw new IOException("Invalid node id");
                URI uri=URI.create(Wire.string(j,"control",512)); NetworkDirectory.requireSecure(uri,"/voice");
                int weight=j.has("weight")?j.get("weight").getAsInt():1;
                int udpPort=j.has("udpPort")?j.get("udpPort").getAsInt():24454;
                boolean enabled=!j.has("enabled") || j.get("enabled").getAsBoolean();
                if(weight<1 || weight>100 || udpPort<1 || udpPort>65535
                        || next.put(id,new Node(id,uri,weight,udpPort,enabled))!=null)
                    throw new IOException("Invalid node weight/id/port");
            }
            nodes=Map.copyOf(next); reports.keySet().retainAll(nodes.keySet()); modified=current;
        } catch(Exception e) {
            if(nodes.isEmpty())throw e;
            modified=current; System.err.println("Invalid node configuration; retaining previous node list");
        }
    }
    private void persist() throws IOException {
        Path parent=state.toAbsolutePath().getParent(); Files.createDirectories(parent);
        Path tmp=Files.createTempFile(parent,"routes-",".tmp");
        try {
            JsonObject root=new JsonObject(); root.addProperty("version",2); JsonObject items=new JsonObject();
            for(var entry:assignments.entrySet()) {
                Assignment a=entry.getValue(); JsonObject item=new JsonObject(); item.addProperty("node",a.node);
                item.addProperty("lastSeen",a.lastSeen); item.addProperty("reservedUntil",a.reservedUntil);
                item.addProperty("observed",a.observed);
                items.add(entry.getKey(),item);
            }
            root.add("assignments",items); Files.writeString(tmp,Wire.GSON.toJson(root));
            Files.move(tmp,state,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            lastPersisted=clock.getAsLong();
        } finally { Files.deleteIfExists(tmp); }
    }
}
