package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.Wire;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Bounded authenticated node reports, separate from public health and voice traffic. */
final class DirectoryNodeMonitor implements AutoCloseable {
    static final int MAX_REPORT_BYTES=512*1024;
    private final RoutingDirectory directory;
    private final RelayServer relay;
    private final String localId,key;
    private final Set<CompletableFuture<?>> inFlight=ConcurrentHashMap.newKeySet();
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final ScheduledExecutorService worker=Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread=new Thread(r,"everywhere-directory-nodes"); thread.setDaemon(true); return thread;
    });
    DirectoryNodeMonitor(RoutingDirectory directory,RelayServer relay,String localId,String key) {
        this(directory,relay,localId,key,true);
    }
    DirectoryNodeMonitor(RoutingDirectory directory,RelayServer relay,String localId,String key,boolean start) {
        if(!localId.matches("[a-z0-9_-]{1,32}") || !key.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Directory node credentials");
        this.directory=directory; this.relay=relay; this.localId=localId; this.key=key;
        if(start)worker.scheduleWithFixedDelay(this::refresh,0,5,TimeUnit.SECONDS);
    }
    void refresh() {
        List<CompletableFuture<HttpResponse<byte[]>>> requests=new ArrayList<>();
        try {
            List<CompletableFuture<Void>> remote=new ArrayList<>();
            for(var node:directory.nodes()) {
                if(node.id().equals(localId)) {
                    directory.observe(node.id(),parse(snapshot(relay),System.currentTimeMillis())); continue;
                }
                URI control=node.control(); String scheme="wss".equals(control.getScheme())?"https":"http";
                URI uri=new URI(scheme,null,control.getHost(),control.getPort(),"/directory-node",null,null);
                HttpRequest request=HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3))
                        .header("Authorization","Bearer "+key).header("Accept","application/json").GET().build();
                CompletableFuture<HttpResponse<byte[]>> raw=http.sendAsync(request,info -> new DirectoryMirror.BoundedBody(MAX_REPORT_BYTES));
                requests.add(raw); inFlight.add(raw); raw.whenComplete((response,error) -> inFlight.remove(raw));
                remote.add(raw.thenAccept(response -> {
                    if(response.statusCode()!=200)return;
                    try {
                        var report=parseBody(response.body());
                        directory.observe(node.id(),parse(report,System.currentTimeMillis()));
                    } catch(RuntimeException invalid) { /* No fresh observation; the old one expires. */ }
                }).exceptionally(error -> null));
            }
            CompletableFuture.allOf(remote.toArray(CompletableFuture[]::new)).get(4,TimeUnit.SECONDS);
            directory.collect();
        } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        catch(Exception unavailable) { /* Fail closed for new groups after bounded report freshness. */ }
        finally { for(var request:requests)if(!request.isDone())request.cancel(true); }
    }
    static JsonObject snapshot(RelayServer relay) {
        synchronized(relay) {
            JsonObject health=relay.health(); JsonObject report=new JsonObject();
            report.addProperty("status",health.get("status").getAsString()); report.addProperty("capacity",health.get("capacity").getAsInt());
            report.addProperty("connections",health.get("connections").getAsInt()); JsonArray rooms=new JsonArray();
            relay.activeRooms().stream().sorted().forEach(rooms::add); report.add("activeRooms",rooms); return report;
        }
    }
    static JsonObject parseBody(byte[] bytes) {
        if(bytes.length>MAX_REPORT_BYTES)throw new IllegalArgumentException("Node report size");
        // Server-authenticated monitoring has a separate bound from player control messages.
        return JsonParser.parseString(new String(bytes,StandardCharsets.UTF_8)).getAsJsonObject();
    }
    static RoutingDirectory.Report parse(JsonObject value,long now) {
        JsonArray list=value.getAsJsonArray("activeRooms"); if(list.size()>RelayServer.MAX_SESSIONS)throw new IllegalArgumentException("Node rooms");
        Set<String> rooms=new HashSet<>(); for(JsonElement room:list)rooms.add(room.getAsString());
        return new RoutingDirectory.Report("ok".equals(value.get("status").getAsString()),value.get("capacity").getAsInt(),
                value.get("connections").getAsInt(),rooms,now);
    }
    @Override public void close() { worker.shutdownNow(); inFlight.forEach(request -> request.cancel(true)); http.shutdownNow(); }
}
