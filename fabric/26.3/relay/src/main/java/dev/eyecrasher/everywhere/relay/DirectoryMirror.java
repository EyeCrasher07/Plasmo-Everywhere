package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import java.util.function.LongSupplier;

/** Read-through secondary. It can repeat a reserved assignment, never invent or migrate one. */
final class DirectoryMirror implements DirectoryRoutes {
    private static final int MAX_CACHE=1024, MAX_BODY=8192;
    static final long SAFETY_MILLIS=5000;
    private static final long REFRESH_MILLIS=30_000;
    private static final int MAX_IN_FLIGHT=16;
    private final URI upstream;
    private final Path state;
    private final LongSupplier clock;
    private final HttpClient http;
    private final String directoryKey;
    private final LinkedHashMap<String,JsonObject> cache=new LinkedHashMap<>(16,0.75f,true);
    private final Map<String,Pending> pending=new HashMap<>();
    private final Map<String,Long> retryAfter=new HashMap<>();
    private final ScheduledThreadPoolExecutor deadlines=new ScheduledThreadPoolExecutor(1,task -> {
        Thread thread=new Thread(task,"everywhere-mirror-deadlines"); thread.setDaemon(true); return thread;
    });
    private boolean closed;
    private static final class Pending {
        final CompletableFuture<JsonObject> result=new CompletableFuture<>();
        CompletableFuture<HttpResponse<byte[]>> request;
        ScheduledFuture<?> deadline;
    }

    DirectoryMirror(URI upstream,Path state) throws Exception {
        this(upstream,state,null);
    }
    DirectoryMirror(URI upstream,Path state,String directoryKey) throws Exception {
        this(upstream,state,System::currentTimeMillis,HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
                .followRedirects(HttpClient.Redirect.NEVER).build(),directoryKey);
    }
    DirectoryMirror(URI upstream,Path state,LongSupplier clock,HttpClient http) throws Exception {
        this(upstream,state,clock,http,null);
    }
    DirectoryMirror(URI upstream,Path state,LongSupplier clock,HttpClient http,String directoryKey) throws Exception {
        if(directoryKey!=null && !directoryKey.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Directory key");
        NetworkDirectory.requireSecure(upstream,"/route"); this.upstream=upstream; this.state=state; this.clock=clock; this.http=http;
        this.directoryKey=directoryKey;
        deadlines.setRemoveOnCancelPolicy(true);
        if(!Files.exists(state))return;
        if(Files.size(state)>4*1024*1024)throw new IOException("Directory mirror state too large");
        JsonObject saved=JsonParser.parseString(Files.readString(state)).getAsJsonObject();
        if(saved.size()>MAX_CACHE)throw new IOException("Directory mirror capacity");
        for(var item:saved.entrySet()) {
            if(!item.getKey().matches("[a-f0-9]{64}"))throw new IOException("Invalid mirrored room");
            try {
                JsonObject route=validate(item.getValue().getAsJsonObject());
                if(fresh(route))cache.put(item.getKey(),route);
            } catch(IOException expired) { /* Expired reservations are deliberately not restored. */ }
        }
    }
    @Override public JsonObject route(String room) throws Exception {
        if(!room.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Invalid room");
        Pending operation; boolean start=false;
        synchronized(this) {
            if(closed)throw new IOException("Directory mirror closed");
            JsonObject saved=cache.get(room);
            // One shared authoritative reservation serves the whole room, including bursts of newcomers.
            if(saved!=null && fresh(saved) && (saved.get("validUntil").getAsLong()-clock.getAsLong()>REFRESH_MILLIS
                    || clock.getAsLong()<retryAfter.getOrDefault(room,0L)))
                return saved.deepCopy();
            operation=pending.get(room);
            if(operation==null) {
                if(pending.size()>=MAX_IN_FLIGHT) {
                    if(saved!=null && fresh(saved))return saved.deepCopy();
                    throw new IOException("Directory mirror busy");
                }
                operation=new Pending(); pending.put(room,operation); start=true;
            }
        }
        if(start)fetch(room,operation);
        try {
            return operation.result.get(4,TimeUnit.SECONDS).deepCopy();
        } catch(InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw interrupted;
        } catch(ExecutionException|TimeoutException unavailable) {
            throw new IOException("Primary directory unavailable and no current reserved route");
        }
    }
    private void fetch(String room,Pending operation) {
        try {
            HttpRequest.Builder request=HttpRequest.newBuilder(URI.create(upstream+"?room="+room)).timeout(Duration.ofSeconds(3))
                    .header("Accept","application/json").GET();
            if(directoryKey!=null)request.header("Authorization","Bearer "+directoryKey);
            synchronized(this) {
                if(closed) { finish(room,operation,null); return; }
                operation.request=http.sendAsync(request.build(),info -> new BoundedBody(MAX_BODY));
                // Bound the complete body as well as connection/response headers.
                operation.deadline=deadlines.schedule(() -> operation.request.cancel(true),3500,TimeUnit.MILLISECONDS);
            }
            operation.request.whenComplete((received,error) -> {
                JsonObject route=null;
                if(error==null && received.statusCode()==200)try {
                    route=validate(Wire.parse(new String(received.body(),StandardCharsets.UTF_8)));
                } catch(IOException|RuntimeException invalid) { /* A malformed upstream never replaces a valid cache. */ }
                finish(room,operation,route);
            });
        } catch(RuntimeException unavailable) { finish(room,operation,null); }
    }
    private synchronized void finish(String room,Pending operation,JsonObject route) {
        pending.remove(room,operation);
        if(operation.deadline!=null)operation.deadline.cancel(false);
        if(closed) { operation.result.completeExceptionally(new IOException("Directory mirror closed")); return; }
        if(route!=null) {
            cache.entrySet().removeIf(entry -> !fresh(entry.getValue())); JsonObject old=cache.put(room,route);
            while(cache.size()>MAX_CACHE)cache.remove(cache.keySet().iterator().next());
            retryAfter.keySet().retainAll(cache.keySet()); retryAfter.remove(room);
            try { if(!route.equals(old))persist(); } catch(IOException unavailable) { /* The primary's durable reservation remains valid in memory. */ }
        }
        JsonObject saved=cache.get(room);
        if(saved!=null && fresh(saved)) {
            if(route==null)retryAfter.put(room,clock.getAsLong()+5000);
            operation.result.complete(saved);
        }
        else operation.result.completeExceptionally(new IOException("No current authoritative route"));
    }
    private boolean fresh(JsonObject route) {
        long now=clock.getAsLong(),until=route.get("validUntil").getAsLong();
        return now+SAFETY_MILLIS<until && until<=now+RoutingDirectory.LEASE_MILLIS+SAFETY_MILLIS;
    }
    private JsonObject validate(JsonObject route) throws IOException {
        try {
            if(route.get("protocol").getAsInt()!=1 || !Wire.string(route,"node",32).matches("[a-z0-9_-]{1,32}"))
                throw new IOException("Directory protocol");
            URI control=URI.create(Wire.string(route,"control",512)); NetworkDirectory.requireSecure(control,"/voice");
            List<RoutingDirectory.Node> nodes=new ArrayList<>();
            if(route.has("probes")) {
                JsonArray probes=route.getAsJsonArray("probes");
                if(probes.size()>4)throw new IOException("Directory probe limit");
                for(JsonElement item:probes) {
                    JsonObject p=item.getAsJsonObject(); URI uri=URI.create(Wire.string(p,"control",512));
                    NetworkDirectory.requireSecure(uri,"/voice"); int port=p.get("udpPort").getAsInt();
                    if(port<1 || port>65535)throw new IOException("Directory probe port");
                    nodes.add(new RoutingDirectory.Node("probe-"+nodes.size(),uri,1,port,true));
                }
            }
            long until=route.get("validUntil").getAsLong();
            JsonObject clean=new JsonObject(); clean.addProperty("protocol",1); clean.addProperty("node",route.get("node").getAsString());
            clean.addProperty("control",control.toString()); clean.addProperty("validUntil",until); JsonArray probes=new JsonArray();
            for(var node:nodes) { JsonObject p=new JsonObject(); p.addProperty("control",node.control().toString()); p.addProperty("udpPort",node.udpPort()); probes.add(p); }
            clean.add("probes",probes);
            if(route.has("federation") && route.get("federation").getAsInt()==1 && route.has("nodes")) {
                JsonArray choices=route.getAsJsonArray("nodes");if(choices.size()>16)throw new IOException("Directory node limit");
                JsonArray validated=new JsonArray();Set<String> ids=new HashSet<>();Set<URI> urls=new HashSet<>();
                for(JsonElement choice:choices) {
                    JsonObject item=choice.getAsJsonObject();NetworkDirectory.Node value=new NetworkDirectory.Node(Wire.string(item,"id",32),
                            URI.create(Wire.string(item,"control",512)),item.get("udpPort").getAsInt());
                    if(!ids.add(value.id()) || !urls.add(value.control()))throw new IOException("Duplicate directory node");
                    JsonObject output=new JsonObject();output.addProperty("id",value.id());output.addProperty("control",value.control().toString());
                    output.addProperty("udpPort",value.udpPort());validated.add(output);
                }
                clean.addProperty("federation",1);clean.add("nodes",validated);
            }
            if(!fresh(clean))throw new IOException("Directory reservation expired");
            return clean;
        } catch(IOException invalid) { throw invalid; }
        catch(RuntimeException invalid) { throw new IOException("Invalid directory response"); }
    }
    private void persist() throws IOException {
        Path parent=state.toAbsolutePath().getParent(); Files.createDirectories(parent); Path tmp=Files.createTempFile(parent,"mirror-",".tmp");
        try {
            Files.writeString(tmp,Wire.GSON.toJson(cache));
            Files.move(tmp,state,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(tmp); }
    }
    @Override public synchronized void close() {
        if(closed)return; closed=true;
        for(Pending operation:List.copyOf(pending.values())) {
            if(operation.request!=null)operation.request.cancel(true);
            operation.result.completeExceptionally(new IOException("Directory mirror closed"));
        }
        pending.clear(); deadlines.shutdownNow(); http.shutdownNow();
    }

    static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final int maximum;
        private final CompletableFuture<byte[]> result=new CompletableFuture<>();
        private final ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        private Flow.Subscription subscription;
        BoundedBody(int maximum) { this.maximum=maximum; }
        public CompletionStage<byte[]> getBody() { return result; }
        public void onSubscribe(Flow.Subscription value) { subscription=value; value.request(1); }
        public void onNext(List<ByteBuffer> chunks) {
            for(ByteBuffer chunk:chunks) {
                if(bytes.size()+chunk.remaining()>maximum) {
                    subscription.cancel(); result.completeExceptionally(new IOException("Server response too large")); return;
                }
                byte[] value=new byte[chunk.remaining()]; chunk.get(value); bytes.writeBytes(value);
            }
            subscription.request(1);
        }
        public void onError(Throwable error) { result.completeExceptionally(error); }
        public void onComplete() { result.complete(bytes.toByteArray()); }
    }
}
