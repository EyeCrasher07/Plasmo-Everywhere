package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.function.IntConsumer;
import java.util.concurrent.*;
import java.util.concurrent.Flow;

/** Trusted HTTPS entry point assigns the same voice node to everyone in a Minecraft server. */
public final class NetworkDirectory {
    public record Probe(URI control, int udpPort) {
        public Probe {
            requireSecure(control, "/voice");
            if (udpPort < 1 || udpPort > 65535) throw new IllegalArgumentException("Probe port");
        }
    }
    public record Node(String id,URI control,int udpPort) {
        public Node {
            if(id==null || !id.matches("[a-z0-9_-]{1,32}"))throw new IllegalArgumentException("Node identifier");
            new Probe(control,udpPort);
        }
    }
    public record Route(URI control, List<Probe> probes,List<Node> nodes) {
        public Route(URI control,List<Probe> probes) { this(control,probes,List.of()); }
        public Route {
            requireSecure(control, "/voice");probes=List.copyOf(probes);nodes=List.copyOf(nodes);
            if(probes.size()>4 || nodes.size()>16 || nodes.stream().map(Node::id).distinct().count()!=nodes.size()
                    || nodes.stream().map(Node::control).distinct().count()!=nodes.size())throw new IllegalArgumentException("Node list");
        }
    }
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(4)).build();
    private static final ScheduledExecutorService HEDGE = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "everywhere-directory"); thread.setDaemon(true); return thread;
    });
    private NetworkDirectory() {}
    public static CompletableFuture<URI> resolve(URI directory, String room) {
        return lookup(directory, room).thenApply(Route::control);
    }
    public static CompletableFuture<Route> lookup(URI directory, String room) {
        return lookup(directory, room, HTTP);
    }
    /** Alternate entry points must share the same authoritative assignments; this never picks a voice node itself. */
    public static CompletableFuture<Route> lookup(List<URI> directories, String room) {
        return lookup(directories, room, new ConnectionTrace());
    }
    public static CompletableFuture<Route> lookup(List<URI> directories, String room, ConnectionTrace trace) {
        java.util.Objects.requireNonNull(trace);
        List<URI> endpoints = validated(directories, room);
        int attempt = trace.attempts();
        long started = System.nanoTime();
        trace.recordForAttempt(attempt, "DIRECTORY", "START", -1, null, endpoints.size());
        if (endpoints.size() == 1) {
            CompletableFuture<Route> single = lookup(endpoints.get(0), room, HTTP, trace, 0, attempt);
            single.whenComplete((route, error) -> trace.recordForAttempt(attempt, "DIRECTORY", single.isCancelled() ? "CANCELLED" : error == null ? "SUCCESS" : "FAILED",
                    elapsed(started), error, 0));
            return single;
        }
        CompletableFuture<Route> result = new CompletableFuture<>();
        Object lock = new Object();
        List<CompletableFuture<Route>> requests = new ArrayList<>(java.util.Collections.nCopies(endpoints.size(), null));
        List<ScheduledFuture<?>> timers = new ArrayList<>();
        int[] failures = {0}; Throwable[] lastError = {null};
        IntConsumer[] begin = new IntConsumer[1];
        begin[0] = index -> {
            synchronized (lock) {
                if (result.isDone() || requests.get(index) != null) return;
                CompletableFuture<Route> request = lookup(endpoints.get(index), room, HTTP, trace, index, attempt); requests.set(index, request);
                request.whenComplete((route, error) -> {
                    synchronized (lock) {
                        if (route != null) {
                            if (result.complete(route)) {
                                timers.forEach(timer -> timer.cancel(false));
                                // A successful primary also warms its secondary's bounded read-through cache.
                                if (index == 0 && requests.get(1) == null) {
                                    CompletableFuture<Route> warm = lookup(endpoints.get(1), room, HTTP, trace, 1, attempt);
                                    requests.set(1, warm); warm.exceptionally(ignored -> null);
                                }
                                for (int n=0; n<requests.size(); n++) {
                                    var pending = requests.get(n);
                                    if (pending != null && n != index && !(index == 0 && n == 1)) pending.cancel(true);
                                }
                            }
                            return;
                        }
                        failures[0]++; lastError[0] = error;
                        if (result.isDone()) return;
                        for (int n=0; n<requests.size(); n++) if (requests.get(n) == null) { begin[0].accept(n); break; }
                        if (failures[0] == endpoints.size()) result.completeExceptionally(lastError[0]);
                    }
                });
            }
        };
        result.whenComplete((route, error) -> {
            trace.recordForAttempt(attempt, "DIRECTORY", result.isCancelled() ? "CANCELLED" : error == null ? "SUCCESS" : "FAILED", elapsed(started), error, -1);
            synchronized (lock) {
                timers.forEach(timer -> timer.cancel(false));
                if (result.isCancelled()) requests.forEach(request -> { if (request != null) request.cancel(true); });
            }
        });
        begin[0].accept(0);
        synchronized (lock) {
            for (int n=1; n<endpoints.size() && !result.isDone(); n++) {
                int index=n; timers.add(HEDGE.schedule(() -> begin[0].accept(index), 1500L*n, TimeUnit.MILLISECONDS));
            }
        }
        return result;
    }
    /** Keeps a bounded secondary lease useful during long sessions; no audio or player identity is sent. */
    public static Warmup keepWarm(List<URI> directories, String room) {
        return new Warmup(validated(directories, room), room);
    }
    public static final class Warmup implements AutoCloseable {
        private final URI secondary;
        private final String room;
        private final ScheduledFuture<?> timer;
        private CompletableFuture<Route> pending;
        private boolean closed;
        private Warmup(List<URI> directories, String room) {
            this.room = room; secondary = directories.size() < 2 ? null : directories.get(1);
            long period = ThreadLocalRandom.current().nextLong(23000, 28001);
            timer = secondary == null ? null : HEDGE.scheduleWithFixedDelay(this::refresh, period, period, TimeUnit.MILLISECONDS);
        }
        private synchronized void refresh() {
            if (closed || pending != null && !pending.isDone()) return;
            pending = lookup(secondary, room);
            pending.exceptionally(ignored -> null);
        }
        @Override public synchronized void close() {
            if (closed) return; closed = true;
            if (timer != null) timer.cancel(false);
            if (pending != null) pending.cancel(true);
        }
    }
    private static List<URI> validated(List<URI> directories, String room) {
        if (directories == null || directories.isEmpty() || directories.size() > 4)
            throw new IllegalArgumentException("Expected one to four directory endpoints");
        if (room == null || !room.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Invalid room");
        for (URI directory : directories) requireSecure(directory, "/route");
        return List.copyOf(new LinkedHashSet<>(directories));
    }
    // Client injection permits real, certificate-verified TLS recovery tests without global TLS changes.
    static CompletableFuture<Route> lookup(URI directory, String room, HttpClient http) {
        return lookup(directory, room, http, new ConnectionTrace(), 0, 0);
    }
    private static CompletableFuture<Route> lookup(URI directory, String room, HttpClient http, ConnectionTrace trace, int index, int attempt) {
        requireSecure(directory, "/route");
        if (!room.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Invalid room");
        long started = System.nanoTime();
        ConnectionTrace.Stage stage = endpointStage(index, "");
        trace.recordForAttempt(attempt, stage, ConnectionTrace.Result.START, -1, null, index);
        var request=HttpRequest.newBuilder(URI.create(directory+"?room="+room)).timeout(Duration.ofSeconds(6))
                .header("Accept","application/json").GET().build();
        TlsFallback fallback = new TlsFallback(http, trace, endpointStage(index, "_TLS"), attempt);
        CompletableFuture<HttpResponse<byte[]>> pending = fallback.get(request, info -> new LimitedBody());
        CompletableFuture<Route> result = new CompletableFuture<>();
        result.whenComplete((route, error) -> trace.recordForAttempt(attempt, stage, result.isCancelled() ? ConnectionTrace.Result.CANCELLED
                        : error == null ? ConnectionTrace.Result.SUCCESS : ConnectionTrace.Result.FAILED,
                elapsed(started), error, index));
        result.whenComplete((route, error) -> { if (result.isCancelled()) { pending.cancel(true); fallback.close(); } });
        pending.whenComplete((response, error) -> {
            try {
                if (error != null) { result.completeExceptionally(error); return; }
                trace.recordForAttempt(attempt, endpointStage(index, "_HTTP"), ConnectionTrace.Result.RECEIVED, elapsed(started), null, response.statusCode());
                result.complete(parse(response));
            } catch (RuntimeException | IOException invalid) { result.completeExceptionally(invalid); }
            finally { fallback.close(); }
        });
        return result;
    }
    private static long elapsed(long started) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started); }
    private static ConnectionTrace.Stage endpointStage(int index, String suffix) {
        return ConnectionTrace.Stage.valueOf("DIRECTORY_" + (index + 1) + suffix);
    }
    private static Route parse(HttpResponse<byte[]> response) throws IOException {
            if(response.statusCode()!=200)throw new IOException("Directory unavailable");
            var j=Wire.parse(new String(response.body(),StandardCharsets.UTF_8));
            if(j.get("protocol").getAsInt()!=1)throw new IllegalArgumentException("Directory protocol");
            if (j.has("validUntil")) {
                var value = j.get("validUntil");
                if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber() || !value.toString().matches("[0-9]{1,16}"))
                    throw new IOException("Invalid directory lease");
                long until = value.getAsLong(), now = System.currentTimeMillis();
                if (until <= now || until-now > TimeUnit.MINUTES.toMillis(5)) throw new IOException("Directory lease expired or invalid");
            }
            URI control=URI.create(Wire.string(j,"control",512)); requireSecure(control,"/voice");
            List<Probe> probes = new ArrayList<>();
            if (j.has("probes")) {
                var list = j.getAsJsonArray("probes");
                if (list.size() > 4) throw new IllegalArgumentException("Probe limit");
                for (var item : list) {
                    var p = item.getAsJsonObject();
                    probes.add(new Probe(URI.create(Wire.string(p,"control",512)), p.get("udpPort").getAsInt()));
                }
            }
            List<Node> nodes=new ArrayList<>();
            if(j.has("federation") && j.get("federation").getAsInt()==1 && j.has("nodes")) {
                var list=j.getAsJsonArray("nodes");if(list.size()>16)throw new IllegalArgumentException("Node limit");
                for(var item:list) {
                    var node=item.getAsJsonObject();nodes.add(new Node(Wire.string(node,"id",32),
                            URI.create(Wire.string(node,"control",512)),node.get("udpPort").getAsInt()));
                }
            }
            return new Route(control, probes,nodes);
    }
    public static void requireSecure(URI uri,String path) {
        String secure=path.equals("/voice")?"wss":"https", local=path.equals("/voice")?"ws":"http";
        if(uri==null || uri.toString().length()>512 || uri.getHost()==null || uri.getUserInfo()!=null || uri.getFragment()!=null || uri.getQuery()!=null
                || uri.getPort() == 0 || uri.getPort() > 65535
                || !path.equals(uri.getPath()) || !(secure.equals(uri.getScheme())
                || (local.equals(uri.getScheme()) && "127.0.0.1".equals(uri.getHost()))))
            throw new IllegalArgumentException("Invalid service endpoint");
    }
    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final CompletableFuture<byte[]> body=new CompletableFuture<>();
        private final ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        private Flow.Subscription subscription;
        public CompletionStage<byte[]> getBody() { return body; }
        public void onSubscribe(Flow.Subscription subscription) { this.subscription=subscription; subscription.request(1); }
        public void onNext(List<ByteBuffer> chunks) {
            for(ByteBuffer chunk:chunks) {
                if(bytes.size()+chunk.remaining()>8192) {
                    subscription.cancel(); body.completeExceptionally(new IOException("Directory response too large")); return;
                }
                byte[] data=new byte[chunk.remaining()]; chunk.get(data); bytes.writeBytes(data);
            }
            subscription.request(1);
        }
        public void onError(Throwable error) { body.completeExceptionally(error); }
        public void onComplete() { body.complete(bytes.toByteArray()); }
    }
}
