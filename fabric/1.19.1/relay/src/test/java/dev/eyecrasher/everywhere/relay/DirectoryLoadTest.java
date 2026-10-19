package dev.eyecrasher.everywhere.relay;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpServer;
import dev.eyecrasher.everywhere.core.Wire;
import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Actual concurrent HTTP stalls must not become a voice-health outage or one query per player. */
public final class DirectoryLoadTest {
    public static void main(String[] ignored) throws Exception {
        Path dir=Files.createTempDirectory("everywhere-directory-load-");
        try {
            verifyCoalescing(dir);
            verifyHealthIsolation(dir);
            verifyAuthenticatedBudget();
            verifyClose(dir);
            verifyCompleteBodyDeadline(dir);
            verifyFetchBound(dir);
            verifyMonitorBodyDeadline(dir);
            System.out.println("Directory load: 100 shared-room callers coalesce, bounded admission, independent health/discovery and close cancellation passed");
        } finally { DirectoryMaintenanceTest.remove(dir); }
    }
    private static void verifyCoalescing(Path dir) throws Exception {
        AtomicLong now=new AtomicLong(3_000_000); String room="1".repeat(64),key="5".repeat(64);
        try(Origin origin=new Origin(now); DirectoryMirror mirror=new DirectoryMirror(origin.uri(),dir.resolve("coalesce.json"),
                now::get,HttpClient.newHttpClient(),key); ExecutorService callers=Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch start=new CountDownLatch(1); List<Future<JsonObject>> requests=new ArrayList<>();
            for(int n=0;n<100;n++)requests.add(callers.submit(() -> { start.await(); return mirror.route(room); }));
            start.countDown(); await(() -> origin.hits.get()==1,"First shared origin query"); origin.release.countDown();
            for(var request:requests)require(request.get(4,TimeUnit.SECONDS).get("node").getAsString().equals("a"),"Concurrent callers disagreed on route");
            require(origin.hits.get()==1,"100 same-room callers created duplicate upstream queries");
            require(origin.authorization.get().equals("Bearer "+key),"Mirror omitted its server-only proof");
            for(int n=0;n<100;n++)mirror.route(room);
            require(origin.hits.get()==1,"Fresh shared reservation was fetched once per player");
            now.addAndGet(31_000); JsonObject renewed=mirror.route(room);
            require(origin.hits.get()==2 && renewed.get("validUntil").getAsLong()==now.get()+60_000,"Shared reservation did not renew near expiry");
            origin.online.set(false); now.addAndGet(31_000); long reserved=renewed.get("validUntil").getAsLong();
            for(int n=0;n<100;n++)require(mirror.route(room).get("validUntil").getAsLong()==reserved,"Outage extended an authoritative reservation");
            require(origin.hits.get()==3,"Fast primary errors caused a same-room retry storm");
            now.set(reserved-DirectoryMirror.SAFETY_MILLIS);
            try { mirror.route(room); throw new AssertionError("Retry backoff served an expired lease"); } catch(IOException expected) {}
        }
    }
    private static void verifyHealthIsolation(Path dir) throws Exception {
        String key="6".repeat(64); AtomicLong now=new AtomicLong(System.currentTimeMillis());
        try(Origin origin=new Origin(now);
            DirectoryMirror mirror=new DirectoryMirror(origin.uri(),dir.resolve("health.json"));
            RelayServer relay=new RelayServer("127.0.0.1",0,0,null);
            HealthServer health=new HealthServer(relay,0,mirror,key);
            HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()) {
            relay.start(); await(() -> relay.getPort()>0,"Health isolation relay start");
            URI base=URI.create("http://127.0.0.1:"+health.port()); List<CompletableFuture<HttpResponse<String>>> stalled=new ArrayList<>();
            for(int n=0;n<4;n++)stalled.add(http.sendAsync(routeRequest(base,n),HttpResponse.BodyHandlers.ofString()));
            await(() -> origin.hits.get()==4,"Four stalled directory requests");
            for(int n=4;n<28;n++)stalled.add(http.sendAsync(routeRequest(base,n),HttpResponse.BodyHandlers.ofString()));
            var body=HttpResponse.BodyHandlers.ofString();
            require(http.send(HttpRequest.newBuilder(base.resolve("/health")).timeout(Duration.ofSeconds(1)).GET().build(),body).statusCode()==200,
                    "Stalled origin blocked public relay health");
            require(http.send(HttpRequest.newBuilder(base.resolve("/directory-node")).timeout(Duration.ofSeconds(1))
                    .header("Authorization","Bearer "+key).GET().build(),body).statusCode()==200,"Stalled origin blocked private node snapshot");
            require(http.send(HttpRequest.newBuilder(base.resolve("/tcp-discovery")).timeout(Duration.ofSeconds(1))
                    .header("X-Real-IP","8.8.4.4").header("X-Real-Port","45000").GET().build(),body).statusCode()==200,
                    "Stalled origin blocked TCP source observation");
            await(() -> stalled.stream().anyMatch(request -> request.isDone() && !request.isCompletedExceptionally()
                    && request.join().statusCode()==503),"Bounded route admission returns 503");
            origin.release.countDown();
            for(var request:stalled) {
                try { int status=request.get(5,TimeUnit.SECONDS).statusCode(); require(status==200 || status==503,"Unexpected overload response"); }
                catch(ExecutionException rejectedConnection) { /* The bounded HTTP admission may close excess connections. */ }
            }
        }
    }
    private static HttpRequest routeRequest(URI base,int n) {
        return HttpRequest.newBuilder(base.resolve("/route?room="+String.format("%064x",n+1))).timeout(Duration.ofSeconds(6))
                .header("X-Real-IP","8.8.8."+(n+1)).GET().build();
    }
    private static void verifyAuthenticatedBudget() throws Exception {
        String key="7".repeat(64); DirectoryRoutes routes=room -> response(System.currentTimeMillis()+60_000);
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null); HealthServer health=new HealthServer(relay,0,routes,key);
            HttpClient http=HttpClient.newHttpClient()) {
            URI endpoint=URI.create("http://127.0.0.1:"+health.port()+"/route?room="+"2".repeat(64)); var body=HttpResponse.BodyHandlers.ofString();
            for(int n=0;n<32;n++)require(http.send(HttpRequest.newBuilder(endpoint).header("Authorization","Bearer "+key)
                    .header("X-Real-IP","8.8.8.8").GET().build(),body).statusCode()==200,"Authenticated mirror hit a player's shared-IP budget");
            int limited=0;
            for(int n=0;n<32;n++)if(http.send(HttpRequest.newBuilder(endpoint).header("Authorization","Bearer "+"8".repeat(64))
                    .header("X-Real-IP","8.8.8.8").GET().build(),body).statusCode()==429)limited++;
            require(limited>0,"An arbitrary client proof bypassed ordinary rate limits");
        }
    }
    private static void verifyCompleteBodyDeadline(Path dir) throws Exception {
        try(Origin origin=new Origin(new AtomicLong(System.currentTimeMillis()));
            DirectoryMirror mirror=new DirectoryMirror(origin.uri(),dir.resolve("body-deadline.json"))) {
            origin.stallBody.set(true); origin.release.countDown(); long start=System.nanoTime();
            try { mirror.route("4".repeat(64)); throw new AssertionError("Stalled complete body succeeded"); } catch(IOException expected) {}
            require(System.nanoTime()-start<TimeUnit.MILLISECONDS.toNanos(4500),"Stalled complete body retained its fetch slot indefinitely");
            origin.stallBody.set(false); origin.bodyRelease.countDown();
            require(mirror.route("4".repeat(64)).get("node").getAsString().equals("a") && origin.hits.get()==2,
                    "Full-body timeout did not free the shared pending request");
        }
    }
    private static void verifyFetchBound(Path dir) throws Exception {
        try(Origin origin=new Origin(new AtomicLong(System.currentTimeMillis()));
            DirectoryMirror mirror=new DirectoryMirror(origin.uri(),dir.resolve("fetch-bound.json"));
            ExecutorService callers=Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> requests=new ArrayList<>();
            for(int n=0;n<20;n++) {
                String room=String.format("%064x",n+100);
                requests.add(callers.submit(() -> { try { mirror.route(room); return true; } catch(IOException busy) { return false; } }));
            }
            await(() -> origin.hits.get()==16 && requests.stream().filter(Future::isDone).count()>=4,"Bounded origin fan-out");
            origin.release.countDown(); int successes=0;
            for(var request:requests)if(request.get(4,TimeUnit.SECONDS))successes++;
            require(successes==16 && origin.hits.get()==16,"Mirror exceeded its 16 in-flight origin budget");
        }
    }
    private static void verifyMonitorBodyDeadline(Path dir) throws Exception {
        CountDownLatch release=new CountDownLatch(1); AtomicInteger active=new AtomicInteger(),hits=new AtomicInteger();
        HttpServer origin=HttpServer.create(new InetSocketAddress("127.0.0.1",0),16);
        ExecutorService handlers=Executors.newVirtualThreadPerTaskExecutor(); origin.setExecutor(handlers);
        origin.createContext("/directory-node",exchange -> {
            try {
                active.incrementAndGet(); hits.incrementAndGet();
                exchange.sendResponseHeaders(200,65536); exchange.getResponseBody().write('{'); exchange.getResponseBody().flush();
                // Slow incomplete body, so cancellation must close the actual connection, not merely a dependent future.
                while(!release.await(100,TimeUnit.MILLISECONDS)) { exchange.getResponseBody().write(' '); exchange.getResponseBody().flush(); }
            } catch(IOException cancelled) { /* Closing the raw upstream request reaches this actual socket. */ }
            catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { active.decrementAndGet(); exchange.close(); }
        }); origin.start();
        try(RelayServer local=new RelayServer("127.0.0.1",0,0,null)) {
            Path config=dir.resolve("monitor-stall-nodes.json");
            Files.writeString(config,"{\"nodes\":[{\"id\":\"a\",\"control\":\"ws://127.0.0.1:1/voice\"},"
                    +"{\"id\":\"b\",\"control\":\"ws://127.0.0.1:"+origin.getAddress().getPort()+"/voice\"}]}");
            RoutingDirectory routes=new RoutingDirectory(config,dir.resolve("monitor-stall.json"),true);
            try(DirectoryNodeMonitor monitor=new DirectoryNodeMonitor(routes,local,"a","9".repeat(64),false)) {
                long start=System.nanoTime(); monitor.refresh();
                require(hits.get()==1 && System.nanoTime()-start<TimeUnit.MILLISECONDS.toNanos(4500),"Stalled node report did not obey its complete-body deadline");
                await(() -> active.get()==0,"Timed-out monitor report socket was cancelled");
                // Closing returns immediately even after a response started but never supplied its body.
                start=System.nanoTime(); monitor.close();
                require(System.nanoTime()-start<TimeUnit.SECONDS.toNanos(1),"Closing monitor retained a stalled report");
            }
        } finally { release.countDown(); origin.stop(0); handlers.shutdownNow(); }
    }
    private static void verifyClose(Path dir) throws Exception {
        try(Origin origin=new Origin(new AtomicLong(System.currentTimeMillis()));
            DirectoryMirror mirror=new DirectoryMirror(origin.uri(),dir.resolve("close.json"));
            ExecutorService callers=Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Boolean> waiting=callers.submit(() -> {
                try { mirror.route("3".repeat(64)); return false; } catch(IOException expected) { return true; }
            });
            await(() -> origin.hits.get()==1,"Pending mirror query"); mirror.close();
            require(waiting.get(1,TimeUnit.SECONDS),"Closing mirror did not cancel a pending upstream request");
        }
    }
    private static JsonObject response(long until) {
        return RoutingDirectory.response(new RoutingDirectory.Node("a",URI.create("wss://a.example/voice"),1,24454,true),List.of(),until);
    }
    private static final class Origin implements AutoCloseable {
        final AtomicInteger hits=new AtomicInteger(); final AtomicBoolean online=new AtomicBoolean(true);
        final AtomicBoolean stallBody=new AtomicBoolean(); final CountDownLatch bodyRelease=new CountDownLatch(1);
        final AtomicReference<String> authorization=new AtomicReference<>("");
        final CountDownLatch release=new CountDownLatch(1);
        final HttpServer server; final ExecutorService tasks=Executors.newVirtualThreadPerTaskExecutor();
        Origin(AtomicLong now) throws Exception {
            server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),128); server.setExecutor(tasks);
            server.createContext("/route",exchange -> {
                try {
                    hits.incrementAndGet(); authorization.set(Objects.toString(exchange.getRequestHeaders().getFirst("Authorization"),""));
                    if(!release.await(10,TimeUnit.SECONDS))return;
                    byte[] bytes=Wire.GSON.toJson(response(now.get()+60_000)).getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(online.get()?200:503,bytes.length);
                    if(stallBody.get()) { exchange.getResponseBody().write(bytes[0]); exchange.getResponseBody().flush(); bodyRelease.await(10,TimeUnit.SECONDS); }
                    else exchange.getResponseBody().write(bytes);
                } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                finally { exchange.close(); }
            }); server.start();
        }
        URI uri() { return URI.create("http://127.0.0.1:"+server.getAddress().getPort()+"/route"); }
        @Override public void close() { release.countDown(); bodyRelease.countDown(); server.stop(0); tasks.shutdownNow(); }
    }
    private static void await(BooleanSupplier condition,String message) throws Exception {
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
        while(!condition.getAsBoolean()) { if(System.nanoTime()>until)throw new AssertionError(message); Thread.sleep(5); }
    }
    private static void require(boolean ok,String message) { if(!ok)throw new AssertionError(message); }
}
