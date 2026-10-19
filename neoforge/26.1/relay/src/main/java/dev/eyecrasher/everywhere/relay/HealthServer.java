package dev.eyecrasher.everywhere.relay;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import dev.eyecrasher.everywhere.core.Wire;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.*;
import dev.eyecrasher.everywhere.core.RateLimit;
import dev.eyecrasher.everywhere.core.Endpoints;
import com.google.gson.JsonObject;
import java.security.MessageDigest;

/** Loopback-only health endpoint, exposed through the HTTPS front end. */
final class HealthServer implements AutoCloseable {
    private final HttpServer http;
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(2,2,0,TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(64),new ThreadPoolExecutor.AbortPolicy());
    // Directory lookups may wait for a remote node. They must never occupy health/discovery workers.
    private final ThreadPoolExecutor routes = new ThreadPoolExecutor(4,4,0,TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(16),new ThreadPoolExecutor.AbortPolicy());
    private final RateLimit internalRoutes=new RateLimit(24,64);
    private final Map<String,RateLimit> requests=new LinkedHashMap<>();
    HealthServer(RelayServer relay, int port) throws Exception {
        this(relay,port,null);
    }
    HealthServer(RelayServer relay, int port, DirectoryRoutes directory) throws Exception {
        this(relay,port,directory,null);
    }
    HealthServer(RelayServer relay, int port, DirectoryRoutes directory,String directoryKey) throws Exception {
        if(directoryKey!=null && !directoryKey.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Directory key");
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 16);
        http.setExecutor(executor);
        http.createContext("/health", exchange -> {
            try {
                if (!exchange.getRequestURI().getPath().equals("/health") || !exchange.getRequestMethod().equals("GET")) {
                    exchange.sendResponseHeaders(404,-1); return;
                }
                var state = relay.health();
                byte[] bytes = Wire.GSON.toJson(state).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control", "no-store");
                exchange.sendResponseHeaders(state.get("status").getAsString().equals("ok") ? 200 : 503, bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        http.createContext("/tcp-discovery",exchange -> {
            try {
                if(!exchange.getRequestMethod().equals("GET") || !exchange.getRequestURI().toString().equals("/tcp-discovery")) {
                    exchange.sendResponseHeaders(400,-1); return;
                }
                // Only the local front end supplies these; it overwrites client-provided headers.
                String ip=exchange.getRequestHeaders().getFirst("X-Real-IP");
                String sourcePort=exchange.getRequestHeaders().getFirst("X-Real-Port");
                if(ip==null || sourcePort==null || !sourcePort.matches("[0-9]{1,5}")) { exchange.sendResponseHeaders(400,-1); return; }
                JsonObject response=Wire.message("tcp-discovery"); response.addProperty("protocol",1);
                response.addProperty("ip",ip); response.addProperty("port",Integer.parseInt(sourcePort));
                try { Endpoints.read(response,false); }
                catch(IllegalArgumentException invalid) { exchange.sendResponseHeaders(400,-1); return; }
                synchronized(requests) {
                    if(requests.size()>=1024 && !requests.containsKey(ip))requests.remove(requests.keySet().iterator().next());
                    if(!requests.computeIfAbsent(ip,k -> new RateLimit(4,16)).take()) { exchange.sendResponseHeaders(429,-1); return; }
                }
                byte[] bytes=Wire.GSON.toJson(response).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type","application/json; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control","no-store");
                exchange.sendResponseHeaders(200,bytes.length); exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        if(directoryKey!=null)http.createContext("/directory-node",exchange -> {
            try {
                String auth=exchange.getRequestHeaders().getFirst("Authorization");
                if(!exchange.getRequestMethod().equals("GET") || !exchange.getRequestURI().toString().equals("/directory-node")) {
                    exchange.sendResponseHeaders(404,-1); return;
                }
                if(!authenticated(auth,directoryKey)) {
                    exchange.sendResponseHeaders(403,-1); return;
                }
                byte[] bytes=Wire.GSON.toJson(DirectoryNodeMonitor.snapshot(relay)).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type","application/json; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control","no-store");
                exchange.sendResponseHeaders(200,bytes.length); exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        if(directory!=null)http.createContext("/route",exchange -> {
            boolean delegated=false;
            try {
                String query=exchange.getRequestURI().getRawQuery();
                if(!exchange.getRequestMethod().equals("GET") || !exchange.getRequestURI().getPath().equals("/route")
                        || query==null || !query.matches("room=[a-f0-9]{64}")) {
                    exchange.sendResponseHeaders(400,-1); return;
                }
                String ip=exchange.getRequestHeaders().getFirst("X-Real-IP");
                if(ip==null || !ip.matches("[0-9a-fA-F:.]{3,45}"))ip="loopback";
                // A configured mirror shares one source IP for all rooms. Give its server-only proof
                // a separate bounded budget; never accept a client-supplied original-IP override.
                if(authenticated(exchange.getRequestHeaders().getFirst("Authorization"),directoryKey)) {
                    if(!internalRoutes.take()) { exchange.sendResponseHeaders(429,-1); return; }
                } else synchronized(requests) {
                    if(requests.size()>=1024 && !requests.containsKey(ip))requests.remove(requests.keySet().iterator().next());
                    if(!requests.computeIfAbsent(ip,k -> new RateLimit(4,16)).take()) { exchange.sendResponseHeaders(429,-1); return; }
                }
                try { routes.execute(new RouteTask(exchange,directory,query.substring(5),System.nanoTime())); delegated=true; }
                catch(RejectedExecutionException busy) { exchange.sendResponseHeaders(503,-1); }
            } finally { if(!delegated)exchange.close(); }
        });
        http.start();
    }
    private static boolean authenticated(String auth,String key) {
        if(key==null || auth==null)return false;
        byte[] expected=("Bearer "+key).getBytes(StandardCharsets.US_ASCII);
        return auth.length()==expected.length && MessageDigest.isEqual(expected,auth.getBytes(StandardCharsets.US_ASCII));
    }
    private record RouteTask(HttpExchange exchange,DirectoryRoutes directory,String room,long acceptedAt) implements Runnable {
        @Override public void run() {
            try {
                byte[] bytes; int status;
                try {
                    // Do not start another upstream operation for an already stale queued HTTP request.
                    if(System.nanoTime()-acceptedAt>TimeUnit.SECONDS.toNanos(1))throw new java.io.IOException("Directory queue expired");
                    bytes=Wire.GSON.toJson(directory.route(room)).getBytes(StandardCharsets.UTF_8); status=200;
                }
                catch(Exception unavailable) { bytes="{\"status\":\"unavailable\"}".getBytes(StandardCharsets.UTF_8); status=503; }
                exchange.getResponseHeaders().set("Content-Type","application/json; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control","no-store");
                exchange.sendResponseHeaders(status,bytes.length); exchange.getResponseBody().write(bytes);
            } catch(java.io.IOException disconnected) { /* A closed HTTP client owns no voice session. */ }
            finally { exchange.close(); }
        }
    }
    int port() { return http.getAddress().getPort(); }
    @Override public void close() {
        http.stop(0); executor.shutdownNow();
        for(Runnable queued:routes.shutdownNow())if(queued instanceof RouteTask task)task.exchange.close();
    }
}
