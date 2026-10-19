package dev.eyecrasher.everywhere.core;

import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Real bounded HTTP exchanges; loopback cleartext is only permitted for local tests. */
final class DirectoryFallbackTest {
    static void verify() throws Exception {
        ExecutorService workers=Executors.newVirtualThreadPerTaskExecutor();
        HttpServer primary=HttpServer.create(new InetSocketAddress("127.0.0.1",0),8);
        HttpServer secondary=HttpServer.create(new InetSocketAddress("127.0.0.1",0),8);
        primary.setExecutor(workers); secondary.setExecutor(workers);
        AtomicInteger status=new AtomicInteger(200), delay=new AtomicInteger(), secondaryCalls=new AtomicInteger();
        AtomicLong lease=new AtomicLong(60_000);
        primary.createContext("/route", exchange -> {
            try {
                if(delay.get()>0)Thread.sleep(delay.get());
                respond(exchange,status.get(),route(lease.get()));
            } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        secondary.createContext("/route", exchange -> {
            try { secondaryCalls.incrementAndGet(); respond(exchange,200,route(60_000)); }
            finally { exchange.close(); }
        });
        primary.start(); secondary.start();
        URI a=URI.create("http://127.0.0.1:"+primary.getAddress().getPort()+"/route");
        URI b=URI.create("http://127.0.0.1:"+secondary.getAddress().getPort()+"/route");
        String room="f".repeat(64);
        try {
            var result=NetworkDirectory.lookup(List.of(a,b),room).get(4,TimeUnit.SECONDS);
            require(result.control().equals(URI.create("wss://example.test/voice")),"Primary assignment preserved");
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(secondaryCalls.get()==0 && System.nanoTime()<deadline)Thread.sleep(10);
            require(secondaryCalls.get()>0,"Successful primary warms the secondary");
            status.set(503);
            ConnectionTrace trace = new ConnectionTrace(); trace.startAttempt();
            require(NetworkDirectory.lookup(List.of(a,b),room,trace).get(4,TimeUnit.SECONDS).equals(result),"HTTP failure uses the alternate directory");
            require(trace.snapshot().stream().anyMatch(event -> event.stage().equals("DIRECTORY_1_HTTP") && event.code() == 503)
                            && trace.snapshot().stream().anyMatch(event -> event.stage().equals("DIRECTORY_2_HTTP") && event.code() == 200),
                    "Trace retains per-endpoint HTTP failure and successful fallback");
            require(trace.snapshot().stream().noneMatch(event -> event.toString().contains(room) || event.toString().contains("127.0.0.1")),
                    "Directory trace does not retain server room or service endpoint");
            status.set(200); delay.set(4500);
            long started=System.nanoTime();
            require(NetworkDirectory.lookup(List.of(a,b),room).get(4,TimeUnit.SECONDS).equals(result),"A stalled primary does not block a live alternate");
            require(System.nanoTime()-started<TimeUnit.SECONDS.toNanos(4),"Alternate entry point was not hedged");
            delay.set(0); lease.set(-1);
            try { NetworkDirectory.lookup(a,room).get(4,TimeUnit.SECONDS); throw new AssertionError("Expired route accepted"); }
            catch(ExecutionException expected) {}
            lease.set(TimeUnit.MINUTES.toMillis(10));
            try { NetworkDirectory.lookup(a,room).get(4,TimeUnit.SECONDS); throw new AssertionError("Unbounded route lease accepted"); }
            catch(ExecutionException expected) {}
            require(NetworkDirectory.lookup(List.of(a,b),room).get(4,TimeUnit.SECONDS).equals(result),"Invalid primary lease can use valid alternate");
            for(URI invalid:List.of(URI.create("http://example.test/route"),URI.create("https://example.test:0/route"),
                    URI.create("https://example.test/route?target=evil"))) {
                try { NetworkDirectory.lookup(List.of(invalid),room); throw new AssertionError("Unsafe endpoint accepted"); }
                catch(IllegalArgumentException expected) {}
            }
            System.out.println("Directory client: alternate entry, hedged timeout, secondary warmup, bounded leases and secure endpoints passed");
        } finally { primary.stop(0); secondary.stop(0); workers.shutdownNow(); }
    }
    private static byte[] route(long lease) {
        return ("{\"protocol\":1,\"control\":\"wss://example.test/voice\",\"probes\":[],\"validUntil\":"+(System.currentTimeMillis()+lease)+"}").getBytes(StandardCharsets.UTF_8);
    }
    private static void respond(HttpExchange exchange,int status,byte[] bytes) throws java.io.IOException {
        exchange.sendResponseHeaders(status,bytes.length); exchange.getResponseBody().write(bytes);
    }
    private static void require(boolean value,String label) { if(!value)throw new AssertionError(label); }
}
