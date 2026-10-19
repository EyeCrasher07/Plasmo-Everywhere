package dev.eyecrasher.everywhere.core;

import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLContext;

/** Also compiled independently with --release 17 and run by the installed Java 17 runtime. */
public final class RuntimeCompatTest {
    public static void main(String[] args) throws Exception {
        verify();
        if (args.length == 2) {
            SSLContext tls;
            try (InputStream input = Files.newInputStream(Path.of(args[1]))) { tls = Tls.pinned(input); }
            URI endpoint = URI.create(args[0]);
            voice(endpoint, tls, true, true, false, "Direct"); Thread.sleep(1100);
            voice(endpoint, tls, false, true, false, "RelayUdp"); Thread.sleep(1100);
            voice(endpoint, tls, false, false, false, "RelayWss"); Thread.sleep(1100);
            voice(endpoint, tls, true, true, true, "Tcp");
        }
    }
    static void verify() throws Exception {
        AtomicReference<Thread> current = new AtomicReference<>();
        Thread thread = RuntimeCompat.start("everywhere-compat-test", () -> current.set(Thread.currentThread()));
        thread.join(2000);
        check(current.get() == thread && thread.isDaemon() && thread.getName().equals("everywhere-compat-test"),
                "Named daemon-compatible network worker");
        if (Runtime.version().feature() >= 21)
            check(Boolean.TRUE.equals(Thread.class.getMethod("isVirtual").invoke(thread)), "Modern runtimes retain virtual workers");
        boundedWorkers(); cancelHttp();
        System.out.println("Runtime compatibility: Java " + Runtime.version().feature()
                + ", named daemon/virtual workers, bounded queues and explicit pending HTTP cancellation passed");
    }
    private static void boundedWorkers() throws Exception {
        ExecutorService workers = RuntimeCompat.workers("everywhere-compat-bound", 2);
        CountDownLatch started = new CountDownLatch(2), release = new CountDownLatch(1);
        try {
            for (int n = 0; n < 2; n++) workers.execute(() -> {
                started.countDown();
                try { release.await(); } catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
            });
            check(started.await(2, TimeUnit.SECONDS), "Bounded workers start concurrently");
            try { workers.execute(() -> {}); throw new AssertionError("Excess worker accepted"); }
            catch (RejectedExecutionException expected) { /* No queue or extra platform threads. */ }
        } finally { release.countDown(); workers.shutdownNow(); }
        check(workers.awaitTermination(2, TimeUnit.SECONDS), "Bounded workers stop");
    }
    private static void cancelHttp() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 2);
        ExecutorService workers = RuntimeCompat.workers("everywhere-compat-http", 2);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        server.setExecutor(workers);
        server.createContext("/pending", exchange -> {
            entered.countDown();
            try { release.await(); }
            catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        server.start();
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        try (TlsFallback fallback = new TlsFallback(http)) {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/pending"))
                    .timeout(Duration.ofSeconds(4)).GET().build();
            var pending = fallback.get(request, HttpResponse.BodyHandlers.ofString());
            check(entered.await(2, TimeUnit.SECONDS), "Real HTTP operation started");
            fallback.close();
            check(pending.isCancelled(), "Closing owner cancels the real pending operation on older JDKs");
            check(fallback.get(request, HttpResponse.BodyHandlers.ofString()).isCompletedExceptionally(), "Closed owner cannot start new operations");
            check(RuntimeCompat.shutdown(http) == (Runtime.version().feature() >= 21), "Only supported native HTTP shutdown is reported");
        } finally { RuntimeCompat.shutdown(http); release.countDown(); server.stop(0); workers.shutdownNow(); }
    }
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio = new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer, long sequence, int distance, byte[] bytes, boolean end) { if (!end) audio.add(bytes); }
    }
    private static final class SilentSocket extends DatagramSocket {
        SilentSocket() throws SocketException { super(); }
        public void send(DatagramPacket packet) { /* Simulated UDP-blocked network retains TCP discovery. */ }
    }
    private static void voice(URI uri, SSLContext tls, boolean direct, boolean udp, boolean tcpOnly, String label) throws Exception {
        UUID a = Wire.offlineUuid("Compat" + label + "A"), b = Wire.offlineUuid("Compat" + label + "B");
        Sink sa = new Sink(), sb = new Sink();
        try (DatagramSocket ua = !udp ? null : tcpOnly ? new SilentSocket() : new DatagramSocket();
             DatagramSocket ub = !udp ? null : tcpOnly ? new SilentSocket() : new DatagramSocket();
             VoiceClient ca = client(uri, tls, a, "Compat" + label + "A", direct, udp, sa, ua, tcpOnly);
             VoiceClient cb = client(uri, tls, b, "Compat" + label + "B", direct, udp, sb, ub, tcpOnly)) {
            ca.connect().get(10, TimeUnit.SECONDS); cb.connect().get(10, TimeUnit.SECONDS);
            ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
            await(() -> ca.peers().size() == 1 && cb.peers().size() == 1, "TLS peer discovery");
            if (tcpOnly) await(() -> ca.networkStats().tcpDirect() == 1 && cb.networkStats().tcpDirect() == 1, "Java 17 direct TCP");
            else if (direct) await(() -> ca.networkStats().udpDirect() == 1 && cb.networkStats().udpDirect() == 1, "Java 17 direct UDP");
            else if (udp) await(() -> ca.hasUdp() && cb.hasUdp(), "Java 17 authenticated UDP relay");
            byte[] payload = {3, 7, 11}; ca.sendAudio(Set.of(b), 0, 16, payload, false); cb.sendAudio(Set.of(a), 0, 16, payload, false);
            check(Arrays.equals(sb.audio.poll(3, TimeUnit.SECONDS), payload) && Arrays.equals(sa.audio.poll(3, TimeUnit.SECONDS), payload),
                    "Bidirectional encrypted voice on " + label);
            check(ca.isReady() && cb.isReady(), "Transport stayed connected");
            System.out.println("Java " + Runtime.version().feature() + " genuine TLS control + encrypted " + label + " voice passed");
        }
    }
    private static VoiceClient client(URI uri, SSLContext tls, UUID id, String name, boolean direct, boolean udp,
                                      Sink sink, DatagramSocket socket, boolean automatic) throws Exception {
        return new VoiceClient(uri, tls, new VoiceClient.Identity(id, name, id, name, "b".repeat(64), "minecraft:overworld"),
                direct, udp, challenge -> CompletableFuture.completedFuture(false), sink, List.of(), socket, automatic);
    }
    private static void await(BooleanSupplier condition, String label) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        check(condition.getAsBoolean(), label);
    }
    private static void check(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
}
