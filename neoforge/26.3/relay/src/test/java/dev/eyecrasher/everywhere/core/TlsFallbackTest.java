package dev.eyecrasher.everywhere.core;

import com.sun.net.httpserver.*;
import dev.eyecrasher.everywhere.relay.RelayServer;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import javax.net.ssl.*;

/** Real TLS endpoints behind a deliberately limited DPI model, not a test of a real ISP. */
public final class TlsFallbackTest {
    private record Certificates(SSLContext server, SSLContext trusted) {}
    public static void main(String[] args) throws Exception {
        if (args.length > 0) { for (String endpoint : args) publicEndpoint(URI.create(endpoint)); return; }
        Certificates certificates = certificates();
        recordBoundaries(certificates.trusted);
        https(certificates);
        voice(certificates);
        System.out.println("TLS recovery checks passed (local model only; no real censorship claim)");
    }
    private static Certificates certificates() throws Exception {
        Path folder = Files.createTempDirectory("everywhere-test-tls-");
        Path store = folder.resolve("fixture.p12"); char[] password = "local-test-only".toCharArray();
        try {
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                    "-genkeypair", "-alias", "fixture", "-keyalg", "EC", "-groupname", "secp256r1", "-validity", "2",
                    "-dname", "CN=localhost", "-ext", "SAN=dns:localhost", "-keystore", store.toString(),
                    "-storetype", "PKCS12", "-storepass", new String(password), "-noprompt")
                    .redirectErrorStream(true).start();
            if (!process.waitFor(15, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new AssertionError("Test keytool timeout"); }
            if (process.exitValue() != 0) throw new AssertionError("Test certificate generation failed");
            KeyStore keys = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(store)) { keys.load(input, password); }
            var manager = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); manager.init(keys, password);
            SSLContext server = SSLContext.getInstance("TLS"); server.init(manager.getKeyManagers(), null, null);
            SSLContext trusted = Tls.pinned(new ByteArrayInputStream(keys.getCertificate("fixture").getEncoded()));
            return new Certificates(server, trusted);
        } finally { Arrays.fill(password, '\0'); Files.deleteIfExists(store); Files.deleteIfExists(folder); }
    }
    private static HttpClient http(SSLContext context) {
        return HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(2))
                .proxy(ProxySelector.of(null)).version(HttpClient.Version.HTTP_1_1).build();
    }
    private static void recordBoundaries(SSLContext context) throws Exception {
        SSLEngine engine = context.createSSLEngine("voice.example.test", 443); engine.setUseClientMode(true);
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setServerNames(List.of(new SNIHostName("voice.example.test"))); engine.setSSLParameters(parameters); engine.beginHandshake();
        ByteBuffer bytes = ByteBuffer.allocate(65536); engine.wrap(ByteBuffer.allocate(0), bytes); bytes.flip();
        byte[] original = new byte[bytes.remaining()]; bytes.get(original);
        original = TlsFragmentProxy.readRecord(new ByteArrayInputStream(original));
        byte[] split = TlsFragmentProxy.fragment(original);
        int first = u16(split, 3), next = 5+first, second = u16(split, next+3);
        check(split.length == original.length+5 && next+5+second == split.length, "Exactly two bounded TLS records");
        ByteArrayOutputStream payload = new ByteArrayOutputStream(); payload.write(split, 5, first); payload.write(split, next+5, second);
        check(Arrays.equals(payload.toByteArray(), Arrays.copyOfRange(original, 5, original.length)), "Handshake transcript unchanged");
        String hello = new String(original, StandardCharsets.ISO_8859_1);
        int sni = hello.indexOf("voice.example.test");
        check(sni >= 0 && next > sni && next < sni+"voice.example.test".length(), "Split inside actual JSSE SNI");
        for (byte[] invalid : List.of(new byte[0], new byte[]{22,3,3,127,-1}, new byte[]{22,3,3,0,4,1,0})) {
            try { TlsFragmentProxy.readRecord(new ByteArrayInputStream(invalid)); throw new AssertionError("Invalid record accepted"); }
            catch (IOException expected) {}
        }
        System.out.println("TLS records: real JSSE ClientHello split inside SNI, transcript preserved, malformed input bounded");
    }
    private static void https(Certificates certificates) throws Exception {
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 4);
        server.setHttpsConfigurator(new HttpsConfigurator(certificates.server));
        AtomicInteger status = new AtomicInteger(200), requests = new AtomicInteger();
        server.createContext("/route", exchange -> {
            requests.incrementAndGet();
            byte[] body = "{\"protocol\":1,\"control\":\"wss://localhost:443/voice\",\"probes\":[]}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.get(), body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.createContext("/voice", exchange -> { exchange.sendResponseHeaders(403, -1); exchange.close(); });
        server.start();
        try (Gate gate = new Gate(server.getAddress().getPort()); HttpClient http = http(certificates.trusted)) {
            URI uri = gate.uri("https", "/route");
            HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3)).GET().build();
            try (TlsFallback fallback = new TlsFallback(http)) {
                check(fallback.get(request, HttpResponse.BodyHandlers.ofString()).get(8, TimeUnit.SECONDS).statusCode() == 200, "Normal HTTPS");
                check(!fallback.attempted() && !fallback.recovered() && gate.fragmented.get() == 0, "Healthy path has no recovery traffic");
            }
            status.set(503);
            try (TlsFallback fallback = new TlsFallback(http)) {
                check(fallback.get(request, HttpResponse.BodyHandlers.ofString()).get(8, TimeUnit.SECONDS).statusCode() == 503, "HTTP response preserved");
                check(!fallback.attempted(), "HTTP service error is not a DPI trigger");
            }
            status.set(200); gate.requireFragments.set(true);
            // A separate HttpClient guarantees a new TCP/TLS connection instead of a pooled healthy one.
            try (HttpClient fresh = http(certificates.trusted)) {
                var route = NetworkDirectory.lookup(uri, "e".repeat(64), fresh).get(10, TimeUnit.SECONDS);
                check(route.control().equals(URI.create("wss://localhost:443/voice")), "Directory recovered with validated TLS");
                check(gate.blocked.get() > 0 && gate.fragmented.get() > 0, "Ordinary TLS blocked; record split passed");
            }
            gate.hangComplete.set(true);
            try (HttpClient fresh = http(certificates.trusted); TlsFallback fallback = new TlsFallback(fresh)) {
                check(fallback.get(request, HttpResponse.BodyHandlers.ofString()).get(10, TimeUnit.SECONDS).statusCode() == 200,
                        "A stalled TLS handshake must also recover within bounded time");
                check(fallback.recovered(), "Timeout activates the recovery path");
            }
            gate.hangComplete.set(false);
            gate.requireFragments.set(false);
            int fragments = gate.fragmented.get();
            try (HttpClient untrusted = http(SSLContext.getDefault()); TlsFallback fallback = new TlsFallback(untrusted)) {
                failure(fallback.get(request, HttpResponse.BodyHandlers.ofString()));
                check(!fallback.attempted(), "Untrusted certificate must not trigger recovery");
            }
            try (HttpClient fresh = http(certificates.trusted); TlsFallback fallback = new TlsFallback(fresh)) {
                URI wrongHost = URI.create("https://127.0.0.1:"+gate.port()+"/route");
                failure(fallback.get(HttpRequest.newBuilder(wrongHost).timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString()));
                check(!fallback.attempted(), "Wrong certificate hostname must not trigger recovery");
            }
            check(gate.fragmented.get() == fragments, "Certificate failures generated no fragmentation");
            gate.requireFragments.set(true);
            try (HttpClient untrusted = http(SSLContext.getDefault()); TlsFallback fallback = new TlsFallback(untrusted)) {
                failure(fallback.get(request, HttpResponse.BodyHandlers.ofString()));
                check(fallback.attempted() && !fallback.recovered(), "Recovery must still reject an untrusted certificate");
            }
            try (HttpClient fresh = http(certificates.trusted); TlsFallback fallback = new TlsFallback(fresh)) {
                URI wrongHost = URI.create("https://127.0.0.1:"+gate.port()+"/route");
                failure(fallback.get(HttpRequest.newBuilder(wrongHost).timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString()));
                check(fallback.attempted() && !fallback.recovered(), "Recovery must still verify the certificate hostname");
            }
            gate.requireFragments.set(false);
            try (HttpClient fresh = http(certificates.trusted); TlsFallback fallback = new TlsFallback(fresh)) {
                failure(fallback.connect(gate.uri("wss", "/voice"), new WebSocket.Listener() {}));
                check(!fallback.attempted(), "A rejected WebSocket upgrade must not trigger recovery");
            }
            gate.rejectAll.set(true);
            try (HttpClient fresh = http(certificates.trusted); TlsFallback fallback = new TlsFallback(fresh)) {
                failure(fallback.get(request, HttpResponse.BodyHandlers.ofString()));
                check(fallback.attempted() && !fallback.recovered(), "Both TLS strategies can fail normally");
                int attempts = gate.fragmented.get();
                failure(fallback.get(request, HttpResponse.BodyHandlers.ofString()));
                check(gate.fragmented.get() == attempts, "Only one recovery strategy per operation owner");
            }
            gate.rejectAll.set(false);
            try (TlsFragmentProxy proxy = new TlsFragmentProxy(uri)) {
                InetSocketAddress address = proxy.address();
                check(address.getAddress().isLoopbackAddress(), "Adapter is loopback only");
                try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
                    socket.setSoTimeout(2000);
                    socket.getOutputStream().write("CONNECT unrelated.invalid:443 HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                    check(new String(socket.getInputStream().readNBytes(12), StandardCharsets.US_ASCII).startsWith("HTTP/1.1 403"), "Other destination rejected");
                }
                proxy.close();
                try (Socket ignored = new Socket(address.getAddress(), address.getPort())) { throw new AssertionError("Adapter listener leaked"); }
                catch (ConnectException expected) {}
            }
            gate.requireFragments.set(true);
            ProxySelector configured = new ProxySelector() {
                public List<Proxy> select(URI ignored) { return List.of(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", 1))); }
                public void connectFailed(URI ignored, SocketAddress address, IOException error) {}
            };
            try (HttpClient proxied = HttpClient.newBuilder().sslContext(certificates.trusted).proxy(configured).connectTimeout(Duration.ofMillis(300)).build();
                 TlsFallback fallback = new TlsFallback(proxied)) {
                failure(fallback.get(request, HttpResponse.BodyHandlers.ofString()));
                check(!fallback.attempted(), "Configured HTTP proxy must not be bypassed");
            }
            check(requests.get() == 4, "No application requests duplicated on TLS/certificate failures");
            System.out.println("HTTPS: directory recovery, HTTP errors preserved, TLS verification on both paths, bounded failure, fixed destination, cleanup and proxy policy passed");
        } finally { server.stop(0); }
    }
    private static void publicEndpoint(URI uri) throws Exception {
        NetworkDirectory.requireSecure(uri, "/voice");
        try (TlsFragmentProxy proxy = new TlsFragmentProxy(uri);
             HttpClient http = HttpClient.newBuilder().proxy(ProxySelector.of(proxy.address()))
                     .connectTimeout(Duration.ofSeconds(5)).build()) {
            URI health = new URI("https", uri.getRawAuthority(), "/health", null, null);
            var response = http.send(HttpRequest.newBuilder(health).timeout(Duration.ofSeconds(8)).GET().build(), HttpResponse.BodyHandlers.ofString());
            check(response.statusCode() == 200 && Wire.parse(response.body()).get("status").getAsString().equals("ok"), "Public HTTPS through record split");
            WebSocket socket = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(8)).buildAsync(uri, new WebSocket.Listener() {
                public void onOpen(WebSocket webSocket) { webSocket.request(1); }
            }).get(10, TimeUnit.SECONDS);
            socket.abort();
            System.out.println("Public node "+uri.getHost()+": split-record HTTPS health and WSS upgrade passed with default certificate/hostname verification");
        }
    }
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio = new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer, long sequence, int distance, byte[] bytes, boolean end) { if (!end) audio.add(bytes); }
    }
    private static VoiceClient client(URI uri, SSLContext context, String name, Sink sink) throws Exception {
        UUID id = Wire.offlineUuid(name);
        return new VoiceClient(uri, context, new VoiceClient.Identity(id, name, id, name, "d".repeat(64), "minecraft:overworld"),
                false, false, challenge -> CompletableFuture.completedFuture(false), sink);
    }
    private static void voice(Certificates certificates) throws Exception {
        try (RelayServer relay = new RelayServer("127.0.0.1", 0, 0, certificates.server)) {
            relay.start(); await(() -> relay.getPort() > 0, "TLS relay start");
            try (Gate gate = new Gate(relay.getPort())) {
                gate.requireFragments.set(true); URI uri = gate.uri("wss", "/voice");
                Sink a = new Sink(), b = new Sink();
                try (VoiceClient ca = client(uri, certificates.trusted, "TlsActor", a);
                     VoiceClient cb = client(uri, certificates.trusted, "TlsFriend", b)) {
                    ca.connect().get(15, TimeUnit.SECONDS); cb.connect().get(15, TimeUnit.SECONDS);
                    ca.nearby(Set.of(Wire.offlineUuid("TlsFriend"))); cb.nearby(Set.of(Wire.offlineUuid("TlsActor")));
                    await(() -> ca.peers().size() == 1 && cb.peers().size() == 1, "TLS peers");
                    await(() -> ca.controlTransport().equals("TLS-record-split") && cb.controlTransport().equals("TLS-record-split"), "Recovery diagnostics");
                    byte[] payload = {31, 7, 11}; ca.sendAudio(Set.of(Wire.offlineUuid("TlsFriend")), 0, 16, payload, false);
                    check(Arrays.equals(b.audio.poll(3, TimeUnit.SECONDS), payload), "Encrypted voice over recovered WSS");
                    cb.sendAudio(Set.of(Wire.offlineUuid("TlsActor")), 0, 16, payload, false);
                    check(Arrays.equals(a.audio.poll(3, TimeUnit.SECONDS), payload), "Reverse encrypted voice over recovered WSS");
                    check(gate.blocked.get() >= 2 && gate.fragmented.get() >= 2, "Each client recovered from normal TLS failure");
                }
                System.out.println("WSS: two real VoiceClients automatically recovered, discovered each other and exchanged encrypted frames in both directions");
            }
        }
    }
    /** Drops complete ClientHellos, but forwards fragmented handshakes to a genuine TLS endpoint. */
    private static final class Gate implements AutoCloseable {
        final AtomicBoolean requireFragments = new AtomicBoolean();
        final AtomicBoolean rejectAll = new AtomicBoolean();
        final AtomicBoolean hangComplete = new AtomicBoolean();
        final AtomicInteger blocked = new AtomicInteger(), fragmented = new AtomicInteger();
        private final ServerSocket listener = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final int target;
        Gate(int target) throws IOException { this.target = target; tasks.execute(this::accept); }
        int port() { return listener.getLocalPort(); }
        URI uri(String scheme, String path) { return URI.create(scheme+"://localhost:"+port()+path); }
        private void accept() {
            while (!listener.isClosed()) {
                try { Socket socket = listener.accept(); sockets.add(socket); tasks.execute(() -> serve(socket)); }
                catch (IOException | RejectedExecutionException ignored) { return; }
            }
        }
        private void serve(Socket client) {
            Socket upstream = new Socket(); sockets.add(upstream);
            try (client; upstream) {
                client.setSoTimeout(5000);
                // The adapter may send a one-byte handshake fragment, so this model reads TLS records independently.
                byte[] header = client.getInputStream().readNBytes(5);
                if (header.length != 5) return;
                int size = u16(header, 3); if (size < 1 || size > 16384) return;
                byte[] body = client.getInputStream().readNBytes(size); if (body.length != size) return;
                boolean complete = body.length >= 4 && body[0] == 1
                        && 4+(((body[1]&255)<<16) | ((body[2]&255)<<8) | (body[3]&255)) <= body.length;
                if (!complete) fragmented.incrementAndGet();
                if (rejectAll.get() || requireFragments.get() && complete) {
                    blocked.incrementAndGet();
                    if (hangComplete.get() && complete) client.getInputStream().read();
                    return;
                }
                upstream.connect(new InetSocketAddress("127.0.0.1", target), 2000);
                upstream.getOutputStream().write(header); upstream.getOutputStream().write(body); upstream.getOutputStream().flush();
                client.setSoTimeout(0); tasks.submit(() -> pump(upstream, client)); pump(client, upstream);
            } catch (IOException ignored) {}
            finally { sockets.remove(client); sockets.remove(upstream); }
        }
        private static void pump(Socket source, Socket target) {
            try { source.getInputStream().transferTo(target.getOutputStream()); } catch (IOException ignored) {}
            finally { closeSocket(source); closeSocket(target); }
        }
        public void close() throws IOException { listener.close(); sockets.forEach(Gate::closeSocket); tasks.shutdownNow(); }
        private static void closeSocket(Socket socket) { try { socket.close(); } catch (IOException ignored) {} }
    }
    private static int u16(byte[] b, int p) { return ((b[p]&255)<<8) | (b[p+1]&255); }
    private static void failure(CompletableFuture<?> future) throws Exception {
        try { future.get(10, TimeUnit.SECONDS); throw new AssertionError("Failure expected"); } catch (ExecutionException expected) {}
    }
    private static void check(boolean value, String message) { if (!value) throw new AssertionError(message); }
    private static void await(BooleanSupplier condition, String label) throws Exception {
        long until = System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean()) { if (System.nanoTime() > until) throw new AssertionError(label); Thread.sleep(20); }
    }
}
