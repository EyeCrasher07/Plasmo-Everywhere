package dev.eyecrasher.everywhere.core;

import dev.eyecrasher.everywhere.relay.RelayServer;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Real encrypted packets through two UDP paths; a local fixture stands in for a second NAT mapping. */
final class UdpBackupTest {
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio = new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer, long sequence, int distance, byte[] bytes, boolean end) {
            if (!end) audio.add(bytes);
        }
    }
    private static final class FilterSocket extends DatagramSocket {
        final Set<SocketAddress> blocked = ConcurrentHashMap.newKeySet();
        FilterSocket() throws SocketException { super(); }
        public void send(DatagramPacket packet) throws IOException {
            if (!blocked.contains(packet.getSocketAddress())) super.send(packet);
        }
    }
    private static final class Forwarder implements AutoCloseable {
        final DatagramSocket socket = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        final Thread worker;
        Forwarder(InetSocketAddress a, InetSocketAddress b) throws SocketException {
            worker = Thread.ofVirtual().name("test-secondary-udp").start(() -> {
                try {
                    while (!socket.isClosed()) {
                        var packet = new DatagramPacket(new byte[Wire.MAX_UDP + 1], Wire.MAX_UDP + 1);
                        socket.receive(packet);
                        var target = packet.getSocketAddress().equals(a) ? b : packet.getSocketAddress().equals(b) ? a : null;
                        if (target == null || packet.getLength() > Wire.MAX_UDP) continue;
                        Thread.sleep(10); // The secondary path is deliberately slower than loopback.
                        socket.send(new DatagramPacket(packet.getData(), packet.getLength(), target));
                    }
                } catch (IOException | InterruptedException stopped) { /* The test closes its own socket. */ }
            });
        }
        InetSocketAddress address() { return new InetSocketAddress("127.0.0.1", socket.getLocalPort()); }
        public void close() { socket.close(); try { worker.join(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
    }

    static void verify() throws Exception {
        try (RelayServer relay = new RelayServer("127.0.0.1", 0, 0, null)) {
            relay.start(); await(() -> relay.getPort() > 0, "Backup relay start");
            URI uri = URI.create("ws://127.0.0.1:" + relay.getPort() + "/voice");
            UUID a = Wire.offlineUuid("BackupActor"), b = Wire.offlineUuid("BackupFriend");
            Sink sa = new Sink(), sb = new Sink();
            try (FilterSocket ua = new FilterSocket(); FilterSocket ub = new FilterSocket();
                 VoiceClient ca = client(uri, a, "BackupActor", sa, ua); VoiceClient cb = client(uri, b, "BackupFriend", sb, ub)) {
                ca.connect().get(5, TimeUnit.SECONDS); cb.connect().get(5, TimeUnit.SECONDS);
                ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.networkStats().udpDirect() == 1 && cb.networkStats().udpDirect() == 1, "Primary UDP consent");
                ((TcpTransport)field(ca, "tcp")).close(); ((TcpTransport)field(cb, "tcp")).close();
                UUID originalA = ca.sessionId(), originalB = cb.sessionId();
                exchange(ca, cb, sa, sb, a, b, 0);
                var addressA = new InetSocketAddress("127.0.0.1", ua.getLocalPort());
                var addressB = new InetSocketAddress("127.0.0.1", ub.getLocalPort());
                try (Forwarder secondary = new Forwarder(addressA, addressB)) {
                    InetSocketAddress backup = secondary.address();
                    // Seed real pair-encrypted traffic from a new port of the already permitted IP.
                    // Both clients must perform their own fresh consent checks before using it.
                    seed(ca, ua, backup); seed(cb, ub, backup);
                    await(() -> ca.peerDiagnostics().getFirst().confirmed() >= 2
                            && cb.peerDiagnostics().getFirst().confirmed() >= 2, "Live secondary UDP consent while primary remains active");
                    check(!ca.peerDiagnostics().toString().contains("127.0.0.1")
                            && !ca.connectionDiagnostics().toString().contains("127.0.0.1"), "Diagnostic records must not expose endpoints");
                    ua.blocked.add(addressB); ub.blocked.add(addressA);
                    await(() -> backup.equals(path(ca).selected(System.nanoTime()))
                            && backup.equals(path(cb).selected(System.nanoTime())), "Warm UDP backup selected after primary loss");
                    exchange(ca, cb, sa, sb, a, b, 1);
                    check(voice(relay) == 0, "A working alternate UDP route must not relay media through the VPS");
                }
                await(() -> ca.networkStats().udpDirect() == 0 && cb.networkStats().udpDirect() == 0, "Both UDP paths expire");
                exchange(ca, cb, sa, sb, a, b, 2);
                check(voice(relay) >= 2, "Relay reserve used after both direct routes fail");
                check(originalA.equals(ca.sessionId()) && originalB.equals(cb.sessionId()), "Route changes must preserve voice sessions");
            }
        }
        System.out.println("UDP backup: real encrypted voice, independent consent, direct route failover with zero VPS media, then relay fallback passed");
    }

    private static VoiceClient client(URI uri, UUID id, String name, Sink sink, DatagramSocket socket) throws Exception {
        return new VoiceClient(uri, null, new VoiceClient.Identity(id, name, id, name, "f".repeat(64), "minecraft:overworld"),
                true, true, challenge -> CompletableFuture.completedFuture(false), sink, List.of(), socket, true);
    }
    private static Object peer(VoiceClient client) throws ReflectiveOperationException {
        return ((Map<?,?>)field(client, "peers")).values().iterator().next();
    }
    private static DirectPath path(VoiceClient client) {
        try { return (DirectPath)field(peer(client), "path"); }
        catch (ReflectiveOperationException e) { throw new AssertionError(e); }
    }
    private static void seed(VoiceClient client, DatagramSocket socket, InetSocketAddress endpoint) throws ReflectiveOperationException, IOException {
        PeerCrypto crypto = (PeerCrypto)field(peer(client), "crypto");
        byte[] bytes = crypto.seal(Wire.PING, Wire.random(16)).encode();
        socket.send(new DatagramPacket(bytes, bytes.length, endpoint));
    }
    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        var field = owner.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(owner);
    }
    private static void exchange(VoiceClient a, VoiceClient b, Sink sa, Sink sb, UUID pa, UUID pb, long sequence) throws Exception {
        byte[] payload = {42, 19, (byte)sequence};
        a.sendAudio(Set.of(pb), sequence, 16, payload, false); b.sendAudio(Set.of(pa), sequence, 16, payload, false);
        check(Arrays.equals(payload, sb.audio.poll(3, TimeUnit.SECONDS)), "Forward encrypted voice on selected path");
        check(Arrays.equals(payload, sa.audio.poll(3, TimeUnit.SECONDS)), "Reverse encrypted voice on selected path");
    }
    private static long voice(RelayServer relay) {
        var health = relay.health(); return health.get("relayedUdpVoicePackets").getAsLong() + health.get("relayedWssVoicePackets").getAsLong();
    }
    private static void await(BooleanSupplier condition, String label) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        check(condition.getAsBoolean(), label);
    }
    private static void check(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
}
