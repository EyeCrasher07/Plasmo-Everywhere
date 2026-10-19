package dev.eyecrasher.everywhere.core;

import dev.eyecrasher.everywhere.relay.RelayServer;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Local send failures after UDP consent, using real encrypted transports and injected socket errors. */
public final class UdpSendFallbackTest {
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio = new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer, long sequence, int distance, byte[] bytes, boolean end) {
            if (!end) audio.add(bytes);
        }
    }
    private static final class FailingSocket extends DatagramSocket {
        volatile boolean dropAll, failDirectVoice, deliverThenFailDirect, failRelayVoice, failChecks;
        volatile byte[] failedDirect, relayFrame;
        int directFailures, relayFailures;
        FailingSocket() throws SocketException { super(); }
        public void send(DatagramPacket packet) throws IOException {
            byte[] bytes = Arrays.copyOfRange(packet.getData(), packet.getOffset(), packet.getOffset() + packet.getLength());
            int magic = bytes.length < 5 ? 0 : ByteBuffer.wrap(bytes).getInt();
            if (magic == Wire.MAGIC && bytes[4] == Wire.VOICE && failDirectVoice) {
                failedDirect = bytes; directFailures++;
                if (deliverThenFailDirect) super.send(packet);
                throw new IOException("Injected direct UDP send failure");
            }
            if (magic == Wire.UDP_MAGIC && bytes.length >= 47 && bytes[28] == Wire.ROUTE) {
                relayFrame = Arrays.copyOfRange(bytes, 31, bytes.length - 16);
                if (failRelayVoice) { relayFailures++; throw new IOException("Injected relay UDP send failure"); }
            }
            if (failChecks && (Stun.isMessage(bytes) || magic == Wire.MAGIC && bytes[4] == Wire.PING))
                throw new IOException("Injected optional UDP check failure");
            if (!dropAll) super.send(packet);
        }
    }

    public static void main(String[] args) throws Exception {
        relayFallback(); tcpFallback();
        System.out.println("UDP send failures: same-frame TCP / UDP relay / WebSocket fallback, accurate accepted-send counts, replay protection and direct recovery passed");
    }

    private static void relayFallback() throws Exception {
        try (RelayServer relay = new RelayServer("127.0.0.1", 0, 0, null)) {
            relay.start(); await(() -> relay.getPort() > 0, "Relay start");
            URI uri = URI.create("ws://127.0.0.1:" + relay.getPort() + "/voice");
            UUID a = Wire.offlineUuid("SendActor"), b = Wire.offlineUuid("SendFriend");
            Sink sa = new Sink(), sb = new Sink();
            try (FailingSocket ua = new FailingSocket(); FailingSocket ub = new FailingSocket();
                 VoiceClient ca = client(uri, a, "SendActor", sa, ua, false);
                 VoiceClient cb = client(uri, b, "SendFriend", sb, ub, false)) {
                connect(ca, cb, a, b);
                UUID original = ca.sessionId();
                send(ca, b, sb, 0, "Healthy direct UDP");
                check(ca.networkStats().directUdpVoicePackets() == 1 && voice(relay) == 0, "Baseline direct media");

                ua.failDirectVoice = true;
                send(ca, b, sb, 1, "Immediate UDP relay fallback after local direct send error");
                check(ua.directFailures == 1 && Arrays.equals(ua.failedDirect, ua.relayFrame), "Fallback must reuse the same encrypted frame");
                check(ca.networkStats().directUdpVoicePackets() == 1 && ca.networkStats().relayedVoicePackets() == 1,
                        "A failed direct send must not increment the direct counter");
                check(count(relay, "relayedUdpVoicePackets") == 1 && count(relay, "relayedWssVoicePackets") == 0,
                        "Available authenticated UDP relay must precede WebSocket");

                ua.failRelayVoice = true; ua.deliverThenFailDirect = true;
                send(ca, b, sb, 2, "Immediate WebSocket fallback when both UDP sends fail locally");
                check(ua.directFailures == 2 && ua.relayFailures == 1, "Both local UDP failures injected");
                check(ca.networkStats().directUdpVoicePackets() == 1 && ca.networkStats().relayedVoicePackets() == 2,
                        "One accepted fallback must count once across failed transports");
                // Relay health counts its outgoing transport, independent of the sender's
                // upload path. This receiver still has authenticated UDP available.
                check(count(relay, "relayedUdpVoicePackets") == 2 && count(relay, "relayedWssVoicePackets") == 0,
                        "WebSocket upload must preserve the recipient's usable UDP route");
                byte[] replayFrame = ua.failedDirect;

                ua.deliverThenFailDirect = false;
                AtomicInteger pending = (AtomicInteger)field(ca, "pendingSends");
                await(() -> pending.get() == 0, "WebSocket sends complete");
                pending.set(12);
                try { ca.sendAudio(Set.of(b), 3, 16, new byte[]{9, 3}, false); }
                finally { pending.set(0); }
                check(ca.networkStats().relayedVoicePackets() == 2, "A rejected WebSocket queue entry must not count as sent");
                check(sb.audio.poll(150, TimeUnit.MILLISECONDS) == null, "Rejected frame unexpectedly delivered");

                ua.failDirectVoice = false; ua.failRelayVoice = false; ua.failChecks = true;
                var sendUdp = VoiceClient.class.getDeclaredMethod("sendUdp", byte[].class, InetSocketAddress.class);
                sendUdp.setAccessible(true);
                var endpoint = new InetSocketAddress("127.0.0.1", ub.getLocalPort());
                check(Boolean.FALSE.equals(sendUdp.invoke(ca, Stun.request(Wire.random(12)), endpoint)), "Optional STUN send error injected");
                PeerCrypto crypto = (PeerCrypto)field(peer(ca), "crypto");
                check(Boolean.FALSE.equals(sendUdp.invoke(ca, crypto.seal(Wire.PING, Wire.random(16)).encode(), endpoint)),
                        "Optional candidate check error injected");
                send(ca, b, sb, 4, "Recovered direct UDP survives errors from optional checks");
                check(ca.networkStats().directUdpVoicePackets() == 2 && ca.networkStats().relayedVoicePackets() == 2,
                        "Local errors must not permanently disable working direct UDP");
                check(original.equals(ca.sessionId()), "Fallback must preserve the authenticated session");

                ua.send(new DatagramPacket(replayFrame, replayFrame.length, endpoint));
                check(sb.audio.poll(150, TimeUnit.MILLISECONDS) == null, "Fallback replay played duplicate audio");
            }
        }
    }

    private static void tcpFallback() throws Exception {
        try (RelayServer relay = new RelayServer("127.0.0.1", 0, 0, null)) {
            relay.start(); await(() -> relay.getPort() > 0, "TCP fallback relay start");
            URI uri = URI.create("ws://127.0.0.1:" + relay.getPort() + "/voice");
            UUID a = Wire.offlineUuid("TcpSendActor"), b = Wire.offlineUuid("TcpSendFriend");
            Sink sa = new Sink(), sb = new Sink();
            try (FailingSocket ua = new FailingSocket(); FailingSocket ub = new FailingSocket();
                 VoiceClient ca = client(uri, a, "TcpSendActor", sa, ua, true);
                 VoiceClient cb = client(uri, b, "TcpSendFriend", sb, ub, true)) {
                ua.dropAll = true; ub.dropAll = true;
                ca.connect().get(5, TimeUnit.SECONDS); cb.connect().get(5, TimeUnit.SECONDS);
                ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.networkStats().tcpDirect() == 1 && cb.networkStats().tcpDirect() == 1, "Authenticated TCP established");
                ua.dropAll = false; ub.dropAll = false;
                await(() -> ca.networkStats().udpDirect() == 1 && cb.networkStats().udpDirect() == 1, "UDP upgrades while TCP stays warm");
                check(((TcpTransport)field(ca, "tcp")).ready(cb.sessionId()), "TCP reserve remains authenticated");
                ua.failDirectVoice = true;
                send(ca, b, sb, 0, "Immediate authenticated TCP fallback after direct UDP send error");
                check(ua.directFailures == 1 && ca.networkStats().directUdpVoicePackets() == 0
                        && ca.networkStats().directTcpVoicePackets() == 1 && ca.networkStats().relayedVoicePackets() == 0,
                        "Accepted TCP fallback must be counted without a false UDP success");
                check(voice(relay) == 0, "Available direct TCP must precede VPS media");
            }
        }
    }

    private static VoiceClient client(URI uri, UUID id, String name, Sink sink, DatagramSocket socket, boolean automatic) throws Exception {
        return new VoiceClient(uri, null, new VoiceClient.Identity(id, name, id, name, "d".repeat(64), "minecraft:overworld"),
                true, true, challenge -> CompletableFuture.completedFuture(false), sink, List.of(), socket, automatic);
    }
    private static void connect(VoiceClient a, VoiceClient b, UUID pa, UUID pb) throws Exception {
        a.connect().get(5, TimeUnit.SECONDS); b.connect().get(5, TimeUnit.SECONDS);
        a.nearby(Set.of(pb)); b.nearby(Set.of(pa));
        await(() -> a.hasUdp() && b.hasUdp() && a.networkStats().udpDirect() == 1 && b.networkStats().udpDirect() == 1,
                "Direct UDP consent and authenticated relay return path");
    }
    private static void send(VoiceClient a, UUID recipient, Sink sink, long sequence, String label) throws Exception {
        byte[] payload = {9, (byte)sequence};
        a.sendAudio(Set.of(recipient), sequence, 16, payload, false);
        check(Arrays.equals(sink.audio.poll(3, TimeUnit.SECONDS), payload), label);
        check(sink.audio.poll(100, TimeUnit.MILLISECONDS) == null, label + " produced duplicate audio");
    }
    private static Object peer(VoiceClient client) throws ReflectiveOperationException {
        return ((Map<?, ?>)field(client, "peers")).values().iterator().next();
    }
    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        var field = owner.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(owner);
    }
    private static long count(RelayServer relay, String key) { return relay.health().get(key).getAsLong(); }
    private static long voice(RelayServer relay) { return count(relay, "relayedUdpVoicePackets") + count(relay, "relayedWssVoicePackets"); }
    private static void await(BooleanSupplier condition, String label) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(10);
        check(condition.getAsBoolean(), label);
    }
    private static void check(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
}
