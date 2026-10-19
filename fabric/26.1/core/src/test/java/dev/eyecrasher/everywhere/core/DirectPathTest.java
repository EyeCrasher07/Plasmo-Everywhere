package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static dev.eyecrasher.everywhere.core.ProtocolTest.check;

/** Deterministic route races and outages, using a monotonic clock and delayed replies. */
final class DirectPathTest {
    private record Reply(long at, InetSocketAddress source, byte[] nonce) {}
    private static final class Network {
        final DirectPath path = new DirectPath();
        final Map<InetSocketAddress,Long> delays = new HashMap<>();
        final PriorityQueue<Reply> replies = new PriorityQueue<>(Comparator.comparingLong(Reply::at));
        long now = TimeUnit.SECONDS.toNanos(100);
        final long step = millis(20);
        void announce(InetSocketAddress... endpoints) { path.candidates(List.of(endpoints), now); }
        void answer(InetSocketAddress endpoint, long milliseconds) { delays.put(endpoint, millis(milliseconds)); }
        void block(InetSocketAddress endpoint) { delays.remove(endpoint); replies.removeIf(reply -> reply.source.equals(endpoint)); }
        void run(long milliseconds) { run(milliseconds, () -> {}); }
        void run(long milliseconds, Runnable observation) {
            long end = now + millis(milliseconds);
            while (now < end) {
                while (!replies.isEmpty() && replies.peek().at <= now) {
                    Reply reply = replies.remove(); path.pong(reply.nonce, reply.source, now);
                }
                var probe = path.probe(now, () -> true);
                if (probe != null && delays.containsKey(probe.target()))
                    replies.add(new Reply(now + delays.get(probe.target()), probe.target(), probe.nonce()));
                observation.run(); now += step;
            }
        }
    }

    static void verify() throws Exception {
        lateCandidateAndReplies(); warmBackup(); failedLan(); rebindingAtCapacity(); diagnosticsAndBudget(); steadyBudget();
        System.out.println("Direct paths: late route upgrade, stable RTT selection, warm failover, safe LAN discovery, full-list port rebinding and bounded checks passed");
    }

    private static void lateCandidateAndReplies() throws Exception {
        var slow = endpoint("8.8.8.8", 10000); var fast = endpoint("1.1.1.1", 10001);
        Network network = new Network(); network.answer(slow, 80); network.announce(slow); network.run(1000);
        check(slow.equals(network.path.selected(network.now)), "Initial direct path established");
        network.answer(slow, 2000); network.run(20); // A reply to this heartbeat arrives much later.
        network.answer(fast, 20); network.announce(slow, fast);
        check(slow.equals(network.path.selected(network.now)), "An untested address does not displace a working path");
        network.run(600);
        check(fast.equals(network.path.selected(network.now)), "A better late candidate is checked despite active P2P");
        network.run(5000, () -> check(fast.equals(network.path.selected(network.now)), "Late or slower pongs do not steal audio from the better path"));
    }

    private static void warmBackup() throws Exception {
        var primary = endpoint("8.8.8.8", 11000); var reserve = endpoint("1.1.1.1", 11001);
        Network network = new Network(); network.answer(primary, 20); network.answer(reserve, 80);
        network.announce(primary, reserve); network.run(6000);
        check(network.path.diagnostics(network.now).confirmed() == 2, "Alternative direct path remains confirmed");
        network.block(primary); long failedAt = network.now;
        long[] switched = {-1};
        network.run(5000, () -> {
            var selected = network.path.selected(network.now);
            check(selected != null, "Warm alternative avoids a relay gap when the primary stops replying");
            if (reserve.equals(selected) && switched[0] < 0) switched[0] = network.now;
        });
        check(switched[0] >= failedAt && switched[0] - failedAt <= millis(1600), "Responsive backup replaces the stalled path within 1.6 seconds");
        check(reserve.equals(network.path.selected(network.now)), "Failed primary does not reclaim selection using stale consent");
        network.answer(primary, 20); network.run(5000);
        check(primary.equals(network.path.selected(network.now)), "Recovered better direct path is selected without rejoining");
        network.block(primary); network.block(reserve); network.run(4000);
        check(network.path.selected(network.now) == null, "All direct routes expire when both endpoints stop responding");
    }

    private static void failedLan() throws Exception {
        var internet = endpoint("8.8.8.8", 12000); var lan = endpoint("192.168.10.2", 12001);
        Network network = new Network(); network.answer(internet, 40); network.announce(internet); network.run(1000);
        network.path.host(lan, network.now);
        check(internet.equals(network.path.selected(network.now)), "LAN discovery alone preserves proven public P2P");
        network.run(5000, () -> check(internet.equals(network.path.selected(network.now)), "Blocked LAN unicast does not cause relay fallback"));
        network.answer(lan, 20); network.run(4000);
        check(lan.equals(network.path.selected(network.now)), "LAN can replace public P2P after its own consent check");
        network.path.host(endpoint("192.168.10.3", 12002), network.now);
        network.path.host(endpoint("192.168.10.4", 12003), network.now + 1);
        check(lan.equals(network.path.selected(network.now + 2)), "A third LAN address does not evict the working local path");
        network.run(24000);
        check(internet.equals(network.path.selected(network.now)), "Expired LAN discovery returns to a confirmed public path");
    }

    private static void rebindingAtCapacity() throws Exception {
        var old = endpoint("8.8.8.8", 13000); var rebound = endpoint("8.8.8.8", 13001);
        Network network = new Network(); network.announce(old);
        network.path.authenticatedPing(rebound, network.now); network.answer(rebound, 20); network.run(600);
        check(rebound.equals(network.path.selected(network.now)), "Initial reflexive port is confirmed");
        network.announce(old, endpoint("8.8.8.8", 13002), endpoint("8.8.8.8", 13003),
                endpoint("8.8.8.8", 13004), endpoint("8.8.8.8", 13005));
        var newest = endpoint("8.8.8.8", 14000); network.path.authenticatedPing(newest, network.now);
        check(network.path.count() <= 6, "Fresh port retains candidate bound");
        DirectPath.Probe probe = null;
        for (int n = 0; n < 3; n++) {
            var next = network.path.probe(network.now + n, () -> true);
            if (next != null && next.target().equals(newest)) { probe = next; break; }
        }
        check(probe != null, "Fresh reflexive port gets a consent check even with five advertised ports and an active learned port");
        check(rebound.equals(network.path.selected(network.now + 4)), "New reflexive port is not selected before its own response");
        network.path.pong(probe.nonce(), newest, network.now + millis(20));
        check(network.path.diagnostics(network.now + millis(21)).confirmed() >= 2, "New reflexive endpoint receives independent consent");
        check(!network.path.permits(endpoint("9.9.9.9", 14000)), "Port rebinding does not authorize an unrelated IP");
    }

    private static void diagnosticsAndBudget() throws Exception {
        var address = endpoint("8.8.8.8", 15000); Network network = new Network(); network.announce(address);
        for (int i = 0; i < 100; i++) check(network.path.probe(network.now + i, () -> false) == null, "No probe without global budget");
        check(network.path.checks() == 0, "Denied budget does not consume checks or postpone discovery");
        network.run(12000);
        var stats = network.path.diagnostics(network.now);
        check(stats.checks() > 0 && stats.replies() == 0 && stats.expiredChecks() > 0 && stats.rttMillis() == -1,
                "Diagnostics distinguish sent checks, no answers and expired checks without diagnosing a NAT type");
        check(!stats.toString().contains("8.8.8.8"), "Route diagnostics contain no network addresses");
    }

    private static void steadyBudget() throws Exception {
        List<Network> peers = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            Network network = new Network(); var a = endpoint("8.8.8.8", 16000 + i); var b = endpoint("1.1.1.1", 16000 + i);
            network.answer(a, 20); network.answer(b, 80); network.announce(a, b, endpoint("9.9.9.9", 16000 + i));
            network.run(6000); peers.add(network);
        }
        long before = peers.stream().mapToLong(network -> network.path.checks()).sum();
        for (var network : peers) network.run(20000);
        long checks = peers.stream().mapToLong(network -> network.path.checks()).sum() - before;
        check(checks <= 20 * 60, "24 active peers with backups and exploration fit below the existing 64 checks/second budget in steady state");
    }

    private static InetSocketAddress endpoint(String ip, int port) throws Exception { return new InetSocketAddress(InetAddress.getByName(ip), port); }
    private static long millis(long value) { return TimeUnit.MILLISECONDS.toNanos(value); }
}
