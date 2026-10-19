package dev.eyecrasher.everywhere.core;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Per-peer connectivity checks; called only after signaling / packet authentication. */
final class DirectPath {
    record Probe(InetSocketAddress target, byte[] nonce) {}
    record Diagnostics(int candidates, int confirmed, long checks, long replies, long expiredChecks, int rttMillis) {}
    private record Pending(InetSocketAddress target, long at) {}
    private static final long CONSENT = TimeUnit.SECONDS.toNanos(3);
    private static final long CHECK_TIMEOUT = TimeUnit.SECONDS.toNanos(8);
    private static final long HEARTBEAT = TimeUnit.SECONDS.toNanos(1);
    private static final long BACKUP_HEARTBEAT = TimeUnit.MILLISECONDS.toNanos(1250);
    private static final long EXPLORATION = TimeUnit.SECONDS.toNanos(2);

    private static final class Candidate {
        long checkedAt, pongAt, rtt;
        boolean checked, confirmed;
        boolean fresh(long now) { return confirmed && now - pongAt >= 0 && now - pongAt < CONSENT; }
        boolean due(long now, long interval) { return !checked || now - checkedAt >= interval; }
    }

    private final Map<InetSocketAddress,Candidate> candidates = new LinkedHashMap<>();
    private final Map<String,Pending> pending = new LinkedHashMap<>();
    private final Map<InetSocketAddress,Long> hosts = new LinkedHashMap<>();
    private Set<InetSocketAddress> announced = Set.of();
    private InetSocketAddress selected, learned;
    private long startedAt, checks, replies, expiredChecks, exploredAt, evaluatedAt;
    private boolean explored, evaluated;

    synchronized void candidates(Collection<InetSocketAddress> endpoints, long now) {
        Set<InetSocketAddress> next = new LinkedHashSet<>(endpoints);
        if (next.size() > 5) throw new IllegalArgumentException("Candidate limit");
        if (next.equals(announced)) return;
        announced = Set.copyOf(next); startedAt = now; explored = false; evaluated = false;
        if (learned != null && !permits(learned, now)) learned = null;
        if (selected != null && !permits(selected, now)) selected = null;
        if (selected != null && !announced.contains(selected)) learned = selected;
        candidates.keySet().removeIf(e -> !next.contains(e) && !hosts.containsKey(e) && !e.equals(learned));
        next.forEach(e -> candidates.computeIfAbsent(e, ignored -> new Candidate()));
        pending.entrySet().removeIf(e -> !permits(e.getValue().target, now));
        trim();
    }

    synchronized boolean permits(InetSocketAddress source) { return permits(source, System.nanoTime()); }
    private boolean permits(InetSocketAddress source, long now) {
        return source != null && (announced.stream().anyMatch(e -> e.getAddress().equals(source.getAddress()))
                || hosts.entrySet().stream().anyMatch(e -> e.getKey().getAddress().equals(source.getAddress())
                && now - e.getValue() >= 0 && now - e.getValue() < TimeUnit.SECONDS.toNanos(30)));
    }

    // Only called after a pair-encrypted beacon was received on a directly attached private subnet.
    synchronized void host(InetSocketAddress endpoint, long now) {
        if (!HostNetworks.privateAddress(endpoint.getAddress()) || endpoint.getPort() < 1) return;
        boolean added = !hosts.containsKey(endpoint);
        if (added && hosts.size() >= 2) {
            var oldest = hosts.entrySet().stream().filter(e -> !e.getKey().equals(selected))
                    .min(Comparator.comparingInt((Map.Entry<InetSocketAddress,Long> e) -> selected != null
                            && e.getKey().getAddress().equals(selected.getAddress()) ? 1 : 0)
                            .thenComparingLong(Map.Entry::getValue)).orElseThrow().getKey();
            hosts.remove(oldest); candidates.remove(oldest);
        }
        hosts.put(endpoint, now); candidates.computeIfAbsent(endpoint, ignored -> new Candidate());
        evaluated = false;
        // Discovery is not consent: keep the public path while checking the LAN address.
        if (added) { startedAt = now; explored = false; }
        trim();
    }

    synchronized void authenticatedPing(InetSocketAddress source, long now) { learn(source, now); }
    private void learn(InetSocketAddress source, long now) {
        if (!permits(source, now) || candidates.containsKey(source)) return;
        selected(now);
        if (learned != null && !announced.contains(learned) && !learned.equals(selected)) candidates.remove(learned);
        learned = source; candidates.put(source, new Candidate()); explored = false; evaluated = false; trim();
    }

    private void trim() {
        candidates.keySet().removeIf(e -> !announced.contains(e) && !hosts.containsKey(e) && !e.equals(learned) && !e.equals(selected));
        // A fresh peer-reflexive port must still get checked when advertised slots are full.
        while (candidates.size() > 6 + hosts.size()) {
            InetSocketAddress victim = null;
            for (var entry : candidates.entrySet()) {
                var endpoint = entry.getKey();
                if (endpoint.equals(selected) || endpoint.equals(learned) || hosts.containsKey(endpoint)) continue;
                if (victim == null || !entry.getValue().confirmed) victim = endpoint;
            }
            if (victim == null) break;
            candidates.remove(victim);
        }
    }

    synchronized void pong(byte[] nonce, InetSocketAddress source, long now) {
        if (nonce.length != 16 || !permits(source, now)) return;
        Pending check = pending.get(Wire.base64(nonce));
        if (check == null || now - check.at < 0 || now - check.at > CHECK_TIMEOUT
                || !check.target.getAddress().equals(source.getAddress())) return;
        pending.remove(Wire.base64(nonce));
        if (!check.target.equals(source)) { learn(source, now); return; }
        Candidate path = candidates.get(source);
        if (path == null) return;
        long sample = now - check.at;
        path.rtt = path.confirmed ? (path.rtt * 3 + sample) / 4 : sample;
        path.confirmed = true; path.pongAt = now; replies++;
        evaluated = false;
        if (!announced.contains(source) && !hosts.containsKey(source)) learned = source;
        selected(now); trim();
    }

    synchronized InetSocketAddress selected(long now) {
        Candidate current = candidates.get(selected);
        // Audio packets share the latest network-tick result; avoid rescanning pending checks per frame.
        if (evaluated && now - evaluatedAt >= 0 && now - evaluatedAt < TimeUnit.MILLISECONDS.toNanos(50)
                && (selected == null || current != null && current.fresh(now))) return selected;
        evaluated = true; evaluatedAt = now;
        if (current == null || !current.fresh(now) || !permits(selected, now)) selected = null;
        boolean currentUnresponsive = selected != null && unresponsive(selected, candidates.get(selected), now);
        for (var entry : candidates.entrySet()) {
            if (!entry.getValue().fresh(now) || !permits(entry.getKey(), now)) continue;
            if (unresponsive(entry.getKey(), entry.getValue(), now)) continue;
            if (selected == null || currentUnresponsive || improves(entry.getKey(), entry.getValue(), selected, candidates.get(selected))) {
                selected = entry.getKey();
                currentUnresponsive = false;
            }
        }
        return selected;
    }

    private boolean unresponsive(InetSocketAddress endpoint, Candidate path, long now) {
        long timeout = Math.max(TimeUnit.MILLISECONDS.toNanos(300), Math.min(TimeUnit.MILLISECONDS.toNanos(1500), path.rtt * 4));
        return pending.values().stream().anyMatch(check -> check.target.equals(endpoint)
                && check.at > path.pongAt && now - check.at >= timeout);
    }

    private static boolean improves(InetSocketAddress next, Candidate nextPath, InetSocketAddress current, Candidate currentPath) {
        if (next.equals(current)) return false;
        boolean nextLan = HostNetworks.privateAddress(next.getAddress()), currentLan = HostNetworks.privateAddress(current.getAddress());
        if (nextLan && !currentLan && nextPath.rtt <= currentPath.rtt + TimeUnit.MILLISECONDS.toNanos(5)) return true;
        // A small jitter change or late reply must not keep moving audio between paths.
        long margin = Math.max(TimeUnit.MILLISECONDS.toNanos(15), currentPath.rtt / 5);
        if (!nextLan && currentLan) margin = Math.max(margin, TimeUnit.MILLISECONDS.toNanos(25));
        return nextPath.rtt + margin < currentPath.rtt;
    }

    private InetSocketAddress backup(InetSocketAddress active, long now) {
        InetSocketAddress result = null;
        for (var entry : candidates.entrySet()) {
            if (entry.getKey().equals(active) || !entry.getValue().fresh(now) || !permits(entry.getKey(), now)) continue;
            if (result == null || entry.getValue().rtt < candidates.get(result).rtt) result = entry.getKey();
        }
        return result;
    }

    synchronized Probe probe(long now, BooleanSupplier budget) {
        hosts.entrySet().removeIf(e -> now - e.getValue() >= TimeUnit.SECONDS.toNanos(30));
        var iterator = pending.values().iterator();
        while (iterator.hasNext()) if (now - iterator.next().at > CHECK_TIMEOUT) { iterator.remove(); expiredChecks++; }
        candidates.keySet().removeIf(e -> !permits(e, now));
        evaluated = false;
        InetSocketAddress active = selected(now); trim();
        InetSocketAddress target = null;
        boolean exploration = false;
        if (active != null) {
            InetSocketAddress reserve = backup(active, now);
            if (candidates.get(active).due(now, HEARTBEAT)) target = active;
            else if (reserve != null && candidates.get(reserve).due(now, BACKUP_HEARTBEAT)) target = reserve;
            else if (!explored || now - exploredAt >= EXPLORATION) {
                target = oldest(active, reserve, now, EXPLORATION);
                exploration = target != null;
            }
        } else {
            long interval = now - startedAt < CHECK_TIMEOUT ? TimeUnit.MILLISECONDS.toNanos(400) : EXPLORATION;
            target = oldest(null, null, now, interval);
        }
        if (target == null || !budget.getAsBoolean()) return null;
        Candidate path = candidates.get(target); path.checked = true; path.checkedAt = now;
        if (exploration) { explored = true; exploredAt = now; }
        byte[] nonce = Wire.random(16);
        if (pending.size() >= 32) pending.remove(pending.keySet().iterator().next());
        pending.put(Wire.base64(nonce), new Pending(target, now)); checks++;
        return new Probe(target, nonce);
    }

    private InetSocketAddress oldest(InetSocketAddress except, InetSocketAddress backup, long now, long interval) {
        Candidate reflexive = candidates.get(learned);
        if (reflexive != null && !reflexive.checked && !learned.equals(except) && !learned.equals(backup)) return learned;
        InetSocketAddress result = null;
        for (var entry : candidates.entrySet()) {
            if (entry.getKey().equals(except) || entry.getKey().equals(backup) || !entry.getValue().due(now, interval)) continue;
            Candidate previous = candidates.get(result), candidate = entry.getValue();
            if (result == null || previous.checked && (!candidate.checked || candidate.checkedAt < previous.checkedAt)) result = entry.getKey();
        }
        return result;
    }

    synchronized Diagnostics diagnostics(long now) {
        InetSocketAddress active = selected(now);
        int confirmed = (int)candidates.values().stream().filter(c -> c.fresh(now)).count();
        return new Diagnostics(candidates.size(), confirmed, checks, replies, expiredChecks,
                active == null ? -1 : (int)TimeUnit.NANOSECONDS.toMillis(candidates.get(active).rtt));
    }
    synchronized int count() { return candidates.size(); }
    synchronized long checks() { return checks; }
}
