package dev.eyecrasher.everywhere.fabric;

import java.util.concurrent.ThreadLocalRandom;

/** Bounded backoff avoids every client reconnecting on the same tick after an outage. */
final class RetryDelay {
    private RetryDelay() {}
    static int ticks(int attempt) { return ticks(attempt, ThreadLocalRandom.current().nextDouble()); }
    static int ticks(int attempt, double random) {
        int base = Math.min(20 << Math.min(6, Math.max(1, attempt)), 600);
        double unit = Math.min(Math.nextDown(1.0), Math.max(0, random));
        return Math.max(20, (int) Math.round(base * (0.75 + 0.5 * unit)));
    }
}
