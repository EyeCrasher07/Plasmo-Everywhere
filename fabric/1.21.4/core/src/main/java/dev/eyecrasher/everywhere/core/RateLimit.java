package dev.eyecrasher.everywhere.core;

public final class RateLimit {
    private final double rate, burst;
    private double tokens;
    private long last = System.nanoTime();
    public RateLimit(double rate, double burst) { this.rate = rate; this.burst = burst; tokens = burst; }
    public synchronized boolean take() {
        long now = System.nanoTime();
        tokens = Math.min(burst, tokens + (now - last) * rate / 1_000_000_000D); last = now;
        if (tokens < 1) return false;
        tokens--; return true;
    }
}
