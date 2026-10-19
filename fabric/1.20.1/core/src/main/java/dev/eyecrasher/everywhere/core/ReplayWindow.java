package dev.eyecrasher.everywhere.core;

/** A 64-packet sliding window, updated only AFTER authentication succeeds. */
public final class ReplayWindow {
    private long highest = -1, bitmap;
    public synchronized boolean accept(long sequence) {
        if (sequence < 0) return false;
        if (sequence > highest) {
            long delta = sequence - highest;
            bitmap = delta >= 64 ? 1 : (bitmap << delta) | 1;
            highest = sequence;
            return true;
        }
        long delta = highest - sequence;
        if (delta >= 64 || (bitmap & (1L << delta)) != 0) return false;
        bitmap |= 1L << delta;
        return true;
    }
}
