package dev.eyecrasher.everywhere.core;

import java.io.IOException;
import java.net.BindException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

final class ConnectionTraceTest {
    static void verify() throws Exception {
        AtomicReference<ConnectionTrace> reference = new AtomicReference<>();
        List<ConnectionTrace.Event> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean outsideLock = new AtomicBoolean(true);
        ConnectionTrace trace = new ConnectionTrace(event -> {
            if (Thread.holdsLock(reference.get())) outsideLock.set(false);
            delivered.add(event);
        });
        reference.set(trace);
        int original = trace.startAttempt();
        String privateValue = "secret-token https://private.invalid/route?room=private-player 192.168.1.1";
        require(ConnectionTrace.category(new CompletionException(new BindException(privateValue))).equals("BIND"),
                "Local binding failures have a precise safe category");
        Throwable sensitive = new IOException(privateValue) {
            @Override public String getMessage() { throw new AssertionError("Message was inspected"); }
            @Override public String toString() { throw new AssertionError("Throwable was printed"); }
        };
        trace.record(privateValue, privateValue, Long.MAX_VALUE, sensitive, Integer.MAX_VALUE);
        var sanitized = trace.snapshot().getFirst();
        require(sanitized.stage().equals("UNKNOWN") && sanitized.result().equals("UNKNOWN")
                && sanitized.error().equals("IO") && sanitized.code() == -1, "Untrusted text never enters history");
        require(!sanitized.toString().contains("secret") && !sanitized.toString().contains("192.168"), "Serialized event has no private text");
        trace.startAttempt();
        trace.recordForAttempt(original, "DIRECTORY_1", "FAILED", 4,
                new CompletionException(new UnknownHostException(privateValue)), 0);
        require(trace.snapshot().getLast().attempt() == original && trace.snapshot().getLast().error().equals("DNS"),
                "Late asynchronous errors retain original attempt and safe cause category");
        Throwable badCause = new RuntimeException(privateValue) {
            @Override public synchronized Throwable getCause() { throw new AssertionError("Broken throwable"); }
        };
        trace.record("ACCOUNT", "FAILED", -4, badCause, 0);
        require(trace.snapshot().getLast().durationMillis() == -1 && trace.snapshot().getLast().error().equals("UNEXPECTED"),
                "Malformed exceptions do not break recording");
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> pending = new ArrayList<>();
            for (int n = 0; n < 1000; n++) pending.add(workers.submit(() -> trace.record("SESSION", "START", 0, null, 0)));
            for (Future<?> request : pending) request.get(5, TimeUnit.SECONDS);
        }
        require(trace.snapshot().size() == ConnectionTrace.CAPACITY && trace.dropped() == 1003 - ConnectionTrace.CAPACITY,
                "Concurrent event history is bounded and counts dropped records");
        require(delivered.size() == 1003 && outsideLock.get(), "Already sanitized sink events run outside trace lock");
        try { trace.snapshot().clear(); throw new AssertionError("Mutable snapshot"); }
        catch (UnsupportedOperationException expected) {}
        new ConnectionTrace(event -> { throw new AssertionError("Broken logger"); }).record("SESSION", "START", 0, null, 0);
        require(VoiceClient.closeResult("Account verification failed") == ConnectionTrace.Result.ACCOUNT_REJECTED
                        && VoiceClient.closeResult(privateValue) == ConnectionTrace.Result.OTHER,
                "Only known close reasons are retained");
        System.out.println("Connection trace: bounded concurrent history, immutable snapshots, attempt binding, safe categories and isolated sinks passed");
    }
    private static void require(boolean value, String label) { if (!value) throw new AssertionError(label); }
}
