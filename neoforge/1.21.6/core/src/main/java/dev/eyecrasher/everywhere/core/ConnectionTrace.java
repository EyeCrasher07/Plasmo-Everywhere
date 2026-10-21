package dev.eyecrasher.everywhere.core;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.channels.UnresolvedAddressException;
import java.security.cert.CertificateException;
import java.security.cert.CertPathValidatorException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

/** Bounded connection history. Only fixed labels, timings and numeric codes may leave this class. */
public final class ConnectionTrace {
    public enum Stage {
        UNKNOWN, SESSION, IDENTITY, ACCOUNT, RETRY, LIFECYCLE, AUDIO, REPORT, AUTH_ENVIRONMENT,
        CONFIG, GAME_CONNECTION, NATIVE_VOICE, DIRECTORY, NODE_SELECTION, NODE_PROBE,
        DIRECTORY_1, DIRECTORY_2, DIRECTORY_3, DIRECTORY_4,
        DIRECTORY_1_HTTP, DIRECTORY_2_HTTP, DIRECTORY_3_HTTP, DIRECTORY_4_HTTP,
        DIRECTORY_1_TLS, DIRECTORY_2_TLS, DIRECTORY_3_TLS, DIRECTORY_4_TLS,
        CONTROL_TLS, CONTROL_CONNECT, CONTROL_OPEN, CHALLENGE, AUTHENTICATION, JOIN_SENT,
        WELCOME, CONTROL_CLOSE, CONTROL_ERROR, UDP_START, UDP_BOUND, TRANSPORTS,
        TCP_LISTENER, TCP_SHARED_PORT, LAN_DISCOVERY, ROUTER_MAPPING, TCP_DISCOVERY, DISCOVERY,
        NETWORK_ERROR, UDP_READ, UDP_SEND, NETWORK_TICK, SESSION_CLOSED, READY
    }
    public enum Result {
        UNKNOWN, START, SUCCESS, FAILED, SKIPPED, UNVERIFIED, VERIFIED, FALLBACK, CANCELLED,
        CLOSED, READY, RETAINED, OFFICIAL, ALTERNATE, RECOVERY_START, RECOVERED,
        TIMEOUT, REJECTED, PENDING, SENT, RECEIVED, ENABLED, DISABLED, UNAVAILABLE,
        CONNECTION_LIMIT, INVALID_MESSAGE, SESSION_EXISTS, CAPACITY_REACHED, VERIFIED_RESERVED,
        VERIFIED_RECLAIMED, ACCOUNT_REJECTED, JOIN_TIMEOUT, SLOW_CONNECTION,
        SESSION_REPLACED, HEARTBEAT_TIMEOUT, REFRESH, OTHER
    }
    public record Event(long elapsedMillis, int attempt, String stage, String result,
                        long durationMillis, String error, int code) {}
    public static final int CAPACITY = 256;
    private final long started = System.nanoTime();
    private final ArrayDeque<Event> events = new ArrayDeque<>(CAPACITY);
    private final Consumer<Event> sink;
    private int attempts;
    private long dropped;

    public ConnectionTrace() { this(event -> {}); }
    public ConnectionTrace(Consumer<Event> sink) { this.sink = Objects.requireNonNull(sink); }
    public synchronized int startAttempt() {
        if (attempts < Integer.MAX_VALUE) attempts++;
        return attempts;
    }
    public void record(String stage, String result, long durationMillis, Throwable error, int code) {
        record(label(Stage.class, stage), label(Result.class, result), durationMillis, error, code);
    }
    public void record(Stage stage, Result result, long durationMillis, Throwable error, int code) {
        recordForAttempt(attempts(), stage, result, durationMillis, error, code);
    }
    public void recordForAttempt(int attempt, String stage, String result, long durationMillis, Throwable error, int code) {
        recordForAttempt(attempt, label(Stage.class, stage), label(Result.class, result), durationMillis, error, code);
    }
    public void recordForAttempt(int attempt, Stage stage, Result result, long durationMillis, Throwable error, int code) {
        Event event;
        synchronized (this) {
            event = new Event(Math.max(0, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)), Math.max(0, Math.min(attempts, attempt)),
                    (stage == null ? Stage.UNKNOWN : stage).name(), (result == null ? Result.UNKNOWN : result).name(),
                    Math.max(-1, Math.min(TimeUnit.DAYS.toMillis(7), durationMillis)), category(error),
                    code >= -1 && code <= 65535 ? code : -1);
            if (events.size() == CAPACITY) { events.removeFirst(); if (dropped < Long.MAX_VALUE) dropped++; }
            events.addLast(event);
        }
        // Never call external logging code while holding the history lock or pass it a Throwable.
        try { sink.accept(event); } catch (Throwable ignored) { /* Diagnostics cannot break a connection. */ }
    }
    public synchronized List<Event> snapshot() { return List.copyOf(events); }
    public synchronized long dropped() { return dropped; }
    public synchronized int attempts() { return attempts; }
    private static <T extends Enum<T>> T label(Class<T> type, String value) {
        if (value != null && value.length() <= 32) try { return Enum.valueOf(type, value); }
        catch (IllegalArgumentException ignored) {}
        return Enum.valueOf(type, "UNKNOWN");
    }
    /** Never reads messages, stack traces or suppressed errors; known authlib types map to fixed labels. */
    static String category(Throwable error) {
        if (error == null) return "";
        String fallback = "UNEXPECTED";
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int depth = 0; error != null && depth < 8 && seen.add(error); depth++) {
            String authentication = switch (error.getClass().getName()) {
                case "com.mojang.authlib.exceptions.AuthenticationException" -> "AUTHENTICATION";
                case "com.mojang.authlib.exceptions.InvalidCredentialsException", "com.mojang.authlib.exceptions.UserMigratedException" -> "INVALID_CREDENTIALS";
                case "com.mojang.authlib.exceptions.AuthenticationUnavailableException" -> "AUTH_UNAVAILABLE";
                case "com.mojang.authlib.minecraft.client.MinecraftClientHttpException", "com.mojang.authlib.minecraft.client.MinecraftClientException" -> "AUTH_HTTP";
                default -> "";
            };
            if (!authentication.isEmpty()) return authentication;
            if (error instanceof CertificateException || error instanceof CertPathValidatorException
                    || error instanceof SSLPeerUnverifiedException) return "TLS_CERTIFICATE";
            if (error instanceof UnknownHostException || error instanceof UnresolvedAddressException) return "DNS";
            if (error instanceof BindException) return "BIND";
            if (error instanceof HttpConnectTimeoutException) return "CONNECT_TIMEOUT";
            if (error instanceof HttpTimeoutException || error instanceof SocketTimeoutException || error instanceof TimeoutException) return "TIMEOUT";
            if (error instanceof ConnectException) return "CONNECT";
            if (error instanceof NoRouteToHostException) return "NO_ROUTE";
            if (error instanceof WebSocketHandshakeException) return "HTTP_UPGRADE";
            if (error instanceof CancellationException) return "CANCELLED";
            if (error instanceof InterruptedException) return "INTERRUPTED";
            if (error instanceof SSLException) fallback = "TLS";
            else if (error instanceof IOException && !fallback.equals("TLS")) fallback = "IO";
            else if (error instanceof IllegalArgumentException && fallback.equals("UNEXPECTED")) fallback = "INVALID_DATA";
            else if (error instanceof SecurityException && fallback.equals("UNEXPECTED")) fallback = "SECURITY";
            try { error = error.getCause(); } catch (Throwable ignored) { break; }
        }
        return fallback;
    }
    static int httpStatus(Throwable error) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int depth = 0; error != null && depth < 8 && seen.add(error); depth++) {
            if (error instanceof WebSocketHandshakeException upgrade) {
                int code = upgrade.getResponse().statusCode();
                return code >= 100 && code <= 599 ? code : -1;
            }
            try { error = error.getCause(); } catch (Throwable ignored) { break; }
        }
        return -1;
    }
}
