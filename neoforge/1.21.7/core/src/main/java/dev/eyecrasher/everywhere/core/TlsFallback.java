package dev.eyecrasher.everywhere.core;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.nio.channels.UnresolvedAddressException;
import java.security.cert.CertificateException;
import java.security.cert.CertPathValidatorException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import javax.net.ssl.SSLPeerUnverifiedException;

/** One bounded application-local retry; ordinary connections and OS network settings stay untouched. */
final class TlsFallback implements AutoCloseable {
    private final HttpClient ordinary;
    private final ConnectionTrace trace;
    private final ConnectionTrace.Stage traceStage;
    private final int traceAttempt;
    private boolean attempted, closed;
    private volatile boolean recovered;
    private HttpClient retry;
    private TlsFragmentProxy proxy;
    private final Set<CompletableFuture<?>> operations = ConcurrentHashMap.newKeySet();

    TlsFallback(HttpClient ordinary) { this(ordinary, new ConnectionTrace(), ConnectionTrace.Stage.CONTROL_TLS); }
    TlsFallback(HttpClient ordinary, ConnectionTrace trace, ConnectionTrace.Stage stage) {
        this(ordinary, trace, stage, trace.attempts());
    }
    TlsFallback(HttpClient ordinary, ConnectionTrace trace, ConnectionTrace.Stage stage, int attempt) {
        this.ordinary = ordinary; this.trace = trace; this.traceStage = stage; this.traceAttempt = attempt;
    }
    boolean recovered() { return recovered; }
    synchronized boolean attempted() { return attempted; }

    <T> CompletableFuture<HttpResponse<T>> get(HttpRequest request, HttpResponse.BodyHandler<T> body) {
        if (!request.method().equals("GET")) throw new IllegalArgumentException("Only idempotent service reads can retry");
        return run(request.uri(), () -> ordinary.sendAsync(request, body), client -> client.sendAsync(request, body));
    }
    CompletableFuture<WebSocket> connect(URI target, WebSocket.Listener listener) {
        Function<HttpClient, CompletableFuture<WebSocket>> operation = client -> {
            var builder = client.newWebSocketBuilder(); ordinary.connectTimeout().ifPresent(builder::connectTimeout);
            return builder.buildAsync(target, listener);
        };
        return run(target, () -> operation.apply(ordinary), operation);
    }
    private <T> CompletableFuture<T> run(URI target, Supplier<CompletableFuture<T>> first,
                                         Function<HttpClient, CompletableFuture<T>> second) {
        long initialStarted = System.nanoTime();
        trace.recordForAttempt(traceAttempt, traceStage, ConnectionTrace.Result.START, -1, null, 0);
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<T>> pending = new AtomicReference<>();
        synchronized (this) {
            if (closed) return CompletableFuture.failedFuture(new CancellationException("TLS operation owner closed"));
            operations.add(result);
        }
        result.whenComplete((value, error) -> {
            operations.remove(result);
            if (result.isCancelled()) { var operation = pending.get(); if (operation != null) operation.cancel(true); }
        });
        CompletableFuture<T> initial;
        try { initial = first.get(); } catch (RuntimeException error) {
            trace.recordForAttempt(traceAttempt, traceStage, ConnectionTrace.Result.FAILED, elapsed(initialStarted), error, status(error, 0));
            result.completeExceptionally(error); return result;
        }
        pending.set(initial);
        if (result.isCancelled()) initial.cancel(true);
        initial.whenComplete((value, error) -> {
            if (error == null) {
                trace.recordForAttempt(traceAttempt, traceStage, ConnectionTrace.Result.SUCCESS, elapsed(initialStarted), null, 0);
                result.complete(value); return;
            }
            trace.recordForAttempt(traceAttempt, traceStage, result.isCancelled() ? ConnectionTrace.Result.CANCELLED : ConnectionTrace.Result.FAILED,
                    elapsed(initialStarted), error, status(error, 0));
            if (result.isDone() || !retryable(error)) { result.completeExceptionally(error); return; }
            long recoveryStarted = System.nanoTime();
            try {
                HttpClient client = recovery(target);
                if (client == null) {
                    trace.recordForAttempt(traceAttempt, traceStage, ConnectionTrace.Result.SKIPPED, elapsed(recoveryStarted), null, 1);
                    result.completeExceptionally(error); return;
                }
                trace.recordForAttempt(traceAttempt, traceStage, ConnectionTrace.Result.RECOVERY_START, -1, null, 1);
                CompletableFuture<T> next = second.apply(client); pending.set(next);
                if (result.isCancelled()) next.cancel(true);
                next.whenComplete((replacement, failure) -> {
                    trace.recordForAttempt(traceAttempt, traceStage, failure == null ? ConnectionTrace.Result.RECOVERED
                                    : result.isCancelled() ? ConnectionTrace.Result.CANCELLED : ConnectionTrace.Result.FAILED,
                            elapsed(recoveryStarted), failure, status(failure, 1));
                    if (failure == null) { recovered = true; result.complete(replacement); }
                    else result.completeExceptionally(failure);
                });
            } catch (IOException | RuntimeException failure) {
                trace.recordForAttempt(traceAttempt, traceStage, ConnectionTrace.Result.FAILED, elapsed(recoveryStarted), failure, status(failure, 1));
                result.completeExceptionally(failure);
            }
        });
        return result;
    }
    private static int status(Throwable error, int fallback) { int code = ConnectionTrace.httpStatus(error); return code < 0 ? fallback : code; }
    private static long elapsed(long started) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started); }
    private synchronized HttpClient recovery(URI target) throws IOException {
        if (closed || attempted || !List.of("https", "wss").contains(target.getScheme()) || !directProxy(target)) return null;
        attempted = true;
        proxy = new TlsFragmentProxy(target);
        var builder = HttpClient.newBuilder().sslContext(ordinary.sslContext()).sslParameters(ordinary.sslParameters())
                .version(ordinary.version()).followRedirects(HttpClient.Redirect.NEVER).proxy(ProxySelector.of(proxy.address()));
        ordinary.connectTimeout().ifPresent(builder::connectTimeout);
        retry = builder.build(); return retry;
    }
    private boolean directProxy(URI target) {
        try {
            ProxySelector selector = ordinary.proxy().orElse(ProxySelector.getDefault());
            if (selector == null) return true;
            URI request = new URI("https", target.getRawAuthority(), "/", null, null);
            List<Proxy> selected = selector.select(request);
            return selected != null && !selected.isEmpty() && selected.stream().allMatch(p -> p.type() == Proxy.Type.DIRECT);
        } catch (URISyntaxException | RuntimeException ignored) { return false; }
    }
    static boolean retryable(Throwable error) {
        boolean network = false;
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof CertificateException || cause instanceof CertPathValidatorException
                    || cause instanceof SSLPeerUnverifiedException || cause instanceof WebSocketHandshakeException
                    || cause instanceof UnknownHostException || cause instanceof UnresolvedAddressException
                    || cause instanceof NoRouteToHostException || cause instanceof CancellationException) return false;
            if (cause instanceof IOException) network = true;
        }
        return network;
    }
    @Override public void close() {
        TlsFragmentProxy currentProxy;
        HttpClient currentRetry;
        List<CompletableFuture<?>> pending;
        synchronized (this) {
            if (closed) return;
            closed = true; currentProxy = proxy; currentRetry = retry; pending = List.copyOf(operations);
        }
        pending.forEach(operation -> operation.cancel(true));
        if (currentProxy != null) currentProxy.close();
        if (currentRetry != null) RuntimeCompat.shutdown(currentRetry);
    }
}
