package dev.eyecrasher.everywhere.forge;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Owns an uninstalled connection until the game thread accepts or cancels it. */
final class ConnectionPreparation<T> {
    private final ThreadPoolExecutor worker;
    private final Consumer<T> discard;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final FutureTask<Void> task;
    private T value;
    private boolean cancelled, claimed;

    ConnectionPreparation(ThreadPoolExecutor worker, Callable<T> prepare, Consumer<T> discard) {
        this.worker = Objects.requireNonNull(worker);
        this.discard = Objects.requireNonNull(discard);
        task = new FutureTask<>(() -> {
            try {
                T prepared = Objects.requireNonNull(prepare.call());
                boolean abandoned;
                synchronized (this) {
                    abandoned = cancelled;
                    if (!abandoned) value = prepared;
                }
                if (abandoned) discard.accept(prepared);
                else completion.complete(null);
            } catch (Throwable error) {
                completion.completeExceptionally(error);
            }
            return null;
        });
        try { worker.execute(task); }
        catch (RejectedExecutionException error) { completion.completeExceptionally(error); }
    }
    CompletableFuture<Void> completion() { return completion; }
    synchronized T claim() {
        if (cancelled || claimed || !completion.isDone() || completion.isCompletedExceptionally()) return null;
        claimed = true;
        T accepted = value; value = null;
        return accepted;
    }
    void cancel() {
        T abandoned;
        synchronized (this) {
            if (cancelled || claimed) return;
            cancelled = true;
            abandoned = value; value = null;
        }
        // Do not interrupt a future attempt or close resources on the game thread.
        task.cancel(true); worker.remove(task);
        completion.cancel(false);
        if (abandoned != null) discard.accept(abandoned);
    }
}
