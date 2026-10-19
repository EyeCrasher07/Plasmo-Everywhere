package dev.eyecrasher.everywhere.core;

import java.lang.reflect.*;
import java.net.http.HttpClient;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Optional newer-JDK facilities without linking the Java 17 client to their classes. */
final class RuntimeCompat {
    private record VirtualThreads(Method builder, Method name, Method unstarted) {}
    private static final VirtualThreads VIRTUAL = virtualThreads();
    private static final Method HTTP_SHUTDOWN = method(HttpClient.class, "shutdownNow");
    private RuntimeCompat() {}

    private static VirtualThreads virtualThreads() {
        try {
            Class<?> builder = Class.forName("java.lang.Thread$Builder");
            return new VirtualThreads(Thread.class.getMethod("ofVirtual"), builder.getMethod("name", String.class),
                    builder.getMethod("unstarted", Runnable.class));
        } catch (ReflectiveOperationException | SecurityException unavailable) { return null; }
    }
    private static Method method(Class<?> owner, String name) {
        try { return owner.getMethod(name); }
        catch (NoSuchMethodException | SecurityException unavailable) { return null; }
    }
    private static Thread thread(String name, Runnable task) {
        if (VIRTUAL != null) try {
            Object builder = VIRTUAL.builder.invoke(null);
            VIRTUAL.name.invoke(builder, name);
            return (Thread)VIRTUAL.unstarted.invoke(builder, task);
        } catch (InvocationTargetException failure) { throw propagate(failure.getCause()); }
        catch (IllegalAccessException unavailable) { /* A restricted runtime can use daemon platform threads. */ }
        Thread thread = new Thread(task, name); thread.setDaemon(true); return thread;
    }
    static Thread start(String name, Runnable task) {
        Thread thread = thread(name, task); thread.start(); return thread;
    }
    static ThreadFactory factory(String prefix) {
        AtomicInteger next = new AtomicInteger();
        return task -> thread(prefix + "-" + next.incrementAndGet(), task);
    }
    static ExecutorService workers(String prefix, int maximum) {
        return new ThreadPoolExecutor(0, maximum, 30, TimeUnit.SECONDS, new SynchronousQueue<>(), factory(prefix));
    }
    /** Java 17 has no public shutdown API: owners must cancel outstanding requests and abort sockets. */
    static boolean shutdown(HttpClient client) {
        if (HTTP_SHUTDOWN == null) return false;
        try { HTTP_SHUTDOWN.invoke(client); return true; }
        catch (InvocationTargetException failure) { throw propagate(failure.getCause()); }
        catch (IllegalAccessException unavailable) { return false; }
    }
    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof Error error) throw error;
        return failure instanceof RuntimeException runtime ? runtime : new IllegalStateException(failure);
    }
}
