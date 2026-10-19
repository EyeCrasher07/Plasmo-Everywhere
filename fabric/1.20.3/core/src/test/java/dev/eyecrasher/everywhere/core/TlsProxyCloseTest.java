package dev.eyecrasher.everywhere.core;

import java.io.IOException;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;

/** Concurrent close must finish resource cleanup before either caller returns. */
public final class TlsProxyCloseTest {
    private static final class HeldSocket extends Socket {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        @Override public void close() throws IOException {
            entered.countDown();
            try { if (!release.await(3, TimeUnit.SECONDS)) throw new IOException("Fixture release timeout"); }
            catch (InterruptedException stopped) { Thread.currentThread().interrupt(); throw new IOException(stopped); }
            super.close();
        }
    }
    public static void main(String[] args) throws Exception { verify(); }
    static void verify() throws Exception {
        try (TlsFragmentProxy proxy = new TlsFragmentProxy(URI.create("https://example.test/route"))) {
            var field = TlsFragmentProxy.class.getDeclaredField("sockets"); field.setAccessible(true);
            @SuppressWarnings("unchecked") Set<Socket> sockets = (Set<Socket>)field.get(proxy);
            HeldSocket held = new HeldSocket(); sockets.add(held);
            CountDownLatch secondStarted = new CountDownLatch(1), secondClosed = new CountDownLatch(1);
            Thread first = RuntimeCompat.start("everywhere-close-first", proxy::close);
            Thread second = null;
            try {
                check(held.entered.await(2, TimeUnit.SECONDS), "First close reached resource cleanup");
                second = RuntimeCompat.start("everywhere-close-second", () -> {
                    secondStarted.countDown(); proxy.close(); secondClosed.countDown();
                });
                check(secondStarted.await(2, TimeUnit.SECONDS), "Second close entered");
                check(!secondClosed.await(150, TimeUnit.MILLISECONDS), "Concurrent close returned before resource cleanup completed");
            } finally {
                held.release.countDown(); first.join(2000); if (second != null) second.join(2000);
            }
            check(!first.isAlive() && second != null && !second.isAlive() && held.isClosed(), "Both closes completed cleanup");
        }
        System.out.println("TLS proxy: concurrent close waits for complete owned-resource cleanup");
    }
    private static void check(boolean condition, String label) { if (!condition) throw new AssertionError(label); }
}
