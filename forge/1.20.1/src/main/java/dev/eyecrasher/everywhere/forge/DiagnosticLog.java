package dev.eyecrasher.everywhere.forge;

import dev.eyecrasher.everywhere.core.ConnectionTrace;
import dev.eyecrasher.everywhere.core.Wire;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Only sanitized events enter this bounded local writer. Network and game threads never wait for disk. */
final class DiagnosticLog implements AutoCloseable {
    static final long MAX_BYTES=1024*1024;
    private final Path file;
    private final String run=UUID.randomUUID().toString().substring(0,8);
    private final AtomicLong dropped=new AtomicLong();
    private final ThreadPoolExecutor writer=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(256), task -> {
        Thread thread=new Thread(task,"everywhere-diagnostic-log"); thread.setDaemon(true); return thread;
    },new ThreadPoolExecutor.AbortPolicy());
    private volatile boolean failed;
    DiagnosticLog(Path folder) { file=folder.resolve("connection.log"); }
    void append(ConnectionTrace.Event event) {
        try { writer.execute(() -> write(event)); }
        catch(RejectedExecutionException ignored) { dropped.incrementAndGet(); }
    }
    private void write(ConnectionTrace.Event event) {
        if(failed) { dropped.incrementAndGet(); return; }
        try {
            byte[] line=(Wire.GSON.toJson(Map.of("utc",Instant.now().toString(),"run",run,"event",event))+"\n").getBytes(StandardCharsets.UTF_8);
            Files.createDirectories(file.getParent());
            if(Files.exists(file) && Files.size(file)+line.length>MAX_BYTES)
                Files.move(file,file.resolveSibling("connection.previous.log"),StandardCopyOption.REPLACE_EXISTING);
            Files.write(file,line,StandardOpenOption.CREATE,StandardOpenOption.APPEND);
        } catch(Exception error) {
            failed=true; dropped.incrementAndGet();
            EverywhereMod.LOG.warn("Local connection log unavailable: {}",error.getClass().getSimpleName());
        }
    }
    long dropped() { return dropped.get(); }
    boolean writeFailed() { return failed; }
    @Override public void close() {
        writer.shutdown();
        try { if(!writer.awaitTermination(1,TimeUnit.SECONDS))writer.shutdownNow(); }
        catch(InterruptedException error) { Thread.currentThread().interrupt(); writer.shutdownNow(); }
    }
}
