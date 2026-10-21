package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Aggregate successful sessions only: no account, nickname, address or room identifiers. */
final class UsageStatistics implements AutoCloseable {
    static final int RETAIN_DAYS = 31;
    private static final long MAX = 9_007_199_254_740_991L;
    private final Path file;
    private final Clock clock;
    private final ScheduledExecutorService writer;
    private final Object fileLock = new Object();
    private final NavigableMap<LocalDate, Day> days = new TreeMap<>();
    private long since, sessions, verified, resumed, completed, durationMillis, longestMillis, peak, lastJoin, savedAt;
    private int online;
    private boolean writable = true, storageHealthy = true;

    private static final class Day {
        long sessions, peak;
        JsonObject json(LocalDate date) {
            JsonObject item = new JsonObject();
            item.addProperty("date", date.toString());
            item.addProperty("sessionsStarted", sessions);
            item.addProperty("peakOnline", peak);
            return item;
        }
    }

    UsageStatistics(Path file) { this(file, Clock.systemUTC(), true); }
    UsageStatistics(Path file, Clock clock, boolean background) {
        this.file = file;
        this.clock = clock;
        since = clock.millis();
        try { load(); }
        catch (IOException | RuntimeException invalid) {
            // Preserve unreadable/corrupt history for the operator. Voice remains available.
            days.clear(); sessions=verified=resumed=completed=durationMillis=longestMillis=peak=lastJoin=0;
            since=clock.millis(); writable=false; storageHealthy=false;
            System.err.println("Usage statistics history unavailable; original file preserved");
        }
        if (background) {
            writer = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "everywhere-usage-writer"); thread.setDaemon(true); return thread;
            });
            writer.scheduleWithFixedDelay(this::persist, 0, 30, TimeUnit.SECONDS);
        } else writer = null;
    }

    synchronized void joined(boolean accountVerified, boolean provenResumption, int connections) {
        sessions++;
        if (accountVerified) verified++;
        if (provenResumption) resumed++;
        online=connections;
        peak=Math.max(peak, connections);
        lastJoin=clock.millis();
        day().sessions++;
    }

    synchronized void left(long connectedMillis, int connections) {
        day(); // An active session spanning midnight contributes to the new day's peak.
        completed++;
        long duration=Math.max(0, connectedMillis);
        durationMillis=Math.min(MAX, durationMillis+Math.min(duration, MAX-durationMillis));
        longestMillis=Math.max(longestMillis, Math.min(MAX, duration));
        online=connections;
        day();
    }

    private Day day() {
        LocalDate today=Instant.ofEpochMilli(clock.millis()).atOffset(ZoneOffset.UTC).toLocalDate();
        days.headMap(today.minusDays(RETAIN_DAYS-1), false).clear();
        Day day=days.computeIfAbsent(today, ignored -> new Day());
        day.peak=Math.max(day.peak, online);
        // A backwards wall-clock adjustment must not let old dates grow storage unbounded.
        while (days.size()>RETAIN_DAYS) days.pollFirstEntry();
        return day;
    }

    synchronized JsonObject snapshot() {
        JsonObject result=state();
        LocalDate today=Instant.ofEpochMilli(clock.millis()).atOffset(ZoneOffset.UTC).toLocalDate();
        result.add("today", day().json(today));
        result.addProperty("persistent", writable && storageHealthy);
        if (savedAt>0) result.addProperty("lastSavedAt", Instant.ofEpochMilli(savedAt).toString());
        return result;
    }

    private JsonObject state() {
        day();
        JsonObject result=new JsonObject();
        result.addProperty("schema", 1);
        result.addProperty("since", Instant.ofEpochMilli(since).toString());
        result.addProperty("sessionsStarted", sessions);
        result.addProperty("verifiedSessions", verified);
        result.addProperty("unverifiedSessions", sessions-verified);
        result.addProperty("resumedSessions", resumed);
        result.addProperty("completedSessions", completed);
        result.addProperty("connectedMillis", durationMillis);
        result.addProperty("longestSessionMillis", longestMillis);
        result.addProperty("peakOnline", peak);
        if (lastJoin>0) result.addProperty("lastSessionAt", Instant.ofEpochMilli(lastJoin).toString());
        JsonArray history=new JsonArray();
        days.forEach((date,day) -> history.add(day.json(date)));
        result.add("days", history);
        return result;
    }

    private void load() throws IOException {
        if (!Files.exists(file)) return;
        if (Files.isSymbolicLink(file) || !Files.isRegularFile(file) || Files.size(file)>65536)
            throw new IOException("Invalid statistics file");
        JsonObject json=JsonParser.parseString(Files.readString(file)).getAsJsonObject();
        if (number(json,"schema")!=1) throw new IOException("Unsupported statistics schema");
        since=Instant.parse(json.get("since").getAsString()).toEpochMilli();
        sessions=number(json,"sessionsStarted"); verified=number(json,"verifiedSessions");
        resumed=number(json,"resumedSessions"); completed=number(json,"completedSessions");
        durationMillis=number(json,"connectedMillis"); longestMillis=number(json,"longestSessionMillis");
        peak=number(json,"peakOnline");
        if (json.has("lastSessionAt")) lastJoin=Instant.parse(json.get("lastSessionAt").getAsString()).toEpochMilli();
        if (verified>sessions || number(json,"unverifiedSessions")!=sessions-verified || resumed>sessions || completed>sessions
                || peak>RelayServer.MAX_SESSIONS || longestMillis>durationMillis)
            throw new IOException("Inconsistent statistics counters");
        JsonArray history=json.getAsJsonArray("days");
        if (history.size()>RETAIN_DAYS) throw new IOException("Statistics history too large");
        for (JsonElement value:history) {
            JsonObject item=value.getAsJsonObject(); LocalDate date=LocalDate.parse(item.get("date").getAsString());
            Day day=new Day(); day.sessions=number(item,"sessionsStarted"); day.peak=number(item,"peakOnline");
            if (day.sessions>sessions || day.peak>peak || days.put(date,day)!=null)
                throw new IOException("Invalid statistics day");
        }
        savedAt=Files.getLastModifiedTime(file).toMillis();
    }

    private static long number(JsonObject json, String key) {
        JsonPrimitive value=json.getAsJsonPrimitive(key);
        if (!value.isNumber()) throw new IllegalArgumentException("Expected statistics counter");
        long number=value.getAsBigDecimal().toBigIntegerExact().longValueExact();
        if (number<0 || number>MAX) throw new IllegalArgumentException("Invalid statistics counter");
        return number;
    }

    void persist() {
        synchronized (fileLock) {
            String json;
            synchronized (this) {
                if (!writable) return;
                json=state().toString()+"\n";
            }
            Path temporary=null;
            try {
                Path parent=file.toAbsolutePath().getParent(); Files.createDirectories(parent);
                temporary=Files.createTempFile(parent,"usage-", ".tmp");
                try (FileChannel output=FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    ByteBuffer data=StandardCharsets.UTF_8.encode(json);
                    while (data.hasRemaining()) output.write(data);
                    output.force(true);
                }
                try { Files.move(temporary,file,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING); }
                catch (AtomicMoveNotSupportedException unsupported) { Files.move(temporary,file,StandardCopyOption.REPLACE_EXISTING); }
                synchronized (this) { storageHealthy=true; savedAt=clock.millis(); }
            } catch (IOException | RuntimeException unavailable) {
                synchronized (this) {
                    if (storageHealthy) System.err.println("Usage statistics could not be saved; voice remains available");
                    storageHealthy=false;
                }
            } finally {
                if (temporary!=null) try { Files.deleteIfExists(temporary); } catch (IOException ignored) {}
            }
        }
    }

    @Override public void close() {
        if (writer!=null) {
            writer.shutdown();
            try { writer.awaitTermination(2,TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
        persist();
    }
}
