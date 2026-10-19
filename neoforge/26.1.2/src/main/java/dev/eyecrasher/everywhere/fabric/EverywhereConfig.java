package dev.eyecrasher.everywhere.fabric;

import dev.eyecrasher.everywhere.core.Wire;
import net.neoforged.fml.loading.FMLPaths;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

final class EverywhereConfig {
    static final String DEFAULT_DIRECTORY = "https://79.76.120.82/route";
    boolean enabled = true;
    boolean direct = true;
    boolean udp = true;
    String relay = "wss://79.76.120.82/voice";
    String directory = DEFAULT_DIRECTORY;
    List<String> backupDirectories = List.of("https://158.179.207.22/route");
    private static Path path() { return FMLPaths.CONFIGDIR.get().resolve("plasmo-everywhere.json"); }
    static EverywhereConfig load() {
        try {
            if (Files.exists(path())) {
                if (Files.size(path()) > 8192) throw new IllegalArgumentException("Config too large");
                EverywhereConfig config = Wire.GSON.fromJson(Files.readString(path()), EverywhereConfig.class);
                if (config == null || config.relay == null) throw new IllegalArgumentException("Config empty");
                return config;
            }
        } catch (Exception e) { EverywhereMod.LOG.warn("Cannot read Everywhere config; using defaults"); }
        EverywhereConfig config = new EverywhereConfig(); config.save(); return config;
    }
    List<URI> directoryUris() {
        // A custom legacy directory is an explicit override. Never send its rooms to our defaults.
        if (directory == null || directory.isBlank()) return List.of();
        LinkedHashSet<URI> urls = new LinkedHashSet<>(); urls.add(URI.create(directory));
        if (DEFAULT_DIRECTORY.equals(directory) && backupDirectories != null)
            backupDirectories.stream().filter(Objects::nonNull).filter(s -> !s.isBlank()).limit(3)
                    .map(URI::create).forEach(urls::add);
        return List.copyOf(urls);
    }
    void save() {
        try { Files.writeString(path(), Wire.GSON.toJson(this)); }
        catch (Exception e) { EverywhereMod.LOG.warn("Cannot save Everywhere config"); }
    }
}
