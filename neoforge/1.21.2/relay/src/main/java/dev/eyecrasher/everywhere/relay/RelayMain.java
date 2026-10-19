package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.net.URI;
import java.util.*;

public final class RelayMain {
    public static void main(String[] args) throws Exception {
        Map<String,String> options = new HashMap<>();
        for (String arg : args) {
            String[] pair = arg.replaceFirst("^--", "").split("=", 2);
            if (pair.length != 2) throw new IllegalArgumentException("Expected --option=value");
            options.put(pair[0], pair[1]);
        }
        String bind = options.getOrDefault("bind", "0.0.0.0");
        int port = Integer.parseInt(options.getOrDefault("port", "443"));
        int udpPort = Integer.parseInt(options.getOrDefault("udp-port", "24454"));
        var tls = options.containsKey("keystore") ? Tls.server(Path.of(options.get("keystore")), Path.of(options.get("password-file"))) : null;
        if (tls == null && !bind.equals("127.0.0.1")) throw new IllegalArgumentException("Public relay requires TLS");
        int capacity=Integer.parseInt(options.getOrDefault("max-sessions","256"));
        int reserve=Integer.parseInt(options.getOrDefault("verified-reserve",String.valueOf(RelayServer.defaultReserve(capacity))));
        RelayServer relay = new RelayServer(bind, port, options.getOrDefault("udp-bind", bind), udpPort, tls,
                Boolean.parseBoolean(options.getOrDefault("proxy", "false")),capacity,reserve);
        String directoryKey=null;
        if(options.containsKey("directory-key-file")) {
            Path keyFile=Path.of(options.get("directory-key-file"));
            if(Files.size(keyFile)>256)throw new IllegalArgumentException("Directory key file too large");
            directoryKey=Files.readString(keyFile).trim();
            if(!directoryKey.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Directory key must contain 64 hex characters");
        }
        if(options.containsKey("nodes") && options.containsKey("directory-upstream"))
            throw new IllegalArgumentException("Choose directory authority or mirror");
        RoutingDirectory primary=options.containsKey("nodes")?new RoutingDirectory(Path.of(options.get("nodes")),
                Path.of(options.getOrDefault("routes","routes.json")),directoryKey!=null):null;
        if(primary!=null && directoryKey!=null && !options.containsKey("node-id"))
            throw new IllegalArgumentException("Monitored directory requires --node-id");
        DirectoryRoutes directory=primary;
        if(options.containsKey("directory-upstream"))directory=new DirectoryMirror(URI.create(options.get("directory-upstream")),
                Path.of(options.getOrDefault("directory-cache","directory-mirror.json")),directoryKey);
        DirectoryNodeMonitor monitor=primary!=null && directoryKey!=null
                ?new DirectoryNodeMonitor(primary,relay,options.get("node-id"),directoryKey):null;
        HealthServer health = options.containsKey("health-port") ? new HealthServer(relay, Integer.parseInt(options.get("health-port")),directory,directoryKey) : null;
        DirectoryRoutes routes=directory;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if(monitor!=null)monitor.close(); if (health != null) health.close(); if(routes!=null)routes.close(); relay.close();
        }));
        relay.start();
        System.out.println("Plasmo Everywhere relay starting; control=" + port + ", udp=" + udpPort + ", TLS=" + (tls != null));
    }
}
