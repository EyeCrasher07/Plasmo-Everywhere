package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

final class RegionalDirectoryTest {
    static void verify() throws Exception {
        Path folder=Files.createTempDirectory("everywhere-regional-directory-"),config=folder.resolve("nodes.json");
        Files.writeString(config,"{\"nodes\":[{\"id\":\"a\",\"control\":\"wss://a.example.com/voice\"},{\"id\":\"b\",\"control\":\"wss://b.example.com/voice\"}]}");
        String room="f".repeat(64);RoutingDirectory directory=new RoutingDirectory(config,folder.resolve("routes.json"),true);
        long now=System.currentTimeMillis();
        for(String id:List.of("a","b"))directory.observe(id,new RoutingDirectory.Report(true,500,1,Set.of(),now,"test-mesh",true,Set.of(id.equals("a")?"b":"a")));
        JsonObject response=directory.route(room);String home=response.get("node").getAsString();
        if(response.getAsJsonArray("nodes").size()!=2)throw new AssertionError("Ready mesh candidates missing");
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start();
            try(HealthServer primary=new HealthServer(relay,0,directory);
                DirectoryMirror mirror=new DirectoryMirror(URI.create("http://127.0.0.1:"+primary.port()+"/route"),folder.resolve("mirror.json"));
                HealthServer secondary=new HealthServer(relay,0,mirror)) {
                for(HealthServer endpoint:List.of(primary,secondary)) {
                    var parsed=NetworkDirectory.lookup(URI.create("http://127.0.0.1:"+endpoint.port()+"/route"),room).get(5,TimeUnit.SECONDS);
                    if(parsed.nodes().size()!=2)throw new AssertionError("Directory/mirror stripped regional catalog");
                    if(!parsed.control().equals(URI.create("wss://"+home+".example.com/voice")))throw new AssertionError("Legacy assignment changed");
                }
            }
        }
        String other=home.equals("a")?"b":"a";
        directory.observe(other,new RoutingDirectory.Report(true,500,500,Set.of(),System.currentTimeMillis(),"test-mesh",true,Set.of(home)));
        if(directory.route(room).getAsJsonArray("nodes").size()!=1)throw new AssertionError("Full node advertised");
        directory.observe(home,new RoutingDirectory.Report(true,500,1,Set.of(),System.currentTimeMillis(),"test-mesh",true,Set.of()));
        if(directory.route(room).has("nodes"))throw new AssertionError("Missing mesh links allowed isolated node selection");
        directory.observe(home,new RoutingDirectory.Report(true,500,1,Set.of(),System.currentTimeMillis(),"test-mesh",false,Set.of()));
        if(directory.route(room).has("nodes"))throw new AssertionError("Unsynchronized home allowed regional split");
        directory.observe(home,new RoutingDirectory.Report(true,500,1,Set.of(),System.currentTimeMillis()));
        if(directory.route(room).has("nodes"))throw new AssertionError("Legacy node falsely advertised federation");
        directory.observe(home,new RoutingDirectory.Report(true,500,1,Set.of(),System.currentTimeMillis()-RoutingDirectory.REPORT_MILLIS-1000,"test-mesh",false,Set.of()));
        directory.observe(other,new RoutingDirectory.Report(true,500,1,Set.of(),System.currentTimeMillis(),"test-mesh",false,Set.of()));
        JsonArray remaining=directory.route(room).getAsJsonArray("nodes");
        if(remaining==null || remaining.size()!=1 || !remaining.get(0).getAsJsonObject().get("id").getAsString().equals(other))
            throw new AssertionError("Unavailable legacy home prevented regional failover");
        System.out.println("Regional directory passed: ready mesh, mirror preservation, full-node exclusion and safe legacy behavior");
    }
}
