package dev.eyecrasher.everywhere.core;

import com.google.gson.*;
import java.util.*;

final class WorldPresenceTest {
    static void run() {
        UUID a=UUID.randomUUID(),b=UUID.randomUUID();String dim="minecraft:overworld",world=WorldPresence.fingerprint(123,dim);
        var pa=new WorldPresence.Point(1,0,80,0);var pb=new WorldPresence.Point(2,3,80,0);
        var sa=new WorldPresence.Snapshot(a,dim,world,100,pa,Map.of(b,pb));
        var sb=new WorldPresence.Snapshot(b,dim,world,120,pb,Map.of(a,pa));
        require(WorldPresence.matches(sa,sb) && WorldPresence.matches(sb,sa),"Mutual observations must match symmetrically");
        require(sa.equals(WorldPresence.read(WorldPresence.write(sa))),"Presence wire roundtrip");
        require(!WorldPresence.matches(sa,new WorldPresence.Snapshot(b,dim,WorldPresence.fingerprint(124,dim),120,pb,Map.of(a,pa))),"Different worlds");
        require(!WorldPresence.matches(sa,new WorldPresence.Snapshot(b,"minecraft:the_nether",world,120,pb,Map.of(a,pa))),"Different dimensions");
        require(!WorldPresence.matches(sa,new WorldPresence.Snapshot(b,dim,world,300,pb,Map.of(a,pa))),"Different world clocks");
        require(!WorldPresence.matches(sa,new WorldPresence.Snapshot(b,dim,world,120,pb,Map.of())),"One-sided visibility");
        require(!WorldPresence.matches(sa,new WorldPresence.Snapshot(b,dim,world,120,new WorldPresence.Point(3,3,80,0),Map.of(a,pa))),"Different entity identifiers");
        require(!WorldPresence.matches(sa,new WorldPresence.Snapshot(b,dim,world,120,new WorldPresence.Point(2,90,80,0),Map.of(a,pa))),"Different positions");
        require(WorldPresence.matches(sa,new WorldPresence.Snapshot(b,dim,world,120,new WorldPresence.Point(2,6,80,0),Map.of(a,pa))),"Movement and packet delay tolerance");
        require(!WorldPresence.matches(sa,sa),"Repeated player UUID cannot match itself");
        rejects(() -> new WorldPresence.Point(1,Double.NaN,0,0));
        rejects(() -> new WorldPresence.Point(1,Double.POSITIVE_INFINITY,0,0));
        rejects(() -> new WorldPresence.Snapshot(a,dim,world,Long.MAX_VALUE,pa,Map.of()));
        rejects(() -> new WorldPresence.Snapshot(a,dim,world,0,pa,Map.of(a,pa)));
        JsonObject invalid=WorldPresence.write(sa);invalid.addProperty("ticks",1.5);rejects(() -> WorldPresence.read(invalid));
        JsonObject oversized=WorldPresence.write(sa);JsonObject points=oversized.getAsJsonObject("nearby");
        for(int n=0;n<25;n++)points.add(UUID.randomUUID().toString(),new JsonArray());
        rejects(() -> WorldPresence.read(oversized));
        System.out.println("World presence: mutual visibility, world/clock/entity/position isolation and bounded protocol OK");
    }
    private static void require(boolean test,String message) { if(!test)throw new AssertionError(message); }
    private static void rejects(Runnable task) {
        try { task.run();throw new AssertionError("Invalid presence accepted"); }
        catch(IllegalArgumentException expected) {}
    }
}
