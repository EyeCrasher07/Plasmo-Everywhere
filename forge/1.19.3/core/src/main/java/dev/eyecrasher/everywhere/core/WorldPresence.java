package dev.eyecrasher.everywhere.core;

import com.google.gson.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Short-lived evidence of a shared Minecraft world, never a permanent server alias. */
public final class WorldPresence {
    public static final int MAX_NEARBY=24;
    public static final long MAX_AGE_MILLIS=10_000;
    private static final long MAX_TICK_DIFFERENCE=160;
    private WorldPresence() {}

    public record Point(int entity,double x,double y,double z) {
        public Point {
            if(!coordinate(x) || !coordinate(y) || !coordinate(z))throw new IllegalArgumentException("Presence position");
        }
        private static boolean coordinate(double value) { return Double.isFinite(value) && Math.abs(value)<=64_000_000; }
        private boolean agrees(Point other) {
            double dx=x-other.x,dy=y-other.y,dz=z-other.z;
            return entity==other.entity && dx*dx+dy*dy+dz*dz<=64;
        }
    }
    public record Snapshot(UUID player,String dimension,String world,long ticks,Point self,Map<UUID,Point> nearby) {
        public Snapshot {
            Objects.requireNonNull(player);Objects.requireNonNull(self);
            if(dimension==null || dimension.isEmpty() || dimension.length()>128 || world==null || !world.matches("[a-f0-9]{64}")
                    || ticks<0 || ticks>Long.MAX_VALUE-1024 || nearby.size()>MAX_NEARBY || nearby.containsKey(player))
                throw new IllegalArgumentException("World presence");
            nearby=Map.copyOf(nearby);
        }
        /** Game time changes even while players stand still; refresh that evidence periodically. */
        public boolean samePositions(Snapshot other) {
            return other!=null && player.equals(other.player) && dimension.equals(other.dimension) && world.equals(other.world)
                    && self.equals(other.self) && nearby.equals(other.nearby);
        }
    }
    public static String fingerprint(long biomeSeed,String dimension) {
        try {
            byte[] value=("everywhere/world-presence/v1/"+biomeSeed+"/"+dimension).getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch(NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    public static boolean matches(Snapshot a,Snapshot b) {
        if(a==null || b==null || a.player.equals(b.player) || !a.dimension.equals(b.dimension) || !a.world.equals(b.world)
                || Math.abs(a.ticks-b.ticks)>MAX_TICK_DIFFERENCE)return false;
        Point seenB=a.nearby.get(b.player),seenA=b.nearby.get(a.player);
        return seenB!=null && seenA!=null && seenB.agrees(b.self) && seenA.agrees(a.self);
    }
    public static JsonObject write(Snapshot value) {
        JsonObject j=new JsonObject();j.addProperty("player",value.player.toString());j.addProperty("dimension",value.dimension);
        j.addProperty("world",value.world);j.addProperty("ticks",value.ticks);j.add("self",point(value.self));
        JsonObject nearby=new JsonObject();value.nearby.forEach((id,p) -> nearby.add(id.toString(),point(p)));j.add("nearby",nearby);
        return j;
    }
    public static Snapshot read(JsonObject j) {
        JsonObject observations=j.getAsJsonObject("nearby");
        if(observations.size()>MAX_NEARBY)throw new IllegalArgumentException("Presence nearby limit");
        Map<UUID,Point> nearby=new HashMap<>();observations.entrySet().forEach(entry -> nearby.put(UUID.fromString(entry.getKey()),point(entry.getValue().getAsJsonArray())));
        return new Snapshot(Wire.id(j,"player"),Wire.string(j,"dimension",128),Wire.string(j,"world",64),
                integer(j.get("ticks")),point(j.getAsJsonArray("self")),nearby);
    }
    private static JsonArray point(Point value) {
        JsonArray a=new JsonArray();a.add(value.entity);a.add(value.x);a.add(value.y);a.add(value.z);return a;
    }
    private static Point point(JsonArray a) {
        if(a.size()!=4)throw new IllegalArgumentException("Presence point size");
        long entity=integer(a.get(0));if(entity<Integer.MIN_VALUE || entity>Integer.MAX_VALUE)throw new IllegalArgumentException("Presence entity");
        return new Point((int)entity,number(a.get(1)),number(a.get(2)),number(a.get(3)));
    }
    private static double number(JsonElement value) {
        if(!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())throw new IllegalArgumentException("Presence number");
        return value.getAsDouble();
    }
    private static long integer(JsonElement value) {
        if(!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())throw new IllegalArgumentException("Presence integer");
        try { return value.getAsBigDecimal().longValueExact(); }
        catch(ArithmeticException invalid) { throw new IllegalArgumentException("Presence integer",invalid); }
    }
}
