package dev.eyecrasher.everywhere.relay;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpServer;
import dev.eyecrasher.everywhere.core.Wire;
import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/** A real HTTP origin with a fake wall clock exercises warm cache and partition boundaries. */
public final class DirectoryMirrorTest {
    public static void main(String[] ignored) throws Exception {
        Path dir=Files.createTempDirectory("everywhere-directory-mirror-"); AtomicLong now=new AtomicLong(2_000_000);
        String room="d".repeat(64),unknown="e".repeat(64); AtomicBoolean online=new AtomicBoolean(true);
        AtomicReference<byte[]> body=new AtomicReference<>(); AtomicInteger hits=new AtomicInteger();
        JsonObject route=RoutingDirectory.response(new RoutingDirectory.Node("a",URI.create("wss://a.example/voice"),1,24454,true),List.of(),now.get()+60_000);
        body.set(Wire.GSON.toJson(route).getBytes(StandardCharsets.UTF_8));
        HttpServer origin=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        origin.createContext("/route",exchange -> {
            try {
                hits.incrementAndGet(); byte[] bytes=body.get(); exchange.sendResponseHeaders(online.get()?200:503,bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        }); origin.start(); URI upstream=URI.create("http://127.0.0.1:"+origin.getAddress().getPort()+"/route");
        Path state=dir.resolve("mirror.json");
        try {
            try(DirectoryMirror mirror=new DirectoryMirror(upstream,state,now::get,HttpClient.newHttpClient())) {
                require(mirror.route(room).equals(route),"Warm-through did not preserve authoritative assignment");
                require(hits.get()==1 && Files.exists(state),"Warm-through did not persist reservation");
                online.set(false); now.addAndGet(10_000);
                require(mirror.route(room).equals(route),"Secondary lost a current cached assignment during origin outage");
                unavailable(() -> mirror.route(unknown),"Secondary invented an assignment for an unknown room");
                require(mirror.route(room).get("validUntil").getAsLong()==route.get("validUntil").getAsLong(),"Cache renewed its own authority");
            }
            try(DirectoryMirror restored=new DirectoryMirror(upstream,state,now::get,HttpClient.newHttpClient())) {
                require(restored.route(room).equals(route),"Secondary restart lost a current reservation");
                now.set(route.get("validUntil").getAsLong()-DirectoryMirror.SAFETY_MILLIS);
                unavailable(() -> restored.route(room),"Secondary served an expired or near-expiry reservation");
                now.addAndGet(120_000);
                unavailable(() -> restored.route(room),"Repeated cache reads extended the lease during a partition");
            }
            online.set(true); route.addProperty("validUntil",now.get()+60_000); route.addProperty("control","wss://b.example/voice"); route.addProperty("node","b");
            body.set(Wire.GSON.toJson(route).getBytes(StandardCharsets.UTF_8));
            try(DirectoryMirror recovered=new DirectoryMirror(upstream,state,now::get,HttpClient.newHttpClient())) {
                require(recovered.route(room).get("node").getAsString().equals("b"),"Secondary ignored recovered authoritative routing");
                body.set(new byte[9000]); unavailable(() -> recovered.route(unknown),"Oversized upstream body accepted");
                route.addProperty("control","ws://untrusted.example/voice");
                body.set(Wire.GSON.toJson(route).getBytes(StandardCharsets.UTF_8));
                unavailable(() -> recovered.route(unknown),"Insecure upstream control address accepted");
                route.addProperty("control","wss://b.example/voice"); route.addProperty("validUntil",now.get()+600_000);
                body.set(Wire.GSON.toJson(route).getBytes(StandardCharsets.UTF_8));
                unavailable(() -> recovered.route(unknown),"Unbounded upstream lease accepted");
            }
            System.out.println("Secondary directory: warm-through, durable bounded reservations, no allocation during partition, expiry and recovery passed");
        } finally { origin.stop(0); DirectoryMaintenanceTest.remove(dir); }
    }
    @FunctionalInterface private interface Throwing { void run() throws Exception; }
    private static void unavailable(Throwing action,String message) throws Exception {
        try { action.run(); throw new AssertionError(message); } catch(IOException expected) {}
    }
    private static void require(boolean ok,String message) { if(!ok)throw new AssertionError(message); }
}
