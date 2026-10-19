package dev.eyecrasher.everywhere.relay;

import com.sun.net.httpserver.HttpServer;
import dev.eyecrasher.everywhere.core.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.*;

public final class DirectoryTest {
    public static void main(String[] args) throws Exception {
        Path dir=Files.createTempDirectory("everywhere-routing-test-");
        Path config=dir.resolve("nodes.json"), state=dir.resolve("routes.json");
        String a="{\"id\":\"node-a\",\"control\":\"wss://a.example/voice\",\"weight\":1}";
        String b="{\"id\":\"node-b\",\"control\":\"wss://b.example/voice\",\"weight\":1}";
        String room="a".repeat(64);
        try {
            Files.writeString(config,"{\"nodes\":["+a+"]}");
            RoutingDirectory directory=new RoutingDirectory(config,state);
            require(directory.route(room).get("node").getAsString().equals("node-a"),"First node assignment");
            Files.writeString(config,"{\"nodes\":["+a+","+b+"]}");
            Files.setLastModifiedTime(config,FileTime.fromMillis(System.currentTimeMillis()+1000));
            require(directory.route(room).get("node").getAsString().equals("node-a"),"Adding node split an existing room");
            Set<String> assigned=new HashSet<>();
            for(int n=0;n<8;n++)assigned.add(directory.route("0".repeat(63)+n).get("node").getAsString());
            require(assigned.size()==2,"New rooms did not distribute across nodes");
            RoutingDirectory restored=new RoutingDirectory(config,state);
            require(restored.route(room).get("node").getAsString().equals("node-a"),"Affinity lost after restart");
            try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null);
                HealthServer http=new HealthServer(relay,0,restored)) {
                URI endpoint=URI.create("http://127.0.0.1:"+http.port()+"/route");
                URI result=NetworkDirectory.resolve(endpoint,room).get(5,TimeUnit.SECONDS);
                require(result.equals(URI.create("wss://a.example/voice")),"Automatic client discovery");
                var route=NetworkDirectory.lookup(endpoint,room).get(5,TimeUnit.SECONDS);
                require(route.probes().size()==2 && route.probes().stream().allMatch(p -> p.udpPort()==24454),"Trusted multi-node STUN discovery");
                verifyTcpEndpoint(http.port());
            }
            Files.writeString(config,"{\"nodes\":["+b+"]}");
            Files.setLastModifiedTime(config,FileTime.fromMillis(System.currentTimeMillis()+2000));
            try { directory.route(room); throw new AssertionError("Removing a configured node silently moved an existing room"); }
            catch(java.io.IOException expected) {}
            verifyOversize();
            DirectoryMaintenanceTest.main(new String[0]);
            DirectoryAbuseTest.main(new String[0]);
            DirectoryMirrorTest.main(new String[0]);
            DirectoryLoadTest.main(new String[0]);
            System.out.println("Node directory: automatic discovery, weighted assignment, stable rooms, persistence and size bounds passed");
        } finally {
            try(var paths=Files.walk(dir)) { for(Path p:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(p); }
        }
    }
    private static void verifyTcpEndpoint(int port) throws Exception {
        URI uri=URI.create("http://127.0.0.1:"+port+"/tcp-discovery");
        try(var client=java.net.http.HttpClient.newHttpClient()) {
            var handler=java.net.http.HttpResponse.BodyHandlers.ofString();
            require(client.send(java.net.http.HttpRequest.newBuilder(uri).GET().build(),handler).statusCode()==400,"Missing front-end source accepted");
            var good=client.send(java.net.http.HttpRequest.newBuilder(uri).header("X-Real-IP","8.8.4.4").header("X-Real-Port","45000").GET().build(),handler);
            require(good.statusCode()==200 && Wire.parse(good.body()).get("port").getAsInt()==45000,"TCP observation response");
            require(good.headers().firstValue("Cache-Control").orElse("").equals("no-store"),"TCP mapping cached");
            require(client.send(java.net.http.HttpRequest.newBuilder(uri).header("X-Real-IP","10.0.0.1").header("X-Real-Port","45000").GET().build(),handler).statusCode()==400,"Private source published");
            require(client.send(java.net.http.HttpRequest.newBuilder(URI.create(uri+"?target=http://8.8.8.8/")).GET().build(),handler).statusCode()==400,"Discovery accepted target parameter");
            require(client.send(java.net.http.HttpRequest.newBuilder(uri).POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build(),handler).statusCode()==400,"TCP discovery method");
        }
    }
    private static void verifyOversize() throws Exception {
        HttpServer http=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        http.createContext("/route",exchange -> {
            try { byte[] bytes=new byte[9000]; exchange.sendResponseHeaders(200,bytes.length); exchange.getResponseBody().write(bytes); }
            finally { exchange.close(); }
        }); http.start();
        try {
            try {
                NetworkDirectory.resolve(URI.create("http://127.0.0.1:"+http.getAddress().getPort()+"/route"),"b".repeat(64)).get(5,TimeUnit.SECONDS);
                throw new AssertionError("Oversized directory response accepted");
            } catch(ExecutionException expected) {
                require(expected.getCause() instanceof java.io.IOException,"Wrong failure for oversized response");
            }
        } finally { http.stop(0); }
    }
    private static void require(boolean ok,String label) { if(!ok)throw new AssertionError(label); }
}
