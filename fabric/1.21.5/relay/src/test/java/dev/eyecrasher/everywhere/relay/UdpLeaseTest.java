package dev.eyecrasher.everywhere.relay;

import com.google.gson.JsonObject;
import dev.eyecrasher.everywhere.core.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/** Renewal only skips return-routability checks for a previously confirmed exact endpoint. */
public final class UdpLeaseTest {
    private static final class Guest extends WebSocketClient {
        final CompletableFuture<JsonObject> welcome=new CompletableFuture<>();
        Guest(URI uri) { super(uri); }
        @Override public void onOpen(ServerHandshake handshake) {}
        @Override public void onMessage(String text) {
            JsonObject message=Wire.parse(text);
            if(message.get("type").getAsString().equals("challenge")) {
                JsonObject join=Wire.message("join");
                join.addProperty("player",Wire.offlineUuid("UdpLease").toString()); join.addProperty("name","UdpLease");
                join.addProperty("room","e".repeat(64)); join.addProperty("dimension","minecraft:overworld");
                try {
                    var keys=java.security.KeyPairGenerator.getInstance("X25519").generateKeyPair();
                    join.addProperty("key",Wire.base64(keys.getPublic().getEncoded())); send(Wire.GSON.toJson(join));
                } catch(Exception error) { welcome.completeExceptionally(error); }
            } else if(message.get("type").getAsString().equals("welcome"))welcome.complete(message);
        }
        @Override public void onClose(int code,String reason,boolean remote) {
            welcome.completeExceptionally(new IllegalStateException("Closed: "+code+" "+reason));
        }
        @Override public void onError(Exception error) { welcome.completeExceptionally(error); }
    }
    public static void main(String[] args) throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,0,null);
            DatagramSocket first=new DatagramSocket(new InetSocketAddress("127.0.0.1",0));
            DatagramSocket rebound=new DatagramSocket(new InetSocketAddress("127.0.0.1",0))) {
            server.start(); long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(server.getPort()==0) {
                if(System.nanoTime()>until)throw new AssertionError("UDP lease server start"); Thread.sleep(5);
            }
            Guest guest=new Guest(URI.create("ws://127.0.0.1:"+server.getPort()+"/voice"));
            try {
                guest.connect(); JsonObject welcome=guest.welcome.get(5,TimeUnit.SECONDS);
                UUID session=Wire.id(welcome,"session"); byte[] key=Wire.unbase64(welcome.get("udpKey").getAsString(),32);
                InetSocketAddress relay=new InetSocketAddress("127.0.0.1",server.udpPort());
                first.setSoTimeout(1500); rebound.setSoTimeout(1500);
                transmit(first,relay,session,key,0,Wire.BIND,new byte[0]);
                Wire.Datagram challenge=receive(first,key); require(challenge.op()==Wire.CHALLENGE,"Initial endpoint skipped challenge");
                transmit(first,relay,session,key,1,Wire.CONFIRM,challenge.body());
                require(receive(first,key).op()==Wire.BOUND,"Initial endpoint not confirmed");
                byte[] renewal=Wire.datagram(session,2,Wire.BIND,new byte[0],key,false);
                first.send(new DatagramPacket(renewal,renewal.length,relay));
                require(receive(first,key).op()==Wire.BOUND,"Confirmed lease renewal unnecessarily challenged");
                first.send(new DatagramPacket(renewal,renewal.length,relay)); silence(first,key,"Replayed lease renewal accepted");
                transmit(rebound,relay,session,key,3,Wire.BIND,new byte[0]);
                Wire.Datagram reboundChallenge=receive(rebound,key);
                require(reboundChallenge.op()==Wire.CHALLENGE,"NAT-rebound endpoint skipped return-routability");
                // A pending alternate endpoint cannot disable the currently proven endpoint.
                transmit(first,relay,session,key,4,Wire.BIND,new byte[0]);
                require(receive(first,key).op()==Wire.BOUND,"Pending NAT rebind disrupted existing endpoint");
                transmit(rebound,relay,session,key,5,Wire.CONFIRM,reboundChallenge.body());
                require(receive(rebound,key).op()==Wire.BOUND,"NAT-rebound endpoint did not confirm");
                transmit(rebound,relay,session,key,6,Wire.BIND,new byte[0]);
                require(receive(rebound,key).op()==Wire.BOUND,"New confirmed endpoint cannot renew");
                transmit(first,relay,session,key,7,Wire.BIND,new byte[0]);
                require(receive(first,key).op()==Wire.CHALLENGE,"Old endpoint retained renewal privilege after NAT rebind");
                byte[] forged=Wire.datagram(session,8,Wire.BIND,new byte[0],Wire.random(32),false);
                rebound.send(new DatagramPacket(forged,forged.length,relay)); silence(rebound,key,"Unauthenticated confirmed-endpoint renewal accepted");
                System.out.println("UDP lease: exact-endpoint renewal, replay/MAC rejection and challenged NAT rebind passed");
            } finally { guest.closeBlocking(); }
        }
    }
    private static void transmit(DatagramSocket socket,InetSocketAddress relay,UUID session,byte[] key,
                                 long sequence,byte op,byte[] body) throws Exception {
        byte[] bytes=Wire.datagram(session,sequence,op,body,key,false);
        socket.send(new DatagramPacket(bytes,bytes.length,relay));
    }
    private static Wire.Datagram receive(DatagramSocket socket,byte[] key) throws Exception {
        byte[] bytes=new byte[Wire.MAX_UDP]; DatagramPacket packet=new DatagramPacket(bytes,bytes.length);
        socket.receive(packet); return Wire.openDatagram(Arrays.copyOf(bytes,packet.getLength()),key,true);
    }
    private static void silence(DatagramSocket socket,byte[] key,String failure) throws Exception {
        socket.setSoTimeout(150);
        try { receive(socket,key); throw new AssertionError(failure); }
        catch(SocketTimeoutException expected) {}
        finally { socket.setSoTimeout(1500); }
    }
    private static void require(boolean condition,String message) { if(!condition)throw new AssertionError(message); }
}
