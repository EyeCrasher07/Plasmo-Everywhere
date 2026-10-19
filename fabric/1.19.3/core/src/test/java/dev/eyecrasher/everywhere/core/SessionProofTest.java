package dev.eyecrasher.everywhere.core;

import java.net.URI;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static dev.eyecrasher.everywhere.core.ProtocolTest.check;

final class SessionProofTest {
    static void verify() throws Exception {
        byte[] secret=Wire.random(32); UUID previous=UUID.randomUUID(),player=UUID.randomUUID();
        String room="c".repeat(64),challenge="d".repeat(40),key=Wire.base64(PeerCrypto.keyPair().getPublic().getEncoded());
        byte[] proof=SessionProof.sign(secret,previous,player,room,challenge,key);
        check(SessionProof.verify(proof,secret,previous,player,room,challenge,key),"Reconnect proof accepted");
        check(!SessionProof.verify(proof,Wire.random(32),previous,player,room,challenge,key),"Other session secret rejected");
        check(!SessionProof.verify(proof,secret,UUID.randomUUID(),player,room,challenge,key),"Previous session bound");
        check(!SessionProof.verify(proof,secret,previous,UUID.randomUUID(),room,challenge,key),"Player UUID bound");
        check(!SessionProof.verify(proof,secret,previous,player,"e".repeat(64),challenge,key),"Minecraft room bound");
        check(!SessionProof.verify(proof,secret,previous,player,room,"e".repeat(40),key),"Fresh handshake prevents replay");
        check(!SessionProof.verify(proof,secret,previous,player,room,challenge,Wire.base64(PeerCrypto.keyPair().getPublic().getEncoded())),"New encryption key bound");
        check(!SessionProof.verify(new byte[1],secret,previous,player,room,challenge,key),"Malformed proof rejected");
        check(VoiceClient.closeMessage(1008,"This player already has a session").contains("already has a session"),"Policy reason visible");
        check(VoiceClient.closeMessage(1008,"unknown\nsecret=do-not-print").equals("Voice service disconnected (1008)"),"Unknown remote reason not reflected");
        check(VoiceClient.closeMessage(1008,null).equals("Voice service disconnected (1008)"),"Empty close reason safe");
        AtomicInteger authenticated=new AtomicInteger(),closed=new AtomicInteger();
        var identity=new VoiceClient.Identity(player,"ClosedClient",player,"ClosedClient",room,"minecraft:overworld");
        try(VoiceClient client=new VoiceClient(URI.create("ws://127.0.0.1:1/voice"),null,identity,false,false,
                c -> {authenticated.incrementAndGet();return CompletableFuture.completedFuture(false);},
                new VoiceClient.Listener(){public void closed(String reason){closed.incrementAndGet();}})) {
            client.close();
            Socket socket=new Socket(); client.onOpen(socket);
            var message=Wire.message("challenge"); message.addProperty("protocol",1); message.addProperty("challenge",challenge);
            client.onText(socket,message.toString(),true); client.onPing(socket,ByteBuffer.allocate(0));
            check(socket.aborted && socket.pongs==0 && authenticated.get()==0,"Late WebSocket callbacks cannot revive a closed session");
            check(closed.get()==1,"Close callback occurs once");
        }
        System.out.println("Session proofs: player/room/key/challenge binding, replay rejection, safe close reasons and late callback cleanup passed");
    }
    private static final class Socket implements WebSocket {
        boolean aborted; int pongs;
        public CompletableFuture<WebSocket> sendText(CharSequence x,boolean last){return CompletableFuture.completedFuture(this);}
        public CompletableFuture<WebSocket> sendBinary(ByteBuffer x,boolean last){return CompletableFuture.completedFuture(this);}
        public CompletableFuture<WebSocket> sendPing(ByteBuffer x){return CompletableFuture.completedFuture(this);}
        public CompletableFuture<WebSocket> sendPong(ByteBuffer x){pongs++;return CompletableFuture.completedFuture(this);}
        public CompletableFuture<WebSocket> sendClose(int code,String reason){return CompletableFuture.completedFuture(this);}
        public void request(long n){}
        public String getSubprotocol(){return "";}
        public boolean isOutputClosed(){return aborted;}
        public boolean isInputClosed(){return aborted;}
        public void abort(){aborted=true;}
    }
}
