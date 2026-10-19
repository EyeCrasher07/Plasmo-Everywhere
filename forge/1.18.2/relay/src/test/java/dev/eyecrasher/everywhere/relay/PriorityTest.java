package dev.eyecrasher.everywhere.relay;

import com.google.gson.JsonObject;
import dev.eyecrasher.everywhere.core.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Proofs are simulated only through a package-private constructor, never a wire/CLI flag. */
public final class PriorityTest {
    private static final String ROOM="d".repeat(64);
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer,long seq,int distance,byte[] bytes,boolean end) {
            if(!end)audio.add(bytes);
        }
    }
    private static VoiceClient client(URI uri,String name,UUID player,UUID account,boolean online,Sink sink) throws Exception {
        return new VoiceClient(uri,null,new VoiceClient.Identity(player,name,account,name,ROOM,"minecraft:overworld"),
                false,false,challenge -> CompletableFuture.completedFuture(online),sink);
    }
    private static URI start(RelayServer server) throws Exception {
        server.start(); await(() -> server.getPort()>0,"Priority server start");
        return URI.create("ws://127.0.0.1:"+server.getPort()+"/voice");
    }
    public static void main(String[] args) throws Exception {
        verifyProfiles(); verifyReserve(); verifyReclaim(); verifyLateProof(); verifyPendingBound();
        System.out.println("Verified priority: reservation, proof rejection, identity ownership, cleanup races and pending bounds passed");
    }
    private static void verifyProfiles() {
        UUID account=UUID.randomUUID();
        String body="{\"id\":\""+account.toString().replace("-","")+"\",\"name\":\"PremiumOwner\"}";
        require(RelayServer.matchesProfile(body,"premiumowner",account),"Valid profile rejected");
        require(!RelayServer.matchesProfile(body,"AnotherOwner",account),"Wrong profile name accepted");
        require(!RelayServer.matchesProfile(body,"PremiumOwner",UUID.randomUUID()),"Wrong account ID accepted");
        for(String invalid:List.of("{}","not json","x".repeat(Wire.MAX_TEXT+1)))
            require(!RelayServer.matchesProfile(invalid,"PremiumOwner",account),"Invalid profile accepted");
        require(RelayServer.defaultReserve(256)==26 && RelayServer.defaultReserve(64)==7,"Default reserve");
    }
    private static void verifyReserve() throws Exception {
        UUID premium=UUID.randomUUID(),extra=UUID.randomUUID(),unproven=UUID.randomUUID();
        RelayServer.AccountVerifier proofs=(challenge,name,account) -> CompletableFuture.completedFuture(
                (account.equals(premium) && name.equals("PremiumOwner")) || (account.equals(extra) && name.equals("ExtraPremium")));
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,3,1,proofs)) {
            URI uri=start(server);
            UUID a=Wire.offlineUuid("QuotaA"),b=Wire.offlineUuid("QuotaB"); Sink sa=new Sink(),sb=new Sink();
            try(VoiceClient ca=client(uri,"QuotaA",a,a,false,sa); VoiceClient cb=client(uri,"QuotaB",b,b,false,sb);
                VoiceClient denied=client(uri,"QuotaC",Wire.offlineUuid("QuotaC"),UUID.randomUUID(),false,new Sink());
                VoiceClient spoof=client(uri,"FakePremium",unproven,unproven,true,new Sink());
                VoiceClient owner=client(uri,"PremiumOwner",premium,premium,true,new Sink());
                VoiceClient full=client(uri,"ExtraPremium",extra,extra,true,new Sink())) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS);
                reject(denied,"Unverified account took reserved slot");
                // Claiming online=true is insufficient: a failed proof gets neither admission nor priority.
                reject(spoof,"Unproven license was accepted");
                owner.connect().get(5,TimeUnit.SECONDS);
                reject(full,"Verified account exceeded hard capacity");
                require(server.health().get("connections").getAsInt()==3,"Capacity changed");
                require(server.health().get("verifiedConnections").getAsInt()==1,"Verified count");
                ca.nearby(Set.of(b,premium)); cb.nearby(Set.of(a)); owner.nearby(Set.of(a));
                await(() -> ca.peers().size()==2,"Quota peers");
                require(ca.peers().stream().anyMatch(p -> p.player().equals(premium) && p.verified()),"Missing verified peer flag");
                byte[] voice={1,9,4}; ca.sendAudio(Set.of(b),0,16,voice,false);
                require(Arrays.equals(sb.audio.poll(3,TimeUnit.SECONDS),voice),"Existing offline conversation disrupted");
            }
        }
        // A client-supplied 'verified' flag must not confer priority.
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,1,proofs)) {
            URI uri=start(server); UUID id=Wire.offlineUuid("RawOffline");
            try(RawGuest guest=new RawGuest(uri,id,"RawOffline",true)) {
                guest.open(); guest.welcome.get(5,TimeUnit.SECONDS);
                require(server.health().get("verifiedConnections").getAsInt()==0,"Client forged verified status");
            }
        }
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,1,
                (challenge,name,account) -> CompletableFuture.completedFuture(true))) {
            URI uri=start(server);
            try(VoiceClient mismatch=client(uri,"WrongBinding",UUID.randomUUID(),UUID.randomUUID(),true,new Sink())) {
                reject(mismatch,"Verified account claimed an unrelated player's UUID");
            }
        }
    }
    private static void verifyReclaim() throws Exception {
        UUID account=UUID.randomUUID(),player=Wire.offlineUuid("PremiumOwner"),witnessId=Wire.offlineUuid("Witness");
        RelayServer.AccountVerifier proofs=(challenge,name,id) -> CompletableFuture.completedFuture(account.equals(id) && name.equals("PremiumOwner"));
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,3,1,proofs)) {
            URI uri=start(server); Sink ownerSink=new Sink(),witnessSink=new Sink();
            try(VoiceClient imposter=client(uri,"PremiumOwner",player,player,false,new Sink());
                VoiceClient witness=client(uri,"Witness",witnessId,witnessId,false,witnessSink);
                VoiceClient owner=client(uri,"PremiumOwner",player,account,true,ownerSink);
                VoiceClient retry=client(uri,"PremiumOwner",player,player,false,new Sink());
                VoiceClient duplicate=client(uri,"PremiumOwner",player,account,true,new Sink())) {
                imposter.connect().get(5,TimeUnit.SECONDS);
                var oldSocket=server.getConnections().iterator().next();
                witness.connect().get(5,TimeUnit.SECONDS);
                imposter.nearby(Set.of(witnessId)); witness.nearby(Set.of(player));
                await(() -> witness.peers().size()==1,"Imposter discovery");
                owner.connect().get(5,TimeUnit.SECONDS); owner.nearby(Set.of(witnessId));
                await(() -> !imposter.isReady(),"Old client still ready");
                // Force a delayed/duplicate cleanup after the new owner has joined.
                server.onClose(oldSocket,1008,"Verified account reclaimed this player",false);
                await(() -> witness.peers().stream().anyMatch(p -> p.session().equals(owner.sessionId()) && p.verified()),"Verified owner not discovered");
                reject(retry,"Unverified account displaced verified owner");
                reject(duplicate,"Second verified connection displaced an active verified session");
                byte[] voice={5,2,8}; witness.sendAudio(Set.of(player),1,16,voice,false);
                require(Arrays.equals(ownerSink.audio.poll(3,TimeUnit.SECONDS),voice),"Old onClose removed new owner's room entry");
                require(server.health().get("connections").getAsInt()==2,"Identity replacement leaked a slot");
            }
        }
    }
    private static void verifyLateProof() throws Exception {
        UUID account=UUID.randomUUID(),player=Wire.offlineUuid("LateOwner");
        CompletableFuture<Boolean> proof=new CompletableFuture<>(); CountDownLatch called=new CountDownLatch(1);
        RelayServer.AccountVerifier verifier=(challenge,name,id) -> { called.countDown(); return proof; };
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,1,verifier)) {
            URI uri=start(server);
            try(VoiceClient pending=client(uri,"LateOwner",player,account,true,new Sink())) {
                pending.connect(); require(called.await(5,TimeUnit.SECONDS),"Verification did not start");
                pending.close(); await(() -> server.health().get("pendingConnections").getAsInt()==0,"Pending socket did not close");
                try(VoiceClient offline=client(uri,"LateOwner",player,player,false,new Sink())) {
                    offline.connect().get(5,TimeUnit.SECONDS); proof.complete(true);
                    require(offline.isReady() && server.health().get("connections").getAsInt()==1
                            && server.health().get("verifiedConnections").getAsInt()==0,"Closed claimant reclaimed a live session");
                }
            }
        }
    }
    private static void verifyPendingBound() throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,true,2,1,(c,n,a) -> CompletableFuture.completedFuture(false))) {
            URI uri=start(server); List<java.net.http.WebSocket> sockets=new ArrayList<>();
            HttpClient http=HttpClient.newHttpClient();
            try {
                for(int i=0;i<17;i++) {
                    var socket=http.newWebSocketBuilder().header("X-Real-IP","203.0.113."+(i+1))
                            .buildAsync(uri,new java.net.http.WebSocket.Listener() {}).get(5,TimeUnit.SECONDS);
                    sockets.add(socket);
                }
                await(() -> server.health().get("pendingConnections").getAsInt()==16,"Pending handshake bound");
                require(server.health().get("connections").getAsInt()==0,"Pending clients entered voice");
            } finally { sockets.forEach(java.net.http.WebSocket::abort); http.close(); }
        }
    }
    private static final class RawGuest implements java.net.http.WebSocket.Listener,AutoCloseable {
        final HttpClient http=HttpClient.newHttpClient(); final URI uri; final UUID player; final String name; final boolean forged;
        final CompletableFuture<JsonObject> welcome=new CompletableFuture<>(); final StringBuilder text=new StringBuilder();
        java.net.http.WebSocket ws;
        RawGuest(URI uri,UUID player,String name,boolean forged) { this.uri=uri; this.player=player; this.name=name; this.forged=forged; }
        void open() throws Exception { ws=http.newWebSocketBuilder().buildAsync(uri,this).get(5,TimeUnit.SECONDS); }
        @Override public CompletionStage<?> onText(java.net.http.WebSocket socket,CharSequence data,boolean last) {
            text.append(data);
            if(last) {
                JsonObject j=Wire.parse(text.toString()); text.setLength(0);
                if(j.get("type").getAsString().equals("challenge")) {
                    JsonObject join=Wire.message("join"); join.addProperty("player",player.toString()); join.addProperty("name",name);
                    join.addProperty("room",ROOM); join.addProperty("dimension","minecraft:overworld");
                    join.addProperty("key",Wire.base64(PeerCrypto.keyPair().getPublic().getEncoded()));
                    join.addProperty("online",false); join.addProperty("verified",forged);
                    socket.sendText(Wire.GSON.toJson(join),true);
                } else if(j.get("type").getAsString().equals("welcome"))welcome.complete(j);
            }
            socket.request(1); return null;
        }
        @Override public void close() { if(ws!=null)ws.abort(); http.close(); }
    }
    private static void reject(VoiceClient client,String message) throws Exception {
        try { client.connect().get(5,TimeUnit.SECONDS); throw new AssertionError(message); }
        catch(ExecutionException expected) {}
    }
    private static void await(BooleanSupplier condition,String message) throws Exception {
        long deadline=System.nanoTime()+Duration.ofSeconds(8).toNanos();
        while(!condition.getAsBoolean()) { if(System.nanoTime()>deadline)throw new AssertionError(message); Thread.sleep(20); }
    }
    private static void require(boolean condition,String message) { if(!condition)throw new AssertionError(message); }
}
