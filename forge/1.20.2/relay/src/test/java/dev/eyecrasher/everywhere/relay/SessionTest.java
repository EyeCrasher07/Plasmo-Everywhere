package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Same UUIDs on independent Minecraft servers, plus authenticated replacement of one's own session. */
public final class SessionTest {
    private static final String A="1".repeat(64), B="2".repeat(64);
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        volatile String closed="";
        public void audio(VoiceClient.PeerInfo p,long seq,int distance,byte[] data,boolean end){if(!end)audio.add(data);}
        public void closed(String reason){closed=reason;}
    }
    public static void main(String[] args) throws Exception {
        if(args.length>0) { verifyRooms(URI.create(args[0]),null,false); System.out.println("Public session checks passed"); return; }
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,4,1,
                (challenge,name,account) -> CompletableFuture.completedFuture(true))) {
            verifyRooms(start(server),server,true);
        }
        verifyUnverifiedFallback(); verifyVerified(); verifyLease();
        System.out.println("Session checks passed: independent servers, unverified auth fallback, owned reconnect, forged claims, verified ownership and idle leases");
    }
    private static VoiceClient client(URI uri,String name,UUID player,UUID account,String room,boolean online,Sink sink,VoiceClient.Resumption resume) throws Exception {
        boolean local=uri.getScheme().equals("ws");
        return new VoiceClient(uri,null,new VoiceClient.Identity(player,name,account,name,room,"minecraft:overworld"),
                local,local,c -> CompletableFuture.completedFuture(online),sink,List.of(),resume);
    }
    private static URI start(RelayServer server) throws Exception {
        server.start(); await(() -> server.getPort()>0,"Session server start");
        return URI.create("ws://127.0.0.1:"+server.getPort()+"/voice");
    }
    private static void verifyRooms(URI uri,RelayServer server,boolean verifiedA) throws Exception {
        UUID actor=Wire.offlineUuid("RoomActor"),friend=Wire.offlineUuid("RoomFriend"),account=UUID.randomUUID();
        Sink sa=new Sink(),sb=new Sink(),sfa=new Sink(),sfb=new Sink();
        try(VoiceClient ca=client(uri,"RoomActor",actor,account,A,verifiedA,sa,null);
            VoiceClient cb=client(uri,"RoomActor",actor,actor,B,false,sb,null);
            VoiceClient fa=client(uri,"RoomFriend",friend,friend,A,false,sfa,null);
            VoiceClient fb=client(uri,"RoomFriend",friend,friend,B,false,sfb,null)) {
            var opening=ca.connect(); require(opening==ca.connect(),"connect() opened more than one transport"); opening.get(10,TimeUnit.SECONDS);
            for(var c:List.of(cb,fa,fb))c.connect().get(10,TimeUnit.SECONDS);
            ca.nearby(Set.of(friend)); cb.nearby(Set.of(friend)); fa.nearby(Set.of(actor)); fb.nearby(Set.of(actor));
            await(() -> only(ca,fa) && only(fa,ca) && only(cb,fb) && only(fb,cb),"Same nick/UUID mixed across Minecraft servers");
            if(server!=null)await(() -> direct(ca) && direct(fa) && direct(cb) && direct(fb),"Independent direct paths on both Minecraft servers");
            require(ca.diagnostics().verified()==verifiedA && !cb.diagnostics().verified(),"Verification crossed server boundary");
            byte[] one={1,2,3},two={4,5,6};
            ca.sendAudio(Set.of(friend),0,16,one,false); cb.sendAudio(Set.of(friend),0,16,two,false);
            received(sfa,one,"Room A audio"); received(sfb,two,"Room B audio");
            fa.sendAudio(Set.of(actor),0,16,one,false); fb.sendAudio(Set.of(actor),0,16,two,false);
            received(sa,one,"Room A reverse audio"); received(sb,two,"Room B reverse audio");
            require(sfa.audio.poll(150,TimeUnit.MILLISECONDS)==null && sfb.audio.poll(150,TimeUnit.MILLISECONDS)==null,"Cross-room audio leak");
            if(server==null)Thread.sleep(1100); // Respect the live node's per-IP handshake budget.
            Sink duplicateSink=new Sink();
            try(var duplicate=client(uri,"RoomActor",actor,actor,B,false,duplicateSink,null)) {
                reject(duplicate,"Same-server duplicate admitted without ownership proof");
                await(() -> !duplicateSink.closed.isEmpty(),"Duplicate close callback");
                require(duplicateSink.closed.contains("already has a session"),"Missing useful duplicate rejection reason");
            }
            if(server!=null) {
                try(RawGuest forged=new RawGuest(uri,actor,"RoomActor",B,cb.sessionId())) {
                    forged.open(); require(forged.closed.get(5,TimeUnit.SECONDS).equals("This player already has a session"),"Forged reconnect admitted");
                }
                Thread.sleep(1100); // This test now makes seven handshakes from one loopback IP.
            } else Thread.sleep(1100);
            Sink resumedSink=new Sink();
            try(var resumed=client(uri,"RoomActor",actor,actor,B,false,resumedSink,cb.resumption())) {
                resumed.connect().get(10,TimeUnit.SECONDS); resumed.nearby(Set.of(friend));
                await(() -> !cb.isReady() && only(fb,resumed),"Owned offline reconnect did not replace old session");
                if(server!=null)await(() -> direct(resumed) && direct(fb),"Direct path resumed with fresh session keys");
                require(!resumed.sessionId().equals(cb.sessionId()),"Reconnect reused encryption session");
                require(ca.isReady() && only(fa,ca),"Reconnect on B displaced same identity on A");
                resumed.sendAudio(Set.of(friend),1,16,two,false); received(sfb,two,"Resumed audio");
                fb.sendAudio(Set.of(actor),1,16,two,false); received(resumedSink,two,"Resumed reverse audio");
                if(server!=null)require(server.health().get("connections").getAsInt()==4,"Reconnect changed occupied capacity");
            }
            require(ca.isReady() && only(fa,ca),"Closing B removed A's same-name session");
            ca.sendAudio(Set.of(friend),2,16,one,false); received(sfa,one,"Other server continued after close");
        }
        System.out.println("Same nick and UUID on two servers: independent admission/audio/close; owned reconnect without an extra slot passed");
    }
    private static void verifyUnverifiedFallback() throws Exception {
        UUID player=Wire.offlineUuid("FallbackOwner"),account=UUID.randomUUID(),witness=Wire.offlineUuid("FallbackFriend");
        AtomicInteger proofs=new AtomicInteger();
        RelayServer.AccountVerifier verifier=(challenge,name,id) -> {
            proofs.incrementAndGet();
            return CompletableFuture.completedFuture(name.equals("FallbackOwner") && id.equals(account));
        };
        // A launcher may report success for a non-Mojang session service. The relay must reject
        // that online claim; a fresh offline attempt may use only the normal unverified policy.
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,3,1,verifier)) {
            URI uri=start(server); Sink witnessSink=new Sink(),claimSink=new Sink(),fallbackSink=new Sink();
            UUID alternateAccount=UUID.randomUUID();
            try(var friend=client(uri,"FallbackFriend",witness,witness,A,false,witnessSink,null);
                var claim=client(uri,"FallbackOwner",player,alternateAccount,A,true,claimSink,null)) {
                friend.connect().get(5,TimeUnit.SECONDS);
                reject(claim,claimSink,"Account verification failed","Unproven online claim admitted");
                await(() -> server.health().get("pendingConnections").getAsInt()==0,"Rejected auth left a pending connection");
                require(server.health().get("connections").getAsInt()==1
                        && server.health().get("verifiedConnections").getAsInt()==0,"Failed proof consumed capacity or became verified");
                try(var fallback=client(uri,"FallbackOwner",player,alternateAccount,A,false,fallbackSink,null)) {
                    fallback.connect().get(5,TimeUnit.SECONDS); fallback.nearby(Set.of(witness)); friend.nearby(Set.of(player));
                    await(() -> only(friend,fallback) && only(fallback,friend),"Unverified fallback discovery");
                    require(!fallback.diagnostics().verified() && friend.peers().stream().noneMatch(VoiceClient.PeerInfo::verified),
                            "Fallback inherited verified status");
                    require(proofs.get()==1,"Offline fallback performed or reused an account proof");
                    byte[] audio={6,2,9}; fallback.sendAudio(Set.of(witness),0,16,audio,false);
                    received(witnessSink,audio,"Fallback audio");
                    friend.sendAudio(Set.of(player),0,16,audio,false); received(fallbackSink,audio,"Fallback reverse audio");
                    Sink ownerSink=new Sink(),duplicateSink=new Sink();
                    try(var owner=client(uri,"FallbackOwner",player,account,A,true,ownerSink,null)) {
                        owner.connect().get(5,TimeUnit.SECONDS); owner.nearby(Set.of(witness));
                        await(() -> !fallback.isReady() && only(friend,owner),"Verified owner could not reclaim fallback identity");
                        require(owner.diagnostics().verified() && friend.peers().iterator().next().verified(),"Verified reclaim lost its status");
                        try(var duplicate=client(uri,"FallbackOwner",player,alternateAccount,A,false,duplicateSink,fallback.resumption())) {
                            reject(duplicate,duplicateSink,"This player already has a session","Fallback displaced verified owner");
                        }
                        require(owner.isReady() && server.health().get("connections").getAsInt()==2
                                && server.health().get("verifiedConnections").getAsInt()==1,"Fallback changed verified ownership/accounting");
                    }
                }
            }
        }
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,1,verifier)) {
            URI uri=start(server); Sink claimSink=new Sink(),fallbackSink=new Sink(); UUID alternateAccount=UUID.randomUUID();
            try(var friend=client(uri,"FallbackFriend",witness,witness,A,false,new Sink(),null);
                var claim=client(uri,"FallbackOwner",player,alternateAccount,A,true,claimSink,null);
                var fallback=client(uri,"FallbackOwner",player,alternateAccount,A,false,fallbackSink,null);
                var owner=client(uri,"FallbackOwner",player,account,A,true,new Sink(),null)) {
                friend.connect().get(5,TimeUnit.SECONDS);
                reject(claim,claimSink,"Account verification failed","Unproven claim took reserved slot");
                reject(fallback,fallbackSink,"Remaining slots reserved for verified accounts","Fallback took verified reserve");
                require(server.health().get("connections").getAsInt()==1
                        && server.health().get("verifiedConnections").getAsInt()==0,"Rejected fallback changed capacity/accounting");
                owner.connect().get(5,TimeUnit.SECONDS);
                require(owner.diagnostics().verified() && server.health().get("connections").getAsInt()==2
                        && server.health().get("verifiedConnections").getAsInt()==1,"Fallback prevented genuine owner from using reserve");
            }
        }
        System.out.println("Failed online proof: fresh unverified retry exchanges audio; no verified status, reserve access or verified-owner displacement");
    }
    private static void verifyVerified() throws Exception {
        UUID account=UUID.randomUUID(),player=Wire.offlineUuid("ResumeOwner"),witness=Wire.offlineUuid("ResumeWitness");
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,2,1,
                (c,n,a) -> CompletableFuture.completedFuture(true))) {
            URI uri=start(server); Sink ownerSink=new Sink(),witnessSink=new Sink();
            try(var owner=client(uri,"ResumeOwner",player,account,A,true,ownerSink,null)) {
                owner.connect().get(5,TimeUnit.SECONDS); var oldSocket=server.getConnections().iterator().next();
                try(var peer=client(uri,"ResumeWitness",witness,witness,A,false,witnessSink,null)) {
                    peer.connect().get(5,TimeUnit.SECONDS); peer.nearby(Set.of(player)); owner.nearby(Set.of(witness));
                    await(() -> only(peer,owner),"Verified resume witness");
                    var credential=owner.resumption();
                    try(var unverified=client(uri,"ResumeOwner",player,account,A,false,new Sink(),credential)) {
                        reject(unverified,"Unverified reconnect inherited verified ownership");
                    }
                    try(var unrelated=client(uri,"ResumeOwner",player,UUID.randomUUID(),A,true,new Sink(),credential)) {
                        reject(unrelated,"Another verified account used the owner's reconnect credential");
                    }
                    try(var resumed=client(uri,"ResumeOwner",player,account,A,true,new Sink(),credential)) {
                        resumed.connect().get(5,TimeUnit.SECONDS); resumed.nearby(Set.of(witness));
                        await(() -> !owner.isReady() && only(peer,resumed),"Verified owner reconnect");
                        server.onClose(oldSocket,1008,"Session replaced by reconnect",false);
                        try(var stale=client(uri,"ResumeOwner",player,account,A,true,new Sink(),credential)) {
                            reject(stale,"Stale credential displaced current session");
                        }
                        byte[] audio={7,4}; resumed.sendAudio(Set.of(witness),0,16,audio,false); received(witnessSink,audio,"Verified audio after late cleanup");
                        require(server.health().get("connections").getAsInt()==2 && server.health().get("verifiedConnections").getAsInt()==1,
                                "Verified reconnect bypassed capacity/accounting");
                    }
                }
            }
        }
        System.out.println("Verified reconnect: fresh Mojang proof, same account, stale credential rejection and late onClose cleanup passed");
    }
    private static void verifyLease() throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,0,null)) {
            URI uri=start(server);
            try(RawGuest guest=new RawGuest(uri,Wire.offlineUuid("SilentGuest"),"SilentGuest",A,null)) {
                guest.open(); guest.welcome.get(5,TimeUnit.SECONDS);
                server.getConnections().iterator().next().sendPing();
                await(() -> guest.pings.get()>0,"WebSocket is alive");
                long now=System.nanoTime(); server.expire(now+TimeUnit.SECONDS.toNanos(29));
                require(server.health().get("connections").getAsInt()==1,"Idle lease expired early");
                server.expire(now+TimeUnit.SECONDS.toNanos(31));
                require(guest.closed.get(5,TimeUnit.SECONDS).equals("Voice heartbeat timed out"),"Idle app session kept alive by WebSocket pongs");
                require(server.health().get("connections").getAsInt()==0,"Idle lease leaked player slot");
            }
        }
        System.out.println("Idle sessions: WebSocket pong alone cannot retain player slot beyond the application heartbeat lease");
    }
    private static final class RawGuest implements java.net.http.WebSocket.Listener,AutoCloseable {
        final HttpClient http=HttpClient.newHttpClient(); final URI uri; final UUID player,previous;
        final String name,room; final StringBuilder text=new StringBuilder(); final AtomicInteger pings=new AtomicInteger();
        final CompletableFuture<Void> welcome=new CompletableFuture<>(); final CompletableFuture<String> closed=new CompletableFuture<>();
        java.net.http.WebSocket socket;
        RawGuest(URI uri,UUID player,String name,String room,UUID previous){this.uri=uri;this.player=player;this.name=name;this.room=room;this.previous=previous;}
        void open() throws Exception {socket=http.newWebSocketBuilder().buildAsync(uri,this).get(5,TimeUnit.SECONDS);}
        public CompletionStage<?> onText(java.net.http.WebSocket ws,CharSequence data,boolean last) {
            text.append(data);
            if(last) {
                var j=Wire.parse(text.toString());text.setLength(0);
                if(j.get("type").getAsString().equals("challenge")) {
                    var join=Wire.message("join"); join.addProperty("player",player.toString()); join.addProperty("name",name);
                    join.addProperty("room",room); join.addProperty("dimension","minecraft:overworld"); join.addProperty("online",false);
                    join.addProperty("key",Wire.base64(PeerCrypto.keyPair().getPublic().getEncoded()));
                    if(previous!=null) {join.addProperty("previousSession",previous.toString());join.addProperty("sessionProof",Wire.base64(Wire.random(32)));}
                    ws.sendText(join.toString(),true);
                } else if(j.get("type").getAsString().equals("welcome"))welcome.complete(null);
            }
            ws.request(1);return null;
        }
        public CompletionStage<?> onPing(java.net.http.WebSocket ws,ByteBuffer data){pings.incrementAndGet();ws.request(1);return ws.sendPong(data);}
        public CompletionStage<?> onClose(java.net.http.WebSocket ws,int code,String reason){closed.complete(reason);return null;}
        public void close(){if(socket!=null)socket.abort();http.close();}
    }
    private static boolean only(VoiceClient a,VoiceClient b){return a.peers().size()==1 && a.peers().iterator().next().session().equals(b.sessionId());}
    private static boolean direct(VoiceClient client){return client.hasUdp() && client.peers().stream().anyMatch(VoiceClient.PeerInfo::direct);}
    private static void received(Sink sink,byte[] bytes,String label) throws Exception {require(Arrays.equals(sink.audio.poll(5,TimeUnit.SECONDS),bytes),label);}
    private static void reject(VoiceClient client,String label) throws Exception {
        try {client.connect().get(5,TimeUnit.SECONDS);throw new AssertionError(label);}catch(ExecutionException expected){}
    }
    private static void reject(VoiceClient client,Sink sink,String reason,String label) throws Exception {
        reject(client,label); await(() -> !sink.closed.isEmpty(),"Missing rejection callback");
        require(sink.closed.equals("Voice service disconnected (1008): "+reason),label+": "+sink.closed);
    }
    private static void require(boolean ok,String label){if(!ok)throw new AssertionError(label);}
    private static void await(BooleanSupplier condition,String label) throws Exception {
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(12);
        while(!condition.getAsBoolean()){if(System.nanoTime()>until)throw new AssertionError(label);Thread.sleep(25);}
    }
}
