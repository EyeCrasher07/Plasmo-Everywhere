package dev.eyecrasher.everywhere.forge;

import dev.eyecrasher.everywhere.core.VoiceClient;
import dev.eyecrasher.everywhere.core.Wire;
import dev.eyecrasher.everywhere.core.ConnectionTrace;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** No launcher, account credentials, audio devices, or public network needed. */
public final class ClientStateTest {
    public static void main(String[] args) throws Exception {
        scopeBoundaries(); retryJitter(); preparationOwnership(); reportPrivacy(); verificationFallback(); retainedDiagnostics(); diagnosticLog(); revokedQueuedAudio();
        System.out.println("Client state: strict session boundaries, cancellable background setup, bounded retry jitter, private diagnostics, queued session revocation OK");
    }
    private static void scopeBoundaries() {
        Object connection=new Object(); UUID player=UUID.randomUUID();
        SessionScope original=new SessionScope(connection,"room-a","overworld",player);
        require(original.retains(new SessionScope(connection,"room-a","overworld",player)),"Same connection and scope must retain the session");
        require(!original.retains(new SessionScope(new Object(),"room-a","overworld",player)),"A new Minecraft connection must restart");
        require(!original.retains(new SessionScope(connection,"room-b","overworld",player)),"A new Minecraft address must restart");
        require(!original.retains(new SessionScope(connection,"room-a","the_nether",player)),"A new dimension must restart");
        require(!original.retains(new SessionScope(connection,"room-a","overworld",UUID.randomUUID())),"Another local UUID must restart");
        require(!original.retains(null),"Missing scope must not retain");
        // Deliberately equal objects do not prove that this is the same active connection.
        record EquivalentConnection(int id) {}
        require(!new SessionScope(new EquivalentConnection(1),"room","dimension",player)
                .retains(new SessionScope(new EquivalentConnection(1),"room","dimension",player)),"Connection equality is insufficient");
    }
    private static void retryJitter() {
        for(int attempt=-2;attempt<=100;attempt++) {
            int low=RetryDelay.ticks(attempt,0),high=RetryDelay.ticks(attempt,1);
            require(low>=20 && high<=750 && low<high,"Retry jitter must be bounded and nonzero");
            for(int point=0;point<=100;point++) {
                int delay=RetryDelay.ticks(attempt,point/100.0);
                require(delay>=low && delay<=high,"Retry delay outside its window");
            }
        }
        require(RetryDelay.ticks(1,0)==30 && RetryDelay.ticks(1,1)==50,"Initial retry window changed");
        require(RetryDelay.ticks(6,0)==450 && RetryDelay.ticks(6,1)==750,"Final retry window changed");
    }
    private static void preparationOwnership() throws Exception {
        Thread caller=Thread.currentThread();
        ThreadPoolExecutor worker=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(8));
        class Resource implements AutoCloseable {
            final AtomicInteger closes=new AtomicInteger();
            final CompletableFuture<Thread> closed=new CompletableFuture<>();
            @Override public void close() { closes.incrementAndGet(); closed.complete(Thread.currentThread()); }
        }
        java.util.function.Consumer<Resource> retire=resource -> worker.execute(resource::close);
        try {
            CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
            Resource cancelledResource=new Resource();
            var pending=new ConnectionPreparation<>(worker,() -> {
                require(Thread.currentThread()!=caller,"Preparation ran on the game thread");
                entered.countDown();
                // Key generation may finish despite interruption. Its late result must still close.
                while(release.getCount()!=0)try { release.await(); }catch(InterruptedException ignored){}
                return cancelledResource;
            },retire);
            require(entered.await(5,TimeUnit.SECONDS),"Background preparation did not start");
            pending.cancel(); pending.cancel(); release.countDown();
            require(cancelledResource.closed.get(5,TimeUnit.SECONDS)!=caller,"Cancellation closed a connection on the game thread");
            require(cancelledResource.closes.get()==1 && pending.claim()==null,"Cancelled startup leaked or transferred its late result");

            Resource completedResource=new Resource();
            var completed=new ConnectionPreparation<>(worker,() -> completedResource,retire);
            completed.completion().get(5,TimeUnit.SECONDS);
            completed.cancel(); completed.cancel();
            require(completedResource.closed.get(5,TimeUnit.SECONDS)!=caller,"Completed orphan closed on the game thread");
            require(completedResource.closes.get()==1 && completed.claim()==null,"Cancellation after preparation leaked or double-closed");

            Resource adoptedResource=new Resource();
            var adopted=new ConnectionPreparation<>(worker,() -> adoptedResource,retire);
            adopted.completion().get(5,TimeUnit.SECONDS);
            require(adopted.claim()==adoptedResource && adopted.claim()==null,"Connection ownership transferred more than once");
            adopted.cancel(); require(adoptedResource.closes.get()==0,"Preparation closed an installed connection");
            retire.accept(adoptedResource); adoptedResource.closed.get(5,TimeUnit.SECONDS);

            IllegalStateException failure=new IllegalStateException("private-failure");
            var failed=new ConnectionPreparation<Resource>(worker,() -> { throw failure; },retire);
            try { failed.completion().get(5,TimeUnit.SECONDS); throw new AssertionError("Setup exception was lost"); }
            catch(ExecutionException error) { require(error.getCause()==failure,"Setup exception was replaced"); }
            require(failed.claim()==null,"Failed preparation transferred a resource");

            CountDownLatch busy=new CountDownLatch(1),unblock=new CountDownLatch(1);
            worker.execute(() -> { busy.countDown(); try { unblock.await(); }catch(InterruptedException ignored){} });
            require(busy.await(5,TimeUnit.SECONDS),"Queue test did not start");
            AtomicInteger cancelledRuns=new AtomicInteger();
            var queued=new ConnectionPreparation<>(worker,() -> { cancelledRuns.incrementAndGet(); return new Resource(); },retire);
            queued.cancel();
            require(worker.getQueue().isEmpty(),"Cancelled startup retained its bounded queue slot");
            unblock.countDown();
            worker.submit(() -> {}).get(5,TimeUnit.SECONDS);
            require(cancelledRuns.get()==0,"A queued cancelled startup still constructed a connection");
        } finally {
            worker.shutdownNow(); require(worker.awaitTermination(5,TimeUnit.SECONDS),"Connection worker did not finish");
        }
    }
    private static void reportPrivacy() {
        UUID id=UUID.randomUUID(); String secret="private-token-NEVER-EXPORT";
        String bad="https://198.51.100.17:443/private?token="+secret;
        var peer=new VoiceClient.PeerDiagnostics(id,bad,"VPS-WSS",5,2,40,30,3,25,4,3,1,"waiting");
        var poison=new VoiceClient.PeerDiagnostics(UUID.randomUUID(),secret,bad,2,1,1,1,0,-1,1,0,0,bad);
        var network=new VoiceClient.ConnectionDiagnostics(true,2,1,2,true,bad,1,1,1,bad,3,bad);
        String report=DiagnosticReport.render(Map.of("plasmo_everywhere","0.1.0-beta.1","unknownCredential",secret),
                "CONNECTED","connection",2,4,10,new VoiceClient.NetworkStats(1,0,0,100,0,3),network,List.of(peer,poison),true,true,bad);
        require(!report.contains(secret) && !report.contains("198.51.100.17") && !report.contains(id.toString()),"Private identity/endpoint escaped into report");
        require(!report.contains("https:") && !report.contains("unknownCredential"),"Unexpected field escaped into report");
        var json=Wire.GSON.fromJson(report,com.google.gson.JsonObject.class);
        require(json.get("schema").getAsInt()==2,"Wrong diagnostic schema");
        require(json.getAsJsonArray("anonymousPeerPaths").size()==2,"Peer counters missing");
        require(json.getAsJsonObject("outgoingVoicePackets").get("directUdp").getAsLong()==100,"Useful packet counters missing");
        require(json.getAsJsonArray("anonymousPeerPaths").get(1).getAsJsonObject().get("route").getAsString().equals("unknown"),"Untrusted route label retained");
        String disconnected=DiagnosticReport.render(Map.of(),bad,bad,0,0,0,null,null,List.of(),false,false,"unavailable");
        require(!disconnected.contains("198.51.100.17"),"Untrusted status label retained");
    }
    private static void verificationFallback() {
        UUID account=UUID.randomUUID();
        require(VerificationPolicy.canVerifyIdentity(account,"Friend",account,"friend"),"Official identity should be eligible");
        require(VerificationPolicy.canVerifyIdentity(Wire.offlineUuid("Friend"),"Friend",account,"Friend"),"Official account on offline server should be eligible");
        require(!VerificationPolicy.canVerifyIdentity(UUID.randomUUID(),"Friend",account,"Friend"),"Custom server identity must use unverified admission");
        require(!VerificationPolicy.canVerifyIdentity(Wire.offlineUuid("Other"),"Other",account,"Friend"),"Changed offline nickname must use unverified admission");
        VerificationPolicy policy=new VerificationPolicy();
        require(policy.shouldVerify(),"New connection should try verification");
        for(String reason:List.of("Voice connection failed","Account verification failed","Voice service disconnected (1008): This player already has a session",
                "Voice service disconnected (1008): Voice capacity reached","Voice service disconnected (1000): Account verification failed"))
            require(!policy.rejected(reason)&&policy.shouldVerify(),"Unrelated error triggered auth fallback");
        require(policy.rejected("Voice service disconnected (1008): Account verification failed"),"Exact rejection did not switch mode");
        require(!policy.shouldVerify(),"Ely-style successful login would loop into verification again");
        require(!policy.rejected("Voice service disconnected (1008): Account verification failed"),"Fallback must happen only once");
        policy.reset(); require(policy.shouldVerify(),"Another Minecraft connection must be able to verify again");
    }
    private static void retainedDiagnostics() {
        ConnectionTrace trace=new ConnectionTrace(); trace.startAttempt();
        trace.record("ACCOUNT","START",0,null,0);
        trace.record("ACCOUNT","SUCCESS",42,null,0);
        trace.record("CONTROL_CLOSE","FAILED",50,null,1008);
        trace.record("ACCOUNT","FALLBACK",0,null,1008);
        var last=new DiagnosticReport.Snapshot(1,"2026-10-18T19:26:22Z",false,false,false,"normal",
                new VoiceClient.NetworkStats(0,0,0,0,0,0),null,List.of());
        String text=DiagnosticReport.renderDetailed(Map.of(),"RETRYING","verification",1,1,0,null,last,trace,false,true,true,0,false);
        var json=Wire.GSON.fromJson(text,com.google.gson.JsonObject.class);
        require(json.getAsJsonArray("timeline").size()==4,"Failed-connection history was lost");
        require(json.getAsJsonObject("lastConnection").get("attempt").getAsInt()==1,"Last failed transport not retained");
        require(json.get("accountMode").getAsString().equals("unverified-fallback"),"Fallback mode missing");
        require(json.getAsJsonObject("lastConnection").get("accountVerification").getAsString().equals("pending"),"Failed handshake incorrectly reported an account verdict");
        require(text.contains("1008")&&text.contains("42"),"Close code or auth duration missing");
    }
    private static void diagnosticLog() {
        Path folder=null;
        try {
            folder=Files.createTempDirectory("everywhere-diagnostic-test");
            Files.writeString(folder.resolve("connection.log"),"x".repeat((int)DiagnosticLog.MAX_BYTES));
            try(DiagnosticLog log=new DiagnosticLog(folder)) {
                ConnectionTrace trace=new ConnectionTrace(log::append); trace.startAttempt();
                trace.record("ACCOUNT","FAILED",12,new IllegalStateException("token-private-NEVER-LOG"),1008);
            }
            String text=Files.readString(folder.resolve("connection.log"));
            require(text.contains("ACCOUNT")&&text.contains("1008"),"Automatic log did not persist before ready");
            require(!text.contains("token-private-NEVER-LOG"),"Exception message leaked into log");
            require(Files.size(folder.resolve("connection.log"))<=DiagnosticLog.MAX_BYTES
                    &&Files.size(folder.resolve("connection.previous.log"))<=DiagnosticLog.MAX_BYTES,"Connection logs are unbounded");
        } catch(Exception error) { throw new AssertionError("Diagnostic logging failed",error); }
        finally { if(folder!=null)try { for(Path path:List.of(folder.resolve("connection.log"),folder.resolve("connection.previous.log"),folder))Files.deleteIfExists(path); }catch(Exception ignored){} }
    }
    private static void revokedQueuedAudio() {
        UUID player=UUID.randomUUID();
        var oldPeer=new VoiceClient.PeerInfo(UUID.randomUUID(),player,"same-player",true,true);
        var newPeer=new VoiceClient.PeerInfo(UUID.randomUUID(),player,"same-player",true,true);
        var other=new VoiceClient.PeerInfo(UUID.randomUUID(),UUID.randomUUID(),"other-player",false,false);
        AudioSessionGate gate=new AudioSessionGate();
        gate.update(List.of(oldPeer,other));
        var inbox=new java.util.concurrent.ArrayBlockingQueue<VoiceClient.PeerInfo>(8);
        inbox.add(oldPeer); inbox.add(other);
        // A reconnect replaces only the voice session; the Minecraft player entity stays nearby.
        gate.update(List.of(newPeer,other));
        inbox.removeIf(peer -> !gate.permits(peer));
        require(inbox.size()==1 && inbox.peek()==other,"Revoked queued voice survived replacement for the same player UUID");
        inbox.add(oldPeer); // Simulate a callback that won a race with removal from the queue.
        inbox.add(newPeer);
        List<VoiceClient.PeerInfo> played=new ArrayList<>();
        for(VoiceClient.PeerInfo next;(next=inbox.poll())!=null;)if(gate.permits(next))played.add(next);
        require(played.equals(List.of(other,newPeer)),"Playback admitted a late old-session callback");
        require(!gate.permits(new VoiceClient.PeerInfo(newPeer.session(),other.player(),"forged",true,true)),"Session ID alone was accepted for another player");
        gate.clear();
        require(!gate.permits(newPeer) && !gate.permits(other),"Old-world voice permission survived a clear");
    }
    private static void require(boolean ok,String message) { if(!ok)throw new AssertionError(message); }
}
