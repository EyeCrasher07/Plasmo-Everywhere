package dev.eyecrasher.everywhere.relay;

import com.google.gson.JsonObject;
import dev.eyecrasher.everywhere.core.VoiceClient;
import dev.eyecrasher.everywhere.core.Wire;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Bounded synthetic relay test. Never accepts an external target or records audio. */
public final class ReleaseLoadTest {
    private static final int MAGIC=0x50454c31, PAYLOAD_BYTES=96, GROUP_SIZE=25, MAX_DELIVERIES_PER_SECOND=4500;
    private static final long LATE_NANOS=TimeUnit.MILLISECONDS.toNanos(250);
    private record Options(int clients,int seconds,boolean udp,int pps,int speakersPerRoom) {}
    private static final class Sink implements VoiceClient.Listener {
        final int group,frames;
        final Map<Integer,UUID> speakers;
        final Map<Integer,BitSet> seen=new HashMap<>();
        final List<Long> latencies=new ArrayList<>();
        long invalid,duplicates,late;
        volatile String closed="";
        Sink(int group,int frames,Map<Integer,UUID> speakers) {
            this.group=group; this.frames=frames; this.speakers=Map.copyOf(speakers);
            speakers.keySet().forEach(speaker -> seen.put(speaker,new BitSet(frames)));
        }
        public synchronized void audio(VoiceClient.PeerInfo peer,long sequence,int distance,byte[] data,boolean end) {
            if(end)return;
            if(data.length!=PAYLOAD_BYTES) { invalid++; return; }
            ByteBuffer b=ByteBuffer.wrap(data);
            if(b.getInt()!=MAGIC || b.getInt()!=group) { invalid++; return; }
            int speaker=b.getInt();
            if(!peer.player().equals(speakers.get(speaker)) || b.getLong()!=sequence
                    || sequence<0 || sequence>=frames || distance!=16) { invalid++; return; }
            long elapsed=System.nanoTime()-b.getLong();
            if(elapsed<0) { invalid++; return; }
            while(b.hasRemaining())if(b.get()!=(byte)0x5a) { invalid++; return; }
            BitSet speakerSeen=seen.get(speaker);
            if(speakerSeen.get((int)sequence)) { duplicates++; return; }
            speakerSeen.set((int)sequence); latencies.add(elapsed); if(elapsed>LATE_NANOS)late++;
        }
        public void closed(String reason) { closed=reason; }
        synchronized long received() { return latencies.size(); }
        synchronized List<Long> samples() { return List.copyOf(latencies); }
    }
    private record Participant(int index,UUID player,VoiceClient client,Sink sink) {}
    private record Group(int id,List<Participant> members,List<Participant> speakers,Set<UUID> recipients) {}

    public static void main(String[] args) throws Exception {
        if(args.length==1 && args[0].equals("--help")) {
            System.out.println("ReleaseLoadTest --clients=2..100 --seconds=1..60 --udp=true|false --pps=1..20 --speakersPerRoom=1..25");
            System.out.println("Defaults: 10 clients, 30 seconds, UDP, 20 frames/s/group. Own loopback relay only.");
            System.out.println("Default one speaker per <=25-client room; 1100 ms connection spacing. No real audio or external URL.");
            System.out.println("Synthetic delivery rate is bounded to "+MAX_DELIVERIES_PER_SECOND+"/s, below the relay UDP budget.");
            return;
        }
        Options options=parse(args);
        JsonObject report=run(options);
        System.out.println("RELEASE_LOAD_RESULT "+report);
        if(report.get("missingDeliveries").getAsLong()!=0 || report.get("invalidFrames").getAsLong()!=0
                || report.get("duplicateFrames").getAsLong()!=0 || report.get("unexpectedCloses").getAsLong()!=0
                || report.get("connectionsAfterCleanup").getAsInt()!=0)
            throw new AssertionError("Synthetic relay load check failed; see aggregate report");
    }
    private static Options parse(String[] args) {
        Map<String,String> values=new HashMap<>();
        for(String arg:args) {
            int equal=arg.indexOf('=');
            if(!arg.startsWith("--") || equal<3)throw new IllegalArgumentException("Expected --name=value; use --help");
            String name=arg.substring(2,equal),value=arg.substring(equal+1);
            if(!Set.of("clients","seconds","udp","pps","speakersPerRoom").contains(name) || values.putIfAbsent(name,value)!=null)
                throw new IllegalArgumentException("Unknown or duplicate option");
        }
        String udp=values.getOrDefault("udp","true");
        if(!udp.equals("true") && !udp.equals("false"))throw new IllegalArgumentException("udp must be true or false");
        Options options=new Options(number(values,"clients",10,2,100),number(values,"seconds",30,1,60),Boolean.parseBoolean(udp),
                number(values,"pps",20,1,20),number(values,"speakersPerRoom",1,1,GROUP_SIZE));
        int deliveriesPerSecond=0;
        for(int start=0;start<options.clients;start+=GROUP_SIZE) {
            int members=Math.min(GROUP_SIZE,options.clients-start);
            deliveriesPerSecond+=Math.min(members,options.speakersPerRoom)*(members-1)*options.pps;
        }
        if(deliveriesPerSecond>MAX_DELIVERIES_PER_SECOND)
            throw new IllegalArgumentException("Synthetic delivery rate exceeds bounded "+MAX_DELIVERIES_PER_SECOND+"/s limit");
        return options;
    }
    private static int number(Map<String,String> values,String name,int fallback,int min,int max) {
        int value=Integer.parseInt(values.getOrDefault(name,Integer.toString(fallback)));
        if(value<min || value>max)throw new IllegalArgumentException(name+" outside bounded range");
        return value;
    }
    private static JsonObject run(Options options) throws Exception {
        List<Participant> participants=new ArrayList<>(); List<Group> groups=new ArrayList<>();
        int frames=options.seconds*options.pps;
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,options.clients,0,
                (challenge,name,account) -> CompletableFuture.completedFuture(false))) {
            server.start(); await(() -> server.getPort()>0,10,"Local relay startup");
            URI uri=URI.create("ws://127.0.0.1:"+server.getPort()+"/voice");
            long setupStarted=System.nanoTime(),lastConnect=0;
            try {
                for(int start=0;start<options.clients;start+=GROUP_SIZE) {
                    int id=groups.size(),end=Math.min(start+GROUP_SIZE,options.clients);
                    String room=String.format(Locale.ROOT,"%064x",id+1);
                    Map<Integer,UUID> speakers=new HashMap<>();
                    for(int index=start;index<Math.min(end,start+options.speakersPerRoom);index++)
                        speakers.put(index,Wire.offlineUuid("Load"+index));
                    List<Participant> members=new ArrayList<>(); Set<UUID> recipients=new HashSet<>();
                    for(int index=start;index<end;index++) {
                        if(lastConnect!=0)sleepUntil(lastConnect+TimeUnit.MILLISECONDS.toNanos(1100));
                        lastConnect=System.nanoTime(); String name="Load"+index; UUID player=Wire.offlineUuid(name);
                        Map<Integer,UUID> expectedSpeakers=new HashMap<>(speakers); expectedSpeakers.remove(index);
                        Sink sink=new Sink(id,frames,expectedSpeakers);
                        VoiceClient client=new VoiceClient(uri,null,
                                new VoiceClient.Identity(player,name,player,name,room,"minecraft:overworld"),
                                false,options.udp,challenge -> CompletableFuture.completedFuture(false),sink);
                        Participant participant=new Participant(index,player,client,sink);
                        participants.add(participant); members.add(participant);
                        recipients.add(player);
                        client.connect().get(10,TimeUnit.SECONDS);
                        if(participants.size()%10==0)System.out.println("Local load setup: "+participants.size()+"/"+options.clients+" clients");
                    }
                    groups.add(new Group(id,List.copyOf(members),List.copyOf(members.subList(0,speakers.size())),Set.copyOf(recipients)));
                    for(Participant member:members) {
                        Set<UUID> nearby=new HashSet<>(); for(Participant other:members)if(other!=member)nearby.add(other.player);
                        member.client.nearby(nearby);
                    }
                }
                await(() -> groups.stream().allMatch(g -> g.members.stream().allMatch(p -> p.client.peers().size()==g.members.size()-1)),15,"Room discovery");
                if(options.udp)await(() -> participants.stream().allMatch(p -> p.client.hasUdp()),10,"UDP binding");
                long setupNanos=System.nanoTime()-setupStarted;
                JsonObject before=server.health(); long started=System.nanoTime();
                long intended=0;
                for(int frame=0;frame<frames;frame++) {
                    sleepUntil(started+(long)frame*TimeUnit.SECONDS.toNanos(1)/options.pps);
                    for(Group group:groups) {
                        for(Participant speaker:group.speakers) {
                            byte[] payload=new byte[PAYLOAD_BYTES]; Arrays.fill(payload,(byte)0x5a);
                            ByteBuffer.wrap(payload).putInt(MAGIC).putInt(group.id).putInt(speaker.index)
                                    .putLong(frame).putLong(System.nanoTime());
                            intended+=group.recipients.size()-1;
                            speaker.client.sendAudio(group.recipients,frame,16,payload,false);
                        }
                    }
                }
                sleepUntil(started+TimeUnit.SECONDS.toNanos(options.seconds));
                long expected=intended;
                awaitDelivery(participants,expected,3);
                JsonObject after=server.health(); List<Long> latencies=new ArrayList<>();
                long invalid=0,duplicates=0,late=0,unexpectedCloses=0;
                for(Participant participant:participants) {
                    Sink sink=participant.sink; latencies.addAll(sink.samples());
                    invalid+=sink.invalid; duplicates+=sink.duplicates; late+=sink.late;
                    if(!participant.client.isReady() || !sink.closed.isEmpty())unexpectedCloses++;
                }
                Collections.sort(latencies);
                JsonObject result=new JsonObject(); result.addProperty("scope","own-loopback-relay-synthetic");
                result.addProperty("clients",options.clients); result.addProperty("rooms",groups.size());
                result.addProperty("requestedSpeakersPerRoom",options.speakersPerRoom);
                int speakers=groups.stream().mapToInt(g -> g.speakers.size()).sum();
                result.addProperty("speakers",speakers); result.addProperty("sentAudioFrames",(long)speakers*frames);
                result.addProperty("seconds",options.seconds); result.addProperty("framesPerSpeakerPerSecond",options.pps);
                result.addProperty("payloadBytes",PAYLOAD_BYTES); result.addProperty("directEnabled",false);
                result.addProperty("requestedTransport",options.udp?"UDP":"WS-loopback");
                result.addProperty("setupSeconds",setupNanos/1_000_000_000.0);
                result.addProperty("measurementSeconds",(System.nanoTime()-started)/1_000_000_000.0);
                result.addProperty("intendedDeliveries",intended); result.addProperty("receivedDeliveries",latencies.size());
                result.addProperty("intendedDeliveriesPerSecond",intended/options.seconds);
                result.addProperty("missingDeliveries",intended-latencies.size()); result.addProperty("invalidFrames",invalid);
                result.addProperty("duplicateFrames",duplicates); result.addProperty("lateOver250ms",late);
                result.addProperty("unexpectedCloses",unexpectedCloses);
                result.addProperty("latencyMeasure","same-JVM monotonic one-way delivery, not internet RTT");
                result.addProperty("p50Millis",percentile(latencies,.50)); result.addProperty("p95Millis",percentile(latencies,.95));
                result.addProperty("p99Millis",percentile(latencies,.99)); result.addProperty("maxMillis",percentile(latencies,1));
                for(String name:List.of("relayedUdpVoicePackets","relayedWssVoicePackets"))
                    result.addProperty(name,after.get(name).getAsLong()-before.get(name).getAsLong());
                result.addProperty("queueMetricsAvailable",false);
                result.addProperty("wholeTestJvmHeapBytes",ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                result.addProperty("connectionsBeforeCleanup",after.get("connections").getAsInt());
                for(Participant participant:participants)participant.client.close();
                await(() -> server.health().get("connections").getAsInt()==0,10,"Connection cleanup");
                result.addProperty("connectionsAfterCleanup",server.health().get("connections").getAsInt());
                return result;
            } finally { for(Participant participant:participants)participant.client.close(); }
        }
    }
    private static double percentile(List<Long> sorted,double percentile) {
        return sorted.isEmpty()?0:sorted.get(Math.max(0,(int)Math.ceil(sorted.size()*percentile)-1))/1_000_000.0;
    }
    private static void awaitDelivery(List<Participant> participants,long expected,int seconds) throws InterruptedException {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);
        while(participants.stream().mapToLong(p -> p.sink.received()).sum()<expected && System.nanoTime()<deadline)Thread.sleep(20);
    }
    private static void await(BooleanSupplier condition,int seconds,String label) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);
        while(!condition.getAsBoolean()) {
            if(System.nanoTime()>=deadline)throw new AssertionError(label);
            Thread.sleep(25);
        }
    }
    private static void sleepUntil(long deadline) throws InterruptedException {
        long remaining=deadline-System.nanoTime(); if(remaining>0)TimeUnit.NANOSECONDS.sleep(remaining);
    }
}
