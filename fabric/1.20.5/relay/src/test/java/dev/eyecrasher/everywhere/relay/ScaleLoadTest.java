package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import com.sun.net.httpserver.HttpServer;
import dev.eyecrasher.everywhere.core.*;
import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Bounded synthetic admission/control/opaque-media load; never targets a public host or Mojang. */
public final class ScaleLoadTest {
    private static final int MAGIC=0x50455331, PAYLOAD_BYTES=96, MAX_FORWARD_PPS=4500;
    private static final String FIXTURE="everywhere-scale-loopback-v1";
    private record Options(String mode,int clients,int seconds,int relayed,int pps,boolean udp,int rooms,
                           int changes,int admissionMillis,int port,int healthPort,int serverSeconds,URI control,URI health) {}

    public static void main(String[] args) throws Exception {
        if(args.length==1 && args[0].equals("--help")) {
            System.out.println("ScaleLoadTest --mode=combined|server|client --clients=2..1000 --seconds=1..120");
            System.out.println("  --relayed=0..1000 --pps=1..20 --udp=true|false --rooms=1..500 --changes=0..25");
            System.out.println("  --admissionMillis=125..1500 --port=0..65535 --healthPort=0..65535 --serverSeconds=30..600");
            System.out.println("Client mode requires --control=ws://127.0.0.1:PORT/voice --health=http://127.0.0.1:PORT/scale-health");
            System.out.println("Defaults: 1000 offline synthetic clients, 1 game room, 200 relay users at 20 fps, 60 s, UDP.");
            System.out.println("Other clients exercise discovery/control only: actual P2P, TLS, Mojang and internet capacity are not measured.");
            System.out.println("Admission remains below the production 10/s budget; synthetic source headers work only in this isolated fixture.");
            return;
        }
        Options o=parse(args);
        if(o.mode.equals("server")) {
            try(Fixture fixture=new Fixture(o)) {
                System.out.println("SCALE_FIXTURE_READY "+fixture.description());
                Thread.sleep(TimeUnit.SECONDS.toMillis(o.serverSeconds));
            }
            return;
        }
        try(Fixture fixture=o.mode.equals("combined")?new Fixture(o):null) {
            URI control=fixture==null?o.control:fixture.control(),health=fixture==null?o.health:fixture.healthUri();
            try(Load load=new Load(o,control,health)) {
                JsonObject result=load.run();
                System.out.println("SCALE_LOAD_RESULT "+result);
                if(result.get("missingDeliveries").getAsLong()!=0 || result.get("invalidFrames").getAsLong()!=0
                        || result.get("duplicateFrames").getAsLong()!=0 || result.get("unexpectedCloses").getAsLong()!=0
                        || result.get("connectionsAfterCleanup").getAsInt()!=0 || result.get("unfinishedControlChanges").getAsInt()!=0)
                    throw new AssertionError("Synthetic scale check failed; see aggregate result");
            }
        }
    }

    private static Options parse(String[] args) {
        Map<String,String> v=new HashMap<>();
        Set<String> names=Set.of("mode","clients","seconds","relayed","pps","udp","rooms","changes","admissionMillis",
                "port","healthPort","serverSeconds","control","health");
        for(String arg:args) {
            int eq=arg.indexOf('=');
            if(!arg.startsWith("--") || eq<3 || !names.contains(arg.substring(2,eq))
                    || v.putIfAbsent(arg.substring(2,eq),arg.substring(eq+1))!=null)
                throw new IllegalArgumentException("Unknown or duplicate option; use --help");
        }
        String mode=v.getOrDefault("mode","combined"),udp=v.getOrDefault("udp","true");
        if(!Set.of("combined","server","client").contains(mode) || !Set.of("true","false").contains(udp))
            throw new IllegalArgumentException("Invalid mode or udp option");
        int clients=number(v,"clients",1000,2,1000),relayed=number(v,"relayed",Math.min(200,clients),0,clients);
        int rooms=number(v,"rooms",1,1,clients/2),pps=number(v,"pps",20,1,20);
        if((clients&1)!=0 || (relayed&1)!=0)throw new IllegalArgumentException("clients and relayed must be even (mutual pairs)");
        int changes=number(v,"changes",10,0,25);
        changes=Math.min(changes,(clients-relayed)/2);
        if(relayed*pps>MAX_FORWARD_PPS || Boolean.parseBoolean(udp) && relayed*pps+clients/2>MAX_FORWARD_PPS)
            throw new IllegalArgumentException("Media plus 4-second UDP binding exceeds bounded "+MAX_FORWARD_PPS+" packets/s");
        URI control=v.containsKey("control")?loopbackUri(v.get("control"),"ws","/voice"):null;
        URI health=v.containsKey("health")?loopbackUri(v.get("health"),"http","/scale-health"):null;
        if(mode.equals("client") && (control==null || health==null))throw new IllegalArgumentException("Client mode needs loopback fixture URLs");
        if(!mode.equals("client") && (control!=null || health!=null))throw new IllegalArgumentException("Targets are only allowed in client mode");
        return new Options(mode,clients,number(v,"seconds",60,1,120),relayed,pps,Boolean.parseBoolean(udp),rooms,
                changes,number(v,"admissionMillis",125,125,1500),number(v,"port",0,0,65535),
                number(v,"healthPort",0,0,65535),number(v,"serverSeconds",300,30,600),control,health);
    }
    private static int number(Map<String,String> values,String name,int fallback,int min,int max) {
        int n=Integer.parseInt(values.getOrDefault(name,Integer.toString(fallback)));
        if(n<min || n>max)throw new IllegalArgumentException(name+" outside bounded range");
        return n;
    }
    private static URI loopbackUri(String text,String scheme,String path) {
        URI u=URI.create(text);
        if(!scheme.equals(u.getScheme()) || !Set.of("127.0.0.1","[::1]","::1").contains(u.getHost())
                || u.getPort()<1 || !path.equals(u.getPath()) || u.getRawQuery()!=null || u.getRawUserInfo()!=null || u.getRawFragment()!=null)
            throw new IllegalArgumentException("Only an isolated literal-loopback fixture is accepted");
        return u;
    }

    /** Separate mode permits server-only heap/CPU measurements, including through an SSH TCP forward. */
    private static final class Fixture implements AutoCloseable {
        final RelayServer relay;
        final HttpServer health;
        final ExecutorService executor=Executors.newSingleThreadExecutor();
        Fixture(Options o) throws Exception {
            relay=new RelayServer("127.0.0.1",o.port,"127.0.0.1",0,null,true,1024,0,
                    (challenge,name,account) -> { throw new AssertionError("Synthetic clients must never request online verification"); });
            health=HttpServer.create(new InetSocketAddress("127.0.0.1",o.healthPort),16);
            health.setExecutor(executor);
            health.createContext("/scale-health",exchange -> {
                try {
                    if(!exchange.getRequestMethod().equals("GET") || !exchange.getRequestURI().toString().equals("/scale-health")) {
                        exchange.sendResponseHeaders(404,-1); return;
                    }
                    JsonObject state=relay.health(); state.addProperty("fixture",FIXTURE);
                    state.addProperty("processCpuNanos",processCpuNanos());
                    state.addProperty("heapBytes",ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                    state.addProperty("threads",ManagementFactory.getThreadMXBean().getThreadCount());
                    byte[] bytes=Wire.GSON.toJson(state).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type","application/json");
                    exchange.sendResponseHeaders(200,bytes.length); exchange.getResponseBody().write(bytes);
                } finally { exchange.close(); }
            });
            relay.start(); await(() -> relay.getPort()>0,10,"Fixture startup"); health.start();
        }
        URI control() { return URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice"); }
        URI healthUri() { return URI.create("http://127.0.0.1:"+health.getAddress().getPort()+"/scale-health"); }
        JsonObject description() {
            JsonObject j=new JsonObject(); j.addProperty("control",control().toString()); j.addProperty("health",healthUri().toString());
            j.addProperty("udpPort",relay.udpPort()); j.addProperty("fixture",FIXTURE); j.addProperty("version",BuildVersion.VERSION);
            return j;
        }
        public void close() { health.stop(0); executor.shutdownNow(); relay.close(); }
    }

    private static final class Load implements AutoCloseable {
        final Options o;
        final URI control,health;
        final ExecutorService executor=Executors.newFixedThreadPool(8);
        final ScheduledExecutorService maintenance=Executors.newSingleThreadScheduledExecutor();
        final HttpClient http=HttpClient.newBuilder().executor(executor).connectTimeout(Duration.ofSeconds(5)).build();
        final DatagramSocket udp;
        final Thread reader;
        final List<Client> clients=new CopyOnWriteArrayList<>();
        final Map<UUID,Client> bySession=new ConcurrentHashMap<>();
        final AtomicLong maintenanceErrors=new AtomicLong();
        volatile boolean closing;
        Load(Options o,URI control,URI health) throws Exception {
            this.o=o; this.control=control; this.health=health;
            udp=o.udp?new DatagramSocket(new InetSocketAddress("127.0.0.1",0)):null;
            if(udp!=null) { udp.setReceiveBufferSize(4*1024*1024); udp.setSendBufferSize(1024*1024); }
            reader=udp==null?null:Thread.ofPlatform().daemon().name("scale-shared-udp").unstarted(this::receiveUdp);
            if(reader!=null)reader.start();
            maintenance.scheduleAtFixedRate(this::maintain,100,100,TimeUnit.MILLISECONDS);
        }
        JsonObject state() throws Exception {
            HttpResponse<String> response=http.send(HttpRequest.newBuilder(health).timeout(Duration.ofSeconds(5)).GET().build(),HttpResponse.BodyHandlers.ofString());
            JsonObject state=Wire.parse(response.body());
            if(response.statusCode()!=200 || !FIXTURE.equals(state.get("fixture").getAsString())
                    || !BuildVersion.VERSION.equals(state.get("version").getAsString()))
                throw new IllegalStateException("Target is not the matching isolated scale fixture");
            if(state.get("capacity").getAsInt()<o.clients)throw new IllegalStateException("Fixture capacity is below requested clients");
            return state;
        }
        JsonObject run() throws Exception {
            JsonObject first=state();
            if(first.get("connections").getAsInt()!=0)throw new IllegalStateException("Fixture must be empty before load");
            long setup=System.nanoTime(),lastConnect=0;
            for(int index=0;index<o.clients;index++) {
                if(lastConnect!=0)sleepUntil(lastConnect+TimeUnit.MILLISECONDS.toNanos(o.admissionMillis));
                lastConnect=System.nanoTime(); Client client=new Client(this,index); clients.add(client);
                http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5)).header("X-Real-IP",syntheticAddress(index))
                        .buildAsync(control,client).get(7,TimeUnit.SECONDS);
                client.ready.get(7,TimeUnit.SECONDS);
                if(index%2==1) { clients.get(index-1).nearby(true); client.nearby(true); }
                if((index+1)%100==0)System.out.println("Scale admission "+(index+1)+"/"+o.clients);
            }
            await(() -> clients.stream().allMatch(c -> c.peer!=null && (!o.udp || c.bound)),20,"Mutual discovery / UDP binding");
            JsonObject before=state();
            if(before.get("connections").getAsInt()!=o.clients)throw new AssertionError("Not all requested clients were admitted");
            long setupNanos=System.nanoTime()-setup,start=System.nanoTime(),nextChange=start;
            int frames=o.seconds*o.pps,changeIndex=o.relayed; long intended=0,controlChanges=0,peakHeap=before.get("heapBytes").getAsLong();
            for(int frame=0;frame<frames;frame++) {
                sleepUntil(start+(long)frame*TimeUnit.SECONDS.toNanos(1)/o.pps);
                for(int index=0;index<o.relayed;index++) {
                    Client c=clients.get(index); byte[] payload=new byte[PAYLOAD_BYTES]; Arrays.fill(payload,(byte)0x5a);
                    ByteBuffer.wrap(payload).putInt(MAGIC).putInt(index).putInt(frame).putLong(System.nanoTime());
                    c.media(c.crypto.seal(Wire.VOICE,payload).encode()); intended++;
                }
                long now=System.nanoTime();
                if(o.changes>0 && now>=nextChange) {
                    nextChange=now+TimeUnit.SECONDS.toNanos(1)/o.changes;
                    Client c=clients.get(changeIndex); changeIndex+=2; if(changeIndex>=o.clients)changeIndex=o.relayed;
                    if(c.probe.compareAndSet(0,1)) { c.probeStarted=System.nanoTime(); c.nearby(false); controlChanges++; }
                }
                if(frame%o.pps==0) {
                    JsonObject current=state(); peakHeap=Math.max(peakHeap,current.get("heapBytes").getAsLong());
                    if(current.get("connections").getAsInt()!=o.clients)throw new AssertionError("Connection count dropped during measurement: "+current);
                }
            }
            sleepUntil(start+TimeUnit.SECONDS.toNanos(o.seconds));
            long expected=intended;
            long drainDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(clients.stream().mapToLong(c -> c.received.get()).sum()<expected && System.nanoTime()<drainDeadline)Thread.sleep(25);
            await(() -> clients.stream().noneMatch(c -> c.probe.get()!=0),5,"Control change drain");
            JsonObject after=state(),result=new JsonObject(); List<Long> audio=new ArrayList<>(),controlLatency=new ArrayList<>(),joinLatency=new ArrayList<>();
            long invalid=maintenanceErrors.get(),duplicates=0,closes=0;
            for(Client c:clients) {
                synchronized(c.latencies) { audio.addAll(c.latencies); }
                synchronized(c.controlLatencies) { controlLatency.addAll(c.controlLatencies); }
                joinLatency.add(c.joinNanos); invalid+=c.invalid.get(); duplicates+=c.duplicates.get();
                if(c.failed.get() || !c.socket.isInputClosed() && c.socket.isOutputClosed())closes++;
            }
            Collections.sort(audio); Collections.sort(controlLatency); Collections.sort(joinLatency);
            double elapsed=(System.nanoTime()-start)/1_000_000_000.0;
            result.addProperty("scope",o.mode.equals("combined")?"same-JVM-loopback-client-and-server":"separate-JVM-loopback-fixture");
            result.addProperty("version",BuildVersion.VERSION); result.addProperty("clients",o.clients); result.addProperty("gameRooms",o.rooms);
            result.addProperty("relayUsers",o.relayed); result.addProperty("controlOnlyUsers",o.clients-o.relayed);
            result.addProperty("transport",o.udp?"UDP":"WS"); result.addProperty("encryptedPayloadBytes",PAYLOAD_BYTES);
            result.addProperty("framesPerSecondPerRelayUser",o.pps); result.addProperty("setupSeconds",setupNanos/1_000_000_000.0);
            result.addProperty("measurementSeconds",elapsed); result.addProperty("intendedDeliveries",intended);
            result.addProperty("receivedDeliveries",audio.size()); result.addProperty("missingDeliveries",intended-audio.size());
            result.addProperty("invalidFrames",invalid); result.addProperty("duplicateFrames",duplicates); result.addProperty("unexpectedCloses",closes);
            result.addProperty("controlChanges",controlChanges); result.addProperty("completedControlChanges",controlLatency.size());
            result.addProperty("unfinishedControlChanges",clients.stream().filter(c -> c.probe.get()!=0).count());
            result.addProperty("joinP95Millis",percentile(joinLatency,.95)); result.addProperty("controlChangeP95Millis",percentile(controlLatency,.95));
            result.addProperty("mediaP50Millis",percentile(audio,.50)); result.addProperty("mediaP95Millis",percentile(audio,.95));
            result.addProperty("mediaP99Millis",percentile(audio,.99)); result.addProperty("mediaMaxMillis",percentile(audio,1));
            result.addProperty("lateOver250ms",audio.stream().filter(n -> n>TimeUnit.MILLISECONDS.toNanos(250)).count());
            result.addProperty("latencyMeasure","same-load-generator monotonic delivery, not internet RTT");
            result.addProperty("fixtureCpuCorePercent",100.0*(after.get("processCpuNanos").getAsLong()-before.get("processCpuNanos").getAsLong())/(elapsed*1_000_000_000));
            result.addProperty("fixturePeakSampledHeapBytes",peakHeap); result.addProperty("fixtureThreads",after.get("threads").getAsInt());
            result.addProperty("resourceMeasure",o.mode.equals("combined")?"includes load generator and relay":"isolated relay fixture process only");
            for(String name:List.of("relayedUdpVoicePackets","relayedWssVoicePackets"))
                result.addProperty(name,after.get(name).getAsLong()-before.get(name).getAsLong());
            for(String name:List.of("peerRefreshes","peerDescriptions","nearbyDeclarations","udpBudgetDrops","relayQueueDrops","controlQueueDrops"))
                if(before.has(name) && after.has(name))result.addProperty(name,after.get(name).getAsLong()-before.get(name).getAsLong());
            result.addProperty("connectionsBeforeCleanup",after.get("connections").getAsInt());
            closing=true; maintenance.shutdownNow();
            for(Client c:clients)c.socket.sendClose(WebSocket.NORMAL_CLOSURE,"Scale test complete");
            long cleanup=System.nanoTime();
            await(() -> { try { return state().get("connections").getAsInt()==0; } catch(Exception e) { return false; } },15,"Session cleanup");
            result.addProperty("cleanupSeconds",(System.nanoTime()-cleanup)/1_000_000_000.0);
            result.addProperty("connectionsAfterCleanup",state().get("connections").getAsInt());
            return result;
        }
        private void maintain() {
            if(closing)return;
            long now=System.nanoTime();
            for(Client c:clients)if(c.session!=null && !c.failed.get()) {
                try {
                    if(now-c.lastHeartbeat>TimeUnit.SECONDS.toNanos(8)) { c.lastHeartbeat=now; c.send(Wire.message("heartbeat")); }
                    if(udp!=null && now-c.lastBind>TimeUnit.SECONDS.toNanos(4)) { c.lastBind=now; c.datagram(Wire.BIND,new byte[0]); }
                } catch(Exception failure) { maintenanceErrors.incrementAndGet(); c.fail(failure); }
            }
        }
        private void receiveUdp() {
            byte[] buffer=new byte[Wire.MAX_UDP+1];
            while(!closing)try {
                DatagramPacket packet=new DatagramPacket(buffer,buffer.length); udp.receive(packet);
                if(packet.getLength()>Wire.MAX_UDP)continue;
                byte[] bytes=Arrays.copyOf(buffer,packet.getLength()); Client c=bySession.get(Wire.datagramSession(bytes));
                if(c==null || packet.getPort()!=c.udpPort)continue;
                Wire.Datagram d=Wire.openDatagram(bytes,c.udpKey,true);
                if(!c.replay.accept(d.sequence())) { c.duplicates.incrementAndGet(); continue; }
                if(d.op()==Wire.CHALLENGE)c.datagram(Wire.CONFIRM,d.body());
                else if(d.op()==Wire.BOUND && !c.bound) {
                    c.bound=true; JsonObject path=Wire.message("path"); path.addProperty("udp",true); c.send(path);
                } else if(d.op()==Wire.ROUTE)c.receive(Wire.decode(d.body()));
            } catch(Exception failure) { if(!closing)maintenanceErrors.incrementAndGet(); }
        }
        public void close() {
            closing=true; maintenance.shutdownNow(); if(udp!=null)udp.close();
            for(Client c:clients)if(c.socket!=null)c.socket.abort();
            http.shutdownNow(); executor.shutdownNow();
        }
    }

    private static final class Client implements WebSocket.Listener {
        final Load load;
        final int index;
        final String name,room;
        final UUID player,partner;
        final KeyPair keys=PeerCrypto.keyPair();
        final CompletableFuture<Void> ready=new CompletableFuture<>();
        final ReplayWindow replay=new ReplayWindow();
        final AtomicLong udpCounter=new AtomicLong(),received=new AtomicLong(),invalid=new AtomicLong(),duplicates=new AtomicLong();
        final AtomicBoolean failed=new AtomicBoolean();
        final AtomicInteger probe=new AtomicInteger();
        final BitSet seen=new BitSet();
        final List<Long> latencies=new ArrayList<>(),controlLatencies=new ArrayList<>();
        final StringBuilder text=new StringBuilder();
        final ByteArrayOutputStream binary=new ByteArrayOutputStream();
        final long started=System.nanoTime();
        volatile WebSocket socket;
        volatile UUID session,peer;
        volatile PeerCrypto crypto;
        volatile byte[] udpKey;
        volatile int udpPort;
        volatile boolean bound;
        volatile long lastHeartbeat,lastBind,joinNanos,probeStarted;
        CompletableFuture<?> tail=CompletableFuture.completedFuture(null);
        Client(Load load,int index) {
            this.load=load; this.index=index; name="Scale"+index; player=Wire.offlineUuid(name); partner=Wire.offlineUuid("Scale"+(index^1));
            room=String.format(Locale.ROOT,"%064x",1+(index/2)%load.o.rooms);
        }
        public void onOpen(WebSocket ws) { socket=ws; ws.request(1); }
        public CompletionStage<?> onText(WebSocket ws,CharSequence part,boolean last) {
            try {
                if(text.length()+part.length()>Wire.MAX_TEXT)throw new IllegalArgumentException("Oversized fixture control");
                text.append(part); if(last) { control(Wire.parse(text.toString())); text.setLength(0); }
            } catch(Exception failure) { fail(failure); }
            ws.request(1); return null;
        }
        private void control(JsonObject j) {
            switch(Wire.string(j,"type",24)) {
                case "challenge" -> {
                    if(j.get("protocol").getAsInt()!=1)throw new IllegalArgumentException("Protocol mismatch");
                    JsonObject join=Wire.message("join"); join.addProperty("player",player.toString()); join.addProperty("name",name);
                    join.addProperty("room",room); join.addProperty("dimension","minecraft:overworld");
                    join.addProperty("key",Wire.base64(keys.getPublic().getEncoded())); join.addProperty("online",false); send(join);
                }
                case "welcome" -> {
                    if(session!=null || j.get("verified").getAsBoolean())throw new IllegalArgumentException("Unexpected fixture identity");
                    udpKey=Wire.unbase64(Wire.string(j,"udpKey",64),32); udpPort=j.get("udpPort").getAsInt();
                    if(udpKey.length!=32 || udpPort<1 || udpPort>65535)throw new IllegalArgumentException("UDP credentials");
                    session=Wire.id(j,"session"); load.bySession.put(session,this); lastHeartbeat=System.nanoTime();
                    joinNanos=System.nanoTime()-started; ready.complete(null);
                }
                case "peers" -> {
                    JsonArray peers=j.getAsJsonArray("peers");
                    if(peers.size()>1)throw new IllegalArgumentException("Synthetic pair overflow");
                    if(peers.isEmpty()) {
                        if(probe.compareAndSet(1,2))nearby(true);
                    } else {
                        JsonObject p=peers.get(0).getAsJsonObject(); UUID next=Wire.id(p,"session");
                        if(!Wire.id(p,"player").equals(partner))throw new IllegalArgumentException("Wrong nearby player");
                        if(peer==null) { crypto=new PeerCrypto(keys,session,next,Wire.unbase64(Wire.string(p,"key",128),64)); peer=next; }
                        else if(!peer.equals(next))throw new IllegalArgumentException("Unexpected peer replacement");
                        if(probe.compareAndSet(2,0))synchronized(controlLatencies) { controlLatencies.add(System.nanoTime()-probeStarted); }
                    }
                }
                default -> throw new IllegalArgumentException("Unexpected fixture message");
            }
        }
        void nearby(boolean present) {
            JsonObject j=Wire.message("nearby"); JsonArray players=new JsonArray(); if(present)players.add(partner.toString());
            j.add("players",players); send(j);
        }
        synchronized void send(JsonObject message) {
            String encoded=Wire.GSON.toJson(message);
            tail=tail.thenCompose(ignored -> socket.sendText(encoded,true)); tail.exceptionally(failure -> { fail(failure); return null; });
        }
        void media(byte[] bytes) throws Exception {
            if(load.o.udp)datagram(Wire.ROUTE,bytes);
            else synchronized(this) {
                if(!tail.isDone())throw new IllegalStateException("Synthetic sender queue is falling behind");
                tail=socket.sendBinary(ByteBuffer.wrap(bytes),true); tail.exceptionally(failure -> { fail(failure); return null; });
            }
        }
        void datagram(byte op,byte[] body) throws Exception {
            byte[] bytes=Wire.datagram(session,udpCounter.getAndIncrement(),op,body,udpKey,false);
            load.udp.send(new DatagramPacket(bytes,bytes.length,InetAddress.getLoopbackAddress(),udpPort));
        }
        public CompletionStage<?> onBinary(WebSocket ws,ByteBuffer part,boolean last) {
            try {
                if(binary.size()+part.remaining()>Wire.MAX_FRAME)throw new IllegalArgumentException("Oversized fixture frame");
                byte[] bytes=new byte[part.remaining()]; part.get(bytes); binary.writeBytes(bytes);
                if(last) { receive(Wire.decode(binary.toByteArray())); binary.reset(); }
            } catch(Exception failure) { invalid.incrementAndGet(); }
            ws.request(1); return null;
        }
        synchronized void receive(Wire.Frame frame) {
            if(frame.kind()!=Wire.VOICE || crypto==null || !frame.from().equals(peer)) { invalid.incrementAndGet(); return; }
            byte[] plain=crypto.open(frame);
            if(plain==null || plain.length!=PAYLOAD_BYTES) { invalid.incrementAndGet(); return; }
            ByteBuffer b=ByteBuffer.wrap(plain);
            if(b.getInt()!=MAGIC || b.getInt()!=(index^1)) { invalid.incrementAndGet(); return; }
            int sequence=b.getInt(); long elapsed=System.nanoTime()-b.getLong();
            if(sequence<0 || sequence>=load.o.seconds*load.o.pps || elapsed<0) { invalid.incrementAndGet(); return; }
            while(b.hasRemaining())if(b.get()!=(byte)0x5a) { invalid.incrementAndGet(); return; }
            if(seen.get(sequence)) { duplicates.incrementAndGet(); return; }
            seen.set(sequence); synchronized(latencies) { latencies.add(elapsed); } received.incrementAndGet();
        }
        public CompletionStage<?> onPing(WebSocket ws,ByteBuffer data) { ws.request(1); return ws.sendPong(data); }
        public CompletionStage<?> onPong(WebSocket ws,ByteBuffer data) { ws.request(1); return null; }
        public CompletionStage<?> onClose(WebSocket ws,int code,String reason) {
            if(!load.closing)fail(new IllegalStateException("Synthetic connection closed: "+code+" "+reason)); return null;
        }
        public void onError(WebSocket ws,Throwable failure) { fail(failure); }
        void fail(Throwable failure) {
            if(!load.closing && failed.compareAndSet(false,true)) {
                System.err.println("Synthetic client "+index+" failed: "+failure.getClass().getSimpleName()+": "+failure.getMessage());
                ready.completeExceptionally(failure);
            }
        }
    }
    private static String syntheticAddress(int index) { return "8.8."+(index/250)+"."+(1+index%250); }
    private static long processCpuNanos() {
        return ((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
    }
    private static double percentile(List<Long> sorted,double p) {
        return sorted.isEmpty()?0:sorted.get(Math.max(0,(int)Math.ceil(sorted.size()*p)-1))/1_000_000.0;
    }
    private static void await(BooleanSupplier condition,int seconds,String description) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);
        while(!condition.getAsBoolean()) {
            if(System.nanoTime()>=deadline)throw new AssertionError(description);
            Thread.sleep(25);
        }
    }
    private static void sleepUntil(long deadline) throws InterruptedException {
        long remaining=deadline-System.nanoTime(); if(remaining>0)TimeUnit.NANOSECONDS.sleep(remaining);
    }
}
