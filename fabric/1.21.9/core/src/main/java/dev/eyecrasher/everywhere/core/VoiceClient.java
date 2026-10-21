package dev.eyecrasher.everywhere.core;

import com.google.gson.*;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.*;
import java.security.KeyPair;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.net.ssl.SSLContext;

/** Minecraft-independent client, also exercised by the relay integration tests. */
public final class VoiceClient implements AutoCloseable, WebSocket.Listener {
    public record Identity(UUID player, String name, UUID account, String accountName, String room, String dimension) {}
    public record PeerInfo(UUID session, UUID player, String name, boolean verified, boolean direct) {}
    public record Diagnostics(boolean verified, int candidates, long checks, int mappedServers, boolean varyingMapping) {}
    /** Media counters count accepted local sends/queues, not acknowledgements from the recipient. */
    public record NetworkStats(int udpDirect,int lanDirect,int tcpDirect,long directUdpVoicePackets,long directTcpVoicePackets,long relayedVoicePackets) {}
    public record PeerDiagnostics(UUID player, String name, String route, int candidates, int confirmed,
                                  long checks, long replies, long expiredChecks, int udpRttMillis,
                                  int tcpCandidates, long tcpAttempts, long tcpFailures, String tcpState) {}
    public record ConnectionDiagnostics(boolean automaticTransports, int publicIPv4, int publicIPv6,
                                        int stunMappings, boolean mappingVaries, String tcpListener,
                                        int gateways, int udpMappings, int tcpMappings, String routerMethods,
                                        long mappingFailures, String lanDiscovery) {}
    /** Memory-only credential; never serialized or included in diagnostic output. */
    public static final class Resumption {
        private final UUID session, player;
        private final String room;
        private final byte[] secret;
        private Resumption(UUID session, Identity identity, byte[] secret) {
            this.session = session; player = identity.player; room = identity.room; this.secret = secret.clone();
        }
        private boolean matches(Identity identity) { return player.equals(identity.player) && room.equals(identity.room); }
    }
    @FunctionalInterface public interface Authenticator { CompletableFuture<Boolean> authenticate(String challenge); }
    public interface Listener {
        default void ready() {}
        default void peers(Collection<PeerInfo> peers) {}
        default void audio(PeerInfo peer, long sequence, int distance, byte[] opus, boolean end) {}
        default void closed(String reason) {}
    }
    private final Identity identity;
    private final URI uri;
    private final boolean directEnabled, udpEnabled;
    private final boolean automaticRoutes;
    private final SSLContext tls;
    private final Listener listener;
    private final Authenticator authenticator;
    private final Resumption previous;
    private final KeyPair keys = PeerCrypto.keyPair();
    private final HttpClient http;
    private final TlsFallback tlsFallback;
    private final ConnectionTrace trace;
    private final int traceAttempt;
    private final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "everywhere-network"));
    private final ConcurrentMap<UUID,Peer> peers = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConnectionTrace.Stage,Long> lastTraceFailure = new ConcurrentHashMap<>();
    // Preserve counters/replay windows if the same session leaves and re-enters proximity.
    // Recreating its crypto would reuse AES-GCM nonces under the same session keys.
    private final Map<UUID,Peer> knownPeers = new HashMap<>();
    private final DatagramSocket udp;
    private final List<NetworkDirectory.Probe> discovery;
    private final RateLimit probeBudget = new RateLimit(64, 64);
    private record StunCheck(byte[] nonce, long at) {}
    private record Mapping(InetSocketAddress endpoint, long at) {}
    private final Map<InetSocketAddress,StunCheck> stunChecks = new ConcurrentHashMap<>();
    private final Map<InetSocketAddress,Mapping> mappings = new ConcurrentHashMap<>();
    private final Map<URI,Mapping> tcpMappings = new ConcurrentHashMap<>();
    private volatile List<InetSocketAddress> stunServers = List.of();
    private volatile List<InetAddress> hostAddresses = List.of();
    private volatile TcpTransport tcp;
    private volatile LanDiscovery lan;
    private volatile RouterMapper router;
    private ScheduledExecutorService discoveryClock;
    private boolean transportExtension;
    private volatile boolean presenceExtension;
    private WorldPresence.Snapshot presence,lastPublishedPresence;
    private long lastPresencePublished;
    private long lastLan,lastTcpDiscovery;
    private boolean reportedSharedPortUnavailable;
    private final AtomicLong directUdpVoice=new AtomicLong(),directTcpVoice=new AtomicLong(),relayedVoice=new AtomicLong();
    private volatile String advertisedCandidates = "";
    private boolean candidateExtension;
    private volatile boolean verified;
    private final AtomicLong udpCounter = new AtomicLong();
    private final ReplayWindow udpReplay = new ReplayWindow();
    private final AtomicInteger pendingSends = new AtomicInteger();
    private final AtomicBoolean ended = new AtomicBoolean();
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile Resumption resumption;
    private CompletableFuture<?> writes = CompletableFuture.completedFuture(null);
    private final StringBuilder text = new StringBuilder();
    private ByteArrayOutputStream binary = new ByteArrayOutputStream();
    private volatile WebSocket ws;
    private volatile UUID session;
    private volatile byte[] udpKey;
    private volatile InetSocketAddress relay;
    private volatile Set<UUID> nearby = Set.of();
    private volatile boolean ready, joined;
    private boolean advertisedUdp;
    private long lastBind, lastHeartbeat, lastStun;
    private int probeCursor;
    private int reportedDirect;
    private volatile long boundAt;
    private final CompletableFuture<Void> connected = new CompletableFuture<>();
    private long connectStarted;

    private static final class Peer {
        final UUID session, player;
        final String name, publicKey;
        final boolean verified;
        final PeerCrypto crypto;
        final RateLimit receive = new RateLimit(65, 100);
        final RateLimit control = new RateLimit(20, 32);
        final DirectPath path = new DirectPath();
        final long created=System.nanoTime();
        final Map<InetSocketAddress,Long> hostTcp=new ConcurrentHashMap<>();
        volatile List<InetSocketAddress> tcpCandidates=List.of();
        volatile boolean transports;
        final ArrayDeque<PendingAudio> pending=new ArrayDeque<>();
        boolean warmed;
        long lastBeacon;
        Peer(JsonObject j, KeyPair keys, UUID self) {
            session = Wire.id(j, "session"); player = Wire.id(j, "player"); name = Wire.string(j, "name", 16);
            publicKey = Wire.string(j, "key", 128); verified = j.get("verified").getAsBoolean();
            crypto = new PeerCrypto(keys, self, session, Wire.unbase64(publicKey, 64));
        }
        PeerInfo info(boolean tcp) { return new PeerInfo(session, player, name, verified, tcp || path.selected(System.nanoTime()) != null); }
    }
    private record PendingAudio(byte[] plain,boolean end,long at) {}
    public VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                       Authenticator authenticator, Listener listener) throws Exception {
        this(uri, tls, identity, directEnabled, udpEnabled, authenticator, listener, List.of());
    }
    public VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                       Authenticator authenticator, Listener listener, List<NetworkDirectory.Probe> discovery) throws Exception {
        this(uri, tls, identity, directEnabled, udpEnabled, authenticator, listener, discovery, (Resumption) null);
    }
    public VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                       Authenticator authenticator, Listener listener, List<NetworkDirectory.Probe> discovery, Resumption previous) throws Exception {
        this(uri, tls, identity, directEnabled, udpEnabled, authenticator, listener, discovery, previous, new ConnectionTrace());
    }
    public VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                       Authenticator authenticator, Listener listener, List<NetworkDirectory.Probe> discovery,
                       Resumption previous, ConnectionTrace trace) throws Exception {
        this(uri, tls, identity, directEnabled, udpEnabled, authenticator, listener, discovery,
                udpEnabled ? new DatagramSocket() : null, previous,true,trace);
    }
    // Socket injection stays package-private for deterministic NAT integration tests.
    VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                Authenticator authenticator, Listener listener, List<NetworkDirectory.Probe> discovery, DatagramSocket socket) throws Exception {
        this(uri, tls, identity, directEnabled, udpEnabled, authenticator, listener, discovery, socket, null,false,new ConnectionTrace());
    }
    VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                Authenticator authenticator, Listener listener, List<NetworkDirectory.Probe> discovery, DatagramSocket socket,boolean automaticRoutes) throws Exception {
        this(uri,tls,identity,directEnabled,udpEnabled,authenticator,listener,discovery,socket,null,automaticRoutes,new ConnectionTrace());
    }
    private VoiceClient(URI uri, SSLContext tls, Identity identity, boolean directEnabled, boolean udpEnabled,
                Authenticator authenticator, Listener listener, List<NetworkDirectory.Probe> discovery, DatagramSocket socket, Resumption previous,boolean automaticRoutes,
                ConnectionTrace trace) throws Exception {
        if (!uri.getScheme().equals("wss") && !(uri.getScheme().equals("ws") && uri.getHost().equals("127.0.0.1")))
            throw new IllegalArgumentException("TLS is required outside loopback");
        this.uri = uri; this.identity = identity; this.listener = listener; this.authenticator = authenticator;
        this.trace = Objects.requireNonNull(trace); this.traceAttempt = trace.attempts();
        this.previous = previous != null && previous.matches(identity) ? previous : null;
        this.directEnabled = directEnabled; this.udpEnabled = udpEnabled;
        this.automaticRoutes=automaticRoutes; this.tls=tls;
        if (discovery.size() > 4) throw new IllegalArgumentException("Discovery limit");
        this.discovery = List.copyOf(discovery);
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(8));
        if (tls != null) builder.sslContext(tls);
        http = builder.build();
        tlsFallback = new TlsFallback(http, trace, ConnectionTrace.Stage.CONTROL_TLS, traceAttempt);
        udp = socket;
        trace.recordForAttempt(traceAttempt, "UDP_START", udp == null ? "DISABLED" : "SUCCESS", -1, null, -1);
    }
    public CompletableFuture<Void> connect() {
        if (ended.get() || !started.compareAndSet(false, true)) return connected;
        connectStarted = System.nanoTime();
        trace.recordForAttempt(traceAttempt, "CONTROL_CONNECT", "START", -1, null, -1);
        tlsFallback.connect(uri, this)
                .whenComplete((socket, error) -> {
                    if (error != null) fail("Cannot connect to voice service", error);
                    else if (ended.get()) socket.abort();
                });
        if (udp != null) daemon(this::readUdp, "everywhere-udp").start();
        clock.scheduleAtFixedRate(this::tick, 50, 50, TimeUnit.MILLISECONDS);
        // Two bounded TLS attempts plus the voice handshake; plain loopback tests keep their old deadline.
        clock.schedule(() -> { if (!ready) fail("Voice handshake timed out"); }, uri.getScheme().equals("wss") ? 25 : 15, TimeUnit.SECONDS);
        return connected;
    }
    public boolean isReady() { return ready && !ended.get(); }
    public boolean hasUdp() { return udp != null && !udp.isClosed() && boundAt != 0 && System.nanoTime() - boundAt < TimeUnit.SECONDS.toNanos(6); }
    public UUID sessionId() { return session; }
    public String controlTransport() { return tlsFallback.recovered() ? "TLS-record-split" : "normal"; }
    public Resumption resumption() { return resumption; }
    public ConnectionTrace connectionTrace() { return trace; }
    public Collection<PeerInfo> peers() { return peers.values().stream().map(p -> p.info(tcp!=null && tcp.ready(p.session))).toList(); }
    public NetworkStats networkStats() {
        int udpCount=0,lanCount=0,tcpCount=0; long now=System.nanoTime();
        for(Peer peer:peers.values()) {
            InetSocketAddress selected=peer.path.selected(now);
            if(selected!=null && udp!=null && !udp.isClosed()) { udpCount++; if(HostNetworks.privateAddress(selected.getAddress()))lanCount++; }
            else if(tcp!=null && tcp.ready(peer.session))tcpCount++;
        }
        return new NetworkStats(udpCount,lanCount,tcpCount,directUdpVoice.get(),directTcpVoice.get(),relayedVoice.get());
    }
    public Diagnostics diagnostics() {
        var active = mappings.values().stream().filter(m -> System.nanoTime() - m.at < TimeUnit.SECONDS.toNanos(30)).toList();
        return new Diagnostics(verified, peers.values().stream().mapToInt(p -> p.path.count()).sum(),
                peers.values().stream().mapToLong(p -> p.path.checks()).sum(), active.size(),
                active.stream().map(Mapping::endpoint).distinct().count() > 1);
    }
    /** Local diagnostic counts only: never include public/private endpoints, keys or credentials. */
    public List<PeerDiagnostics> peerDiagnostics() {
        long now=System.nanoTime(); List<PeerDiagnostics> result=new ArrayList<>();
        TcpTransport currentTcp=tcp;
        for(Peer peer:peers.values()) {
            var stats=peer.path.diagnostics(now); var direct=peer.path.selected(now);
            var tcpStats=currentTcp==null?null:currentTcp.diagnostics(peer.session);
            Set<InetSocketAddress> endpoints=new HashSet<>(peer.tcpCandidates);
            peer.hostTcp.forEach((endpoint,at) -> { if(now-at>=0 && now-at<TimeUnit.SECONDS.toNanos(30))endpoints.add(endpoint); });
            boolean tcpReady=tcpStats!=null && tcpStats.ready();
            String route=direct!=null && udp!=null && !udp.isClosed()?(HostNetworks.privateAddress(direct.getAddress())?"LAN":"direct-UDP")
                    :tcpReady?"direct-TCP":hasUdp()?"VPS-UDP":"VPS-WSS";
            String tcpState=!directEnabled?"disabled":!peer.transports?"not-negotiated"
                    :currentTcp==null || currentTcp.isClosed()?"listener-unavailable":tcpReady?"ready"
                    :tcpStats.connecting()?"connecting":endpoints.isEmpty()?"no-candidates"
                    :tcpStats.lastFailure().isEmpty()?"waiting":tcpStats.lastFailure();
            result.add(new PeerDiagnostics(peer.player,peer.name,route,stats.candidates(),stats.confirmed(),
                    stats.checks(),stats.replies(),stats.expiredChecks(),stats.rttMillis(),endpoints.size(),
                    tcpStats==null?0:tcpStats.attempts(),tcpStats==null?0:tcpStats.failures(),tcpState));
        }
        result.sort(Comparator.comparing(PeerDiagnostics::name)); return List.copyOf(result);
    }
    public ConnectionDiagnostics connectionDiagnostics() {
        Diagnostics stats=diagnostics(); RouterMapper currentRouter=router; TcpTransport currentTcp=tcp; LanDiscovery currentLan=lan;
        var routerStats=currentRouter==null?null:currentRouter.diagnostics();
        String tcpState=!directEnabled?"disabled":!transportExtension?"not-negotiated"
                :currentTcp==null || currentTcp.isClosed()?"unavailable":"ready";
        return new ConnectionDiagnostics(transportExtension,(int)hostAddresses.stream().filter(a -> a instanceof Inet4Address).count(),
                (int)hostAddresses.stream().filter(a -> a instanceof Inet6Address).count(),stats.mappedServers(),stats.varyingMapping(),tcpState,
                routerStats==null?0:routerStats.gateways(),routerStats==null?0:routerStats.udpMappings(),routerStats==null?0:routerStats.tcpMappings(),
                routerStats==null?"":routerStats.methods(),routerStats==null?0:routerStats.failures(),currentLan==null?"unavailable":currentLan.diagnostics());
    }
    public void nearby(Set<UUID> players) { nearby(players,null); }
    public void nearby(Set<UUID> players,WorldPresence.Snapshot observations) {
        if (players.size() > 24) throw new IllegalArgumentException("Nearby limit");
        Set<UUID> next = Set.copyOf(players);
        if(observations!=null && (!observations.player().equals(identity.player) || !observations.dimension().equals(identity.dimension)
                || !observations.nearby().keySet().equals(next)))throw new IllegalArgumentException("Presence identity");
        boolean publish;
        synchronized(this) {
            boolean changed=!next.equals(nearby),withdrawn=presence!=null && observations==null;
            presence=observations;nearby=next;
            long age=System.nanoTime()-lastPresencePublished;
            boolean refresh=presenceExtension && observations!=null && (lastPublishedPresence==null
                    || age>=TimeUnit.SECONDS.toNanos(3) || age>=TimeUnit.SECONDS.toNanos(1) && !observations.samePositions(lastPublishedPresence));
            publish=ready && (changed || withdrawn || refresh);
        }
        for(Peer peer:peers.values())if(!next.contains(peer.player))synchronized(peer) { peer.pending.clear(); }
        if(publish)publishNearby();
    }
    private synchronized void publishNearby() {
        JsonObject j = Wire.message("nearby"); JsonArray ids = new JsonArray(); nearby.forEach(id -> ids.add(id.toString())); j.add("players", ids);
        if(presenceExtension && presence!=null && !nearby.isEmpty())j.add("presence",WorldPresence.write(presence));
        lastPublishedPresence=presence;lastPresencePublished=System.nanoTime();send(j);
    }
    public void sendAudio(Set<UUID> recipients, long audioSequence, int distance, byte[] opus, boolean end) {
        if (!isReady() || opus.length > 800 || distance < 1 || distance > 32) return;
        byte[] plain = ByteBuffer.allocate(10 + opus.length).putLong(audioSequence).putShort((short) distance).put(opus).array();
        for (Peer peer : peers.values()) {
            if (!recipients.contains(peer.player) || !nearby.contains(peer.player)) continue;
            synchronized(peer) {
                long now=System.nanoTime();
                boolean warming=automaticRoutes && directEnabled && !peer.warmed && now-peer.created<TimeUnit.SECONDS.toNanos(3)
                        && !directReady(peer,now);
                if(warming || !peer.pending.isEmpty()) {
                    peer.pending.add(new PendingAudio(plain,end,now)); flush(peer,now);
                } else route(peer,new PendingAudio(plain,end,now));
            }
        }
    }
    private boolean directReady(Peer peer,long now) {
        return directEnabled && (udp!=null && !udp.isClosed() && peer.path.selected(now)!=null || tcp!=null && tcp.ready(peer.session));
    }
    private void flush(Peer peer,long now) {
        if(peer.pending.isEmpty())return;
        // Race paths without seconds of silence/backlog: at most 250 ms or 16 audio frames.
        if(!directReady(peer,now) && now-peer.pending.getFirst().at<TimeUnit.MILLISECONDS.toNanos(250) && peer.pending.size()<16)return;
        peer.warmed=true;
        while(!peer.pending.isEmpty())route(peer,peer.pending.removeFirst());
    }
    private void route(Peer peer,PendingAudio audio) {
        if(!isReady() || peers.get(peer.session)!=peer || !nearby.contains(peer.player))return;
        byte[] bytes=peer.crypto.seal(audio.end?Wire.END:Wire.VOICE,audio.plain).encode();
        InetSocketAddress direct=peer.path.selected(System.nanoTime());
        if(!audio.end && directEnabled && udp!=null && !udp.isClosed() && direct!=null && sendUdp(bytes,direct)) {
            directUdpVoice.incrementAndGet();
        } else if(directEnabled && tcp!=null && tcp.send(peer.session,bytes)) {
            if(!audio.end)directTcpVoice.incrementAndGet();
        } else {
            // Reuse the encrypted frame when a local UDP send fails. Its replay protection
            // also prevents duplicate playback if a transport fails after handing it off.
            boolean accepted=!audio.end && hasUdp() && relayUdp(Wire.ROUTE,bytes);
            if(!accepted)accepted=sendBinary(bytes);
            if(!audio.end && accepted)relayedVoice.incrementAndGet();
        }
    }
    @Override public void onOpen(WebSocket socket) {
        trace.recordForAttempt(traceAttempt, "CONTROL_OPEN", "SUCCESS", elapsed(connectStarted), null, -1);
        ws = socket; if (ended.get()) socket.abort(); else socket.request(1);
    }
    @Override public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
        if (ended.get()) { socket.abort(); return null; }
        try {
            if (text.length() + data.length() > Wire.MAX_TEXT) throw new IllegalArgumentException("Message size");
            text.append(data);
            if (last) { JsonObject j = Wire.parse(text.toString()); text.setLength(0); control(j); }
        } catch (Exception e) { fail("Invalid voice service message", e); }
        socket.request(1); return null;
    }
    private void control(JsonObject j) throws Exception {
        String type = Wire.string(j, "type", 24);
        switch (type) {
            case "challenge" -> {
                if (joined || j.get("protocol").getAsInt() != 1) throw new IllegalArgumentException("Protocol");
                joined = true;
                String challenge = Wire.string(j, "challenge", 40);
                if (!challenge.matches("[a-f0-9]{40}")) throw new IllegalArgumentException("Challenge");
                boolean canResume = previous != null && j.has("resumption") && j.get("resumption").getAsBoolean();
                trace.recordForAttempt(traceAttempt, "CHALLENGE", "RECEIVED", elapsed(connectStarted), null, canResume ? 1 : 0);
                long authenticationStarted = System.nanoTime();
                trace.recordForAttempt(traceAttempt, "AUTHENTICATION", "START", -1, null, -1);
                CompletableFuture<Boolean> authentication;
                try { authentication = authenticator.authenticate(challenge); }
                catch (RuntimeException failure) {
                    trace.recordForAttempt(traceAttempt, "AUTHENTICATION", "FAILED", elapsed(authenticationStarted), failure, -1);
                    throw failure;
                }
                authentication.handle((online, error) -> {
                    boolean accepted = error == null && Boolean.TRUE.equals(online);
                    trace.recordForAttempt(traceAttempt, "AUTHENTICATION", error != null ? "FAILED" : accepted ? "SUCCESS" : "UNVERIFIED",
                            elapsed(authenticationStarted), error, accepted ? 1 : 0);
                    return accepted;
                }).thenAccept(online -> {
                    if (ended.get()) return;
                    JsonObject join = Wire.message("join"); join.addProperty("player", identity.player.toString());
                    join.addProperty("name", identity.name); join.addProperty("accountId", identity.account.toString());
                    join.addProperty("accountName", identity.accountName); join.addProperty("room", identity.room);
                    join.addProperty("dimension", identity.dimension); join.addProperty("key", Wire.base64(keys.getPublic().getEncoded()));
                    if(automaticRoutes && directEnabled)join.addProperty("transports",1);
                    if (canResume) {
                        join.addProperty("previousSession", previous.session.toString());
                        join.addProperty("sessionProof", Wire.base64(SessionProof.sign(previous.secret, previous.session,
                                identity.player, identity.room, challenge, join.get("key").getAsString())));
                    }
                    join.addProperty("online", online); send(join);
                    trace.recordForAttempt(traceAttempt, "JOIN_SENT", "SENT", elapsed(connectStarted), null, online ? 1 : 0);
                });
            }
            case "welcome" -> {
                if (session != null) throw new IllegalArgumentException("Duplicate welcome");
                session = Wire.id(j, "session"); udpKey = Wire.unbase64(Wire.string(j, "udpKey", 64), 32);
                if (udpKey.length != 32) throw new IllegalArgumentException("UDP key");
                int port = j.get("udpPort").getAsInt();
                if (port < 1 || port > 65535) throw new IllegalArgumentException("UDP port");
                // The hostname is the configured relay, never a hostname chosen by another player.
                relay = new InetSocketAddress(uri.getHost(), port);
                verified = j.has("verified") && j.get("verified").getAsBoolean();
                trace.recordForAttempt(traceAttempt, "WELCOME", verified ? "VERIFIED" : "UNVERIFIED", elapsed(connectStarted), null, -1);
                candidateExtension = j.has("candidates") && j.get("candidates").getAsBoolean();
                transportExtension=automaticRoutes && j.has("transports") && j.get("transports").getAsInt()==1;
                presenceExtension=j.has("presence") && j.get("presence").getAsInt()==1;
                if (candidateExtension && directEnabled && udp != null) prepareDiscovery();
                resumption = new Resumption(session, identity, udpKey);
                if(directEnabled && transportExtension)startTransports();
                ready = true;
                trace.recordForAttempt(traceAttempt, "CONTROL_CONNECT", "READY", elapsed(connectStarted), null, -1);
                connected.complete(null); publishNearby(); listener.ready();
            }
            case "peers" -> {
                if (session == null) throw new IllegalArgumentException("Not ready");
                JsonArray list = j.getAsJsonArray("peers"); if (list.size() > 24) throw new IllegalArgumentException("Peer limit");
                Set<UUID> keep = new HashSet<>();
                for (JsonElement e : list) {
                    JsonObject p = e.getAsJsonObject(); UUID id = Wire.id(p, "session");
                    UUID player = Wire.id(p, "player");
                    if (!nearby.contains(player) || id.equals(session)) continue;
                    try {
                        Peer peer = knownPeers.get(id);
                        if (peer == null) {
                            if (knownPeers.size() >= 1024) { fail("Refreshing long-lived voice session"); return; }
                            peer = new Peer(p, keys, session); knownPeers.put(id, peer);
                        }
                        if (!peer.publicKey.equals(Wire.string(p, "key", 128)) || !peer.player.equals(player))
                            throw new IllegalArgumentException("Session key changed");
                        List<InetSocketAddress> endpoints = new ArrayList<>();
                        if (directEnabled && p.has("ip")) {
                            try { endpoints.add(Endpoints.read(p, localTest())); } catch (IllegalArgumentException ignored) {}
                        }
                        if (directEnabled && p.has("candidates")) {
                            JsonArray extra = p.getAsJsonArray("candidates");
                            if (extra.size() > 4) throw new IllegalArgumentException("Candidate limit");
                            for (var item : extra) try { endpoints.add(Endpoints.read(item.getAsJsonObject(), localTest())); }
                            catch (IllegalArgumentException ignored) {}
                        }
                        peer.path.candidates(endpoints, System.nanoTime()); peers.put(id,peer); keep.add(id);
                        peer.transports=transportExtension && p.has("transports") && p.get("transports").getAsInt()==1;
                        if(peer.transports && p.has("tcp")) {
                            JsonArray array=p.getAsJsonArray("tcp"); if(array.size()>4)throw new IllegalArgumentException("TCP candidate limit");
                            List<InetSocketAddress> addresses=new ArrayList<>();
                            for(var value:array)try { addresses.add(Endpoints.read(value.getAsJsonObject(),localTest())); }
                            catch(IllegalArgumentException ignored) {}
                            peer.tcpCandidates=List.copyOf(addresses);
                        }
                    } catch (IllegalArgumentException invalidPeer) { peers.remove(id); }
                }
                for(var entry:peers.entrySet())if(!keep.contains(entry.getKey()))synchronized(entry.getValue()) { entry.getValue().pending.clear(); }
                peers.keySet().retainAll(keep); if(tcp!=null)tcp.retain(keep); listener.peers(peers());
            }
            default -> throw new IllegalArgumentException("Message type");
        }
    }
    @Override public CompletionStage<?> onBinary(WebSocket socket, ByteBuffer data, boolean last) {
        if (ended.get()) { socket.abort(); return null; }
        try {
            if (binary.size() + data.remaining() > Wire.MAX_FRAME) throw new IllegalArgumentException("Frame size");
            byte[] bytes = new byte[data.remaining()]; data.get(bytes); binary.writeBytes(bytes);
            if (last) { receive(Wire.decode(binary.toByteArray()), null); binary.reset(); }
        } catch (Exception e) { binary.reset(); }
        socket.request(1); return null;
    }
    @Override public CompletionStage<?> onPing(WebSocket socket, ByteBuffer data) {
        if (ended.get()) { socket.abort(); return null; }
        socket.request(1); return socket.sendPong(data);
    }
    @Override public CompletionStage<?> onPong(WebSocket socket, ByteBuffer data) { if (ended.get()) socket.abort(); else socket.request(1); return null; }
    static String closeMessage(int code, String reason) {
        Set<String> known = Set.of("Connection limit", "Invalid control message", "This player already has a session",
                "Voice capacity reached", "Remaining slots reserved for verified accounts", "Verified account reclaimed this player",
                "Account verification failed", "Join timeout", "Slow connection", "Session replaced by reconnect", "Voice heartbeat timed out");
        return "Voice service disconnected (" + code + ")" + (reason != null && known.contains(reason) ? ": " + reason : "");
    }
    static ConnectionTrace.Result closeResult(String reason) {
        if (reason == null) return ConnectionTrace.Result.OTHER;
        return switch (reason) {
            case "Connection limit" -> ConnectionTrace.Result.CONNECTION_LIMIT;
            case "Invalid control message" -> ConnectionTrace.Result.INVALID_MESSAGE;
            case "This player already has a session" -> ConnectionTrace.Result.SESSION_EXISTS;
            case "Voice capacity reached" -> ConnectionTrace.Result.CAPACITY_REACHED;
            case "Remaining slots reserved for verified accounts" -> ConnectionTrace.Result.VERIFIED_RESERVED;
            case "Verified account reclaimed this player" -> ConnectionTrace.Result.VERIFIED_RECLAIMED;
            case "Account verification failed" -> ConnectionTrace.Result.ACCOUNT_REJECTED;
            case "Join timeout" -> ConnectionTrace.Result.JOIN_TIMEOUT;
            case "Slow connection" -> ConnectionTrace.Result.SLOW_CONNECTION;
            case "Session replaced by reconnect" -> ConnectionTrace.Result.SESSION_REPLACED;
            case "Voice heartbeat timed out" -> ConnectionTrace.Result.HEARTBEAT_TIMEOUT;
            default -> ConnectionTrace.Result.OTHER;
        };
    }
    @Override public CompletionStage<?> onClose(WebSocket socket, int code, String reason) {
        trace.recordForAttempt(traceAttempt, ConnectionTrace.Stage.CONTROL_CLOSE, closeResult(reason), elapsed(connectStarted), null, code);
        if(!ready && Set.of("Voice capacity reached","Remaining slots reserved for verified accounts").contains(reason))NodeSelection.failed(uri);
        fail(closeMessage(code, reason)); return null;
    }
    @Override public void onError(WebSocket socket, Throwable error) {
        trace.recordForAttempt(traceAttempt, "CONTROL_ERROR", "FAILED", elapsed(connectStarted), error, -1);
        fail("Voice connection failed", error);
    }
    private void readUdp() {
        byte[] buffer = new byte[Wire.MAX_UDP + 1];
        while (!ended.get()) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length); udp.receive(packet);
                if (packet.getLength() > Wire.MAX_UDP || session == null) continue;
                byte[] bytes = Arrays.copyOf(buffer, packet.getLength());
                InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
                if (Stun.isMessage(bytes)) { receiveStun(bytes, source); continue; }
                if (source.equals(relay)) {
                    Wire.Datagram d = Wire.openDatagram(bytes, udpKey, true);
                    if (!d.session().equals(session) || !udpReplay.accept(d.sequence())) continue;
                    if (d.op() == Wire.CHALLENGE && d.body().length == 16) relayUdp(Wire.CONFIRM, d.body());
                    else if (d.op() == Wire.BOUND) {
                        if (!hasUdp()) {
                            advertisedCandidates = "";
                            trace.recordForAttempt(traceAttempt, "UDP_BOUND", "READY", elapsed(connectStarted), null, -1);
                        }
                        boundAt = System.nanoTime();
                    }
                    else if (d.op() == Wire.ROUTE) receive(Wire.decode(d.body()), null);
                } else if (directEnabled) receive(Wire.decode(bytes), source);
            } catch (Exception error) {
                if (udp.isClosed()) return;
                if (error instanceof IOException) networkFailure(ConnectionTrace.Stage.UDP_READ, error);
            }
        }
    }
    private void receive(Wire.Frame frame, InetSocketAddress source) {
        Peer peer = peers.get(frame.from());
        if (peer == null || !nearby.contains(peer.player) || (source != null && !peer.path.permits(source))) return;
        boolean control = frame.kind() == Wire.PING || frame.kind() == Wire.PONG;
        if (!(control ? peer.control : peer.receive).take()) return;
        byte[] plain = peer.crypto.open(frame); if (plain == null) return;
        if (frame.kind() == Wire.PING && plain.length == 16 && source != null) {
            peer.path.authenticatedPing(source, System.nanoTime());
            sendUdp(peer.crypto.seal(Wire.PONG, plain).encode(), source);
        } else if (frame.kind() == Wire.PONG && source != null) {
            peer.path.pong(plain, source, System.nanoTime());
        } else receiveAudio(peer,frame,plain);
    }
    private void receiveAudio(Peer peer,Wire.Frame frame,byte[] plain) {
        if ((frame.kind() == Wire.VOICE || frame.kind() == Wire.END) && plain.length >= 10 && plain.length <= 810) {
            ByteBuffer b = ByteBuffer.wrap(plain); long seq = b.getLong(); int distance = Short.toUnsignedInt(b.getShort());
            if (seq < 0 || distance < 1 || distance > 32) return;
            byte[] opus = new byte[b.remaining()]; b.get(opus);
            listener.audio(peer.info(tcp!=null && tcp.ready(peer.session)), seq, distance, opus, frame.kind() == Wire.END);
        }
    }
    private synchronized void startTransports() {
        if(ended.get())return;
        long started = System.nanoTime();
        trace.recordForAttempt(traceAttempt, "TRANSPORTS", "START", -1, null, -1);
        try {
            tcp=new TcpTransport(session,udp==null?0:udp.getLocalPort(),new TcpTransport.Handler() {
                public PeerCrypto crypto(UUID id) {
                    Peer peer=peers.get(id); return !ended.get() && peer!=null && peer.transports && nearby.contains(peer.player)?peer.crypto:null;
                }
                public boolean permits(UUID id,InetAddress source) {
                    Peer peer=peers.get(id); if(peer==null || !peer.transports)return false;
                    if(peer.tcpCandidates.stream().anyMatch(e -> e.getAddress().equals(source)))return true;
                    long now=System.nanoTime();
                    return peer.hostTcp.entrySet().stream().anyMatch(e -> e.getKey().getAddress().equals(source)
                            && now-e.getValue()<TimeUnit.SECONDS.toNanos(30));
                }
                public void received(Wire.Frame frame,byte[] plain) {
                    Peer peer=peers.get(frame.from());
                    if(peer!=null && nearby.contains(peer.player) && peer.receive.take())receiveAudio(peer,frame,plain);
                }
            });
            trace.recordForAttempt(traceAttempt, "TCP_LISTENER", "READY", elapsed(started), null, -1);
        } catch(IOException error) {
            trace.recordForAttempt(traceAttempt, "TCP_LISTENER", "FAILED", elapsed(started), error, -1);
        }
        discoveryClock=Executors.newSingleThreadScheduledExecutor(r -> daemon(r,"everywhere-discovery"));
        discoveryClock.scheduleWithFixedDelay(this::refreshDiscovery,0,15,TimeUnit.SECONDS);
    }
    private void refreshDiscovery() {
        if(ended.get())return;
        try {
            hostAddresses=localTest()?List.of(InetAddress.getByName("127.0.0.1")):HostNetworks.publicAddresses();
            if(!localTest()) {
                List<HostNetworks.Link> links=HostNetworks.links();
                if(udp!=null) {
                    if(lan==null || lan.isClosed())try {
                        lan=new LanDiscovery(session,links,this::receiveLan);
                        trace.recordForAttempt(traceAttempt, "LAN_DISCOVERY", "READY", -1, null, links.size());
                    } catch(IOException error) { networkFailure(ConnectionTrace.Stage.LAN_DISCOVERY, error); }
                    else lan.update(links);
                }
                if(router==null)router=new RouterMapper(udp==null || udp.isClosed()?0:udp.getLocalPort(),tcp==null?0:tcp.port(),relay.getAddress());
                refreshTcpDiscovery();
            }
            advertisedCandidates="";
        } catch(Exception error) { networkFailure(ConnectionTrace.Stage.DISCOVERY, error); }
        finally {
            if(ended.get()) {
                if(lan!=null)lan.close(); if(router!=null)router.close(); if(tcp!=null)tcp.close();
            }
        }
    }
    private void refreshTcpDiscovery() {
        TcpTransport transport=tcp;
        if(transport==null || ended.get())return;
        if(!transport.sharedPortAvailable()) { tcpSharedPortUnavailable(transport); return; }
        if(System.nanoTime()-lastTcpDiscovery<=TimeUnit.SECONDS.toNanos(30))return;
        lastTcpDiscovery=System.nanoTime();
        LinkedHashSet<URI> probes=new LinkedHashSet<>(); probes.add(uri);
        for(var probe:discovery) { if(probes.size()>=2)break; probes.add(probe.control()); }
        int probeIndex=0;
        for(URI probe:probes) {
            if(ended.get())break;
            long started=System.nanoTime();
            trace.recordForAttempt(traceAttempt,"TCP_DISCOVERY","START",-1,null,probeIndex);
            try {
                tcpMappings.put(probe,new Mapping(TcpDiscovery.lookup(transport,probe,tls),System.nanoTime()));
                trace.recordForAttempt(traceAttempt,"TCP_DISCOVERY","SUCCESS",elapsed(started),null,probeIndex);
            } catch(TcpTransport.SharedPortUnavailableException unavailable) {
                trace.recordForAttempt(traceAttempt,"TCP_DISCOVERY","UNAVAILABLE",elapsed(started),unavailable,probeIndex);
                tcpSharedPortUnavailable(transport);
                break;
            } catch(Exception error) {
                trace.recordForAttempt(traceAttempt,"TCP_DISCOVERY","FAILED",elapsed(started),error,probeIndex);
            }
            probeIndex++;
        }
    }
    private void tcpSharedPortUnavailable(TcpTransport transport) {
        // Only observations made using the listener's actual port are advertised.
        tcpMappings.clear();
        if(reportedSharedPortUnavailable)return;
        reportedSharedPortUnavailable=true;
        trace.recordForAttempt(traceAttempt,"TCP_SHARED_PORT","UNAVAILABLE",-1,transport.sharedPortFailure(),-1);
    }
    private byte[] lanBeacon(Peer peer) {
        byte[] plain=ByteBuffer.allocate(24).putInt(LanDiscovery.MARKER).putShort((short)(udp==null?0:udp.getLocalPort()))
                .putShort((short)(tcp==null?0:tcp.port())).put(Wire.random(16)).array();
        return peer.crypto.seal(Wire.PING,plain).encode();
    }
    private void receiveLan(byte[] bytes,InetAddress source) {
        if(ended.get() || udp==null)return;
        try {
            Wire.Frame frame=Wire.decode(bytes); Peer peer=peers.get(frame.from());
            if(peer==null || !peer.transports || !nearby.contains(peer.player) || frame.kind()!=Wire.PING || !peer.control.take())return;
            byte[] plain=peer.crypto.open(frame);
            if(plain==null || plain.length!=24)return;
            var b=ByteBuffer.wrap(plain); if(b.getInt()!=LanDiscovery.MARKER)return;
            int udpPort=Short.toUnsignedInt(b.getShort()),tcpPort=Short.toUnsignedInt(b.getShort()); long now=System.nanoTime();
            if(udpPort>0)peer.path.host(new InetSocketAddress(source,udpPort),now);
            if(tcpPort>0) {
                peer.hostTcp.entrySet().removeIf(e -> now-e.getValue()>TimeUnit.SECONDS.toNanos(30));
                if(peer.hostTcp.size()<2 || peer.hostTcp.containsKey(new InetSocketAddress(source,tcpPort)))
                    peer.hostTcp.put(new InetSocketAddress(source,tcpPort),now);
            }
        } catch(IllegalArgumentException ignored) {}
    }
    private void tick() {
        if (!ready || ended.get()) return;
        try {
            long now = System.nanoTime();
            boolean available = hasUdp();
            if (available != advertisedUdp) {
                advertisedUdp = available;
                if (!available) trace.recordForAttempt(traceAttempt, "UDP_BOUND", "UNAVAILABLE", elapsed(connectStarted), null, -1);
                JsonObject path = Wire.message("path"); path.addProperty("udp", available); send(path);
            }
            if (udpEnabled && now - lastBind > TimeUnit.SECONDS.toNanos(2)) { lastBind = now; relayUdp(Wire.BIND, new byte[0]); }
            if (now - lastHeartbeat > TimeUnit.SECONDS.toNanos(8)) { lastHeartbeat = now; send(Wire.message("heartbeat")); }
            if (directEnabled) {
                if (candidateExtension && udp != null && !udp.isClosed()) {
                    if (now - lastStun > TimeUnit.SECONDS.toNanos(10)) {
                        lastStun = now;
                        for (var target : stunServers) {
                            byte[] nonce = Wire.random(12); stunChecks.put(target, new StunCheck(nonce, now));
                            sendUdp(Stun.request(nonce), target);
                        }
                    }
                }
                if(candidateExtension)publishCandidates(now);
                List<Peer> check = new ArrayList<>(peers.values());
                if (!check.isEmpty()) Collections.rotate(check, probeCursor++ % check.size());
                for (Peer p : check) {
                    if(udp!=null && !udp.isClosed()) {
                        DirectPath.Probe probe = p.path.probe(now, probeBudget::take);
                        if (probe != null) sendUdp(p.crypto.seal(Wire.PING, probe.nonce()).encode(), probe.target());
                    }
                }
                if(lan!=null && now-lastLan>TimeUnit.SECONDS.toNanos(2)) {
                    lastLan=now;
                    for(Peer peer:check)if(peer.transports && nearby.contains(peer.player)) {
                        var path=peer.path.selected(now);
                        long interval=TimeUnit.SECONDS.toNanos(path!=null && HostNetworks.privateAddress(path.getAddress())?10:2);
                        if(now-peer.lastBeacon>=interval) { peer.lastBeacon=now; lan.send(lanBeacon(peer)); }
                    }
                }
                if(tcp!=null) {
                    Map<UUID,List<InetSocketAddress>> targets=new HashMap<>();
                    for(Peer peer:check)if(peer.transports) {
                        LinkedHashSet<InetSocketAddress> endpoints=new LinkedHashSet<>();
                        peer.hostTcp.entrySet().removeIf(e -> now-e.getValue()>TimeUnit.SECONDS.toNanos(30));
                        endpoints.addAll(peer.hostTcp.keySet()); endpoints.addAll(peer.tcpCandidates);
                        targets.put(peer.session,List.copyOf(endpoints));
                    }
                    tcp.tick(now,targets,id -> {
                        Peer peer=peers.get(id); return peer!=null && (udp!=null && peer.path.selected(now)!=null
                                || udp!=null && peer.path.count()>0 && now-peer.created<TimeUnit.MILLISECONDS.toNanos(150));
                    });
                }
            }
            for(Peer peer:peers.values())synchronized(peer) {
                if(!nearby.contains(peer.player))peer.pending.clear(); else flush(peer,now);
            }
            int direct = (int) peers().stream().filter(PeerInfo::direct).count();
            if (direct != reportedDirect) { reportedDirect = direct; listener.peers(peers()); }
        } catch (Exception error) { networkFailure(ConnectionTrace.Stage.NETWORK_TICK, error); }
    }
    private boolean localTest() { return uri.getHost().equals("127.0.0.1"); }
    private void prepareDiscovery() {
        LinkedHashSet<InetSocketAddress> servers = new LinkedHashSet<>(); servers.add(relay);
        for (var probe : discovery) {
            if (servers.size() >= 4) break;
            InetSocketAddress address = new InetSocketAddress(probe.control().getHost(), probe.udpPort());
            if (!address.isUnresolved() && (Endpoints.isPublic(address.getAddress()) || (localTest() && address.getAddress().isLoopbackAddress())))
                servers.add(address);
        }
        stunServers = List.copyOf(servers);
        if(!localTest())hostAddresses=HostNetworks.publicAddresses();
    }
    private void receiveStun(byte[] bytes, InetSocketAddress source) {
        StunCheck check = stunChecks.get(source);
        if (check == null || System.nanoTime() - check.at > TimeUnit.SECONDS.toNanos(4)) return;
        InetSocketAddress mapped = Stun.mapped(bytes, check.nonce);
        if (!(Endpoints.isPublic(mapped.getAddress()) || (localTest() && mapped.getAddress().isLoopbackAddress()))) return;
        if (stunChecks.remove(source, check)) mappings.put(source, new Mapping(mapped, System.nanoTime()));
    }
    private void publishCandidates(long now) {
        LinkedHashSet<InetSocketAddress> endpoints = new LinkedHashSet<>();
        if(udp!=null && !udp.isClosed())hostAddresses.stream().limit(2).forEach(ip -> endpoints.add(new InetSocketAddress(ip,udp.getLocalPort())));
        if(router!=null)endpoints.addAll(router.candidates(false));
        mappings.values().stream().filter(m -> now - m.at < TimeUnit.SECONDS.toNanos(30))
                .map(Mapping::endpoint).sorted(Comparator.comparing(InetSocketAddress::toString)).forEach(endpoints::add);
        JsonArray array = new JsonArray(); endpoints.stream().limit(4).forEach(e -> array.add(Endpoints.write(e)));
        JsonArray tcpArray=new JsonArray();
        if(tcp!=null) {
            LinkedHashSet<InetSocketAddress> addresses=new LinkedHashSet<>();
            hostAddresses.stream().limit(2).forEach(ip -> addresses.add(new InetSocketAddress(ip,tcp.port())));
            if(router!=null)addresses.addAll(router.candidates(true));
            if(tcp.sharedPortAvailable())tcpMappings.values().stream().filter(m -> now-m.at<TimeUnit.SECONDS.toNanos(60)).map(Mapping::endpoint).forEach(addresses::add);
            addresses.stream().limit(4).forEach(e -> tcpArray.add(Endpoints.write(e)));
        }
        String signature = array.toString()+tcpArray;
        if (signature.equals(advertisedCandidates)) return;
        advertisedCandidates = signature;
        JsonObject message = Wire.message("candidates"); message.add("candidates", array);
        if(transportExtension)message.add("tcp",tcpArray); send(message);
    }
    private boolean relayUdp(byte op, byte[] bytes) {
        return relay != null && udpKey != null
                && sendUdp(Wire.datagram(session, udpCounter.getAndIncrement(), op, bytes, udpKey, false), relay);
    }
    private boolean sendUdp(byte[] bytes, InetSocketAddress target) {
        if (udp == null || target == null || ended.get()) return false;
        try { udp.send(new DatagramPacket(bytes, bytes.length, target)); return true; }
        catch (IOException error) { networkFailure(ConnectionTrace.Stage.UDP_SEND, error); return false; }
    }
    private synchronized void send(JsonObject j) {
        WebSocket socket = ws; if (socket == null || ended.get()) return;
        String value = Wire.GSON.toJson(j);
        writes = writes.handle((v,e) -> null).thenCompose(v -> socket.sendText(value, true))
                .exceptionally(e -> { fail("Voice control send failed", e); return null; });
    }
    private synchronized boolean sendBinary(byte[] bytes) {
        WebSocket socket = ws; if (socket == null || ended.get()) return false;
        if (pendingSends.incrementAndGet() > 12) { pendingSends.decrementAndGet(); return false; }
        writes = writes.handle((v,e) -> null).thenCompose(v -> socket.sendBinary(ByteBuffer.wrap(bytes), true))
                .whenComplete((v,e) -> { pendingSends.decrementAndGet(); if (e != null) fail("Voice send failed", e); });
        return true;
    }
    private void fail(String reason) { fail(reason, null); }
    private synchronized void fail(String reason, Throwable cause) {
        if (!ended.compareAndSet(false, true)) return;
        if(!ready && Set.of("Cannot connect to voice service","Voice handshake timed out","Voice connection failed","Voice control send failed").contains(reason))
            NodeSelection.failed(uri);
        trace.recordForAttempt(traceAttempt, "SESSION_CLOSED", reason.equals("Voice session closed") ? "CLOSED"
                : reason.equals("Voice handshake timed out") ? "TIMEOUT" : "FAILED", elapsed(connectStarted), cause, -1);
        ready = false; connected.completeExceptionally(new IOException(reason, cause));
        clock.shutdownNow(); if(discoveryClock!=null)discoveryClock.shutdownNow();
        if(tcp!=null)tcp.close(); if(lan!=null)lan.close(); if(router!=null)router.close();
        if (udp != null) udp.close();
        WebSocket socket = ws; if (socket != null) socket.abort();
        tlsFallback.close(); RuntimeCompat.shutdown(http);
        listener.closed(reason);
    }
    @Override public void close() { fail("Voice session closed"); }
    private void networkFailure(ConnectionTrace.Stage stage, Throwable error) {
        long now = System.nanoTime();
        lastTraceFailure.compute(stage, (ignored, previous) -> {
            if (previous == null || now - previous >= TimeUnit.SECONDS.toNanos(30)) {
                trace.recordForAttempt(traceAttempt, stage, ConnectionTrace.Result.FAILED, elapsed(connectStarted), error, -1);
                return now;
            }
            return previous;
        });
    }
    private static long elapsed(long started) { return started == 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started); }
    private static Thread daemon(Runnable r, String name) { Thread t = new Thread(r, name); t.setDaemon(true); return t; }
}
