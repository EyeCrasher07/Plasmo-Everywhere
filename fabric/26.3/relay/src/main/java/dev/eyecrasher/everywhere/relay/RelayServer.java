package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import org.java_websocket.*;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.*;
import java.net.*;
import java.net.http.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;

/** Stateful discovery and bounded opaque packet forwarding. No audio decoding. */
public final class RelayServer extends WebSocketServer implements AutoCloseable {
    static final int MAX_SESSIONS = 4096;
    private static final int MAX_NEARBY = 24, MAX_QUEUE = 65536, MAX_PENDING = 16;
    private final int maxSessions, verifiedReserve;
    private record CloseRequest(WebSocket socket,int code,String reason) {}
    // A transport close can call onClose while holding WebSocketImpl's monitor.
    // Never acquire that monitor while holding this relay's state monitor.
    private final Map<WebSocket,CloseRequest> pendingCloses = new IdentityHashMap<>();
    private final BlockingQueue<CloseRequest> closeQueue;
    private final Thread closer;
    @FunctionalInterface
    interface AccountVerifier {
        CompletableFuture<Boolean> verify(String challenge, String name, UUID account);
    }
    private final AccountVerifier verifier;
    private final Map<WebSocket,Session> sockets = new HashMap<>();
    private final Map<UUID,Session> sessions = new HashMap<>();
    private final Map<String,Map<UUID,Session>> rooms = new HashMap<>();
    private final Map<String,NearbyIndex<Session>> nearbyIndexes = new HashMap<>();
    private final Map<UUID,RemotePeer> remoteSessions=new HashMap<>();
    private final Map<String,Map<UUID,RemotePeer>> remoteRooms=new HashMap<>();
    private final Map<String,Integer> remoteCounts=new HashMap<>();
    private final Map<UUID,RateLimit> remotePacketLimits=new HashMap<>();
    private final RateLimit federationPackets=new RateLimit(10000,2000);
    private volatile Federation federation;
    private final Map<String,RateLimit> ipLimits = new LinkedHashMap<>();
    private final RateLimit accepts = new RateLimit(10, 20), udpBudget = new RateLimit(5000, 1000);
    private final RateLimit stunBudget = new RateLimit(64, 128);
    // Accessed only by the UDP reader; bounded independently of authenticated voice sessions.
    private final Map<InetAddress,RateLimit> stunLimits = new LinkedHashMap<>();
    private final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService auth = Executors.newFixedThreadPool(2);
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(4)).executor(auth).build();
    private final DatagramSocket udp;
    private final Thread reader;
    private final boolean trustedProxy;
    private final long startedAt = System.currentTimeMillis();
    private long relayedUdpVoice,relayedWssVoice,peerRefreshes,peerDescriptions,relayQueueDrops,controlQueueDrops;
    private final AtomicLong udpBudgetDrops = new AtomicLong();
    private int verifiedSessions;
    private UsageStatistics usage;
    private volatile boolean closed;

    private record RemotePeer(String node,UUID id,UUID player,String name,String room,String dimension,String key,
                              boolean verified,boolean transports,Set<UUID> nearby,InetSocketAddress endpoint,
                              Set<InetSocketAddress> candidates,Set<InetSocketAddress> tcp) {}

    private static final class Session {
        final WebSocket ws;
        final UUID id = UUID.randomUUID();
        final String challenge = HexFormat.of().formatHex(Wire.random(20));
        final byte[] udpKey = Wire.random(32);
        final AtomicLong udpCounter = new AtomicLong();
        final ReplayWindow udpReplay = new ReplayWindow();
        final RateLimit control = new RateLimit(4, 12), packets = new RateLimit(1500, 200);
        final long created = System.nanoTime();
        Set<UUID> nearby = Set.of();
        Set<UUID> links = Set.of();
        Set<InetSocketAddress> candidates = Set.of();
        Set<InetSocketAddress> tcpCandidates = Set.of();
        InetAddress controlAddress;
        UUID player, account, previousSession;
        byte[] sessionProof;
        String name, room, dimension, publicKey;
        boolean joining, joined, verified, udpPreferred, transports, closeRequested;
        InetSocketAddress endpoint, pendingEndpoint;
        byte[] pendingChallenge;
        long lastUdp, pendingAt, lastControl = created, joinedAt;
        Session(WebSocket ws) { this.ws = ws; }
    }

    public RelayServer(String host, int port, int udpPort, SSLContext tls) throws Exception {
        this(host, port, host, udpPort, tls, false);
    }
    public RelayServer(String host, int port, String udpHost, int udpPort, SSLContext tls, boolean trustedProxy) throws Exception {
        this(host,port,udpHost,udpPort,tls,trustedProxy,256);
    }
    public RelayServer(String host, int port, String udpHost, int udpPort, SSLContext tls, boolean trustedProxy,int maxSessions) throws Exception {
        this(host,port,udpHost,udpPort,tls,trustedProxy,maxSessions,defaultReserve(maxSessions));
    }
    public RelayServer(String host, int port, String udpHost, int udpPort, SSLContext tls, boolean trustedProxy,
                       int maxSessions, int verifiedReserve) throws Exception {
        this(host,port,udpHost,udpPort,tls,trustedProxy,maxSessions,verifiedReserve,null);
    }
    // Verifier injection is package-private for deterministic tests; production always uses Mojang.
    RelayServer(String host, int port, String udpHost, int udpPort, SSLContext tls, boolean trustedProxy,
                int maxSessions, int verifiedReserve, AccountVerifier verifier) throws Exception {
        super(new InetSocketAddress(host, port),2,List.of(new BoundedRelayDraft()));
        if(maxSessions<1 || maxSessions>MAX_SESSIONS)throw new IllegalArgumentException("Session limit must be 1 to "+MAX_SESSIONS);
        if(verifiedReserve<0 || verifiedReserve>=maxSessions)throw new IllegalArgumentException("Reserve must be below capacity");
        this.maxSessions=maxSessions; this.verifiedReserve=verifiedReserve;
        closeQueue=new ArrayBlockingQueue<>(maxSessions+MAX_PENDING);
        this.verifier=verifier==null?this::verifyAccount:verifier;
        this.trustedProxy = trustedProxy;
        if (tls != null) setWebSocketFactory(new DefaultSSLWebSocketServerFactory(tls));
        setConnectionLostTimeout(20); setReuseAddr(true);
        udp = new DatagramSocket(null);
        try {
            // Absorb short bursts on the shared socket; the OS may cap this hint.
            udp.setReceiveBufferSize(4*1024*1024);
            udp.bind(new InetSocketAddress(udpHost, udpPort));
        } catch (SocketException | RuntimeException error) { udp.close(); throw error; }
        reader = new Thread(this::receiveUdp, "everywhere-relay-udp"); reader.setDaemon(true);
        closer=new Thread(this::closeSockets,"everywhere-relay-close"); closer.setDaemon(true); closer.start();
    }
    static int defaultReserve(int capacity) { return Math.min(capacity-1,(capacity+9)/10); }
    synchronized void enableFederation(Federation.Config config) {
        if(federation!=null)throw new IllegalStateException("Federation already configured");
        federation=new Federation(this,config);
    }
    // Server-to-server routing maintenance only; never part of the public health response.
    synchronized Set<String> activeRooms() { return Set.copyOf(rooms.keySet()); }
    public int udpPort() { return udp.getLocalPort(); }
    synchronized void enableUsage(Path file) {
        if (usage!=null || !sessions.isEmpty()) throw new IllegalStateException("Usage must be enabled before accepting players");
        usage=new UsageStatistics(file);
    }
    public synchronized JsonObject health() {
        boolean ok = !closed && reader.isAlive() && !udp.isClosed() && getPort() > 0;
        JsonObject j = new JsonObject(); j.addProperty("status", ok ? "ok" : "unavailable");
        j.addProperty("connections", sessions.size()); j.addProperty("uptimeSeconds", (System.currentTimeMillis()-startedAt)/1000);
        j.addProperty("capacity",maxSessions);
        int verified=verifiedSessions;
        j.addProperty("verifiedConnections",verified); j.addProperty("unverifiedConnections",sessions.size()-verified);
        j.addProperty("verifiedReserve",verifiedReserve); j.addProperty("pendingConnections",sockets.size()-sessions.size());
        j.addProperty("relayedUdpVoicePackets",relayedUdpVoice); j.addProperty("relayedWssVoicePackets",relayedWssVoice);
        j.addProperty("peerRefreshes",peerRefreshes); j.addProperty("peerDescriptions",peerDescriptions);
        j.addProperty("nearbyDeclarations",nearbyIndexes.values().stream().mapToInt(NearbyIndex::edges).sum());
        j.addProperty("udpBudgetDrops",udpBudgetDrops.get()); j.addProperty("relayQueueDrops",relayQueueDrops);
        j.addProperty("controlQueueDrops",controlQueueDrops);
        if(federation!=null) {
            JsonObject mesh=federation.health();mesh.addProperty("remoteSessions",remoteSessions.size());j.add("federation",mesh);
        }
        if (usage!=null) j.add("usage",usage.snapshot());
        j.addProperty("version", dev.eyecrasher.everywhere.core.BuildVersion.VERSION); j.addProperty("checkedAt", java.time.Instant.now().toString());
        return j;
    }
    @Override public void onStart() {
        reader.start(); clock.scheduleAtFixedRate(() -> expire(System.nanoTime()), 1, 1, TimeUnit.SECONDS);
        System.out.println("Relay ready on TCP " + getPort() + ", UDP " + udpPort());
    }
    @Override public void onOpen(WebSocket ws, ClientHandshake handshake) {
        if("/federation".equals(handshake.getResourceDescriptor())) {
            Federation mesh=federation;
            if(mesh==null || !mesh.accept(ws,handshake))ws.close(1008,"Federation authentication required");
            return;
        }
        boolean admitted;
        synchronized(this) { admitted=admit(ws,handshake); }
        if(!admitted)ws.close(1008,"Connection limit");
    }
    private boolean admit(WebSocket ws,ClientHandshake handshake) {
        String ip = ws.getRemoteSocketAddress().getAddress().getHostAddress();
        if (trustedProxy && ws.getRemoteSocketAddress().getAddress().isLoopbackAddress()) {
            String forwarded = handshake.getFieldValue("X-Real-IP");
            if (forwarded.matches("[0-9a-fA-F:.]{3,45}")) ip = forwarded;
        }
        if (ipLimits.size() >= 1024 && !ipLimits.containsKey(ip)) ipLimits.remove(ipLimits.keySet().iterator().next());
        // Let a bounded number of newcomers prove their account even when voice slots are full.
        // Pending connections cannot discover peers or send voice.
        long detachedCloses=pendingCloses.keySet().stream().filter(socket -> !sockets.containsKey(socket)).count();
        if (closed || sockets.size()+detachedCloses>=maxSessions+MAX_PENDING
                || !"/voice".equals(handshake.getResourceDescriptor()) || sockets.size()-sessions.size() >= MAX_PENDING
                || !accepts.take() || !ipLimits.computeIfAbsent(ip, k -> new RateLimit(1, 6)).take()) {
            return false;
        }
        Session s = new Session(ws);
        try {
            JsonObject source=new JsonObject(); source.addProperty("ip",ip); source.addProperty("port",1);
            s.controlAddress=Endpoints.read(source,!trustedProxy && udp.getLocalAddress().isLoopbackAddress()).getAddress();
        } catch(IllegalArgumentException ignored) {}
        sockets.put(ws, s);
        JsonObject j = Wire.message("challenge"); j.addProperty("challenge", s.challenge); j.addProperty("protocol", 1);
        j.addProperty("resumption", true); send(s, j); return true;
    }
    @Override public synchronized void onMessage(WebSocket ws, String text) {
        if(federation!=null && federation.incoming(ws)) { federation.text(ws,text);return; }
        Session s = sockets.get(ws);
        if (s == null || s.closeRequested) return;
        try {
            if (!s.control.take()) throw new IllegalArgumentException("Control rate");
            JsonObject j = Wire.parse(text); String type = Wire.string(j, "type", 24);
            if (type.equals("join")) { join(s, j); return; }
            if (!s.joined) throw new IllegalArgumentException("Not joined");
            if (type.equals("nearby")) {
                JsonArray list = j.getAsJsonArray("players");
                if (list.size() > MAX_NEARBY) throw new IllegalArgumentException("Nearby limit");
                Set<UUID> ids = new HashSet<>();
                for (JsonElement item : list) ids.add(UUID.fromString(item.getAsString()));
                if (!ids.equals(s.nearby)) updateNearby(s,Set.copyOf(ids));
            } else if (type.equals("path")) {
                s.udpPreferred = j.get("udp").getAsBoolean() && s.endpoint != null;
            } else if (type.equals("candidates")) {
                JsonArray list = j.getAsJsonArray("candidates");
                if (list.size() > 4) throw new IllegalArgumentException("Candidate limit");
                Set<InetSocketAddress> next = new LinkedHashSet<>();
                for (var item : list) {
                    InetSocketAddress endpoint = Endpoints.read(item.getAsJsonObject(), !trustedProxy && udp.getLocalAddress().isLoopbackAddress());
                    // Extra IPv4 mappings can vary in port, never in the validated source IP.
                    // Global IPv6 host candidates receive small consent checks before any media.
                    if (endpoint.getAddress() instanceof Inet6Address
                            || (s.endpoint != null && endpoint.getAddress().equals(s.endpoint.getAddress()))) next.add(endpoint);
                }
                Set<InetSocketAddress> tcp=new LinkedHashSet<>();
                if(s.transports && j.has("tcp")) {
                    JsonArray values=j.getAsJsonArray("tcp");
                    if(values.size()>4)throw new IllegalArgumentException("TCP candidate limit");
                    for(var item:values) {
                        InetSocketAddress endpoint=Endpoints.read(item.getAsJsonObject(),!trustedProxy && udp.getLocalAddress().isLoopbackAddress());
                        if(endpoint.getAddress() instanceof Inet6Address || endpoint.getAddress().equals(s.controlAddress)
                                || s.endpoint!=null && endpoint.getAddress().equals(s.endpoint.getAddress()))tcp.add(endpoint);
                    }
                }
                if (!next.equals(s.candidates) || !tcp.equals(s.tcpCandidates)) {
                    s.candidates=Set.copyOf(next); s.tcpCandidates=Set.copyOf(tcp); refreshPeerData(s);
                }
            } else if (!type.equals("heartbeat")) throw new IllegalArgumentException("Message type");
            s.lastControl = System.nanoTime();
        } catch (Exception e) { requestClose(ws,1008,"Invalid control message"); }
    }
    private void join(Session s, JsonObject j) throws Exception {
        if (s.joining || s.joined) throw new IllegalArgumentException("Already joining");
        s.joining = true;
        s.player = Wire.id(j, "player"); s.name = Wire.string(j, "name", 16);
        if (!s.name.matches("[A-Za-z0-9_]{1,16}")) throw new IllegalArgumentException("Player name");
        s.room = Wire.string(j, "room", 64); s.dimension = Wire.string(j, "dimension", 128);
        s.transports=j.has("transports") && j.get("transports").getAsInt()==1;
        if (!s.room.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Room");
        s.publicKey = Wire.string(j, "key", 128);
        KeyFactory.getInstance("X25519").generatePublic(new X509EncodedKeySpec(Wire.unbase64(s.publicKey, 64)));
        if (j.has("previousSession") || j.has("sessionProof")) {
            s.previousSession = Wire.id(j, "previousSession");
            s.sessionProof = Wire.unbase64(Wire.string(j, "sessionProof", 64), 32);
            if (s.sessionProof.length != 32) throw new IllegalArgumentException("Reconnect proof size");
        }
        boolean online = j.has("online") && j.get("online").getAsBoolean();
        if (!online) { finishJoin(s, false); return; }
        String accountName = Wire.string(j, "accountName", 16);
        UUID accountId = Wire.id(j, "accountId");
        s.account = accountId;
        if (!accountName.equalsIgnoreCase(s.name)
                || !(s.player.equals(accountId) || s.player.equals(Wire.offlineUuid(s.name))))
            throw new IllegalArgumentException("Account mismatch");
        verifier.verify(s.challenge,accountName,accountId).orTimeout(6,TimeUnit.SECONDS)
                .whenComplete((verified,error) -> {
                    synchronized(this) {
                        if(sockets.get(s.ws)!=s || s.closeRequested || !s.ws.isOpen())return;
                        if(error!=null || !Boolean.TRUE.equals(verified))requestClose(s.ws,1008,"Account verification failed");
                        else finishJoin(s,true);
                    }
                });
    }
    private CompletableFuture<Boolean> verifyAccount(String challenge,String accountName,UUID accountId) {
        URI uri = URI.create("https://sessionserver.mojang.com/session/minecraft/hasJoined?username="
                + URLEncoder.encode(accountName, StandardCharsets.UTF_8) + "&serverId=" + challenge);
        return http.sendAsync(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(6)).GET().build(), HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> response.statusCode()==200 && matchesProfile(response.body(),accountName,accountId));
    }
    static boolean matchesProfile(String body,String name,UUID account) {
        try {
            JsonObject profile=Wire.parse(body);
            return profile.get("id").getAsString().equalsIgnoreCase(account.toString().replace("-",""))
                    && profile.get("name").getAsString().equalsIgnoreCase(name);
        } catch(RuntimeException invalid) { return false; }
    }
    private void finishJoin(Session s, boolean verified) {
        if(sockets.get(s.ws)!=s || s.closeRequested || !s.ws.isOpen())return;
        Map<UUID,Session> room = rooms.get(s.room);
        Session previous=room==null?null:room.get(s.player);
        RemotePeer remote=remoteRooms.getOrDefault(s.room,Map.of()).get(s.player);
        if(remote!=null && (!verified || remote.verified)) {
            requestClose(s.ws,1008,"This player already has a session");return;
        }
        boolean resumes = previous != null && previous.id.equals(s.previousSession)
                && (!previous.verified || (verified && Objects.equals(previous.account, s.account)))
                && s.sessionProof != null && SessionProof.verify(s.sessionProof, previous.udpKey, previous.id,
                        s.player, s.room, s.challenge, s.publicKey);
        if(previous!=null && !resumes && (!verified || previous.verified)) {
            requestClose(s.ws,1008,"This player already has a session"); return;
        }
        int replaced=previous==null?0:1;
        if(sessions.size()-replaced>=maxSessions) { requestClose(s.ws,1008,"Voice capacity reached"); return; }
        int unverifiedReplaced = previous != null && !previous.verified ? 1 : 0;
        if(!verified && sessions.size()-verifiedSessions-unverifiedReplaced>=maxSessions-verifiedReserve) {
            requestClose(s.ws,1008,"Remaining slots reserved for verified accounts"); return;
        }
        if(previous!=null) {
            detach(previous);
            requestClose(previous.ws,1008,resumes ? "Session replaced by reconnect" : "Verified account reclaimed this player");
        }
        s.verified = verified; s.joined = true; s.lastControl = System.nanoTime(); sessions.put(s.id, s);
        s.joinedAt=System.nanoTime();
        if(usage!=null)usage.joined(verified,resumes,sessions.size());
        if(verified)verifiedSessions++;
        room=rooms.computeIfAbsent(s.room,k -> new HashMap<>());
        nearbyIndexes.computeIfAbsent(s.room,k -> new NearbyIndex<>());
        room.put(s.player, s);
        JsonObject j = Wire.message("welcome"); j.addProperty("session", s.id.toString());
        j.addProperty("udpKey", Wire.base64(s.udpKey)); j.addProperty("udpPort", udpPort());
        j.addProperty("verified", verified); j.addProperty("candidates", true); j.addProperty("transports",1); send(s, j); refresh(s);publish(s);
    }
    private boolean allowed(Session a, Session b) {
        return a != b && a.joined && b.joined && !a.closeRequested && !b.closeRequested && a.room.equals(b.room) && a.dimension.equals(b.dimension)
                && a.nearby.contains(b.player) && b.nearby.contains(a.player);
    }
    private void updateNearby(Session s,Set<UUID> next) {
        // Only an old/new mutually declared neighbor can gain or lose this player.
        // Bound the work by two lists of 24, even if every other client names s.
        Map<UUID,Session> room=rooms.get(s.room);
        NearbyIndex<Session> index=nearbyIndexes.get(s.room);
        Set<Session> affected=new LinkedHashSet<>(); affected.add(s);
        collectObservers(s,s.nearby,room,index,affected);
        collectObservers(s,next,room,index,affected);
        index.replace(s,s.nearby,next); s.nearby=next;
        affected.forEach(this::refresh);
        publish(s);
    }
    private static void collectObservers(Session s,Set<UUID> players,Map<UUID,Session> room,
                                         NearbyIndex<Session> index,Set<Session> affected) {
        for(UUID player:players) {
            Session candidate=room.get(player);
            if(candidate!=null && index.watches(s.player,candidate))affected.add(candidate);
        }
    }
    private void collectPeerDataUpdates(Session s,Set<Session> affected) {
        affected.add(s);
        collectObservers(s,s.nearby,rooms.get(s.room),nearbyIndexes.get(s.room),affected);
    }
    private void refreshPeerData(Session s) {
        Set<Session> affected=new LinkedHashSet<>(); collectPeerDataUpdates(s,affected);
        affected.forEach(this::refresh);
        publish(s);
    }
    private void refresh(Session a) {
        if(!a.joined || a.closeRequested)return;
        Map<UUID,Session> room = rooms.get(a.room);
        if (room == null) return;
        peerRefreshes++;
        JsonObject j = Wire.message("peers"); JsonArray list = new JsonArray(); Set<UUID> links = new HashSet<>();
        for (UUID player : a.nearby) {
            Session b = room.get(player);
            if(b==null) {
                RemotePeer remote=remoteRooms.getOrDefault(a.room,Map.of()).get(player);
                if(remote!=null && allowed(a,remote)) { links.add(remote.id);peerDescriptions++;list.add(description(a,remote)); }
                continue;
            }
            if (!allowed(a, b)) continue;
            links.add(b.id); peerDescriptions++; JsonObject p = new JsonObject();
            p.addProperty("session", b.id.toString()); p.addProperty("player", b.player.toString());
            p.addProperty("name", b.name); p.addProperty("key", b.publicKey); p.addProperty("verified", b.verified);
            if (b.endpoint != null) { p.addProperty("ip", b.endpoint.getAddress().getHostAddress()); p.addProperty("port", b.endpoint.getPort()); }
            JsonArray candidates = new JsonArray();
            for (var endpoint : b.candidates)
                if (endpoint.getAddress() instanceof Inet6Address || (b.endpoint != null && endpoint.getAddress().equals(b.endpoint.getAddress())))
                    candidates.add(Endpoints.write(endpoint));
            p.add("candidates", candidates);
            if(a.transports && b.transports) {
                p.addProperty("transports",1); JsonArray tcp=new JsonArray();
                b.tcpCandidates.forEach(endpoint -> tcp.add(Endpoints.write(endpoint))); p.add("tcp",tcp);
            }
            list.add(p);
        }
        a.links = Set.copyOf(links); j.add("peers", list); send(a, j);
    }
    @Override public synchronized void onMessage(WebSocket ws, ByteBuffer buffer) {
        if(federation!=null && federation.incoming(ws)) { federation.data(ws,buffer);return; }
        Session s = sockets.get(ws);
        if (s == null || s.closeRequested || !s.joined || !s.packets.take() || buffer.remaining() > Wire.MAX_FRAME) return;
        byte[] bytes = new byte[buffer.remaining()]; buffer.get(bytes);
        try { forward(s, Wire.decode(bytes), bytes); } catch (IllegalArgumentException ignored) {}
    }
    private void forward(Session s, Wire.Frame frame, byte[] bytes) {
        if (!frame.from().equals(s.id)) return;
        Session target = sessions.get(frame.to());
        if(target==null) {
            RemotePeer remote=remoteSessions.get(frame.to());
            if(remote!=null && federation!=null && s.links.contains(remote.id) && allowed(s,remote)
                    && media(frame))federation.forward(remote.node,bytes);
            return;
        }
        if (target == null || !s.links.contains(target.id) || !allowed(s, target)) return;
        deliver(target,frame,bytes);
    }
    private void deliver(Session target,Wire.Frame frame,byte[] bytes) {
        if (frame.kind() != Wire.END && target.udpPreferred && target.endpoint != null && System.nanoTime() - target.lastUdp < TimeUnit.SECONDS.toNanos(8)) {
            if(sendUdp(target, Wire.ROUTE, bytes, target.endpoint) && frame.kind()==Wire.VOICE)relayedUdpVoice++;
        } else if (queued(target.ws) < MAX_QUEUE) {
            target.ws.send(bytes); if(frame.kind()==Wire.VOICE)relayedWssVoice++;
        } else relayQueueDrops++;
    }
    private void receiveUdp() {
        byte[] buffer = new byte[Wire.MAX_UDP + 1];
        while (!closed) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length); udp.receive(packet);
                if (packet.getLength() > Wire.MAX_UDP) continue;
                byte[] bytes = Arrays.copyOf(buffer, packet.getLength());
                if (Stun.isRequest(bytes)) {
                    InetAddress ip = packet.getAddress();
                    if (!stunBudget.take()) continue;
                    if (stunLimits.size() >= 1024 && !stunLimits.containsKey(ip)) stunLimits.remove(stunLimits.keySet().iterator().next());
                    if (stunLimits.computeIfAbsent(ip, k -> new RateLimit(2, 6)).take()) {
                        byte[] response = Stun.response(bytes, (InetSocketAddress) packet.getSocketAddress());
                        udp.send(new DatagramPacket(response, response.length, packet.getSocketAddress()));
                    }
                    continue;
                }
                if (!udpBudget.take()) { udpBudgetDrops.incrementAndGet(); continue; }
                synchronized (this) {
                    Session s = sessions.get(Wire.datagramSession(bytes));
                    if (s == null || s.closeRequested || !s.packets.take()) continue;
                    Wire.Datagram d = Wire.openDatagram(bytes, s.udpKey, false);
                    if (!s.udpReplay.accept(d.sequence())) continue;
                    InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
                    if (d.op() == Wire.BIND) {
                        // A fresh authenticated packet from the already confirmed endpoint renews
                        // the lease. A new/NAT-rebound address still has to prove return routability.
                        if(source.equals(s.endpoint)) {
                            s.lastUdp=System.nanoTime(); sendUdp(s,Wire.BOUND,new byte[0],source); continue;
                        }
                        s.pendingEndpoint = source; s.pendingChallenge = Wire.random(16); s.pendingAt = System.nanoTime();
                        sendUdp(s, Wire.CHALLENGE, s.pendingChallenge, source);
                    } else if (d.op() == Wire.CONFIRM && source.equals(s.pendingEndpoint)
                            && System.nanoTime() - s.pendingAt < TimeUnit.SECONDS.toNanos(5)
                            && Arrays.equals(s.pendingChallenge, d.body())) {
                        boolean changed = !source.equals(s.endpoint); s.endpoint = source; s.lastUdp = System.nanoTime();
                        s.pendingChallenge = null; s.pendingEndpoint = null;
                        sendUdp(s, Wire.BOUND, new byte[0], source); if (changed) refreshPeerData(s);
                    } else if (d.op() == Wire.ROUTE && source.equals(s.endpoint)) {
                        s.lastUdp = System.nanoTime(); forward(s, Wire.decode(d.body()), d.body());
                    }
                }
            } catch (Exception e) { if (closed) return; }
        }
    }
    private boolean sendUdp(Session s, byte op, byte[] body, InetSocketAddress endpoint) {
        try {
            byte[] bytes = Wire.datagram(s.id, s.udpCounter.getAndIncrement(), op, body, s.udpKey, true);
            udp.send(new DatagramPacket(bytes, bytes.length, endpoint)); return true;
        } catch (Exception ignored) { return false; }
    }
    private static boolean media(Wire.Frame frame) { return frame.kind()==Wire.VOICE || frame.kind()==Wire.END; }
    private boolean allowed(Session local,RemotePeer remote) {
        return local.joined && !local.closeRequested && !local.player.equals(remote.player)
                && local.room.equals(remote.room) && local.dimension.equals(remote.dimension)
                && local.nearby.contains(remote.player) && remote.nearby.contains(local.player);
    }
    private JsonObject description(Session local,RemotePeer remote) {
        JsonObject peer=new JsonObject();peer.addProperty("session",remote.id.toString());peer.addProperty("player",remote.player.toString());
        peer.addProperty("name",remote.name);peer.addProperty("key",remote.key);peer.addProperty("verified",remote.verified);
        if(remote.endpoint!=null) { peer.addProperty("ip",remote.endpoint.getAddress().getHostAddress());peer.addProperty("port",remote.endpoint.getPort()); }
        JsonArray candidates=new JsonArray();remote.candidates.forEach(address -> candidates.add(Endpoints.write(address)));peer.add("candidates",candidates);
        if(local.transports && remote.transports) {
            peer.addProperty("transports",1);JsonArray tcp=new JsonArray();remote.tcp.forEach(address -> tcp.add(Endpoints.write(address)));peer.add("tcp",tcp);
        }
        return peer;
    }
    private JsonObject member(Session local) {
        JsonObject value=Wire.message("member");value.addProperty("session",local.id.toString());value.addProperty("player",local.player.toString());
        value.addProperty("name",local.name);value.addProperty("room",local.room);value.addProperty("dimension",local.dimension);
        value.addProperty("key",local.publicKey);value.addProperty("verified",local.verified);value.addProperty("transports",local.transports);
        JsonArray nearby=new JsonArray();local.nearby.forEach(player -> nearby.add(player.toString()));value.add("nearby",nearby);
        if(local.endpoint!=null)value.add("endpoint",Endpoints.write(local.endpoint));
        JsonArray candidates=new JsonArray();local.candidates.forEach(address -> candidates.add(Endpoints.write(address)));value.add("candidates",candidates);
        JsonArray tcp=new JsonArray();local.tcpCandidates.forEach(address -> tcp.add(Endpoints.write(address)));value.add("tcp",tcp);
        return value;
    }
    private void publish(Session local) { if(federation!=null && local.joined && !local.closeRequested)federation.broadcast(member(local)); }
    synchronized void federationSnapshot(Consumer<JsonObject> send) {
        for(Session local:sessions.values())if(local.joined && !local.closeRequested)send.accept(member(local));
    }
    private Set<InetSocketAddress> remoteEndpoints(JsonObject value,String field) {
        JsonArray array=value.getAsJsonArray(field);if(array.size()>4)throw new IllegalArgumentException("Federation candidates");
        Set<InetSocketAddress> result=new LinkedHashSet<>();
        for(JsonElement element:array)result.add(Endpoints.read(element.getAsJsonObject(),federation.loopbackOnly()));
        return Set.copyOf(result);
    }
    synchronized void federationMember(String node,JsonObject value) {
        UUID id=Wire.id(value,"session"),player=Wire.id(value,"player");String room=Wire.string(value,"room",64);
        String name=Wire.string(value,"name",16),key=Wire.string(value,"key",128),dimension=Wire.string(value,"dimension",128);
        if(!room.matches("[a-f0-9]{64}") || !name.matches("[A-Za-z0-9_]{1,16}") || sessions.containsKey(id))
            throw new IllegalArgumentException("Federation identity");
        try { KeyFactory.getInstance("X25519").generatePublic(new X509EncodedKeySpec(Wire.unbase64(key,64))); }
        catch(GeneralSecurityException invalid) { throw new IllegalArgumentException("Federation public key",invalid); }
        RemotePeer previous=remoteSessions.get(id);
        if(previous!=null && (!previous.node.equals(node) || !previous.player.equals(player) || !previous.room.equals(room)))
            throw new IllegalArgumentException("Federation session changed identity");
        if(previous==null && remoteCounts.getOrDefault(node,0)>=MAX_SESSIONS)
            throw new IllegalArgumentException("Federation session limit");
        JsonArray list=value.getAsJsonArray("nearby");if(list.size()>MAX_NEARBY)throw new IllegalArgumentException("Federation nearby limit");
        Set<UUID> nearby=new HashSet<>();list.forEach(element -> nearby.add(UUID.fromString(element.getAsString())));
        RemotePeer next=new RemotePeer(node,id,player,name,room,dimension,key,value.get("verified").getAsBoolean(),value.get("transports").getAsBoolean(),
                Set.copyOf(nearby),value.has("endpoint")?Endpoints.read(value.getAsJsonObject("endpoint"),federation.loopbackOnly()):null,
                remoteEndpoints(value,"candidates"),remoteEndpoints(value,"tcp"));
        remoteSessions.put(id,next);
        if(previous==null) { remoteCounts.merge(node,1,Integer::sum);remotePacketLimits.put(id,new RateLimit(1500,200)); }
        Map<UUID,RemotePeer> peers=remoteRooms.computeIfAbsent(room,unused -> new HashMap<>());
        RemotePeer existing=peers.get(player);
        if(existing==null || existing.id.equals(id) || wins(next.verified,next.node,id,existing.verified,existing.node,existing.id))peers.put(player,next);
        Session local=rooms.getOrDefault(room,Map.of()).get(player);
        if(local!=null && wins(next.verified,node,id,local.verified,federation.config.node(),local.id)) {
            detach(local);requestClose(local.ws,1008,"This player already has a session");
        }
        refreshRemote(previous);refreshRemote(next);
    }
    private static boolean wins(boolean verified,String node,UUID id,boolean otherVerified,String otherNode,UUID otherId) {
        if(verified!=otherVerified)return verified;
        int order=node.compareTo(otherNode);return order<0 || order==0 && id.compareTo(otherId)<0;
    }
    private void refreshRemote(RemotePeer remote) {
        if(remote==null)return;
        Map<UUID,Session> locals=rooms.getOrDefault(remote.room,Map.of());
        for(UUID player:remote.nearby) { Session local=locals.get(player);if(local!=null && local.nearby.contains(remote.player))refresh(local); }
    }
    synchronized void federationLeave(String node,UUID session) {
        RemotePeer remote=remoteSessions.get(session);if(remote==null)return;
        if(!remote.node.equals(node))throw new IllegalArgumentException("Federation session owner");
        remoteSessions.remove(session);remotePacketLimits.remove(session);remoteCounts.computeIfPresent(node,(key,count) -> count==1?null:count-1);
        Map<UUID,RemotePeer> room=remoteRooms.get(remote.room);
        if(room!=null) { room.remove(remote.player,remote);if(room.isEmpty())remoteRooms.remove(remote.room); }
        refreshRemote(remote);
    }
    synchronized void federationDisconnected(String node) {
        List<UUID> removed=remoteSessions.values().stream().filter(remote -> remote.node.equals(node)).map(RemotePeer::id).toList();
        removed.forEach(id -> federationLeave(node,id));
    }
    synchronized boolean federationFrame(String node,Wire.Frame frame,byte[] bytes) {
        RemotePeer source=remoteSessions.get(frame.from());Session target=sessions.get(frame.to());
        if(source==null || !source.node.equals(node) || target==null || !media(frame) || !target.links.contains(source.id)
                || !allowed(target,source) || !federationPackets.take() || !remotePacketLimits.get(source.id).take())return false;
        deliver(target,frame,bytes);return true;
    }
    private void send(Session s, JsonObject message) {
        if(s.closeRequested)return;
        if (s.ws.isOpen() && queued(s.ws) < MAX_QUEUE) s.ws.send(Wire.GSON.toJson(message));
        else { controlQueueDrops++; requestClose(s.ws,1008,"Slow connection"); }
    }
    private synchronized void requestClose(WebSocket socket,int code,String reason) {
        if(closed || pendingCloses.containsKey(socket))return;
        // Admission counts detached pending closes too, so even churn cannot grow
        // this queue beyond the maximum admitted sockets plus pending handshakes.
        if(pendingCloses.size()>=maxSessions+MAX_PENDING)throw new IllegalStateException("Close queue capacity invariant");
        Session session=sockets.get(socket); if(session!=null)session.closeRequested=true;
        CloseRequest request=new CloseRequest(socket,code,reason); pendingCloses.put(socket,request);
        if(!closeQueue.offer(request))throw new IllegalStateException("Close queue capacity invariant");
    }
    private void closeSockets() {
        while(!Thread.currentThread().isInterrupted() && (!closed || !closeQueue.isEmpty())) {
            CloseRequest request;
            try { request=closeQueue.poll(100,TimeUnit.MILLISECONDS);if(request==null)continue; }
            catch(InterruptedException stopped) { Thread.currentThread().interrupt(); return; }
            try { request.socket.close(request.code,request.reason); }
            catch(RuntimeException error) { onError(request.socket,error); }
            finally { synchronized(this) { pendingCloses.remove(request.socket,request); } }
        }
    }
    private static long queued(WebSocket ws) {
        if (!(ws instanceof WebSocketImpl impl)) return Long.MAX_VALUE;
        long total = 0;
        for (ByteBuffer b : impl.outQueue) { total += b.remaining(); if (total >= MAX_QUEUE) break; }
        return total;
    }
    // Explicit time parameter lets tests prove lease expiry without waiting half a minute.
    synchronized void expire(long now) {
        Set<Session> changed = new LinkedHashSet<>();
        for (Session s : List.copyOf(sockets.values())) {
            if (!s.ws.isOpen()) { detach(s); continue; }
            if (!s.joined && now - s.created > TimeUnit.SECONDS.toNanos(15)) { detach(s); requestClose(s.ws,1008,"Join timeout"); continue; }
            if (s.joined && now - s.lastControl > TimeUnit.SECONDS.toNanos(30)) {
                detach(s); requestClose(s.ws,1008,"Voice heartbeat timed out"); continue;
            }
            if (s.endpoint != null && now - s.lastUdp > TimeUnit.SECONDS.toNanos(12)) {
                s.endpoint = null; collectPeerDataUpdates(s,changed);publish(s);
            }
        }
        changed.forEach(this::refresh);
    }
    @Override public synchronized void onClose(WebSocket ws, int code, String reason, boolean remote) {
        if(federation!=null && federation.incoming(ws)) { federation.disconnected(ws);return; }
        Session s = sockets.get(ws);
        if(s!=null)detach(s);
    }
    private void detach(Session s) {
        sockets.remove(s.ws,s); s.joined=false; s.links=Set.of();
        if (sessions.remove(s.id,s)) {
            if(federation!=null) { JsonObject leave=Wire.message("leave");leave.addProperty("session",s.id.toString());federation.broadcast(leave); }
            if(usage!=null)usage.left(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-s.joinedAt),sessions.size());
            if(s.verified)verifiedSessions--;
            Set<Session> affected=new LinkedHashSet<>();
            NearbyIndex<Session> index=nearbyIndexes.get(s.room);
            Map<UUID,Session> room = rooms.get(s.room); room.remove(s.player,s);
            collectObservers(s,s.nearby,room,index,affected);
            index.replace(s,s.nearby,Set.of());
            if (room.isEmpty()) { rooms.remove(s.room); nearbyIndexes.remove(s.room); }
            else affected.forEach(this::refresh);
        }
    }
    @Override public void onError(WebSocket ws, Exception e) {
        // Never log control messages, authentication material, or per-client addresses.
        System.err.println("Relay transport error: " + e.getClass().getSimpleName());
    }
    @Override public void close() {
        synchronized(this) { if(closed)return; closed=true; }
        if(federation!=null)federation.close();
        udp.close(); clock.shutdownNow(); auth.shutdownNow();
        try { stop(2000); }
        catch(InterruptedException error) { Thread.currentThread().interrupt(); }
        finally {
            try { closer.join(2000); } catch(InterruptedException error) { Thread.currentThread().interrupt(); }
            finally { closer.interrupt(); }
            synchronized(this) {
                for(Session session:List.copyOf(sessions.values()))detach(session);
                closeQueue.clear(); pendingCloses.clear();
            }
            if(usage!=null)usage.close();
        }
    }
}
