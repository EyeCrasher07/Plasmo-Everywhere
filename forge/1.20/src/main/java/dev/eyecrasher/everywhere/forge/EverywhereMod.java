package dev.eyecrasher.everywhere.forge;

import dev.eyecrasher.everywhere.core.*;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.components.AbstractWidget;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.client.event.ClientPlayerNetworkEvent;
import net.minecraftforge.client.event.ScreenEvent;
import net.minecraftforge.client.event.RegisterClientCommandsEvent;
import net.minecraftforge.client.event.RegisterKeyMappingsEvent;
import net.minecraftforge.event.GameShuttingDownEvent;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;
import static net.minecraft.commands.Commands.literal;
import net.minecraft.client.KeyMapping;
import com.mojang.blaze3d.platform.InputConstants;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.components.Tooltip;
import net.minecraft.client.gui.screens.PauseScreen;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.multiplayer.resolver.ServerAddress;
import net.minecraft.network.chat.Component;
import org.slf4j.*;
import su.plo.voice.api.client.connection.ServerConnection;
import su.plo.voice.api.addon.ClientAddonsLoader;
import su.plo.voice.api.addon.AddonLoaderScope;
import su.plo.voice.api.addon.annotation.Addon;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Addon(id="plasmo_everywhere", name="Plasmo Everywhere", version=BuildVersion.VERSION, authors={}, scope=AddonLoaderScope.CLIENT)
public final class EverywhereMod {
    public static final Logger LOG = LoggerFactory.getLogger("PlasmoEverywhere");
    private static EverywhereMod instance;
    enum State { WAITING, CHECKING, FINDING, CONNECTING, CONNECTED, MOVING, RETRYING, NATIVE, DISABLED, DISCONNECTED }
    private EverywhereConfig config;
    private DiagnosticLog diagnosticLog;
    private ConnectionTrace trace;
    private DiagnosticReport.Snapshot lastConnection;
    private final VerificationPolicy verificationPolicy=new VerificationPolicy();
    private volatile VoiceClient transport;
    private VoiceClient.Resumption resumption;
    private NetworkDirectory.Warmup directoryWarmup;
    private ConnectionPreparation<PreparedTransport> preparation;
    private volatile Thread connectionThread;
    // At most one startup is owned by the game thread. Cancellation removes queued startups;
    // the same worker retires old network resources before constructing their replacements.
    private final ThreadPoolExecutor connectionWorker = new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(8),task -> {
                Thread thread=new Thread(task,"everywhere-connect"); thread.setDaemon(true); connectionThread=thread; return thread;
            },new ThreadPoolExecutor.AbortPolicy());
    private volatile PlasmoBridge bridge;
    private ClientLevel world;
    private SessionScope scope;
    private volatile boolean nativeVoice, worldAudioAllowed;
    private boolean resolving, settingsRequested;
    private int ticks, attempt, waitTicks, nextDiagnosticsTick, resumeWorldTick;
    private long retainedWorldChanges, sessionRestarts;
    private volatile long generation, audioEpoch;
    private State state = State.WAITING;
    private String retryCause = "connection";
    private KeyMapping settingsKey;
    private volatile Map<UUID,Double> distances = Map.of();
    private Collection<VoiceClient.PeerInfo> peers = List.of();
    private int directConnections;
    private final AudioSessionGate audioSessions = new AudioSessionGate();
    private final ArrayBlockingQueue<Received> received = new ArrayBlockingQueue<>(256);
    private record Received(long generation, long audioEpoch, VoiceClient.PeerInfo peer, long seq, int distance, byte[] opus, boolean end) {}
    private record PreparedTransport(VoiceClient client) implements AutoCloseable {
        @Override public void close() { client.close(); }
    }
    private static final class DirectoryFailure extends Exception {
        DirectoryFailure(Exception cause) { super(cause); }
    }

    public static void initialize() { new EverywhereMod().onInitializeClient(); }
    private void onInitializeClient() {
        instance = this; config = EverywhereConfig.load();
        diagnosticLog=new DiagnosticLog(Minecraft.getInstance().gameDirectory.toPath().resolve("logs/plasmo-everywhere"));
        trace=new ConnectionTrace(event -> {
            diagnosticLog.append(event);
            if(event.stage().equals("CONTROL_CLOSE") || Set.of("FAILED","FALLBACK").contains(event.result()))
                LOG.info("Connection attempt {}: {} {} ({} ms, {}, code={})",event.attempt(),event.stage(),event.result(),event.durationMillis(),event.error(),event.code());
        });
        trace.record("LIFECYCLE","START",0,null,0);
        ClientAddonsLoader.INSTANCE.load(this);
        // Leave the optional shortcut unbound so it cannot steal another mod's key.
        settingsKey = new KeyMapping("key.plasmo_everywhere.settings", InputConstants.UNKNOWN.getValue(), "key.categories.multiplayer");
        FMLJavaModLoadingContext.get().getModEventBus().addListener((RegisterKeyMappingsEvent event) -> event.register(settingsKey));
        MinecraftForge.EVENT_BUS.addListener((ScreenEvent.Init.Post event) -> {
            if (event.getScreen() instanceof PauseScreen) addPauseButton(Minecraft.getInstance(),event);
        });
        MinecraftForge.EVENT_BUS.addListener((ClientPlayerNetworkEvent.LoggingIn event) -> {
            var mc=Minecraft.getInstance();
            shutdown(false); verificationPolicy.reset();
            trace.record("LIFECYCLE","START",0,null,0);
            nativeVoice = false; world = mc.level; waitTicks = 100; attempt = 0;
            state = State.CHECKING;
        });
        MinecraftForge.EVENT_BUS.addListener((ClientPlayerNetworkEvent.LoggingOut event) -> {
            shutdown(false); trace.record("LIFECYCLE","CLOSED",0,null,0);
            nativeVoice = false; world = null; state = State.DISCONNECTED;
        });
        MinecraftForge.EVENT_BUS.addListener((GameShuttingDownEvent event) -> {
            shutdown(); connectionWorker.shutdown(); trace.record("LIFECYCLE","CLOSED",0,null,0); diagnosticLog.close();
        });
        MinecraftForge.EVENT_BUS.addListener((TickEvent.ClientTickEvent event) -> {
            if (event.phase==TickEvent.Phase.END) tick(Minecraft.getInstance());
        });
        MinecraftForge.EVENT_BUS.addListener((RegisterClientCommandsEvent event) -> event.getDispatcher().register(
                literal("everywhere").executes(ctx -> { message(summary()); return 1; })
                .then(literal("settings").executes(ctx -> { settingsRequested=true; return 1; }))
                .then(literal("debug").executes(ctx -> { diagnostics(true); return 1; }))
                .then(literal("report").executes(ctx -> { message(saveReport()); return 1; }))
                .then(literal("reconnect").executes(ctx -> { reconnect(); message(statusComponent()); return 1; }))
                .then(literal("off").executes(ctx -> { setEnabled(false); message(statusComponent()); return 1; }))
                .then(literal("on").executes(ctx -> { setEnabled(true); message(statusComponent()); return 1; }))));
        if(Boolean.getBoolean("everywhere.uiSmoke")) {
            try { Class.forName("dev.eyecrasher.everywhere.forge.UiSmoke").getConstructor().newInstance(); }
            catch(ReflectiveOperationException error) { throw new IllegalStateException("Missing development UI smoke driver",error); }
        }
        LOG.info("Plasmo Everywhere {} initialized for Forge 1.20",BuildVersion.VERSION);
    }
    public static ServerConnection fallbackConnection() { return instance == null ? null : instance.bridge; }
    public static void nativeDetected() {
        EverywhereMod mod = instance;
        if (mod == null) return;
        mod.nativeVoice = true;
        // Native setup owns all Plasmo state after this adapter closes.
        mod.shutdown(); mod.state = State.NATIVE;
    }
    public static void levelChanging(ClientLevel next) {
        EverywhereMod mod = instance;
        if (mod == null || mod.world == next) return;
        // Do not change native PV state. Defer identity decisions until the next client tick,
        // when both the new local player and dimension have been installed.
        mod.suspendWorldAudio();
        if (!mod.nativeVoice && mod.transport != null) mod.state = State.MOVING;
    }
    private void suspendWorldAudio() {
        worldAudioAllowed = false; audioEpoch++; distances = Map.of(); audioSessions.clear(); received.clear();
        if (transport != null) transport.nearby(Set.of());
        try { if (bridge != null) bridge.resetWorld(); }
        catch (RuntimeException error) { LOG.warn("Could not finish old-world voice cleanup",error); }
        peers = List.of(); directConnections = 0;
    }
    private void tick(Minecraft mc) {
        ticks++;
        // ChatScreen closes itself after synchronous command dispatch. Open on the next tick.
        if (settingsRequested) { settingsRequested=false; openSettings(null); }
        while (settingsKey.consumeClick()) if (mc.screen == null) openSettings(null);
        if (!config.enabled || mc.player == null || mc.level == null || mc.getCurrentServer() == null) {
            if (transport != null || bridge != null || resolving) shutdown(config.enabled);
            if (!config.enabled) state = State.DISABLED;
            return;
        }
        if (nativeVoice) return;
        SessionScope current;
        try { current = captureScope(mc); }
        catch (Exception invalid) { if (transport != null || resolving) shutdown(false); state = State.WAITING; return; }
        boolean changingWorld = world != mc.level;
        if (changingWorld || scope != null && !scope.retains(current)) {
            suspendWorldAudio(); world = mc.level;
            if (scope != null && scope.retains(current) && (transport != null || resolving)) {
                retainedWorldChanges++; resumeWorldTick = ticks + 1;
            } else {
                shutdown(false); attempt = 0; waitTicks = 20; state = State.MOVING;
            }
            return;
        }
        if (waitTicks > 0) { waitTicks--; return; }
        if (transport == null) { if(!resolving)connect(mc,current); return; }
        if (ticks < resumeWorldTick || bridge == null || !transport.isReady()) return;
        List<net.minecraft.world.entity.player.Player> local = new ArrayList<>(mc.level.players());
        local.removeIf(p -> p == mc.player || p.isSpectator() || p.distanceToSqr(mc.player) > 48*48);
        local.sort(Comparator.comparingDouble(p -> p.distanceToSqr(mc.player)));
        Map<UUID,Double> snapshot = new HashMap<>();
        local.stream().limit(24).forEach(p -> snapshot.put(p.getUUID(), Math.sqrt(p.distanceToSqr(mc.player))));
        boolean resuming = !worldAudioAllowed;
        distances = Map.copyOf(snapshot);
        if (resuming || ticks % 10 == 0) transport.nearby(snapshot.keySet());
        if (resuming) {
            peers = transport.peers(); audioSessions.update(peers); bridge.updatePlayers(peers); worldAudioAllowed = true; state = State.CONNECTED;
        }
        Set<UUID> audibleSessions = peers.stream().filter(p -> snapshot.containsKey(p.player())).map(VoiceClient.PeerInfo::session).collect(Collectors.toSet());
        bridge.retainSources(audibleSessions);
        for (int n=0; n<96; n++) {
            Received r=received.poll(); if(r==null)break;
            if(r.generation!=generation || r.audioEpoch!=audioEpoch || !audioSessions.permits(r.peer)
                    || !audibleSessions.contains(r.peer.session()) || !snapshot.containsKey(r.peer.player()) || snapshot.get(r.peer.player()) > r.distance)continue;
            if(mc.level.getPlayerByUUID(r.peer.player())==null)continue;
            try { bridge.audio(r.peer,r.seq,r.distance,r.opus,r.end); }
            catch (Exception e) { LOG.debug("Dropped unusable audio source",e); }
        }
        if (ticks >= nextDiagnosticsTick && !peers.isEmpty()) {
            nextDiagnosticsTick=ticks+1200; diagnostics(false);
        }
    }
    private static SessionScope captureScope(Minecraft mc) throws Exception {
        ServerAddress address = ServerAddress.parseString(mc.getCurrentServer().ip);
        String canonical = address.getHost().toLowerCase(Locale.ROOT)+":"+address.getPort();
        String room = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8)));
        return new SessionScope(mc.getConnection(), room, mc.level.dimension().toString(), mc.player.getUUID());
    }
    private void connect(Minecraft mc, SessionScope current) {
        long epoch=++generation, started=System.nanoTime(); scope=current; sessionRestarts++;
        int traceAttempt=trace.startAttempt();
        trace.recordForAttempt(traceAttempt,"SESSION","START",0,null,0);
        trace.recordForAttempt(traceAttempt,"CONFIG","ENABLED",0,null,(config.direct?1:0)+(config.udp?2:0));
        try {
            UUID serverId=UUID.nameUUIDFromBytes(("everywhere/"+current.room()).getBytes(StandardCharsets.UTF_8));
            String name=mc.player.getGameProfile().getName();
            var user=mc.getUser();
            VoiceClient.Identity id=new VoiceClient.Identity(current.player(),name,user.getGameProfile().getId(),user.getName(),current.room(),current.dimension());
            List<URI> directories=config.directoryUris();
            URI fallback=directories.isEmpty()?URI.create(config.relay):null;
            boolean direct=config.direct, udp=config.udp, verify=verificationPolicy.shouldVerify();
            VoiceClient.Resumption previous=resumption;
            // Capture Minecraft-owned objects and identity on its thread. Only the existing
            // session service's network call is used from the authentication worker.
            var sessionService=mc.getMinecraftSessionService();
            var account=user.getGameProfile(); String accessToken=user.getAccessToken();
            VoiceClient.Authenticator authenticator=challenge -> CompletableFuture.supplyAsync(() -> {
                if(epoch!=generation)return false;
                long authStarted=System.nanoTime();
                if(!verify) {
                    trace.recordForAttempt(traceAttempt,"ACCOUNT","UNVERIFIED",0,null,0); return false;
                }
                if(!VerificationPolicy.canVerifyIdentity(id.player(),id.name(),id.account(),id.accountName())
                        || accessToken==null || accessToken.length()<20) {
                    trace.recordForAttempt(traceAttempt,"ACCOUNT","SKIPPED",0,null,0); return false;
                }
                trace.recordForAttempt(traceAttempt,"ACCOUNT","START",0,null,0);
                try {
                    // The game's session service owns its token; it never reaches our voice nodes or logs.
                    sessionService.joinServer(account,accessToken,challenge);
                    if(epoch==generation)trace.recordForAttempt(traceAttempt,"ACCOUNT","SUCCESS",elapsed(authStarted),null,0);
                    return true;
                } catch(Exception error) {
                    if(epoch==generation)trace.recordForAttempt(traceAttempt,"ACCOUNT","FAILED",elapsed(authStarted),error,0);
                    return false;
                }
            });
            resolving=true; state=directories.isEmpty()?State.CONNECTING:State.FINDING;
            ConnectionPreparation<PreparedTransport> pending=new ConnectionPreparation<>(connectionWorker,() -> {
                // HttpClient/JCA initialization, directory access and key generation must never
                // hold up the render thread, including the first ever connection in this JVM.
                NetworkDirectory.Route route;
                if(directories.isEmpty()) route=new NetworkDirectory.Route(fallback,List.of());
                else {
                    try {
                        CompletableFuture<NetworkDirectory.Route> lookup=NetworkDirectory.lookup(directories,current.room(),trace);
                        try {
                            if(epoch!=generation)throw new CancellationException();
                            route=lookup.get(25,TimeUnit.SECONDS);
                        } finally { if(!lookup.isDone())lookup.cancel(true); }
                    } catch(Exception error) { throw new DirectoryFailure(error); }
                }
                if(epoch!=generation || Thread.currentThread().isInterrupted())throw new CancellationException();
                return prepareTransport(mc,epoch,traceAttempt,serverId,id,route,direct,udp,previous,authenticator);
            },this::retire);
            preparation=pending;
            pending.completion().whenComplete((unused,error) -> mc.execute(() -> {
                if(epoch!=generation || preparation!=pending) { pending.cancel(); return; }
                preparation=null; resolving=false;
                if(error!=null) {
                    trace.recordForAttempt(traceAttempt,"SESSION","FAILED",elapsed(started),error,0);
                    retry(epoch,error instanceof DirectoryFailure?"directory":"setup"); return;
                }
                try {
                    if(nativeVoice || !config.enabled || scope==null || !scope.retains(captureScope(mc))) {
                        pending.cancel(); shutdown(false); waitTicks=20; state=State.MOVING; return;
                    }
                    PreparedTransport prepared=pending.claim();
                    if(prepared==null)throw new CancellationException();
                    VoiceClient client=prepared.client(); transport=client; state=State.CONNECTING;
                    trace.recordForAttempt(traceAttempt,"SESSION","SUCCESS",elapsed(started),null,0);
                    connectionWorker.execute(() -> {
                        if(epoch!=generation || transport!=client)return;
                        try { client.connect(); }
                        catch(Exception failure) { mc.execute(() -> {
                            if(epoch!=generation)return;
                            trace.recordForAttempt(traceAttempt,"SESSION","FAILED",elapsed(started),failure,0);
                            retry(epoch,"setup");
                        }); }
                    });
                } catch(Exception failure) {
                    pending.cancel(); trace.recordForAttempt(traceAttempt,"SESSION","FAILED",elapsed(started),failure,0);
                    retry(epoch,"setup");
                }
            }));
        } catch(Exception error) {
            trace.recordForAttempt(traceAttempt,"SESSION","FAILED",elapsed(started),error,0);
            LOG.warn("Everywhere connect failed: {}",error.getClass().getSimpleName()); retry(epoch,"setup");
        }
    }
    private PreparedTransport prepareTransport(Minecraft mc,long epoch,int traceAttempt,UUID serverId,VoiceClient.Identity id,
                                               NetworkDirectory.Route route,boolean direct,boolean udp,
                                               VoiceClient.Resumption previous,VoiceClient.Authenticator authenticator) throws Exception {
        URI uri=route.control();
        int nodeIndex=0;
        for(int n=0;n<route.probes().size();n++)if(route.probes().get(n).control().equals(uri))nodeIndex=n+1;
        trace.recordForAttempt(traceAttempt,"DIRECTORY","SUCCESS",0,null,nodeIndex);
        PlasmoBridge.Prepared preparedBridge=PlasmoBridge.prepare(uri);
        if(epoch!=generation || Thread.currentThread().isInterrupted())throw new CancellationException();
        VoiceClient client=new VoiceClient(uri,null,id,direct,udp,authenticator,new VoiceClient.Listener() {
            public void ready() { mc.execute(() -> {
                if(epoch!=generation || nativeVoice || transport==null)return;
                long audioStarted=System.nanoTime();
                trace.recordForAttempt(traceAttempt,"AUDIO","START",0,null,0);
                try {
                    if (scope==null || !scope.retains(captureScope(mc))) { shutdown(false); waitTicks=20; state=State.MOVING; return; }
                    bridge=new PlasmoBridge(EverywhereMod.this,serverId,preparedBridge); bridge.initialize();
                    List<URI> directories=config.directoryUris();
                    if (directoryWarmup==null && !directories.isEmpty()) directoryWarmup=NetworkDirectory.keepWarm(directories,id.room());
                    attempt=0; nextDiagnosticsTick=ticks+200; state=State.CONNECTED;
                    trace.recordForAttempt(traceAttempt,"AUDIO","READY",elapsed(audioStarted),null,0);
                    LOG.info("External Plasmo Voice session initialized");
                } catch(Exception error) {
                    trace.recordForAttempt(traceAttempt,"AUDIO","FAILED",elapsed(audioStarted),error,0);
                    LOG.error("Cannot initialize Plasmo Voice adapter: {}",error.getClass().getSimpleName()); retry(epoch,"audio");
                }
            }); }
            public void peers(Collection<VoiceClient.PeerInfo> list) { mc.execute(() -> {
                if(epoch!=generation)return; peers=List.copyOf(list); audioSessions.update(peers);
                received.removeIf(audio -> !audioSessions.permits(audio.peer));
                if(bridge!=null && worldAudioAllowed)bridge.updatePlayers(peers);
                int direct=(int)list.stream().filter(VoiceClient.PeerInfo::direct).count();
                if(direct!=directConnections) { directConnections=direct; LOG.info("Direct voice paths: {} of {}",direct,list.size()); }
            }); }
            public void audio(VoiceClient.PeerInfo peer,long seq,int distance,byte[] opus,boolean end) {
                long worldEpoch=audioEpoch;
                if(epoch!=generation || !worldAudioAllowed)return;
                Received r=new Received(epoch,worldEpoch,peer,seq,distance,opus,end);
                if(!received.offer(r)) { received.poll(); received.offer(r); }
            }
            public void closed(String reason) { mc.execute(() -> {
                if(epoch!=generation)return;
                if(verificationPolicy.rejected(reason)) {
                    trace.recordForAttempt(traceAttempt,"ACCOUNT","FALLBACK",0,null,1008);
                    LOG.info("Mojang verification was rejected; retrying without verified-account priority");
                    retry(epoch,"verification"); resumption=null;
                } else retry(epoch,retryCode(reason));
            }); }
        },route.probes(),previous,trace);
        return new PreparedTransport(client);
    }
    private static long elapsed(long started) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started); }
    private void retire(AutoCloseable resource) {
        Runnable close=() -> {
            try { resource.close(); }
            catch(Exception error) { trace.record("SESSION_CLOSED","FAILED",0,error,0); }
        };
        // A cancelled startup may finish after CLIENT_STOPPING shuts the executor down.
        if(Thread.currentThread()==connectionThread)close.run();
        else connectionWorker.execute(close);
    }
    private static String retryCode(String reason) {
        if (reason == null) return "connection";
        if (reason.contains("already has a session") || reason.contains("reclaimed this player")) return "identity";
        if (reason.contains("capacity") || reason.contains("reserved") || reason.contains("Connection limit")) return "capacity";
        if (reason.contains("verification")) return "verification";
        return "connection";
    }
    private void retry(long epoch,String reason) {
        if(epoch!=generation)return;
        shutdown(true); attempt=Math.min(attempt+1,6); waitTicks=RetryDelay.ticks(attempt);
        retryCause=reason; state=State.RETRYING; trace.record("RETRY","START",waitTicks*50L,null,attempt);
        LOG.info("Voice reconnect scheduled: cause={}, attempt={}, delayTicks={}",retryCause,attempt,waitTicks);
    }
    private void shutdown() { shutdown(false); }
    private void shutdown(boolean keepResumption) {
        generation++; audioEpoch++; worldAudioAllowed=false; resolving=false; scope=null; audioSessions.clear();
        // Clear ownership before cancellation: its completion may run callbacks inline.
        ConnectionPreparation<PreparedTransport> pending=preparation; preparation=null;
        if (pending!=null) pending.cancel();
        VoiceClient old=transport;
        if(old!=null)try { lastConnection=DiagnosticReport.Snapshot.capture(old,trace.attempts()); }
        catch(RuntimeException error) { trace.record("REPORT","FAILED",0,error,0); }
        transport=null;
        NetworkDirectory.Warmup warmup=directoryWarmup; directoryWarmup=null;
        if (warmup!=null) retire(warmup);
        if (!keepResumption) resumption=null;
        else if (old!=null && old.resumption()!=null) resumption=old.resumption();
        try { if(bridge!=null)bridge.close(); }
        catch (RuntimeException error) { LOG.warn("Could not finish voice audio cleanup",error); }
        finally {
            bridge=null;
            try { if(old!=null)retire(old); }
            finally { distances=Map.of(); peers=List.of(); directConnections=0; received.clear(); }
        }
    }
    void sendAudio(long sequence,int distance,byte[] opus,boolean end) {
        VoiceClient client=transport; if(client==null || !worldAudioAllowed)return;
        Set<UUID> recipients=distances.entrySet().stream().filter(e -> e.getValue()<=distance).map(Map.Entry::getKey).collect(Collectors.toSet());
        client.sendAudio(recipients,sequence,distance,opus,end);
    }
    boolean transportReady() { return transport!=null && transport.isReady(); }
    boolean enabled() { return config.enabled; }
    void setEnabled(boolean enabled) {
        config.enabled=enabled; config.save();
        if (!enabled) { shutdown(); state=State.DISABLED; }
        else { attempt=0; waitTicks=0; state=nativeVoice?State.NATIVE:State.WAITING; }
    }
    void reconnect() {
        shutdown(true); attempt=0; waitTicks=0;
        state=!config.enabled?State.DISABLED:nativeVoice?State.NATIVE:State.CONNECTING;
    }
    Component statusComponent() {
        if (!config.enabled) return tr("status.disabled");
        if (state==State.RETRYING) return tr("status.retrying",tr("error."+retryCause),Math.max(1,(waitTicks+19)/20));
        return tr("status."+state.name().toLowerCase(Locale.ROOT));
    }
    Component routeComponent() {
        VoiceClient client=transport;
        if (client==null || !client.isReady())return tr("paths.none");
        var stats=client.networkStats();
        return tr("paths.count",stats.udpDirect()+stats.tcpDirect(),Math.max(0,client.peers().size()-stats.udpDirect()-stats.tcpDirect()));
    }
    Component accountComponent() {
        VoiceClient client=transport;
        if (nativeVoice || !config.enabled) return Component.empty();
        if (client==null || !client.isReady()) return tr("account.pending");
        return tr(client.diagnostics().verified()?"account.verified":"account.unverified");
    }
    private Component summary() { return Component.empty().append(statusComponent()).append(" · ").append(routeComponent()); }
    private void diagnostics(boolean show) {
        VoiceClient client=transport;
        if(show) {
            var history=trace.snapshot();
            diagnosticLine("Connection trace: attempts="+trace.attempts()+", retained="+history.size()+", dropped="+trace.dropped(),true);
            history.stream().skip(Math.max(0,history.size()-16)).forEach(event -> diagnosticLine(
                    "+"+event.elapsedMillis()+"ms attempt="+event.attempt()+" "+event.stage()+" "+event.result()
                            +" duration="+event.durationMillis()+"ms error="+event.error()+" code="+event.code(),true));
        }
        if(client==null || !client.isReady()) { if(show)message(summary()); return; }
        var network=client.connectionDiagnostics(); var packets=client.networkStats();
        diagnosticLine("Network: publicIPv4="+network.publicIPv4()+", publicIPv6="+network.publicIPv6()
                +", STUN="+network.stunMappings()+", mappingVaries="+network.mappingVaries()+", TCP="+network.tcpListener()
                +", gateways="+network.gateways()+", mappedUDP="+network.udpMappings()+", mappedTCP="+network.tcpMappings()
                +", mappingFailures="+network.mappingFailures()+", control="+client.controlTransport(),show);
        diagnosticLine("Voice packets: directUDP="+packets.directUdpVoicePackets()+", directTCP="+packets.directTcpVoicePackets()
                +", VPS="+packets.relayedVoicePackets(),show);
        int n=0;
        for(var peer:client.peerDiagnostics())if(show || peer.route().startsWith("VPS")) {
            diagnosticLine("Path "+(++n)+": "+peer.route()+", UDP candidates="+peer.candidates()+", confirmed="+peer.confirmed()
                    +", checks="+peer.checks()+", replies="+peer.replies()+", expired="+peer.expiredChecks()
                    +", RTT="+(peer.udpRttMillis()<0?"n/a":peer.udpRttMillis()+"ms")
                    +", TCP candidates="+peer.tcpCandidates()+", attempts="+peer.tcpAttempts()+", failures="+peer.tcpFailures()
                    +", TCP="+peer.tcpState(),show);
        }
    }
    Component saveReport() {
        try {
            DiagnosticReport.write(Minecraft.getInstance().gameDirectory.toPath().resolve("logs/plasmo-everywhere"),
                    state.name(),retryCause,attempt,sessionRestarts,retainedWorldChanges,transport,
                    lastConnection,trace,verificationPolicy.shouldVerify(),config.direct,config.udp,diagnosticLog.dropped(),diagnosticLog.writeFailed());
            return tr("report.saved");
        } catch (Exception error) { LOG.warn("Cannot save local diagnostic report: {}",error.getClass().getSimpleName()); return tr("report.failed"); }
    }
    private void addPauseButton(Minecraft mc,ScreenEvent.Init.Post event) {
        Screen screen=event.getScreen(); int width=screen.width,height=screen.height;
        var widgets=screen.children().stream().filter(AbstractWidget.class::isInstance).map(AbstractWidget.class::cast).toList();
        int x=8,y=8;
        outer: for(int row=height-28;row>=8;row-=24)for(int column:new int[]{8,Math.max(8,width-118)}) {
            boolean overlap=false;
            for(var widget:widgets)if(widget.visible && column<(widget.getX()+widget.getWidth())+3 && column+110>widget.getX()-3
                    && row<(widget.getY()+widget.getHeight())+3 && row+20>widget.getY()-3) { overlap=true; break; }
            if(!overlap) { x=column; y=row; break outer; }
        }
        event.addListener(Button.builder(Component.literal("Everywhere"),button -> openSettings(screen)).bounds(x,y,110,20)
                .tooltip(Tooltip.create(tr("screen.open"))).build());
    }
    private void openSettings(Screen parent) { Minecraft.getInstance().setScreen(new EverywhereScreen(parent,this)); }
    private static void diagnosticLine(String text,boolean show) { LOG.info("{}",text); if(show)message(Component.literal(text)); }
    static Component tr(String key,Object... args) { return Component.translatable("plasmo_everywhere."+key,args); }
    private static void message(Component text) {
        var mc=Minecraft.getInstance(); if(mc.player!=null)mc.gui.getChat().addMessage(Component.literal("[Everywhere] ").append(text));
    }
}
