package dev.eyecrasher.everywhere.fabric;

import dev.eyecrasher.everywhere.core.VoiceClient;
import net.minecraft.client.Minecraft;
import su.plo.voice.api.client.connection.ServerConnection;
import su.plo.voice.api.client.event.connection.ServerInfoInitializedEvent;
import su.plo.voice.api.client.event.socket.UdpClientClosedEvent;
import su.plo.voice.api.client.socket.UdpClient;
import su.plo.voice.client.ModVoiceClient;
import su.plo.voice.client.connection.VoiceServerInfo;
import su.plo.voice.client.config.VoiceClientConfig;
import su.plo.voice.proto.data.audio.capture.*;
import su.plo.voice.proto.data.audio.codec.opus.*;
import su.plo.voice.proto.data.audio.line.VoiceSourceLine;
import su.plo.voice.proto.data.audio.source.PlayerSourceInfo;
import su.plo.voice.proto.data.encryption.EncryptionInfo;
import su.plo.voice.proto.data.player.VoicePlayerInfo;
import su.plo.voice.proto.packets.Packet;
import su.plo.voice.proto.packets.tcp.clientbound.*;
import su.plo.voice.proto.packets.tcp.serverbound.PlayerAudioEndPacket;
import su.plo.voice.proto.packets.udp.serverbound.PlayerAudioPacket;
import su.plo.voice.proto.packets.udp.clientbound.SourceAudioPacket;
import java.net.*;
import java.security.*;
import java.util.*;

/** Only the external-session adapter touches Plasmo Voice internals. */
final class PlasmoBridge implements ServerConnection, ClientPacketTcpHandler {
    private final EverywhereMod owner;
    private final ModVoiceClient pv;
    private final Map<UUID,VoicePlayerInfo> players = new java.util.concurrent.ConcurrentHashMap<>();
    private final UdpBridge udp = new UdpBridge();
    private final UUID secret = UUID.randomUUID();
    private final InetSocketAddress address;
    private KeyPair rsa;
    private volatile boolean closed;
    private final UUID serverId;
    record Prepared(InetSocketAddress address, KeyPair rsa) {}
    /** Pure preparation: RSA providers and entropy may be slow on the first connection. */
    static Prepared prepare(URI relay) throws GeneralSecurityException {
        // This is Plasmo's display address. The real connection belongs to VoiceClient.
        var address = InetSocketAddress.createUnresolved(relay.getHost(), relay.getPort() > 0 ? relay.getPort() : 443);
        return new Prepared(address, KeyPairGenerator.getInstance("RSA").generateKeyPair());
    }
    PlasmoBridge(EverywhereMod owner, UUID serverId, Prepared prepared) {
        this.owner = owner; this.serverId = serverId;
        pv = ModVoiceClient.INSTANCE; address = prepared.address(); rsa = prepared.rsa();
    }
    void initialize() throws Exception {
        var codec = new su.plo.voice.proto.data.audio.codec.CodecInfo("opus", Map.of("mode","VOIP","bitrate","32000"));
        var line = new VoiceSourceLine("proximity", "pv.activation.proximity", "plasmovoice:textures/icons/speaker.png", 1.0, 1, null);
        var activation = new VoiceActivation("proximity", "pv.activation.proximity", "plasmovoice:textures/icons/microphone.png",
                List.of(8,16,32), 16, true, false, true, null, 1);
        ConfigPacket packet = new ConfigPacket(serverId, new CaptureInfo(48000, 800, codec), null,
                Set.of(line), Set.of(activation), Map.of());
        var info = new VoiceServerInfo(pv, serverId, secret, address, null, packet);
        pv.setServerInfo(info);
        if (pv.getConfig().getServers().getById(serverId).isEmpty()) pv.getConfig().getServers().put(serverId, new VoiceClientConfig.Server());
        pv.getUdpClientManager().setClient(udp);
        pv.getSourceLineManager().register(line);
        pv.getActivationManager().register(info.getVoiceInfo().getActivations());
        pv.getAudioCapture().initialize(info);
        var output = pv.getDeviceManager().openOutputDevice(info.getVoiceInfo().getFormat(false),su.plo.voice.api.util.Params.EMPTY);
        pv.getDeviceManager().add(output); pv.getDeviceManager().startJob();
        pv.getAudioCapture().start();
        updatePlayers(List.of());
        pv.getEventBus().call(new ServerInfoInitializedEvent(info, packet));
    }
    void updatePlayers(Collection<VoiceClient.PeerInfo> peers) {
        players.clear();
        var self = Minecraft.getInstance().player;
        if (self != null) players.put(self.getUUID(), player(self.getUUID(), self.getGameProfile().getName()));
        peers.forEach(p -> players.put(p.player(), player(p.player(), p.name())));
    }
    private static VoicePlayerInfo player(UUID id, String name) { return new VoicePlayerInfo(id,name,false,false,false); }
    void audio(VoiceClient.PeerInfo peer, long sequence, int distance, byte[] opus, boolean end) {
        if (closed) return;
        if (end) {
            pv.getSourceManager().getSourceById(peer.session(), false).ifPresent(s -> s.process(new SourceAudioEndPacket(peer.session(),sequence)));
            return;
        }
        if (pv.getSourceManager().getSourceById(peer.session(), false).isEmpty()) {
            var source = new PlayerSourceInfo("plasmo_everywhere", peer.session(), VoiceSourceLine.PROXIMITY_ID,
                    null, (byte) 0, new su.plo.voice.proto.data.audio.codec.CodecInfo("opus", Map.of()), false, true, 0, player(peer.player(),peer.name()));
            pv.getSourceManager().update(source);
        }
        pv.getSourceManager().getSourceById(peer.session(), false)
                .ifPresent(s -> s.process(new SourceAudioPacket(sequence,(byte)0,opus,peer.session(),(short)distance)));
    }
    void retainSources(Set<UUID> sessions) {
        for (var s : List.copyOf(pv.getSourceManager().getSources()))
            if ("plasmo_everywhere".equals(s.getSourceInfo().getAddonId())
                    && !sessions.contains(s.getSourceInfo().getId())) s.closeAsync();
    }
    void resetWorld() {
        players.clear();
        // Preserve microphone/device settings while discarding old playback and decoder queues.
        if (!closed && pv.getServerInfo().map(i -> i.getSecret().equals(secret)).orElse(false))
            pv.getSourceManager().clear();
    }
    @Override public void sendPacket(Packet<?> packet) { sendPacket(packet,true); }
    @Override public void sendPacket(Packet<?> packet, boolean check) {
        if (closed) return;
        if (packet instanceof PlayerAudioPacket p && p.getActivationId().equals(VoiceActivation.PROXIMITY_ID) && !p.isStereo())
            owner.sendAudio(p.getSequenceNumber(),p.getDistance(),p.getData(),false);
        else if (packet instanceof PlayerAudioEndPacket p && p.getActivationId().equals(VoiceActivation.PROXIMITY_ID))
            owner.sendAudio(p.getSequenceNumber(),p.getDistance(),new byte[0],true);
    }
    @Override public void close() {
        if (closed) return; closed = true;
        // Only clear state owned by this bridge. Native handoff closes us before native setup.
        if (pv.getServerInfo().map(i -> i.getSecret().equals(secret)).orElse(false)) {
            pv.getAudioCapture().stop(); pv.getSourceManager().clear(); pv.getActivationManager().clear();
            pv.getSourceLineManager().clear(); pv.getDeviceManager().stopJob(); pv.getDeviceManager().clear(su.plo.voice.api.client.audio.device.DeviceType.INPUT); pv.getDeviceManager().clear(su.plo.voice.api.client.audio.device.DeviceType.OUTPUT); pv.setServerInfo(null);
        }
        if (pv.getUdpClientManager().getClient().orElse(null) == udp) pv.getUdpClientManager().removeClient(UdpClientClosedEvent.Reason.DISCONNECT);
        players.clear();
    }
    private final class UdpBridge implements UdpClient {
        public void close(UdpClientClosedEvent.Reason reason) { PlasmoBridge.this.close(); }
        public void connect(String host,int port) {}
        public void sendPacket(Packet<?> packet) { PlasmoBridge.this.sendPacket(packet); }
        public UUID getSecret() { return secret; }
        public Optional<InetSocketAddress> getRemoteAddress() { return Optional.of(address); }
        public boolean isClosed() { return closed; }
        public boolean isConnected() { return !closed && owner.transportReady(); }
        public boolean isTimedOut() { return !isConnected(); }
        public long getKeepAlive() { return System.currentTimeMillis(); }
    }
    @Override public SocketAddress getRemoteAddress() { return address; }
    @Override public String getRemoteIp() { return address.getHostString(); }
    @Override public Map<String,String> getLanguage() { return Map.of(); }
    @Override public Collection<VoicePlayerInfo> getPlayers() { return List.copyOf(players.values()); }
    @Override public Optional<VoicePlayerInfo> getPlayerById(UUID id) { return Optional.ofNullable(players.get(id)); }
    @Override public Optional<VoicePlayerInfo> getClientPlayer() {
        return Optional.ofNullable(Minecraft.getInstance().player).flatMap(p -> getPlayerById(p.getUUID()));
    }
    @Override public KeyPair getKeyPair() { return rsa; }
    @Override public void setKeyPair(KeyPair pair) { rsa = pair; }
    @Override public Optional<EncryptionInfo> getEncryptionInfo() { return Optional.empty(); }
    @Override public void handle(ConnectionPacket p) {}
    @Override public void handle(ConfigPlayerInfoPacket p) {}
    @Override public void handle(PlayerInfoRequestPacket p) {}
    @Override public void handle(LanguagePacket p) {}
    @Override public void handle(ConfigPacket p) {}
    @Override public void handle(PlayerListPacket p) {}
    @Override public void handle(PlayerInfoUpdatePacket p) {}
    @Override public void handle(PlayerDisconnectPacket p) {}
    @Override public void handle(SourceAudioEndPacket p) {}
    @Override public void handle(SourceInfoPacket p) {}
    @Override public void handle(SelfSourceInfoPacket p) {}
    @Override public void handle(SourceLineRegisterPacket p) {}
    @Override public void handle(SourceLineUnregisterPacket p) {}
    @Override public void handle(SourceLinePlayerAddPacket p) {}
    @Override public void handle(SourceLinePlayerRemovePacket p) {}
    @Override public void handle(SourceLinePlayersListPacket p) {}
    @Override public void handle(ActivationRegisterPacket p) {}
    @Override public void handle(ActivationUnregisterPacket p) {}
    @Override public void handle(DistanceVisualizePacket p) {}
    @Override public void handle(AnimatedActionBarPacket p) {}
}
