package dev.eyecrasher.everywhere.fabric;

import dev.eyecrasher.everywhere.core.VoiceClient;
import java.util.*;

/** Playback permission is the current session and player pair, never a name or player UUID alone. */
final class AudioSessionGate {
    private volatile Map<UUID,UUID> sessions=Map.of();
    void update(Collection<VoiceClient.PeerInfo> peers) {
        Map<UUID,UUID> next=new HashMap<>();
        for(var peer:peers)next.put(peer.session(),peer.player());
        sessions=Map.copyOf(next);
    }
    void clear() { sessions=Map.of(); }
    boolean permits(VoiceClient.PeerInfo peer) { return peer.player().equals(sessions.get(peer.session())); }
}
