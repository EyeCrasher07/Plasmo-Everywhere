package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.WorldPresence;
import java.util.*;

/** Bounded per-player lookup; ambiguous observations never select an arbitrary voice session. */
final class PresenceIndex {
    private static final int MAX_CANDIDATES=8;
    record Entry(UUID session,WorldPresence.Snapshot snapshot,long receivedAt,boolean verified) {}
    private final Map<UUID,Entry> sessions=new HashMap<>();
    private final Map<UUID,Set<UUID>> players=new HashMap<>();

    void put(UUID session,WorldPresence.Snapshot snapshot,long now) { put(session,snapshot,now,false); }
    void put(UUID session,WorldPresence.Snapshot snapshot,long now,boolean verified) {
        Entry previous=sessions.get(session);
        if(previous!=null && !previous.snapshot.player().equals(snapshot.player()))throw new IllegalArgumentException("Presence owner changed");
        sessions.put(session,new Entry(session,snapshot,now,verified));
        players.computeIfAbsent(snapshot.player(),unused -> new HashSet<>()).add(session);
    }
    void remove(UUID session) {
        Entry previous=sessions.remove(session);if(previous==null)return;
        Set<UUID> values=players.get(previous.snapshot.player());values.remove(session);if(values.isEmpty())players.remove(previous.snapshot.player());
    }
    Set<UUID> observers(WorldPresence.Snapshot snapshot) {
        Set<UUID> result=new HashSet<>();
        for(UUID player:snapshot.nearby().keySet()) {
            Set<UUID> ids=players.getOrDefault(player,Set.of());if(ids.size()>MAX_CANDIDATES)continue;
            for(UUID id:ids)if(sessions.get(id).snapshot.nearby().containsKey(snapshot.player()))result.add(id);
        }
        return result;
    }
    UUID match(UUID session,UUID player,long now) {
        Entry self=sessions.get(session);if(!fresh(self,now))return null;
        UUID candidate=unique(self,player,now);if(candidate==null)return null;
        Entry other=sessions.get(candidate);
        return session.equals(unique(other,self.snapshot.player(),now))?candidate:null;
    }
    private UUID unique(Entry self,UUID player,long now) {
        Set<UUID> ids=players.getOrDefault(player,Set.of());if(ids.size()>MAX_CANDIDATES)return null;
        UUID result=null;boolean verified=false,ambiguous=false;
        for(UUID id:ids) {
            Entry other=sessions.get(id);
            if(fresh(other,now) && WorldPresence.matches(self.snapshot,other.snapshot)) {
                if(result==null || other.verified && !verified) { result=id;verified=other.verified;ambiguous=false; }
                else if(other.verified==verified)ambiguous=true;
            }
        }
        return ambiguous?null:result;
    }
    private static boolean fresh(Entry value,long now) {
        return value!=null && now>=value.receivedAt && now-value.receivedAt<=WorldPresence.MAX_AGE_MILLIS*1_000_000;
    }
    List<UUID> expired(long now) { return sessions.values().stream().filter(value -> !fresh(value,now)).map(Entry::session).toList(); }
}
