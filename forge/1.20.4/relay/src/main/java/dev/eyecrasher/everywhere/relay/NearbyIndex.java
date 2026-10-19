package dev.eyecrasher.everywhere.relay;

import java.util.*;

/** Room-local reverse declarations, including players who have not connected yet. */
final class NearbyIndex<T> {
    private final Map<UUID,Set<T>> observers = new HashMap<>();
    private int edges;

    void replace(T observer, Set<UUID> previous, Set<UUID> next) {
        for(UUID player:previous) {
            if(next.contains(player))continue;
            Set<T> values=observers.get(player);
            if(values!=null && values.remove(observer)) {
                edges--;
                if(values.isEmpty())observers.remove(player);
            }
        }
        for(UUID player:next) {
            if(previous.contains(player))continue;
            if(observers.computeIfAbsent(player,k -> new HashSet<>()).add(observer))edges++;
        }
    }

    boolean watches(UUID player,T observer) {
        Set<T> values=observers.get(player);
        return values!=null && values.contains(observer);
    }

    int edges() { return edges; }
}
