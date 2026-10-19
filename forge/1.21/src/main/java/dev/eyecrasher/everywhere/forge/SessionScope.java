package dev.eyecrasher.everywhere.forge;

import java.util.Objects;
import java.util.UUID;

/** A world instance may change inside one connection; these boundaries must not. */
record SessionScope(Object connection, String room, String dimension, UUID player) {
    SessionScope {
        Objects.requireNonNull(connection);
        Objects.requireNonNull(room);
        Objects.requireNonNull(dimension);
        Objects.requireNonNull(player);
    }
    boolean retains(SessionScope next) {
        return next != null && connection == next.connection && room.equals(next.room)
                && dimension.equals(next.dimension) && player.equals(next.player);
    }
}
