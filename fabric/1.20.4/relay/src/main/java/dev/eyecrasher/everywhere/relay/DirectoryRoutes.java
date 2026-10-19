package dev.eyecrasher.everywhere.relay;

import com.google.gson.JsonObject;

/** One routing authority; alternate front ends must preserve its room assignments. */
interface DirectoryRoutes extends AutoCloseable {
    JsonObject route(String room) throws Exception;
    @Override default void close() {}
}
