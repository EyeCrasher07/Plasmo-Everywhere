package dev.eyecrasher.everywhere.core;

import com.google.gson.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Versioned, bounded frames. An audio frame is opaque to the relay. */
public final class Wire {
    public static final int MAGIC = 0x50455631, UDP_MAGIC = 0x50455531;
    public static final int MAX_BODY = 1000, MAX_FRAME = 1047, MAX_UDP = 1200, MAX_TEXT = 32768;
    public static final byte VOICE = 1, END = 2, PING = 3, PONG = 4;
    public static final byte BIND = 1, CHALLENGE = 2, CONFIRM = 3, BOUND = 4, ROUTE = 5;
    public static final UUID ZERO = new UUID(0, 0);
    public static final Gson GSON = new Gson();
    public static final SecureRandom RANDOM = new SecureRandom();
    private Wire() {}

    public record Frame(byte kind, UUID from, UUID to, long sequence, byte[] body) {
        public byte[] aad() {
            ByteBuffer b = ByteBuffer.allocate(45);
            b.putInt(MAGIC).put(kind); uuid(b, from); uuid(b, to); b.putLong(sequence);
            return b.array();
        }
        public byte[] encode() {
            if (body.length > MAX_BODY || sequence < 0) throw new IllegalArgumentException("Frame size/sequence");
            return ByteBuffer.allocate(47 + body.length).put(aad()).putShort((short) body.length).put(body).array();
        }
    }
    public static Frame decode(byte[] bytes) {
        if (bytes.length < 47 || bytes.length > MAX_FRAME) throw new IllegalArgumentException("Frame size");
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getInt() != MAGIC) throw new IllegalArgumentException("Protocol");
        byte kind = b.get(); UUID from = uuid(b), to = uuid(b); long seq = b.getLong();
        int n = Short.toUnsignedInt(b.getShort());
        if (kind < VOICE || kind > PONG || seq < 0 || n != b.remaining() || n > MAX_BODY)
            throw new IllegalArgumentException("Frame header");
        byte[] body = new byte[n]; b.get(body);
        return new Frame(kind, from, to, seq, body);
    }
    public record Datagram(UUID session, long sequence, byte op, byte[] body) {}
    public static byte[] datagram(UUID session, long sequence, byte op, byte[] body, byte[] key, boolean server) {
        if (body.length > MAX_FRAME || sequence < 0) throw new IllegalArgumentException("Datagram size");
        ByteBuffer b = ByteBuffer.allocate(31 + body.length);
        b.putInt(UDP_MAGIC); uuid(b, session); b.putLong(sequence).put(op).putShort((short) body.length).put(body);
        byte[] bytes = b.array(), tag = mac(key, bytes, server);
        return ByteBuffer.allocate(bytes.length + 16).put(bytes).put(tag, 0, 16).array();
    }
    public static UUID datagramSession(byte[] bytes) {
        if (bytes.length < 47 || bytes.length > MAX_UDP || ByteBuffer.wrap(bytes).getInt() != UDP_MAGIC)
            throw new IllegalArgumentException("Datagram header");
        return uuid(ByteBuffer.wrap(bytes, 4, 16));
    }
    public static Datagram openDatagram(byte[] bytes, byte[] key, boolean server) {
        UUID session = datagramSession(bytes);
        byte[] signed = Arrays.copyOf(bytes, bytes.length - 16);
        if (!MessageDigest.isEqual(Arrays.copyOf(mac(key, signed, server), 16),
                Arrays.copyOfRange(bytes, bytes.length - 16, bytes.length)))
            throw new IllegalArgumentException("Datagram authentication");
        ByteBuffer b = ByteBuffer.wrap(signed); b.position(20);
        long seq = b.getLong(); byte op = b.get(); int n = Short.toUnsignedInt(b.getShort());
        if (seq < 0 || n != b.remaining()) throw new IllegalArgumentException("Datagram size");
        byte[] body = new byte[n]; b.get(body);
        return new Datagram(session, seq, op, body);
    }
    private static byte[] mac(byte[] key, byte[] bytes, boolean server) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256"); mac.init(new SecretKeySpec(key, "HmacSHA256"));
            mac.update((byte) (server ? 1 : 0)); return mac.doFinal(bytes);
        } catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }
    public static byte[] random(int n) { byte[] b = new byte[n]; RANDOM.nextBytes(b); return b; }
    public static String base64(byte[] b) { return Base64.getEncoder().encodeToString(b); }
    public static byte[] unbase64(String s, int max) {
        if (s.length() > max * 2) throw new IllegalArgumentException("Encoded size");
        byte[] bytes = Base64.getDecoder().decode(s);
        if (bytes.length > max) throw new IllegalArgumentException("Decoded size");
        return bytes;
    }
    public static JsonObject message(String type) { JsonObject j = new JsonObject(); j.addProperty("type", type); return j; }
    public static JsonObject parse(String s) {
        if (s.length() > MAX_TEXT) throw new IllegalArgumentException("Message size");
        return JsonParser.parseString(s).getAsJsonObject();
    }
    public static String string(JsonObject j, String key, int max) {
        String s = j.get(key).getAsString();
        if (s.length() > max) throw new IllegalArgumentException("Field size");
        return s;
    }
    public static UUID id(JsonObject j, String key) { return UUID.fromString(string(j, key, 36)); }
    public static UUID offlineUuid(String name) {
        return UUID.nameUUIDFromBytes(("OfflinePlayer:" + name).getBytes(StandardCharsets.UTF_8));
    }
    private static void uuid(ByteBuffer b, UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(), b.getLong()); }
}
