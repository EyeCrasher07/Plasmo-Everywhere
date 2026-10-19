package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Bounded RFC 8489 Binding / XOR-MAPPED-ADDRESS codec, used on our existing UDP port. */
public final class Stun {
    private static final int COOKIE = 0x2112a442;
    private Stun() {}
    public static byte[] request(byte[] transaction) {
        if (transaction.length != 12) throw new IllegalArgumentException("STUN transaction");
        return ByteBuffer.allocate(20).putShort((short) 1).putShort((short) 0).putInt(COOKIE).put(transaction).array();
    }
    public static boolean isMessage(byte[] bytes) {
        return bytes.length >= 20 && (bytes[0] & 0xc0) == 0 && ByteBuffer.wrap(bytes).getInt(4) == COOKIE;
    }
    public static boolean isRequest(byte[] bytes) {
        return isMessage(bytes) && bytes.length == 20 && ByteBuffer.wrap(bytes).getInt(0) == 0x00010000;
    }
    public static byte[] response(byte[] request, InetSocketAddress source) {
        if (!isRequest(request)) throw new IllegalArgumentException("STUN request");
        byte[] address = source.getAddress().getAddress(), mask = Arrays.copyOfRange(request, 4, 20);
        int size = address.length;
        ByteBuffer b = ByteBuffer.allocate(28 + size).putShort((short) 0x0101).putShort((short) (8 + size))
                .putInt(COOKIE).put(request, 8, 12).putShort((short) 0x0020).putShort((short) (4 + size))
                .put((byte) 0).put((byte) (size == 4 ? 1 : 2)).putShort((short) (source.getPort() ^ (COOKIE >>> 16)));
        for (int i = 0; i < size; i++) b.put((byte) (address[i] ^ mask[i]));
        return b.array();
    }
    public static InetSocketAddress mapped(byte[] bytes, byte[] transaction) {
        if (!isMessage(bytes) || transaction.length != 12 || bytes.length > 128) throw new IllegalArgumentException("STUN size");
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getShort() != 0x0101 || Short.toUnsignedInt(b.getShort()) != bytes.length - 20
                || !Arrays.equals(transaction, Arrays.copyOfRange(bytes, 8, 20))) throw new IllegalArgumentException("STUN response");
        b.position(20);
        while (b.remaining() >= 4) {
            int type = Short.toUnsignedInt(b.getShort()), len = Short.toUnsignedInt(b.getShort());
            if (len > b.remaining()) throw new IllegalArgumentException("STUN attribute size");
            int next = b.position() + ((len + 3) & ~3);
            if (next > bytes.length) throw new IllegalArgumentException("STUN padding");
            if (type == 0x0020) {
                if (len != 8 && len != 20) throw new IllegalArgumentException("STUN address size");
                if (b.get() != 0) throw new IllegalArgumentException("STUN reserved");
                int family = b.get() & 255, port = Short.toUnsignedInt(b.getShort()) ^ (COOKIE >>> 16);
                int size = family == 1 ? 4 : family == 2 ? 16 : 0;
                if (size == 0 || len != size + 4 || port == 0) throw new IllegalArgumentException("STUN address family");
                byte[] address = new byte[size], mask = Arrays.copyOfRange(bytes, 4, 20);
                for (int i = 0; i < size; i++) address[i] = (byte) (b.get() ^ mask[i]);
                try { return new InetSocketAddress(InetAddress.getByAddress(address), port); }
                catch (UnknownHostException impossible) { throw new IllegalArgumentException(impossible); }
            }
            b.position(next);
        }
        throw new IllegalArgumentException("Missing STUN mapped address");
    }
}
