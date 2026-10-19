package dev.eyecrasher.everywhere.core;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.*;
import javax.crypto.spec.*;

/** Pair-specific X25519 + HKDF-SHA256 + AES-GCM. Signaling authenticates the public keys. */
public final class PeerCrypto {
    private final UUID self, peer;
    private final byte[] sendKey, receiveKey;
    private final AtomicLong counter = new AtomicLong();
    private final ReplayWindow replay = new ReplayWindow();
    public static KeyPair keyPair() {
        try { return KeyPairGenerator.getInstance("X25519").generateKeyPair(); }
        catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }
    public PeerCrypto(KeyPair local, UUID self, UUID peer, byte[] peerKey) {
        this.self = self; this.peer = peer;
        try {
            PublicKey remote = KeyFactory.getInstance("X25519").generatePublic(new X509EncodedKeySpec(peerKey));
            KeyAgreement a = KeyAgreement.getInstance("X25519"); a.init(local.getPrivate()); a.doPhase(remote, true);
            byte[] shared = a.generateSecret();
            sendKey = derive(shared, self, peer); receiveKey = derive(shared, peer, self);
            Arrays.fill(shared, (byte) 0);
        } catch (GeneralSecurityException e) { throw new IllegalArgumentException("Invalid peer key", e); }
    }
    public Wire.Frame seal(byte kind, byte[] plaintext) {
        long seq = counter.getAndIncrement();
        if (seq < 0 || plaintext.length > Wire.MAX_BODY - 16) throw new IllegalArgumentException("Audio size/sequence");
        Wire.Frame header = new Wire.Frame(kind, self, peer, seq, new byte[0]);
        try { return new Wire.Frame(kind, self, peer, seq, crypt(Cipher.ENCRYPT_MODE, sendKey, header, plaintext)); }
        catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }
    public synchronized byte[] open(Wire.Frame frame) {
        if (!frame.from().equals(peer) || !frame.to().equals(self)) return null;
        try {
            byte[] plain = crypt(Cipher.DECRYPT_MODE, receiveKey, frame, frame.body());
            return replay.accept(frame.sequence()) ? plain : null;
        } catch (GeneralSecurityException e) { return null; }
    }
    private static byte[] crypt(int mode, byte[] key, Wire.Frame h, byte[] input) throws GeneralSecurityException {
        // The key is directional and session-specific; counters never reset under this key.
        byte[] iv = ByteBuffer.allocate(12).putInt(0x50455631).putLong(h.sequence()).array();
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
        c.updateAAD(h.aad()); return c.doFinal(input);
    }
    private static byte[] derive(byte[] shared, UUID from, UUID to) throws GeneralSecurityException {
        Mac m = Mac.getInstance("HmacSHA256");
        m.init(new SecretKeySpec("PlasmoEverywhere/v1".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] prk = m.doFinal(shared);
        m.init(new SecretKeySpec(prk, "HmacSHA256"));
        m.update(("voice/" + from + "/" + to).getBytes(StandardCharsets.UTF_8));
        m.update((byte) 1); return m.doFinal();
    }
}
