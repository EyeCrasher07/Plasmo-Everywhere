package dev.eyecrasher.everywhere.core;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Reconnect proof binds an existing session's secret to this handshake and new key. */
public final class SessionProof {
    private SessionProof() {}
    public static byte[] sign(byte[] secret, UUID previous, UUID player, String room, String challenge, String publicKey) {
        if (secret.length != 32 || !room.matches("[a-f0-9]{64}") || !challenge.matches("[a-f0-9]{40}")
                || !publicKey.matches("[A-Za-z0-9+/=]{1,128}")) throw new IllegalArgumentException("Reconnect proof fields");
        String input = String.join("\n", "PlasmoEverywhere/reconnect/v1", previous.toString(), player.toString(), room, challenge, publicKey);
        try {
            Mac mac = Mac.getInstance("HmacSHA256"); mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(input.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) { throw new IllegalStateException(e); }
    }
    public static boolean verify(byte[] proof, byte[] secret, UUID previous, UUID player, String room, String challenge, String publicKey) {
        return proof.length == 32 && MessageDigest.isEqual(proof, sign(secret, previous, player, room, challenge, publicKey));
    }
}
