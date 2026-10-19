package dev.eyecrasher.everywhere.fabric;

import dev.eyecrasher.everywhere.core.Wire;
import java.util.UUID;

/** A launcher may authenticate against another provider. Only the relay can confirm Mojang ownership. */
final class VerificationPolicy {
    private boolean unverified;
    static boolean canVerifyIdentity(UUID player,String name,UUID account,String accountName) {
        return player!=null && name!=null && account!=null && accountName!=null && name.equalsIgnoreCase(accountName)
                && (player.equals(account) || player.equals(Wire.offlineUuid(name)));
    }
    synchronized void reset() { unverified=false; }
    synchronized boolean shouldVerify() { return !unverified; }
    synchronized boolean rejected(String reason) {
        if (unverified || !"Voice service disconnected (1008): Account verification failed".equals(reason)) return false;
        unverified=true;
        return true;
    }
}
