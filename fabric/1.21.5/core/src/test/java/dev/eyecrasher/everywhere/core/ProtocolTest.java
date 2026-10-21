package dev.eyecrasher.everywhere.core;

import java.security.KeyPair;
import java.util.*;

public final class ProtocolTest {
    public static void main(String[] args) throws Exception {
        WorldPresenceTest.run();
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(), outsider = UUID.randomUUID();
        KeyPair ka = PeerCrypto.keyPair(), kb = PeerCrypto.keyPair();
        PeerCrypto alice = new PeerCrypto(ka, a, b, kb.getPublic().getEncoded());
        PeerCrypto bob = new PeerCrypto(kb, b, a, ka.getPublic().getEncoded());
        byte[] voice = {1,2,3,4};
        Wire.Frame frame = alice.seal(Wire.VOICE, voice);
        check(Arrays.equals(bob.open(Wire.decode(frame.encode())), voice), "Pair roundtrip");
        check(bob.open(frame) == null, "Replay rejected");
        frame = alice.seal(Wire.VOICE, voice);
        check(bob.open(new Wire.Frame(Wire.END, frame.from(), frame.to(), frame.sequence(), frame.body())) == null, "Type substitution rejected");
        check(bob.open(new Wire.Frame(frame.kind(), outsider, frame.to(), frame.sequence(), frame.body())) == null, "Sender substitution rejected");
        check(Arrays.equals(bob.open(frame), voice), "Unauthenticated data does not advance replay window");
        Wire.Frame reverse = bob.seal(Wire.VOICE, voice);
        check(Arrays.equals(alice.open(reverse), voice), "Directional keys");
        ReplayWindow window = new ReplayWindow();
        check(window.accept(100) && window.accept(98) && !window.accept(98) && !window.accept(1), "Out-of-order window");
        check(window.accept(1000) && !window.accept(999-64) && !window.accept(-1), "Window bounds");
        byte[] key = Wire.random(32), packet = Wire.datagram(a, 0, Wire.ROUTE, frame.encode(), key, false);
        check(Wire.openDatagram(packet, key, false).session().equals(a), "UDP authentication");
        rejects(() -> Wire.openDatagram(packet, key, true), "UDP direction reflection");
        packet[30] ^= 1;
        rejects(() -> Wire.openDatagram(packet, key, false), "Tampered UDP rejected");
        rejects(() -> Wire.decode(new byte[Wire.MAX_FRAME+1]), "Oversized packet");
        byte[] bad = frame.encode(); bad[46] ^= 1;
        rejects(() -> Wire.decode(bad), "Length mismatch");
        Random random = new Random(14);
        for (int i=0; i<10000; i++) {
            byte[] input = new byte[random.nextInt(1500)]; random.nextBytes(input);
            try { Wire.decode(input); } catch (IllegalArgumentException expected) {}
            try { Wire.openDatagram(input, key, true); } catch (IllegalArgumentException expected) {}
        }
        System.out.println("Protocol checks passed: encryption, identity binding, tampering, replay, bounds, 10000 malformed inputs");
        TraversalTest.verify();
        DirectPathTest.verify();
        SessionProofTest.verify();
        AutomaticNetworkTest.verify();
        TcpCapabilityTest.verify();
        DirectoryFallbackTest.verify();
        ConnectionTraceTest.verify();
        RuntimeCompatTest.verify();
        TlsProxyCloseTest.verify();
        NodeSelectionTest.verify();
    }
    public static void check(boolean value, String name) { if (!value) throw new AssertionError(name); }
    private static void rejects(Runnable action, String name) {
        try { action.run(); } catch (IllegalArgumentException expected) { return; }
        throw new AssertionError(name);
    }
}
