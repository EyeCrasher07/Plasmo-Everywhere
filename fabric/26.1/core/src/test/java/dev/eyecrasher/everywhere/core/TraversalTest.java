package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static dev.eyecrasher.everywhere.core.ProtocolTest.check;

final class TraversalTest {
    static void verify() throws Exception {
        byte[] transaction = Wire.random(12), request = Stun.request(transaction);
        for (String ip : List.of("8.8.8.8", "2606:4700:4700::1111")) {
            var source = new InetSocketAddress(InetAddress.getByName(ip), 49152);
            byte[] reply = Stun.response(request, source);
            check(Stun.mapped(reply, transaction).equals(source), "STUN IPv4/IPv6 XOR address");
            rejects(() -> Stun.mapped(reply, Wire.random(12)), "STUN transaction substitution");
            reply[3] ^= 1;
            rejects(() -> Stun.mapped(reply, transaction), "STUN length mismatch");
        }
        for (String ip : List.of("localhost", "abc", "127.1", "0x08080808", "8.8.8.999", "127.0.0.1",
                "10.0.0.1", "192.168.1.1", "100.64.0.1", "169.254.0.1", "224.0.0.1", "::1", "fc00::1", "fe80::1", "2001:db8::1")) {
            var j = new com.google.gson.JsonObject(); j.addProperty("ip",ip); j.addProperty("port",12345);
            rejects(() -> Endpoints.read(j,false), "Non-public/non-literal candidate: "+ip);
        }
        var a = new InetSocketAddress("8.8.8.8", 10000);
        var rebound = new InetSocketAddress(a.getAddress(), 10001);
        var thirdParty = new InetSocketAddress("1.1.1.1",10001);
        long now = TimeUnit.SECONDS.toNanos(100);
        DirectPath path = new DirectPath(); path.candidates(List.of(a), now);
        var first = path.probe(now, () -> true);
        check(first != null && first.target().equals(a), "Initial connectivity check");
        path.pong(first.nonce(), thirdParty, now+1);
        check(path.selected(now+2)==null && path.count()==1, "Unannounced IP cannot redirect check");
        path.pong(first.nonce(), rebound, now+3);
        check(path.selected(now+4)==null && path.count()==2, "New port requires its own consent check");
        var reflexive = path.probe(now+5, () -> true);
        check(reflexive.target().equals(rebound), "Triggered check to authenticated new port");
        path.pong(reflexive.nonce(), rebound, now+6);
        check(path.selected(now+7).equals(rebound), "Peer-reflexive path selected");
        path.candidates(List.of(a),now+8);
        check(path.selected(now+9).equals(rebound), "Repeated signaling preserves confirmed path");
        path.candidates(List.of(a,new InetSocketAddress(a.getAddress(),10002),new InetSocketAddress(a.getAddress(),10003),
                new InetSocketAddress(a.getAddress(),10004),new InetSocketAddress(a.getAddress(),10005)),now+10);
        for (int i=0;i<100;i++) {
            long at=now+TimeUnit.SECONDS.toNanos(i+1);
            path.authenticatedPing(new InetSocketAddress(a.getAddress(),11000+i),at);
            check(path.count()<=6,"Candidate bound after authenticated port change");
            var keepalive=path.probe(at,() -> true);
            check(path.count()<=6,"Candidate bound before connectivity reply");
            if(keepalive!=null)path.pong(keepalive.nonce(),keepalive.target(),at+1);
            check(path.count()<=6,"Peer-reflexive candidate storage remains bounded");
        }
        now+=TimeUnit.SECONDS.toNanos(100);
        path.pong(reflexive.nonce(), rebound, now+TimeUnit.SECONDS.toNanos(4));
        check(path.selected(now+TimeUnit.SECONDS.toNanos(6))==null, "Expired path and replayed consent fall back");
        path.candidates(List.of(thirdParty),now+TimeUnit.SECONDS.toNanos(7));
        check(!path.permits(rebound) && path.count()==1, "Retired IP removed");
        DirectPath delayed = new DirectPath(); delayed.candidates(List.of(a),now);
        var older = delayed.probe(now,() -> true);
        delayed.probe(now+TimeUnit.SECONDS.toNanos(1),() -> true);
        delayed.pong(older.nonce(),a,now+TimeUnit.SECONDS.toNanos(2));
        check(delayed.selected(now+TimeUnit.SECONDS.toNanos(2)).equals(a), "Delayed pong survives a newer check");
        Random random = new Random(21);
        for (int i=0;i<10000;i++) {
            byte[] input = new byte[random.nextInt(1500)]; random.nextBytes(input);
            try { Stun.mapped(input,transaction); } catch (IllegalArgumentException expected) {}
        }
        System.out.println("Traversal: STUN v4/v6, literal/public candidates, consent, changed ports, delayed/replayed replies and malformed packets passed");
    }
    private static void rejects(Runnable action, String name) {
        try { action.run(); } catch (IllegalArgumentException expected) { return; }
        throw new AssertionError(name);
    }
}
