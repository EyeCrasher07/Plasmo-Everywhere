package dev.eyecrasher.everywhere.core;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static dev.eyecrasher.everywhere.core.ProtocolTest.check;

/** Reproduces a listener-port bind refusal without relying on the host operating system. */
final class TcpCapabilityTest {
    private static final TcpTransport.Handler NO_PEERS=new TcpTransport.Handler() {
        public PeerCrypto crypto(UUID peer) { return null; }
        public boolean permits(UUID peer,InetAddress source) { return false; }
        public void received(Wire.Frame frame,byte[] plain) { throw new AssertionError("No media peer"); }
    };
    static void verify() throws Exception {
        refusedBinding(); retryableIo(); wrongPort();
        System.out.println("TCP capability: one local bind refusal, no repeated discovery or false candidate, ephemeral outbound and listener retained; unrelated I/O remains retryable");
    }
    private static void refusedBinding() throws Exception {
        AtomicInteger binds=new AtomicInteger();
        ConnectionTrace trace=new ConnectionTrace(); trace.startAttempt();
        var identity=new VoiceClient.Identity(UUID.randomUUID(),"TCPTest",null,null,"a".repeat(64),"minecraft:overworld");
        try(var tcp=new TcpTransport(UUID.randomUUID(),0,NO_PEERS,(channel,port) -> {
            binds.incrementAndGet(); throw new BindException("private OS error must not enter report");
        }); var client=new VoiceClient(URI.create("ws://127.0.0.1:1/voice"),null,identity,true,true,
                challenge -> CompletableFuture.completedFuture(false),new VoiceClient.Listener() {},
                List.of(new NetworkDirectory.Probe(URI.create("wss://unused.invalid/voice"),24454)),null,trace);
            var listener=new ServerSocket(0,2,InetAddress.getLoopbackAddress())) {
            var tcpField=VoiceClient.class.getDeclaredField("tcp"); tcpField.setAccessible(true); tcpField.set(client,tcp);
            var refresh=VoiceClient.class.getDeclaredMethod("refreshTcpDiscovery"); refresh.setAccessible(true);
            var last=VoiceClient.class.getDeclaredField("lastTcpDiscovery"); last.setAccessible(true);
            for(int n=0;n<5;n++) {
                last.setLong(client,System.nanoTime()-TimeUnit.SECONDS.toNanos(31)); refresh.invoke(client);
            }
            check(binds.get()==1 && !tcp.sharedPortAvailable(),"Only one bind attempted across discovery rounds and nodes");
            var events=trace.snapshot();
            check(events.stream().filter(e -> e.stage().equals("TCP_SHARED_PORT") && e.result().equals("UNAVAILABLE")
                    && e.error().equals("BIND")).count()==1,"Single precise capability diagnostic");
            check(events.stream().filter(e -> e.stage().equals("TCP_DISCOVERY") && e.result().equals("START")).count()==1,
                    "Unsupported local sharing is not retried for a second node or later refresh");
            var mappings=VoiceClient.class.getDeclaredField("tcpMappings"); mappings.setAccessible(true);
            check(((Map<?,?>)mappings.get(client)).isEmpty(),"No mapping advertised after shared bind refusal");
            check(events.stream().noneMatch(e -> e.toString().contains("private OS")),"Bind error text is not exported");
            listener.setSoTimeout(2000);
            for(int n=0;n<2;n++)try(Socket outgoing=tcp.openSocket(new InetSocketAddress("127.0.0.1",listener.getLocalPort()),false);
                                     Socket accepted=listener.accept()) {
                check(outgoing.getLocalPort()!=tcp.port(),"Ordinary outgoing TCP uses an ephemeral port");
                outgoing.getOutputStream().write(42); accepted.setSoTimeout(2000);
                check(accepted.getInputStream().read()==42,"Ordinary outgoing TCP still transfers data");
            }
            check(binds.get()==1 && !tcp.isClosed(),"Outbound fallback does not repeat refused binding or close listener");
            var udpField=VoiceClient.class.getDeclaredField("udp"); udpField.setAccessible(true);
            DatagramSocket udp=(DatagramSocket)udpField.get(client);
            check(udp!=null && !udp.isClosed(),"TCP sharing failure leaves the UDP socket available");
            try(Socket incoming=new Socket("127.0.0.1",tcp.port())) {
                check(incoming.isConnected(),"Incoming listener retained after capability failure");
            }
            try { TcpDiscovery.lookup(tcp,URI.create("ws://127.0.0.1:1/voice"),null); throw new AssertionError("Ephemeral discovery admitted"); }
            catch(TcpTransport.SharedPortUnavailableException expected) {}
            check(binds.get()==1,"Direct lookup also respects cached local capability");
        }
    }
    private static void retryableIo() throws Exception {
        AtomicInteger binds=new AtomicInteger();
        try(var tcp=new TcpTransport(UUID.randomUUID(),0,NO_PEERS,(channel,port) -> {
            binds.incrementAndGet(); throw new IOException("temporary resource failure");
        })) {
            for(int n=0;n<2;n++)try {
                tcp.openSocket(new InetSocketAddress("127.0.0.1",1),true);
                throw new AssertionError("I/O failure admitted");
            } catch(TcpTransport.SharedPortUnavailableException wrong) { throw new AssertionError("General I/O disabled shared port",wrong); }
            catch(IOException expected) {}
            check(binds.get()==2 && tcp.sharedPortAvailable(),"General I/O does not become a permanent bind capability failure");
        }
    }
    private static void wrongPort() throws Exception {
        try(var tcp=new TcpTransport(UUID.randomUUID(),0,NO_PEERS,(channel,port) -> channel.bind(new InetSocketAddress(0)))) {
            try { TcpDiscovery.lookup(tcp,URI.create("ws://127.0.0.1:1/voice"),null); throw new AssertionError("Wrong port admitted"); }
            catch(TcpTransport.SharedPortUnavailableException expected) {}
            check(!tcp.sharedPortAvailable(),"An ephemeral socket is never mistaken for a listener-port observation");
        }
    }
}
