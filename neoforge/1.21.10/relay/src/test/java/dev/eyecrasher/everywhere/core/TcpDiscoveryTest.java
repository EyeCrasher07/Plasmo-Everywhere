package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.util.*;

/** TLS and same-local-port TCP observations against real nodes, without voice accounts or router changes. */
public final class TcpDiscoveryTest {
    public static void main(String[] args) throws Exception {
        try(var tcp=new TcpTransport(UUID.randomUUID(),0,new TcpTransport.Handler() {
            public PeerCrypto crypto(UUID peer) { return null; }
            public boolean permits(UUID peer,InetAddress source) { return false; }
            public void received(Wire.Frame frame,byte[] plain) { throw new AssertionError("No media session"); }
        })) {
            Set<InetSocketAddress> observations=new HashSet<>();
            for(String address:args) {
                URI uri=URI.create(address);
                InetSocketAddress endpoint;
                try { endpoint=TcpDiscovery.lookup(tcp,uri,null); }
                catch(TcpTransport.SharedPortUnavailableException unavailable) {
                    System.out.println("Same-port TCP discovery unavailable on this OS/JVM; no external mapping observed. Incoming TCP and ordinary outbound TCP remain available.");
                    return;
                }
                if(!Endpoints.isPublic(endpoint.getAddress()))throw new AssertionError("Public TCP mapping");
                observations.add(endpoint);
                System.out.println("Public HTTPS TCP discovery passed for "+uri.getHost()+"; shared local listener port, certificate verification and bounded response");
            }
            System.out.println("TCP observations: nodes="+args.length+", mapping="+(observations.size()<=1?"stable":"varies across destinations"));
        }
    }
}
