package dev.eyecrasher.everywhere.relay;

import java.net.*;

/** Verify the actual shared UDP socket, including platforms that cap SO_RCVBUF. */
public final class UdpSocketBufferTest {
    public static void main(String[] args) throws Exception {
        try(DatagramSocket reference=new DatagramSocket(null);
            RelayServer server=new RelayServer("127.0.0.1",0,0,null)) {
            // Compare effective OS settings instead of assuming the requested hint is granted.
            reference.setReceiveBufferSize(4*1024*1024);
            reference.bind(new InetSocketAddress("127.0.0.1",0));
            var field=RelayServer.class.getDeclaredField("udp"); field.setAccessible(true);
            DatagramSocket socket=(DatagramSocket)field.get(server);
            int effective=socket.getReceiveBufferSize();
            if(effective!=reference.getReceiveBufferSize())
                throw new AssertionError("Relay UDP receive buffer did not request the bounded 4MiB profile");
            if(!socket.isBound() || !socket.getLocalAddress().isLoopbackAddress())
                throw new AssertionError("UDP buffer setup changed the bind address");
            int port=socket.getLocalPort();
            server.close();
            if(!socket.isClosed())throw new AssertionError("Configured UDP socket was not closed");
            try(DatagramSocket rebound=new DatagramSocket(new InetSocketAddress("127.0.0.1",port))) {
                if(rebound.getLocalPort()!=port)throw new AssertionError("UDP endpoint was not released");
            }
            System.out.println("Relay UDP receive buffer: requested=4194304, effective="+effective+"; close releases endpoint");
        }
        try(DatagramSocket occupied=new DatagramSocket(new InetSocketAddress("127.0.0.1",0))) {
            try(RelayServer unexpected=new RelayServer("127.0.0.1",0,occupied.getLocalPort(),null)) {
                throw new AssertionError("Occupied UDP endpoint did not fail construction");
            } catch(BindException expected) {}
            if(occupied.isClosed())throw new AssertionError("Failed bind closed another socket");
        }
    }
}
