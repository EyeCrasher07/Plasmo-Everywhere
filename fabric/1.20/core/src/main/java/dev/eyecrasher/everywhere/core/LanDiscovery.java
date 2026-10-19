package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.concurrent.atomic.LongAdder;

/** Link-local multicast carries only pair-encrypted beacons for already-authorized peers. */
final class LanDiscovery implements AutoCloseable {
    static final int PORT=24457, MARKER=0x50454c31;
    private final DatagramChannel channel;
    private final DatagramSocket socket;
    private final InetSocketAddress group=new InetSocketAddress("239.192.83.92",PORT);
    private final BiConsumer<byte[],InetAddress> receive;
    private final UUID self;
    private final RateLimit incoming=new RateLimit(64,64),outgoing=new RateLimit(64,64);
    private volatile List<HostNetworks.Link> links=List.of();
    private Map<NetworkInterface,MembershipKey> joined=Map.of();
    private final LongAdder sent=new LongAdder(),received=new LongAdder(),outside=new LongAdder();
    private volatile String failure="none";
    LanDiscovery(UUID self,List<HostNetworks.Link> links,BiConsumer<byte[],InetAddress> receive) throws IOException {
        this.receive=receive; this.self=self;
        channel=DatagramChannel.open(StandardProtocolFamily.INET); socket=channel.socket(); socket.setReuseAddress(true);
        try {
            socket.bind(new InetSocketAddress(PORT)); socket.setSoTimeout(500);
            socket.setBroadcast(true);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_TTL,1);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP,true);
            update(links);
            RuntimeCompat.start("everywhere-lan", this::read);
        } catch(IOException|RuntimeException e) { socket.close(); throw e; }
    }
    synchronized void update(List<HostNetworks.Link> next) {
        Set<NetworkInterface> networks=new LinkedHashSet<>();
        for(var link:next)try { if(link.network().supportsMulticast())networks.add(link.network()); }
        catch(SocketException ignored) {}
        joined.forEach((network,key) -> { if(!networks.contains(network))key.drop(); });
        Map<NetworkInterface,MembershipKey> accepted=new LinkedHashMap<>();
        for(var network:networks)try {
            MembershipKey key=joined.get(network);
            accepted.put(network,key==null?channel.join(group.getAddress(),network):key);
        } catch(IOException ignored) {}
        joined=Map.copyOf(accepted);
        links=List.copyOf(next);
    }
    synchronized void send(byte[] encrypted) {
        if(encrypted.length>Wire.MAX_FRAME || socket.isClosed())return;
        for(var network:joined.keySet())try {
            if(!outgoing.take())return;
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF,network);
            socket.send(new DatagramPacket(encrypted,encrypted.length,group));
            sent.increment();
        } catch(IOException ignored) {}
        // Some LANs suppress multicast while still delivering their subnet's broadcast.
        for(var link:links)try {
            InetAddress broadcast=HostNetworks.broadcast(link);
            if(broadcast==null)continue;
            if(!outgoing.take())return;
            socket.send(new DatagramPacket(encrypted,encrypted.length,broadcast,PORT)); sent.increment();
        } catch(IOException ignored) {}
    }
    static boolean localSource(InetAddress source,List<HostNetworks.Link> links) {
        return HostNetworks.privateAddress(source) && links.stream().anyMatch(link -> link.contains(source));
    }
    private void read() {
        while(!socket.isClosed())try {
            var packet=new DatagramPacket(new byte[Wire.MAX_FRAME+1],Wire.MAX_FRAME+1); socket.receive(packet);
            if(packet.getLength()<47 || packet.getLength()>Wire.MAX_FRAME)continue;
            var header=ByteBuffer.wrap(packet.getData());
            if(header.getInt(0)!=Wire.MAGIC || header.getLong(21)!=self.getMostSignificantBits()
                    || header.getLong(29)!=self.getLeastSignificantBits() || !incoming.take())continue;
            if(!localSource(packet.getAddress(),links)) { outside.increment(); continue; }
            received.increment();
            try { receive.accept(Arrays.copyOf(packet.getData(),packet.getLength()),packet.getAddress()); }
            catch(RuntimeException ignored) {}
        } catch(SocketTimeoutException idle) { /* No LAN participants is normal. */ }
        catch(IOException|RuntimeException error) {
            failure=error.getClass().getSimpleName(); socket.close(); return;
        }
    }
    @Override public void close() { socket.close(); }
    boolean isClosed() { return socket.isClosed(); }
    String diagnostics() { return "interfaces="+joined.size()+",sent="+sent.sum()+",received="+received.sum()+",outside="+outside.sum()+",failure="+failure; }
}
