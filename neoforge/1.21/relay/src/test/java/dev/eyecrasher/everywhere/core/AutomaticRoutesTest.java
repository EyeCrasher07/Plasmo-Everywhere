package dev.eyecrasher.everywhere.core;

import dev.eyecrasher.everywhere.relay.RelayServer;
import java.io.*;
import java.net.*;
import java.nio.channels.DatagramChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Actual encrypted audio over direct TCP / LAN and controlled transport failures. */
public final class AutomaticRoutesTest {
    private static final class Sink implements VoiceClient.Listener {
        final BlockingQueue<byte[]> audio=new LinkedBlockingQueue<>();
        public void audio(VoiceClient.PeerInfo peer,long sequence,int distance,byte[] bytes,boolean end) { if(!end)audio.add(bytes); }
    }
    private static final class FilterSocket extends DatagramSocket {
        final InetSocketAddress relay;
        volatile boolean everything,blockLoopbackPeers=true;
        FilterSocket(InetSocketAddress relay,boolean everything) throws SocketException { super(); this.relay=relay; this.everything=everything; }
        public void send(DatagramPacket packet) throws IOException {
            if(everything || blockLoopbackPeers && packet.getAddress().isLoopbackAddress() && !packet.getSocketAddress().equals(relay))return;
            super.send(packet);
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length>0 && args[0].equals("backup")) { UdpBackupTest.verify(); return; }
        if(args.length>0 && args[0].equals("lan")) { lan(); return; }
        if(args.length>0 && args[0].equals("tcp")) { tcp(); return; }
        if(args.length>0 && args[0].equals("send-fallback")) { UdpSendFallbackTest.main(new String[0]); return; }
        tcp(); UdpBackupTest.verify(); UdpSendFallbackTest.main(new String[0]); warmup(); revokedWarmup(); lan();
        System.out.println("Automatic routes passed: direct TCP, warm UDP backup, authenticated first-voice race, LAN route/host-policy checks and relay fallback");
    }
    private static void tcp() throws Exception {
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start(); await(() -> relay.getPort()>0,"TCP relay start");
            var udpRelay=new InetSocketAddress("127.0.0.1",relay.udpPort()); URI uri=URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice");
            UUID a=Wire.offlineUuid("TcpActor"),b=Wire.offlineUuid("TcpFriend"); Sink sa=new Sink(),sb=new Sink();
            try(FilterSocket ua=new FilterSocket(udpRelay,true); FilterSocket ub=new FilterSocket(udpRelay,true);
                VoiceClient ca=client(uri,a,"TcpActor",sa,ua); VoiceClient cb=client(uri,b,"TcpFriend",sb,ub)) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS); ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.networkStats().tcpDirect()==1 && cb.networkStats().tcpDirect()==1,"TCP direct with all UDP blocked");
                check(!ca.hasUdp() && !cb.hasUdp(),"Test accidentally used UDP");
                exchange(ca,cb,sa,sb,a,b,"TCP encrypted voice");
                check(voice(relay)==0 && ca.networkStats().directTcpVoicePackets()>0 && cb.networkStats().directTcpVoicePackets()>0,"TCP voice reached relay");
                TcpTransport ta=tcp(ca),tb=tcp(cb); int portA=ta.port(),portB=tb.port();
                // An unauthenticated TCP client cannot send media or replace the established peer.
                try(Socket stranger=new Socket("127.0.0.1",portA)) {
                    stranger.getOutputStream().write(new byte[]{0,1,0}); stranger.getOutputStream().flush();
                    stranger.setSoTimeout(2500);
                    try { check(stranger.getInputStream().read()==-1,"Unauthenticated connection retained"); }
                    catch(SocketException reset) { /* Rejecting an unread malformed frame may send TCP RST. */ }
                }
                exchange(ca,cb,sa,sb,a,b,"Existing TCP survived bogus newcomer");
                ua.everything=false; ub.everything=false; ua.blockLoopbackPeers=false; ub.blockLoopbackPeers=false;
                await(() -> ca.networkStats().udpDirect()==1 && cb.networkStats().udpDirect()==1,"UDP recovered while TCP stays available");
                exchange(ca,cb,sa,sb,a,b,"Automatic TCP to direct UDP upgrade");
                check(voice(relay)==0 && ca.networkStats().directUdpVoicePackets()>0,"Recovered direct UDP unexpectedly relayed");
                ua.everything=true; ub.everything=true;
                await(() -> ca.networkStats().udpDirect()==0 && cb.networkStats().udpDirect()==0
                        && ca.networkStats().tcpDirect()==1 && cb.networkStats().tcpDirect()==1,"TCP used after direct UDP expires");
                exchange(ca,cb,sa,sb,a,b,"Automatic direct UDP to direct TCP fallback");
                check(voice(relay)==0,"A usable direct TCP route was bypassed for VPS");
                UUID originalA=ca.sessionId(),originalB=cb.sessionId(); ta.close(); tb.close();
                await(() -> ca.networkStats().tcpDirect()==0 && cb.networkStats().tcpDirect()==0,"TCP path loss");
                await(() -> !ca.hasUdp() && !cb.hasUdp(),"Blocked UDP reserve expires before WSS fallback");
                exchange(ca,cb,sa,sb,a,b,"WSS fallback after direct TCP loss");
                check(voice(relay)>=2,"Reserve was not used after all direct paths failed");
                check(ca.sessionId().equals(originalA) && cb.sessionId().equals(originalB),"Fallback unnecessarily rejoined session");
                closedPort(portA); closedPort(portB);
            }
        }
    }
    private static void warmup() throws Exception {
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start(); await(() -> relay.getPort()>0,"Warmup relay start");
            URI uri=URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice");
            UUID a=Wire.offlineUuid("WarmActor"),b=Wire.offlineUuid("WarmFriend"); Sink sa=new Sink(),sb=new Sink();
            try(DatagramSocket ua=new DatagramSocket(); DatagramSocket ub=new DatagramSocket();
                VoiceClient ca=client(uri,a,"WarmActor",sa,ua); VoiceClient cb=client(uri,b,"WarmFriend",sb,ub)) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS);
                await(() -> ca.hasUdp() && cb.hasUdp(),"Warmup UDP bind");
                ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.peers().size()==1 && cb.peers().size()==1,"Warmup peer visibility");
                byte[] first={11,22,33}; ca.sendAudio(Set.of(b),0,16,first,false);
                check(Arrays.equals(sb.audio.poll(2,TimeUnit.SECONDS),first),"First utterance was lost during direct race");
                check(voice(relay)==0 && ca.networkStats().directUdpVoicePackets()+ca.networkStats().directTcpVoicePackets()==1,"Initial voice unnecessarily relayed");
            }
        }
    }
    private static void revokedWarmup() throws Exception {
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start(); await(() -> relay.getPort()>0,"Revoked warmup relay start");
            URI uri=URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice"); var endpoint=new InetSocketAddress("127.0.0.1",relay.udpPort());
            UUID a=Wire.offlineUuid("RevokeActor"),b=Wire.offlineUuid("RevokeFriend"); Sink sa=new Sink(),sb=new Sink();
            try(FilterSocket ua=new FilterSocket(endpoint,true); FilterSocket ub=new FilterSocket(endpoint,true);
                VoiceClient ca=client(uri,a,"RevokeActor",sa,ua); VoiceClient cb=client(uri,b,"RevokeFriend",sb,ub)) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS);
                tcp(ca).close(); tcp(cb).close(); ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.peers().size()==1 && cb.peers().size()==1,"Revoked warmup discovery");
                ca.sendAudio(Set.of(b),0,16,new byte[]{4,4},false);
                cb.nearby(Set.of()); await(() -> ca.peers().isEmpty(),"Revoked warmup visibility");
                Thread.sleep(300); cb.nearby(Set.of(a));
                await(() -> ca.peers().size()==1 && cb.peers().size()==1,"Warmup rediscovery");
                check(sb.audio.poll(350,TimeUnit.MILLISECONDS)==null,"Queued utterance leaked after peer left and reentered proximity");
                exchange(ca,cb,sa,sb,a,b,"Fresh audio after warmup revocation");
            }
        }
    }
    private static void lan() throws Exception {
        List<HostNetworks.Link> links=HostNetworks.links().stream().filter(link -> {
            try { return link.network().supportsMulticast(); } catch(SocketException e) { return false; }
        }).toList();
        if(links.isEmpty()) { System.out.println("LAN live multicast unavailable on this host; local-scope/authentication unit checks still run"); return; }
        boolean live=localDiscoveryAvailable(links.getFirst());
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start(); await(() -> relay.getPort()>0,"LAN relay start");
            var udpRelay=new InetSocketAddress("127.0.0.1",relay.udpPort()); URI uri=URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice");
            UUID a=Wire.offlineUuid("LanActor"),b=Wire.offlineUuid("LanFriend"); Sink sa=new Sink(),sb=new Sink();
            try(FilterSocket ua=new FilterSocket(udpRelay,false); FilterSocket ub=new FilterSocket(udpRelay,false);
                VoiceClient ca=client(uri,a,"LanActor",sa,ua); VoiceClient cb=client(uri,b,"LanFriend",sb,ub)) {
                ca.connect().get(5,TimeUnit.SECONDS); cb.connect().get(5,TimeUnit.SECONDS); ca.nearby(Set.of(b)); cb.nearby(Set.of(a));
                await(() -> ca.peers().size()==1 && cb.peers().size()==1,"LAN peers");
                tcp(ca).close(); tcp(cb).close();
                LanDiscovery la=installLan(ca,links),lb=installLan(cb,links);
                if(!live) {
                    // Independent raw-socket preflight established this host blocks local group delivery.
                    // Feed exactly the authenticated datagrams a working LAN would deliver; media still
                    // traverses real UDP sockets to the directly attached private address.
                    exchange(ca,cb,sa,sb,a,b,"Reserve when host blocks LAN discovery");
                    beacon(ca,cb,links.getFirst().address()); beacon(cb,ca,links.getFirst().address());
                }
                try { await(() -> ca.networkStats().lanDirect()==1 && cb.networkStats().lanDirect()==1,"Authenticated LAN multicast / direct media"); }
                catch(AssertionError e) { throw new AssertionError(e.getMessage()+"; A="+la.diagnostics()+" B="+lb.diagnostics()+" paths="+ca.diagnostics()+" / "+cb.diagnostics(),e); }
                exchange(ca,cb,sa,sb,a,b,"LAN encrypted voice");
                check(live?voice(relay)==0:voice(relay)==2,"LAN media unexpectedly reached relay");
                System.out.println(live?"Live multicast/broadcast discovery passed":"Authenticated LAN beacon fixture and actual private-address UDP passed; live group delivery blocked by host");
            }
        }
    }
    private static boolean localDiscoveryAvailable(HostNetworks.Link link) throws Exception {
        try(var channel=DatagramChannel.open(StandardProtocolFamily.INET)) {
            channel.setOption(StandardSocketOptions.SO_REUSEADDR,true); channel.bind(new InetSocketAddress(0));
            channel.setOption(StandardSocketOptions.SO_BROADCAST,true);
            channel.setOption(StandardSocketOptions.IP_MULTICAST_IF,link.network());
            channel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP,true); channel.setOption(StandardSocketOptions.IP_MULTICAST_TTL,1);
            InetAddress group=InetAddress.getByName("239.192.83.92"); var membership=channel.join(group,link.network());
            DatagramSocket socket=channel.socket(); socket.setSoTimeout(150);
            boolean unicast=echo(socket,link.address()),broadcast=echo(socket,HostNetworks.broadcast(link)),multicast=echo(socket,group);
            membership.drop();
            System.out.println("Independent LAN preflight: unicast="+unicast+", broadcast="+broadcast+", multicast="+multicast);
            check(unicast,"Host cannot deliver local private-address UDP");
            return multicast || broadcast;
        }
    }
    private static boolean echo(DatagramSocket socket,InetAddress destination) throws Exception {
        if(destination==null)return false;
        byte[] bytes=Wire.random(16); socket.send(new DatagramPacket(bytes,bytes.length,destination,socket.getLocalPort()));
        long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(400);
        while(System.nanoTime()<deadline)try {
            var packet=new DatagramPacket(new byte[64],64); socket.receive(packet);
            if(Arrays.equals(bytes,Arrays.copyOf(packet.getData(),packet.getLength())))return true;
        } catch(SocketTimeoutException ignored) {}
        return false;
    }
    private static void beacon(VoiceClient source,VoiceClient target,InetAddress address) throws Exception {
        var peers=VoiceClient.class.getDeclaredField("peers"); peers.setAccessible(true);
        Object peer=((Map<?,?>)peers.get(source)).values().iterator().next();
        var encode=VoiceClient.class.getDeclaredMethod("lanBeacon",peer.getClass()); encode.setAccessible(true);
        byte[] bytes=(byte[])encode.invoke(source,peer);
        var receive=VoiceClient.class.getDeclaredMethod("receiveLan",byte[].class,InetAddress.class); receive.setAccessible(true);
        byte[] tampered=bytes.clone(); tampered[tampered.length-1]^=1;
        receive.invoke(target,tampered,address);
        check(target.networkStats().lanDirect()==0,"Forged LAN beacon accepted");
        receive.invoke(target,bytes,address);
    }
    private static LanDiscovery installLan(VoiceClient client,List<HostNetworks.Link> links) throws Exception {
        var receive=VoiceClient.class.getDeclaredMethod("receiveLan",byte[].class,InetAddress.class); receive.setAccessible(true);
        var field=VoiceClient.class.getDeclaredField("lan"); field.setAccessible(true);
        var discovery=new LanDiscovery(client.sessionId(),links,(bytes,source) -> {
            try { receive.invoke(client,bytes,source); } catch(ReflectiveOperationException e) { throw new IllegalStateException(e); }
        }); field.set(client,discovery); return discovery;
    }
    private static TcpTransport tcp(VoiceClient client) throws Exception {
        var field=VoiceClient.class.getDeclaredField("tcp"); field.setAccessible(true); return (TcpTransport)field.get(client);
    }
    private static VoiceClient client(URI uri,UUID id,String name,Sink sink,DatagramSocket socket) throws Exception {
        return new VoiceClient(uri,null,new VoiceClient.Identity(id,name,id,name,"e".repeat(64),"minecraft:overworld"),
                true,true,c -> CompletableFuture.completedFuture(false),sink,List.of(),socket,true);
    }
    private static long voice(RelayServer server) { var health=server.health(); return health.get("relayedUdpVoicePackets").getAsLong()+health.get("relayedWssVoicePackets").getAsLong(); }
    private static void exchange(VoiceClient a,VoiceClient b,Sink sa,Sink sb,UUID pa,UUID pb,String label) throws Exception {
        byte[] payload={1,3,5,7};
        a.sendAudio(Set.of(pb),0,16,payload,false); b.sendAudio(Set.of(pa),0,16,payload,false);
        check(Arrays.equals(sb.audio.poll(3,TimeUnit.SECONDS),payload),label+" forward");
        check(Arrays.equals(sa.audio.poll(3,TimeUnit.SECONDS),payload),label+" reverse");
        System.out.println(label+" passed");
    }
    private static void closedPort(int port) throws Exception {
        try(Socket socket=new Socket()) { socket.connect(new InetSocketAddress("127.0.0.1",port),200); }
        catch(IOException expected) { return; }
        throw new AssertionError("TCP listener retained after close");
    }
    private static void await(BooleanSupplier condition,String name) throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
        while(!condition.getAsBoolean() && System.nanoTime()<end)Thread.sleep(10);
        check(condition.getAsBoolean(),name);
    }
    private static void check(boolean value,String name) { if(!value)throw new AssertionError(name); }
}
