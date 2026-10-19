package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Best-effort background PCP, NAT-PMP and UPnP. Only this process's own UDP/TCP ports. */
final class RouterMapper implements AutoCloseable {
    record Diagnostics(int gateways, int udpMappings, int tcpMappings, String methods, long failures) {}
    private enum Method { PCP, PMP, UPNP }
    private static final class Mapping {
        final HostNetworks.Gateway gateway;
        final int internal;
        final boolean tcp;
        final byte[] nonce=Wire.random(12);
        Method method;
        volatile MappingProtocol.Lease lease;
        volatile long expires;
        long renew;
        UpnpGateway upnp;
        Mapping(HostNetworks.Gateway gateway,int internal,boolean tcp) { this.gateway=gateway; this.internal=internal; this.tcp=tcp; }
    }
    private final int udpPort,tcpPort;
    private final int gatewayPort;
    private final Supplier<List<HostNetworks.Gateway>> gatewayProvider;
    private final LongSupplier clock;
    private final ScheduledExecutorService worker=Executors.newSingleThreadScheduledExecutor(r -> {
        var t=new Thread(r,"everywhere-router"); t.setDaemon(true); return t;
    });
    private final List<Mapping> mappings=new CopyOnWriteArrayList<>();
    private final AtomicReference<DatagramSocket> active=new AtomicReference<>();
    private final AtomicBoolean closed=new AtomicBoolean();
    private final LongAdder failures=new LongAdder();
    private volatile int gatewayCount;
    private final String owner="PlasmoEverywhere-"+UUID.randomUUID().toString().substring(0,8);
    private long discoverAt;
    RouterMapper(int udpPort,int tcpPort,InetAddress destination) {
        this(udpPort,tcpPort,() -> {
            InetAddress source=HostNetworks.sourceFor(destination);
            return HostNetworks.gateways(HostNetworks.links()).stream().filter(gateway -> gateway.link().address().equals(source)).toList();
        },5351,System::nanoTime);
    }
    // Local fixture injects its gateway/clock; production always reads the OS default gateway.
    RouterMapper(int udpPort,int tcpPort,Supplier<List<HostNetworks.Gateway>> gateways,int gatewayPort,LongSupplier clock) {
        this.udpPort=udpPort; this.tcpPort=tcpPort;
        this.gatewayProvider=gateways; this.gatewayPort=gatewayPort; this.clock=clock;
        worker.scheduleWithFixedDelay(this::tick,0,5,TimeUnit.SECONDS);
    }
    List<InetSocketAddress> candidates(boolean tcp) {
        if(closed.get())return List.of();
        long now=clock.getAsLong();
        return mappings.stream().filter(m -> m.tcp==tcp && m.lease!=null && now<m.expires).map(m -> m.lease.external()).limit(2).toList();
    }
    Diagnostics diagnostics() {
        long now=clock.getAsLong();
        var active=mappings.stream().filter(m -> !closed.get() && m.lease!=null && now<m.expires).toList();
        String methods=active.stream().map(m -> m.method==Method.PMP?"NAT-PMP":m.method==Method.UPNP?"UPnP":"PCP")
                .distinct().sorted().collect(java.util.stream.Collectors.joining(","));
        return new Diagnostics(gatewayCount,(int)active.stream().filter(m -> !m.tcp).count(),
                (int)active.stream().filter(m -> m.tcp).count(),methods,failures.sum());
    }
    private void tick() {
        if(closed.get())return;
        try {
            long now=clock.getAsLong();
            if(now>=discoverAt) {
                discoverAt=now+TimeUnit.SECONDS.toNanos(60);
                var gateways=gatewayProvider.get();
                gatewayCount=gateways.size();
                for(var old:List.copyOf(mappings))if(!gateways.contains(old.gateway)) {
                    mappings.remove(old); remove(old);
                }
                for(var gateway:gateways)for(boolean tcp:new boolean[]{false,true}) {
                    int port=tcp?tcpPort:udpPort;
                    if(port<1 || mappings.stream().anyMatch(m -> m.gateway.equals(gateway) && m.tcp==tcp))continue;
                    Mapping mapping=new Mapping(gateway,port,tcp);
                    try {
                        create(mapping);
                        boolean retained;
                        synchronized(mappings) { retained=!closed.get(); if(retained)mappings.add(mapping); }
                        if(!retained) { remove(mapping); if(mapping.upnp!=null)mapping.upnp.close(); }
                    }
                    catch(Exception ignored) { failures.increment(); if(mapping.upnp!=null)mapping.upnp.close(); }
                    if(closed.get())return;
                }
            }
            for(var mapping:List.copyOf(mappings)) {
                if(closed.get())return;
                if(clock.getAsLong()<mapping.renew)continue;
                try { renew(mapping); }
                catch(Exception ignored) {
                    failures.increment();
                    mapping.renew=clock.getAsLong()+TimeUnit.SECONDS.toNanos(10);
                    if(clock.getAsLong()>mapping.expires) {
                        mappings.remove(mapping); remove(mapping);
                        if(mapping.upnp!=null)mapping.upnp.close();
                    }
                }
            }
        } catch(Exception ignored) { /* A router is optional; direct checks and relay remain available. */ }
    }
    private void create(Mapping mapping) throws Exception {
        if(closed.get())throw new IOException("Closed");
        try {
            mapping.method=Method.PCP; renew(mapping); return;
        } catch(Exception ignored) {}
        if(closed.get())throw new IOException("Closed");
        try {
            mapping.method=Method.PMP; renew(mapping); return;
        } catch(Exception ignored) {}
        if(closed.get())throw new IOException("Closed");
        mapping.upnp=UpnpGateway.discover(mapping.gateway,owner);
        mapping.method=Method.UPNP; renew(mapping);
    }
    private void renew(Mapping mapping) throws Exception {
        int external=mapping.lease==null?0:mapping.lease.external().getPort();
        MappingProtocol.Lease next;
        if(mapping.method==Method.PCP) {
            byte[] request=MappingProtocol.pcp(mapping.gateway.link().address(),mapping.internal,mapping.tcp,mapping.nonce,external,120);
            byte[] response=exchange(mapping.gateway,request);
            try { next=MappingProtocol.pcpReply(response,mapping.internal,mapping.tcp,mapping.nonce); }
            catch(IllegalArgumentException e) {
                // A malformed successful reply can still have allocated our own nonce/port.
                if(response.length>=4 && response[0]==2 && response[1]==(byte)129 && response[3]==0)
                    exchange(mapping.gateway,MappingProtocol.pcp(mapping.gateway.link().address(),mapping.internal,mapping.tcp,mapping.nonce,0,0));
                throw e;
            }
        } else if(mapping.method==Method.PMP) {
            var address=MappingProtocol.pmpAddress(exchange(mapping.gateway,new byte[]{0,0}));
            byte[] response=exchange(mapping.gateway,MappingProtocol.pmp(mapping.internal,mapping.tcp,external,120));
            try { next=MappingProtocol.pmpReply(response,mapping.internal,mapping.tcp,address); }
            catch(IllegalArgumentException e) {
                if(response.length>=4 && response[0]==0 && response[2]==0 && response[3]==0)
                    exchange(mapping.gateway,MappingProtocol.pmp(mapping.internal,mapping.tcp,0,0));
                throw e;
            }
        } else next=mapping.upnp.map(mapping.internal,mapping.tcp,mapping.lease==null?null:mapping.upnp.entry(mapping.lease,mapping.internal));
        mapping.lease=next;
        mapping.expires=clock.getAsLong()+TimeUnit.SECONDS.toNanos(next.seconds());
        mapping.renew=clock.getAsLong()+TimeUnit.SECONDS.toNanos(Math.max(10,Math.min(60,next.seconds()/2)));
    }
    private byte[] exchange(HostNetworks.Gateway gateway,byte[] request) throws IOException {
        try(DatagramSocket socket=new DatagramSocket(new InetSocketAddress(gateway.link().address(),0))) {
            active.set(socket); socket.connect(gateway.address(),gatewayPort);
            for(int timeout:new int[]{250,500}) {
                socket.setSoTimeout(timeout); socket.send(new DatagramPacket(request,request.length));
                try {
                    var packet=new DatagramPacket(new byte[1101],1101); socket.receive(packet);
                    if(packet.getLength()>1100)throw new IOException("Mapping response size");
                    return Arrays.copyOf(packet.getData(),packet.getLength());
                } catch(SocketTimeoutException ignored) {}
            }
            throw new SocketTimeoutException("Gateway unavailable");
        } finally { active.set(null); }
    }
    private void remove(Mapping mapping) {
        if(mapping.lease==null)return;
        try {
            if(mapping.method==Method.PCP)
                exchange(mapping.gateway,MappingProtocol.pcp(mapping.gateway.link().address(),mapping.internal,mapping.tcp,mapping.nonce,mapping.lease.external().getPort(),0));
            else if(mapping.method==Method.PMP)
                exchange(mapping.gateway,MappingProtocol.pmp(mapping.internal,mapping.tcp,mapping.lease.external().getPort(),0));
            else mapping.upnp.delete(mapping.upnp.entry(mapping.lease,mapping.internal),mapping.tcp);
        } catch(Exception ignored) { /* Finite leases expire even after a crash or lost delete request. */ }
    }
    @Override public void close() {
        if(!closed.compareAndSet(false,true))return;
        worker.shutdownNow(); DatagramSocket socket=active.getAndSet(null); if(socket!=null)socket.close();
        List<Mapping> owned;
        synchronized(mappings) { owned=List.copyOf(mappings); mappings.clear(); }
        var cleanup=new Thread(() -> {
            try { worker.awaitTermination(2,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
            for(var mapping:owned) { remove(mapping); if(mapping.upnp!=null)mapping.upnp.close(); }
        },"everywhere-router-cleanup");
        cleanup.setDaemon(true); cleanup.start();
    }
}
