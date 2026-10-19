package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Strict PCP MAP / NAT-PMP wire codecs. No third-party mappings or permanent leases. */
final class MappingProtocol {
    record Lease(InetSocketAddress external,int seconds) {}
    static byte[] pcp(Inet4Address local,int port,boolean tcp,byte[] nonce,int externalPort,int seconds) {
        if(nonce.length!=12 || port<1 || port>65535 || externalPort<0 || externalPort>65535 || seconds<0 || seconds>120)
            throw new IllegalArgumentException("Mapping request");
        var b=ByteBuffer.allocate(60);
        b.put((byte)2).put((byte)1).putShort((short)0).putInt(seconds);
        b.put(new byte[10]).putShort((short)0xffff).put(local.getAddress());
        b.put(nonce).put((byte)(tcp?6:17)).put(new byte[3]);
        b.putShort((short)port).putShort((short)externalPort).put(new byte[10]).putShort((short)0xffff).putInt(0);
        return b.array();
    }
    static Lease pcpReply(byte[] response,int port,boolean tcp,byte[] nonce) {
        if(response.length<60 || response.length>1100)throw new IllegalArgumentException("PCP size");
        var b=ByteBuffer.wrap(response);
        if(b.get()!=2 || b.get()!=(byte)129 || b.get()!=0 || b.get()!=0)throw new IllegalArgumentException("PCP response");
        long lifetime=Integer.toUnsignedLong(b.getInt()); b.position(24);
        byte[] actual=new byte[12]; b.get(actual);
        if(!Arrays.equals(actual,nonce) || b.get()!=(byte)(tcp?6:17))throw new IllegalArgumentException("PCP binding");
        b.position(40);
        if(Short.toUnsignedInt(b.getShort())!=port)throw new IllegalArgumentException("PCP internal port");
        int externalPort=Short.toUnsignedInt(b.getShort()); byte[] address=new byte[16]; b.get(address);
        return lease(address,externalPort,lifetime);
    }
    static byte[] pmp(int port,boolean tcp,int externalPort,int seconds) {
        if(port<1 || port>65535 || externalPort<0 || externalPort>65535 || seconds<0 || seconds>120)
            throw new IllegalArgumentException("Mapping request");
        return ByteBuffer.allocate(12).put((byte)0).put((byte)(tcp?2:1)).putShort((short)0)
                .putShort((short)port).putShort((short)externalPort).putInt(seconds).array();
    }
    static Inet4Address pmpAddress(byte[] response) {
        if(response.length!=12 || response[0]!=0 || response[1]!=(byte)128 || response[2]!=0 || response[3]!=0)
            throw new IllegalArgumentException("NAT-PMP address");
        try {
            var address=(Inet4Address)InetAddress.getByAddress(Arrays.copyOfRange(response,8,12));
            if(!Endpoints.isPublic(address))throw new IllegalArgumentException("Non-public external address");
            return address;
        } catch(UnknownHostException e) { throw new IllegalArgumentException(e); }
    }
    static Lease pmpReply(byte[] response,int port,boolean tcp,Inet4Address address) {
        if(response.length!=16 || response[0]!=0 || response[1]!=(byte)(tcp?130:129)
                || response[2]!=0 || response[3]!=0)throw new IllegalArgumentException("NAT-PMP response");
        var b=ByteBuffer.wrap(response); b.position(8);
        if(Short.toUnsignedInt(b.getShort())!=port)throw new IllegalArgumentException("NAT-PMP internal port");
        int external=Short.toUnsignedInt(b.getShort()); long lifetime=Integer.toUnsignedLong(b.getInt());
        return lease(address.getAddress(),external,lifetime);
    }
    private static Lease lease(byte[] address,int port,long seconds) {
        try {
            var ip=InetAddress.getByAddress(address);
            if(port==0 || seconds<30 || seconds>3600 || !Endpoints.isPublic(ip))throw new IllegalArgumentException("Invalid mapping lease");
            return new Lease(new InetSocketAddress(ip,port),(int)Math.min(seconds,3600));
        } catch(UnknownHostException e) { throw new IllegalArgumentException(e); }
    }
}
