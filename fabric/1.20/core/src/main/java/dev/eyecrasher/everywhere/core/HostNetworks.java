package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Reads interfaces and the existing default gateway; never changes host routing. */
final class HostNetworks {
    record Link(NetworkInterface network, Inet4Address address, short prefix) {
        boolean contains(InetAddress other) { return subnet(address, other, prefix); }
    }
    record Gateway(Link link, Inet4Address address) {}
    static List<Link> links() {
        List<Link> result=new ArrayList<>();
        try {
            for(var network:Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if(!network.isUp() || network.isLoopback())continue;
                for(var address:network.getInterfaceAddresses()) {
                    if(address.getAddress() instanceof Inet4Address v4 && privateAddress(v4)
                            && address.getNetworkPrefixLength()>0 && address.getNetworkPrefixLength()<32 && result.size()<8)
                        result.add(new Link(network,v4,address.getNetworkPrefixLength()));
                }
            }
        } catch(SocketException ignored) {}
        return List.copyOf(result);
    }
    static boolean privateAddress(InetAddress address) {
        byte[] a=address.getAddress();
        if(a.length!=4)return false;
        int x=a[0]&255,y=a[1]&255;
        return x==10 || x==172 && y>=16 && y<=31 || x==192 && y==168 || x==100 && y>=64 && y<=127;
    }
    static boolean subnet(InetAddress a,InetAddress b,int prefix) {
        byte[] x=a.getAddress(),y=b.getAddress();
        if(x.length!=y.length || prefix<1 || prefix>x.length*8)return false;
        for(int i=0;i<x.length;i++) {
            int bits=Math.min(8,Math.max(0,prefix-i*8)),mask=bits==0?0:255<<(8-bits);
            if((x[i]&mask)!=(y[i]&mask))return false;
        }
        return true;
    }
    static InetAddress broadcast(Link link) {
        if(link.prefix()<1 || link.prefix()>30)return null;
        byte[] bytes=link.address().getAddress();
        for(int i=0;i<4;i++) {
            int bits=Math.min(8,Math.max(0,link.prefix()-i*8));
            bytes[i]|=(byte)(bits==8?0:(1<<(8-bits))-1);
        }
        try { return InetAddress.getByAddress(bytes); } catch(UnknownHostException e) { return null; }
    }
    static List<InetAddress> publicAddresses() {
        LinkedHashSet<InetAddress> result=new LinkedHashSet<>();
        try {
            for(var network:Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if(!network.isUp() || network.isLoopback())continue;
                for(var address:Collections.list(network.getInetAddresses()))
                    if(Endpoints.isPublic(address) && result.size()<8)result.add(InetAddress.getByAddress(address.getAddress()));
            }
        } catch(IOException ignored) {}
        return List.copyOf(result);
    }
    static List<Gateway> gateways(List<Link> links) {
        String os=System.getProperty("os.name","").toLowerCase(Locale.ROOT);
        try {
            if(os.contains("linux"))return parseLinux(Files.readString(Path.of("/proc/net/route")),links);
            if(os.contains("win")) {
                String root=System.getenv("SystemRoot");
                if(root==null)return List.of();
                return parseNumericRoutes(command(Path.of(root,"System32","route.exe").toString(),"PRINT","-4"),links);
            }
            if(os.contains("mac") || os.contains("bsd"))
                return parseNumericRoutes(command("/usr/sbin/netstat","-rn","-f","inet"),links);
        } catch(Exception ignored) {}
        return List.of();
    }
    static InetAddress sourceFor(InetAddress destination) {
        try(DatagramSocket socket=new DatagramSocket()) {
            socket.connect(destination,443); return socket.getLocalAddress();
        } catch(SocketException e) { return null; }
    }
    static List<Gateway> parseLinux(String text,List<Link> links) {
        LinkedHashSet<Gateway> result=new LinkedHashSet<>();
        for(String line:text.split("\\R")) {
            String[] columns=line.trim().split("\\s+");
            if(columns.length<8 || !columns[1].equals("00000000") || !columns[7].equals("00000000"))continue;
            try {
                if((Integer.parseInt(columns[3],16)&3)!=3)continue;
                long value=Long.parseLong(columns[2],16); byte[] bytes=new byte[4];
                for(int i=0;i<4;i++)bytes[i]=(byte)(value>>>(8*i));
                var gateway=(Inet4Address)InetAddress.getByAddress(bytes);
                for(Link link:links)if(link.network().getName().equals(columns[0]) && link.contains(gateway)
                        && privateAddress(gateway) && !gateway.equals(link.address()))result.add(new Gateway(link,gateway));
            } catch(IllegalArgumentException|UnknownHostException ignored) {}
        }
        return result.stream().limit(2).toList();
    }
    static List<Gateway> parseNumericRoutes(String text,List<Link> links) {
        LinkedHashSet<Gateway> result=new LinkedHashSet<>();
        for(String line:text.split("\\R")) {
            String[] c=line.trim().split("\\s+");
            if(c.length<3 || !(c[0].equals("default") || c[0].equals("0.0.0.0") && c[1].equals("0.0.0.0")))continue;
            String value=c[c[0].equals("default")?1:2];
            if(!value.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}"))continue;
            try {
                var address=InetAddress.getByName(value);
                if(!(address instanceof Inet4Address v4) || !privateAddress(v4))continue;
                for(Link link:links)if(link.contains(v4) && !link.address().equals(v4))result.add(new Gateway(link,v4));
            } catch(UnknownHostException ignored) {}
        }
        return result.stream().limit(2).toList();
    }
    private static String command(String... args) throws IOException,InterruptedException {
        Process process=new ProcessBuilder(args).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        try {
            if(!process.waitFor(1500,TimeUnit.MILLISECONDS))return "";
            byte[] bytes=process.getInputStream().readNBytes(65537);
            return process.exitValue()==0 && bytes.length<=65536?new String(bytes,StandardCharsets.UTF_8):"";
        } finally { process.destroyForcibly(); }
    }
}
