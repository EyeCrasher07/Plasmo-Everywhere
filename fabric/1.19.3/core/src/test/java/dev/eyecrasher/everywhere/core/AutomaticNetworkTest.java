package dev.eyecrasher.everywhere.core;

import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import static dev.eyecrasher.everywhere.core.ProtocolTest.check;

final class AutomaticNetworkTest {
    static void verify() throws Exception {
        codecs(); networks(); gateway(true); gateway(false); upnp(); tcpResponse(); tcpPortObservation(); tcpMutualProof(false); tcpMutualProof(true); concurrentCrypto();
        System.out.println("Automatic networking: PCP/NAT-PMP lease lifecycle, UPnP ownership/expiry, local scopes, bounded HTTP/XML and concurrent replay checks passed");
    }
    private static void codecs() throws Exception {
        Inet4Address local=(Inet4Address)InetAddress.getByName("192.168.1.25"); byte[] nonce=Wire.random(12);
        byte[] request=MappingProtocol.pcp(local,32000,false,nonce,0,120);
        check(request.length==60 && request[36]==17 && request[54]==(byte)255,"PCP IPv4 request and protocol");
        byte[] response=pcpResponse(request);
        check(MappingProtocol.pcpReply(response,32000,false,nonce).external().getPort()==45017,"PCP external mapping");
        byte[] bad=response.clone(); bad[24]^=1; reject(() -> MappingProtocol.pcpReply(bad,32000,false,nonce),"PCP nonce");
        reject(() -> MappingProtocol.pcpReply(response,32001,false,nonce),"PCP internal binding");
        reject(() -> MappingProtocol.pcpReply(response,32000,true,nonce),"PCP protocol binding");
        for(int lifetime:new int[]{0,-1,3601}) {
            byte[] wrong=response.clone(); ByteBuffer.wrap(wrong).putInt(4,lifetime);
            reject(() -> MappingProtocol.pcpReply(wrong,32000,false,nonce),"PCP finite lifetime");
        }
        byte[] privateIp=response.clone(); privateIp[56]=10;
        reject(() -> MappingProtocol.pcpReply(privateIp,32000,false,nonce),"PCP nested NAT address");
        byte[] address=pmpAddress();
        check(MappingProtocol.pmpAddress(address).getHostAddress().equals("8.8.4.4"),"NAT-PMP public address");
        address[8]=10; reject(() -> MappingProtocol.pmpAddress(address),"NAT-PMP nested NAT");
    }
    private static void networks() throws Exception {
        var loop=NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress());
        var link=new HostNetworks.Link(loop,(Inet4Address)InetAddress.getByName("192.168.1.25"),(short)24);
        check(LanDiscovery.localSource(InetAddress.getByName("192.168.1.50"),List.of(link)),"Attached LAN source");
        for(String source:List.of("192.168.2.50","10.0.0.1","127.0.0.1","8.8.8.8","169.254.1.2"))
            check(!LanDiscovery.localSource(InetAddress.getByName(source),List.of(link)),"Unattached source: "+source);
        String route=loop.getName()+" 00000000 0101A8C0 0003 0 0 10 00000000 0 0 0\n";
        check(HostNetworks.parseLinux(route,List.of(link)).getFirst().address().getHostAddress().equals("192.168.1.1"),"Linux gateway read");
        check(HostNetworks.parseLinux(route.replace("0003","0001"),List.of(link)).isEmpty(),"No fabricated gateway");
        check(HostNetworks.parseNumericRoutes("0.0.0.0 0.0.0.0 192.168.1.1 192.168.1.25 25",List.of(link)).size()==1,"Windows numeric gateway");
        check(HostNetworks.parseNumericRoutes("default router.example",List.of(link)).isEmpty(),"No gateway DNS from route strings");
        var local=InetAddress.getByName("192.168.1.1");
        for(String url:List.of("http://8.8.8.8/a","http://router.example/a","http://192.168.1.1@8.8.8.8/a","file:///etc/passwd","http://192.168.1.1:0/a"))
            reject(() -> UpnpGateway.localUri(URI.create(url),local),"IGD redirect outside actual gateway");
        reject(() -> UpnpGateway.xml("<!DOCTYPE x [<!ENTITY e SYSTEM 'file:///etc/passwd'>]><x>&e;</x>".getBytes(StandardCharsets.UTF_8)),"IGD external entities");
        reject(() -> UpnpGateway.xml(new byte[65537]),"IGD XML bound");
    }
    private static void gateway(boolean pcp) throws Exception {
        try(FakeGateway server=new FakeGateway(pcp)) {
            var loop=(Inet4Address)InetAddress.getByName("127.0.0.1");
            var gateway=new HostNetworks.Gateway(new HostNetworks.Link(null,loop,(short)8),loop);
            AtomicLong time=new AtomicLong(1);
            RouterMapper mapper=new RouterMapper(31001,31002,() -> List.of(gateway),server.socket.getLocalPort(),time::get);
            try {
                await(() -> mapper.candidates(false).size()==1 && mapper.candidates(true).size()==1,"Gateway leases");
                check(server.created.get()==2,"Only own two ports mapped");
                time.addAndGet(TimeUnit.SECONDS.toNanos(61));
                var tick=RouterMapper.class.getDeclaredMethod("tick"); tick.setAccessible(true); tick.invoke(mapper);
                check(server.created.get()==4,"Both leases renewed with same mapping identity");
                time.addAndGet(TimeUnit.SECONDS.toNanos(121));
                check(mapper.candidates(false).isEmpty() && mapper.candidates(true).isEmpty(),"Expired leases not advertised");
            } finally { mapper.close(); }
            await(() -> server.deleted.get()==2,"Own leases removed at close");
            check(server.ports.equals(Set.of(31001,31002)),"Gateway scope did not expand");
        }
    }
    private static final class FakeGateway implements AutoCloseable {
        final DatagramSocket socket=new DatagramSocket(new InetSocketAddress("127.0.0.1",0));
        final AtomicInteger created=new AtomicInteger(),deleted=new AtomicInteger();
        final Set<Integer> ports=ConcurrentHashMap.newKeySet();
        FakeGateway(boolean pcp) throws IOException {
            Thread.ofVirtual().start(() -> {
                while(!socket.isClosed())try {
                    var packet=new DatagramPacket(new byte[1500],1500); socket.receive(packet);
                    byte[] request=Arrays.copyOf(packet.getData(),packet.getLength()),response;
                    if(request[0]==2) {
                        response=pcpResponse(request);
                        if(!pcp)response[3]=1;
                        else { ports.add(Short.toUnsignedInt(ByteBuffer.wrap(request).getShort(40))); count(ByteBuffer.wrap(request).getInt(4)); }
                    } else if(request.length==2)response=pmpAddress();
                    else {
                        int internal=Short.toUnsignedInt(ByteBuffer.wrap(request).getShort(4));
                        int lifetime=ByteBuffer.wrap(request).getInt(8); ports.add(internal); count(lifetime);
                        response=ByteBuffer.allocate(16).put((byte)0).put((byte)(128+request[1])).putShort((short)0).putInt(0)
                                .putShort((short)internal).putShort((short)(45000+request[1])).putInt(lifetime).array();
                    }
                    socket.send(new DatagramPacket(response,response.length,packet.getSocketAddress()));
                } catch(IOException|RuntimeException ignored) { if(socket.isClosed())return; }
            });
        }
        private void count(int seconds) { (seconds==0?deleted:created).incrementAndGet(); }
        public void close() { socket.close(); }
    }
    private static byte[] pcpResponse(byte[] request) {
        byte[] reply=request.clone(); reply[1]=(byte)129; Arrays.fill(reply,8,24,(byte)0);
        ByteBuffer.wrap(reply).putShort(42,(short)(45000+request[36]));
        Arrays.fill(reply,44,60,(byte)0); reply[54]=(byte)255; reply[55]=(byte)255;
        reply[56]=8; reply[57]=8; reply[58]=4; reply[59]=4; return reply;
    }
    private static byte[] pmpAddress() { return ByteBuffer.allocate(12).put((byte)0).put((byte)128).putShort((short)0).putInt(0).put(new byte[]{8,8,4,4}).array(); }
    private static void upnp() throws Exception {
        HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),4);
        AtomicBoolean mapped=new AtomicBoolean(),permanent=new AtomicBoolean(); AtomicInteger deletes=new AtomicInteger();
        server.createContext("/control",exchange -> {
            try {
                String action=exchange.getRequestHeaders().getFirst("SOAPAction");
                String body=new String(exchange.getRequestBody().readNBytes(10000),StandardCharsets.UTF_8);
                String fields; int status=200;
                if(action.contains("GetExternalIPAddress"))fields="<NewExternalIPAddress>8.8.4.4</NewExternalIPAddress>";
                else if(action.contains("GetSpecificPortMappingEntry")) {
                    if(!mapped.get()) { fields="<errorCode>714</errorCode>"; status=500; }
                    else fields="<NewInternalClient>127.0.0.1</NewInternalClient><NewInternalPort>31000</NewInternalPort>"
                            +"<NewPortMappingDescription>owner-test</NewPortMappingDescription><NewLeaseDuration>"+(permanent.get()?0:120)+"</NewLeaseDuration>";
                } else if(action.contains("AddAnyPortMapping") || action.contains("AddPortMapping")) {
                    check(body.contains("<NewLeaseDuration>120</NewLeaseDuration>"),"Finite UPnP request");
                    mapped.set(true); fields="<NewReservedPort>45000</NewReservedPort>";
                } else if(action.contains("DeletePortMapping")) { mapped.set(false); deletes.incrementAndGet(); fields=""; }
                else { fields="<errorCode>401</errorCode>"; status=500; }
                byte[] response=("<s:Envelope xmlns:s='http://schemas.xmlsoap.org/soap/envelope/'><s:Body><r>"+fields+"</r></s:Body></s:Envelope>").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status,response.length); exchange.getResponseBody().write(response);
            } finally { exchange.close(); }
        }); server.start();
        try(var igd=new UpnpGateway((Inet4Address)InetAddress.getByName("127.0.0.1"),(Inet4Address)InetAddress.getByName("127.0.0.1"),
                URI.create("http://127.0.0.1:"+server.getAddress().getPort()+"/control"),"urn:schemas-upnp-org:service:WANIPConnection:2","owner-test")) {
            var lease=igd.map(31000,false,null); check(lease.seconds()==120,"UPnP finite lease");
            igd.map(31000,false,igd.entry(lease,31000));
            igd.delete(igd.entry(lease,31000),false); check(deletes.get()==1 && !mapped.get(),"UPnP owned cleanup");
            permanent.set(true); reject(() -> igd.map(31000,false,null),"Router ignoring requested finite lease");
            check(!mapped.get() && deletes.get()==2,"Permanent lease immediately removed");
        } finally { server.stop(0); }
    }
    private static void tcpResponse() throws Exception {
        String json="{\"protocol\":1,\"ip\":\"8.8.4.4\",\"port\":45000}";
        String response="HTTP/1.1 200 OK\r\nContent-Length: "+json.length()+"\r\n\r\n"+json;
        check(TcpDiscovery.response(new ByteArrayInputStream(response.getBytes(StandardCharsets.US_ASCII)),false).getPort()==45000,"TCP observed endpoint");
        reject(() -> TcpDiscovery.response(new ByteArrayInputStream(response.replace("8.8.4.4","1.2.3.4").replace("45000","00000").getBytes(StandardCharsets.US_ASCII)),false),"TCP invalid endpoint");
        reject(() -> TcpDiscovery.response(new ByteArrayInputStream("HTTP/1.1 200 OK\r\nContent-Length: 99999\r\n\r\n".getBytes(StandardCharsets.US_ASCII)),false),"TCP body bound");
        reject(() -> TcpDiscovery.response(new ByteArrayInputStream("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".getBytes(StandardCharsets.US_ASCII)),false),"TCP unsupported framing");
    }
    private static void concurrentCrypto() throws Exception {
        UUID a=UUID.randomUUID(),b=UUID.randomUUID(); var ka=PeerCrypto.keyPair(); var kb=PeerCrypto.keyPair();
        var sender=new PeerCrypto(ka,a,b,kb.getPublic().getEncoded()); var receiver=new PeerCrypto(kb,b,a,ka.getPublic().getEncoded());
        try(var pool=Executors.newFixedThreadPool(8)) {
            for(int i=0;i<50;i++) {
                Wire.Frame frame=sender.seal(Wire.PING,new byte[16]); AtomicInteger accepted=new AtomicInteger();
                List<Future<?>> checks=new ArrayList<>();
                for(int n=0;n<8;n++)checks.add(pool.submit(() -> { if(receiver.open(frame)!=null)accepted.incrementAndGet(); }));
                for(var result:checks)result.get();
                check(accepted.get()==1,"One replay decision across UDP/TCP/LAN threads");
            }
        }
    }
    private static void tcpPortObservation() throws Exception {
        HttpServer http=HttpServer.create(new InetSocketAddress("127.0.0.1",0),4);
        http.createContext("/tcp-discovery",exchange -> {
            try {
                byte[] response=("{\"protocol\":1,\"ip\":\"127.0.0.1\",\"port\":"+exchange.getRemoteAddress().getPort()+"}").getBytes(StandardCharsets.US_ASCII);
                exchange.sendResponseHeaders(200,response.length); exchange.getResponseBody().write(response);
            } finally { exchange.close(); }
        }); http.start();
        try(var tcp=new TcpTransport(UUID.randomUUID(),0,new TcpTransport.Handler() {
            public PeerCrypto crypto(UUID peer) { return null; }
            public boolean permits(UUID peer,InetAddress source) { return false; }
            public void received(Wire.Frame frame,byte[] plain) { throw new AssertionError("No media peer"); }
        })) {
            try {
                var external=TcpDiscovery.lookup(tcp,URI.create("ws://127.0.0.1:"+http.getAddress().getPort()+"/voice"),null);
                check(external.getPort()==tcp.port(),"TCP observation did not use the listener's port");
            } catch(TcpTransport.SharedPortUnavailableException unsupported) {
                check(!tcp.sharedPortAvailable(),"Unsupported sharing is remembered");
                System.out.println("Same-port TCP observation unavailable on this OS/JVM; fallback tested separately");
            }
        } finally { http.stop(0); }
    }
    private static void tcpMutualProof(boolean refuseSharedPort) throws Exception {
        UUID a=UUID.randomUUID(),b=UUID.randomUUID(); var ka=PeerCrypto.keyPair(); var kb=PeerCrypto.keyPair();
        var alice=new PeerCrypto(ka,a,b,kb.getPublic().getEncoded()); var bob=new PeerCrypto(kb,b,a,ka.getPublic().getEncoded());
        try(var tcp=new TcpTransport(a,0,new TcpTransport.Handler() {
            public PeerCrypto crypto(UUID peer) { return peer.equals(b)?alice:null; }
            public boolean permits(UUID peer,InetAddress source) { return peer.equals(b) && source.isLoopbackAddress(); }
            public void received(Wire.Frame frame,byte[] plain) {}
        },(channel,port) -> {
            if(refuseSharedPort)throw new BindException("Injected listener-port sharing refusal");
            channel.bind(new InetSocketAddress(port));
        }); var listener=new ServerSocket(0,2,InetAddress.getLoopbackAddress())) {
            listener.setSoTimeout(2000);
            tcp.tick(System.nanoTime(),Map.of(b,List.of(new InetSocketAddress("127.0.0.1",listener.getLocalPort()))),id -> false);
            try(Socket raw=listener.accept()) {
                raw.setSoTimeout(2000); var input=new DataInputStream(raw.getInputStream()); var output=new DataOutputStream(raw.getOutputStream());
                byte[] fresh=Wire.random(16); writeFrame(output,bob.seal(Wire.PING,fresh));
                Wire.Frame challenge=readFrame(input); byte[] nonce=bob.open(challenge);
                check(challenge.kind()==Wire.PING && nonce!=null && nonce.length==16,"Simultaneous TCP open first challenge");
                writeFrame(output,bob.seal(Wire.PONG,nonce));
                Wire.Frame reply=readFrame(input); check(reply.kind()==Wire.PONG && Arrays.equals(bob.open(reply),fresh),"Simultaneous TCP mutual challenge");
                await(() -> tcp.ready(b),"Simultaneous TCP active opens authenticated");
                if(refuseSharedPort)check(!tcp.sharedPortAvailable() && raw.getPort()!=tcp.port(),"Authenticated TCP works after ephemeral fallback");
            }
            tcp.retain(Set.of());
            try(Socket claimant=new Socket("127.0.0.1",tcp.port())) {
                claimant.setSoTimeout(2200); var input=new DataInputStream(claimant.getInputStream());
                writeFrame(new DataOutputStream(claimant.getOutputStream()),bob.seal(Wire.PING,Wire.random(16)));
                check(readFrame(input).kind()==Wire.PONG && readFrame(input).kind()==Wire.PING,"Passive TCP requires a fresh reciprocal proof");
                Thread.sleep(100); check(!tcp.ready(b),"An encrypted old claim alone admitted a TCP session");
                check(input.read()==-1,"Unfinished TCP challenge timed out");
            }
        }
    }
    private static Wire.Frame readFrame(DataInputStream input) throws IOException {
        int length=input.readUnsignedShort(); check(length>=47 && length<=Wire.MAX_FRAME,"Test TCP framing");
        byte[] bytes=input.readNBytes(length); return Wire.decode(bytes);
    }
    private static void writeFrame(DataOutputStream output,Wire.Frame frame) throws IOException {
        byte[] bytes=frame.encode(); output.writeShort(bytes.length); output.write(bytes); output.flush();
    }
    private static void await(BooleanSupplier condition,String name) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(!condition.getAsBoolean() && System.nanoTime()<deadline)Thread.sleep(10);
        check(condition.getAsBoolean(),name);
    }
    private interface Checked { void run() throws Exception; }
    private static void reject(Checked action,String name) throws Exception {
        try { action.run(); } catch(Exception expected) { return; }
        throw new AssertionError(name);
    }
}
