package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Flow;
import javax.xml.XMLConstants;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

/** UPnP IGD limited to the OS default gateway and finite leases owned by this client. */
final class UpnpGateway implements AutoCloseable {
    record Entry(int port,int internal,String client,String description,int seconds) {}
    record Reply(int status,Document xml) { String value(String name) { return text(xml,name); } }
    private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofMillis(700))
            .followRedirects(HttpClient.Redirect.NEVER).proxy(new ProxySelector() {
                public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
                public void connectFailed(URI uri,SocketAddress address,IOException error) {}
            }).build();
    private final Inet4Address gateway,local;
    private final URI control;
    private final String service,owner;
    private final Set<CompletableFuture<?>> requests=ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed=new AtomicBoolean();
    UpnpGateway(Inet4Address gateway,Inet4Address local,URI control,String service,String owner) {
        this.gateway=gateway; this.local=local; this.control=control; this.service=service; this.owner=owner;
    }
    static UpnpGateway discover(HostNetworks.Gateway gateway,String owner) throws Exception {
        URI location=null;
        try(MulticastSocket socket=new MulticastSocket(new InetSocketAddress(gateway.link().address(),0))) {
            socket.setNetworkInterface(gateway.link().network()); socket.setTimeToLive(1); socket.setSoTimeout(1200);
            byte[] request=("M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\n"
                    +"ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
            socket.send(new DatagramPacket(request,request.length,InetAddress.getByName("239.255.255.250"),1900));
            long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(1300);
            for(int i=0;i<8 && System.nanoTime()<deadline;i++) {
                var packet=new DatagramPacket(new byte[2048],2048); socket.receive(packet);
                if(!packet.getAddress().equals(gateway.address()))continue;
                for(String line:new String(packet.getData(),0,packet.getLength(),StandardCharsets.US_ASCII).split("\r\n")) {
                    if(line.toLowerCase(Locale.ROOT).startsWith("location:"))
                        location=localUri(URI.create(line.substring(9).trim()),gateway.address());
                }
                if(location!=null)break;
            }
        }
        if(location==null)throw new IOException("No local IGD");
        var probe=new UpnpGateway(gateway.address(),gateway.link().address(),location,"",owner);
        try {
            var description=probe.request(HttpRequest.newBuilder(location).GET());
            if(description.status()!=200)throw new IOException("IGD description");
            URI base=location; String declared=text(description.xml(),"URLBase");
            if(!declared.isBlank())base=localUri(URI.create(declared),gateway.address());
            NodeList services=description.xml().getElementsByTagNameNS("*","service");
            for(int i=0;i<services.getLength();i++) {
                Element element=(Element)services.item(i); String kind=text(element,"serviceType");
                if(!Set.of("urn:schemas-upnp-org:service:WANIPConnection:1","urn:schemas-upnp-org:service:WANIPConnection:2",
                        "urn:schemas-upnp-org:service:WANPPPConnection:1").contains(kind))continue;
                URI control=localUri(base.resolve(text(element,"controlURL")),gateway.address());
                return new UpnpGateway(gateway.address(),gateway.link().address(),control,kind,owner);
            }
            throw new IOException("No mapping service");
        } finally { probe.close(); }
    }
    MappingProtocol.Lease map(int internal,boolean tcp,Entry previous) throws Exception {
        String protocol=tcp?"TCP":"UDP";
        Reply address=soap("GetExternalIPAddress",Map.of());
        if(address.status()!=200)throw new IOException("IGD public address response");
        String external=address.value("NewExternalIPAddress");
        if(!external.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}"))throw new IOException("IGD public address");
        InetAddress publicIp=InetAddress.getByName(external);
        if(!Endpoints.isPublic(publicIp))throw new IOException("IGD is behind another NAT");
        int port=previous==null?internal:previous.port();
        boolean any=previous==null && service.endsWith(":2");
        if(!any) {
            Reply existing=specific(port,protocol);
            if(existing.value("errorCode").equals("714")) { /* unused port */ }
            else if(!owned(existing,internal))throw new IOException("Port belongs to another application");
        }
        Map<String,String> args=new LinkedHashMap<>();
        args.put("NewRemoteHost",""); args.put("NewExternalPort",Integer.toString(port)); args.put("NewProtocol",protocol);
        args.put("NewInternalPort",Integer.toString(internal)); args.put("NewInternalClient",local.getHostAddress());
        args.put("NewEnabled","1"); args.put("NewPortMappingDescription",owner); args.put("NewLeaseDuration","120");
        Reply added=soap(any?"AddAnyPortMapping":"AddPortMapping",args);
        if(added.status()!=200 || !added.value("errorCode").isBlank())throw new IOException("IGD refused finite lease");
        if(any)port=Integer.parseInt(added.value("NewReservedPort"));
        if(port<1 || port>65535)throw new IOException("IGD mapped port");
        Reply confirmed=specific(port,protocol);
        if(!owned(confirmed,internal))throw new IOException("IGD ownership confirmation");
        int seconds=Integer.parseInt(confirmed.value("NewLeaseDuration"));
        if(seconds<30 || seconds>3600) {
            delete(new Entry(port,internal,local.getHostAddress(),owner,seconds),tcp);
            throw new IOException("IGD did not retain finite lease");
        }
        return new MappingProtocol.Lease(new InetSocketAddress(publicIp,port),Math.min(seconds,3600));
    }
    Entry entry(MappingProtocol.Lease lease,int internal) {
        return new Entry(lease.external().getPort(),internal,local.getHostAddress(),owner,lease.seconds());
    }
    void delete(Entry entry,boolean tcp) throws Exception {
        if(!owned(specific(entry.port(),tcp?"TCP":"UDP"),entry.internal()))return;
        soap("DeletePortMapping",Map.of("NewRemoteHost","","NewExternalPort",Integer.toString(entry.port()),"NewProtocol",tcp?"TCP":"UDP"));
    }
    private Reply specific(int port,String protocol) throws Exception {
        return soap("GetSpecificPortMappingEntry",Map.of("NewRemoteHost","","NewExternalPort",Integer.toString(port),"NewProtocol",protocol));
    }
    private boolean owned(Reply reply,int internal) {
        return reply.status()==200 && reply.value("NewInternalClient").equals(local.getHostAddress())
                && reply.value("NewInternalPort").equals(Integer.toString(internal)) && reply.value("NewPortMappingDescription").equals(owner);
    }
    private Reply soap(String action,Map<String,String> args) throws Exception {
        StringBuilder body=new StringBuilder("<?xml version=\"1.0\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:")
                .append(action).append(" xmlns:u=\"").append(service).append("\">");
        args.forEach((k,v) -> body.append('<').append(k).append('>').append(escape(v)).append("</").append(k).append('>'));
        body.append("</u:").append(action).append("></s:Body></s:Envelope>");
        return request(HttpRequest.newBuilder(control).header("Content-Type","text/xml; charset=\"utf-8\"")
                .header("SOAPAction","\""+service+"#"+action+"\"").POST(HttpRequest.BodyPublishers.ofString(body.toString())));
    }
    private Reply request(HttpRequest.Builder builder) throws Exception {
        if(closed.get())throw new IOException("IGD client closed");
        var future=http.sendAsync(builder.timeout(Duration.ofMillis(1200)).build(),info -> new LimitedBody());
        requests.add(future);
        if(closed.get())future.cancel(true);
        try {
            var response=future.get(1400,TimeUnit.MILLISECONDS);
            return new Reply(response.statusCode(),xml(response.body()));
        } finally { requests.remove(future); if(!future.isDone())future.cancel(true); }
    }
    static URI localUri(URI uri,InetAddress gateway) {
        if(!"http".equals(uri.getScheme()) || !gateway.getHostAddress().equals(uri.getHost()) || uri.getUserInfo()!=null
                || uri.getFragment()!=null || uri.toString().length()>1024 || uri.getPort()==0 || uri.getPort()>65535)
            throw new IllegalArgumentException("IGD URL must stay on the observed default gateway");
        return uri;
    }
    static Document xml(byte[] bytes) throws Exception {
        if(bytes.length>65536)throw new IOException("IGD XML size");
        DocumentBuilderFactory f=DocumentBuilderFactory.newInstance(); f.setNamespaceAware(true);
        f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING,true);
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl",true);
        f.setFeature("http://xml.org/sax/features/external-general-entities",false);
        f.setFeature("http://xml.org/sax/features/external-parameter-entities",false);
        f.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD,""); f.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA,"");
        var builder=f.newDocumentBuilder(); builder.setErrorHandler(new DefaultHandler() {
            public void error(SAXParseException e) throws SAXException { throw e; }
            public void fatalError(SAXParseException e) throws SAXException { throw e; }
        });
        return builder.parse(new ByteArrayInputStream(bytes));
    }
    private static String text(Node node,String name) {
        NodeList list=node instanceof Document d?d.getElementsByTagNameNS("*",name):((Element)node).getElementsByTagNameNS("*",name);
        return list.getLength()==1?list.item(0).getTextContent().trim():"";
    }
    private static String escape(String text) { return text.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\"","&quot;"); }
    @Override public void close() {
        if(!closed.compareAndSet(false,true))return;
        requests.forEach(request -> request.cancel(true)); RuntimeCompat.shutdown(http);
    }
    static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        final CompletableFuture<byte[]> result=new CompletableFuture<>();
        final ByteArrayOutputStream bytes=new ByteArrayOutputStream(); Flow.Subscription subscription;
        public CompletionStage<byte[]> getBody() { return result; }
        public void onSubscribe(Flow.Subscription next) { subscription=next; next.request(1); }
        public void onNext(List<ByteBuffer> parts) {
            for(var part:parts) {
                if(part.remaining()>65536-bytes.size()) { subscription.cancel(); result.completeExceptionally(new IOException("IGD response size")); return; }
                byte[] block=new byte[part.remaining()]; part.get(block); bytes.writeBytes(block);
            }
            subscription.request(1);
        }
        public void onError(Throwable error) { result.completeExceptionally(error); }
        public void onComplete() { result.complete(bytes.toByteArray()); }
    }
}
