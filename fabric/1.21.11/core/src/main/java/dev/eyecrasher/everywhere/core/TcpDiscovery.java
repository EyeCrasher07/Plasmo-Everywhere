package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.net.ssl.*;

/** Observe the external TCP mapping from the same reusable local port as the direct listener. */
final class TcpDiscovery {
    static InetSocketAddress lookup(TcpTransport transport,URI relay,SSLContext context) throws Exception {
        boolean secure=relay.getScheme().equals("wss");
        if(!secure && !relay.getHost().equals("127.0.0.1"))throw new IOException("Discovery TLS required");
        int port=relay.getPort()>0?relay.getPort():(secure?443:80);
        try(Socket raw=transport.openSocket(new InetSocketAddress(relay.getHost(),port),true)) {
            if(raw.getLocalPort()!=transport.port())throw new IOException("TCP shared port unavailable");
            var deadline=transport.deadline(raw,2200);
            try {
                Socket socket=raw;
                if(secure) {
                    var factory=(context==null?SSLContext.getDefault():context).getSocketFactory();
                    SSLSocket ssl=(SSLSocket)factory.createSocket(raw,relay.getHost(),port,true);
                    var parameters=ssl.getSSLParameters(); parameters.setEndpointIdentificationAlgorithm("HTTPS"); ssl.setSSLParameters(parameters);
                    ssl.startHandshake(); socket=ssl;
                }
                String host=relay.getHost().contains(":")?"["+relay.getHost()+"]":relay.getHost();
                String request="GET /tcp-discovery HTTP/1.1\r\nHost: "+host+":"+port+"\r\nConnection: close\r\n\r\n";
                socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().flush();
                return response(socket.getInputStream(),!secure);
            } finally { deadline.cancel(false); }
        }
    }
    static InetSocketAddress response(InputStream input,boolean localTest) throws IOException {
        ByteArrayOutputStream header=new ByteArrayOutputStream(); int match=0;
        byte[] end={'\r','\n','\r','\n'};
        while(match<4) {
            int next=input.read(); if(next<0 || header.size()>=4096)throw new IOException("TCP discovery headers");
            header.write(next); match=next==end[match]?match+1:(next=='\r'?1:0);
        }
        String[] lines=header.toString(StandardCharsets.US_ASCII).split("\r\n");
        if(!lines[0].matches("HTTP/1\\.[01] 200(?: .*)?"))throw new IOException("TCP discovery unavailable");
        Map<String,String> headers=new HashMap<>();
        for(int i=1;i<lines.length;i++) {
            int colon=lines[i].indexOf(':'); if(colon<1)continue;
            String key=lines[i].substring(0,colon).toLowerCase(Locale.ROOT);
            if(headers.putIfAbsent(key,lines[i].substring(colon+1).trim())!=null)throw new IOException("Duplicate discovery header");
        }
        if(headers.containsKey("transfer-encoding"))throw new IOException("TCP discovery encoding");
        try {
            int length=Integer.parseInt(headers.getOrDefault("content-length","0"));
            if(length<1 || length>512)throw new IOException("TCP discovery body size");
            byte[] bytes=input.readNBytes(length); if(bytes.length!=length)throw new EOFException();
            var json=Wire.parse(new String(bytes,StandardCharsets.UTF_8));
            if(json.get("protocol").getAsInt()!=1)throw new IOException("TCP discovery protocol");
            return Endpoints.read(json,localTest);
        } catch(RuntimeException e) { throw new IOException("Invalid TCP discovery",e); }
    }
}
