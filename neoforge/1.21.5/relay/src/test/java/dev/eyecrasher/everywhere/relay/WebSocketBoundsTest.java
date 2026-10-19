package dev.eyecrasher.everywhere.relay;

import dev.eyecrasher.everywhere.core.Wire;
import org.java_websocket.*;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.enums.Opcode;
import org.java_websocket.exceptions.LimitExceededException;
import org.java_websocket.framing.*;
import org.java_websocket.handshake.ServerHandshake;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/** Covers unfinished fragments: a final-fragment-only bound does not protect heap. */
public final class WebSocketBoundsTest {
    public static void main(String[] args) throws Exception {
        List<byte[]> binaries=new ArrayList<>(); List<String> texts=new ArrayList<>();
        WebSocketListener listener=(WebSocketListener)Proxy.newProxyInstance(WebSocketBoundsTest.class.getClassLoader(),
                new Class<?>[]{WebSocketListener.class},(proxy,method,values) -> {
                    if(method.getName().equals("onWebsocketMessage")) {
                        if(values[1] instanceof String text)texts.add(text);
                        else { ByteBuffer b=(ByteBuffer)values[1]; byte[] bytes=new byte[b.remaining()]; b.get(bytes); binaries.add(bytes); }
                    }
                    return null;
                });
        BoundedRelayDraft draft=new BoundedRelayDraft(); WebSocketImpl socket=new WebSocketImpl(listener,draft);
        draft.processFrame(socket,binary(Wire.MAX_TEXT,false));
        try { draft.processFrame(socket,continuation(1,false)); throw new AssertionError("Unfinished aggregate exceeded message limit"); }
        catch(LimitExceededException expected) {}
        // Reset after rejection must release the previous fragmented message.
        draft.processFrame(socket,binary(3,false)); draft.processFrame(socket,new PingFrame());
        draft.processFrame(socket,new PongFrame()); draft.processFrame(socket,continuation(2,true));
        require(binaries.size()==1 && binaries.getFirst().length==5,"Control interleaving or aggregate reset corrupted message");
        draft.processFrame(socket,binary(Wire.MAX_TEXT-1,false)); draft.processFrame(socket,continuation(1,true));
        require(binaries.getLast().length==Wire.MAX_TEXT,"Legal limit-sized fragmented binary rejected");
        TextFrame first=new TextFrame(); first.setFin(false); first.setPayload(ByteBuffer.wrap("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        draft.processFrame(socket,first);
        ContinuousFrame last=continuation(0,true); last.setPayload(ByteBuffer.wrap(" world".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        draft.processFrame(socket,last); require(texts.equals(List.of("hello world")),"Fragmented text changed");
        draft.processFrame(socket,binary(0,false));
        for(int n=1;n<BoundedRelayDraft.MAX_FRAGMENTS;n++)draft.processFrame(socket,continuation(0,false));
        try { draft.processFrame(socket,continuation(0,false)); throw new AssertionError("Empty fragments grew without bound"); }
        catch(LimitExceededException expected) {}
        draft.processFrame(socket,binary(Wire.MAX_TEXT,false));
        require(draft.copyInstance() instanceof BoundedRelayDraft,"Negotiated connection lost its fragment guard");
        BoundedRelayDraft independent=(BoundedRelayDraft)draft.copyInstance();
        independent.processFrame(new WebSocketImpl(listener,independent),binary(1,true));
        draft.reset(); draft.processFrame(socket,binary(1,true));
        verifyUnjoinedNetworkClient();
        System.out.println("WebSocket bounds: unfinished/empty fragment limits, copy/reset, legal text/binary and control interleaving passed");
    }
    private static void verifyUnjoinedNetworkClient() throws Exception {
        try(RelayServer relay=new RelayServer("127.0.0.1",0,0,null)) {
            relay.start(); long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(relay.getPort()==0) { if(System.nanoTime()>until)throw new AssertionError("Fragment server start"); Thread.sleep(5); }
            CompletableFuture<Integer> closed=new CompletableFuture<>();
            WebSocketClient attacker=new WebSocketClient(URI.create("ws://127.0.0.1:"+relay.getPort()+"/voice")) {
                public void onOpen(ServerHandshake handshake) {}
                public void onMessage(String text) {}
                public void onClose(int code,String reason,boolean remote) { closed.complete(code); }
                public void onError(Exception error) { closed.completeExceptionally(error); }
            };
            try {
                require(attacker.connectBlocking(5,TimeUnit.SECONDS),"Fragment client connect");
                attacker.sendFragmentedFrame(Opcode.BINARY,ByteBuffer.allocate(Wire.MAX_TEXT),false);
                attacker.sendFragmentedFrame(Opcode.BINARY,ByteBuffer.allocate(1),false);
                require(closed.get(5,TimeUnit.SECONDS)==1009,"Unjoined fragmented attack was not rejected before final frame");
                require(relay.health().get("status").getAsString().equals("ok"),"Rejected fragments stopped relay");
            } finally { attacker.closeBlocking(); }
        }
    }
    private static BinaryFrame binary(int bytes,boolean last) {
        BinaryFrame frame=new BinaryFrame(); frame.setFin(last); frame.setPayload(ByteBuffer.allocate(bytes)); return frame;
    }
    private static ContinuousFrame continuation(int bytes,boolean last) {
        ContinuousFrame frame=new ContinuousFrame(); frame.setFin(last); frame.setPayload(ByteBuffer.allocate(bytes)); return frame;
    }
    private static void require(boolean condition,String failure) { if(!condition)throw new AssertionError(failure); }
}
