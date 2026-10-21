package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.Wire;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.*;

/** Authenticated full mesh. Nodes exchange membership and opaque end-to-end encrypted frames. */
final class Federation implements AutoCloseable {
    static final int MAX_NODES=16, CONTROL_QUEUE=4*1024*1024, VOICE_QUEUE=256*1024;
    record Config(String network,String node,String key,Map<String,URI> peers) {
        Config {
            if(network==null || !network.matches("[a-z0-9_-]{1,64}") || node==null || !node.matches("[a-z0-9_-]{1,32}")
                    || key==null || !key.matches("[a-f0-9]{64}") || peers.size()>=MAX_NODES || peers.containsKey(node))
                throw new IllegalArgumentException("Federation configuration");
            peers=Map.copyOf(peers);
            for(var entry:peers.entrySet()) {
                URI uri=entry.getValue();
                if(!entry.getKey().matches("[a-z0-9_-]{1,32}") || uri.getHost()==null || uri.getUserInfo()!=null
                        || uri.getQuery()!=null || uri.getFragment()!=null || !"/federation".equals(uri.getPath())
                        || uri.getPort()==0 || uri.getPort()>65535 || !("wss".equals(uri.getScheme())
                        || "ws".equals(uri.getScheme()) && "127.0.0.1".equals(uri.getHost())))
                    throw new IllegalArgumentException("Federation peer endpoint");
            }
        }
        static Config read(Path config,Path keyFile) throws Exception {
            if(Files.size(config)>16384 || Files.size(keyFile)>256)throw new IllegalArgumentException("Federation file size");
            JsonObject json=Wire.parse(Files.readString(config)); Map<String,URI> peers=new LinkedHashMap<>();
            JsonArray list=json.getAsJsonArray("peers"); if(list.size()>=MAX_NODES)throw new IllegalArgumentException("Federation node limit");
            for(var value:list) {
                JsonObject peer=value.getAsJsonObject(); String id=Wire.string(peer,"id",32);
                if(peers.putIfAbsent(id,URI.create(Wire.string(peer,"url",512)))!=null)throw new IllegalArgumentException("Duplicate federation node");
            }
            return new Config(Wire.string(json,"network",64),Wire.string(json,"node",32),Files.readString(keyFile).trim(),peers);
        }
        @Override public String toString() { return "Federation.Config[node="+node+", peers="+peers.size()+"]"; }
    }
    private record Link(String node,WebSocket socket,long openedAt) {
        Link(String node,WebSocket socket) { this(node,socket,System.nanoTime()); }
    }
    private final RelayServer relay;
    final Config config;
    private final Map<String,Link> links=new ConcurrentHashMap<>();
    private final Map<WebSocket,Link> incoming=new ConcurrentHashMap<>();
    private final Map<String,WebSocketClient> outbound=new ConcurrentHashMap<>();
    private final Map<String,Long> outboundStarted=new ConcurrentHashMap<>();
    private final Set<String> synchronizedNodes=ConcurrentHashMap.newKeySet();
    private final ScheduledThreadPoolExecutor worker=new ScheduledThreadPoolExecutor(1,r -> {
        Thread t=new Thread(r,"everywhere-federation");t.setDaemon(true);return t;
    });
    private final AtomicLong voiceSent=new AtomicLong(),voiceReceived=new AtomicLong(),voiceDropped=new AtomicLong();
    private volatile boolean closed;

    Federation(RelayServer relay,Config config) {
        this.relay=relay;this.config=config;worker.setRemoveOnCancelPolicy(true);
        worker.scheduleWithFixedDelay(() -> {
            try { connect(); }
            catch(RuntimeException unavailable) { /* A failed dial must not cancel all future retries. */ }
        },0,2,TimeUnit.SECONDS);
    }
    private void connect() {
        if(closed || relay.getPort()<1)return;
        for(var peer:config.peers.entrySet()) {
            long now=System.nanoTime();Link link=links.get(peer.getKey());
            if(link!=null) {
                if(link.socket.isOpen() && (synchronizedNodes.contains(peer.getKey()) || now-link.openedAt<TimeUnit.SECONDS.toNanos(10)))continue;
                disconnected(peer.getKey(),link.socket);link.socket.closeConnection(1001,"Federation synchronization timed out");
            }
            if(config.node.compareTo(peer.getKey())>=0)continue;
            WebSocketClient previous=outbound.get(peer.getKey());
            if(previous!=null && !previous.isClosed()) {
                if(now-outboundStarted.getOrDefault(peer.getKey(),now)<TimeUnit.SECONDS.toNanos(10))continue;
                previous.closeConnection(1001,"Federation connection timed out");
            }
            String id=peer.getKey();
            WebSocketClient client=new WebSocketClient(peer.getValue(),new BoundedRelayDraft(),
                    Map.of("Authorization","Bearer "+config.key,"X-Everywhere-Node",config.node,"X-Everywhere-Network",config.network),3000) {
                @Override public void onOpen(ServerHandshake handshake) { opened(id,getConnection()); }
                @Override public void onMessage(String text) { message(id,getConnection(),text); }
                @Override public void onMessage(ByteBuffer data) { binary(id,getConnection(),data); }
                @Override public void onClose(int code,String reason,boolean remote) { disconnected(id,getConnection()); }
                @Override public void onError(Exception error) { /* Reconnect without logging credentials or endpoints. */ }
            };
            client.setConnectionLostTimeout(5);client.setDaemon(true);outbound.put(id,client);outboundStarted.put(id,now);client.connect();
        }
    }
    boolean accept(WebSocket socket,ClientHandshake handshake) {
        String id=handshake.getFieldValue("X-Everywhere-Node"),network=handshake.getFieldValue("X-Everywhere-Network");
        byte[] expected=("Bearer "+config.key).getBytes(StandardCharsets.US_ASCII);
        String auth=handshake.getFieldValue("Authorization");
        if(closed || !config.network.equals(network) || !config.peers.containsKey(id) || id.compareTo(config.node)>=0
                || auth.length()!=expected.length || !MessageDigest.isEqual(expected,auth.getBytes(StandardCharsets.US_ASCII))
                || links.containsKey(id))return false;
        Link link=new Link(id,socket);incoming.put(socket,link);opened(id,socket);return true;
    }
    private void opened(String node,WebSocket socket) {
        synchronized(relay) {
            if(closed || links.putIfAbsent(node,new Link(node,socket))!=null) { closeLater(socket);return; }
            relay.federationSnapshot(message -> send(node,message));
            send(node,Wire.message("sync"));
        }
    }
    boolean incoming(WebSocket socket) { return incoming.containsKey(socket); }
    void text(WebSocket socket,String text) { Link link=incoming.get(socket);if(link!=null)message(link.node,socket,text); }
    void data(WebSocket socket,ByteBuffer bytes) { Link link=incoming.get(socket);if(link!=null)binary(link.node,socket,bytes); }
    void disconnected(WebSocket socket) {
        Link link=incoming.remove(socket);if(link!=null)disconnected(link.node,socket);
    }
    private void disconnected(String node,WebSocket socket) {
        synchronized(relay) {
            Link link=links.get(node);
            if(link!=null && link.socket==socket && links.remove(node,link)) {
                synchronizedNodes.remove(node);relay.federationDisconnected(node);
            }
        }
    }
    private boolean current(String node,WebSocket socket) {
        Link link=links.get(node);return !closed && link!=null && link.socket==socket && socket.isOpen();
    }
    private void message(String node,WebSocket socket,String text) {
        synchronized(relay) {
            if(!current(node,socket))return;
            try {
                JsonObject message=Wire.parse(text);String type=Wire.string(message,"type",24);
                if(type.equals("sync"))synchronizedNodes.add(node);
                else if(type.equals("member"))relay.federationMember(node,message);
                else if(type.equals("leave"))relay.federationLeave(node,Wire.id(message,"session"));
                else throw new IllegalArgumentException("Federation message type");
            } catch(RuntimeException invalid) { disconnected(node,socket);closeLater(socket); }
        }
    }
    private void binary(String node,WebSocket socket,ByteBuffer data) {
        synchronized(relay) {
            if(!current(node,socket) || !synchronizedNodes.contains(node) || data.remaining()>Wire.MAX_FRAME)return;
            byte[] bytes=new byte[data.remaining()];data.get(bytes);
            try { if(relay.federationFrame(node,Wire.decode(bytes),bytes))voiceReceived.incrementAndGet(); }
            catch(IllegalArgumentException invalid) { /* Bounded malformed media is dropped. */ }
        }
    }
    void broadcast(JsonObject message) { for(String node:links.keySet())send(node,message); }
    private void send(String node,JsonObject message) {
        Link link=links.get(node);if(link==null)return;
        if(!link.socket.isOpen() || queued(link.socket)>=CONTROL_QUEUE) {
            disconnected(node,link.socket);closeLater(link.socket);return;
        }
        try { link.socket.send(Wire.GSON.toJson(message)); }
        catch(RuntimeException stopped) { disconnected(node,link.socket);closeLater(link.socket); }
    }
    boolean forward(String node,byte[] frame) {
        Link link=links.get(node);
        if(link==null || !synchronizedNodes.contains(node) || !link.socket.isOpen() || queued(link.socket)>=VOICE_QUEUE) {
            voiceDropped.incrementAndGet();return false;
        }
        try { link.socket.send(frame);voiceSent.incrementAndGet();return true; }
        catch(RuntimeException stopped) { voiceDropped.incrementAndGet();return false; }
    }
    static long queued(WebSocket socket) {
        if(!(socket instanceof WebSocketImpl implementation))return Long.MAX_VALUE;
        long bytes=0;for(ByteBuffer value:implementation.outQueue) { bytes+=value.remaining();if(bytes>=CONTROL_QUEUE)break; }return bytes;
    }
    private void closeLater(WebSocket socket) {
        if(worker.isShutdown())return;
        worker.execute(() -> socket.close(1008,"Federation connection closed"));
    }
    boolean ready() { return !closed && synchronizedNodes.containsAll(config.peers.keySet()); }
    boolean loopbackOnly() { return config.peers.values().stream().allMatch(uri -> uri.getScheme().equals("ws") && uri.getHost().equals("127.0.0.1")); }
    JsonObject health() {
        JsonObject json=new JsonObject();json.addProperty("protocol",1);json.addProperty("network",config.network);
        json.addProperty("node",config.node);json.addProperty("ready",ready());json.addProperty("links",synchronizedNodes.size());
        JsonArray peers=new JsonArray();synchronizedNodes.stream().sorted().forEach(peers::add);json.add("peers",peers);
        json.addProperty("expectedLinks",config.peers.size());json.addProperty("sentFrames",voiceSent.get());
        json.addProperty("receivedFrames",voiceReceived.get());json.addProperty("droppedFrames",voiceDropped.get());return json;
    }
    @Override public void close() {
        closed=true;worker.shutdownNow();
        synchronized(relay) { for(String node:List.copyOf(links.keySet()))relay.federationDisconnected(node); }
        // Never wait for transport callbacks under the relay state lock.
        for(WebSocketClient client:outbound.values())client.close();
        for(Link link:incoming.values())link.socket.close(1001,"Node stopping");
        links.clear();incoming.clear();synchronizedNodes.clear();
    }
}
