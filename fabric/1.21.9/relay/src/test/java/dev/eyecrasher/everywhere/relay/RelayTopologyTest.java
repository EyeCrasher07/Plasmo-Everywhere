package dev.eyecrasher.everywhere.relay;

import com.google.gson.*;
import dev.eyecrasher.everywhere.core.*;
import org.java_websocket.*;
import org.java_websocket.drafts.Draft_6455;
import java.lang.reflect.*;
import java.util.*;

/** Deterministic topology/capacity regression; real network throughput is measured separately. */
public final class RelayTopologyTest {
    private static final int CLIENTS=1000, NEARBY=24;
    private static final String ROOM="f".repeat(64);
    private static final Class<?> SESSION;
    private static final Constructor<?> CONSTRUCTOR;
    private static final Method FINISH;
    static {
        try {
            SESSION=Class.forName(RelayServer.class.getName()+"$Session");
            CONSTRUCTOR=SESSION.getDeclaredConstructor(WebSocket.class); CONSTRUCTOR.setAccessible(true);
            FINISH=RelayServer.class.getDeclaredMethod("finishJoin",SESSION,boolean.class); FINISH.setAccessible(true);
        } catch(ReflectiveOperationException error) { throw new ExceptionInInitializerError(error); }
    }
    private static final class Socket extends WebSocketImpl {
        volatile boolean open=true;
        volatile String reason;
        JsonArray peers=new JsonArray();
        int peerMessages;
        Socket(RelayServer server) { super(server,new Draft_6455()); }
        @Override public boolean isOpen() { return open; }
        @Override public void send(String text) {
            JsonObject message=Wire.parse(text);
            if(message.get("type").getAsString().equals("peers")) {
                peers=message.getAsJsonArray("peers"); peerMessages++;
            }
        }
        @Override public void close(int code,String reason) { this.reason=reason; open=false; }
    }
    private record Actor(Socket socket,UUID player) {}
    public static void main(String[] args) throws Exception {
        verifyCrowdedRoom(); verifyFanIn();
        try(RelayServer ignored=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,RelayServer.MAX_SESSIONS,0)) {}
        try {
            new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,RelayServer.MAX_SESSIONS+1,0);
            throw new AssertionError("Unbounded session capacity");
        } catch(IllegalArgumentException expected) {}
        System.out.println("Relay topology: 1000 same-room sessions, bounded neighbor churn, fan-in isolation and complete cleanup passed");
    }
    private static void verifyCrowdedRoom() throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,CLIENTS,0)) {
            List<Actor> actors=actors(server);
            require(metric(server,"peerRefreshes")==CLIENTS,"Joining clients refreshed an entire room");
            require(metric(server,"verifiedConnections")==CLIENTS/2,"Verified accounting at capacity");
            for(int n=0;n<CLIENTS;n++)nearby(server,actors.get(n),group(actors,n));
            require(metric(server,"nearbyDeclarations")==CLIENTS*NEARBY,"Nearby index missing declarations");
            for(Actor actor:actors)require(actor.socket.peers.size()==NEARBY,"Mutual neighborhood lost");
            Actor a=actors.get(0); long before=metric(server,"peerRefreshes");
            int unrelated=actors.get(25).socket.peerMessages;
            nearby(server,a,Set.of());
            require(metric(server,"peerRefreshes")-before==NEARBY+1,"Nearby revocation broadcast to unrelated clients");
            require(a.socket.peers.isEmpty(),"Revoked player's links retained");
            for(int n=1;n<=NEARBY;n++)require(actors.get(n).socket.peers.size()==NEARBY-1,"Peer did not observe revocation");
            require(actors.get(25).socket.peerMessages==unrelated,"Unrelated conversation refreshed");
            nearby(server,a,group(actors,0));
            require(a.socket.peers.size()==NEARBY,"Rediscovery did not restore mutual neighbors");
            before=metric(server,"peerRefreshes");
            candidates(server,a);
            require(metric(server,"peerRefreshes")-before==NEARBY+1,"Candidate update broadcast to entire room");
            require(actors.get(25).socket.peerMessages==unrelated,"Candidate identity leaked to unrelated conversation");
            for(int n=1;n<=NEARBY;n++) {
                JsonObject peer=actors.get(n).socket.peers.asList().stream().map(JsonElement::getAsJsonObject)
                        .filter(p -> p.get("player").getAsString().equals(a.player.toString())).findFirst().orElseThrow();
                require(peer.getAsJsonArray("tcp").size()==1,"Candidate did not reach consenting neighbor");
            }
            Actor excess=actor(server,CLIENTS,false);
            long closeDeadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
            while(excess.socket.isOpen() && System.nanoTime()<closeDeadline)Thread.sleep(1);
            require(!excess.socket.isOpen() && "Voice capacity reached".equals(excess.socket.reason),"1000-session capacity not enforced");
            server.onClose(excess.socket,1008,excess.socket.reason,false);
            require(metric(server,"connections")==CLIENTS,"Capacity rejection displaced an existing player");
            for(Actor value:actors)server.onClose(value.socket,1000,"test complete",false);
            require(metric(server,"connections")==0 && metric(server,"nearbyDeclarations")==0
                    && metric(server,"verifiedConnections")==0,"Session/topology accounting leaked after cleanup");
        }
    }
    private static void verifyFanIn() throws Exception {
        try(RelayServer server=new RelayServer("127.0.0.1",0,"127.0.0.1",0,null,false,CLIENTS,0)) {
            List<Actor> actors=actors(server); Actor subject=actors.get(0);
            for(int n=1;n<CLIENTS;n++)nearby(server,actors.get(n),Set.of(subject.player));
            nearby(server,subject,group(actors,0));
            require(subject.socket.peers.size()==NEARBY,"Fan-in mutual consent");
            int bystander=actors.get(999).socket.peerMessages;
            long before=metric(server,"peerRefreshes"); candidates(server,subject);
            require(metric(server,"peerRefreshes")-before==NEARBY+1,"Unreciprocated fan-in made candidate update unbounded");
            require(actors.get(999).socket.peerMessages==bystander && actors.get(999).socket.peers.isEmpty(),
                    "One-sided nearby declaration received peer identity");
            Set<UUID> moved=new HashSet<>();
            for(int n=25;n<25+NEARBY;n++)moved.add(actors.get(n).player);
            before=metric(server,"peerRefreshes"); nearby(server,subject,moved);
            require(metric(server,"peerRefreshes")-before==2*NEARBY+1,"Neighborhood swap exceeded old/new neighbor bound");
            for(int n=1;n<=NEARBY;n++)require(actors.get(n).socket.peers.isEmpty(),"Old neighbor retained moved player");
            for(int n=25;n<25+NEARBY;n++)require(actors.get(n).socket.peers.size()==1,"New neighbor did not discover moved player");
            before=metric(server,"peerRefreshes"); server.onClose(subject.socket,1000,"test leave",false);
            require(metric(server,"peerRefreshes")-before==NEARBY,"Leave broadcast to unreciprocated fan-in");
            Actor resumed=actor(server,0,true); nearby(server,resumed,group(actors,0));
            require(resumed.socket.peers.size()==NEARBY,"Waiting declarations lost across a same-player reconnect");
            // A late close callback belonging to the old session cannot erase the new session or its index entries.
            server.onClose(subject.socket,1000,"late close",false);
            require(metric(server,"connections")==CLIENTS && resumed.socket.peers.size()==NEARBY,"Late close erased new session");
        }
    }
    private static List<Actor> actors(RelayServer server) throws Exception {
        List<Actor> result=new ArrayList<>();
        for(int n=0;n<CLIENTS;n++)result.add(actor(server,n,n%2==0));
        return result;
    }
    @SuppressWarnings("unchecked")
    private static Actor actor(RelayServer server,int number,boolean verified) throws Exception {
        // Supply admitted sessions to exercise the real topology code without changing production
        // handshake/Mojang rate limits. Transport admission and voice use the separate network suite.
        Socket socket=new Socket(server); Object session=CONSTRUCTOR.newInstance(socket);
        UUID player=new UUID(0,number+1); String name="Scale"+number;
        set(session,"player",player); set(session,"account",player); set(session,"name",name);
        set(session,"room",ROOM); set(session,"dimension","minecraft:overworld"); set(session,"publicKey","test-key");
        set(session,"transports",true);
        Field sockets=RelayServer.class.getDeclaredField("sockets"); sockets.setAccessible(true);
        ((Map<WebSocket,Object>)sockets.get(server)).put(socket,session);
        FINISH.invoke(server,session,verified);
        return new Actor(socket,player);
    }
    private static Set<UUID> group(List<Actor> actors,int member) {
        Set<UUID> players=new HashSet<>(); int start=member/25*25;
        for(int n=start;n<start+25;n++)if(n!=member)players.add(actors.get(n).player);
        return players;
    }
    private static void nearby(RelayServer server,Actor actor,Set<UUID> players) {
        JsonObject message=Wire.message("nearby"); JsonArray ids=new JsonArray();
        players.forEach(player -> ids.add(player.toString())); message.add("players",ids);
        server.onMessage(actor.socket,Wire.GSON.toJson(message));
        require(actor.socket.isOpen(),"Topology update rejected: "+actor.socket.reason);
    }
    private static void candidates(RelayServer server,Actor actor) {
        JsonObject message=Wire.message("candidates"); message.add("candidates",new JsonArray());
        JsonArray tcp=new JsonArray(); JsonObject endpoint=new JsonObject();
        endpoint.addProperty("ip","2606:4700:4700::1111"); endpoint.addProperty("port",24454); tcp.add(endpoint);
        message.add("tcp",tcp); server.onMessage(actor.socket,Wire.GSON.toJson(message));
        require(actor.socket.isOpen(),"Candidate update rejected");
    }
    private static void set(Object object,String name,Object value) throws Exception {
        Field field=SESSION.getDeclaredField(name); field.setAccessible(true); field.set(object,value);
    }
    private static long metric(RelayServer server,String name) { return server.health().get(name).getAsLong(); }
    private static void require(boolean condition,String message) { if(!condition)throw new AssertionError(message); }
}
