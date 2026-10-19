package dev.eyecrasher.everywhere.fabric;

import dev.eyecrasher.everywhere.core.VoiceClient;
import dev.eyecrasher.everywhere.core.ConnectionTrace;
import dev.eyecrasher.everywhere.core.Wire;
import net.fabricmc.loader.api.FabricLoader;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** An explicit allowlist: never serialize a VoiceClient, peer record, URI, or configuration. */
final class DiagnosticReport {
    private static final Set<String> STATES=Set.of("WAITING","CHECKING","FINDING","CONNECTING","CONNECTED","MOVING","RETRYING","NATIVE","DISABLED","DISCONNECTED");
    private static final Set<String> REASONS=Set.of("connection","directory","setup","audio","identity","capacity","verification");
    private static final Set<String> ROUTES=Set.of("LAN","direct-UDP","direct-TCP","VPS-UDP","VPS-WSS");
    private static final Set<String> TCP=Set.of("disabled","not-negotiated","listener-unavailable","ready","connecting","no-candidates","waiting",
            "timeout","no-route","handshake-failed","connect-failed","socket-unavailable");
    record Snapshot(int attempt,String capturedUtc,boolean ready,boolean admitted,boolean verified,String control,
                    VoiceClient.NetworkStats packets,VoiceClient.ConnectionDiagnostics network,List<VoiceClient.PeerDiagnostics> peers) {
        static Snapshot capture(VoiceClient client,int attempt) {
            return new Snapshot(attempt,Instant.now().toString(),client.isReady(),client.sessionId()!=null,client.diagnostics().verified(),
                    client.controlTransport(),client.networkStats(),client.connectionDiagnostics(),List.copyOf(client.peerDiagnostics()));
        }
    }
    private DiagnosticReport() {}
    static void write(Path folder,String state,String reason,int attempt,long restarts,long retained,VoiceClient client,
                      Snapshot last,ConnectionTrace trace,boolean verificationAllowed,boolean direct,boolean udp,long logDropped,boolean logFailed) throws Exception {
        Map<String,String> versions=new LinkedHashMap<>();
        for(String mod:List.of("plasmo_everywhere","minecraft","fabricloader","fabric-api","plasmovoice"))
            versions.put(mod,FabricLoader.getInstance().getModContainer(mod).map(m -> m.getMetadata().getVersion().getFriendlyString()).orElse("unknown"));
        versions.put("java",Integer.toString(Runtime.version().feature()));
        String os=System.getProperty("os.name","").toLowerCase(Locale.ROOT);
        versions.put("platform",os.contains("win")?"windows":os.contains("mac")?"macos":os.contains("linux")?"linux":"other");
        String text=renderDetailed(versions,state,reason,attempt,restarts,retained,
                client==null?null:Snapshot.capture(client,trace.attempts()),last,trace,verificationAllowed,direct,udp,logDropped,logFailed);
        Files.createDirectories(folder);
        Files.writeString(folder.resolve("diagnostic-report.json"),text,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
    }
    static String render(Map<String,String> versions,String state,String reason,int attempt,long restarts,long retained,
                         VoiceClient.NetworkStats packets,VoiceClient.ConnectionDiagnostics network,List<VoiceClient.PeerDiagnostics> peers,
                         boolean ready,boolean verified,String control) {
        Map<String,Object> report=new LinkedHashMap<>();
        report.put("schema",2); report.put("createdUtc",Instant.now().toString());
        Map<String,String> knownVersions=new LinkedHashMap<>();
        for(String key:List.of("plasmo_everywhere","minecraft","fabricloader","fabric-api","plasmovoice","java","platform")) {
            String value=versions.getOrDefault(key,"unknown");
            knownVersions.put(key,value.matches("[A-Za-z0-9_.+\\-]{1,80}")?value:"unknown");
        }
        report.put("versions",knownVersions);
        report.put("state",STATES.contains(state)?state:"WAITING"); report.put("retryReason",REASONS.contains(reason)?reason:"connection");
        report.put("retryAttempt",Math.max(0,attempt)); report.put("sessionsStarted",Math.max(0,restarts)); report.put("worldsRetained",Math.max(0,retained));
        report.put("ready",ready); report.put("accountVerified",verified);
        report.put("controlTransport",Set.of("normal","TLS-record-split","unavailable").contains(control)?control:"unknown");
        if(packets!=null) {
            report.put("paths",Map.of("udp",packets.udpDirect(),"lan",packets.lanDirect(),"tcp",packets.tcpDirect()));
            report.put("outgoingVoicePackets",Map.of("directUdp",packets.directUdpVoicePackets(),"directTcp",packets.directTcpVoicePackets(),"relay",packets.relayedVoicePackets()));
        }
        if(network!=null) {
            Map<String,Object> details=new LinkedHashMap<>();
            details.put("automaticTransports",network.automaticTransports());
            details.put("publicIpv4Candidates",network.publicIPv4()); details.put("publicIpv6Candidates",network.publicIPv6());
            details.put("stunMappings",network.stunMappings()); details.put("mappingVaries",network.mappingVaries());
            details.put("tcpListener",safe(network.tcpListener(),Set.of("disabled","not-negotiated","unavailable","ready")));
            details.put("gateways",network.gateways()); details.put("udpMappings",network.udpMappings());
            details.put("tcpMappings",network.tcpMappings()); details.put("mappingFailures",network.mappingFailures());
            report.put("network",details);
        }
        List<Map<String,Object>> paths=new ArrayList<>();
        for(var peer:peers.stream().limit(24).toList()) {
            Map<String,Object> path=new LinkedHashMap<>();
            path.put("route",ROUTES.contains(peer.route())?peer.route():"unknown");
            path.put("udpCandidates",peer.candidates()); path.put("confirmed",peer.confirmed()); path.put("checks",peer.checks());
            path.put("replies",peer.replies()); path.put("expiredChecks",peer.expiredChecks()); path.put("udpRttMs",peer.udpRttMillis());
            path.put("tcpCandidates",peer.tcpCandidates()); path.put("tcpAttempts",peer.tcpAttempts()); path.put("tcpFailures",peer.tcpFailures());
            path.put("tcpState",TCP.contains(peer.tcpState())?peer.tcpState():"unknown"); paths.add(path);
        }
        report.put("anonymousPeerPaths",paths);
        return Wire.GSON.toJson(report)+System.lineSeparator();
    }
    static String renderDetailed(Map<String,String> versions,String state,String reason,int attempt,long restarts,long retained,
                                 Snapshot current,Snapshot last,ConnectionTrace trace,boolean verificationAllowed,boolean direct,boolean udp,long logDropped,boolean logFailed) {
        var report=Wire.GSON.fromJson(render(versions,state,reason,attempt,restarts,retained,
                current==null?null:current.packets,current==null?null:current.network,current==null?List.of():current.peers,
                current!=null&&current.ready,current!=null&&current.verified,current==null?"unavailable":current.control),com.google.gson.JsonObject.class);
        report.addProperty("accountVerification",current==null?"not-connected":current.admitted?(current.verified?"verified":"unverified"):"pending");
        report.addProperty("accountMode",verificationAllowed?"automatic-verification":"unverified-fallback");
        report.add("options",Wire.GSON.toJsonTree(Map.of("direct",direct,"udp",udp)));
        report.addProperty("connectionAttempts",trace.attempts());
        report.addProperty("traceEventsDropped",trace.dropped());
        report.addProperty("logEventsDropped",Math.max(0,logDropped));
        report.addProperty("logWriteFailed",logFailed);
        report.add("timeline",Wire.GSON.toJsonTree(trace.snapshot()));
        if(last!=null) {
            var previous=Wire.GSON.fromJson(render(Map.of(),"DISCONNECTED","connection",0,0,0,last.packets,last.network,last.peers,
                    last.ready,last.verified,last.control),com.google.gson.JsonObject.class);
            for(String key:List.of("schema","versions","state","retryReason","retryAttempt","sessionsStarted","worldsRetained","createdUtc"))previous.remove(key);
            previous.addProperty("accountVerification",last.admitted?(last.verified?"verified":"unverified"):"pending");
            previous.addProperty("attempt",last.attempt); previous.addProperty("capturedUtc",last.capturedUtc);
            report.add("lastConnection",previous);
        }
        return new com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(report)+System.lineSeparator();
    }
    private static String safe(String value,Set<String> allowed) { return allowed.contains(value)?value:"unknown"; }
}
