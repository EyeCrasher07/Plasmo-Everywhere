package dev.eyecrasher.everywhere.fabric;
import dev.eyecrasher.everywhere.core.VoiceClient;
import net.fabricmc.api.ClientModInitializer;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.minecraft.client.Minecraft;
import net.minecraft.network.Connection;
import net.minecraft.network.protocol.PacketFlow;
import org.slf4j.LoggerFactory;
import su.plo.voice.client.ModVoiceClient;
import su.plo.voice.client.connection.ModClientChannelHandler;
import su.plo.voice.api.audio.codec.AudioDecoder;
import su.plo.voice.proto.data.audio.codec.opus.OpusDecoderInfo;
import su.plo.voice.proto.packets.Packet;
import su.plo.voice.proto.packets.tcp.clientbound.LanguagePacket;
import java.net.URI;
import java.util.*;
/** Development-only runtime adapter/codec/source and native-handoff validation. */
public final class BridgeSmoke implements ClientModInitializer {
 private int ticks,step; private boolean done; private PlasmoBridge bridge; private EverywhereMod owner; private UUID source;
 public void onInitializeClient() {
  if(!Boolean.getBoolean("everywhere.bridgeSmoke"))return;
  ClientTickEvents.END_CLIENT_TICK.register(this::tick);
 }
 private void tick(Minecraft mc) {
  if(done)return;
  if(++ticks>1200) { finish(mc,false,"Timeout");return; }
  if(mc.getOverlay()!=null || ticks<100)return;
  try {
   var pv=ModVoiceClient.INSTANCE;
   if(step==0) {
    var f=EverywhereMod.class.getDeclaredField("instance");f.setAccessible(true);owner=(EverywhereMod)f.get(null);
    bridge=new PlasmoBridge(owner,UUID.randomUUID(),PlasmoBridge.prepare(URI.create("wss://127.0.0.1/voice")));
    f=EverywhereMod.class.getDeclaredField("bridge");f.setAccessible(true);f.set(owner,bridge);
    pv.getConfig().getVoice().getDisableInputDevice().set(true);
    bridge.initialize();
    if(pv.getServerInfo().isEmpty() || pv.getServerConnection().orElse(null)!=bridge)throw new AssertionError("Fallback connection mixin");
    var encoder=pv.getServerInfo().orElseThrow().createOpusEncoder(false);encoder.open();
    AudioDecoder decoder=pv.getCodecManager().createDecoder(new su.plo.voice.proto.data.audio.codec.CodecInfo("opus",Map.of()),48000,false,960,1);decoder.open();
    short[] samples=new short[960];for(int n=0;n<samples.length;n++)samples[n]=(short)(Math.sin(n*Math.PI*2*440/48000)*2000);
    byte[] encoded=encoder.encode(samples);short[] decoded=decoder.decode(encoded);encoder.close();decoder.close();
    long energy=0;for(short sample:decoded)energy+=(long)sample*sample;
    if(decoded.length!=960 || energy<1000)throw new AssertionError("Real Opus decoder");
    source=UUID.randomUUID();var peer=new VoiceClient.PeerInfo(source,UUID.randomUUID(),"BridgePeer",false,false);
    bridge.updatePlayers(List.of(peer));bridge.audio(peer,0,32,encoded,false);bridge.audio(peer,1,32,new byte[0],true);
    LoggerFactory.getLogger("EverywhereBridgeSmoke").info("EVERYWHERE_BRIDGE_SMOKE CODEC samples={} bytes={} source={}",decoded.length,encoded.length,source!=null);
    step=ticks;return;
   }
   if(ticks-step<30)return;
   if(pv.getSourceManager().getSourceById(source,false).isEmpty())throw new AssertionError("Player source creation");
   bridge.resetWorld();
   if(!pv.getSourceManager().getSources().isEmpty())throw new AssertionError("Old-world source cleanup");
   var handler=new ModClientChannelHandler(pv);
   var method=ModClientChannelHandler.class.getDeclaredMethod("receive",Connection.class,Packet.class);method.setAccessible(true);
   try { method.invoke(handler,new Connection(PacketFlow.CLIENTBOUND),new LanguagePacket()); }
   catch(java.lang.reflect.InvocationTargetException expected) { /* No native network channel in this fixture. */ }
   var nativeField=EverywhereMod.class.getDeclaredField("nativeVoice");nativeField.setAccessible(true);
   if(!nativeField.getBoolean(owner) || EverywhereMod.fallbackConnection()!=null || pv.getServerInfo().isPresent())throw new AssertionError("Native priority cleanup");
   handler.close();finish(mc,true,"codec=real-Opus source=created world-reset=clean native-hook=receive fallback=closed");
  } catch(Throwable e) { LoggerFactory.getLogger("EverywhereBridgeSmoke").error("Bridge smoke failure",e);finish(mc,false,e.getClass().getSimpleName()); }
 }
 private void finish(Minecraft mc,boolean ok,String detail) { done=true;if(bridge!=null)bridge.close();LoggerFactory.getLogger("EverywhereBridgeSmoke").info("EVERYWHERE_BRIDGE_SMOKE {} {}",ok?"PASS":"FAIL",detail);mc.stop(); }
}
