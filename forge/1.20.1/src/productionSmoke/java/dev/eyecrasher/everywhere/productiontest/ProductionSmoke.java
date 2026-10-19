package dev.eyecrasher.everywhere.productiontest;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.Screen;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.fml.common.Mod;
import java.lang.reflect.*;

/** Isolated release-JAR smoke driver, excluded from every distributable archive. */
@su.plo.voice.api.addon.annotation.Addon(id="everywhere_production_smoke",version="1.0",authors={})
@Mod("everywhere_production_smoke")
public final class ProductionSmoke {
    private int ticks, opened;
    private boolean done;
    private su.plo.voice.api.audio.codec.AudioEncoder encoder;
    private int sent, firstSent, finishedAt;
    private final java.util.concurrent.atomic.AtomicInteger decoded=new java.util.concurrent.atomic.AtomicInteger();
    public ProductionSmoke() {
        if(Boolean.getBoolean("everywhere.audioSmoke"))su.plo.voice.api.addon.ClientAddonsLoader.INSTANCE.load(this);
        MinecraftForge.EVENT_BUS.addListener((TickEvent.ClientTickEvent event) -> {
            if(event.phase==TickEvent.Phase.END)tick(Minecraft.getInstance());
        });
    }
    private void tick(Minecraft mc) {
        if(Boolean.getBoolean("everywhere.audioSmoke")) { audioTick(mc);return; }
        if(done)return;
        if(++ticks>1600)throw new AssertionError("Production UI startup timeout");
        if(mc.getOverlay()!=null || ticks<100)return;
        try {
            if(opened==0) {
                Class<?> mod=Class.forName("dev.eyecrasher.everywhere.forge.EverywhereMod");
                Field instance=mod.getDeclaredField("instance");instance.setAccessible(true);
                Object owner=instance.get(null);
                if(owner==null)throw new AssertionError("Release addon not initialized");
                Class<?> type=Class.forName("dev.eyecrasher.everywhere.forge.EverywhereScreen");
                Constructor<?> constructor=type.getDeclaredConstructor(Screen.class,mod);constructor.setAccessible(true);
                mc.setScreen((Screen)constructor.newInstance(mc.screen,owner));
                if(mc.screen.children().size()!=4)throw new AssertionError("Settings widget count");
                Method report=mod.getDeclaredMethod("saveReport");report.setAccessible(true);report.invoke(owner);
                Class.forName("su.plo.voice.client.connection.ModClientChannelHandler");
                Class.forName("su.plo.voice.client.audio.capture.VoiceAudioCapture");
                Class.forName("su.plo.voice.client.ModVoiceClient");
                opened=ticks;
                System.out.println("EVERYWHERE_PRODUCTION_SMOKE READY finalJar=true originalPV=true widgets=4 report=saved");
            }
            if(ticks-opened==20)net.minecraft.client.Screenshot.grab(mc.gameDirectory,mc.getMainRenderTarget(),text -> {});
            if(ticks-opened>80) {
                done=true;System.out.println("EVERYWHERE_PRODUCTION_SMOKE PASS");mc.stop();
            }
        } catch(ReflectiveOperationException error) {throw new IllegalStateException(error);}
    }
    private void audioTick(Minecraft mc) {
        if(++ticks>2000)throw new AssertionError("Production audio timeout: decoded="+decoded.get());
        if(finishedAt>0) { if(ticks-finishedAt>80)mc.stop();return; }
        var pv=su.plo.voice.client.ModVoiceClient.INSTANCE;
        if(mc.player==null || mc.level==null || mc.level.players().size()<2 || pv==null
                || pv.getServerInfo().isEmpty() || pv.getServerConnection().isEmpty())return;
        try {
            if(encoder==null) {
                encoder=pv.getServerInfo().orElseThrow().createOpusEncoder(false);encoder.open();
                pv.getEventBus().register(this,su.plo.voice.api.client.event.audio.source.AudioSourceWriteEvent.class,
                        su.plo.voice.api.event.EventPriority.NORMAL,event -> {
                    if(!event.getSource().getSourceInfo().getAddonId().equals("plasmo_everywhere"))return;
                    long energy=0;for(short sample:event.getSamples())energy+=(long)sample*sample;
                    if(energy>1000 && event.getSamples().length==960)decoded.incrementAndGet();
                    event.setCancelled(true);
                });
                firstSent=ticks;
            }
            short[] samples=new short[960];
            for(int n=0;n<samples.length;n++)samples[n]=(short)(Math.sin((sent*960+n)*Math.PI*2*440/48000)*1000);
            byte[] opus=encoder.encode(samples);
            pv.getServerConnection().orElseThrow().sendPacket(new su.plo.voice.proto.packets.udp.serverbound.PlayerAudioPacket(
                    sent++,opus,su.plo.voice.proto.data.audio.capture.VoiceActivation.PROXIMITY_ID,(short)32,false));
            if(decoded.get()>=30 && ticks-firstSent>160) {
                Class<?> mod=Class.forName("dev.eyecrasher.everywhere.forge.EverywhereMod");
                Field instance=mod.getDeclaredField("instance");instance.setAccessible(true);Object owner=instance.get(null);
                Method report=mod.getDeclaredMethod("saveReport");report.setAccessible(true);report.invoke(owner);
                System.out.println("EVERYWHERE_PRODUCTION_AUDIO PASS finalJar=true originalPV=true decoded="+decoded.get()+" sent="+sent);
                finishedAt=ticks;encoder.close();
            }
        } catch(Exception error) {throw new IllegalStateException(error);}
    }

}
