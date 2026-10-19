package dev.eyecrasher.everywhere.smoke;

import net.minecraft.client.Minecraft;
import org.slf4j.LoggerFactory;
import su.plo.voice.api.addon.ClientAddonsLoader;
import su.plo.voice.api.addon.annotation.Addon;
import su.plo.voice.api.audio.codec.AudioEncoder;
import su.plo.voice.api.client.event.audio.source.AudioSourceWriteEvent;
import su.plo.voice.api.event.EventPriority;
import su.plo.voice.client.ModVoiceClient;
import su.plo.voice.proto.data.audio.capture.VoiceActivation;
import su.plo.voice.proto.packets.udp.serverbound.PlayerAudioPacket;
import java.util.concurrent.atomic.AtomicInteger;

/** Uses synthetic PCM only. Checks actual PV Opus decoding before cancelling speaker output. */
@Addon(id="everywhere_smoke", version="1.0", authors={})
public final class AudioSmoke {
    private int ticks, sent, firstSent;
    private AudioEncoder encoder;
    private final AtomicInteger decoded = new AtomicInteger();
    private boolean finished;
    private boolean keepSending=Boolean.getBoolean("everywhere.smokeKeepSending");
    public AudioSmoke() {
        if(!Boolean.getBoolean("everywhere.audioSmoke"))return;
        ClientAddonsLoader.INSTANCE.load(this);
        net.minecraftforge.common.MinecraftForge.EVENT_BUS.addListener((net.minecraftforge.event.TickEvent.ClientTickEvent event) -> {
            if(event.phase==net.minecraftforge.event.TickEvent.Phase.END)tick(Minecraft.getInstance());
        });
    }
    private void tick(Minecraft mc) {
        if (finished && !keepSending) return;
        if (++ticks > 1600 && !finished) { finish(mc,false,"Timed out before decoded audio"); return; }
        var pv = ModVoiceClient.INSTANCE;
        if (mc.player == null || mc.level == null || pv.getServerInfo().isEmpty() || pv.getServerConnection().isEmpty()) return;
        if (mc.level.players().size() < 2) return;
        try {
            if (encoder == null) {
                encoder = pv.getServerInfo().orElseThrow().createOpusEncoder(false); encoder.open();
                pv.getEventBus().register(this,AudioSourceWriteEvent.class,EventPriority.NORMAL,event -> {
                    if (!event.getSource().getSourceInfo().getAddonId().equals("plasmo_everywhere")) return;
                    long energy=0; for (short s : event.getSamples()) energy+=(long)s*s;
                    if (energy > 1000 && event.getSamples().length==960) decoded.incrementAndGet();
                    event.setCancelled(true);
                });
                firstSent = ticks;
            }
            // Twenty 20ms packets per second are sufficient to test decoder/source initialization.
            short[] samples=new short[960];
            for(int n=0;n<samples.length;n++)samples[n]=(short)(Math.sin((sent*960+n)*Math.PI*2*440/48000)*1000);
            byte[] opus=encoder.encode(samples);
            pv.getServerConnection().orElseThrow().sendPacket(new PlayerAudioPacket(sent++,opus,VoiceActivation.PROXIMITY_ID,(short)32,false));
            if (!finished && decoded.get()>=20 && ticks-firstSent>160) finish(mc,true,"decoded="+decoded.get()+", sent="+sent+", remotePlayers="+(mc.level.players().size()-1));
            if (finished && keepSending && ticks%200==0) LoggerFactory.getLogger("EverywhereSmoke").info("EVERYWHERE_AUDIO_SMOKE LIVE decoded={}, sent={}",decoded.get(),sent);
        } catch(Exception e) { finish(mc,false,e.toString()); }
    }
    private void finish(Minecraft mc,boolean ok,String detail) {
        finished=true; if (!ok)keepSending=false; if(encoder!=null && !keepSending)encoder.close();
        LoggerFactory.getLogger("EverywhereSmoke").info("EVERYWHERE_AUDIO_SMOKE {} {}",ok?"PASS":"FAIL",detail);
        if (!ok) mc.stop();
        // Leave successful clients open until the other client has also completed its check.
    }
}
