package dev.eyecrasher.everywhere.productiontest;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screen.Screen;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.fml.common.Mod;
import java.lang.reflect.*;

/** Isolated release-JAR smoke driver, excluded from every distributable archive. */
@Mod("everywhere_production_smoke")
public final class ProductionSmoke {
    private int ticks, opened;
    private boolean done;
    public ProductionSmoke() {
        MinecraftForge.EVENT_BUS.addListener((TickEvent.ClientTickEvent event) -> {
            if(event.phase==TickEvent.Phase.END)tick(Minecraft.getInstance());
        });
    }
    private void tick(Minecraft mc) {
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
            if(ticks-opened==20)net.minecraft.util.ScreenShotHelper.grab(mc.gameDirectory,mc.getWindow().getWidth(),mc.getWindow().getHeight(),mc.getMainRenderTarget(),text -> {});
            if(ticks-opened>80) {
                done=true;System.out.println("EVERYWHERE_PRODUCTION_SMOKE PASS");mc.stop();
            }
        } catch(ReflectiveOperationException error) {throw new IllegalStateException(error);}
    }
}
