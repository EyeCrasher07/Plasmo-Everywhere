package dev.eyecrasher.everywhere.forge;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.ChatScreen;
import com.mojang.blaze3d.platform.InputConstants;
import org.slf4j.LoggerFactory;

/** Development source set only: validates actual widget/render/mixin loading on an isolated display. */
public final class UiSmoke {
    private int ticks, opened;
    private boolean done;
    private final boolean commandMode=Boolean.getBoolean("everywhere.commandUiSmoke");
    private int commandSubmitted;
    public UiSmoke() {
        if(!Boolean.getBoolean("everywhere.uiSmoke") && !commandMode)return;
        net.minecraftforge.common.MinecraftForge.EVENT_BUS.addListener((net.minecraftforge.event.TickEvent.ClientTickEvent event) -> {
            if(event.phase==net.minecraftforge.event.TickEvent.Phase.END)tick(Minecraft.getInstance());
        });
    }
    private void tick(Minecraft mc) {
        if(done)return;
        if(commandMode) { commandTick(mc); return; }
        if(++ticks>1400) { fail(mc,"Client startup timeout"); return; }
        if(mc.getOverlay()!=null || ticks<100)return;
        try {
            if(opened==0) {
                var field=EverywhereMod.class.getDeclaredField("instance"); field.setAccessible(true);
                EverywhereMod owner=(EverywhereMod)field.get(null);
                if(owner==null)throw new AssertionError("Addon not initialized");
                var keyField=EverywhereMod.class.getDeclaredField("settingsKey"); keyField.setAccessible(true);
                if(!((net.minecraft.client.KeyMapping)keyField.get(owner)).isUnbound())throw new AssertionError("Optional key must default to unbound");
                net.minecraft.client.KeyMapping.setAll();
                mc.setScreen(new EverywhereScreen(mc.screen,owner)); opened=ticks;
                var widgets=mc.screen.children().stream().filter(net.minecraft.client.gui.components.AbstractWidget.class::isInstance).toList();
                if(widgets.size()!=4)throw new AssertionError("Expected four settings widgets");
                String notice=owner.saveReport().getString();
                if(notice.contains("plasmo_everywhere.report"))throw new AssertionError("Translations missing");
                LoggerFactory.getLogger("EverywhereUiSmoke").info("EVERYWHERE_UI_SMOKE READY widgets=4 report=saved");
            }
            if(ticks-opened==20)net.minecraft.client.Screenshot.grab(mc.gameDirectory,mc.getMainRenderTarget(),text -> {});
            if(ticks-opened>80) { done=true; LoggerFactory.getLogger("EverywhereUiSmoke").info("EVERYWHERE_UI_SMOKE PASS"); mc.stop(); }
        } catch(Throwable error) { fail(mc,error.getClass().getSimpleName()); }
    }
    private void commandTick(Minecraft mc) {
        if(++ticks>1400) { fail(mc,"Command UI startup timeout"); return; }
        if(mc.getOverlay()!=null || ticks<100 || mc.player==null || mc.getConnection()==null)return;
        try {
            if(commandSubmitted==0) {
                ChatScreen chat=new ChatScreen("/everywhere settings");
                mc.setScreen(chat);
                if(!chat.keyPressed(InputConstants.KEY_RETURN,0,0))
                    throw new AssertionError("Chat did not submit command");
                if(mc.screen!=null)throw new AssertionError("Command must wait until ChatScreen has finished closing");
                commandSubmitted=ticks;
                return;
            }
            if(ticks==commandSubmitted+1) {
                if(!(mc.screen instanceof EverywhereScreen))throw new AssertionError("Settings command did not open on the next tick");
                mc.screen.onClose();
                if(mc.screen!=null)throw new AssertionError("Done returned to the submitted ChatScreen");
                done=true;
                LoggerFactory.getLogger("EverywhereUiSmoke").info("EVERYWHERE_COMMAND_UI_SMOKE PASS command=chat-enter deferred=true parent=none");
                mc.stop();
            }
        } catch(Throwable error) { fail(mc,error.getClass().getSimpleName()); }
    }
    private void fail(Minecraft mc,String label) {
        done=true; LoggerFactory.getLogger("EverywhereUiSmoke").error("EVERYWHERE_UI_SMOKE FAIL {}",label); mc.stop();
    }
}
