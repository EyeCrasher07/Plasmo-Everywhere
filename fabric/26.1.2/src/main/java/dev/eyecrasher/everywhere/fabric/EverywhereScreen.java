package dev.eyecrasher.everywhere.fabric;

import net.minecraft.client.gui.GuiGraphicsExtractor;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.components.Tooltip;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;

final class EverywhereScreen extends Screen {
    private final Screen parent;
    private final EverywhereMod owner;
    private Component notice=Component.empty();
    private int top;
    EverywhereScreen(Screen parent,EverywhereMod owner) {
        super(Component.literal("Plasmo Everywhere")); this.parent=parent; this.owner=owner;
    }
    @Override protected void init() {
        top=Math.max(16,height/2-106);
        int x=width/2-105,y=top+66;
        addRenderableWidget(Button.builder(toggle("setting.enabled",owner.enabled()),button -> {
            owner.setEnabled(!owner.enabled()); button.setMessage(toggle("setting.enabled",owner.enabled()));
        }).bounds(x,y,210,20).build());
        addRenderableWidget(Button.builder(EverywhereMod.tr("action.reconnect"),button -> owner.reconnect()).bounds(x,y+24,210,20).build());
        addRenderableWidget(Button.builder(EverywhereMod.tr("action.report"),button -> notice=owner.saveReport()).bounds(x,y+48,210,20)
                .tooltip(Tooltip.create(EverywhereMod.tr("report.privacy"))).build());
        addRenderableWidget(Button.builder(Component.translatable("gui.done"),button -> onClose()).bounds(x,y+72,210,20).build());
    }
    private static Component toggle(String name,boolean value) {
        return EverywhereMod.tr("setting.toggle",EverywhereMod.tr(name),Component.translatable(value?"options.on":"options.off"));
    }
    @Override public void extractRenderState(GuiGraphicsExtractor graphics,int mouseX,int mouseY,float delta) {
        // Minecraft 26.3 extracts the background before this method; a second blur crashes rendering.
        graphics.centeredText(font,title,width/2,top,0xfff3f3fa);
        String status=owner.statusComponent().getString();
        String clipped=font.plainSubstrByWidth(status,Math.max(40,width-28));
        graphics.centeredText(font,clipped.length()<status.length()?clipped+"…":clipped,width/2,top+20,0xffd5d5df);
        graphics.centeredText(font,owner.routeComponent(),width/2,top+34,0xffb1efc6);
        graphics.centeredText(font,owner.accountComponent(),width/2,top+48,0xffb7bbc9);
        super.extractRenderState(graphics,mouseX,mouseY,delta);
        int noticeY=top+162;
        for (var line:font.split(notice,Math.max(40,width-24))) {
            graphics.centeredText(font,line,width/2,noticeY,0xffd5d5df); noticeY+=font.lineHeight;
        }
    }
    @Override public boolean isPauseScreen() { return false; }
    @Override public void onClose() { minecraft.setScreen(parent); }
}
