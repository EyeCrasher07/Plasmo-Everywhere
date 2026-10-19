package dev.eyecrasher.everywhere.fabric;

import com.mojang.blaze3d.vertex.PoseStack;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.TextComponent;
import net.minecraft.network.chat.TranslatableComponent;

final class EverywhereScreen extends Screen {
    private final Screen parent;
    private final EverywhereMod owner;
    private Component notice=new TextComponent("");
    private int top;
    EverywhereScreen(Screen parent,EverywhereMod owner) {
        super(new TextComponent("Plasmo Everywhere")); this.parent=parent; this.owner=owner;
    }
    @Override protected void init() {
        top=Math.max(16,height/2-106);
        int x=width/2-105,y=top+66;
        addButton(new Button(x,y,210,20,toggle("setting.enabled",owner.enabled()),button -> {
            owner.setEnabled(!owner.enabled()); button.setMessage(toggle("setting.enabled",owner.enabled()));
        }));
        addButton(new Button(x,y+24,210,20,EverywhereMod.tr("action.reconnect"),button -> owner.reconnect()));
        addButton(new Button(x,y+48,210,20,EverywhereMod.tr("action.report"),button -> notice=owner.saveReport(),
                (button,graphics,mouseX,mouseY) -> renderTooltip(graphics,font.split(EverywhereMod.tr("report.privacy"),Math.max(40,width-24)),mouseX,mouseY)));
        addButton(new Button(x,y+72,210,20,new TranslatableComponent("gui.done"),button -> onClose()));
    }
    private static Component toggle(String name,boolean value) {
        return EverywhereMod.tr("setting.toggle",EverywhereMod.tr(name),new TranslatableComponent(value?"options.on":"options.off"));
    }
    @Override public void render(PoseStack graphics,int mouseX,int mouseY,float delta) {
        renderBackground(graphics);
        super.render(graphics,mouseX,mouseY,delta);
        drawCenteredString(graphics,font,title,width/2,top,0xfff3f3fa);
        String status=owner.statusComponent().getString();
        String clipped=font.plainSubstrByWidth(status,Math.max(40,width-28));
        drawCenteredString(graphics,font,clipped.length()<status.length()?clipped+"…":clipped,width/2,top+20,0xffd5d5df);
        drawCenteredString(graphics,font,owner.routeComponent(),width/2,top+34,0xffb1efc6);
        drawCenteredString(graphics,font,owner.accountComponent(),width/2,top+48,0xffb7bbc9);
        int noticeY=top+162;
        for (var line:font.split(notice,Math.max(40,width-24))) {
            font.drawShadow(graphics,line,(width-font.width(line))/2.0f,noticeY,0xffd5d5df); noticeY+=font.lineHeight;
        }
    }
    @Override public boolean isPauseScreen() { return false; }
    @Override public void onClose() { minecraft.setScreen(parent); }
}
