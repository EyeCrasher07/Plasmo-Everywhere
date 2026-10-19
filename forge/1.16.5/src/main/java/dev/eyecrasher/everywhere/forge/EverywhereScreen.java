package dev.eyecrasher.everywhere.forge;

import com.mojang.blaze3d.matrix.MatrixStack;
import net.minecraft.client.gui.widget.button.Button;
import net.minecraft.client.gui.screen.Screen;
import net.minecraft.util.text.ITextComponent;
import net.minecraft.util.text.StringTextComponent;
import net.minecraft.util.text.TranslationTextComponent;

final class EverywhereScreen extends Screen {
    private final Screen parent;
    private final EverywhereMod owner;
    private ITextComponent notice=new StringTextComponent("");
    private int top;
    private Button report;
    EverywhereScreen(Screen parent,EverywhereMod owner) {
        super(new StringTextComponent("Plasmo Everywhere")); this.parent=parent; this.owner=owner;
    }
    @Override protected void init() {
        top=Math.max(16,height/2-106);
        int x=width/2-105,y=top+66;
        addButton(new Button(x,y,210,20,toggle("setting.enabled",owner.enabled()),button -> {
            owner.setEnabled(!owner.enabled()); button.setMessage(toggle("setting.enabled",owner.enabled()));
        }));
        addButton(new Button(x,y+24,210,20,EverywhereMod.tr("action.reconnect"),button -> owner.reconnect()));
        report=addButton(new Button(x,y+48,210,20,EverywhereMod.tr("action.report"),button -> notice=owner.saveReport()));
        addButton(new Button(x,y+72,210,20,new TranslationTextComponent("gui.done"),button -> onClose()));
    }
    private static ITextComponent toggle(String name,boolean value) {
        return EverywhereMod.tr("setting.toggle",EverywhereMod.tr(name),new TranslationTextComponent(value?"options.on":"options.off"));
    }
    @Override public void render(MatrixStack graphics,int mouseX,int mouseY,float delta) {
        renderBackground(graphics);
        drawCenteredString(graphics,font,title,width/2,top,0xfff3f3fa);
        String status=owner.statusComponent().getString();
        String clipped=font.plainSubstrByWidth(status,Math.max(40,width-28));
        drawCenteredString(graphics,font,clipped.length()<status.length()?clipped+"…":clipped,width/2,top+20,0xffd5d5df);
        drawCenteredString(graphics,font,owner.routeComponent(),width/2,top+34,0xffb1efc6);
        drawCenteredString(graphics,font,owner.accountComponent(),width/2,top+48,0xffb7bbc9);
        super.render(graphics,mouseX,mouseY,delta);
        int noticeY=top+162;
        for(var line:font.split(notice,Math.max(40,width-24))) {
            font.drawShadow(graphics,line,(width-font.width(line))/2.0f,noticeY,0xffd5d5df); noticeY+=font.lineHeight;
        }
        if(report!=null && report.isMouseOver(mouseX,mouseY))renderTooltip(graphics,EverywhereMod.tr("report.privacy"),mouseX,mouseY);
    }
    @Override public boolean isPauseScreen() { return false; }
    @Override public void onClose() { minecraft.setScreen(parent); }
}
