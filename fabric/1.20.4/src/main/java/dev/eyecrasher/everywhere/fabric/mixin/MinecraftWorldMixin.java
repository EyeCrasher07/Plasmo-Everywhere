package dev.eyecrasher.everywhere.fabric.mixin;

import dev.eyecrasher.everywhere.fabric.EverywhereMod;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.gui.screens.Screen;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/** Revoke old-world audio before Minecraft installs or clears its level. */
@Mixin(Minecraft.class)
public abstract class MinecraftWorldMixin {
    @Inject(method = "setLevel", at = @At("HEAD"))
    private void everywhere$levelChanging(ClientLevel level, CallbackInfo ci) {
        EverywhereMod.levelChanging(level);
    }
    @Inject(method = "clearClientLevel(Lnet/minecraft/client/gui/screens/Screen;)V", at = @At("HEAD"))
    private void everywhere$levelClearing(Screen screen, CallbackInfo ci) {
        EverywhereMod.levelChanging(null);
    }
}
