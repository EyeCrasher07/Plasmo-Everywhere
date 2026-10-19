package dev.eyecrasher.everywhere.fabric.mixin;

import dev.eyecrasher.everywhere.fabric.EverywhereMod;
import net.minecraft.network.Connection;
import su.plo.voice.client.connection.ModClientChannelHandler;
import su.plo.voice.proto.packets.Packet;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.*;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(value = ModClientChannelHandler.class, remap = false)
public abstract class NativeChannelMixin {
    @Inject(method = "receive(Lnet/minecraft/network/Connection;Lsu/plo/voice/proto/packets/Packet;)V", at = @At("HEAD"))
    private void everywhere$native(Connection connection, Packet<?> packet, CallbackInfo ci) {
        EverywhereMod.nativeDetected();
    }
}
