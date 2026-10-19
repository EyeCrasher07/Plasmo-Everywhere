package dev.eyecrasher.everywhere.smoke.mixin;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.*;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import dev.eyecrasher.everywhere.fabric.EverywhereMod;
import net.minecraft.client.Minecraft;
/** Hold an explicitly synthetic bridge fixture outside a multiplayer world. */
@Mixin(value=EverywhereMod.class,remap=false)
public abstract class BridgeOwnerTickMixin {
 @Inject(method="tick",at=@At("HEAD"),cancellable=true)
 private void everywhere$fixtureTick(Minecraft minecraft,CallbackInfo ci) { if(Boolean.getBoolean("everywhere.bridgeSmoke"))ci.cancel(); }
}
