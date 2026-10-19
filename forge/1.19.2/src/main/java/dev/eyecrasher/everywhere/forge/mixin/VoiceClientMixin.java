package dev.eyecrasher.everywhere.forge.mixin;

import dev.eyecrasher.everywhere.forge.EverywhereMod;
import su.plo.voice.api.client.connection.ServerConnection;
import su.plo.voice.client.ModVoiceClient;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.*;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;
import java.util.Optional;

@Mixin(value = ModVoiceClient.class, remap = false)
public abstract class VoiceClientMixin {
    @Inject(method = "getServerConnection", at = @At("RETURN"), cancellable = true)
    private void everywhere$connection(CallbackInfoReturnable<Optional<ServerConnection>> cir) {
        if (cir.getReturnValue().isEmpty()) {
            ServerConnection fallback = EverywhereMod.fallbackConnection();
            if (fallback != null) cir.setReturnValue(Optional.of(fallback));
        }
    }
}
