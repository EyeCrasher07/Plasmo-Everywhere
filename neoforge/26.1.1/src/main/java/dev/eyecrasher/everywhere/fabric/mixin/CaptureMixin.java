package dev.eyecrasher.everywhere.fabric.mixin;

import su.plo.voice.api.client.connection.ServerInfo;
import su.plo.voice.api.encryption.Encryption;
import su.plo.voice.client.audio.capture.VoiceAudioCapture;
import org.spongepowered.asm.mixin.*;
import org.spongepowered.asm.mixin.injection.*;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(value = VoiceAudioCapture.class, remap = false)
public abstract class CaptureMixin {
    @Shadow private Encryption encryption;
    @Inject(method = "initialize", at = @At("HEAD"))
    private void everywhere$resetCipher(ServerInfo info, CallbackInfo ci) {
        // Native -> fallback -> native must not reuse the preceding session's cipher.
        encryption = null;
    }
}
