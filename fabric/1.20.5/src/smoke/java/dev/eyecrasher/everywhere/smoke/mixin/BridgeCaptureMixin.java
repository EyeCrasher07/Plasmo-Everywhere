package dev.eyecrasher.everywhere.smoke.mixin;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.*;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import su.plo.voice.client.audio.capture.VoiceAudioCapture;
/** Never capture the user's microphone during the synthetic bridge test. */
@Mixin(value=VoiceAudioCapture.class,remap=false)
public abstract class BridgeCaptureMixin {
 @Inject(method="start",at=@At("HEAD"),cancellable=true)
 private void everywhere$syntheticOnly(CallbackInfo ci) { if(Boolean.getBoolean("everywhere.bridgeSmoke"))ci.cancel(); }
}
