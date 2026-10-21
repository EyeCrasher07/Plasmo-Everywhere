package dev.eyecrasher.everywhere.fabric.mixin;

import net.minecraft.world.level.biome.BiomeManager;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.gen.Accessor;

/** The client already receives this obfuscated biome seed from Minecraft. */
@Mixin(BiomeManager.class)
public interface WorldSeedAccessor {
    @Accessor("biomeZoomSeed") long everywhere$biomeSeed();
}
