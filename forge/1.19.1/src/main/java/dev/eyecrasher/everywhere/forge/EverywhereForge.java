package dev.eyecrasher.everywhere.forge;

import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.fml.DistExecutor;
import net.minecraftforge.fml.common.Mod;

@Mod("plasmo_everywhere")
public final class EverywhereForge {
    public EverywhereForge() {
        DistExecutor.safeRunWhenOn(Dist.CLIENT, () -> EverywhereMod::initialize);
    }
}
