package org.shipwrights.krakk.mixin.common;

import net.minecraft.world.level.chunk.ChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkStorage;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;

@Mixin(ChunkAccess.class)
public abstract class KrakkChunkAccessMixin implements KrakkBlockDamageChunkAccess {
    @Unique
    private final KrakkBlockDamageChunkStorage krakk$blockDamageStorage =
            new KrakkBlockDamageChunkStorage((ChunkAccess) (Object) this);

    @Override
    public KrakkBlockDamageChunkStorage krakk$getBlockDamageStorage() {
        return this.krakk$blockDamageStorage;
    }
}
