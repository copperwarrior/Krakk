package org.shipwrights.krakk.mixin.common;

import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.server.level.ChunkHolder;
import net.minecraft.server.level.ServerChunkCache;
import net.minecraft.world.level.ChunkPos;
import org.jetbrains.annotations.Nullable;
import org.shipwrights.krakk.state.network.KrakkChunkHolderAccess;
import org.shipwrights.krakk.state.network.KrakkServerChunkCacheAccess;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;

@Mixin(ServerChunkCache.class)
public abstract class KrakkServerChunkCacheMixin implements KrakkServerChunkCacheAccess {
    @Shadow
    @Nullable
    protected abstract ChunkHolder getVisibleChunkIfPresent(long chunkPosLong);

    @Override
    public boolean krakk$damageStateChanged(BlockPos blockPos) {
        int chunkX = SectionPos.blockToSectionCoord(blockPos.getX());
        int chunkZ = SectionPos.blockToSectionCoord(blockPos.getZ());
        ChunkHolder chunkHolder = this.getVisibleChunkIfPresent(ChunkPos.asLong(chunkX, chunkZ));
        if (chunkHolder instanceof KrakkChunkHolderAccess access) {
            return access.krakk$damageStateChanged(blockPos);
        }
        return false;
    }
}
