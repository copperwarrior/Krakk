package org.shipwrights.krakk.api.client;

import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.resources.ResourceLocation;

/**
 * Client-side damage overlay cache and mining baseline API.
 */
public interface KrakkClientOverlayApi {
    /**
     * Clears all cached client damage overlay state.
     */
    void resetClientState();

    /**
     * Applies a single block damage update.
     */
    void applyDamage(ResourceLocation dimensionId, long posLong, int damageState);

    /**
     * Applies a full section snapshot of damage states.
     */
    void applySection(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates);

    /**
     * Applies a section delta of changed damage states.
     */
    void applySectionDelta(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates);

    /**
     * Clears all cached damage states for a chunk.
     */
    void clearChunk(ResourceLocation dimensionId, int chunkX, int chunkZ);

    /**
     * Returns mining baseline in range {@code [0.0, 1.0]} for client prediction.
     */
    float getMiningBaseline(ResourceLocation dimensionId, long posLong);

    /**
     * Consumes and returns dirty section keys that need cache rebuild.
     */
    long[] consumeDirtySections(ResourceLocation dimensionId);

    /**
     * Returns a copy of currently cached states for one section.
     */
    Long2ByteOpenHashMap snapshotSection(ResourceLocation dimensionId, long sectionKey);

    /**
     * Returns currently known damaged section keys within a chunk range.
     */
    long[] snapshotSectionsInChunkRange(ResourceLocation dimensionId, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ);
}
