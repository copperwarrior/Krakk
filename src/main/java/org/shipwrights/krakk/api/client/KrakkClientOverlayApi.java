package org.shipwrights.krakk.api.client;

import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.resources.ResourceLocation;

public interface KrakkClientOverlayApi {
    void resetClientState();

    void applyDamage(ResourceLocation dimensionId, long posLong, int damageState);

    void applySection(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates);

    void clearChunk(ResourceLocation dimensionId, int chunkX, int chunkZ);

    float getMiningBaseline(ResourceLocation dimensionId, long posLong);

    long[] consumeDirtySections(ResourceLocation dimensionId);

    Long2ByteOpenHashMap snapshotSection(ResourceLocation dimensionId, long sectionKey);
}
