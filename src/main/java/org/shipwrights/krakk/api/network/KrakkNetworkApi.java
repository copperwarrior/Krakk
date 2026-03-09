package org.shipwrights.krakk.api.network;

import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;

public interface KrakkNetworkApi {
    void initClientReceivers();

    void sendDamageSync(ServerLevel level, BlockPos pos, int damageState);

    void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId,
                             int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states);

    void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ);
}
