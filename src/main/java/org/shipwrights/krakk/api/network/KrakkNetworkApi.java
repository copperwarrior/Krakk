package org.shipwrights.krakk.api.network;

import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;

/**
 * Network boundary for Krakk damage state replication.
 */
public interface KrakkNetworkApi {
    /**
     * Registers client receivers for Krakk packets.
     */
    void initClientReceivers();

    /**
     * Sends a single block damage update to relevant players.
     */
    void sendDamageSync(ServerLevel level, BlockPos pos, int damageState);

    /**
     * Sends a chunk section snapshot to one player.
     */
    void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId,
                             int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states);

    /**
     * Informs one player that a chunk's cached damage state should be discarded.
     */
    void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ);
}
