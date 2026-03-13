package org.shipwrights.krakk.api.network;

import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;

import java.util.List;

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
     * Sends one single-block damage payload to multiple known recipients.
     */
    void sendDamageSyncBatch(List<ServerPlayer> players, ResourceLocation dimensionId, long posLong, int damageState);

    /**
     * Sends a chunk section snapshot to one player.
     */
    void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId,
                             int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states);

    /**
     * Sends one section snapshot payload to multiple players.
     */
    void sendSectionSnapshotBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                                  int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states);

    /**
     * Sends one section delta payload to multiple known recipients.
     */
    void sendSectionDeltaBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                               int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states);

    /**
     * Informs one player that a chunk's cached damage state should be discarded.
     */
    void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ);
}
