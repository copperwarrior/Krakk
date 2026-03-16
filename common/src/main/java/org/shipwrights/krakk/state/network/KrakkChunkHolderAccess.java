package org.shipwrights.krakk.state.network;

import net.minecraft.core.BlockPos;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.server.level.ServerPlayer;

import java.util.List;

public interface KrakkChunkHolderAccess {
    boolean krakk$damageStateChanged(BlockPos blockPos);

    List<ServerPlayer> krakk$getTrackingPlayers(ChunkPos chunkPos, boolean onlyOnWatchDistanceEdge);
}
