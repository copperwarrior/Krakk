package org.shipwrights.krakk.state.network;

import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;

import java.util.List;

public interface KrakkServerChunkCacheAccess {
    boolean krakk$damageStateChanged(BlockPos blockPos);

    List<ServerPlayer> krakk$getTrackingPlayers(int chunkX, int chunkZ, boolean onlyOnWatchDistanceEdge);
}
