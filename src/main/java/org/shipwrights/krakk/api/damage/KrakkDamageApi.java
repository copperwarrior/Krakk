package org.shipwrights.krakk.api.damage;

import net.minecraft.core.BlockPos;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.level.block.state.BlockState;

public interface KrakkDamageApi {
    KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                  double impactPower, boolean dropOnBreak);

    void clearDamage(ServerLevel level, BlockPos pos);

    int repairDamage(ServerLevel level, BlockPos pos, int repairAmount);

    int getDamageState(ServerLevel level, BlockPos pos);

    float getMiningBaseline(ServerLevel level, BlockPos pos);

    int takeDamageState(ServerLevel level, BlockPos pos);

    int takeStoredDamageState(ServerLevel level, BlockPos pos);

    boolean isLikelyPistonMoveSource(ServerLevel level, BlockPos sourcePos, BlockState sourceState);

    boolean transferLikelyPistonCompletionDamage(ServerLevel level, BlockPos destinationPos, BlockState destinationState);

    void applyTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState, int transferredState);

    KrakkImpactResult accumulateTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState,
                                                       int addedState, boolean dropOnBreak);

    int getMaxDamageState();

    boolean setDamageStateForDebug(ServerLevel level, BlockPos pos, int damageState);

    void queuePlayerSync(ServerPlayer player);

    void clearQueuedPlayerSync(ServerPlayer player);

    void tickQueuedSyncs(MinecraftServer server);

    void syncChunkToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ, boolean loadIfMissing);
}
