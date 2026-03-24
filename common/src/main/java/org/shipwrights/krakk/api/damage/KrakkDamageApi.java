package org.shipwrights.krakk.api.damage;

import net.minecraft.core.BlockPos;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.level.block.state.BlockState;

/**
 * Server-authoritative block damage and mining baseline API.
 */
public interface KrakkDamageApi {
    double DEFAULT_IMPACT_HEAT_CELSIUS = 22.0D;

    /**
     * Applies impact damage to a block and optionally breaks it.
     *
     * @param level server level owning the block
     * @param pos block position
     * @param state state expected at {@code pos} when called
     * @param source source entity causing the impact, may be null depending on caller
     * @param impactPower impact magnitude used by damage curves
     * @param impactHeatCelsius impact thermal magnitude in Celsius; {@link #DEFAULT_IMPACT_HEAT_CELSIUS} is neutral
     * @param dropOnBreak whether to spawn normal drops when a break occurs
     * @param damageType damage pipeline mode used for break/removal behavior
     * @return impact result containing break flag and resulting damage state
     */
    KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                  double impactPower, double impactHeatCelsius,
                                  boolean dropOnBreak, KrakkDamageType damageType);

    /**
     * Applies impact using explicit heat and basic damage mode.
     */
    default KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                          double impactPower, double impactHeatCelsius, boolean dropOnBreak) {
        return applyImpact(level, pos, state, source, impactPower, impactHeatCelsius, dropOnBreak,
                KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    /**
     * Applies impact using basic damage mode.
     */
    default KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                          double impactPower, boolean dropOnBreak) {
        return applyImpact(level, pos, state, source, impactPower, DEFAULT_IMPACT_HEAT_CELSIUS,
                dropOnBreak, KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    /**
     * Applies impact using explicit heat, basic damage mode, and default drop behavior.
     */
    default KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                          double impactPower, double impactHeatCelsius) {
        return applyImpact(level, pos, state, source, impactPower, impactHeatCelsius, true,
                KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    /**
     * Applies impact using basic damage mode and default drop behavior.
     */
    default KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                          double impactPower) {
        return applyImpact(level, pos, state, source, impactPower, DEFAULT_IMPACT_HEAT_CELSIUS,
                true, KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    /**
     * Applies impact with explicit damage type and default heat.
     */
    default KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                          double impactPower, boolean dropOnBreak, KrakkDamageType damageType) {
        return applyImpact(level, pos, state, source, impactPower, DEFAULT_IMPACT_HEAT_CELSIUS,
                dropOnBreak, damageType);
    }

    /**
     * Clears tracked Krakk damage state at a block position.
     */
    void clearDamage(ServerLevel level, BlockPos pos);

    /**
     * Repairs accumulated Krakk damage by a fixed amount.
     *
     * @return amount of damage state actually removed
     */
    int repairDamage(ServerLevel level, BlockPos pos, int repairAmount);

    /**
     * Returns current tracked damage state for a block.
     */
    int getDamageState(ServerLevel level, BlockPos pos);

    /**
     * Returns mining baseline in range {@code [0.0, 1.0]} where higher means
     * closer to instant break.
     */
    float getMiningBaseline(ServerLevel level, BlockPos pos);

    /**
     * Reads and clears damage state from a block.
     */
    int takeDamageState(ServerLevel level, BlockPos pos);

    /**
     * Reads and clears stored state directly from storage, bypassing block validity
     * checks used by the normal take path.
     */
    int takeStoredDamageState(ServerLevel level, BlockPos pos);

    /**
     * Heuristic used to detect piston source positions when a damaged block moves.
     */
    boolean isLikelyPistonMoveSource(ServerLevel level, BlockPos sourcePos, BlockState sourceState);

    /**
     * Attempts to move damage from likely piston source state into destination.
     */
    boolean transferLikelyPistonCompletionDamage(ServerLevel level, BlockPos destinationPos, BlockState destinationState);

    /**
     * Applies transferred damage state to an already moved/replaced block.
     */
    void applyTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState, int transferredState);

    /**
     * Adds transferred damage to existing state and processes break threshold.
     */
    KrakkImpactResult accumulateTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState,
                                                       int addedState, boolean dropOnBreak);

    /**
     * Returns maximum legal damage state value.
     */
    int getMaxDamageState();

    /**
     * Debug helper for direct state assignment.
     *
     * @return true when the target accepted the state update
     */
    boolean setDamageStateForDebug(ServerLevel level, BlockPos pos, int damageState);

    /**
     * Queues delayed full damage sync for a player.
     */
    void queuePlayerSync(ServerPlayer player);

    /**
     * Removes any pending queued sync for a player.
     */
    void clearQueuedPlayerSync(ServerPlayer player);

    /**
     * Processes queued sync work.
     */
    void tickQueuedSyncs(MinecraftServer server);

    /**
     * Sends damage section snapshots for the target chunk to a specific player.
     */
    void syncChunkToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ, boolean loadIfMissing);
}
