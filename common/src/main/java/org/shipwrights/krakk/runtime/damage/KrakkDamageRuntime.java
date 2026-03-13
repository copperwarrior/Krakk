package org.shipwrights.krakk.runtime.damage;

import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.block.FallingBlock;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.LiquidBlock;
import net.minecraft.world.level.block.entity.BlockEntity;
import net.minecraft.world.level.block.piston.MovingPistonBlock;
import net.minecraft.world.level.block.piston.PistonMovingBlockEntity;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.material.FluidState;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.engine.damage.KrakkDamageCurves;
import org.shipwrights.krakk.engine.damage.KrakkDamageDecay;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkStorage;
import org.shipwrights.krakk.state.network.KrakkServerChunkCacheAccess;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public final class KrakkDamageRuntime implements KrakkDamageApi {
    private static final int MAX_DAMAGE_STATE = KrakkDamageCurves.MAX_DAMAGE_STATE;
    private static final int NO_DAMAGE_STATE = -1;
    private static final int FLUID_TO_FLOWING_THRESHOLD = 4;
    private static final int FLUID_REMOVE_THRESHOLD = 9;
    private static final float STONE_EQUIVALENT_HARDNESS = 1.5F;
    private static final float STONE_EQUIVALENT_RESISTANCE = 6.0F;
    private static final long DAMAGE_DECAY_INTERVAL_TICKS = 24_000L;
    private static final int CONNECT_SYNC_DELAY_TICKS = 12;
    private static final int CONNECT_SYNC_PASSES = 1;
    private static final int CONNECT_SYNC_COLUMNS_PER_TICK = 2;
    private static final Map<UUID, PendingSync> PENDING_PLAYER_SYNCS = new HashMap<>();
    private static final ThreadLocal<SyncBatchContext> SYNC_BATCH = new ThreadLocal<>();
    private static volatile DamageStateConversionHandler damageStateConversionHandler = DamageStateConversionHandler.NOOP;

    public KrakkDamageRuntime() {
    }

    @FunctionalInterface
    public interface DamageStateConversionHandler {
        DamageStateConversionHandler NOOP = (level, blockPos, blockState, damageState) -> false;

        boolean apply(ServerLevel level, BlockPos blockPos, BlockState blockState, int damageState);
    }

    public static void setDamageStateConversionHandler(DamageStateConversionHandler handler) {
        damageStateConversionHandler = handler == null ? DamageStateConversionHandler.NOOP : handler;
    }

    public static void runBatchedSync(ServerLevel level, Runnable action) {
        SyncBatchContext existing = SYNC_BATCH.get();
        if (existing != null) {
            if (existing.level == level) {
                existing.depth++;
                try {
                    action.run();
                } finally {
                    existing.depth--;
                    if (existing.depth <= 0) {
                        flushBatchedSync(existing);
                        SYNC_BATCH.remove();
                    }
                }
            } else {
                action.run();
            }
            return;
        }

        SyncBatchContext context = new SyncBatchContext(level);
        SYNC_BATCH.set(context);
        try {
            action.run();
        } finally {
            flushBatchedSync(context);
            SYNC_BATCH.remove();
        }
    }

    public KrakkImpactResult applyImpact(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source, double impactPower) {
        return applyImpact(level, blockPos, blockState, source, impactPower, true);
    }

    @Override
    public KrakkImpactResult applyImpact(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                         double impactPower, boolean dropOnBreak) {
        return applyImpactInternal(level, blockPos, blockState, source, impactPower, dropOnBreak, false);
    }

    public KrakkImpactResult applyImpactPrevalidated(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                                     double impactPower, boolean dropOnBreak) {
        return applyImpactInternal(level, blockPos, blockState, source, impactPower, dropOnBreak, true);
    }

    private KrakkImpactResult applyImpactInternal(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                                  double impactPower, boolean dropOnBreak, boolean prevalidatedState) {
        if (blockState.isAir()) {
            clearDamage(level, blockPos);
            return new KrakkImpactResult(false, NO_DAMAGE_STATE);
        }

        boolean fluidBlock = isDamageableFluidBlock(blockState);
        float hardness = fluidBlock ? STONE_EQUIVALENT_HARDNESS : blockState.getDestroySpeed(level, blockPos);
        if (!fluidBlock && hardness < 0.0F) {
            clearDamage(level, blockPos);
            return new KrakkImpactResult(false, NO_DAMAGE_STATE);
        }
        if (isInstantMineHardness(hardness)) {
            return breakDamagedBlock(level, blockPos, source, dropOnBreak);
        }

        float resistance = fluidBlock ? STONE_EQUIVALENT_RESISTANCE : blockState.getBlock().getExplosionResistance();
        int addedState = computeDamageStateDelta(blockState, impactPower, resistance, hardness);
        int previousState = prevalidatedState ? getDamageStateUnchecked(level, blockPos) : getDamageState(level, blockPos);
        if (addedState <= 0) {
            return new KrakkImpactResult(false, previousState);
        }

        int nextState = clamp(previousState + addedState, 0, MAX_DAMAGE_STATE);

        if (fluidBlock) {
            if (nextState >= FLUID_REMOVE_THRESHOLD) {
                if (level.setBlock(blockPos, Blocks.AIR.defaultBlockState(), 3)) {
                    return new KrakkImpactResult(true, NO_DAMAGE_STATE);
                }
                setDamageState(level, blockPos, nextState);
                return new KrakkImpactResult(false, nextState);
            }

            if (nextState >= FLUID_TO_FLOWING_THRESHOLD && tryConvertSourceFluidToFlowing(level, blockPos, blockState)) {
                // keep damage state; conversion updates fluid behavior while preserving damage progression
            }
        }
        if (nextState >= MAX_DAMAGE_STATE) {
            return breakDamagedBlock(level, blockPos, source, dropOnBreak);
        }

        setDamageState(level, blockPos, nextState);
        return new KrakkImpactResult(false, nextState);
    }

    @Override
    public void clearDamage(ServerLevel level, BlockPos blockPos) {
        ChunkStorageRef ref = getChunkStorage(level, blockPos, false);
        if (ref == null) {
            return;
        }

        long key = blockPos.asLong();
        if (ref.storage().removeDamageState(key) != NO_DAMAGE_STATE) {
            ref.chunk().setUnsaved(true);
            syncDamageState(level, blockPos, 0);
        }
    }

    public float getMiningProgressFraction(ServerLevel level, BlockPos blockPos) {
        int damageState = getDamageState(level, blockPos);
        return KrakkDamageCurves.toMiningBaseline(damageState);
    }

    @Override
    public float getMiningBaseline(ServerLevel level, BlockPos pos) {
        return getMiningProgressFraction(level, pos);
    }

    public int getRawDamageState(ServerLevel level, BlockPos blockPos) {
        ChunkStorageRef ref = getChunkStorage(level, blockPos, false);
        if (ref == null) {
            return 0;
        }
        return ref.storage().getDamageState(blockPos.asLong());
    }

    @Override
    public int getMaxDamageState() {
        return MAX_DAMAGE_STATE;
    }

    @Override
    public boolean setDamageStateForDebug(ServerLevel level, BlockPos blockPos, int damageState) {
        BlockState liveState = level.getBlockState(blockPos);
        if (liveState.isAir() || liveState.getDestroySpeed(level, blockPos) < 0.0F) {
            clearDamage(level, blockPos);
            return false;
        }

        setDamageState(level, blockPos, damageState);
        return true;
    }

    public void queueConnectSync(ServerPlayer player) {
        PENDING_PLAYER_SYNCS.put(player.getUUID(), new PendingSync(CONNECT_SYNC_DELAY_TICKS, CONNECT_SYNC_PASSES));
    }

    @Override
    public void queuePlayerSync(ServerPlayer player) {
        queueConnectSync(player);
    }

    public void clearQueuedSync(ServerPlayer player) {
        PENDING_PLAYER_SYNCS.remove(player.getUUID());
    }

    @Override
    public void clearQueuedPlayerSync(ServerPlayer player) {
        clearQueuedSync(player);
    }

    @Override
    public void tickQueuedSyncs(MinecraftServer server) {
        if (PENDING_PLAYER_SYNCS.isEmpty()) {
            return;
        }

        Iterator<Map.Entry<UUID, PendingSync>> iterator = PENDING_PLAYER_SYNCS.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<UUID, PendingSync> entry = iterator.next();
            PendingSync pending = entry.getValue();
            if (pending.ticksUntilNextStep > 0) {
                pending.ticksUntilNextStep--;
                continue;
            }

            ServerPlayer player = server.getPlayerList().getPlayer(entry.getKey());
            if (player == null || player.connection == null) {
                iterator.remove();
                continue;
            }

            pending.initializeForPlayer(player);
            syncQueuedPlayerStep(player, pending);
            if (pending.passesRemaining <= 0) {
                iterator.remove();
            }
        }
    }

    private void syncQueuedPlayerStep(ServerPlayer player, PendingSync pending) {
        ServerLevel level = player.serverLevel();
        int columnsProcessed = 0;
        while (columnsProcessed < CONNECT_SYNC_COLUMNS_PER_TICK && pending.passesRemaining > 0) {
            syncChunkColumnToPlayer(player, level, pending.cursorChunkX, pending.cursorChunkZ, false);
            columnsProcessed++;
            if (advancePendingChunkCursor(pending)) {
                continue;
            }

            pending.passesRemaining--;
            if (pending.passesRemaining <= 0) {
                return;
            }
            pending.ticksUntilNextStep = CONNECT_SYNC_DELAY_TICKS;
            pending.dimensionId = null;
            pending.initializeForPlayer(player);
            return;
        }
    }

    private static boolean advancePendingChunkCursor(PendingSync pending) {
        if (pending.cursorChunkZ < pending.maxChunkZ) {
            pending.cursorChunkZ++;
            return true;
        }
        if (pending.cursorChunkX < pending.maxChunkX) {
            pending.cursorChunkX++;
            pending.cursorChunkZ = pending.minChunkZ;
            return true;
        }
        return false;
    }

    public void syncAllDamageStatesToPlayer(ServerPlayer player) {
        ServerLevel level = player.serverLevel();
        ChunkPos centerChunk = player.chunkPosition();
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;
        for (int chunkX = centerChunk.x - viewDistance; chunkX <= centerChunk.x + viewDistance; chunkX++) {
            for (int chunkZ = centerChunk.z - viewDistance; chunkZ <= centerChunk.z + viewDistance; chunkZ++) {
                syncChunkColumnToPlayer(player, level, chunkX, chunkZ, false);
            }
        }
    }

    public void syncChunkColumnToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ) {
        syncChunkColumnToPlayer(player, level, chunkX, chunkZ, false);
    }

    public void syncChunkColumnToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ, boolean loadIfMissing) {
        LevelChunk chunk = level.getChunkSource().getChunkNow(chunkX, chunkZ);
        if (chunk == null && loadIfMissing) {
            chunk = level.getChunk(chunkX, chunkZ);
        }
        if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
            return;
        }
        ResourceLocation dimensionId = level.dimension().location();

        access.krakk$getBlockDamageStorage().forEachSectionView((sectionY, states) -> {
            if (states.isEmpty()) {
                return;
            }
            KrakkApi.network().sendSectionSnapshot(
                    player,
                    dimensionId,
                    chunkX,
                    sectionY,
                    chunkZ,
                    states
            );
        });
    }

    @Override
    public void syncChunkToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ, boolean loadIfMissing) {
        syncChunkColumnToPlayer(player, level, chunkX, chunkZ, loadIfMissing);
    }

    @Override
    public int repairDamage(ServerLevel level, BlockPos blockPos, int repairAmount) {
        if (repairAmount <= 0) {
            return 0;
        }

        int currentState = getDamageState(level, blockPos);
        if (currentState <= 0) {
            return 0;
        }

        int nextState = clamp(currentState - repairAmount, 0, MAX_DAMAGE_STATE);
        if (nextState <= 0) {
            clearDamage(level, blockPos);
            return currentState;
        }

        setDamageState(level, blockPos, nextState);
        return currentState - nextState;
    }

    @Override
    public int takeDamageState(ServerLevel level, BlockPos blockPos) {
        int currentState = getDamageState(level, blockPos);
        if (currentState <= 0) {
            return 0;
        }

        clearDamage(level, blockPos);
        return currentState;
    }

    @Override
    public int takeStoredDamageState(ServerLevel level, BlockPos blockPos) {
        ChunkStorageRef ref = getChunkStorage(level, blockPos, false);
        if (ref == null) {
            return 0;
        }

        int removedState = ref.storage().removeDamageState(blockPos.asLong());
        int clampedState = clamp(removedState, 0, MAX_DAMAGE_STATE);
        if (removedState != NO_DAMAGE_STATE) {
            ref.chunk().setUnsaved(true);
            syncDamageState(level, blockPos, 0);
        }
        return clampedState;
    }

    @Override
    public boolean isLikelyPistonMoveSource(ServerLevel level, BlockPos sourcePos, BlockState sourceState) {
        if (sourceState.isAir()) {
            return false;
        }

        for (Direction direction : Direction.values()) {
            BlockPos possibleDestination = sourcePos.relative(direction);
            if (!level.getBlockState(possibleDestination).is(Blocks.MOVING_PISTON)) {
                continue;
            }

            BlockEntity blockEntity = level.getBlockEntity(possibleDestination);
            if (!(blockEntity instanceof PistonMovingBlockEntity movingBlockEntity)) {
                continue;
            }
            if (movingBlockEntity.isSourcePiston()) {
                continue;
            }
            if (movingBlockEntity.getMovementDirection() != direction) {
                continue;
            }

            BlockState movedState = movingBlockEntity.getMovedState();
            if (movedState.isAir()) {
                continue;
            }
            if (movedState.getBlock() != sourceState.getBlock()) {
                continue;
            }
            return true;
        }

        return false;
    }

    @Override
    public boolean transferLikelyPistonCompletionDamage(ServerLevel level, BlockPos destinationPos, BlockState destinationState) {
        if (destinationState.isAir() || destinationState.getDestroySpeed(level, destinationPos) < 0.0F) {
            return false;
        }

        BlockPos bestSourcePos = null;
        int bestDamageState = 0;
        for (Direction direction : Direction.values()) {
            BlockPos sourceCandidate = destinationPos.relative(direction);
            int candidateState = getRawDamageState(level, sourceCandidate);
            if (candidateState <= 0) {
                continue;
            }

            BlockState candidateLiveState = level.getBlockState(sourceCandidate);
            if (!candidateLiveState.isAir() && !(candidateLiveState.getBlock() instanceof MovingPistonBlock)) {
                continue;
            }
            if (candidateState <= bestDamageState) {
                continue;
            }
            bestDamageState = candidateState;
            bestSourcePos = sourceCandidate.immutable();
        }

        if (bestSourcePos == null) {
            return false;
        }

        int carriedState = takeStoredDamageState(level, bestSourcePos);
        if (carriedState <= 0) {
            return false;
        }
        applyTransferredDamageState(level, destinationPos, destinationState, carriedState);
        return true;
    }

    @Override
    public void applyTransferredDamageState(ServerLevel level, BlockPos blockPos, BlockState expectedState, int transferredState) {
        int clampedState = clamp(transferredState, 0, MAX_DAMAGE_STATE);
        if (clampedState <= 0) {
            return;
        }

        BlockState currentState = level.getBlockState(blockPos);
        if (currentState.isAir() || currentState.getDestroySpeed(level, blockPos) < 0.0F) {
            return;
        }
        if (expectedState != null && currentState.getBlock() != expectedState.getBlock()) {
            return;
        }

        int existingState = getDamageState(level, blockPos);
        int mergedState = Math.max(existingState, clampedState);
        if (mergedState > existingState) {
            setDamageState(level, blockPos, mergedState);
        }
    }

    @Override
    public KrakkImpactResult accumulateTransferredDamageState(ServerLevel level, BlockPos blockPos, BlockState expectedState,
                                                              int addedState, boolean dropOnBreak) {
        int clampedAdded = clamp(addedState, 0, MAX_DAMAGE_STATE);
        if (clampedAdded <= 0) {
            return new KrakkImpactResult(false, getDamageState(level, blockPos));
        }

        BlockState currentState = level.getBlockState(blockPos);
        float hardness = currentState.getDestroySpeed(level, blockPos);
        if (currentState.isAir() || hardness < 0.0F) {
            return new KrakkImpactResult(false, NO_DAMAGE_STATE);
        }
        if (expectedState != null && currentState.getBlock() != expectedState.getBlock()) {
            return new KrakkImpactResult(false, NO_DAMAGE_STATE);
        }
        if (isInstantMineHardness(hardness)) {
            return breakDamagedBlock(level, blockPos, null, dropOnBreak);
        }

        int existingState = getDamageState(level, blockPos);
        int nextState = clamp(existingState + clampedAdded, 0, MAX_DAMAGE_STATE);
        if (nextState >= MAX_DAMAGE_STATE) {
            return breakDamagedBlock(level, blockPos, null, dropOnBreak);
        }

        setDamageState(level, blockPos, nextState);
        return new KrakkImpactResult(false, nextState);
    }

    public void moveDamageState(ServerLevel level, BlockPos fromPos, BlockPos toPos, BlockState expectedDestinationState) {
        if (fromPos.equals(toPos)) {
            return;
        }

        int carriedState = takeStoredDamageState(level, fromPos);
        if (carriedState <= 0) {
            return;
        }

        applyTransferredDamageState(level, toPos, expectedDestinationState, carriedState);
    }

    private static int computeDamageStateDelta(BlockState blockState, double impactPower, float resistance, float hardness) {
        boolean isFallingBlock = blockState.getBlock() instanceof FallingBlock;
        return KrakkDamageCurves.computeImpactDamageDelta(isFallingBlock, impactPower, resistance, hardness);
    }

    @Override
    public int getDamageState(ServerLevel level, BlockPos blockPos) {
        BlockState liveState = level.getBlockState(blockPos);
        if (liveState.isAir() || liveState.getDestroySpeed(level, blockPos) < 0.0F) {
            clearDamage(level, blockPos);
            return 0;
        }
        return getDamageStateUnchecked(level, blockPos);
    }

    private int getDamageStateUnchecked(ServerLevel level, BlockPos blockPos) {
        ChunkStorageRef ref = getChunkStorage(level, blockPos, false);
        if (ref == null) {
            return 0;
        }

        long posLong = blockPos.asLong();
        int state = ref.storage().getDamageState(posLong);
        if (state <= 0) {
            return 0;
        }

        long now = level.getGameTime();
        long lastUpdateTick = ref.storage().getLastUpdateTick(posLong);
        if (lastUpdateTick < 0L) {
            if (ref.storage().setLastUpdateTick(posLong, now)) {
                ref.chunk().setUnsaved(true);
            }
            return state;
        }

        long elapsedTicks = now - lastUpdateTick;
        if (elapsedTicks < DAMAGE_DECAY_INTERVAL_TICKS) {
            return state;
        }

        KrakkDamageDecay.DecayResult decayResult = KrakkDamageDecay.applyDecay(state, elapsedTicks, DAMAGE_DECAY_INTERVAL_TICKS);
        int decayedState = clamp(decayResult.state(), 0, MAX_DAMAGE_STATE);
        if (decayedState <= 0) {
            if (ref.storage().removeDamageState(posLong) != NO_DAMAGE_STATE) {
                ref.chunk().setUnsaved(true);
                syncDamageState(level, blockPos, 0);
            }
            return 0;
        }

        long consumedTicks = decayResult.consumedTicks();
        if (ref.storage().setDamageState(posLong, decayedState, lastUpdateTick + consumedTicks)) {
            ref.chunk().setUnsaved(true);
        }
        if (decayedState != state) {
            syncDamageState(level, blockPos, decayedState);
        }
        return decayedState;
    }

    private void setDamageState(ServerLevel level, BlockPos blockPos, int damageState) {
        ChunkStorageRef ref = getChunkStorage(level, blockPos, true);
        if (ref == null) {
            return;
        }

        long posLong = blockPos.asLong();
        int clampedState = clamp(damageState, 0, MAX_DAMAGE_STATE);
        int previousState = ref.storage().getDamageState(posLong);
        if (ref.storage().setDamageState(posLong, clampedState, level.getGameTime())) {
            ref.chunk().setUnsaved(true);
        }
        if (previousState != clampedState) {
            syncDamageState(level, blockPos, clampedState);
        }
        if (clampedState > 0) {
            BlockState liveState = level.getBlockState(blockPos);
            if (!liveState.isAir()) {
                damageStateConversionHandler.apply(level, blockPos, liveState, clampedState);
            }
        }
    }

    private ChunkStorageRef getChunkStorage(ServerLevel level, BlockPos blockPos, boolean loadIfMissing) {
        int chunkX = blockPos.getX() >> 4;
        int chunkZ = blockPos.getZ() >> 4;

        LevelChunk chunk = level.getChunkSource().getChunkNow(chunkX, chunkZ);
        if (chunk == null && loadIfMissing) {
            chunk = level.getChunkAt(blockPos);
        }
        if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
            return null;
        }
        return new ChunkStorageRef(chunk, access.krakk$getBlockDamageStorage());
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static boolean isInstantMineHardness(float hardness) {
        return hardness == 0.0F;
    }

    private static boolean isDamageableFluidBlock(BlockState blockState) {
        if (!(blockState.getBlock() instanceof LiquidBlock)) {
            return false;
        }
        FluidState fluidState = blockState.getFluidState();
        return !fluidState.isEmpty();
    }

    private static boolean tryConvertSourceFluidToFlowing(ServerLevel level, BlockPos blockPos, BlockState blockState) {
        if (!(blockState.getBlock() instanceof LiquidBlock)) {
            return false;
        }
        FluidState fluidState = blockState.getFluidState();
        if (fluidState.isEmpty() || !fluidState.isSource()) {
            return false;
        }
        if (!blockState.hasProperty(LiquidBlock.LEVEL)) {
            return false;
        }

        int currentLevel = blockState.getValue(LiquidBlock.LEVEL);
        if (currentLevel > 0) {
            return false;
        }

        BlockState flowingState = blockState.setValue(LiquidBlock.LEVEL, 1);
        return level.setBlock(blockPos, flowingState, 3);
    }

    private KrakkImpactResult breakDamagedBlock(ServerLevel level, BlockPos blockPos, Entity source, boolean dropOnBreak) {
        boolean broken = level.destroyBlock(blockPos, dropOnBreak, source);
        if (!broken) {
            int fallbackState = MAX_DAMAGE_STATE - 1;
            setDamageState(level, blockPos, fallbackState);
            return new KrakkImpactResult(false, fallbackState);
        }
        return new KrakkImpactResult(true, NO_DAMAGE_STATE);
    }

    private void syncDamageState(ServerLevel level, BlockPos blockPos, int damageState) {
        SyncBatchContext batch = SYNC_BATCH.get();
        if (batch != null && batch.level == level) {
            batch.markDirtyPosition(blockPos, damageState);
            return;
        }
        markDamageStateChanged(level, blockPos, damageState);
    }

    private static void markDamageStateChanged(ServerLevel level, BlockPos blockPos, int damageState) {
        if (level.getChunkSource() instanceof KrakkServerChunkCacheAccess access) {
            access.krakk$damageStateChanged(blockPos);
            return;
        }
        KrakkApi.network().sendDamageSync(level, blockPos, damageState);
    }

    private static void flushBatchedSync(SyncBatchContext context) {
        if (context.dirtyPositions.isEmpty()) {
            return;
        }

        for (Long2ByteMap.Entry entry : context.dirtyPositions.long2ByteEntrySet()) {
            markDamageStateChanged(context.level, BlockPos.of(entry.getLongKey()), entry.getByteValue());
        }
    }

    private static final class SyncBatchContext {
        private final ServerLevel level;
        private final Long2ByteOpenHashMap dirtyPositions = new Long2ByteOpenHashMap();
        private int depth = 1;

        private SyncBatchContext(ServerLevel level) {
            this.level = level;
        }

        private void markDirtyPosition(BlockPos pos, int damageState) {
            this.dirtyPositions.put(pos.asLong(), (byte) clamp(damageState, 0, MAX_DAMAGE_STATE));
        }
    }

    private record ChunkStorageRef(LevelChunk chunk, KrakkBlockDamageChunkStorage storage) {
    }

    private static final class PendingSync {
        private int ticksUntilNextStep;
        private int passesRemaining;
        private ResourceLocation dimensionId;
        private int minChunkX;
        private int maxChunkX;
        private int minChunkZ;
        private int maxChunkZ;
        private int cursorChunkX;
        private int cursorChunkZ;

        private PendingSync(int ticksUntilNextStep, int passesRemaining) {
            this.ticksUntilNextStep = ticksUntilNextStep;
            this.passesRemaining = passesRemaining;
        }

        private void initializeForPlayer(ServerPlayer player) {
            ServerLevel level = player.serverLevel();
            ResourceLocation currentDimensionId = level.dimension().location();
            if (currentDimensionId.equals(this.dimensionId)) {
                return;
            }

            ChunkPos centerChunk = player.chunkPosition();
            int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;
            this.minChunkX = centerChunk.x - viewDistance;
            this.maxChunkX = centerChunk.x + viewDistance;
            this.minChunkZ = centerChunk.z - viewDistance;
            this.maxChunkZ = centerChunk.z + viewDistance;
            this.cursorChunkX = this.minChunkX;
            this.cursorChunkZ = this.minChunkZ;
            this.dimensionId = currentDimensionId;
        }
    }

}
