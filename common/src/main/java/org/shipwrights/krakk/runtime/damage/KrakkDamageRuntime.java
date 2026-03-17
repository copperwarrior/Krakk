package org.shipwrights.krakk.runtime.damage;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.core.SectionPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ChunkHolder;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Explosion;
import net.minecraft.world.level.block.FallingBlock;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.LiquidBlock;
import net.minecraft.world.level.block.entity.BlockEntity;
import net.minecraft.world.level.block.piston.MovingPistonBlock;
import net.minecraft.world.level.block.piston.PistonMovingBlockEntity;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import net.minecraft.world.level.material.FluidState;
import net.minecraft.world.level.storage.loot.LootParams;
import net.minecraft.world.level.storage.loot.parameters.LootContextParams;
import net.minecraft.world.phys.Vec3;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.api.damage.KrakkDamageType;
import org.shipwrights.krakk.engine.damage.KrakkDamageCurves;
import org.shipwrights.krakk.engine.damage.KrakkDamageDecay;
import org.shipwrights.krakk.api.network.KrakkNetworkApi;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkStorage;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageSectionAccess;
import org.shipwrights.krakk.state.network.KrakkServerChunkCacheAccess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.LongAdder;
import java.lang.reflect.Method;
import org.slf4j.Logger;

public final class KrakkDamageRuntime implements KrakkDamageApi {
    private static final Logger LOGGER = LogUtils.getLogger();
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
    private static final int DEFAULT_BULK_SYNC_PER_BLOCK_LIMIT = 0;
    private static final int DEFAULT_BULK_SYNC_SECTION_SNAPSHOT_LIMIT = 512;
    private static final int DEFAULT_BULK_SYNC_SNAPSHOT_PARALLELISM = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() - 1));
    private static final int DEFAULT_BULK_SYNC_SNAPSHOT_PARALLEL_MIN_SECTIONS = 12;
    private static volatile int bulkSyncPerBlockLimit =
            parseNonNegativeIntProperty("krakk.damage.bulk_sync.per_block_limit", DEFAULT_BULK_SYNC_PER_BLOCK_LIMIT);
    private static volatile int bulkSyncSectionSnapshotLimit =
            parsePositiveIntProperty("krakk.damage.bulk_sync.section_snapshot_limit", DEFAULT_BULK_SYNC_SECTION_SNAPSHOT_LIMIT);
    private static volatile int bulkSyncSnapshotParallelMinSections =
            parsePositiveIntProperty("krakk.damage.bulk_sync.snapshot_parallel_min_sections", DEFAULT_BULK_SYNC_SNAPSHOT_PARALLEL_MIN_SECTIONS);
    private static volatile int bulkSyncSnapshotParallelism =
            parsePositiveIntProperty("krakk.damage.bulk_sync.snapshot_parallelism", DEFAULT_BULK_SYNC_SNAPSHOT_PARALLELISM);
    private static volatile ForkJoinPool bulkSyncSnapshotPool = createSnapshotPool();
    private static final Map<Class<?>, Method> CHUNK_SOURCE_NOTIFY_METHODS = new ConcurrentHashMap<>();
    private static final Set<Class<?>> CHUNK_SOURCE_NO_NOTIFY_METHOD = ConcurrentHashMap.newKeySet();
    private static final Map<Class<?>, Method> CHUNK_SOURCE_VISIBLE_CHUNK_METHODS = new ConcurrentHashMap<>();
    private static final Set<Class<?>> CHUNK_SOURCE_NO_VISIBLE_CHUNK_METHOD = ConcurrentHashMap.newKeySet();
    private static final Map<Class<?>, Method> CHUNK_HOLDER_NOTIFY_METHODS = new ConcurrentHashMap<>();
    private static final Set<Class<?>> CHUNK_HOLDER_NO_NOTIFY_METHOD = ConcurrentHashMap.newKeySet();
    private static final Map<UUID, PendingSync> PENDING_PLAYER_SYNCS = new HashMap<>();
    private static final ThreadLocal<SyncBatchContext> SYNC_BATCH = new ThreadLocal<>();
    private static final ThreadLocal<SyncBatchContext> BULK_SYNC = new ThreadLocal<>();
    private static final ThreadLocal<Integer> SYNC_SUPPRESSION_DEPTH = ThreadLocal.withInitial(() -> 0);
    private static final DamageRuntimeProfileCounters DAMAGE_RUNTIME_PROFILE = new DamageRuntimeProfileCounters();
    private static volatile boolean damageRuntimeProfilingEnabled = false;
    private static volatile DamageStateConversionHandler damageStateConversionHandler = DamageStateConversionHandler.NOOP;
    private static volatile boolean syncDebugLoggingEnabled =
            parseBooleanProperty("krakk.damage.sync_debug_logging", false);
    private static volatile boolean chunkNotifyPacketMirrorEnabled =
            parseBooleanProperty("krakk.damage.sync.chunk_notify_packet_mirror", false);
    private static final LongAdder SYNC_DEBUG_CHUNK_TRACK_SNAPSHOTS = new LongAdder();
    private static final LongAdder SYNC_DEBUG_CHUNK_NOTIFY_SUCCESS = new LongAdder();
    private static final LongAdder SYNC_DEBUG_CHUNK_NOTIFY_FAIL = new LongAdder();
    private static final LongAdder SYNC_DEBUG_PACKET_FALLBACKS = new LongAdder();
    private static final LongAdder SYNC_DEBUG_RECOVERY_RESYNC_REQUESTS = new LongAdder();
    private static final LongAdder SYNC_DEBUG_RECOVERY_RESYNC_SERVED = new LongAdder();

    public KrakkDamageRuntime() {
        if (syncDebugLoggingEnabled) {
            LOGGER.info("Krakk damage sync debug logging is ENABLED");
        }
    }

    @FunctionalInterface
    public interface DamageStateConversionHandler {
        DamageStateConversionHandler NOOP = (level, blockPos, blockState, damageState) -> false;

        boolean apply(ServerLevel level, BlockPos blockPos, BlockState blockState, int damageState);
    }

    @FunctionalInterface
    public interface BulkDebugProgressListener {
        void onProgress(int processed, int total, int applied, int failed);
    }

    public record BulkDebugApplyResult(int attempted, int applied, int failed) {
    }

    public record BulkDebugClearResult(int attempted, int cleared) {
    }

    public record SyncDebugCountersSnapshot(long chunkTrackSnapshots,
                                            long chunkNotifySuccess,
                                            long chunkNotifyFail,
                                            long packetFallbacks,
                                            long recoveryResyncRequests,
                                            long recoveryResyncServed) {
    }

    public static void setDamageStateConversionHandler(DamageStateConversionHandler handler) {
        damageStateConversionHandler = handler == null ? DamageStateConversionHandler.NOOP : handler;
    }

    public static void beginRuntimeProfiling() {
        DAMAGE_RUNTIME_PROFILE.reset();
        damageRuntimeProfilingEnabled = true;
    }

    public static DamageRuntimeProfileSnapshot endRuntimeProfiling() {
        damageRuntimeProfilingEnabled = false;
        return DAMAGE_RUNTIME_PROFILE.snapshot();
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

    public static void beginBulkSync(ServerLevel level) {
        SyncBatchContext existing = BULK_SYNC.get();
        if (existing != null) {
            if (existing.level == level) {
                existing.depth++;
            }
            return;
        }
        BULK_SYNC.set(new SyncBatchContext(level));
    }

    public static void endBulkSync() {
        SyncBatchContext context = BULK_SYNC.get();
        if (context == null) {
            return;
        }
        context.depth--;
        if (context.depth <= 0) {
            flushBatchedSync(context);
            BULK_SYNC.remove();
        }
    }

    public static void runInBulkSync(ServerLevel level, Runnable action) {
        beginBulkSync(level);
        try {
            action.run();
        } finally {
            endBulkSync();
        }
    }

    public static void runWithoutSync(Runnable action) {
        int depth = SYNC_SUPPRESSION_DEPTH.get();
        SYNC_SUPPRESSION_DEPTH.set(depth + 1);
        try {
            action.run();
        } finally {
            int nextDepth = SYNC_SUPPRESSION_DEPTH.get() - 1;
            if (nextDepth <= 0) {
                SYNC_SUPPRESSION_DEPTH.remove();
            } else {
                SYNC_SUPPRESSION_DEPTH.set(nextDepth);
            }
        }
    }

    private static boolean isSyncSuppressed() {
        return SYNC_SUPPRESSION_DEPTH.get() > 0;
    }

    public static void configureBulkSyncFanout(int perBlockLimit, int sectionSnapshotLimit) {
        bulkSyncPerBlockLimit = Math.max(0, perBlockLimit);
        bulkSyncSectionSnapshotLimit = Math.max(1, sectionSnapshotLimit);
    }

    public static void configureBulkSyncSnapshotParallelism(int parallelism, int minSections) {
        bulkSyncSnapshotParallelism = Math.max(1, parallelism);
        bulkSyncSnapshotParallelMinSections = Math.max(1, minSections);
        ForkJoinPool updated = createSnapshotPool();
        ForkJoinPool previous = bulkSyncSnapshotPool;
        bulkSyncSnapshotPool = updated;
        if (previous != null && previous != updated) {
            previous.shutdown();
        }
    }

    public static void setSyncDebugLoggingEnabled(boolean enabled) {
        syncDebugLoggingEnabled = enabled;
    }

    public static boolean isSyncDebugLoggingEnabled() {
        return syncDebugLoggingEnabled;
    }

    public static void recordChunkTrackSnapshotSent(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        SYNC_DEBUG_CHUNK_TRACK_SNAPSHOTS.increment();
        if (syncDebugLoggingEnabled) {
            LOGGER.debug(
                    "Krakk sync chunk-track snapshot: player={} dim={} chunk=({}, {})",
                    player.getGameProfile().getName(),
                    dimensionId,
                    chunkX,
                    chunkZ
            );
        }
    }

    public static void recordRecoveryResyncRequested(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        SYNC_DEBUG_RECOVERY_RESYNC_REQUESTS.increment();
        if (syncDebugLoggingEnabled) {
            LOGGER.debug(
                    "Krakk sync recovery request: player={} dim={} chunk=({}, {})",
                    player.getGameProfile().getName(),
                    dimensionId,
                    chunkX,
                    chunkZ
            );
        }
    }

    public static void recordRecoveryResyncServed(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        SYNC_DEBUG_RECOVERY_RESYNC_SERVED.increment();
        if (syncDebugLoggingEnabled) {
            LOGGER.debug(
                    "Krakk sync recovery served: player={} dim={} chunk=({}, {})",
                    player.getGameProfile().getName(),
                    dimensionId,
                    chunkX,
                    chunkZ
            );
        }
    }

    public static SyncDebugCountersSnapshot getSyncDebugCountersSnapshot() {
        return new SyncDebugCountersSnapshot(
                SYNC_DEBUG_CHUNK_TRACK_SNAPSHOTS.sum(),
                SYNC_DEBUG_CHUNK_NOTIFY_SUCCESS.sum(),
                SYNC_DEBUG_CHUNK_NOTIFY_FAIL.sum(),
                SYNC_DEBUG_PACKET_FALLBACKS.sum(),
                SYNC_DEBUG_RECOVERY_RESYNC_REQUESTS.sum(),
                SYNC_DEBUG_RECOVERY_RESYNC_SERVED.sum()
        );
    }

    public KrakkImpactResult applyImpact(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source, double impactPower) {
        return applyImpact(level, blockPos, blockState, source, impactPower, true, KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    @Override
    public KrakkImpactResult applyImpact(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                         double impactPower, boolean dropOnBreak) {
        return applyImpact(level, blockPos, blockState, source, impactPower, dropOnBreak, KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    @Override
    public KrakkImpactResult applyImpact(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                         double impactPower, boolean dropOnBreak, KrakkDamageType damageType) {
        return applyImpactInternal(level, blockPos, blockState, source, impactPower, dropOnBreak, false, damageType);
    }

    public KrakkImpactResult applyImpactPrevalidated(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                                     double impactPower, boolean dropOnBreak) {
        return applyImpactPrevalidated(level, blockPos, blockState, source, impactPower, dropOnBreak, KrakkDamageType.KRAKK_DAMAGE_BASIC);
    }

    public KrakkImpactResult applyImpactPrevalidated(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                                     double impactPower, boolean dropOnBreak, KrakkDamageType damageType) {
        return applyImpactInternal(level, blockPos, blockState, source, impactPower, dropOnBreak, true, damageType);
    }

    private KrakkImpactResult applyImpactInternal(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                                  double impactPower, boolean dropOnBreak, boolean prevalidatedState,
                                                  KrakkDamageType damageType) {
        KrakkDamageType resolvedDamageType = damageType == null
                ? KrakkDamageType.KRAKK_DAMAGE_BASIC
                : damageType;
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
            return breakDamagedBlock(level, blockPos, blockState, source, dropOnBreak, resolvedDamageType);
        }

        float resistance = fluidBlock ? STONE_EQUIVALENT_RESISTANCE : blockState.getBlock().getExplosionResistance();
        int addedState = computeDamageStateDelta(blockState, impactPower, resistance, hardness);
        if (addedState <= 0) {
            int previousState = getRawDamageState(level, blockPos);
            return new KrakkImpactResult(false, previousState);
        }

        int previousState = getRawDamageState(level, blockPos);
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
            return breakDamagedBlock(level, blockPos, blockState, source, dropOnBreak, resolvedDamageType);
        }

        setDamageState(level, blockPos, nextState);
        return new KrakkImpactResult(false, nextState);
    }

    @Override
    public void clearDamage(ServerLevel level, BlockPos blockPos) {
        boolean profile = damageRuntimeProfilingEnabled;
        long methodStart = profile ? System.nanoTime() : 0L;
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.clearCalls.increment();
        }

        long lookupStart = profile ? System.nanoTime() : 0L;
        ChunkStorageRef ref = getChunkStorage(level, blockPos, false);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.clearLookupNanos.add(System.nanoTime() - lookupStart);
        }
        if (ref == null) {
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.clearLookupMisses.increment();
                DAMAGE_RUNTIME_PROFILE.clearMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }

        long key = blockPos.asLong();
        long removeStart = profile ? System.nanoTime() : 0L;
        int removed = ref.storage().removeDamageState(key);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.clearRemoveNanos.add(System.nanoTime() - removeStart);
        }
        if (removed != NO_DAMAGE_STATE) {
            recordSourceMutation(level, removed, 0, "clearDamage");
            ref.chunk().setUnsaved(true);
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.clearChanged.increment();
            }
            syncDamageState(level, blockPos, 0);
        }
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.clearMethodNanos.add(System.nanoTime() - methodStart);
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

    /**
     * Re-emits existing damage states at {@code origin} and its 6 neighboring blocks.
     * This keeps damage overlays in sync when adjacent block states change and face culling/light context shifts.
     */
    public void refreshAdjacentDamageStates(ServerLevel level, BlockPos origin) {
        if (level == null || origin == null) {
            return;
        }

        int originState = resolveDamageStateForRefresh(level, origin);
        if (originState > 0) {
            syncDamageState(level, origin, originState);
        }

        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
        for (Direction direction : Direction.values()) {
            cursor.setWithOffset(origin, direction);
            int adjacentState = resolveDamageStateForRefresh(level, cursor);
            if (adjacentState > 0) {
                syncDamageState(level, cursor, adjacentState);
            }
        }
    }

    private int resolveDamageStateForRefresh(ServerLevel level, BlockPos blockPos) {
        int state = getRawDamageState(level, blockPos);
        if (state > 0) {
            return state;
        }
        return getMirroredSectionDamageState(level, blockPos);
    }

    private static int getMirroredSectionDamageState(ServerLevel level, BlockPos blockPos) {
        int sectionX = SectionPos.blockToSectionCoord(blockPos.getX());
        int sectionY = SectionPos.blockToSectionCoord(blockPos.getY());
        int sectionZ = SectionPos.blockToSectionCoord(blockPos.getZ());

        LevelChunk chunk = level.getChunkSource().getChunkNow(sectionX, sectionZ);
        if (chunk == null) {
            return 0;
        }

        int sectionIndex = sectionY - (chunk.getMinBuildHeight() >> 4);
        LevelChunkSection[] sections = chunk.getSections();
        if (sectionIndex < 0 || sectionIndex >= sections.length) {
            return 0;
        }

        LevelChunkSection section = sections[sectionIndex];
        if (!(section instanceof KrakkBlockDamageSectionAccess access)) {
            return 0;
        }

        short localIndex = (short) (((blockPos.getY() & 15) << 8) | ((blockPos.getZ() & 15) << 4) | (blockPos.getX() & 15));
        return Math.max(0, Math.min(MAX_DAMAGE_STATE, access.krakk$getDamageStates().get(localIndex)));
    }

    @Override
    public int getMaxDamageState() {
        return MAX_DAMAGE_STATE;
    }

    @Override
    public boolean setDamageStateForDebug(ServerLevel level, BlockPos blockPos, int damageState) {
        boolean profile = damageRuntimeProfilingEnabled;
        long methodStart = profile ? System.nanoTime() : 0L;
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.debugCalls.increment();
        }

        long liveCheckStart = profile ? System.nanoTime() : 0L;
        BlockState liveState = level.getBlockState(blockPos);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.debugLiveCheckNanos.add(System.nanoTime() - liveCheckStart);
        }
        if (liveState.isAir() || liveState.getDestroySpeed(level, blockPos) < 0.0F) {
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.debugRejected.increment();
            }
            clearDamage(level, blockPos);
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.debugMethodNanos.add(System.nanoTime() - methodStart);
            }
            return false;
        }

        setDamageState(level, blockPos, damageState);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.debugApplied.increment();
            DAMAGE_RUNTIME_PROFILE.debugMethodNanos.add(System.nanoTime() - methodStart);
        }
        return true;
    }

    public BulkDebugApplyResult setDamageStatesForDebugBulk(ServerLevel level, LongArrayList positions,
                                                            IntArrayList states, int startOffset,
                                                            int progressInterval,
                                                            BulkDebugProgressListener progressListener) {
        int attempted = Math.min(positions.size(), states.size());
        if (attempted <= 0) {
            return new BulkDebugApplyResult(0, 0, 0);
        }

        Long2ObjectOpenHashMap<SectionWriteBucket> bySection = new Long2ObjectOpenHashMap<>();
        int normalizedOffset = Math.floorMod(startOffset, attempted);
        for (int i = 0; i < attempted; i++) {
            int index = normalizedOffset + i;
            if (index >= attempted) {
                index -= attempted;
            }

            long posLong = positions.getLong(index);
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            long sectionKey = SectionPos.asLong(x >> 4, y >> 4, z >> 4);
            SectionWriteBucket bucket = bySection.computeIfAbsent(sectionKey, unused -> new SectionWriteBucket());
            bucket.positions.add(posLong);
            bucket.states.add(states.getInt(index));
        }

        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
        int applied = 0;
        int failed = 0;
        int processed = 0;

        for (Long2ObjectOpenHashMap.Entry<SectionWriteBucket> entry : bySection.long2ObjectEntrySet()) {
            long sectionKey = entry.getLongKey();
            int sectionX = SectionPos.x(sectionKey);
            int sectionY = SectionPos.y(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);

            LevelChunk chunk = level.getChunkSource().getChunkNow(sectionX, sectionZ);
            if (chunk == null) {
                chunk = level.getChunkAt(new BlockPos(sectionX << 4, sectionY << 4, sectionZ << 4));
            }
            if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
                int sectionSize = entry.getValue().positions.size();
                failed += sectionSize;
                processed += sectionSize;
                if (progressListener != null && progressInterval > 0 && (processed % progressInterval) == 0) {
                    progressListener.onProgress(processed, attempted, applied, failed);
                }
                continue;
            }

            ChunkStorageRef ref = new ChunkStorageRef(chunk, access.krakk$getBlockDamageStorage());
            SectionWriteBucket bucket = entry.getValue();
            int sectionSize = bucket.positions.size();
            for (int i = 0; i < sectionSize; i++) {
                long posLong = bucket.positions.getLong(i);
                cursor.set(BlockPos.getX(posLong), BlockPos.getY(posLong), BlockPos.getZ(posLong));

                boolean profile = damageRuntimeProfilingEnabled;
                long methodStart = profile ? System.nanoTime() : 0L;
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.debugCalls.increment();
                }

                long liveCheckStart = profile ? System.nanoTime() : 0L;
                BlockState liveState = level.getBlockState(cursor);
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.debugLiveCheckNanos.add(System.nanoTime() - liveCheckStart);
                }
                if (liveState.isAir() || liveState.getDestroySpeed(level, cursor) < 0.0F) {
                    if (profile) {
                        DAMAGE_RUNTIME_PROFILE.debugRejected.increment();
                    }
                    clearDamage(level, cursor);
                    failed++;
                    if (profile) {
                        DAMAGE_RUNTIME_PROFILE.debugMethodNanos.add(System.nanoTime() - methodStart);
                    }
                } else {
                    long setMethodStart = profile ? System.nanoTime() : 0L;
                    if (profile) {
                        DAMAGE_RUNTIME_PROFILE.setCalls.increment();
                    }
                    setDamageStateWithResolvedStorage(level, cursor, posLong, bucket.states.getInt(i), ref, liveState);
                    if (profile) {
                        DAMAGE_RUNTIME_PROFILE.setMethodNanos.add(System.nanoTime() - setMethodStart);
                    }
                    applied++;
                    if (profile) {
                        DAMAGE_RUNTIME_PROFILE.debugApplied.increment();
                        DAMAGE_RUNTIME_PROFILE.debugMethodNanos.add(System.nanoTime() - methodStart);
                    }
                }

                processed++;
                if (progressListener != null && progressInterval > 0 && (processed % progressInterval) == 0) {
                    progressListener.onProgress(processed, attempted, applied, failed);
                }
            }
        }

        if (progressListener != null && processed != attempted) {
            progressListener.onProgress(processed, attempted, applied, failed);
        }
        return new BulkDebugApplyResult(attempted, applied, failed);
    }

    public BulkDebugClearResult clearDamageStatesBulk(ServerLevel level, LongArrayList positions, int startOffset,
                                                      int progressInterval,
                                                      BulkDebugProgressListener progressListener) {
        int attempted = positions.size();
        if (attempted <= 0) {
            return new BulkDebugClearResult(0, 0);
        }

        Long2ObjectOpenHashMap<LongArrayList> bySection = new Long2ObjectOpenHashMap<>();
        int normalizedOffset = Math.floorMod(startOffset, attempted);
        for (int i = 0; i < attempted; i++) {
            int index = normalizedOffset + i;
            if (index >= attempted) {
                index -= attempted;
            }

            long posLong = positions.getLong(index);
            long sectionKey = sectionKeyForPos(posLong);
            LongArrayList bucket = bySection.computeIfAbsent(sectionKey, unused -> new LongArrayList());
            bucket.add(posLong);
        }

        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
        int cleared = 0;
        int processed = 0;

        for (Long2ObjectOpenHashMap.Entry<LongArrayList> entry : bySection.long2ObjectEntrySet()) {
            long sectionKey = entry.getLongKey();
            int sectionX = SectionPos.x(sectionKey);
            int sectionY = SectionPos.y(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);

            LevelChunk chunk = level.getChunkSource().getChunkNow(sectionX, sectionZ);
            if (chunk == null) {
                chunk = level.getChunkAt(new BlockPos(sectionX << 4, sectionY << 4, sectionZ << 4));
            }
            if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
                int sectionSize = entry.getValue().size();
                processed += sectionSize;
                if (damageRuntimeProfilingEnabled) {
                    DAMAGE_RUNTIME_PROFILE.clearCalls.add(sectionSize);
                    DAMAGE_RUNTIME_PROFILE.clearLookupMisses.add(sectionSize);
                }
                if (progressListener != null && progressInterval > 0 && (processed % progressInterval) == 0) {
                    progressListener.onProgress(processed, attempted, cleared, 0);
                }
                continue;
            }

            ChunkStorageRef ref = new ChunkStorageRef(chunk, access.krakk$getBlockDamageStorage());
            LongArrayList bucket = entry.getValue();
            int sectionSize = bucket.size();
            for (int i = 0; i < sectionSize; i++) {
                long posLong = bucket.getLong(i);
                cursor.set(BlockPos.getX(posLong), BlockPos.getY(posLong), BlockPos.getZ(posLong));

                boolean profile = damageRuntimeProfilingEnabled;
                long methodStart = profile ? System.nanoTime() : 0L;
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.clearCalls.increment();
                }

                long removeStart = profile ? System.nanoTime() : 0L;
                int removed = ref.storage().removeDamageState(posLong);
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.clearRemoveNanos.add(System.nanoTime() - removeStart);
                }
                if (removed != NO_DAMAGE_STATE) {
                    recordSourceMutation(level, removed, 0, "bulkClearDamage");
                    ref.chunk().setUnsaved(true);
                    if (profile) {
                        DAMAGE_RUNTIME_PROFILE.clearChanged.increment();
                    }
                    syncDamageState(level, cursor, 0);
                    cleared++;
                }
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.clearMethodNanos.add(System.nanoTime() - methodStart);
                }

                processed++;
                if (progressListener != null && progressInterval > 0 && (processed % progressInterval) == 0) {
                    progressListener.onProgress(processed, attempted, cleared, 0);
                }
            }
        }

        if (progressListener != null && processed != attempted) {
            progressListener.onProgress(processed, attempted, cleared, 0);
        }
        return new BulkDebugClearResult(attempted, cleared);
    }

    public void queueConnectSync(ServerPlayer player) {
        // Initial sync now rides vanilla chunk section payloads.
    }

    @Override
    public void queuePlayerSync(ServerPlayer player) {
        // Initial sync now rides vanilla chunk section payloads.
    }

    public void clearQueuedSync(ServerPlayer player) {
        // No queued sync pipeline.
    }

    @Override
    public void clearQueuedPlayerSync(ServerPlayer player) {
        clearQueuedSync(player);
    }

    @Override
    public void tickQueuedSyncs(MinecraftServer server) {
        // No queued sync pipeline.
    }

    public void syncAllDamageStatesToPlayer(ServerPlayer player) {
        ServerLevel level = player.serverLevel();
        ChunkPos centerChunk = player.chunkPosition();
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;
        for (int chunkX = centerChunk.x - viewDistance; chunkX <= centerChunk.x + viewDistance; chunkX++) {
            for (int chunkZ = centerChunk.z - viewDistance; chunkZ <= centerChunk.z + viewDistance; chunkZ++) {
                syncChunkColumnToPlayer(player, level, chunkX, chunkZ, true);
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
        if (chunk == null) {
            return;
        }

        ResourceLocation dimensionId = level.dimension().location();
        LevelChunkSection[] sections = chunk.getSections();
        int sectionY = chunk.getMinBuildHeight() >> 4;
        for (LevelChunkSection section : sections) {
            if (section instanceof KrakkBlockDamageSectionAccess access) {
                Short2ByteOpenHashMap states = access.krakk$getDamageStates();
                if (!states.isEmpty()) {
                    KrakkApi.network().sendSectionSnapshot(
                            player,
                            dimensionId,
                            chunkX,
                            sectionY,
                            chunkZ,
                            new Short2ByteOpenHashMap(states)
                    );
                }
            }
            sectionY++;
        }
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
            recordSourceMutation(level, removedState, 0, "takeStoredDamage");
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
            return breakDamagedBlock(level, blockPos, currentState, null, dropOnBreak, KrakkDamageType.KRAKK_DAMAGE_BASIC);
        }

        int existingState = getDamageState(level, blockPos);
        int nextState = clamp(existingState + clampedAdded, 0, MAX_DAMAGE_STATE);
        if (nextState >= MAX_DAMAGE_STATE) {
            return breakDamagedBlock(level, blockPos, currentState, null, dropOnBreak, KrakkDamageType.KRAKK_DAMAGE_BASIC);
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
                recordSourceMutation(level, state, 0, "decayRemove");
                ref.chunk().setUnsaved(true);
                syncDamageState(level, blockPos, 0);
            }
            return 0;
        }

        long consumedTicks = decayResult.consumedTicks();
        if (ref.storage().setDamageState(posLong, decayedState, lastUpdateTick + consumedTicks)) {
            ref.chunk().setUnsaved(true);
        }
        recordSourceMutation(level, state, decayedState, "decayStep");
        if (decayedState != state) {
            syncDamageState(level, blockPos, decayedState);
        }
        return decayedState;
    }

    private void setDamageState(ServerLevel level, BlockPos blockPos, int damageState) {
        boolean profile = damageRuntimeProfilingEnabled;
        long methodStart = profile ? System.nanoTime() : 0L;
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.setCalls.increment();
        }

        long lookupStart = profile ? System.nanoTime() : 0L;
        ChunkStorageRef ref = getChunkStorage(level, blockPos, true);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.setLookupNanos.add(System.nanoTime() - lookupStart);
        }
        if (ref == null) {
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.setLookupMisses.increment();
                DAMAGE_RUNTIME_PROFILE.setMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }

        setDamageStateWithResolvedStorage(level, blockPos, blockPos.asLong(), damageState, ref, null);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.setMethodNanos.add(System.nanoTime() - methodStart);
        }
    }

    private void setDamageStateWithResolvedStorage(ServerLevel level, BlockPos blockPos, long posLong,
                                                   int damageState, ChunkStorageRef ref,
                                                   BlockState knownLiveState) {
        boolean profile = damageRuntimeProfilingEnabled;
        int clampedState = clamp(damageState, 0, MAX_DAMAGE_STATE);
        long storageStart = profile ? System.nanoTime() : 0L;
        int previousState = ref.storage().getDamageState(posLong);
        recordSourceMutation(level, previousState, clampedState, "setDamageState");
        if (ref.storage().setDamageState(posLong, clampedState, level.getGameTime())) {
            ref.chunk().setUnsaved(true);
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.setStorageWrites.increment();
            }
        }
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.setStorageNanos.add(System.nanoTime() - storageStart);
        }
        if (previousState != clampedState) {
            syncDamageState(level, blockPos, clampedState);
        }
        DamageStateConversionHandler conversionHandler = damageStateConversionHandler;
        if (clampedState > 0 && conversionHandler != DamageStateConversionHandler.NOOP) {
            long conversionStart = profile ? System.nanoTime() : 0L;
            BlockState liveState = knownLiveState != null ? knownLiveState : level.getBlockState(blockPos);
            if (!liveState.isAir()) {
                conversionHandler.apply(level, blockPos, liveState, clampedState);
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.setConversionCalls.increment();
                }
            } else if (profile) {
                DAMAGE_RUNTIME_PROFILE.setConversionSkippedAir.increment();
            }
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.setConversionNanos.add(System.nanoTime() - conversionStart);
            }
        }
    }

    private static void recordSourceMutation(ServerLevel level, int previousState, int nextState, String source) {
        if (isSyncSuppressed()) {
            return;
        }

        SyncBatchContext context = SYNC_BATCH.get();
        if (context == null || context.level != level) {
            context = BULK_SYNC.get();
        }
        if (context == null || context.level != level) {
            return;
        }

        context.recordSourceMutation(
                normalizeDamageState(previousState),
                normalizeDamageState(nextState),
                source
        );
    }

    private static int normalizeDamageState(int value) {
        return value <= 0 ? 0 : clamp(value, 0, MAX_DAMAGE_STATE);
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

    private KrakkImpactResult breakDamagedBlock(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source,
                                                boolean dropOnBreak, KrakkDamageType damageType) {
        boolean broken = damageType == KrakkDamageType.KRAKK_DAMAGE_EXPLOSION
                ? breakDamagedBlockExplosionStyle(level, blockPos, blockState, source, dropOnBreak)
                : level.destroyBlock(blockPos, dropOnBreak, source);
        if (!broken) {
            int fallbackState = MAX_DAMAGE_STATE - 1;
            setDamageState(level, blockPos, fallbackState);
            return new KrakkImpactResult(false, fallbackState);
        }
        clearDamage(level, blockPos);
        return new KrakkImpactResult(true, NO_DAMAGE_STATE);
    }

    private boolean breakDamagedBlockExplosionStyle(ServerLevel level, BlockPos blockPos, BlockState blockState,
                                                    Entity source, boolean dropOnBreak) {
        Explosion explosionContext = new Explosion(
                level,
                source,
                blockPos.getX() + 0.5D,
                blockPos.getY() + 0.5D,
                blockPos.getZ() + 0.5D,
                1.0F,
                false,
                Explosion.BlockInteraction.DESTROY
        );
        if (dropOnBreak && blockState.getBlock().dropFromExplosion(explosionContext)) {
            BlockEntity blockEntity = blockState.hasBlockEntity() ? level.getBlockEntity(blockPos) : null;
            LootParams.Builder lootBuilder = new LootParams.Builder(level)
                    .withParameter(LootContextParams.ORIGIN, Vec3.atCenterOf(blockPos))
                    .withParameter(LootContextParams.TOOL, ItemStack.EMPTY)
                    .withOptionalParameter(LootContextParams.BLOCK_ENTITY, blockEntity)
                    .withOptionalParameter(LootContextParams.THIS_ENTITY, source);
            blockState.spawnAfterBreak(level, blockPos, ItemStack.EMPTY, source instanceof Player);
            blockState.getDrops(lootBuilder).forEach(stack -> net.minecraft.world.level.block.Block.popResource(level, blockPos, stack));
        }
        boolean removed = level.setBlock(blockPos, Blocks.AIR.defaultBlockState(), 3);
        if (removed) {
            blockState.getBlock().wasExploded(level, blockPos, explosionContext);
        }
        return removed;
    }

    private void syncDamageState(ServerLevel level, BlockPos blockPos, int damageState) {
        boolean profile = damageRuntimeProfilingEnabled;
        long methodStart = profile ? System.nanoTime() : 0L;
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.syncCalls.increment();
        }

        if (isSyncSuppressed()) {
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.syncSuppressed.increment();
                DAMAGE_RUNTIME_PROFILE.syncMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }
        SyncBatchContext batch = SYNC_BATCH.get();
        if (batch != null && batch.level == level) {
            if (!batch.markDirtyPosition(blockPos.asLong(), damageState) && profile) {
                DAMAGE_RUNTIME_PROFILE.syncCoalesced.increment();
            }
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.syncBatched.increment();
                DAMAGE_RUNTIME_PROFILE.syncMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }
        SyncBatchContext bulk = BULK_SYNC.get();
        if (bulk != null && bulk.level == level) {
            if (!bulk.markDirtyPosition(blockPos.asLong(), damageState) && profile) {
                DAMAGE_RUNTIME_PROFILE.syncCoalesced.increment();
            }
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.syncBatched.increment();
                DAMAGE_RUNTIME_PROFILE.syncMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }
        markDamageStateChanged(level, blockPos, damageState);
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.syncDirect.increment();
            DAMAGE_RUNTIME_PROFILE.syncMethodNanos.add(System.nanoTime() - methodStart);
        }
    }

    private static void markDamageStateChanged(ServerLevel level, BlockPos blockPos, int damageState) {
        boolean profile = damageRuntimeProfilingEnabled;
        long methodStart = profile ? System.nanoTime() : 0L;
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.markCalls.increment();
        }

        if (isSyncSuppressed()) {
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.markSuppressed.increment();
                DAMAGE_RUNTIME_PROFILE.markMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }

        if (tryMarkDamageStateChangedViaChunkCache(level, blockPos)) {
            SYNC_DEBUG_CHUNK_NOTIFY_SUCCESS.increment();
            if (syncDebugLoggingEnabled) {
                LOGGER.debug(
                        "Krakk sync route: chunk holder damage notify dim={} pos=({}, {}, {}) state={}",
                        level.dimension().location(),
                        blockPos.getX(),
                        blockPos.getY(),
                        blockPos.getZ(),
                        damageState
                );
            }
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.markCacheNotifies.increment();
                DAMAGE_RUNTIME_PROFILE.markMethodNanos.add(System.nanoTime() - methodStart);
            }
            return;
        }

        KrakkApi.network().sendDamageSync(level, blockPos, damageState);
        SYNC_DEBUG_CHUNK_NOTIFY_FAIL.increment();
        SYNC_DEBUG_PACKET_FALLBACKS.increment();
        if (syncDebugLoggingEnabled) {
            LOGGER.debug(
                    "Krakk sync route fallback: direct damage packet dim={} pos=({}, {}, {}) state={}",
                    level.dimension().location(),
                    blockPos.getX(),
                    blockPos.getY(),
                    blockPos.getZ(),
                    damageState
            );
        }
        if (profile) {
            DAMAGE_RUNTIME_PROFILE.markPacketSends.increment();
            DAMAGE_RUNTIME_PROFILE.markMethodNanos.add(System.nanoTime() - methodStart);
        }
    }

    private static void flushBatchedSync(SyncBatchContext context) {
        boolean profile = damageRuntimeProfilingEnabled;
        long start = profile ? System.nanoTime() : 0L;
        if (context.isEmpty()) {
            if (profile) {
                DAMAGE_RUNTIME_PROFILE.flushCalls.increment();
                DAMAGE_RUNTIME_PROFILE.flushMethodNanos.add(System.nanoTime() - start);
            }
            return;
        }
        if (syncDebugLoggingEnabled) {
            LOGGER.debug(
                    "Krakk sync flush start: dim={} dirtyPositions={} dirtySections={} chunkCachePreferred={}",
                    context.level.dimension().location(),
                    context.dirtyPositionCount(),
                    context.dirtySectionCount(),
                    context.chunkCacheRoutePreferred()
            );
        }

        long flushed = flushBatchedSyncPerBlock(context);
        if (profile) {
            if (context.chunkCacheRoutePreferred()) {
                DAMAGE_RUNTIME_PROFILE.flushRouteChunkCache.increment();
            } else {
                DAMAGE_RUNTIME_PROFILE.flushRoutePerBlockFallback.increment();
            }
        }

        if (profile) {
            DAMAGE_RUNTIME_PROFILE.flushCalls.increment();
            DAMAGE_RUNTIME_PROFILE.flushEntries.add(flushed);
            DAMAGE_RUNTIME_PROFILE.flushMethodNanos.add(System.nanoTime() - start);
        }
        if (syncDebugLoggingEnabled) {
            LOGGER.debug(
                    "Krakk sync flush complete: dim={} flushed={} dirtyPositions={} dirtySections={} "
                            + "sourceWrites(calls={} changed={} noop={} toNonZero={} toZero={} z2nz={} nz2z={} nz2nz={} changedBySource={} noopBySource={}) "
                            + "dirtyMarks(calls={} new={} overwrite={} noop={})",
                    context.level.dimension().location(),
                    flushed,
                    context.dirtyPositionCount(),
                    context.dirtySectionCount(),
                    context.sourceMutationCalls,
                    context.sourceMutationChanged,
                    context.sourceMutationNoop,
                    context.sourceMutationToNonZero,
                    context.sourceMutationToZero,
                    context.sourceMutationZeroToNonZero,
                    context.sourceMutationNonZeroToZero,
                    context.sourceMutationNonZeroToNonZero,
                    context.sourceMutationChangedBySource,
                    context.sourceMutationNoopBySource,
                    context.dirtyMarkCalls,
                    context.dirtyMarkNew,
                    context.dirtyMarkOverwrite,
                    context.dirtyMarkNoop
            );
        }
    }

    private static long flushBatchedSyncPerBlock(SyncBatchContext context) {
        long flushed = 0L;
        for (Long2ByteMap.Entry entry : context.dirtyPositionsEntrySet()) {
            markDamageStateChanged(context.level, BlockPos.of(entry.getLongKey()), entry.getByteValue());
            flushed++;
        }
        return flushed;
    }

    private static long flushBatchedSyncViaSectionBatches(SyncBatchContext context) {
        List<ServerPlayer> fallbackPlayers = context.level.players();
        if (fallbackPlayers.isEmpty()) {
            return 0L;
        }

        ResourceLocation dimensionId = context.level.dimension().location();
        int fallbackViewDistance = context.level.getServer().getPlayerList().getViewDistance() + 1;
        long flushed = 0L;
        boolean profile = damageRuntimeProfilingEnabled;
        List<SectionBatchPlan> sectionPlans = new ArrayList<>();

        for (Long2ObjectOpenHashMap.Entry<Short2ByteOpenHashMap> sectionEntry : context.dirtySectionsEntrySet()) {
            long sectionKey = sectionEntry.getLongKey();
            int sectionX = SectionPos.x(sectionKey);
            int sectionY = SectionPos.y(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);
            List<ServerPlayer> recipients = collectTrackingPlayersForChunk(
                    context.level,
                    sectionX,
                    sectionZ,
                    fallbackPlayers,
                    fallbackViewDistance
            );
            if (recipients.isEmpty()) {
                continue;
            }
            Short2ByteOpenHashMap changedStates = sectionEntry.getValue();
            int changedEntries = changedStates.size();
            boolean snapshot = changedEntries >= bulkSyncSectionSnapshotLimit;
            if (snapshot) {
                sectionPlans.add(new SectionBatchPlan(
                        true,
                        sectionX,
                        sectionY,
                        sectionZ,
                        recipients,
                        changedEntries,
                        sectionStateView(context.level, sectionX, sectionY, sectionZ)
                ));
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.flushRouteSectionSnapshot.increment();
                }
            } else {
                sectionPlans.add(new SectionBatchPlan(
                        false,
                        sectionX,
                        sectionY,
                        sectionZ,
                        recipients,
                        changedEntries,
                        changedStates
                ));
                if (profile) {
                    DAMAGE_RUNTIME_PROFILE.flushRouteSectionDelta.increment();
                }
            }
        }

        if (!sectionPlans.isEmpty()) {
            if (canParallelizeSnapshotMaterialization(sectionPlans.size())) {
                List<ForkJoinTask<PreparedSectionBatch>> tasks = new ArrayList<>(sectionPlans.size());
                for (SectionBatchPlan plan : sectionPlans) {
                    tasks.add(bulkSyncSnapshotPool.submit(
                            () -> prepareSectionBatchPayload(context.level, dimensionId, plan)
                    ));
                }
                for (ForkJoinTask<PreparedSectionBatch> task : tasks) {
                    PreparedSectionBatch prepared = task.join();
                    sendPreparedSectionBatch(dimensionId, prepared);
                    flushed += prepared.plan().changedEntries();
                }
            } else {
                for (SectionBatchPlan plan : sectionPlans) {
                    PreparedSectionBatch prepared = prepareSectionBatchPayload(context.level, dimensionId, plan);
                    sendPreparedSectionBatch(dimensionId, prepared);
                    flushed += plan.changedEntries();
                }
            }
        }
        return flushed;
    }

    private static List<ServerPlayer> collectTrackingPlayersForChunk(ServerLevel level,
                                                                     int chunkX,
                                                                     int chunkZ,
                                                                     List<ServerPlayer> fallbackPlayers,
                                                                     int fallbackViewDistance) {
        Object chunkSource = level.getChunkSource();
        if (chunkSource instanceof KrakkServerChunkCacheAccess access) {
            List<ServerPlayer> trackedPlayers = access.krakk$getTrackingPlayers(chunkX, chunkZ, false);
            if (!trackedPlayers.isEmpty()) {
                return new ArrayList<>(trackedPlayers);
            }
            return Collections.emptyList();
        }

        if (fallbackPlayers.isEmpty()) {
            return Collections.emptyList();
        }

        List<ServerPlayer> recipients = new ArrayList<>();
        for (ServerPlayer player : fallbackPlayers) {
            ChunkPos playerChunk = player.chunkPosition();
            if (Math.abs(playerChunk.x - chunkX) > fallbackViewDistance || Math.abs(playerChunk.z - chunkZ) > fallbackViewDistance) {
                continue;
            }
            recipients.add(player);
        }
        return recipients;
    }

    private static PreparedSectionBatch prepareSectionBatchPayload(ServerLevel level, ResourceLocation dimensionId, SectionBatchPlan plan) {
        KrakkNetworkApi network = KrakkApi.network();
        if (plan.snapshot()) {
            Short2ByteOpenHashMap snapshotStates = copySectionView(sectionStateView(level, plan.sectionX(), plan.sectionY(), plan.sectionZ()));
            KrakkNetworkApi.SharedPayload payload = network.serializeSectionSnapshotPayload(
                    dimensionId,
                    plan.sectionX(),
                    plan.sectionY(),
                    plan.sectionZ(),
                    snapshotStates
            );
            return new PreparedSectionBatch(plan, snapshotStates, payload);
        }

        Short2ByteOpenHashMap deltaStates = plan.states();
        KrakkNetworkApi.SharedPayload payload = network.serializeSectionDeltaPayload(
                dimensionId,
                plan.sectionX(),
                plan.sectionY(),
                plan.sectionZ(),
                deltaStates
        );
        return new PreparedSectionBatch(plan, deltaStates, payload);
    }

    private static void sendPreparedSectionBatch(ResourceLocation dimensionId, PreparedSectionBatch prepared) {
        KrakkNetworkApi network = KrakkApi.network();
        SectionBatchPlan plan = prepared.plan();
        KrakkNetworkApi.SharedPayload payload = prepared.payload();
        if (payload != null) {
            network.sendSharedPayloadBatch(plan.recipients(), payload);
            return;
        }

        if (plan.snapshot()) {
            network.sendSectionSnapshotBatch(
                    plan.recipients(),
                    dimensionId,
                    plan.sectionX(),
                    plan.sectionY(),
                    plan.sectionZ(),
                    prepared.states()
            );
            return;
        }
        network.sendSectionDeltaBatch(
                plan.recipients(),
                dimensionId,
                plan.sectionX(),
                plan.sectionY(),
                plan.sectionZ(),
                prepared.states()
        );
    }

    private static Short2ByteOpenHashMap sectionStateView(ServerLevel level, int sectionX, int sectionY, int sectionZ) {
        LevelChunk chunk = level.getChunkSource().getChunkNow(sectionX, sectionZ);
        if (chunk == null) {
            return null;
        }

        int sectionIndex = sectionY - (chunk.getMinBuildHeight() >> 4);
        LevelChunkSection[] sections = chunk.getSections();
        if (sectionIndex < 0 || sectionIndex >= sections.length) {
            return new Short2ByteOpenHashMap();
        }

        LevelChunkSection section = sections[sectionIndex];
        if (!(section instanceof KrakkBlockDamageSectionAccess access)) {
            return new Short2ByteOpenHashMap();
        }

        Short2ByteOpenHashMap sectionView = access.krakk$getDamageStates();
        if (sectionView.isEmpty()) {
            return new Short2ByteOpenHashMap();
        }
        return sectionView;
    }

    private static Short2ByteOpenHashMap copySectionView(Short2ByteOpenHashMap sectionView) {
        if (sectionView == null || sectionView.isEmpty()) {
            return new Short2ByteOpenHashMap();
        }
        return new Short2ByteOpenHashMap(sectionView);
    }

    private static boolean canParallelizeSnapshotMaterialization(int snapshotSections) {
        return bulkSyncSnapshotPool != null && snapshotSections >= bulkSyncSnapshotParallelMinSections;
    }

    private static ForkJoinPool createSnapshotPool() {
        int parallelism = Math.max(1, bulkSyncSnapshotParallelism);
        if (parallelism <= 1) {
            return null;
        }
        return new ForkJoinPool(parallelism);
    }

    private static boolean canUseChunkCacheDirtyNotify(ServerLevel level) {
        Object chunkSource = level.getChunkSource();
        if (chunkSource instanceof KrakkServerChunkCacheAccess) {
            return true;
        }
        if (resolveChunkSourceNotifyMethod(chunkSource.getClass()) != null) {
            return true;
        }
        return resolveChunkSourceVisibleChunkMethod(chunkSource.getClass()) != null
                && resolveChunkHolderNotifyMethod(ChunkHolder.class) != null;
    }

    private static boolean tryMarkDamageStateChangedViaChunkCache(ServerLevel level, BlockPos blockPos) {
        Object chunkSource = level.getChunkSource();
        if (chunkSource instanceof KrakkServerChunkCacheAccess access) {
            return access.krakk$damageStateChanged(blockPos);
        }

        Method notifyMethod = resolveChunkSourceNotifyMethod(chunkSource.getClass());
        if (notifyMethod != null) {
            try {
                return invokeNotifyMethod(notifyMethod, chunkSource, blockPos);
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                CHUNK_SOURCE_NOTIFY_METHODS.remove(chunkSource.getClass());
                CHUNK_SOURCE_NO_NOTIFY_METHOD.add(chunkSource.getClass());
            }
        }
        return tryMarkDamageStateChangedViaChunkHolder(chunkSource, blockPos);
    }

    private static Method resolveChunkSourceNotifyMethod(Class<?> chunkSourceClass) {
        Method cached = CHUNK_SOURCE_NOTIFY_METHODS.get(chunkSourceClass);
        if (cached != null) {
            return cached;
        }
        if (CHUNK_SOURCE_NO_NOTIFY_METHOD.contains(chunkSourceClass)) {
            return null;
        }

        Method resolved = null;
        try {
            resolved = chunkSourceClass.getMethod("krakk$damageStateChanged", BlockPos.class);
        } catch (NoSuchMethodException ignored) {
            try {
                Method declared = chunkSourceClass.getDeclaredMethod("krakk$damageStateChanged", BlockPos.class);
                declared.setAccessible(true);
                resolved = declared;
            } catch (NoSuchMethodException ignoredAgain) {
                CHUNK_SOURCE_NO_NOTIFY_METHOD.add(chunkSourceClass);
                return null;
            }
        }

        CHUNK_SOURCE_NOTIFY_METHODS.put(chunkSourceClass, resolved);
        return resolved;
    }

    private static boolean tryMarkDamageStateChangedViaChunkHolder(Object chunkSource, BlockPos blockPos) {
        Method visibleChunkMethod = resolveChunkSourceVisibleChunkMethod(chunkSource.getClass());
        if (visibleChunkMethod == null) {
            return false;
        }

        long chunkPosLong = ChunkPos.asLong(
                SectionPos.blockToSectionCoord(blockPos.getX()),
                SectionPos.blockToSectionCoord(blockPos.getZ())
        );
        Object chunkHolder;
        try {
            chunkHolder = visibleChunkMethod.invoke(chunkSource, chunkPosLong);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            CHUNK_SOURCE_VISIBLE_CHUNK_METHODS.remove(chunkSource.getClass());
            CHUNK_SOURCE_NO_VISIBLE_CHUNK_METHOD.add(chunkSource.getClass());
            return false;
        }

        if (chunkHolder == null) {
            return false;
        }
        if (chunkHolder instanceof org.shipwrights.krakk.state.network.KrakkChunkHolderAccess directAccess) {
            return directAccess.krakk$damageStateChanged(blockPos);
        }

        Method notifyMethod = resolveChunkHolderNotifyMethod(chunkHolder.getClass());
        if (notifyMethod == null) {
            return false;
        }
        try {
            return invokeNotifyMethod(notifyMethod, chunkHolder, blockPos);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            CHUNK_HOLDER_NOTIFY_METHODS.remove(chunkHolder.getClass());
            CHUNK_HOLDER_NO_NOTIFY_METHOD.add(chunkHolder.getClass());
            return false;
        }
    }

    private static boolean invokeNotifyMethod(Method method, Object target, BlockPos blockPos)
            throws ReflectiveOperationException {
        Object result = method.invoke(target, blockPos);
        if (method.getReturnType() == Boolean.TYPE || method.getReturnType() == Boolean.class) {
            return Boolean.TRUE.equals(result);
        }
        return true;
    }

    private static Method resolveChunkSourceVisibleChunkMethod(Class<?> chunkSourceClass) {
        Method cached = CHUNK_SOURCE_VISIBLE_CHUNK_METHODS.get(chunkSourceClass);
        if (cached != null) {
            return cached;
        }
        if (CHUNK_SOURCE_NO_VISIBLE_CHUNK_METHOD.contains(chunkSourceClass)) {
            return null;
        }

        Method resolved = null;
        try {
            resolved = chunkSourceClass.getMethod("getVisibleChunkIfPresent", long.class);
        } catch (NoSuchMethodException ignored) {
            try {
                Method declared = chunkSourceClass.getDeclaredMethod("getVisibleChunkIfPresent", long.class);
                declared.setAccessible(true);
                resolved = declared;
            } catch (NoSuchMethodException ignoredAgain) {
                CHUNK_SOURCE_NO_VISIBLE_CHUNK_METHOD.add(chunkSourceClass);
                return null;
            }
        }

        CHUNK_SOURCE_VISIBLE_CHUNK_METHODS.put(chunkSourceClass, resolved);
        return resolved;
    }

    private static Method resolveChunkHolderNotifyMethod(Class<?> chunkHolderClass) {
        Method cached = CHUNK_HOLDER_NOTIFY_METHODS.get(chunkHolderClass);
        if (cached != null) {
            return cached;
        }
        if (CHUNK_HOLDER_NO_NOTIFY_METHOD.contains(chunkHolderClass)) {
            return null;
        }

        Method resolved = null;
        try {
            resolved = chunkHolderClass.getMethod("krakk$damageStateChanged", BlockPos.class);
        } catch (NoSuchMethodException ignored) {
            try {
                Method declared = chunkHolderClass.getDeclaredMethod("krakk$damageStateChanged", BlockPos.class);
                declared.setAccessible(true);
                resolved = declared;
            } catch (NoSuchMethodException ignoredAgain) {
                CHUNK_HOLDER_NO_NOTIFY_METHOD.add(chunkHolderClass);
                return null;
            }
        }

        CHUNK_HOLDER_NOTIFY_METHODS.put(chunkHolderClass, resolved);
        return resolved;
    }

    private static int parseNonNegativeIntProperty(String key, int fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Math.max(0, Integer.parseInt(raw.trim()));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static int parsePositiveIntProperty(String key, int fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Math.max(1, Integer.parseInt(raw.trim()));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static boolean parseBooleanProperty(String key, boolean fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String normalized = raw.trim();
        if ("true".equalsIgnoreCase(normalized) || "1".equals(normalized) || "yes".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized) || "0".equals(normalized) || "no".equalsIgnoreCase(normalized)) {
            return false;
        }
        return fallback;
    }

    private static final class SyncBatchContext {
        private final ServerLevel level;
        private final boolean chunkCacheRoutePreferred;
        private final int sectionTrackingThreshold;
        private final Long2ByteOpenHashMap dirtyPositions = new Long2ByteOpenHashMap();
        private Long2ObjectOpenHashMap<Short2ByteOpenHashMap> dirtySections;
        private int dirtyCount;
        private int depth = 1;
        private long sourceMutationCalls;
        private long sourceMutationChanged;
        private long sourceMutationNoop;
        private long sourceMutationToZero;
        private long sourceMutationToNonZero;
        private long sourceMutationZeroToNonZero;
        private long sourceMutationNonZeroToZero;
        private long sourceMutationNonZeroToNonZero;
        private final Map<String, Integer> sourceMutationChangedBySource = new HashMap<>();
        private final Map<String, Integer> sourceMutationNoopBySource = new HashMap<>();
        private long dirtyMarkCalls;
        private long dirtyMarkNew;
        private long dirtyMarkOverwrite;
        private long dirtyMarkNoop;

        private SyncBatchContext(ServerLevel level) {
            this.level = level;
            this.chunkCacheRoutePreferred = canUseChunkCacheDirtyNotify(level);
            // Vanilla packet path now flushes per-block marks; keep dirty tracking in per-block mode.
            this.sectionTrackingThreshold = Integer.MAX_VALUE;
        }

        private boolean isEmpty() {
            return this.dirtyCount <= 0;
        }

        private boolean chunkCacheRoutePreferred() {
            return this.chunkCacheRoutePreferred;
        }

        private boolean usePerBlockFlush() {
            return (this.chunkCacheRoutePreferred && this.dirtyCount <= this.sectionTrackingThreshold) || this.dirtySections == null;
        }

        private int dirtyPositionCount() {
            if (this.dirtySections != null) {
                return this.dirtyCount;
            }
            return this.dirtyPositions.size();
        }

        private int dirtySectionCount() {
            return this.dirtySections == null ? 0 : this.dirtySections.size();
        }

        private Iterable<Long2ByteMap.Entry> dirtyPositionsEntrySet() {
            return this.dirtyPositions.long2ByteEntrySet();
        }

        private Iterable<Long2ObjectOpenHashMap.Entry<Short2ByteOpenHashMap>> dirtySectionsEntrySet() {
            return this.dirtySections.long2ObjectEntrySet();
        }

        private void recordSourceMutation(int previousState, int nextState, String source) {
            this.sourceMutationCalls++;
            if (previousState == nextState) {
                this.sourceMutationNoop++;
                this.sourceMutationNoopBySource.merge(source, 1, Integer::sum);
                return;
            }

            this.sourceMutationChanged++;
            this.sourceMutationChangedBySource.merge(source, 1, Integer::sum);
            if (nextState <= 0) {
                this.sourceMutationToZero++;
                if (previousState > 0) {
                    this.sourceMutationNonZeroToZero++;
                }
                return;
            }

            this.sourceMutationToNonZero++;
            if (previousState <= 0) {
                this.sourceMutationZeroToNonZero++;
            } else {
                this.sourceMutationNonZeroToNonZero++;
            }
        }

        private void enableSectionTracking() {
            if (this.dirtySections != null) {
                return;
            }
            Long2ObjectOpenHashMap<Short2ByteOpenHashMap> sections = new Long2ObjectOpenHashMap<>();
            for (Long2ByteMap.Entry entry : this.dirtyPositions.long2ByteEntrySet()) {
                long posLong = entry.getLongKey();
                long sectionKey = sectionKeyForPos(posLong);
                Short2ByteOpenHashMap sectionStates = sections.computeIfAbsent(sectionKey, unused -> new Short2ByteOpenHashMap());
                sectionStates.put(sectionLocalIndex(posLong), entry.getByteValue());
            }
            this.dirtySections = sections;
            this.dirtyPositions.clear();
        }

        private boolean markDirtyPosition(long posLong, int damageState) {
            this.dirtyMarkCalls++;
            byte clamped = (byte) clamp(damageState, 0, MAX_DAMAGE_STATE);
            if (this.dirtySections == null) {
                boolean existed = this.dirtyPositions.containsKey(posLong);
                if (existed && this.dirtyPositions.get(posLong) == clamped) {
                    this.dirtyMarkNoop++;
                    return false;
                }
                this.dirtyPositions.put(posLong, clamped);
                if (!existed) {
                    this.dirtyMarkNew++;
                    this.dirtyCount++;
                    if (this.dirtyCount > this.sectionTrackingThreshold) {
                        enableSectionTracking();
                    }
                } else {
                    this.dirtyMarkOverwrite++;
                }
                return true;
            }

            long sectionKey = sectionKeyForPos(posLong);
            Short2ByteOpenHashMap sectionStates = this.dirtySections.computeIfAbsent(sectionKey, unused -> new Short2ByteOpenHashMap());
            short localIndex = sectionLocalIndex(posLong);
            boolean existed = sectionStates.containsKey(localIndex);
            if (existed && sectionStates.get(localIndex) == clamped) {
                this.dirtyMarkNoop++;
                return false;
            }
            if (!existed) {
                this.dirtyMarkNew++;
                this.dirtyCount++;
            } else {
                this.dirtyMarkOverwrite++;
            }
            sectionStates.put(localIndex, clamped);
            return true;
        }
    }

    private static long sectionKeyForPos(long posLong) {
        int x = BlockPos.getX(posLong);
        int y = BlockPos.getY(posLong);
        int z = BlockPos.getZ(posLong);
        return SectionPos.asLong(SectionPos.blockToSectionCoord(x), SectionPos.blockToSectionCoord(y), SectionPos.blockToSectionCoord(z));
    }

    private static short sectionLocalIndex(long posLong) {
        int x = BlockPos.getX(posLong) & 15;
        int y = BlockPos.getY(posLong) & 15;
        int z = BlockPos.getZ(posLong) & 15;
        return (short) ((y << 8) | (z << 4) | x);
    }

    private static final class SectionWriteBucket {
        private final LongArrayList positions = new LongArrayList();
        private final IntArrayList states = new IntArrayList();
    }

    private record SectionBatchPlan(boolean snapshot,
                                    int sectionX,
                                    int sectionY,
                                    int sectionZ,
                                    List<ServerPlayer> recipients,
                                    int changedEntries,
                                    Short2ByteOpenHashMap states) {
    }

    private record PreparedSectionBatch(SectionBatchPlan plan,
                                        Short2ByteOpenHashMap states,
                                        KrakkNetworkApi.SharedPayload payload) {
    }

    public record DamageRuntimeProfileSnapshot(
            long debugCalls,
            long debugApplied,
            long debugRejected,
            long debugLiveCheckNanos,
            long debugMethodNanos,
            long clearCalls,
            long clearLookupMisses,
            long clearChanged,
            long clearLookupNanos,
            long clearRemoveNanos,
            long clearMethodNanos,
            long setCalls,
            long setLookupMisses,
            long setStorageWrites,
            long setConversionCalls,
            long setConversionSkippedAir,
            long setLookupNanos,
            long setStorageNanos,
            long setConversionNanos,
            long setMethodNanos,
            long syncCalls,
            long syncSuppressed,
            long syncBatched,
            long syncCoalesced,
            long syncDirect,
            long syncMethodNanos,
            long markCalls,
            long markSuppressed,
            long markCacheNotifies,
            long markPacketSends,
            long markMethodNanos,
            long flushCalls,
            long flushEntries,
            long flushMethodNanos,
            long flushRouteChunkCache,
            long flushRoutePerBlockFallback,
            long flushRouteSectionDelta,
            long flushRouteSectionSnapshot
    ) {
    }

    private static final class DamageRuntimeProfileCounters {
        private final LongAdder debugCalls = new LongAdder();
        private final LongAdder debugApplied = new LongAdder();
        private final LongAdder debugRejected = new LongAdder();
        private final LongAdder debugLiveCheckNanos = new LongAdder();
        private final LongAdder debugMethodNanos = new LongAdder();
        private final LongAdder clearCalls = new LongAdder();
        private final LongAdder clearLookupMisses = new LongAdder();
        private final LongAdder clearChanged = new LongAdder();
        private final LongAdder clearLookupNanos = new LongAdder();
        private final LongAdder clearRemoveNanos = new LongAdder();
        private final LongAdder clearMethodNanos = new LongAdder();
        private final LongAdder setCalls = new LongAdder();
        private final LongAdder setLookupMisses = new LongAdder();
        private final LongAdder setStorageWrites = new LongAdder();
        private final LongAdder setConversionCalls = new LongAdder();
        private final LongAdder setConversionSkippedAir = new LongAdder();
        private final LongAdder setLookupNanos = new LongAdder();
        private final LongAdder setStorageNanos = new LongAdder();
        private final LongAdder setConversionNanos = new LongAdder();
        private final LongAdder setMethodNanos = new LongAdder();
        private final LongAdder syncCalls = new LongAdder();
        private final LongAdder syncSuppressed = new LongAdder();
        private final LongAdder syncBatched = new LongAdder();
        private final LongAdder syncCoalesced = new LongAdder();
        private final LongAdder syncDirect = new LongAdder();
        private final LongAdder syncMethodNanos = new LongAdder();
        private final LongAdder markCalls = new LongAdder();
        private final LongAdder markSuppressed = new LongAdder();
        private final LongAdder markCacheNotifies = new LongAdder();
        private final LongAdder markPacketSends = new LongAdder();
        private final LongAdder markMethodNanos = new LongAdder();
        private final LongAdder flushCalls = new LongAdder();
        private final LongAdder flushEntries = new LongAdder();
        private final LongAdder flushMethodNanos = new LongAdder();
        private final LongAdder flushRouteChunkCache = new LongAdder();
        private final LongAdder flushRoutePerBlockFallback = new LongAdder();
        private final LongAdder flushRouteSectionDelta = new LongAdder();
        private final LongAdder flushRouteSectionSnapshot = new LongAdder();

        private void reset() {
            debugCalls.reset();
            debugApplied.reset();
            debugRejected.reset();
            debugLiveCheckNanos.reset();
            debugMethodNanos.reset();
            clearCalls.reset();
            clearLookupMisses.reset();
            clearChanged.reset();
            clearLookupNanos.reset();
            clearRemoveNanos.reset();
            clearMethodNanos.reset();
            setCalls.reset();
            setLookupMisses.reset();
            setStorageWrites.reset();
            setConversionCalls.reset();
            setConversionSkippedAir.reset();
            setLookupNanos.reset();
            setStorageNanos.reset();
            setConversionNanos.reset();
            setMethodNanos.reset();
            syncCalls.reset();
            syncSuppressed.reset();
            syncBatched.reset();
            syncCoalesced.reset();
            syncDirect.reset();
            syncMethodNanos.reset();
            markCalls.reset();
            markSuppressed.reset();
            markCacheNotifies.reset();
            markPacketSends.reset();
            markMethodNanos.reset();
            flushCalls.reset();
            flushEntries.reset();
            flushMethodNanos.reset();
            flushRouteChunkCache.reset();
            flushRoutePerBlockFallback.reset();
            flushRouteSectionDelta.reset();
            flushRouteSectionSnapshot.reset();
        }

        private DamageRuntimeProfileSnapshot snapshot() {
            return new DamageRuntimeProfileSnapshot(
                    debugCalls.sum(),
                    debugApplied.sum(),
                    debugRejected.sum(),
                    debugLiveCheckNanos.sum(),
                    debugMethodNanos.sum(),
                    clearCalls.sum(),
                    clearLookupMisses.sum(),
                    clearChanged.sum(),
                    clearLookupNanos.sum(),
                    clearRemoveNanos.sum(),
                    clearMethodNanos.sum(),
                    setCalls.sum(),
                    setLookupMisses.sum(),
                    setStorageWrites.sum(),
                    setConversionCalls.sum(),
                    setConversionSkippedAir.sum(),
                    setLookupNanos.sum(),
                    setStorageNanos.sum(),
                    setConversionNanos.sum(),
                    setMethodNanos.sum(),
                    syncCalls.sum(),
                    syncSuppressed.sum(),
                    syncBatched.sum(),
                    syncCoalesced.sum(),
                    syncDirect.sum(),
                    syncMethodNanos.sum(),
                    markCalls.sum(),
                    markSuppressed.sum(),
                    markCacheNotifies.sum(),
                    markPacketSends.sum(),
                    markMethodNanos.sum(),
                    flushCalls.sum(),
                    flushEntries.sum(),
                    flushMethodNanos.sum(),
                    flushRouteChunkCache.sum(),
                    flushRoutePerBlockFallback.sum(),
                    flushRouteSectionDelta.sum(),
                    flushRouteSectionSnapshot.sum()
            );
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
