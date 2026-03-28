package org.shipwrights.krakk.runtime.explosion;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceKey;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;

import java.util.Arrays;
import java.util.UUID;

// ─── Phase enum ───────────────────────────────────────────────────────────────

enum NarrowBandWavefrontPhase {
    SEED,
    PROPAGATE,
    COMPLETE,
    FAILED
}

// ─── Job state machine ────────────────────────────────────────────────────────

final class NarrowBandWavefrontJob {
    private final long jobId;
    private final ResourceKey<Level> dimension;
    private final double centerX;
    private final double centerY;
    private final double centerZ;
    private final double resolvedRadius;
    private final double radiusSq;
    private final int minX;
    private final int maxX;
    private final int minY;
    private final int maxY;
    private final int minZ;
    private final int maxZ;
    private final double totalEnergy;
    private final double impactHeatCelsius;
    private final UUID sourceUuid;
    private final UUID ownerUuid;
    private final boolean applyWorldChanges;
    private final ExplosionProfileTrace trace;
    private final boolean phaseLoggingEnabled;
    private final long phaseTraceId;
    private final long queuedAtNanos;

    private NarrowBandWavefrontPhase phase;
    private String failureMessage;
    private Entity cachedSource;
    private LivingEntity cachedOwner;

    private final Int2DoubleOpenHashMap resistanceCostCache = new Int2DoubleOpenHashMap(256);
    private final WaveChunkStateCache chunkStateCache = new WaveChunkStateCache();
    private final BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
    private final Long2FloatOpenHashMap slownessByPos = new Long2FloatOpenHashMap(2048);
    private final Long2FloatOpenHashMap arrivalByPos = new Long2FloatOpenHashMap(4096);
    private final LongOpenHashSet finalized = new LongOpenHashSet(4096);
    private final WaveLongMinHeap trialQueue = new WaveLongMinHeap(256);

    private int sourceCount;
    private long solveStartNanos;
    private long impactApplyStartNanos;
    // Package-private: written directly by KrakkExplosionRuntime driver methods.
    double impactBudget;
    double directWeight;
    int acceptedNodes;
    int poppedNodes;
    int directImpactApplications;
    int thermalImpactApplications;
    private long lastProgressLogNanos;

    NarrowBandWavefrontJob(long jobId,
                           ResourceKey<Level> dimension,
                           double centerX,
                           double centerY,
                           double centerZ,
                           double resolvedRadius,
                           double radiusSq,
                           int minX,
                           int maxX,
                           int minY,
                           int maxY,
                           int minZ,
                           int maxZ,
                           double totalEnergy,
                           double impactHeatCelsius,
                           UUID sourceUuid,
                           UUID ownerUuid,
                           boolean applyWorldChanges,
                           ExplosionProfileTrace trace,
                           boolean phaseLoggingEnabled,
                           long phaseTraceId,
                           long queuedAtNanos) {
        this.jobId = jobId;
        this.dimension = dimension;
        this.centerX = centerX;
        this.centerY = centerY;
        this.centerZ = centerZ;
        this.resolvedRadius = resolvedRadius;
        this.radiusSq = radiusSq;
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.minZ = minZ;
        this.maxZ = maxZ;
        this.totalEnergy = totalEnergy;
        this.impactHeatCelsius = impactHeatCelsius;
        this.sourceUuid = sourceUuid;
        this.ownerUuid = ownerUuid;
        this.applyWorldChanges = applyWorldChanges;
        this.trace = trace;
        this.phaseLoggingEnabled = phaseLoggingEnabled;
        this.phaseTraceId = phaseTraceId;
        this.queuedAtNanos = queuedAtNanos;
        this.phase = NarrowBandWavefrontPhase.SEED;
        this.failureMessage = null;
        this.lastProgressLogNanos = queuedAtNanos;

        this.resistanceCostCache.defaultReturnValue(Double.NaN);
        this.slownessByPos.defaultReturnValue(Float.NaN);
        this.arrivalByPos.defaultReturnValue(Float.POSITIVE_INFINITY);
    }

    long jobId() {
        return jobId;
    }

    ResourceKey<Level> dimension() {
        return dimension;
    }

    double centerX() {
        return centerX;
    }

    double centerY() {
        return centerY;
    }

    double centerZ() {
        return centerZ;
    }

    double resolvedRadius() {
        return resolvedRadius;
    }

    double radiusSq() {
        return radiusSq;
    }

    int minX() {
        return minX;
    }

    int maxX() {
        return maxX;
    }

    int minY() {
        return minY;
    }

    int maxY() {
        return maxY;
    }

    int minZ() {
        return minZ;
    }

    int maxZ() {
        return maxZ;
    }

    double totalEnergy() {
        return totalEnergy;
    }

    double impactHeatCelsius() {
        return impactHeatCelsius;
    }

    boolean applyWorldChanges() {
        return applyWorldChanges;
    }

    ExplosionProfileTrace trace() {
        return trace;
    }

    boolean phaseLoggingEnabled() {
        return phaseLoggingEnabled;
    }

    long phaseTraceId() {
        return phaseTraceId;
    }

    long queuedAtNanos() {
        return queuedAtNanos;
    }

    long lastProgressLogNanos() {
        return lastProgressLogNanos;
    }

    void lastProgressLogNanos(long value) {
        this.lastProgressLogNanos = value;
    }

    NarrowBandWavefrontPhase phase() {
        return phase;
    }

    void setPhase(NarrowBandWavefrontPhase phase) {
        this.phase = phase == null ? NarrowBandWavefrontPhase.FAILED : phase;
    }

    String failureMessage() {
        return failureMessage;
    }

    void failureMessage(String message) {
        this.failureMessage = message;
    }

    Int2DoubleOpenHashMap resistanceCostCache() {
        return resistanceCostCache;
    }

    WaveChunkStateCache chunkStateCache() {
        return chunkStateCache;
    }

    BlockPos.MutableBlockPos mutablePos() {
        return mutablePos;
    }

    Long2FloatOpenHashMap slownessByPos() {
        return slownessByPos;
    }

    Long2FloatOpenHashMap arrivalByPos() {
        return arrivalByPos;
    }

    LongOpenHashSet finalized() {
        return finalized;
    }

    WaveLongMinHeap trialQueue() {
        return trialQueue;
    }

    int sourceCount() {
        return sourceCount;
    }

    void sourceCount(int sourceCount) {
        this.sourceCount = sourceCount;
    }

    long solveStartNanos() {
        return solveStartNanos;
    }

    void solveStartNanos(long solveStartNanos) {
        this.solveStartNanos = solveStartNanos;
    }

    long impactApplyStartNanos() {
        return impactApplyStartNanos;
    }

    void impactApplyStartNanos(long impactApplyStartNanos) {
        this.impactApplyStartNanos = impactApplyStartNanos;
    }

    Entity resolveSource(ServerLevel level) {
        if (sourceUuid == null) {
            return null;
        }
        if (cachedSource != null && !cachedSource.isRemoved()) {
            return cachedSource;
        }
        Entity resolved = level.getEntity(sourceUuid);
        if (resolved != null && !resolved.isRemoved()) {
            cachedSource = resolved;
            return resolved;
        }
        cachedSource = null;
        return null;
    }

    LivingEntity resolveOwner(ServerLevel level) {
        if (ownerUuid == null) {
            return null;
        }
        if (cachedOwner != null && !cachedOwner.isRemoved()) {
            return cachedOwner;
        }
        Entity resolved = level.getEntity(ownerUuid);
        if (resolved instanceof LivingEntity living && !living.isRemoved()) {
            cachedOwner = living;
            return living;
        }
        cachedOwner = null;
        return null;
    }
}

// ─── Wave helpers ─────────────────────────────────────────────────────────────

final class WaveChunkStateCache {
    private final Long2ObjectOpenHashMap<LevelChunk> chunksByPos = new Long2ObjectOpenHashMap<>();

    BlockState getBlockState(ServerLevel level, BlockPos.MutableBlockPos mutablePos, int x, int y, int z) {
        int chunkX = x >> 4;
        int chunkZ = z >> 4;
        long chunkKey = ChunkPos.asLong(chunkX, chunkZ);
        LevelChunk chunk = chunksByPos.get(chunkKey);
        if (chunk == null) {
            chunk = level.getChunk(chunkX, chunkZ);
            chunksByPos.put(chunkKey, chunk);
        }
        mutablePos.set(x, y, z);
        BlockState worldState = chunk.getBlockState(mutablePos);
        BlockState override = KrakkExplosionRuntime.blockStateProvider.provide(level, mutablePos, worldState);
        return override != null ? override : worldState;
    }
}

final class WaveLongMinHeap {
    private long[] keys;
    private double[] priorities;
    private int size;
    private double polledPriority;

    WaveLongMinHeap(int initialCapacity) {
        int capacity = Math.max(16, initialCapacity);
        this.keys = new long[capacity];
        this.priorities = new double[capacity];
        this.size = 0;
        this.polledPriority = Double.POSITIVE_INFINITY;
    }

    boolean isEmpty() {
        return size <= 0;
    }

    int size() {
        return size;
    }

    void add(long key, double priority) {
        ensureCapacity(size + 1);
        int cursor = size++;
        while (cursor > 0) {
            int parent = (cursor - 1) >>> 1;
            if (priority >= priorities[parent]) {
                break;
            }
            keys[cursor] = keys[parent];
            priorities[cursor] = priorities[parent];
            cursor = parent;
        }
        keys[cursor] = key;
        priorities[cursor] = priority;
    }

    long pollKey() {
        long rootKey = keys[0];
        polledPriority = priorities[0];
        size--;
        if (size > 0) {
            long tailKey = keys[size];
            double tailPriority = priorities[size];
            int cursor = 0;
            while (true) {
                int left = (cursor << 1) + 1;
                if (left >= size) {
                    break;
                }
                int right = left + 1;
                int child = right < size && priorities[right] < priorities[left] ? right : left;
                if (tailPriority <= priorities[child]) {
                    break;
                }
                keys[cursor] = keys[child];
                priorities[cursor] = priorities[child];
                cursor = child;
            }
            keys[cursor] = tailKey;
            priorities[cursor] = tailPriority;
        }
        return rootKey;
    }

    double pollPriority() {
        return polledPriority;
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity <= keys.length) {
            return;
        }
        int newCapacity = Math.max(minCapacity, keys.length << 1);
        keys = Arrays.copyOf(keys, newCapacity);
        priorities = Arrays.copyOf(priorities, newCapacity);
    }
}

record WavefrontSolveStats(int acceptedNodes, int poppedNodes) {
}
