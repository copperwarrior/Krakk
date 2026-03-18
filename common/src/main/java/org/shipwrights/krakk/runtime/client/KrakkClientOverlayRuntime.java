package org.shipwrights.krakk.runtime.client;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.resources.ResourceLocation;
import org.shipwrights.krakk.api.client.KrakkClientOverlayApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.slf4j.Logger;

import java.util.ArrayDeque;

public final class KrakkClientOverlayRuntime implements KrakkClientOverlayApi {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final int KRAKK_DEFERRED_CLEANUP_BUDGET = 512;
    private static final int KRAKK_CLEAR_CHUNK_CLEANUP_BUDGET = 64;

    private final Object lock = new Object();
    private final Long2ByteOpenHashMap damageStates = new Long2ByteOpenHashMap();
    private final Long2ObjectOpenHashMap<LongOpenHashSet> damagePositionsByChunk = new Long2ObjectOpenHashMap<>();
    private final Long2ObjectOpenHashMap<LongOpenHashSet> damagePositionsBySection = new Long2ObjectOpenHashMap<>();
    private final Long2ObjectOpenHashMap<LongOpenHashSet> sectionKeysByChunk = new Long2ObjectOpenHashMap<>();
    private final LongOpenHashSet dirtySections = new LongOpenHashSet();
    private final ArrayDeque<DeferredCleanupCursor> deferredStateCleanup = new ArrayDeque<>();

    private ResourceLocation activeDimensionId = null;

    @Override
    public void resetClientState() {
        synchronized (this.lock) {
            this.clearAll();
            this.activeDimensionId = null;
        }
    }

    @Override
    public void applyDamage(ResourceLocation dimensionId, long posLong, int damageState) {
        synchronized (this.lock) {
            this.krakk$ensureActiveDimension(dimensionId, "applyDamage");

            int clampedState = krakk$clampDamageState(damageState);
            if (clampedState <= 0) {
                this.removeDamageState(posLong);
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk overlay applyDamage remove: dim={} pos=({}, {}, {})",
                            dimensionId,
                            BlockPos.getX(posLong),
                            BlockPos.getY(posLong),
                            BlockPos.getZ(posLong)
                    );
                }
                return;
            }

            this.putDamageState(posLong, (byte) clampedState);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk overlay applyDamage set: dim={} pos=({}, {}, {}) state={} trackedBlocks={}",
                        dimensionId,
                        BlockPos.getX(posLong),
                        BlockPos.getY(posLong),
                        BlockPos.getZ(posLong),
                        clampedState,
                        this.damageStates.size()
                );
            }
        }
    }

    @Override
    public void applySection(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates) {
        synchronized (this.lock) {
            this.krakk$ensureActiveDimension(dimensionId, "applySection");

            long sectionKey = SectionPos.asLong(sectionX, sectionY, sectionZ);
            long chunkKey = toChunkKey(sectionX, sectionZ);
            this.clearSection(sectionKey, chunkKey);

            for (Short2ByteMap.Entry entry : sectionStates.short2ByteEntrySet()) {
                int clampedState = krakk$clampDamageState(entry.getByteValue());
                if (clampedState <= 0) {
                    continue;
                }

                short localIndex = entry.getShortKey();
                int localX = localIndex & 15;
                int localZ = (localIndex >> 4) & 15;
                int localY = (localIndex >> 8) & 15;

                int blockX = (sectionX << 4) | localX;
                int blockY = (sectionY << 4) | localY;
                int blockZ = (sectionZ << 4) | localZ;
                long posLong = BlockPos.asLong(blockX, blockY, blockZ);
                this.putDamageState(posLong, sectionKey, chunkKey, (byte) clampedState);
            }

            this.dirtySections.add(sectionKey);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk overlay applySection: dim={} section=({}, {}, {}) entries={} trackedBlocks={} dirtySections={}",
                        dimensionId,
                        sectionX,
                        sectionY,
                        sectionZ,
                        sectionStates.size(),
                        this.damageStates.size(),
                        this.dirtySections.size()
                );
            }
        }
    }

    @Override
    public void applySectionDelta(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates) {
        synchronized (this.lock) {
            this.krakk$ensureActiveDimension(dimensionId, "applySectionDelta");

            long sectionKey = SectionPos.asLong(sectionX, sectionY, sectionZ);
            long chunkKey = toChunkKey(sectionX, sectionZ);
            for (Short2ByteMap.Entry entry : sectionStates.short2ByteEntrySet()) {
                int localIndex = entry.getShortKey() & 0x0FFF;
                int localX = localIndex & 15;
                int localZ = (localIndex >> 4) & 15;
                int localY = (localIndex >> 8) & 15;

                int blockX = (sectionX << 4) | localX;
                int blockY = (sectionY << 4) | localY;
                int blockZ = (sectionZ << 4) | localZ;
                long posLong = BlockPos.asLong(blockX, blockY, blockZ);

                int clampedState = krakk$clampDamageState(entry.getByteValue());
                if (clampedState <= 0) {
                    this.removeDamageState(posLong, sectionKey, chunkKey);
                } else {
                    this.putDamageState(posLong, sectionKey, chunkKey, (byte) clampedState);
                }
            }
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk overlay applySectionDelta: dim={} section=({}, {}, {}) entries={} trackedBlocks={} dirtySections={}",
                        dimensionId,
                        sectionX,
                        sectionY,
                        sectionZ,
                        sectionStates.size(),
                        this.damageStates.size(),
                        this.dirtySections.size()
                );
            }
        }
    }

    @Override
    public void clearChunk(ResourceLocation dimensionId, int chunkX, int chunkZ) {
        synchronized (this.lock) {
            if (this.activeDimensionId == null || !this.activeDimensionId.equals(dimensionId)) {
                return;
            }

            long chunkKey = toChunkKey(chunkX, chunkZ);
            LongOpenHashSet positions = this.damagePositionsByChunk.remove(chunkKey);
            LongOpenHashSet chunkSectionKeys = this.sectionKeysByChunk.remove(chunkKey);
            if ((positions == null || positions.isEmpty()) && (chunkSectionKeys == null || chunkSectionKeys.isEmpty())) {
                return;
            }

            if (chunkSectionKeys != null) {
                LongIterator sectionIterator = chunkSectionKeys.iterator();
                while (sectionIterator.hasNext()) {
                    long sectionKey = sectionIterator.nextLong();
                    this.damagePositionsBySection.remove(sectionKey);
                    this.dirtySections.add(sectionKey);
                }
            }
            if (positions != null && !positions.isEmpty()) {
                // Defer heavy per-position cleanup to amortize chunk unload cost across frames.
                this.deferredStateCleanup.addLast(new DeferredCleanupCursor(positions));
            }
            this.krakk$runDeferredStateCleanup(KRAKK_CLEAR_CHUNK_CLEANUP_BUDGET);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk overlay clearChunk: dim={} chunk=({}, {}) deferred={} trackedBlocks={} deferredQueue={}",
                        dimensionId,
                        chunkX,
                        chunkZ,
                        positions == null ? 0 : positions.size(),
                        this.damageStates.size(),
                        this.deferredStateCleanup.size()
                );
            }
        }
    }

    @Override
    public float getMiningBaseline(ResourceLocation dimensionId, long posLong) {
        synchronized (this.lock) {
            if (this.activeDimensionId == null || !this.activeDimensionId.equals(dimensionId)) {
                return 0.0F;
            }
            int damageState = krakk$clampDamageState(this.damageStates.get(posLong));
            return damageState / 15.0F;
        }
    }

    @Override
    public long[] consumeDirtySections(ResourceLocation dimensionId) {
        synchronized (this.lock) {
            this.krakk$runDeferredStateCleanup(KRAKK_DEFERRED_CLEANUP_BUDGET);
            if (this.activeDimensionId == null || !this.activeDimensionId.equals(dimensionId) || this.dirtySections.isEmpty()) {
                return new long[0];
            }
            long[] sectionKeys = this.dirtySections.toLongArray();
            this.dirtySections.clear();
            return sectionKeys;
        }
    }

    @Override
    public Long2ByteOpenHashMap snapshotSection(ResourceLocation dimensionId, long sectionKey) {
        synchronized (this.lock) {
            if (this.activeDimensionId == null || !this.activeDimensionId.equals(dimensionId)) {
                return new Long2ByteOpenHashMap();
            }

            LongOpenHashSet positions = this.damagePositionsBySection.get(sectionKey);
            if (positions == null || positions.isEmpty()) {
                return new Long2ByteOpenHashMap();
            }

            Long2ByteOpenHashMap snapshot = new Long2ByteOpenHashMap(positions.size());
            LongIterator iterator = positions.iterator();
            while (iterator.hasNext()) {
                long posLong = iterator.nextLong();
                int damageState = this.damageStates.get(posLong);
                if (damageState > 0) {
                    snapshot.put(posLong, (byte) damageState);
                }
            }
            return snapshot;
        }
    }

    @Override
    public long[] snapshotSectionsInChunkRange(ResourceLocation dimensionId, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        synchronized (this.lock) {
            if (this.activeDimensionId == null || !this.activeDimensionId.equals(dimensionId) || this.damagePositionsBySection.isEmpty()) {
                return new long[0];
            }

            int fromChunkX = Math.min(minChunkX, maxChunkX);
            int toChunkX = Math.max(minChunkX, maxChunkX);
            int fromChunkZ = Math.min(minChunkZ, maxChunkZ);
            int toChunkZ = Math.max(minChunkZ, maxChunkZ);

            LongArrayList sectionKeys = new LongArrayList();
            for (int chunkX = fromChunkX; chunkX <= toChunkX; chunkX++) {
                for (int chunkZ = fromChunkZ; chunkZ <= toChunkZ; chunkZ++) {
                    long chunkKey = toChunkKey(chunkX, chunkZ);
                    LongOpenHashSet chunkSectionKeys = this.sectionKeysByChunk.get(chunkKey);
                    if (chunkSectionKeys == null || chunkSectionKeys.isEmpty()) {
                        continue;
                    }
                    LongIterator sectionIterator = chunkSectionKeys.iterator();
                    while (sectionIterator.hasNext()) {
                        sectionKeys.add(sectionIterator.nextLong());
                    }
                }
            }
            return sectionKeys.toLongArray();
        }
    }

    private void clearAll() {
        this.damageStates.clear();
        this.damagePositionsByChunk.clear();
        this.damagePositionsBySection.clear();
        this.sectionKeysByChunk.clear();
        this.dirtySections.clear();
        this.deferredStateCleanup.clear();
    }

    private void putDamageState(long posLong, byte damageState) {
        long sectionKey = toSectionKeyFromPos(posLong);
        long chunkKey = toChunkKeyFromPos(posLong);
        this.putDamageState(posLong, sectionKey, chunkKey, damageState);
    }

    private void putDamageState(long posLong, long sectionKey, long chunkKey, byte damageState) {
        byte previous = this.damageStates.put(posLong, damageState);
        if (previous == damageState) {
            // Re-emitted identical states are used as a render invalidation signal
            // (for neighbor/blockstate changes that alter visuals without changing damage value).
            this.dirtySections.add(sectionKey);
            return;
        }

        this.dirtySections.add(sectionKey);
        if (previous > 0) {
            return;
        }

        LongOpenHashSet chunkPositions = this.damagePositionsByChunk.get(chunkKey);
        if (chunkPositions == null) {
            chunkPositions = new LongOpenHashSet();
            this.damagePositionsByChunk.put(chunkKey, chunkPositions);
        }
        chunkPositions.add(posLong);

        LongOpenHashSet sectionPositions = this.damagePositionsBySection.get(sectionKey);
        if (sectionPositions == null) {
            sectionPositions = new LongOpenHashSet();
            this.damagePositionsBySection.put(sectionKey, sectionPositions);
            LongOpenHashSet chunkSectionKeys = this.sectionKeysByChunk.get(chunkKey);
            if (chunkSectionKeys == null) {
                chunkSectionKeys = new LongOpenHashSet();
                this.sectionKeysByChunk.put(chunkKey, chunkSectionKeys);
            }
            chunkSectionKeys.add(sectionKey);
        }
        sectionPositions.add(posLong);
    }

    private void removeDamageState(long posLong) {
        long sectionKey = toSectionKeyFromPos(posLong);
        long chunkKey = toChunkKeyFromPos(posLong);
        this.removeDamageState(posLong, sectionKey, chunkKey);
    }

    private void removeDamageState(long posLong, long sectionKey, long chunkKey) {
        byte previous = this.damageStates.remove(posLong);
        if (previous <= 0) {
            return;
        }

        this.dirtySections.add(sectionKey);

        LongOpenHashSet chunkPositions = this.damagePositionsByChunk.get(chunkKey);
        if (chunkPositions != null) {
            chunkPositions.remove(posLong);
            if (chunkPositions.isEmpty()) {
                this.damagePositionsByChunk.remove(chunkKey);
            }
        }

        LongOpenHashSet sectionPositions = this.damagePositionsBySection.get(sectionKey);
        if (sectionPositions != null) {
            sectionPositions.remove(posLong);
            if (sectionPositions.isEmpty()) {
                this.damagePositionsBySection.remove(sectionKey);
                LongOpenHashSet chunkSectionKeys = this.sectionKeysByChunk.get(chunkKey);
                if (chunkSectionKeys != null) {
                    chunkSectionKeys.remove(sectionKey);
                    if (chunkSectionKeys.isEmpty()) {
                        this.sectionKeysByChunk.remove(chunkKey);
                    }
                }
            }
        }
    }

    private void clearSection(long sectionKey, long chunkKey) {
        LongOpenHashSet sectionPositions = this.damagePositionsBySection.remove(sectionKey);
        if (sectionPositions == null || sectionPositions.isEmpty()) {
            return;
        }

        this.dirtySections.add(sectionKey);
        long[] posLongs = sectionPositions.toLongArray();
        for (long posLong : posLongs) {
            this.damageStates.remove(posLong);
        }

        LongOpenHashSet chunkPositions = this.damagePositionsByChunk.get(chunkKey);
        if (chunkPositions != null) {
            for (long posLong : posLongs) {
                chunkPositions.remove(posLong);
            }
            if (chunkPositions.isEmpty()) {
                this.damagePositionsByChunk.remove(chunkKey);
            }
        }

        LongOpenHashSet chunkSectionKeys = this.sectionKeysByChunk.get(chunkKey);
        if (chunkSectionKeys != null) {
            chunkSectionKeys.remove(sectionKey);
            if (chunkSectionKeys.isEmpty()) {
                this.sectionKeysByChunk.remove(chunkKey);
            }
        }
    }

    private void krakk$runDeferredStateCleanup(int budget) {
        int remaining = Math.max(0, budget);
        while (remaining > 0 && !this.deferredStateCleanup.isEmpty()) {
            DeferredCleanupCursor cursor = this.deferredStateCleanup.peekFirst();
            if (cursor == null) {
                break;
            }
            while (remaining > 0 && cursor.iterator.hasNext()) {
                long posLong = cursor.iterator.nextLong();
                cursor.iterator.remove();
                remaining--;

                long chunkKey = toChunkKeyFromPos(posLong);
                LongOpenHashSet activeChunkPositions = this.damagePositionsByChunk.get(chunkKey);
                if (activeChunkPositions != null && activeChunkPositions.contains(posLong)) {
                    continue;
                }

                long sectionKey = toSectionKeyFromPos(posLong);
                LongOpenHashSet activeSectionPositions = this.damagePositionsBySection.get(sectionKey);
                if (activeSectionPositions != null && activeSectionPositions.contains(posLong)) {
                    continue;
                }

                this.damageStates.remove(posLong);
            }
            if (!cursor.iterator.hasNext()) {
                this.deferredStateCleanup.removeFirst();
            }
        }
    }

    private static long toChunkKeyFromPos(long posLong) {
        return toChunkKey(BlockPos.getX(posLong) >> 4, BlockPos.getZ(posLong) >> 4);
    }

    private static long toSectionKeyFromPos(long posLong) {
        return SectionPos.asLong(BlockPos.getX(posLong) >> 4, BlockPos.getY(posLong) >> 4, BlockPos.getZ(posLong) >> 4);
    }

    private static long toChunkKey(int chunkX, int chunkZ) {
        return ((long) chunkX << 32) ^ (chunkZ & 0xFFFFFFFFL);
    }

    private void krakk$ensureActiveDimension(ResourceLocation dimensionId, String source) {
        if (dimensionId.equals(this.activeDimensionId)) {
            return;
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk overlay dimension swap on {}: previousDim={} nextDim={}",
                    source,
                    this.activeDimensionId,
                    dimensionId
            );
        }
        this.clearAll();
        this.activeDimensionId = dimensionId;
    }

    private static int krakk$clampDamageState(int damageState) {
        return Math.max(0, Math.min(15, damageState));
    }

    private static final class DeferredCleanupCursor {
        private final LongIterator iterator;

        private DeferredCleanupCursor(LongOpenHashSet positions) {
            this.iterator = positions.iterator();
        }
    }
}
