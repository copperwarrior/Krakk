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

public final class KrakkClientOverlayRuntime implements KrakkClientOverlayApi {
    private static final Logger LOGGER = LogUtils.getLogger();

    private final Object lock = new Object();
    private final Long2ByteOpenHashMap damageStates = new Long2ByteOpenHashMap();
    private final Long2ObjectOpenHashMap<LongOpenHashSet> damagePositionsByChunk = new Long2ObjectOpenHashMap<>();
    private final Long2ObjectOpenHashMap<LongOpenHashSet> damagePositionsBySection = new Long2ObjectOpenHashMap<>();
    private final LongOpenHashSet dirtySections = new LongOpenHashSet();

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
            if (!dimensionId.equals(this.activeDimensionId)) {
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk overlay dimension swap on applyDamage: previousDim={} nextDim={}",
                            this.activeDimensionId,
                            dimensionId
                    );
                }
                this.clearAll();
                this.activeDimensionId = dimensionId;
            }

            int clampedState = Math.max(0, Math.min(15, damageState));
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
            if (!dimensionId.equals(this.activeDimensionId)) {
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk overlay dimension swap on applySection: previousDim={} nextDim={}",
                            this.activeDimensionId,
                            dimensionId
                    );
                }
                this.clearAll();
                this.activeDimensionId = dimensionId;
            }

            long sectionKey = SectionPos.asLong(sectionX, sectionY, sectionZ);
            this.clearSection(sectionKey);

            for (Short2ByteMap.Entry entry : sectionStates.short2ByteEntrySet()) {
                int clampedState = Math.max(0, Math.min(15, entry.getByteValue()));
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
                this.putDamageState(posLong, (byte) clampedState);
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
            if (!dimensionId.equals(this.activeDimensionId)) {
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk overlay dimension swap on applySectionDelta: previousDim={} nextDim={}",
                            this.activeDimensionId,
                            dimensionId
                    );
                }
                this.clearAll();
                this.activeDimensionId = dimensionId;
            }

            for (Short2ByteMap.Entry entry : sectionStates.short2ByteEntrySet()) {
                int localIndex = entry.getShortKey() & 0x0FFF;
                int localX = localIndex & 15;
                int localZ = (localIndex >> 4) & 15;
                int localY = (localIndex >> 8) & 15;

                int blockX = (sectionX << 4) | localX;
                int blockY = (sectionY << 4) | localY;
                int blockZ = (sectionZ << 4) | localZ;
                long posLong = BlockPos.asLong(blockX, blockY, blockZ);

                int clampedState = Math.max(0, Math.min(15, entry.getByteValue()));
                if (clampedState <= 0) {
                    this.removeDamageState(posLong);
                } else {
                    this.putDamageState(posLong, (byte) clampedState);
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
            LongOpenHashSet positions = this.damagePositionsByChunk.get(chunkKey);
            if (positions == null || positions.isEmpty()) {
                return;
            }

            long[] posLongs = positions.toLongArray();
            for (long posLong : posLongs) {
                this.removeDamageState(posLong);
            }
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk overlay clearChunk: dim={} chunk=({}, {}) removed={} trackedBlocks={}",
                        dimensionId,
                        chunkX,
                        chunkZ,
                        posLongs.length,
                        this.damageStates.size()
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
            int damageState = Math.max(0, Math.min(15, this.damageStates.get(posLong)));
            return damageState / 15.0F;
        }
    }

    @Override
    public long[] consumeDirtySections(ResourceLocation dimensionId) {
        synchronized (this.lock) {
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

            LongArrayList sectionKeys = new LongArrayList(this.damagePositionsBySection.size());
            LongIterator iterator = this.damagePositionsBySection.keySet().iterator();
            while (iterator.hasNext()) {
                long sectionKey = iterator.nextLong();
                int sectionX = SectionPos.x(sectionKey);
                int sectionZ = SectionPos.z(sectionKey);
                if (sectionX < fromChunkX || sectionX > toChunkX || sectionZ < fromChunkZ || sectionZ > toChunkZ) {
                    continue;
                }
                sectionKeys.add(sectionKey);
            }
            return sectionKeys.toLongArray();
        }
    }

    private void clearAll() {
        this.damageStates.clear();
        this.damagePositionsByChunk.clear();
        this.damagePositionsBySection.clear();
        this.dirtySections.clear();
    }

    private void putDamageState(long posLong, byte damageState) {
        byte previous = this.damageStates.put(posLong, damageState);
        long sectionKey = toSectionKeyFromPos(posLong);
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

        long chunkKey = toChunkKeyFromPos(posLong);
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
        }
        sectionPositions.add(posLong);
    }

    private void removeDamageState(long posLong) {
        byte previous = this.damageStates.remove(posLong);
        if (previous <= 0) {
            return;
        }

        long sectionKey = toSectionKeyFromPos(posLong);
        this.dirtySections.add(sectionKey);

        long chunkKey = toChunkKeyFromPos(posLong);
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
            }
        }
    }

    private void clearSection(long sectionKey) {
        LongOpenHashSet sectionPositions = this.damagePositionsBySection.get(sectionKey);
        if (sectionPositions == null || sectionPositions.isEmpty()) {
            return;
        }

        this.dirtySections.add(sectionKey);
        long[] posLongs = sectionPositions.toLongArray();
        for (long posLong : posLongs) {
            this.removeDamageState(posLong);
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
}
