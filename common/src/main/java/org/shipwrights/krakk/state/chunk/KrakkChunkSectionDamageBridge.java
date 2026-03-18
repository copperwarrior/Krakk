package org.shipwrights.krakk.state.chunk;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class KrakkChunkSectionDamageBridge {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final Map<Class<?>, Method> CHUNK_SOURCE_GET_CHUNK_NOW_METHODS = new ConcurrentHashMap<>();
    private static final Set<Class<?>> CHUNK_SOURCE_NO_GET_CHUNK_NOW_METHOD = ConcurrentHashMap.newKeySet();

    private KrakkChunkSectionDamageBridge() {
    }

    public static void applyBlockDamage(Level level, long posLong, int damageState) {
        if (level == null) {
            return;
        }
        int sectionX = SectionPos.blockToSectionCoord(BlockPos.getX(posLong));
        int sectionY = SectionPos.blockToSectionCoord(BlockPos.getY(posLong));
        int sectionZ = SectionPos.blockToSectionCoord(BlockPos.getZ(posLong));
        short localIndex = localIndexFromPosLong(posLong);
        KrakkBlockDamageSectionAccess sectionAccess = getSectionAccess(level, sectionX, sectionY, sectionZ);
        if (sectionAccess == null) {
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk bridge applyBlockDamage dropped: missing section access dim={} section=({}, {}, {}) pos=({}, {}, {}) state={}",
                        level.dimension().location(),
                        sectionX,
                        sectionY,
                        sectionZ,
                        BlockPos.getX(posLong),
                        BlockPos.getY(posLong),
                        BlockPos.getZ(posLong),
                        damageState
                );
            }
            return;
        }

        int clamped = clampDamageState(damageState);
        if (clamped <= 0) {
            sectionAccess.krakk$removeDamageState(localIndex);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk bridge applyBlockDamage removed: dim={} section=({}, {}, {}) pos=({}, {}, {}) localIndex={}",
                        level.dimension().location(),
                        sectionX,
                        sectionY,
                        sectionZ,
                        BlockPos.getX(posLong),
                        BlockPos.getY(posLong),
                        BlockPos.getZ(posLong),
                        localIndex & 0x0FFF
                );
            }
            return;
        }
        sectionAccess.krakk$setDamageState(localIndex, (byte) clamped);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk bridge applyBlockDamage set: dim={} section=({}, {}, {}) pos=({}, {}, {}) localIndex={} state={}",
                    level.dimension().location(),
                    sectionX,
                    sectionY,
                    sectionZ,
                    BlockPos.getX(posLong),
                    BlockPos.getY(posLong),
                    BlockPos.getZ(posLong),
                    localIndex & 0x0FFF,
                    clamped
            );
        }
    }

    public static void applySection(Level level,
                                    int sectionX,
                                    int sectionY,
                                    int sectionZ,
                                    Short2ByteOpenHashMap states,
                                    boolean replaceExisting) {
        if (level == null) {
            return;
        }

        KrakkBlockDamageSectionAccess sectionAccess = getSectionAccess(level, sectionX, sectionY, sectionZ);
        if (sectionAccess == null) {
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk bridge applySection dropped: missing section access dim={} section=({}, {}, {}) replaceExisting={} entries={}",
                        level.dimension().location(),
                        sectionX,
                        sectionY,
                        sectionZ,
                        replaceExisting,
                        states.size()
                );
            }
            return;
        }
        if (replaceExisting) {
            sectionAccess.krakk$clearDamageStates();
        }

        int upserts = 0;
        int removals = 0;
        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            short localIndex = (short) (entry.getShortKey() & 0x0FFF);
            int clamped = clampDamageState(entry.getByteValue());
            if (clamped <= 0) {
                sectionAccess.krakk$removeDamageState(localIndex);
                removals++;
            } else {
                sectionAccess.krakk$setDamageState(localIndex, (byte) clamped);
                upserts++;
            }
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk bridge applySection applied: dim={} section=({}, {}, {}) replaceExisting={} entries={} upserts={} removals={}",
                    level.dimension().location(),
                    sectionX,
                    sectionY,
                    sectionZ,
                    replaceExisting,
                    states.size(),
                    upserts,
                    removals
            );
        }
    }

    public static Long2ByteOpenHashMap snapshotSection(Level level, long sectionKey) {
        Long2ByteOpenHashMap snapshot = new Long2ByteOpenHashMap();
        if (level == null) {
            return snapshot;
        }

        int sectionX = SectionPos.x(sectionKey);
        int sectionY = SectionPos.y(sectionKey);
        int sectionZ = SectionPos.z(sectionKey);
        KrakkBlockDamageSectionAccess sectionAccess = getSectionAccess(level, sectionX, sectionY, sectionZ);
        if (sectionAccess == null) {
            return snapshot;
        }

        Short2ByteOpenHashMap states = sectionAccess.krakk$getDamageStates();
        if (states.isEmpty()) {
            return snapshot;
        }

        int baseX = sectionX << 4;
        int baseY = sectionY << 4;
        int baseZ = sectionZ << 4;
        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            int damageState = clampDamageState(entry.getByteValue());
            if (damageState <= 0) {
                continue;
            }
            short localIndex = (short) (entry.getShortKey() & 0x0FFF);
            int localX = localIndex & 15;
            int localZ = (localIndex >> 4) & 15;
            int localY = (localIndex >> 8) & 15;
            long posLong = BlockPos.asLong(baseX | localX, baseY | localY, baseZ | localZ);
            snapshot.put(posLong, (byte) damageState);
        }
        return snapshot;
    }

    public static void collectSectionKeysInChunkRange(Level level,
                                                      int minChunkX,
                                                      int maxChunkX,
                                                      int minChunkZ,
                                                      int maxChunkZ,
                                                      LongOpenHashSet outSectionKeys) {
        if (level == null || outSectionKeys == null) {
            return;
        }

        for (int chunkX = minChunkX; chunkX <= maxChunkX; chunkX++) {
            for (int chunkZ = minChunkZ; chunkZ <= maxChunkZ; chunkZ++) {
                LevelChunk chunk = getChunkNow(level, chunkX, chunkZ);
                if (chunk == null) {
                    continue;
                }
                LevelChunkSection[] sections = chunk.getSections();
                int sectionY = chunk.getMinBuildHeight() >> 4;
                for (LevelChunkSection section : sections) {
                    if (section instanceof KrakkBlockDamageSectionAccess access
                            && !access.krakk$getDamageStates().isEmpty()) {
                        outSectionKeys.add(SectionPos.asLong(chunkX, sectionY, chunkZ));
                    }
                    sectionY++;
                }
            }
        }
    }

    public static float fallbackMiningBaseline(Level level, long posLong) {
        if (level == null) {
            return 0.0F;
        }

        int sectionX = SectionPos.blockToSectionCoord(BlockPos.getX(posLong));
        int sectionY = SectionPos.blockToSectionCoord(BlockPos.getY(posLong));
        int sectionZ = SectionPos.blockToSectionCoord(BlockPos.getZ(posLong));
        KrakkBlockDamageSectionAccess sectionAccess = getSectionAccess(level, sectionX, sectionY, sectionZ);
        if (sectionAccess == null) {
            return 0.0F;
        }

        int damageState = clampDamageState(sectionAccess.krakk$getDamageStates().get(localIndexFromPosLong(posLong)));
        return damageState <= 0 ? 0.0F : (damageState / 15.0F);
    }

    private static KrakkBlockDamageSectionAccess getSectionAccess(Level level, int sectionX, int sectionY, int sectionZ) {
        LevelChunk chunk = getChunkNow(level, sectionX, sectionZ);
        if (chunk == null) {
            return null;
        }

        int sectionIndex = sectionY - (chunk.getMinBuildHeight() >> 4);
        LevelChunkSection[] sections = chunk.getSections();
        if (sectionIndex < 0 || sectionIndex >= sections.length) {
            return null;
        }

        LevelChunkSection section = sections[sectionIndex];
        if (section instanceof KrakkBlockDamageSectionAccess access) {
            return access;
        }
        return null;
    }

    private static LevelChunk getChunkNow(Level level, int chunkX, int chunkZ) {
        Object chunkSource = level.getChunkSource();
        if (chunkSource == null) {
            return null;
        }

        Class<?> chunkSourceClass = chunkSource.getClass();
        if (CHUNK_SOURCE_NO_GET_CHUNK_NOW_METHOD.contains(chunkSourceClass)) {
            return null;
        }

        Method method = CHUNK_SOURCE_GET_CHUNK_NOW_METHODS.get(chunkSourceClass);
        if (method == null) {
            try {
                method = chunkSourceClass.getMethod("getChunkNow", int.class, int.class);
                CHUNK_SOURCE_GET_CHUNK_NOW_METHODS.put(chunkSourceClass, method);
            } catch (NoSuchMethodException exception) {
                CHUNK_SOURCE_NO_GET_CHUNK_NOW_METHOD.add(chunkSourceClass);
                return null;
            }
        }

        try {
            Object chunk = method.invoke(chunkSource, chunkX, chunkZ);
            if (chunk instanceof LevelChunk levelChunk) {
                return levelChunk;
            }
            return null;
        } catch (ReflectiveOperationException exception) {
            CHUNK_SOURCE_GET_CHUNK_NOW_METHODS.remove(chunkSourceClass);
            CHUNK_SOURCE_NO_GET_CHUNK_NOW_METHOD.add(chunkSourceClass);
            return null;
        }
    }

    private static short localIndexFromPosLong(long posLong) {
        int x = BlockPos.getX(posLong);
        int y = BlockPos.getY(posLong);
        int z = BlockPos.getZ(posLong);
        return (short) (((y & 15) << 8) | ((z & 15) << 4) | (x & 15));
    }

    private static int clampDamageState(int damageState) {
        return Math.max(0, Math.min(15, damageState));
    }
}
