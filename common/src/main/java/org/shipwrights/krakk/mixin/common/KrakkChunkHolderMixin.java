package org.shipwrights.krakk.mixin.common;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortSet;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.server.level.ChunkHolder;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.LevelHeightAccessor;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import org.jetbrains.annotations.Nullable;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageSectionAccess;
import org.shipwrights.krakk.state.network.KrakkChunkHolderAccess;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.slf4j.Logger;

import java.util.List;

@Mixin(ChunkHolder.class)
public abstract class KrakkChunkHolderMixin implements KrakkChunkHolderAccess {
    @Unique
    private static final Logger KRAKK_LOGGER = LogUtils.getLogger();

    @Shadow
    public abstract @Nullable LevelChunk getTickingChunk();

    @Shadow
    @Final
    private ChunkHolder.PlayerProvider playerProvider;

    @Shadow
    @Final
    private LevelHeightAccessor levelHeightAccessor;

    @Shadow
    private boolean hasChangedSections;

    @Unique
    private ShortSet[] krakk$changedDamagePerSection;

    @Inject(method = "<init>", at = @At("RETURN"), require = 0)
    private void krakk$initDamageChangeTracking(ChunkPos chunkPos,
                                                int i,
                                                LevelHeightAccessor levelHeightAccessor,
                                                net.minecraft.world.level.lighting.LevelLightEngine levelLightEngine,
                                                ChunkHolder.LevelChangeListener levelChangeListener,
                                                ChunkHolder.PlayerProvider playerProvider,
                                                CallbackInfo ci) {
        this.krakk$changedDamagePerSection = new ShortSet[levelHeightAccessor.getSectionsCount()];
    }

    @Override
    public boolean krakk$damageStateChanged(BlockPos blockPos) {
        LevelChunk tickingChunk = this.getTickingChunk();
        if (tickingChunk == null) {
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                KRAKK_LOGGER.debug(
                        "Krakk chunk-holder notify dropped: no ticking chunk for pos=({}, {}, {})",
                        blockPos.getX(),
                        blockPos.getY(),
                        blockPos.getZ()
                );
            }
            return false;
        }

        int sectionIndex = this.levelHeightAccessor.getSectionIndex(blockPos.getY());
        if (sectionIndex < 0 || sectionIndex >= this.krakk$changedDamagePerSection.length) {
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                KRAKK_LOGGER.debug(
                        "Krakk chunk-holder notify dropped: section index out of range sectionIndex={} pos=({}, {}, {})",
                        sectionIndex,
                        blockPos.getX(),
                        blockPos.getY(),
                        blockPos.getZ()
                );
            }
            return false;
        }

        ShortSet changed = this.krakk$changedDamagePerSection[sectionIndex];
        if (changed == null) {
            this.hasChangedSections = true;
            changed = new ShortOpenHashSet();
            this.krakk$changedDamagePerSection[sectionIndex] = changed;
        }

        changed.add(SectionPos.sectionRelativePos(blockPos));
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            KRAKK_LOGGER.debug(
                    "Krakk chunk-holder notify queued: chunk=({}, {}) sectionIndex={} dirtyCount={} pos=({}, {}, {})",
                    tickingChunk.getPos().x,
                    tickingChunk.getPos().z,
                    sectionIndex,
                    changed.size(),
                    blockPos.getX(),
                    blockPos.getY(),
                    blockPos.getZ()
            );
        }
        return true;
    }

    @Override
    public List<ServerPlayer> krakk$getTrackingPlayers(ChunkPos chunkPos, boolean onlyOnWatchDistanceEdge) {
        return this.playerProvider.getPlayers(chunkPos, onlyOnWatchDistanceEdge);
    }

    @Inject(method = "broadcastChanges", at = @At("TAIL"), require = 0)
    private void krakk$broadcastDamageChanges(LevelChunk levelChunk, CallbackInfo ci) {
        if (levelChunk == null) {
            return;
        }
        if (this.krakk$changedDamagePerSection == null) {
            return;
        }

        Level level = levelChunk.getLevel();
        List<ServerPlayer> players = this.playerProvider.getPlayers(levelChunk.getPos(), false);
        for (int sectionIndex = 0; sectionIndex < this.krakk$changedDamagePerSection.length; sectionIndex++) {
            ShortSet changedStates = this.krakk$changedDamagePerSection[sectionIndex];
            if (changedStates == null || changedStates.isEmpty()) {
                continue;
            }

            this.krakk$changedDamagePerSection[sectionIndex] = null;
            if (players.isEmpty()) {
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    KRAKK_LOGGER.debug(
                            "Krakk chunk-holder broadcast skipped: no players for chunk=({}, {}) sectionIndex={} changed={}",
                            levelChunk.getPos().x,
                            levelChunk.getPos().z,
                            sectionIndex,
                            changedStates.size()
                    );
                }
                continue;
            }

            int sectionY = this.levelHeightAccessor.getSectionYFromSectionIndex(sectionIndex);
            SectionPos sectionPos = SectionPos.of(levelChunk.getPos(), sectionY);
            LevelChunkSection levelChunkSection = levelChunk.getSection(sectionIndex);
            if (!(levelChunkSection instanceof KrakkBlockDamageSectionAccess access)) {
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    KRAKK_LOGGER.debug(
                            "Krakk chunk-holder broadcast skipped: no damage section access chunk=({}, {}) sectionY={} changed={}",
                            levelChunk.getPos().x,
                            levelChunk.getPos().z,
                            sectionY,
                            changedStates.size()
                    );
                }
                continue;
            }

            Short2ByteOpenHashMap sectionDamageStates = access.krakk$getDamageStates();
            if (changedStates.size() == 1) {
                short packedLocalPos = changedStates.iterator().nextShort();
                BlockPos blockPos = sectionPos.relativeToBlockPos(packedLocalPos);
                short localIndex = krakk$storageLocalIndexFromSectionRelativePos(packedLocalPos);
                int damageState = Math.max(0, Math.min(15, sectionDamageStates.get(localIndex)));
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    KRAKK_LOGGER.debug(
                            "Krakk chunk-holder broadcast single: dim={} chunk=({}, {}) sectionY={} players={} pos=({}, {}, {}) localIndex={} state={}",
                            level.dimension().location(),
                            levelChunk.getPos().x,
                            levelChunk.getPos().z,
                            sectionY,
                            players.size(),
                            blockPos.getX(),
                            blockPos.getY(),
                            blockPos.getZ(),
                            localIndex & 0x0FFF,
                            damageState
                    );
                }
                KrakkApi.network().sendDamageSyncBatch(players, level.dimension().location(), blockPos.asLong(), damageState);
                continue;
            }

            Short2ByteOpenHashMap changedDamageStates = new Short2ByteOpenHashMap(changedStates.size());
            ShortIterator iterator = changedStates.iterator();
            while (iterator.hasNext()) {
                short packedLocalPos = iterator.nextShort();
                short localIndex = krakk$storageLocalIndexFromSectionRelativePos(packedLocalPos);
                byte damageState = (byte) Math.max(0, Math.min(15, sectionDamageStates.get(localIndex)));
                changedDamageStates.put(localIndex, damageState);
            }
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                KRAKK_LOGGER.debug(
                        "Krakk chunk-holder broadcast delta: dim={} chunk=({}, {}) section=({}, {}, {}) players={} changed={} sampleStateCount={}",
                        level.dimension().location(),
                        levelChunk.getPos().x,
                        levelChunk.getPos().z,
                        sectionPos.getX(),
                        sectionPos.getY(),
                        sectionPos.getZ(),
                        players.size(),
                        changedStates.size(),
                        changedDamageStates.size()
                );
            }
            KrakkApi.network().sendSectionDeltaBatch(
                    players,
                    level.dimension().location(),
                    sectionPos.getX(),
                    sectionPos.getY(),
                    sectionPos.getZ(),
                    changedDamageStates
            );
        }
    }

    @Unique
    private static short krakk$storageLocalIndexFromSectionRelativePos(short packedLocalPos) {
        int localX = SectionPos.sectionRelativeX(packedLocalPos);
        int localY = SectionPos.sectionRelativeY(packedLocalPos);
        int localZ = SectionPos.sectionRelativeZ(packedLocalPos);
        return (short) (((localY & 15) << 8) | ((localZ & 15) << 4) | (localX & 15));
    }

}
