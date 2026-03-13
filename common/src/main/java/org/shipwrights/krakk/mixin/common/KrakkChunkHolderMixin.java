package org.shipwrights.krakk.mixin.common;

import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortSet;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ChunkHolder;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.LevelHeightAccessor;
import net.minecraft.world.level.chunk.LevelChunk;
import org.jetbrains.annotations.Nullable;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkStorage;
import org.shipwrights.krakk.state.network.KrakkChunkHolderAccess;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.util.List;

@Mixin(ChunkHolder.class)
public abstract class KrakkChunkHolderMixin implements KrakkChunkHolderAccess {
    @Shadow
    public abstract @Nullable LevelChunk getTickingChunk();

    @Shadow
    @Final
    private LevelHeightAccessor levelHeightAccessor;

    @Shadow
    @Final
    private ChunkHolder.PlayerProvider playerProvider;

    @Shadow
    private boolean hasChangedSections;

    @Unique
    private ShortSet[] krakk$changedDamagePerSection;

    @Override
    public void krakk$damageStateChanged(BlockPos blockPos) {
        LevelChunk levelChunk = this.getTickingChunk();
        if (levelChunk == null) {
            return;
        }

        this.krakk$ensureChangedSectionStorage();
        int sectionIndex = this.levelHeightAccessor.getSectionIndex(blockPos.getY());
        if (sectionIndex < 0 || sectionIndex >= this.krakk$changedDamagePerSection.length) {
            return;
        }

        ShortSet changedDamage = this.krakk$changedDamagePerSection[sectionIndex];
        if (changedDamage == null) {
            changedDamage = new ShortOpenHashSet();
            this.krakk$changedDamagePerSection[sectionIndex] = changedDamage;
            this.hasChangedSections = true;
        }
        changedDamage.add(SectionPos.sectionRelativePos(blockPos));
    }

    @Inject(method = "broadcastChanges", at = @At("TAIL"))
    private void krakk$broadcastDamageStateChanges(LevelChunk levelChunk, CallbackInfo ci) {
        if (this.krakk$changedDamagePerSection == null) {
            return;
        }

        if (!(levelChunk instanceof KrakkBlockDamageChunkAccess access)) {
            this.krakk$clearTrackedDamageChanges();
            return;
        }

        List<ServerPlayer> players = this.playerProvider.getPlayers(levelChunk.getPos(), false);
        if (players.isEmpty()) {
            this.krakk$clearTrackedDamageChanges();
            return;
        }

        ResourceLocation dimensionId = levelChunk.getLevel().dimension().location();
        KrakkBlockDamageChunkStorage storage = access.krakk$getBlockDamageStorage();
        ChunkPos chunkPos = levelChunk.getPos();

        for (int sectionIndex = 0; sectionIndex < this.krakk$changedDamagePerSection.length; sectionIndex++) {
            ShortSet changedDamage = this.krakk$changedDamagePerSection[sectionIndex];
            if (changedDamage == null || changedDamage.isEmpty()) {
                continue;
            }
            this.krakk$changedDamagePerSection[sectionIndex] = null;

            int sectionY = this.levelHeightAccessor.getSectionYFromSectionIndex(sectionIndex);
            Short2ByteOpenHashMap sectionStates = storage.sectionView(sectionY);
            Short2ByteOpenHashMap changedStates = new Short2ByteOpenHashMap(changedDamage.size());
            ShortIterator iterator = changedDamage.iterator();
            while (iterator.hasNext()) {
                short localIndex = (short) (iterator.nextShort() & 0x0FFF);
                int damageState = sectionStates == null ? 0 : Math.max(0, Math.min(15, sectionStates.get(localIndex)));
                changedStates.put(localIndex, (byte) damageState);
            }

            if (changedStates.isEmpty()) {
                continue;
            }

            if (changedStates.size() == 1) {
                Short2ByteMap.Entry single = changedStates.short2ByteEntrySet().iterator().next();
                long posLong = krakk$toBlockPosLong(chunkPos, sectionY, single.getShortKey());
                KrakkApi.network().sendDamageSyncBatch(players, dimensionId, posLong, single.getByteValue());
            } else {
                KrakkApi.network().sendSectionDeltaBatch(players, dimensionId, chunkPos.x, sectionY, chunkPos.z, changedStates);
            }
        }
    }

    @Unique
    private void krakk$ensureChangedSectionStorage() {
        if (this.krakk$changedDamagePerSection == null || this.krakk$changedDamagePerSection.length != this.levelHeightAccessor.getSectionsCount()) {
            this.krakk$changedDamagePerSection = new ShortSet[this.levelHeightAccessor.getSectionsCount()];
        }
    }

    @Unique
    private void krakk$clearTrackedDamageChanges() {
        if (this.krakk$changedDamagePerSection == null) {
            return;
        }
        for (int i = 0; i < this.krakk$changedDamagePerSection.length; i++) {
            this.krakk$changedDamagePerSection[i] = null;
        }
    }

    @Unique
    private static long krakk$toBlockPosLong(ChunkPos chunkPos, int sectionY, short localIndex) {
        int x = (chunkPos.x << 4) | (localIndex & 15);
        int y = (sectionY << 4) | ((localIndex >> 8) & 15);
        int z = (chunkPos.z << 4) | ((localIndex >> 4) & 15);
        return BlockPos.asLong(x, y, z);
    }
}
