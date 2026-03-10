package org.shipwrights.krakk.state.chunk;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.Tag;
import net.minecraft.world.level.chunk.ChunkAccess;
import net.minecraft.world.level.chunk.ImposterProtoChunk;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.ProtoChunk;

public final class KrakkChunkDamagePersistence {
    private KrakkChunkDamagePersistence() {
    }

    public static void writeChunkDamage(ChunkAccess chunk, CompoundTag outTag) {
        KrakkBlockDamageChunkStorage storage = null;
        if (chunk instanceof KrakkBlockDamageChunkAccess access) {
            storage = access.krakk$getBlockDamageStorage();
        }

        // Full chunk serialization often goes through ImposterProtoChunk. Use wrapped LevelChunk storage
        // so we persist the authoritative damage data instead of the imposter's empty storage.
        if (chunk instanceof ImposterProtoChunk imposterProtoChunk) {
            LevelChunk wrappedChunk = imposterProtoChunk.getWrapped();
            if (wrappedChunk instanceof KrakkBlockDamageChunkAccess wrappedAccess) {
                storage = wrappedAccess.krakk$getBlockDamageStorage();
            }
        }

        if (storage == null) {
            return;
        }

        CompoundTag nestedLevelTag = outTag.contains("Level", Tag.TAG_COMPOUND) ? outTag.getCompound("Level") : null;
        CompoundTag damageTag = storage.save();
        if (damageTag.isEmpty()) {
            outTag.remove(KrakkBlockDamageChunkStorage.ROOT_TAG);
            outTag.remove(KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG);
            if (nestedLevelTag != null) {
                nestedLevelTag.remove(KrakkBlockDamageChunkStorage.ROOT_TAG);
                nestedLevelTag.remove(KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG);
            }
        } else {
            outTag.put(KrakkBlockDamageChunkStorage.ROOT_TAG, damageTag);
            outTag.remove(KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG);
            if (nestedLevelTag != null) {
                nestedLevelTag.put(KrakkBlockDamageChunkStorage.ROOT_TAG, damageTag.copy());
                nestedLevelTag.remove(KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG);
            }
        }
    }

    public static void readChunkDamage(ProtoChunk protoChunk, CompoundTag rootTag) {
        CompoundTag holderTag = rootTag;
        String damageTagKey = KrakkBlockDamageChunkStorage.ROOT_TAG;
        if (!holderTag.contains(damageTagKey, Tag.TAG_COMPOUND)
                && !holderTag.contains(KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG, Tag.TAG_COMPOUND)
                && rootTag.contains("Level", Tag.TAG_COMPOUND)) {
            holderTag = rootTag.getCompound("Level");
        }

        if (!holderTag.contains(damageTagKey, Tag.TAG_COMPOUND)
                && holderTag.contains(KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG, Tag.TAG_COMPOUND)) {
            damageTagKey = KrakkBlockDamageChunkStorage.LEGACY_ROOT_TAG;
        }

        if (!holderTag.contains(damageTagKey, Tag.TAG_COMPOUND)) {
            return;
        }

        CompoundTag damageTag = holderTag.getCompound(damageTagKey);
        if (protoChunk instanceof KrakkBlockDamageChunkAccess access) {
            access.krakk$getBlockDamageStorage().load(damageTag);
        }

        if (protoChunk instanceof ImposterProtoChunk imposterProtoChunk) {
            LevelChunk wrappedChunk = imposterProtoChunk.getWrapped();
            if (wrappedChunk instanceof KrakkBlockDamageChunkAccess wrappedAccess) {
                wrappedAccess.krakk$getBlockDamageStorage().load(damageTag);
            }
        }
    }
}
