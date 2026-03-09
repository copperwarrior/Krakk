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
            if (nestedLevelTag != null) {
                nestedLevelTag.remove(KrakkBlockDamageChunkStorage.ROOT_TAG);
            }
        } else {
            outTag.put(KrakkBlockDamageChunkStorage.ROOT_TAG, damageTag);
            if (nestedLevelTag != null) {
                nestedLevelTag.put(KrakkBlockDamageChunkStorage.ROOT_TAG, damageTag.copy());
            }
        }
    }

    public static void readChunkDamage(ProtoChunk protoChunk, CompoundTag rootTag) {
        CompoundTag holderTag = rootTag;
        if (!holderTag.contains(KrakkBlockDamageChunkStorage.ROOT_TAG, Tag.TAG_COMPOUND)
                && rootTag.contains("Level", Tag.TAG_COMPOUND)) {
            holderTag = rootTag.getCompound("Level");
        }
        if (!holderTag.contains(KrakkBlockDamageChunkStorage.ROOT_TAG, Tag.TAG_COMPOUND)) {
            return;
        }

        CompoundTag damageTag = holderTag.getCompound(KrakkBlockDamageChunkStorage.ROOT_TAG);
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
