package org.shipwrights.krakk.mixin.common;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.ai.village.poi.PoiManager;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.ChunkAccess;
import net.minecraft.world.level.chunk.ProtoChunk;
import net.minecraft.world.level.chunk.storage.ChunkSerializer;
import org.shipwrights.krakk.state.chunk.KrakkChunkDamagePersistence;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(ChunkSerializer.class)
public abstract class KrakkChunkSerializerMixin {
    @Inject(method = "write", at = @At("RETURN"), require = 0)
    private static void krakk$writeChunkDamage(ServerLevel level, ChunkAccess chunk,
                                               CallbackInfoReturnable<CompoundTag> cir) {
        KrakkChunkDamagePersistence.writeChunkDamage(chunk, cir.getReturnValue());
    }

    @Inject(method = "read", at = @At("RETURN"), require = 0)
    private static void krakk$readChunkDamage(ServerLevel level, PoiManager poiManager, ChunkPos chunkPos, CompoundTag tag,
                                              CallbackInfoReturnable<ProtoChunk> cir) {
        KrakkChunkDamagePersistence.readChunkDamage(cir.getReturnValue(), tag);
    }
}
