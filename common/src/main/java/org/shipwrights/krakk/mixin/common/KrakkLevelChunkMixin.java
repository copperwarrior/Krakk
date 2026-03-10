package org.shipwrights.krakk.mixin.common;

import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.FallingBlock;
import net.minecraft.world.level.block.piston.MovingPistonBlock;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.ProtoChunk;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(LevelChunk.class)
public abstract class KrakkLevelChunkMixin {
    @Shadow
    @Final
    private Level level;

    @Inject(
            method = "<init>(Lnet/minecraft/server/level/ServerLevel;Lnet/minecraft/world/level/chunk/ProtoChunk;Lnet/minecraft/world/level/chunk/LevelChunk$PostLoadProcessor;)V",
            at = @At("TAIL"),
            require = 0
    )
    private void krakk$copyDamageFromProtoChunkExact(ServerLevel serverLevel, ProtoChunk protoChunk,
                                                     LevelChunk.PostLoadProcessor postLoadProcessor, CallbackInfo ci) {
        krakk$copyDamageFromProto(protoChunk);
    }

    @Unique
    private void krakk$copyDamageFromProto(ProtoChunk protoChunk) {
        if (!(protoChunk instanceof KrakkBlockDamageChunkAccess sourceAccess)) {
            return;
        }
        if (!((Object) this instanceof KrakkBlockDamageChunkAccess targetAccess)) {
            return;
        }

        targetAccess.krakk$getBlockDamageStorage().copyFrom(sourceAccess.krakk$getBlockDamageStorage());
    }

    @Inject(method = "setBlockState", at = @At("RETURN"))
    private void krakk$syncDamageWithChunkStateWrites(BlockPos blockPos, BlockState newState, boolean moved,
                                                      CallbackInfoReturnable<BlockState> cir) {
        if (this.level.isClientSide() || !(this.level instanceof ServerLevel serverLevel)) {
            return;
        }

        BlockState oldState = cir.getReturnValue();
        if (oldState == null) {
            return;
        }
        if (oldState.getBlock() == newState.getBlock()) {
            return;
        }

        if (oldState.getBlock() instanceof MovingPistonBlock && !newState.isAir()) {
            KrakkApi.damage().transferLikelyPistonCompletionDamage(serverLevel, blockPos, newState);
            return;
        }

        // Piston movement migrates damage explicitly via KrakkPistonMovingBlockEntityMixin.
        if (oldState.getBlock() instanceof MovingPistonBlock || newState.getBlock() instanceof MovingPistonBlock) {
            return;
        }
        if (newState.isAir() && KrakkApi.damage().isLikelyPistonMoveSource(serverLevel, blockPos, oldState)) {
            return;
        }

        // Falling blocks migrate damage when they become entities.
        if (oldState.getBlock() instanceof FallingBlock && (newState.isAir() || !newState.getFluidState().isEmpty())) {
            return;
        }

        KrakkApi.damage().clearDamage(serverLevel, blockPos);
    }
}
