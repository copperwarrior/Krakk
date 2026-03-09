package org.shipwrights.krakk.mixin.common;

import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.piston.PistonMovingBlockEntity;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.api.KrakkApi;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(PistonMovingBlockEntity.class)
public abstract class KrakkPistonMovingBlockEntityMixin {
    @Unique
    private boolean krakk$damageMigrated;

    @Inject(method = "finalTick", at = @At("TAIL"))
    private void krakk$migrateDamageOnFinalTick(CallbackInfo ci) {
        krakk$migrateDamageIfReady((PistonMovingBlockEntity) (Object) this);
    }

    @Inject(method = "tick", at = @At("TAIL"))
    private static void krakk$migrateDamageOnTickFinalize(Level level, BlockPos blockPos, BlockState blockState,
                                                          PistonMovingBlockEntity movingBlockEntity, CallbackInfo ci) {
        krakk$migrateDamageIfReady(movingBlockEntity);
    }

    @Unique
    private static void krakk$migrateDamageIfReady(PistonMovingBlockEntity movingBlockEntity) {
        KrakkPistonMovingBlockEntityMixin mixin = (KrakkPistonMovingBlockEntityMixin) (Object) movingBlockEntity;
        if (mixin.krakk$damageMigrated) {
            return;
        }

        Level level = movingBlockEntity.getLevel();
        if (!(level instanceof ServerLevel serverLevel)) {
            return;
        }
        if (movingBlockEntity.isSourcePiston()) {
            return;
        }

        BlockPos sourcePos = movingBlockEntity.getBlockPos();
        Direction movementDirection = movingBlockEntity.getMovementDirection();

        BlockState movedState = movingBlockEntity.getMovedState();
        if (movedState.isAir()) {
            return;
        }

        BlockPos resolvedDestination = krakk$resolveDestinationPos(serverLevel, sourcePos, movementDirection, movedState);
        if (resolvedDestination == null) {
            return;
        }

        BlockPos[] sourceCandidates = new BlockPos[]{
                sourcePos,
                sourcePos.relative(movementDirection.getOpposite())
        };
        for (BlockPos sourceCandidate : sourceCandidates) {
            if (sourceCandidate.equals(resolvedDestination)) {
                continue;
            }
            int carriedState = KrakkApi.damage().takeStoredDamageState(serverLevel, sourceCandidate);
            if (carriedState <= 0) {
                continue;
            }
            KrakkApi.damage().applyTransferredDamageState(serverLevel, resolvedDestination, movedState, carriedState);
            mixin.krakk$damageMigrated = true;
            return;
        }
    }

    @Unique
    private static BlockPos krakk$resolveDestinationPos(ServerLevel level, BlockPos basePos,
                                                        Direction movementDirection, BlockState movedState) {
        BlockPos[] destinationCandidates = new BlockPos[]{
                basePos.relative(movementDirection),
                basePos
        };
        for (BlockPos candidate : destinationCandidates) {
            BlockState liveState = level.getBlockState(candidate);
            if (liveState.is(Blocks.MOVING_PISTON)) {
                continue;
            }
            if (liveState.isAir()) {
                continue;
            }
            if (liveState.getBlock() != movedState.getBlock()) {
                continue;
            }
            return candidate;
        }
        return null;
    }
}
