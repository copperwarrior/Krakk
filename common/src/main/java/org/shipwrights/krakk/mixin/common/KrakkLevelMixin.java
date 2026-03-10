package org.shipwrights.krakk.mixin.common;

import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.FallingBlock;
import net.minecraft.world.level.block.piston.MovingPistonBlock;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.api.KrakkApi;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(Level.class)
public abstract class KrakkLevelMixin {
    @Inject(method = "onBlockStateChange", at = @At("TAIL"))
    private void krakk$clearDamageWhenBlockRemoved(BlockPos blockPos, BlockState oldState, BlockState newState, CallbackInfo ci) {
        Level level = (Level) (Object) this;
        if (level.isClientSide() || !(level instanceof ServerLevel serverLevel)) {
            return;
        }
        if (oldState.isAir()) {
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
