package org.shipwrights.krakk.mixin.common;

import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayerGameMode;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.level.BlockGetter;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.api.KrakkApi;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(ServerPlayerGameMode.class)
public abstract class KrakkServerPlayerGameModeMiningMixin {
    @Shadow
    protected ServerLevel level;

    @Inject(method = "incrementDestroyProgress", at = @At("RETURN"), cancellable = true)
    private void krakk$startProgressFromDamage(BlockState blockState, BlockPos blockPos, int unused,
                                               CallbackInfoReturnable<Float> cir) {
        float vanillaProgress = cir.getReturnValueF();
        cir.setReturnValue(this.krakk$scaleProgressFromDamage(blockPos, vanillaProgress));
    }

    @Redirect(
            method = "handleBlockBreakAction",
            at = @At(
                    value = "INVOKE",
                    target = "Lnet/minecraft/world/level/block/state/BlockState;getDestroyProgress(Lnet/minecraft/world/entity/player/Player;Lnet/minecraft/world/level/BlockGetter;Lnet/minecraft/core/BlockPos;)F"
            )
    )
    private float krakk$applyDamageBaselineToServerChecks(BlockState state, Player player, BlockGetter blockGetter, BlockPos blockPos) {
        float vanillaProgress = state.getDestroyProgress(player, blockGetter, blockPos);
        return this.krakk$scaleProgressFromDamage(blockPos, vanillaProgress);
    }

    private float krakk$scaleProgressFromDamage(BlockPos blockPos, float vanillaProgress) {
        float baselineProgress = KrakkApi.damage().getMiningBaseline(this.level, blockPos);
        if (baselineProgress <= 0.0F) {
            return vanillaProgress;
        }
        if (baselineProgress >= 0.999F) {
            return 1.0F;
        }

        float remainingFraction = Math.max(0.001F, 1.0F - baselineProgress);
        return vanillaProgress / remainingFraction;
    }
}
