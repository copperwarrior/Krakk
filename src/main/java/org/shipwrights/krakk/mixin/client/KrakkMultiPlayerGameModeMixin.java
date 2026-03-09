package org.shipwrights.krakk.mixin.client;

import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.MultiPlayerGameMode;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import org.shipwrights.krakk.api.KrakkApi;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(MultiPlayerGameMode.class)
public abstract class KrakkMultiPlayerGameModeMixin {
    @Shadow
    @Final
    private Minecraft minecraft;

    @Shadow
    private BlockPos destroyBlockPos;

    @Shadow
    private float destroyProgress;

    @Shadow
    private float destroyTicks;

    @Shadow
    private boolean isDestroying;

    @Shadow
    private int destroyDelay;

    @Shadow
    public abstract boolean continueDestroyBlock(BlockPos blockPos, Direction direction);

    @Inject(method = "startDestroyBlock", at = @At("HEAD"))
    private void krakk$clearDestroyDelayForInstantMine(BlockPos blockPos, Direction direction,
                                                       CallbackInfoReturnable<Boolean> cir) {
        if (this.minecraft.level == null || this.minecraft.player == null || this.minecraft.player.getAbilities().instabuild) {
            return;
        }

        float baselineProgress = KrakkApi.clientOverlay().getMiningBaseline(
                this.minecraft.level.dimension().location(),
                blockPos.asLong()
        );
        if (baselineProgress >= 0.999F) {
            this.destroyDelay = 0;
        }
    }

    @Inject(method = "continueDestroyBlock", at = @At("HEAD"))
    private void krakk$clearCarryoverDestroyDelayForInstantMine(BlockPos blockPos, Direction direction,
                                                                CallbackInfoReturnable<Boolean> cir) {
        if (this.minecraft.level == null || this.minecraft.player == null || this.minecraft.player.getAbilities().instabuild) {
            return;
        }

        float baselineProgress = KrakkApi.clientOverlay().getMiningBaseline(
                this.minecraft.level.dimension().location(),
                blockPos.asLong()
        );
        if (baselineProgress >= 0.999F) {
            this.destroyDelay = 0;
        }
    }

    @Inject(method = "startDestroyBlock", at = @At("RETURN"))
    private void krakk$seedDestroyProgressBaseline(BlockPos blockPos, Direction direction,
                                                   CallbackInfoReturnable<Boolean> cir) {
        if (!cir.getReturnValueZ()) {
            return;
        }

        this.krakk$applyDestroyProgressBaseline(blockPos);

        if (this.isDestroying && blockPos.equals(this.destroyBlockPos) && this.destroyProgress >= 1.0F) {
            this.continueDestroyBlock(blockPos, direction);
        }
    }

    @Inject(method = "continueDestroyBlock", at = @At("HEAD"))
    private void krakk$continueWithDestroyProgressBaseline(BlockPos blockPos, Direction direction,
                                                           CallbackInfoReturnable<Boolean> cir) {
        this.krakk$applyDestroyProgressBaseline(blockPos);
    }

    private void krakk$applyDestroyProgressBaseline(BlockPos blockPos) {
        if (!this.isDestroying || !blockPos.equals(this.destroyBlockPos)) {
            return;
        }
        if (this.minecraft.level == null || this.minecraft.player == null || this.minecraft.player.getAbilities().instabuild) {
            return;
        }

        float baselineProgress = KrakkApi.clientOverlay().getMiningBaseline(
                this.minecraft.level.dimension().location(),
                blockPos.asLong()
        );
        if (baselineProgress <= this.destroyProgress) {
            return;
        }

        this.destroyProgress = baselineProgress;
        this.destroyTicks = Math.max(this.destroyTicks, baselineProgress * 10.0F);
    }
}
