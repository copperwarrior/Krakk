package org.shipwrights.krakk.mixin.common;

import net.minecraft.core.BlockPos;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.item.FallingBlockEntity;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.state.entity.KrakkFallingBlockDamageCarrier;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(FallingBlockEntity.class)
public abstract class KrakkFallingBlockEntityMixin implements KrakkFallingBlockDamageCarrier {
    @Unique
    private static final String KRAKK_CARRIED_DAMAGE_STATE_TAG = "KrakkCarriedDamageState";
    @Unique
    private int krakk$carriedDamageState;

    @Inject(method = "fall", at = @At("RETURN"))
    private static void krakk$captureDamageOnFall(Level level, BlockPos blockPos, BlockState blockState,
                                                  CallbackInfoReturnable<FallingBlockEntity> cir) {
        if (!(level instanceof ServerLevel serverLevel)) {
            return;
        }

        FallingBlockEntity entity = cir.getReturnValue();
        if (!(entity instanceof KrakkFallingBlockDamageCarrier carrier)) {
            return;
        }

        int carriedState = KrakkApi.damage().takeDamageState(serverLevel, blockPos);
        if (carriedState > 0) {
            carrier.krakk$setCarriedDamageState(carriedState);
        }
    }

    @Redirect(
            method = "tick",
            at = @At(
                    value = "INVOKE",
                    target = "Lnet/minecraft/world/level/Level;setBlock(Lnet/minecraft/core/BlockPos;Lnet/minecraft/world/level/block/state/BlockState;I)Z"
            )
    )
    private boolean krakk$applyCarriedDamageOnPlacement(Level level, BlockPos blockPos, BlockState blockState, int flags) {
        boolean placed = level.setBlock(blockPos, blockState, flags);
        if (placed && level instanceof ServerLevel serverLevel && this.krakk$carriedDamageState > 0) {
            KrakkApi.damage().applyTransferredDamageState(serverLevel, blockPos, blockState, this.krakk$carriedDamageState);
            this.krakk$carriedDamageState = 0;
        }
        return placed;
    }

    @Inject(method = "addAdditionalSaveData", at = @At("TAIL"))
    private void krakk$saveCarriedDamageState(CompoundTag compoundTag, CallbackInfo ci) {
        if (this.krakk$carriedDamageState > 0) {
            compoundTag.putByte(KRAKK_CARRIED_DAMAGE_STATE_TAG, (byte) this.krakk$carriedDamageState);
        }
    }

    @Inject(method = "readAdditionalSaveData", at = @At("TAIL"))
    private void krakk$readCarriedDamageState(CompoundTag compoundTag, CallbackInfo ci) {
        if (compoundTag.contains(KRAKK_CARRIED_DAMAGE_STATE_TAG)) {
            this.krakk$carriedDamageState = Math.max(0, Math.min(15, compoundTag.getByte(KRAKK_CARRIED_DAMAGE_STATE_TAG)));
        } else {
            this.krakk$carriedDamageState = 0;
        }
    }

    @Override
    public int krakk$getCarriedDamageState() {
        return this.krakk$carriedDamageState;
    }

    @Override
    public void krakk$setCarriedDamageState(int damageState) {
        this.krakk$carriedDamageState = Math.max(0, Math.min(15, damageState));
    }
}
