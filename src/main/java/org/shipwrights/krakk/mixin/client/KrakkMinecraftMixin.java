package org.shipwrights.krakk.mixin.client;

import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.player.LocalPlayer;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.phys.BlockHitResult;
import net.minecraft.world.phys.HitResult;
import org.shipwrights.krakk.api.KrakkApi;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(net.minecraft.client.Minecraft.class)
public abstract class KrakkMinecraftMixin {
    @Shadow
    public ClientLevel level;

    @Shadow
    public LocalPlayer player;

    @Shadow
    private HitResult hitResult;

    @Inject(method = "setLevel", at = @At("HEAD"))
    private void krakk$resetBlockDamageOverlayOnLevelSwap(ClientLevel nextLevel, CallbackInfo ci) {
        KrakkApi.clientOverlay().resetClientState();
    }

    @Redirect(
            method = "startAttack",
            at = @At(
                    value = "INVOKE",
                    target = "Lnet/minecraft/client/player/LocalPlayer;swing(Lnet/minecraft/world/InteractionHand;)V"
            )
    )
    private void krakk$skipSwingOnInstantMineStartAttack(LocalPlayer localPlayer, InteractionHand hand) {
        if (!this.krakk$shouldSkipInstantMineSwing()) {
            localPlayer.swing(hand);
        }
    }

    @Redirect(
            method = "continueAttack",
            at = @At(
                    value = "INVOKE",
                    target = "Lnet/minecraft/client/player/LocalPlayer;swing(Lnet/minecraft/world/InteractionHand;)V"
            )
    )
    private void krakk$skipSwingOnInstantMineContinueAttack(LocalPlayer localPlayer, InteractionHand hand) {
        if (!this.krakk$shouldSkipInstantMineSwing()) {
            localPlayer.swing(hand);
        }
    }

    private boolean krakk$shouldSkipInstantMineSwing() {
        if (this.level == null || this.player == null || this.hitResult == null || this.player.getAbilities().instabuild) {
            return false;
        }
        if (this.hitResult.getType() != HitResult.Type.BLOCK) {
            return false;
        }

        BlockHitResult blockHitResult = (BlockHitResult) this.hitResult;
        float baselineProgress = KrakkApi.clientOverlay().getMiningBaseline(
                this.level.dimension().location(),
                blockHitResult.getBlockPos().asLong()
        );
        return baselineProgress >= 0.999F;
    }
}
