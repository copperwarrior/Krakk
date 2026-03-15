package org.shipwrights.krakk.mixin.common;

import net.minecraft.network.protocol.game.ClientboundLevelChunkWithLightPacket;
import net.minecraft.server.level.ChunkMap;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import org.apache.commons.lang3.mutable.MutableObject;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(ChunkMap.class)
public abstract class KrakkChunkMapMixin {
    @Shadow
    @Final
    private ServerLevel level;

    @Inject(method = "updateChunkTracking", at = @At("TAIL"))
    private void krakk$syncDamageOnChunkTrackUpdate(ServerPlayer player, ChunkPos chunkPos,
                                                    MutableObject<ClientboundLevelChunkWithLightPacket> packetCache,
                                                    boolean wasLoaded, boolean load, CallbackInfo ci) {
        if (load && !wasLoaded) {
            KrakkApi.network().sendChunkUnload(player, this.level.dimension().location(), chunkPos.x, chunkPos.z);
            KrakkApi.damage().syncChunkToPlayer(player, this.level, chunkPos.x, chunkPos.z, true);
            KrakkDamageRuntime.recordChunkTrackSnapshotSent(
                    player,
                    this.level.dimension().location(),
                    chunkPos.x,
                    chunkPos.z
            );
            return;
        }

        if (!load && wasLoaded) {
            KrakkApi.network().sendChunkUnload(player, this.level.dimension().location(), chunkPos.x, chunkPos.z);
        }
    }
}
