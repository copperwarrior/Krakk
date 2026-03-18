package org.shipwrights.krakk.mixin.common;

import com.mojang.logging.LogUtils;
import net.minecraft.network.protocol.game.ClientboundLevelChunkWithLightPacket;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ChunkMap;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import org.apache.commons.lang3.mutable.MutableObject;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.slf4j.Logger;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(ChunkMap.class)
public abstract class KrakkChunkMapMixin {
    @Unique
    private static final Logger KRAKK_LOGGER = LogUtils.getLogger();
    @Unique
    private static final boolean KRAKK_CHUNK_TRACKING_PACKETS_ENABLED =
            krakk$parseBooleanProperty("krakk.server.chunk_tracking_packets", false);

    @Shadow
    @Final
    private ServerLevel level;

    @Inject(method = "updateChunkTracking", at = @At("TAIL"), require = 0)
    void krakk$syncChunkTrackingPackets(ServerPlayer player,
                                        ChunkPos chunkPos,
                                        MutableObject<ClientboundLevelChunkWithLightPacket> packetCache,
                                        boolean wasLoaded,
                                        boolean load,
                                        CallbackInfo ci) {
        if (!KRAKK_CHUNK_TRACKING_PACKETS_ENABLED) {
            return;
        }
        if (wasLoaded == load || player == null || chunkPos == null) {
            return;
        }

        ResourceLocation dimensionId = this.level.dimension().location();
        int chunkX = chunkPos.x;
        int chunkZ = chunkPos.z;
        if (load) {
            KrakkApi.network().sendChunkInit(player, dimensionId, chunkX, chunkZ);
            KrakkApi.damage().syncChunkToPlayer(player, this.level, chunkX, chunkZ, false);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                KRAKK_LOGGER.debug(
                        "Krakk chunk tracking load: dim={} player={} chunk=({}, {})",
                        dimensionId,
                        player.getGameProfile().getName(),
                        chunkX,
                        chunkZ
                );
            }
            return;
        }

        KrakkApi.network().sendChunkUnload(player, dimensionId, chunkX, chunkZ);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            KRAKK_LOGGER.debug(
                    "Krakk chunk tracking unload: dim={} player={} chunk=({}, {})",
                    dimensionId,
                    player.getGameProfile().getName(),
                    chunkX,
                    chunkZ
            );
        }
    }

    @Unique
    private static boolean krakk$parseBooleanProperty(String key, boolean fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String normalized = raw.trim();
        if ("true".equalsIgnoreCase(normalized) || "1".equals(normalized) || "yes".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized) || "0".equals(normalized) || "no".equalsIgnoreCase(normalized)) {
            return false;
        }
        return fallback;
    }
}
