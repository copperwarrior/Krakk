package org.shipwrights.krakk.api;

import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.api.client.KrakkClientOverlayApi;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.api.damage.KrakkDamageType;
import org.shipwrights.krakk.api.explosion.KrakkExplosionApi;
import org.shipwrights.krakk.api.explosion.KrakkExplosionProfile;
import org.shipwrights.krakk.api.network.KrakkNetworkApi;

import java.util.Objects;

/**
 * Service locator for Krakk subsystems.
 * <p>
 * Integrations should read APIs through this class, while Krakk bootstrap code
 * is responsible for installing concrete runtime implementations.
 */
public final class KrakkApi {
    private static volatile KrakkDamageApi damageApi = new NoOpDamageApi();
    private static volatile KrakkExplosionApi explosionApi = new NoOpExplosionApi();
    private static volatile KrakkClientOverlayApi clientOverlayApi = new NoOpClientOverlayApi();
    private static volatile KrakkNetworkApi networkApi = new NoOpNetworkApi();

    private KrakkApi() {
    }

    /**
     * Returns the server-authoritative damage API.
     */
    public static KrakkDamageApi damage() {
        return damageApi;
    }

    /**
     * Returns the explosion simulation API.
     */
    public static KrakkExplosionApi explosions() {
        return explosionApi;
    }

    /**
     * Returns the client overlay and mining baseline API.
     */
    public static KrakkClientOverlayApi clientOverlay() {
        return clientOverlayApi;
    }

    /**
     * Returns the networking boundary API for damage/explosion sync.
     */
    public static KrakkNetworkApi network() {
        return networkApi;
    }

    /**
     * Installs the damage API implementation.
     */
    public static void setDamageApi(KrakkDamageApi api) {
        damageApi = Objects.requireNonNull(api, "api");
    }

    /**
     * Installs the explosion API implementation.
     */
    public static void setExplosionApi(KrakkExplosionApi api) {
        explosionApi = Objects.requireNonNull(api, "api");
    }

    /**
     * Installs the client overlay API implementation.
     */
    public static void setClientOverlayApi(KrakkClientOverlayApi api) {
        clientOverlayApi = Objects.requireNonNull(api, "api");
    }

    /**
     * Installs the network API implementation.
     */
    public static void setNetworkApi(KrakkNetworkApi api) {
        networkApi = Objects.requireNonNull(api, "api");
    }

    private static final class NoOpDamageApi implements KrakkDamageApi {
        @Override
        public KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source,
                                             double impactPower, double impactHeatCelsius,
                                             boolean dropOnBreak, KrakkDamageType damageType) {
            return new KrakkImpactResult(false, 0);
        }

        @Override
        public void clearDamage(ServerLevel level, BlockPos pos) {
        }

        @Override
        public int repairDamage(ServerLevel level, BlockPos pos, int repairAmount) {
            return 0;
        }

        @Override
        public int getDamageState(ServerLevel level, BlockPos pos) {
            return 0;
        }

        @Override
        public float getMiningBaseline(ServerLevel level, BlockPos pos) {
            return 0.0F;
        }

        @Override
        public int takeDamageState(ServerLevel level, BlockPos pos) {
            return 0;
        }

        @Override
        public int takeStoredDamageState(ServerLevel level, BlockPos pos) {
            return 0;
        }

        @Override
        public boolean isLikelyPistonMoveSource(ServerLevel level, BlockPos sourcePos, BlockState sourceState) {
            return false;
        }

        @Override
        public boolean transferLikelyPistonCompletionDamage(ServerLevel level, BlockPos destinationPos, BlockState destinationState) {
            return false;
        }

        @Override
        public void applyTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState, int transferredState) {
        }

        @Override
        public KrakkImpactResult accumulateTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState,
                                                                  int addedState, boolean dropOnBreak) {
            return new KrakkImpactResult(false, 0);
        }

        @Override
        public int getMaxDamageState() {
            return 15;
        }

        @Override
        public boolean setDamageStateForDebug(ServerLevel level, BlockPos pos, int damageState) {
            return false;
        }

        @Override
        public void queuePlayerSync(ServerPlayer player) {
        }

        @Override
        public void clearQueuedPlayerSync(ServerPlayer player) {
        }

        @Override
        public void tickQueuedSyncs(MinecraftServer server) {
        }

        @Override
        public void syncChunkToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ, boolean loadIfMissing) {
        }
    }

    private static final class NoOpExplosionApi implements KrakkExplosionApi {
        @Override
        public void detonate(ServerLevel level, double x, double y, double z, Entity source,
                             LivingEntity owner, KrakkExplosionProfile profile) {
        }
    }

    private static final class NoOpClientOverlayApi implements KrakkClientOverlayApi {
        @Override
        public void resetClientState() {
        }

        @Override
        public void applyDamage(ResourceLocation dimensionId, long posLong, int damageState) {
        }

        @Override
        public void applySection(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates) {
        }

        @Override
        public void applySectionDelta(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ,
                                      Short2ByteOpenHashMap sectionStates) {
        }

        @Override
        public void clearChunk(ResourceLocation dimensionId, int chunkX, int chunkZ) {
        }

        @Override
        public float getMiningBaseline(ResourceLocation dimensionId, long posLong) {
            return 0.0F;
        }

        @Override
        public long[] consumeDirtySections(ResourceLocation dimensionId) {
            return new long[0];
        }

        @Override
        public Long2ByteOpenHashMap snapshotSection(ResourceLocation dimensionId, long sectionKey) {
            return new Long2ByteOpenHashMap();
        }

        @Override
        public long[] snapshotSectionsInChunkRange(ResourceLocation dimensionId, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
            return new long[0];
        }
    }

    private static final class NoOpNetworkApi implements KrakkNetworkApi {
        @Override
        public void initClientReceivers() {
        }

        @Override
        public void sendDamageSync(ServerLevel level, BlockPos pos, int damageState) {
        }

        @Override
        public void sendDamageSyncBatch(java.util.List<ServerPlayer> players, ResourceLocation dimensionId, long posLong, int damageState) {
        }

        @Override
        public void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ,
                                        Short2ByteOpenHashMap states) {
        }

        @Override
        public void sendSectionSnapshotBatch(java.util.List<ServerPlayer> players, ResourceLocation dimensionId,
                                             int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        }

        @Override
        public void sendSectionDeltaBatch(java.util.List<ServerPlayer> players, ResourceLocation dimensionId,
                                          int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        }

        @Override
        public void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        }

        @Override
        public void sendChunkInit(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        }
    }
}
