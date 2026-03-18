package org.shipwrights.krakk.mixin.client;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.multiplayer.ClientPacketListener;
import net.minecraft.network.protocol.game.ClientboundForgetLevelChunkPacket;
import net.minecraft.network.protocol.game.ClientboundLevelChunkWithLightPacket;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageSectionAccess;
import org.slf4j.Logger;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(ClientPacketListener.class)
public abstract class KrakkClientPacketListenerMixin {
    @Unique
    private static final Logger LOGGER = LogUtils.getLogger();
    @Unique
    private static final byte KRAKK_CHUNK_OP_NONE = 0;
    @Unique
    private static final byte KRAKK_CHUNK_OP_LOAD = 1;
    @Unique
    private static final byte KRAKK_CHUNK_OP_UNLOAD = 2;
    @Unique
    private static final byte KRAKK_CHUNK_LOAD_RESULT_PENDING = 0;
    @Unique
    private static final byte KRAKK_CHUNK_LOAD_RESULT_PARTIAL = 1;
    @Unique
    private static final byte KRAKK_CHUNK_LOAD_RESULT_COMPLETE = 2;
    @Unique
    private static final int KRAKK_MIN_PENDING_CHUNK_OPS_PER_TICK = 4;
    @Unique
    private static final int KRAKK_MEDIUM_PENDING_CHUNK_OPS_PER_TICK = 8;
    @Unique
    private static final int KRAKK_HIGH_PENDING_CHUNK_OPS_PER_TICK = 16;
    @Unique
    private static final int KRAKK_MAX_PENDING_CHUNK_OPS_PER_TICK = 24;
    @Unique
    private static final int KRAKK_MEDIUM_PENDING_CHUNK_OP_BACKLOG = 96;
    @Unique
    private static final int KRAKK_HIGH_PENDING_CHUNK_OP_BACKLOG = 224;
    @Unique
    private static final int KRAKK_MAX_PENDING_CHUNK_OP_BACKLOG = 384;
    @Unique
    private static final long KRAKK_PENDING_CHUNK_OP_BUDGET_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.pending_chunk_ops_budget_ms", 3L) * 1_000_000L;
    @Unique
    private static final int KRAKK_PENDING_CHUNK_LOAD_SECTIONS_PER_OP =
            krakk$parsePositiveIntProperty("krakk.client.pending_chunk_load_sections_per_op", 2);
    @Unique
    private static final boolean KRAKK_USE_VANILLA_CHUNK_PACKET_FALLBACK =
            krakk$parseBooleanProperty("krakk.client.chunk_packet_overlay_fallback", true);

    @Shadow
    private ClientLevel level;
    @Unique
    private final LongLinkedOpenHashSet krakk$pendingChunkOpOrder = new LongLinkedOpenHashSet();
    @Unique
    private final Long2ByteOpenHashMap krakk$pendingChunkOps = new Long2ByteOpenHashMap();
    @Unique
    private final Long2IntOpenHashMap krakk$pendingChunkLoadSectionCursors = new Long2IntOpenHashMap();
    @Unique
    private ClientLevel krakk$pendingChunkOpsLevel;

    @Inject(method = "handleLevelChunkWithLight", at = @At("TAIL"), require = 0)
    private void krakk$seedOverlayFromChunkData(ClientboundLevelChunkWithLightPacket packet, CallbackInfo ci) {
        if (!KRAKK_USE_VANILLA_CHUNK_PACKET_FALLBACK) {
            return;
        }
        if (this.level == null) {
            return;
        }
        this.krakk$syncPendingChunkOpLevel();

        int chunkX = packet.getX();
        int chunkZ = packet.getZ();
        ResourceLocation dimensionId = this.level.dimension().location();
        this.krakk$enqueueChunkOp(chunkX, chunkZ, KRAKK_CHUNK_OP_LOAD);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk client packet chunkWithLight queued: dim={} chunk=({}, {}) pending={}",
                    dimensionId,
                    chunkX,
                    chunkZ,
                    this.krakk$pendingChunkOpOrder.size()
            );
        }
    }

    @Inject(method = "tick", at = @At("TAIL"), require = 0)
    private void krakk$processPendingChunkOps(CallbackInfo ci) {
        if (this.level == null) {
            this.krakk$clearPendingChunkOps();
            this.krakk$pendingChunkOpsLevel = null;
            return;
        }
        this.krakk$syncPendingChunkOpLevel();
        if (this.krakk$pendingChunkOpOrder.isEmpty()) {
            return;
        }

        ResourceLocation dimensionId = this.level.dimension().location();
        int pendingCount = this.krakk$pendingChunkOpOrder.size();
        int budget = Math.min(krakk$pendingChunkOpBudget(pendingCount), pendingCount);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + KRAKK_PENDING_CHUNK_OP_BUDGET_NANOS;
        int processed = 0;
        int requeued = 0;
        int applied = 0;
        int partialLoads = 0;
        int unloads = 0;
        while (processed < budget && !this.krakk$pendingChunkOpOrder.isEmpty()) {
            if (System.nanoTime() >= deadlineNanos) {
                break;
            }
            long chunkKey = this.krakk$pendingChunkOpOrder.removeFirstLong();
            byte op = this.krakk$pendingChunkOps.remove(chunkKey);
            if (op == KRAKK_CHUNK_OP_NONE) {
                continue;
            }
            int chunkX = (int) (chunkKey >> 32);
            int chunkZ = (int) chunkKey;
            processed++;

            if (op == KRAKK_CHUNK_OP_UNLOAD) {
                KrakkApi.clientOverlay().clearChunk(dimensionId, chunkX, chunkZ);
                this.krakk$pendingChunkLoadSectionCursors.remove(chunkKey);
                applied++;
                unloads++;
                continue;
            }
            if (op == KRAKK_CHUNK_OP_LOAD) {
                byte loadResult = this.krakk$seedOverlayForChunkSlice(this.level, dimensionId, chunkX, chunkZ, chunkKey);
                if (loadResult == KRAKK_CHUNK_LOAD_RESULT_COMPLETE) {
                    applied++;
                    continue;
                }
                if (loadResult == KRAKK_CHUNK_LOAD_RESULT_PARTIAL) {
                    partialLoads++;
                }
                this.krakk$enqueueChunkOp(chunkX, chunkZ, KRAKK_CHUNK_OP_LOAD);
                requeued++;
                continue;
            }
        }

        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled() && processed > 0) {
            LOGGER.debug(
                    "Krakk client packet chunk op tick: dim={} processed={} applied={} partialLoads={} unloads={} requeued={} pending={} budget={} elapsedMs={}",
                    dimensionId,
                    processed,
                    applied,
                    partialLoads,
                    unloads,
                    requeued,
                    this.krakk$pendingChunkOpOrder.size(),
                    budget,
                    (System.nanoTime() - startNanos) / 1_000_000.0D
            );
        }
    }

    @Unique
    private byte krakk$seedOverlayForChunkSlice(ClientLevel level, ResourceLocation dimensionId, int chunkX, int chunkZ, long chunkKey) {
        LevelChunk chunk = level.getChunkSource().getChunkNow(chunkX, chunkZ);
        if (chunk == null) {
            return KRAKK_CHUNK_LOAD_RESULT_PENDING;
        }

        int sectionIndex;
        if (!this.krakk$pendingChunkLoadSectionCursors.containsKey(chunkKey)) {
            // Replace stale chunk-local states once, then stream section snapshots over multiple ticks.
            KrakkApi.clientOverlay().clearChunk(dimensionId, chunkX, chunkZ);
            sectionIndex = 0;
        } else {
            sectionIndex = this.krakk$pendingChunkLoadSectionCursors.get(chunkKey);
        }
        LevelChunkSection[] sections = chunk.getSections();
        int minSectionY = chunk.getMinBuildHeight() >> 4;
        int processedSections = 0;
        int seededSections = 0;
        int seededEntries = 0;
        while (sectionIndex < sections.length && processedSections < KRAKK_PENDING_CHUNK_LOAD_SECTIONS_PER_OP) {
            LevelChunkSection section = sections[sectionIndex];
            int sectionY = minSectionY + sectionIndex;
            if (section instanceof KrakkBlockDamageSectionAccess access && !access.krakk$getDamageStates().isEmpty()) {
                Short2ByteOpenHashMap statesCopy = new Short2ByteOpenHashMap(access.krakk$getDamageStates());
                KrakkApi.clientOverlay().applySection(
                        dimensionId,
                        chunkX,
                        sectionY,
                        chunkZ,
                        statesCopy
                );
                seededSections++;
                seededEntries += statesCopy.size();
            }
            sectionIndex++;
            processedSections++;
        }
        if (sectionIndex < sections.length) {
            this.krakk$pendingChunkLoadSectionCursors.put(chunkKey, sectionIndex);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk client packet chunkWithLight seed slice: dim={} chunk=({}, {}) cursor={}/{} sections={} entries={}",
                        dimensionId,
                        chunkX,
                        chunkZ,
                        sectionIndex,
                        sections.length,
                        seededSections,
                        seededEntries
                );
            }
            return KRAKK_CHUNK_LOAD_RESULT_PARTIAL;
        }

        this.krakk$pendingChunkLoadSectionCursors.remove(chunkKey);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk client packet chunkWithLight seeded overlay: dim={} chunk=({}, {}) sections={} entries={}",
                    dimensionId,
                    chunkX,
                    chunkZ,
                    seededSections,
                    seededEntries
            );
        }
        return KRAKK_CHUNK_LOAD_RESULT_COMPLETE;
    }

    @Inject(method = "handleForgetLevelChunk", at = @At("TAIL"), require = 0)
    private void krakk$clearOverlayChunkOnForget(ClientboundForgetLevelChunkPacket packet, CallbackInfo ci) {
        if (!KRAKK_USE_VANILLA_CHUNK_PACKET_FALLBACK) {
            return;
        }
        if (this.level == null) {
            return;
        }
        this.krakk$syncPendingChunkOpLevel();
        this.krakk$enqueueChunkOp(packet.getX(), packet.getZ(), KRAKK_CHUNK_OP_UNLOAD);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk client packet forgetChunk queued: dim={} chunk=({}, {}) pending={}",
                    this.level.dimension().location(),
                    packet.getX(),
                    packet.getZ(),
                    this.krakk$pendingChunkOpOrder.size()
            );
        }
    }

    @Unique
    private static long krakk$toChunkKey(int chunkX, int chunkZ) {
        return (((long) chunkX) << 32) ^ (chunkZ & 0xFFFFFFFFL);
    }

    @Unique
    private void krakk$enqueueChunkOp(int chunkX, int chunkZ, byte op) {
        long chunkKey = krakk$toChunkKey(chunkX, chunkZ);
        if (op == KRAKK_CHUNK_OP_UNLOAD) {
            this.krakk$pendingChunkLoadSectionCursors.remove(chunkKey);
        }
        byte previous = this.krakk$pendingChunkOps.put(chunkKey, op);
        if (previous == KRAKK_CHUNK_OP_NONE) {
            this.krakk$pendingChunkOpOrder.add(chunkKey);
            return;
        }
        if (previous != op) {
            this.krakk$pendingChunkOpOrder.remove(chunkKey);
            this.krakk$pendingChunkOpOrder.add(chunkKey);
        }
    }

    @Unique
    private static int krakk$pendingChunkOpBudget(int pendingCount) {
        if (pendingCount >= KRAKK_MAX_PENDING_CHUNK_OP_BACKLOG) {
            return KRAKK_MAX_PENDING_CHUNK_OPS_PER_TICK;
        }
        if (pendingCount >= KRAKK_HIGH_PENDING_CHUNK_OP_BACKLOG) {
            return KRAKK_HIGH_PENDING_CHUNK_OPS_PER_TICK;
        }
        if (pendingCount >= KRAKK_MEDIUM_PENDING_CHUNK_OP_BACKLOG) {
            return KRAKK_MEDIUM_PENDING_CHUNK_OPS_PER_TICK;
        }
        return KRAKK_MIN_PENDING_CHUNK_OPS_PER_TICK;
    }

    @Unique
    private void krakk$syncPendingChunkOpLevel() {
        if (this.level == this.krakk$pendingChunkOpsLevel) {
            return;
        }
        // Preserve queued work on first attachment; clear only on actual level swaps.
        if (this.krakk$pendingChunkOpsLevel != null) {
            this.krakk$clearPendingChunkOps();
        }
        this.krakk$pendingChunkOpsLevel = this.level;
    }

    @Unique
    private void krakk$clearPendingChunkOps() {
        this.krakk$pendingChunkOpOrder.clear();
        this.krakk$pendingChunkOps.clear();
        this.krakk$pendingChunkLoadSectionCursors.clear();
    }

    @Unique
    private static long krakk$parsePositiveLongProperty(String key, long fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Math.max(1L, Long.parseLong(raw.trim()));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    @Unique
    private static int krakk$parsePositiveIntProperty(String key, int fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Math.max(1, Integer.parseInt(raw.trim()));
        } catch (NumberFormatException ignored) {
            return fallback;
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
