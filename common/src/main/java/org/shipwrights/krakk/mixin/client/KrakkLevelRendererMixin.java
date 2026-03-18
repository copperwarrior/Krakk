package org.shipwrights.krakk.mixin.client;

import com.mojang.blaze3d.vertex.BufferBuilder;
import com.mojang.blaze3d.vertex.DefaultVertexFormat;
import com.mojang.blaze3d.vertex.PoseStack;
import com.mojang.blaze3d.vertex.SheetedDecalTextureGenerator;
import com.mojang.blaze3d.vertex.VertexBuffer;
import com.mojang.blaze3d.vertex.VertexConsumer;
import com.mojang.blaze3d.vertex.VertexFormat;
import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import net.minecraft.client.Camera;
import net.minecraft.client.Minecraft;
import net.minecraft.client.color.block.BlockColors;
import net.minecraft.client.model.geom.EntityModelSet;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.renderer.BlockEntityWithoutLevelRenderer;
import net.minecraft.client.renderer.GameRenderer;
import net.minecraft.client.renderer.LevelRenderer;
import net.minecraft.client.renderer.LightTexture;
import net.minecraft.client.renderer.RenderType;
import net.minecraft.client.renderer.ShaderInstance;
import net.minecraft.client.renderer.block.BlockRenderDispatcher;
import net.minecraft.client.renderer.block.BlockModelShaper;
import net.minecraft.client.renderer.chunk.ChunkRenderDispatcher;
import net.minecraft.client.renderer.blockentity.BlockEntityRenderDispatcher;
import net.minecraft.client.resources.model.ModelBakery;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.util.Mth;
import net.minecraft.world.level.block.state.BlockState;
import org.joml.Matrix3f;
import org.joml.Matrix4f;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.client.KrakkClientShaders;
import org.shipwrights.krakk.runtime.client.SectionRebuildWork;
import org.shipwrights.krakk.runtime.client.SectionRenderCache;
import org.shipwrights.krakk.runtime.client.VecRange;
import org.shipwrights.krakk.state.chunk.KrakkChunkSectionDamageBridge;
import org.slf4j.Logger;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Mixin(value = LevelRenderer.class, priority = 900)
public abstract class KrakkLevelRendererMixin {
    @Unique
    private static final Logger KRAKK_LOGGER = LogUtils.getLogger();
    @Unique
    private static final int KRAKK_MAX_SECTION_REBUILDS_PER_FRAME = 12;
    @Unique
    private static final int KRAKK_MAX_SECTION_REBUILDS_PER_FRAME_BACKLOG = 24;
    @Unique
    private static final int KRAKK_URGENT_REBUILD_SHARE_PERCENT = Math.max(
            1,
            Math.min(99, krakk$parsePositiveIntProperty("krakk.client.overlay_urgent_rebuild_share_percent", 75))
    );
    @Unique
    private static final long KRAKK_SECTION_REBUILD_BUDGET_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_rebuild_budget_ms", 8L) * 1_000_000L;
    @Unique
    private static final long KRAKK_SECTION_REBUILD_BACKLOG_BUDGET_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_rebuild_budget_backlog_ms", 12L) * 1_000_000L;
    @Unique
    private static final int KRAKK_SECTION_REBUILD_BLOCKS_PER_PASS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_rebuild_blocks_per_pass", 192);
    @Unique
    private static final int KRAKK_SECTION_REBUILD_BLOCKS_PER_PASS_BACKLOG =
            krakk$parsePositiveIntProperty("krakk.client.overlay_rebuild_blocks_per_pass_backlog", 256);
    @Unique
    private static final int KRAKK_SECTION_REBUILD_STAGE_EXTEND_LIMIT =
            krakk$parsePositiveIntProperty("krakk.client.overlay_rebuild_stage_extend_limit", 192);
    @Unique
    private static final int KRAKK_SECTION_REBUILD_PASS_HARD_LIMIT_MULTIPLIER =
            krakk$parsePositiveIntProperty("krakk.client.overlay_rebuild_pass_hard_limit_multiplier", 2);
    @Unique
    private static final int KRAKK_SECTION_REBUILD_BACKLOG_THRESHOLD = 256;
    @Unique
    private static final int KRAKK_REBUILD_ADVANCE_IDLE = 0;
    @Unique
    private static final int KRAKK_REBUILD_ADVANCE_PROGRESSED = 1;
    @Unique
    private static final int KRAKK_REBUILD_ADVANCE_COMPLETE = 2;
    @Unique
    private static final int KRAKK_MAX_DIRTY_SCANS_PER_FRAME =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dirty_scans_per_frame", 2048);
    @Unique
    private static final long KRAKK_CACHE_SWEEP_INTERVAL_TICKS = 40L;
    @Unique
    private static final long KRAKK_SECTION_DISCOVERY_INTERVAL_TICKS = 20L;
    @Unique
    private static final long KRAKK_SECTION_SNAPSHOT_INTERVAL_TICKS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_snapshot_interval_ticks", 1L);
    @Unique
    private static final int KRAKK_BUFFER_BUILDER_CAPACITY = 256;
    @Unique
    private static final int KRAKK_OVERLAY_RENDER_DISTANCE_CHUNKS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_render_distance_chunks", 8);
    @Unique
    private static final int KRAKK_OVERLAY_MAX_VISIBLE_SECTION_CACHES =
            krakk$parsePositiveIntProperty("krakk.client.overlay_max_visible_section_caches", 384);
    @Unique
    private static final boolean KRAKK_OVERLAY_DYNAMIC_LOD_ENABLED =
            krakk$parseBooleanProperty("krakk.client.overlay_dynamic_lod", true);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MIN_RENDER_DISTANCE_CHUNKS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_min_render_distance_chunks", 5);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MIN_VISIBLE_SECTION_CACHES =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_min_visible_section_caches", 192);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_BACKLOG_STEP_SECTIONS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_backlog_step_sections", 128);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MAX_RANGE_REDUCTION_CHUNKS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_max_range_reduction_chunks", 3);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MAX_CAP_REDUCTION_PERCENT = Math.max(
            1,
            Math.min(90, krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_max_cap_reduction_percent", 45))
    );
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MOVEMENT_THRESHOLD_BLOCKS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_movement_threshold_blocks", 6);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MOVEMENT_RANGE_REDUCTION_CHUNKS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_movement_range_reduction_chunks", 2);
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_MOVEMENT_CAP_REDUCTION_PERCENT = Math.max(
            1,
            Math.min(90, krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_movement_cap_reduction_percent", 20))
    );
    @Unique
    private static final int KRAKK_OVERLAY_DYNAMIC_RECOVERY_VISIBLE_CACHES_PER_FRAME =
            krakk$parsePositiveIntProperty("krakk.client.overlay_dynamic_recovery_visible_caches_per_frame", 16);
    @Unique
    private static final int KRAKK_OVERLAY_DRAW_CALL_BUDGET =
            krakk$parsePositiveIntProperty("krakk.client.overlay_draw_call_budget", 640);
    @Unique
    private static final int KRAKK_OVERLAY_DRAW_CALL_BUDGET_MIN =
            krakk$parsePositiveIntProperty("krakk.client.overlay_draw_call_budget_min", 192);
    @Unique
    private static final int KRAKK_OVERLAY_DRAW_CALL_BUDGET_RECOVERY_PER_FRAME =
            krakk$parsePositiveIntProperty("krakk.client.overlay_draw_call_budget_recovery_per_frame", 24);
    @Unique
    private static final int KRAKK_OVERLAY_DRAW_CALL_PRESSURE_THRESHOLD =
            krakk$parsePositiveIntProperty("krakk.client.overlay_draw_call_pressure_threshold", 512);
    @Unique
    private static final int KRAKK_OVERLAY_DRAW_CALL_HITCH_REDUCTION_PERCENT = Math.max(
            1,
            Math.min(90, krakk$parsePositiveIntProperty("krakk.client.overlay_draw_call_hitch_reduction_percent", 45))
    );
    @Unique
    private static final int KRAKK_OVERLAY_DRAW_CALL_PRESSURE_REDUCTION_PERCENT = Math.max(
            1,
            Math.min(90, krakk$parsePositiveIntProperty("krakk.client.overlay_draw_call_pressure_reduction_percent", 20))
    );
    @Unique
    private static final int KRAKK_BUFFER_BUILDER_BYTES_PER_STAGE_BLOCK = 1024;
    @Unique
    private static final int KRAKK_BUFFER_BUILDER_MAX_CAPACITY = 8 * 1024 * 1024;
    @Unique
    private static final boolean KRAKK_OVERLAY_ASYNC_REBUILD_ENABLED =
            krakk$parseBooleanProperty("krakk.client.overlay_async_rebuild", true);
    @Unique
    private static final int KRAKK_OVERLAY_MAX_ASYNC_REBUILD_TASKS =
            krakk$parsePositiveIntProperty("krakk.client.overlay_async_rebuild_tasks", 2);
    @Unique
    private static final boolean KRAKK_OVERLAY_TIMING_LOGGING_ENABLED =
            krakk$parseBooleanProperty("krakk.client.overlay_timing_logging", true);
    @Unique
    private static final long KRAKK_OVERLAY_TIMING_WARN_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_timing_warn_ms", 50L) * 1_000_000L;
    @Unique
    private static final long KRAKK_OVERLAY_TIMING_HITCH_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_timing_hitch_ms", 16L) * 1_000_000L;
    @Unique
    private static final long KRAKK_OVERLAY_TIMING_HITCH_LOG_INTERVAL_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_timing_hitch_log_interval_ms", 750L) * 1_000_000L;
    @Unique
    private static final long KRAKK_OVERLAY_TIMING_LOG_INTERVAL_NANOS =
            krakk$parsePositiveLongProperty("krakk.client.overlay_timing_log_interval_ms", 5_000L) * 1_000_000L;
    @Unique
    private static final int KRAKK_OVERLAY_TIMING_SPIKE_TOP_SECTION_LIMIT =
            krakk$parsePositiveIntProperty("krakk.client.overlay_timing_spike_top_sections", 10);
    @Unique
    private static final boolean KRAKK_SODIUM_VISIBILITY_ENABLED =
            krakk$parseBooleanProperty("krakk.client.overlay_sodium_visibility", true);
    @Unique
    private static final Map<Class<?>, Field> KRAKK_RENDER_CHUNK_INFO_CHUNK_FIELDS = new ConcurrentHashMap<>();
    @Unique
    private static final Set<Class<?>> KRAKK_RENDER_CHUNK_INFO_NO_CHUNK_FIELD = ConcurrentHashMap.newKeySet();
    @Unique
    private static final String KRAKK_SODIUM_WORLD_RENDERER_CLASS =
            "me.jellysquid.mods.sodium.client.render.SodiumWorldRenderer";
    @Unique
    private static final String KRAKK_SODIUM_RENDER_SECTION_MANAGER_CLASS =
            "me.jellysquid.mods.sodium.client.render.chunk.RenderSectionManager";
    @Unique
    private static final String KRAKK_SODIUM_SORTED_RENDER_LISTS_CLASS =
            "me.jellysquid.mods.sodium.client.render.chunk.lists.SortedRenderLists";
    @Unique
    private static final String KRAKK_SODIUM_CHUNK_RENDER_LIST_CLASS =
            "me.jellysquid.mods.sodium.client.render.chunk.lists.ChunkRenderList";
    @Unique
    private static final String KRAKK_SODIUM_RENDER_REGION_CLASS =
            "me.jellysquid.mods.sodium.client.render.chunk.region.RenderRegion";
    @Unique
    private static final String KRAKK_SODIUM_RENDER_SECTION_CLASS =
            "me.jellysquid.mods.sodium.client.render.chunk.RenderSection";
    @Unique
    private static final String KRAKK_SODIUM_BYTE_ITERATOR_CLASS =
            "me.jellysquid.mods.sodium.client.util.iterator.ByteIterator";
    @Unique
    private static volatile boolean KRAKK_SODIUM_REFLECTION_INITIALIZED = false;
    @Unique
    private static volatile boolean KRAKK_SODIUM_REFLECTION_AVAILABLE = false;
    @Unique
    private static Method KRAKK_SODIUM_INSTANCE_NULLABLE_METHOD;
    @Unique
    private static Field KRAKK_SODIUM_RENDER_SECTION_MANAGER_FIELD;
    @Unique
    private static Method KRAKK_SODIUM_GET_RENDER_LISTS_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_SORTED_LISTS_ITERATOR_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_CHUNK_LIST_SECTIONS_WITH_GEOMETRY_ITERATOR_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_CHUNK_LIST_GET_REGION_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_RENDER_REGION_GET_SECTION_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_X_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_Y_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_Z_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_BYTE_ITERATOR_HAS_NEXT_METHOD;
    @Unique
    private static Method KRAKK_SODIUM_BYTE_ITERATOR_NEXT_BYTE_AS_INT_METHOD;
    @Unique
    private static final ThreadLocal<BlockRenderDispatcher> KRAKK_ASYNC_THREAD_BLOCK_RENDERER = new ThreadLocal<>();
    @Unique
    private static final ThreadLocal<BlockModelShaper> KRAKK_ASYNC_THREAD_MODEL_SHAPER = new ThreadLocal<>();
    @Unique
    private static volatile BlockModelShaper KRAKK_ASYNC_SHARED_MODEL_SHAPER = null;
    @Unique
    private static volatile BlockColors KRAKK_ASYNC_SHARED_BLOCK_COLORS = null;
    @Unique
    private static volatile BlockEntityRenderDispatcher KRAKK_ASYNC_SHARED_BLOCK_ENTITY_RENDER_DISPATCHER = null;
    @Unique
    private static volatile EntityModelSet KRAKK_ASYNC_SHARED_ENTITY_MODEL_SET = null;

    @Unique
    private final LongOpenHashSet krakk$pendingDirtySections = new LongOpenHashSet();
    @Unique
    private final LongLinkedOpenHashSet krakk$urgentDirtySections = new LongLinkedOpenHashSet();
    @Unique
    private final Long2ObjectOpenHashMap<SectionRenderCache> krakk$sectionCaches = new Long2ObjectOpenHashMap<>();
    @Unique
    private final Long2ObjectOpenHashMap<SectionRebuildWork> krakk$sectionRebuildWork = new Long2ObjectOpenHashMap<>();
    @Unique
    private ResourceLocation krakk$activeDimensionId = null;
    @Unique
    private ClientLevel krakk$activeLevel = null;
    @Unique
    private long krakk$lastCacheSweepTick = Long.MIN_VALUE;
    @Unique
    private long krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
    @Unique
    private long krakk$lastSectionSnapshotTick = Long.MIN_VALUE;
    @Unique
    private boolean krakk$loggedRendererInit = false;
    @Unique
    private boolean krakk$loggedOverlayTimingConfig = false;
    @Unique
    private long krakk$overlayTimingWindowStartNanos = Long.MIN_VALUE;
    @Unique
    private long krakk$overlayTimingNextLogNanos = Long.MIN_VALUE;
    @Unique
    private long krakk$overlayTimingNextHitchLogNanos = Long.MIN_VALUE;
    @Unique
    private long krakk$overlayTimingSamples = 0L;
    @Unique
    private long krakk$overlayTimingSlowSamples = 0L;
    @Unique
    private long krakk$overlayTimingTotalNanos = 0L;
    @Unique
    private long krakk$overlayTimingWorstNanos = 0L;
    @Unique
    private long krakk$overlayTimingSyncTotalNanos = 0L;
    @Unique
    private long krakk$overlayTimingRebuildTotalNanos = 0L;
    @Unique
    private long krakk$overlayTimingSweepTotalNanos = 0L;
    @Unique
    private long krakk$overlayTimingDrawTotalNanos = 0L;
    @Unique
    private long krakk$overlayTimingSyncWorstNanos = 0L;
    @Unique
    private long krakk$overlayTimingRebuildWorstNanos = 0L;
    @Unique
    private long krakk$overlayTimingSweepWorstNanos = 0L;
    @Unique
    private long krakk$overlayTimingDrawWorstNanos = 0L;
    @Unique
    private int krakk$overlayTimingWorstCachedSections = 0;
    @Unique
    private int krakk$overlayTimingWorstPendingSections = 0;
    @Unique
    private int krakk$overlayTimingWorstUrgentSections = 0;
    @Unique
    private int krakk$overlayTimingWorstVisibleCachesFound = 0;
    @Unique
    private int krakk$overlayTimingWorstVisibleCachesDrawn = 0;
    @Unique
    private int krakk$overlayTimingWorstRebuilds = 0;
    @Unique
    private int krakk$overlayTimingWorstDrawCalls = 0;
    @Unique
    private int krakk$overlayTimingWorstStagePasses = 0;
    @Unique
    private int krakk$overlayTimingWorstRangeChunks = 0;
    @Unique
    private int krakk$overlayTimingWorstVisibleCap = 0;
    @Unique
    private int krakk$overlayTimingWorstVisibleSectionBufferMax = 0;
    @Unique
    private String krakk$overlayTimingWorstTopSections = "none";
    @Unique
    private long krakk$overlayTimingUploadTotalNanos = 0L;
    @Unique
    private long krakk$overlayTimingUploadWorstNanos = 0L;
    @Unique
    private long krakk$overlayTimingUploadedBytes = 0L;
    @Unique
    private long krakk$overlayTimingUploadedBuffers = 0L;
    @Unique
    private long krakk$overlayTimingCreatedVbos = 0L;
    @Unique
    private long krakk$overlayTimingClosedVbos = 0L;
    @Unique
    private final IntArrayList krakk$overlayTimingVisibleSectionBufferSamples = new IntArrayList();
    @Unique
    private boolean krakk$loggedSodiumVisibilitySource = false;
    @Unique
    private boolean krakk$loggedVanillaVisibilitySource = false;
    @Unique
    private boolean krakk$asyncRebuildRuntimeEnabled = true;
    @Unique
    private boolean krakk$loggedAsyncRebuildDisabled = false;
    @Unique
    private int krakk$overlayAdaptiveRangeChunks = Integer.MIN_VALUE;
    @Unique
    private int krakk$overlayAdaptiveVisibleCacheCap = Integer.MIN_VALUE;
    @Unique
    private int krakk$overlayAdaptiveDrawCallBudget = Integer.MIN_VALUE;
    @Unique
    private long krakk$overlayPreviousFrameNanos = 0L;
    @Unique
    private int krakk$overlayPreviousDrawCalls = 0;
    @Unique
    private int krakk$overlayDrawResumeStage = 1;
    @Unique
    private int krakk$overlayDrawResumeCacheIndex = 0;
    @Unique
    private double krakk$lastOverlayCameraX = Double.NaN;
    @Unique
    private double krakk$lastOverlayCameraZ = Double.NaN;
    @Unique
    private final Long2ByteOpenHashMap krakk$chunkLoadedScratch = new Long2ByteOpenHashMap();
    @Unique
    private final LongArrayList krakk$staleSectionKeysScratch = new LongArrayList();
    @Unique
    private final LongArrayList krakk$urgentCandidatesScratch = new LongArrayList();
    @Unique
    private final LongArrayList krakk$candidatesScratch = new LongArrayList();
    @Unique
    private final LongOpenHashSet krakk$urgentCandidateSetScratch = new LongOpenHashSet();
    @Unique
    private final LongOpenHashSet krakk$visibleSectionsScratch = new LongOpenHashSet();
    @Unique
    private float[] krakk$visibleCacheOffsetXScratch = new float[0];
    @Unique
    private float[] krakk$visibleCacheOffsetYScratch = new float[0];
    @Unique
    private float[] krakk$visibleCacheOffsetZScratch = new float[0];
    @Unique
    private int[] krakk$visibleCacheDrawCallsScratch = new int[0];
    @Unique
    private int[] krakk$visibleCacheBufferCountsScratch = new int[0];
    @Unique
    private int[] krakk$visibleCacheStageCapScratch = new int[0];
    @Unique
    private final Matrix4f krakk$modelViewMatrixScratch = new Matrix4f();
    @Shadow
    @Final
    private ObjectArrayList<?> renderChunksInFrustum;

    @Shadow
    private void renderChunkLayer(RenderType renderType, PoseStack poseStack, double cameraX, double cameraY, double cameraZ, Matrix4f projectionMatrix) {
        throw new AssertionError("Shadow method body");
    }

    @Unique
    private static int krakk$toDestroyStage(int damageState) {
        int clamped = Math.max(0, Math.min(15, damageState));
        if (clamped <= 0) {
            return 0;
        }
        return Math.max(1, Math.min(9, Mth.ceil((clamped * 9.0F) / 15.0F)));
    }

    @Unique
    private static int krakk$getDecalQuarterTurns(long posLong) {
        long mixed = posLong * 0x9E3779B97F4A7C15L;
        mixed ^= (mixed >>> 33);
        mixed *= 0xC2B2AE3D27D4EB4FL;
        mixed ^= (mixed >>> 29);
        return (int) (mixed & 3L);
    }

    @Unique
    private void krakk$clearRenderCache() {
        for (SectionRebuildWork rebuildWork : this.krakk$sectionRebuildWork.values()) {
            this.krakk$recordClosedVboCount(rebuildWork.cancel());
        }
        for (SectionRenderCache cache : this.krakk$sectionCaches.values()) {
            this.krakk$recordClosedVboCount(cache.close());
        }
        this.krakk$sectionCaches.clear();
        this.krakk$sectionRebuildWork.clear();
        this.krakk$pendingDirtySections.clear();
        this.krakk$urgentDirtySections.clear();
        KRAKK_ASYNC_SHARED_MODEL_SHAPER = null;
        KRAKK_ASYNC_SHARED_BLOCK_COLORS = null;
        KRAKK_ASYNC_SHARED_BLOCK_ENTITY_RENDER_DISPATCHER = null;
        KRAKK_ASYNC_SHARED_ENTITY_MODEL_SET = null;
        this.krakk$overlayAdaptiveRangeChunks = Integer.MIN_VALUE;
        this.krakk$overlayAdaptiveVisibleCacheCap = Integer.MIN_VALUE;
        this.krakk$overlayAdaptiveDrawCallBudget = Integer.MIN_VALUE;
        this.krakk$overlayPreviousFrameNanos = 0L;
        this.krakk$overlayPreviousDrawCalls = 0;
        this.krakk$overlayDrawResumeStage = 1;
        this.krakk$overlayDrawResumeCacheIndex = 0;
        this.krakk$lastOverlayCameraX = Double.NaN;
        this.krakk$lastOverlayCameraZ = Double.NaN;
        this.krakk$chunkLoadedScratch.clear();
        this.krakk$staleSectionKeysScratch.clear();
        this.krakk$urgentCandidatesScratch.clear();
        this.krakk$candidatesScratch.clear();
        this.krakk$urgentCandidateSetScratch.clear();
        this.krakk$visibleSectionsScratch.clear();
    }

    @Inject(
            method = "renderLevel(Lcom/mojang/blaze3d/vertex/PoseStack;FJZLnet/minecraft/client/Camera;Lnet/minecraft/client/renderer/GameRenderer;Lnet/minecraft/client/renderer/LightTexture;Lorg/joml/Matrix4f;)V",
            at = @At("HEAD")
    )
    private void krakk$collectDirtySections(PoseStack poseStack, float partialTick, long finishNanoTime, boolean renderBlockOutline,
                                            Camera camera, GameRenderer gameRenderer, LightTexture lightTexture,
                                            Matrix4f projectionMatrix, CallbackInfo ci) {
        Minecraft minecraft = Minecraft.getInstance();
        ClientLevel level = minecraft.level;
        if (level == null) {
            this.krakk$activeLevel = null;
            this.krakk$activeDimensionId = null;
            this.krakk$lastCacheSweepTick = Long.MIN_VALUE;
            this.krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
            this.krakk$lastSectionSnapshotTick = Long.MIN_VALUE;
            this.krakk$loggedSodiumVisibilitySource = false;
            this.krakk$loggedVanillaVisibilitySource = false;
            this.krakk$asyncRebuildRuntimeEnabled = true;
            this.krakk$loggedAsyncRebuildDisabled = false;
            this.krakk$clearRenderCache();
            this.krakk$resetOverlayTimingStats();
            return;
        }

        if (level != this.krakk$activeLevel) {
            this.krakk$activeLevel = level;
            this.krakk$activeDimensionId = level.dimension().location();
            this.krakk$lastCacheSweepTick = Long.MIN_VALUE;
            this.krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
            this.krakk$lastSectionSnapshotTick = Long.MIN_VALUE;
            this.krakk$clearRenderCache();
            this.krakk$loggedRendererInit = false;
            this.krakk$loggedSodiumVisibilitySource = false;
            this.krakk$loggedVanillaVisibilitySource = false;
            this.krakk$asyncRebuildRuntimeEnabled = true;
            this.krakk$loggedAsyncRebuildDisabled = false;
            this.krakk$resetOverlayTimingStats();
        }

        ResourceLocation dimensionId = level.dimension().location();
        if (!dimensionId.equals(this.krakk$activeDimensionId)) {
            this.krakk$activeDimensionId = dimensionId;
            this.krakk$lastCacheSweepTick = Long.MIN_VALUE;
            this.krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
            this.krakk$lastSectionSnapshotTick = Long.MIN_VALUE;
            this.krakk$clearRenderCache();
            this.krakk$loggedRendererInit = false;
            this.krakk$loggedSodiumVisibilitySource = false;
            this.krakk$loggedVanillaVisibilitySource = false;
            this.krakk$asyncRebuildRuntimeEnabled = true;
            this.krakk$loggedAsyncRebuildDisabled = false;
            this.krakk$resetOverlayTimingStats();
        }

        long[] dirtySections = KrakkApi.clientOverlay().consumeDirtySections(dimensionId);
        for (long sectionKey : dirtySections) {
            this.krakk$cancelSectionRebuild(sectionKey);
            this.krakk$pendingDirtySections.add(sectionKey);
            // Prioritize newly-updated sections so live damage changes are visible immediately.
            this.krakk$urgentDirtySections.add(sectionKey);
        }
    }

    @Redirect(
            method = "renderLevel(Lcom/mojang/blaze3d/vertex/PoseStack;FJZLnet/minecraft/client/Camera;Lnet/minecraft/client/renderer/GameRenderer;Lnet/minecraft/client/renderer/LightTexture;Lorg/joml/Matrix4f;)V",
            at = @At(
                    value = "INVOKE",
                    target = "Lnet/minecraft/client/renderer/LevelRenderer;renderChunkLayer(Lnet/minecraft/client/renderer/RenderType;Lcom/mojang/blaze3d/vertex/PoseStack;DDDLorg/joml/Matrix4f;)V"
            ),
            require = 0
    )
    private void krakk$renderChunkLayerWithOverlay(LevelRenderer renderer,
                                                   RenderType renderType,
                                                   PoseStack poseStack,
                                                   double cameraX,
                                                   double cameraY,
                                                   double cameraZ,
                                                   Matrix4f projectionMatrix) {
        this.renderChunkLayer(renderType, poseStack, cameraX, cameraY, cameraZ, projectionMatrix);
        if (renderType == RenderType.translucent()) {
            this.krakk$renderCustomDamageOverlay(poseStack, cameraX, cameraY, cameraZ, projectionMatrix);
        }
    }

    @Unique
    private void krakk$renderCustomDamageOverlay(PoseStack poseStack,
                                                 double cameraX,
                                                 double cameraY,
                                                 double cameraZ,
                                                 Matrix4f projectionMatrix) {
        long overlayStartNanos = System.nanoTime();
        long syncNanos = 0L;
        long rebuildNanos = 0L;
        long sweepNanos = 0L;
        long drawNanos = 0L;
        int visibleCachesFound = 0;
        int visibleCachesDrawn = 0;
        int rebuiltSections = 0;
        int drawCalls = 0;
        int stagePasses = 0;
        int adaptiveRangeChunks = 0;
        int adaptiveVisibleCacheCap = 0;
        int visibleBufferCountTotal = 0;
        int visibleBufferCountMax = 0;
        String topDrawSections = "none";
        Minecraft minecraft = Minecraft.getInstance();
        var profiler = minecraft.getProfiler();
        profiler.push("krakk_damage_overlay");
        try {
            ClientLevel level = minecraft.level;
            if (level == null || this.krakk$activeDimensionId == null) {
                return;
            }
            if (!this.krakk$loggedRendererInit) {
                KRAKK_LOGGER.info("Krakk renderer: chunk-layer overlay integration active");
                this.krakk$loggedRendererInit = true;
            }
            if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED && !this.krakk$loggedOverlayTimingConfig) {
                KRAKK_LOGGER.info(
                        "Krakk overlay timing logging enabled: warnMs={} hitchMs={} hitchIntervalMs={} intervalMs={} snapshotIntervalTicks={} rebuildBudgetMs={} rebuildBacklogBudgetMs={} rebuildBlocksPerPass={} rebuildBlocksPerPassBacklog={} stageExtendLimit={} passHardLimitMultiplier={} asyncRebuild={} asyncTasks={} sodiumVisibility={} dynamicLod={} dynamicMinRange={} dynamicMinVisible={} dynamicBacklogStep={} dynamicMaxRangeReduction={} dynamicMaxCapReductionPercent={} dynamicMovementThresholdBlocks={} dynamicMovementRangeReduction={} dynamicMovementCapReductionPercent={} dynamicRecoveryVisibleCachesPerFrame={} drawCallBudget={} drawCallBudgetMin={} drawCallBudgetRecoveryPerFrame={} drawCallPressureThreshold={} drawCallHitchReductionPercent={} drawCallPressureReductionPercent={}",
                        krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_WARN_NANOS),
                        krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_HITCH_NANOS),
                        krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_HITCH_LOG_INTERVAL_NANOS),
                        krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_LOG_INTERVAL_NANOS),
                        KRAKK_SECTION_SNAPSHOT_INTERVAL_TICKS,
                        krakk$nanosToMillis(KRAKK_SECTION_REBUILD_BUDGET_NANOS),
                        krakk$nanosToMillis(KRAKK_SECTION_REBUILD_BACKLOG_BUDGET_NANOS),
                        KRAKK_SECTION_REBUILD_BLOCKS_PER_PASS,
                        KRAKK_SECTION_REBUILD_BLOCKS_PER_PASS_BACKLOG,
                        KRAKK_SECTION_REBUILD_STAGE_EXTEND_LIMIT,
                        KRAKK_SECTION_REBUILD_PASS_HARD_LIMIT_MULTIPLIER,
                        KRAKK_OVERLAY_ASYNC_REBUILD_ENABLED,
                        KRAKK_OVERLAY_MAX_ASYNC_REBUILD_TASKS,
                        KRAKK_SODIUM_VISIBILITY_ENABLED,
                        KRAKK_OVERLAY_DYNAMIC_LOD_ENABLED,
                        KRAKK_OVERLAY_DYNAMIC_MIN_RENDER_DISTANCE_CHUNKS,
                        KRAKK_OVERLAY_DYNAMIC_MIN_VISIBLE_SECTION_CACHES,
                        KRAKK_OVERLAY_DYNAMIC_BACKLOG_STEP_SECTIONS,
                        KRAKK_OVERLAY_DYNAMIC_MAX_RANGE_REDUCTION_CHUNKS,
                        KRAKK_OVERLAY_DYNAMIC_MAX_CAP_REDUCTION_PERCENT,
                        KRAKK_OVERLAY_DYNAMIC_MOVEMENT_THRESHOLD_BLOCKS,
                        KRAKK_OVERLAY_DYNAMIC_MOVEMENT_RANGE_REDUCTION_CHUNKS,
                        KRAKK_OVERLAY_DYNAMIC_MOVEMENT_CAP_REDUCTION_PERCENT,
                        KRAKK_OVERLAY_DYNAMIC_RECOVERY_VISIBLE_CACHES_PER_FRAME,
                        KRAKK_OVERLAY_DRAW_CALL_BUDGET,
                        KRAKK_OVERLAY_DRAW_CALL_BUDGET_MIN,
                        KRAKK_OVERLAY_DRAW_CALL_BUDGET_RECOVERY_PER_FRAME,
                        KRAKK_OVERLAY_DRAW_CALL_PRESSURE_THRESHOLD,
                        KRAKK_OVERLAY_DRAW_CALL_HITCH_REDUCTION_PERCENT,
                        KRAKK_OVERLAY_DRAW_CALL_PRESSURE_REDUCTION_PERCENT
                );
                this.krakk$loggedOverlayTimingConfig = true;
            }

            BlockRenderDispatcher blockRenderer = minecraft.getBlockRenderer();
            VecRange range = this.krakk$computeOverlayViewBudget(cameraX, cameraZ, minecraft);
            adaptiveRangeChunks = Math.max(1, this.krakk$overlayAdaptiveRangeChunks);
            adaptiveVisibleCacheCap = Math.max(1, this.krakk$overlayAdaptiveVisibleCacheCap);
            int cameraChunkX = Mth.floor(cameraX) >> 4;
            int cameraChunkZ = Mth.floor(cameraZ) >> 4;
            LongOpenHashSet activeVisibleSections = this.krakk$collectActiveVisibleSectionKeys();
            long syncStartNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? System.nanoTime() : 0L;
            profiler.popPush("krakk_overlay_sync");
            this.krakk$enqueueVisibleDamageSections(level, range.minChunkX(), range.maxChunkX(), range.minChunkZ(), range.maxChunkZ());
            syncNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? (System.nanoTime() - syncStartNanos) : 0L;
            long rebuildStartNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? System.nanoTime() : 0L;
            profiler.popPush("krakk_overlay_rebuild");
            rebuiltSections = this.krakk$rebuildDirtySections(
                    level,
                    blockRenderer,
                    range.minChunkX(),
                    range.maxChunkX(),
                    range.minChunkZ(),
                    range.maxChunkZ(),
                    cameraChunkX,
                    cameraChunkZ,
                    activeVisibleSections
            );
            rebuildNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? (System.nanoTime() - rebuildStartNanos) : 0L;
            long sweepStartNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? System.nanoTime() : 0L;
            profiler.popPush("krakk_overlay_cache_sweep");
            this.krakk$sweepSectionCache(level, range.minChunkX(), range.maxChunkX(), range.minChunkZ(), range.maxChunkZ());
            sweepNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? (System.nanoTime() - sweepStartNanos) : 0L;
            long drawStartNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? System.nanoTime() : 0L;
            profiler.popPush("krakk_overlay_draw");
            ShaderInstance overlayShader = KrakkClientShaders.damageOverlayShaderOrFallback();
            if (overlayShader == null) {
                if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED) {
                    drawNanos = System.nanoTime() - drawStartNanos;
                }
                return;
            }
            ArrayList<SectionRenderCache> visibleCaches = this.krakk$collectVisibleCaches(
                    level,
                    range.minChunkX(),
                    range.maxChunkX(),
                    range.minChunkZ(),
                    range.maxChunkZ(),
                    activeVisibleSections
            );
            visibleCachesFound = visibleCaches.size();
            krakk$sortVisibleCachesByDistance(visibleCaches, cameraChunkX, cameraChunkZ);
            if (visibleCaches.size() > adaptiveVisibleCacheCap) {
                visibleCaches.subList(adaptiveVisibleCacheCap, visibleCaches.size()).clear();
            }
            visibleCachesDrawn = visibleCaches.size();
            if (visibleCaches.isEmpty()) {
                this.krakk$overlayDrawResumeStage = 1;
                this.krakk$overlayDrawResumeCacheIndex = 0;
                if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED) {
                    drawNanos = System.nanoTime() - drawStartNanos;
                }
                return;
            }

            int visibleCacheCount = visibleCaches.size();
            int drawCallBudget = this.krakk$computeOverlayDrawCallBudget();
            int drawBudgetPressurePercent = krakk$drawBudgetPressurePercent(drawCallBudget);
            int startStage = Math.max(1, Math.min(9, this.krakk$overlayDrawResumeStage));
            int startCacheIndex = Math.floorMod(this.krakk$overlayDrawResumeCacheIndex, visibleCacheCount);
            this.krakk$ensureVisibleCacheScratchCapacity(visibleCacheCount);
            float[] cacheOffsetX = this.krakk$visibleCacheOffsetXScratch;
            float[] cacheOffsetY = this.krakk$visibleCacheOffsetYScratch;
            float[] cacheOffsetZ = this.krakk$visibleCacheOffsetZScratch;
            int[] cacheStageCaps = this.krakk$visibleCacheStageCapScratch;
            int[] cacheDrawCallCounts = null;
            int[] cacheBufferCounts = null;
            if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED) {
                cacheDrawCallCounts = this.krakk$visibleCacheDrawCallsScratch;
                cacheBufferCounts = this.krakk$visibleCacheBufferCountsScratch;
                Arrays.fill(cacheDrawCallCounts, 0, visibleCacheCount, 0);
                Arrays.fill(cacheBufferCounts, 0, visibleCacheCount, 0);
            }
            for (int cacheIndex = 0; cacheIndex < visibleCacheCount; cacheIndex++) {
                SectionRenderCache cache = visibleCaches.get(cacheIndex);
                cacheOffsetX[cacheIndex] = (float) (cache.originBlockX() - cameraX);
                cacheOffsetY[cacheIndex] = (float) (cache.originBlockY() - cameraY);
                cacheOffsetZ[cacheIndex] = (float) (cache.originBlockZ() - cameraZ);
                long sectionDistanceSquared = krakk$sectionChunkDistanceSquared(
                        cache.sectionX(),
                        cache.sectionZ(),
                        cameraChunkX,
                        cameraChunkZ
                );
                cacheStageCaps[cacheIndex] = krakk$stageCapForSectionDistance(sectionDistanceSquared, drawBudgetPressurePercent);
                if (cacheBufferCounts != null) {
                    int sectionBufferCount = cache.totalStageBufferCount();
                    cacheBufferCounts[cacheIndex] = sectionBufferCount;
                    visibleBufferCountTotal += sectionBufferCount;
                    if (sectionBufferCount > visibleBufferCountMax) {
                        visibleBufferCountMax = sectionBufferCount;
                    }
                    this.krakk$overlayTimingVisibleSectionBufferSamples.add(sectionBufferCount);
                }
            }

            Matrix4f baseModelViewMatrix = poseStack.last().pose();
            Matrix4f modelViewMatrix = this.krakk$modelViewMatrixScratch;
            boolean fogSyncedForCurrentShader = false;
            boolean drawBudgetReached = false;
            int resumeStage = startStage;
            int resumeCacheIndex = startCacheIndex;

            for (int stageOffset = 0; stageOffset < 9; stageOffset++) {
                int stage = ((startStage - 1 + stageOffset) % 9) + 1;
                RenderType stageRenderType = ModelBakery.DESTROY_TYPES.get(stage);
                boolean stageRendered = false;
                int stageStartCacheIndex = stageOffset == 0 ? startCacheIndex : 0;

                for (int cacheOffset = 0; cacheOffset < visibleCacheCount; cacheOffset++) {
                    int cacheIndex = (stageStartCacheIndex + cacheOffset) % visibleCacheCount;
                    if (stage > cacheStageCaps[cacheIndex]) {
                        continue;
                    }
                    SectionRenderCache cache = visibleCaches.get(cacheIndex);
                    if (!cache.hasStage(stage)) {
                        continue;
                    }
                    ArrayList<VertexBuffer> stageBuffers = cache.getStageBuffers(stage);
                    if (stageBuffers == null || stageBuffers.isEmpty()) {
                        continue;
                    }

                    if (!stageRendered) {
                        stageRenderType.setupRenderState();
                        stageRendered = true;
                    }

                    modelViewMatrix.set(baseModelViewMatrix).translate(
                            cacheOffsetX[cacheIndex],
                            cacheOffsetY[cacheIndex],
                            cacheOffsetZ[cacheIndex]
                    );
                    for (int bufferIndex = 0; bufferIndex < stageBuffers.size(); bufferIndex++) {
                        if (drawCalls >= drawCallBudget) {
                            drawBudgetReached = true;
                            resumeStage = stage;
                            resumeCacheIndex = cacheIndex;
                            break;
                        }
                        VertexBuffer buffer = stageBuffers.get(bufferIndex);
                        buffer.bind();
                        try {
                            if (!fogSyncedForCurrentShader) {
                                KrakkClientShaders.syncFogUniforms(overlayShader);
                                fogSyncedForCurrentShader = true;
                            }
                            buffer.drawWithShader(modelViewMatrix, projectionMatrix, overlayShader);
                            drawCalls++;
                            if (cacheDrawCallCounts != null) {
                                cacheDrawCallCounts[cacheIndex]++;
                            }
                        } catch (RuntimeException exception) {
                            if (!KrakkClientShaders.isRegisteredCustomShader(overlayShader)) {
                                throw exception;
                            }

                            KrakkClientShaders.markCustomShaderRenderFailure(overlayShader, exception);
                            ShaderInstance fallbackShader = KrakkClientShaders.damageOverlayShaderOrFallback();
                            if (fallbackShader == null || fallbackShader == overlayShader) {
                                throw exception;
                            }

                            overlayShader = fallbackShader;
                            fogSyncedForCurrentShader = false;
                            KrakkClientShaders.syncFogUniforms(overlayShader);
                            fogSyncedForCurrentShader = true;
                            buffer.drawWithShader(modelViewMatrix, projectionMatrix, overlayShader);
                            drawCalls++;
                            if (cacheDrawCallCounts != null) {
                                cacheDrawCallCounts[cacheIndex]++;
                            }
                        }
                    }
                    if (drawBudgetReached) {
                        break;
                    }
                }

                if (stageRendered) {
                    stagePasses++;
                    VertexBuffer.unbind();
                    stageRenderType.clearRenderState();
                }
                if (drawBudgetReached) {
                    break;
                }
            }
            if (drawBudgetReached) {
                this.krakk$overlayDrawResumeStage = resumeStage;
                this.krakk$overlayDrawResumeCacheIndex = (resumeCacheIndex + 1) % visibleCacheCount;
            } else {
                this.krakk$overlayDrawResumeStage = 1;
                this.krakk$overlayDrawResumeCacheIndex = 0;
            }
            if (cacheDrawCallCounts != null && cacheBufferCounts != null) {
                topDrawSections = krakk$buildTopSectionDrawSummary(
                        visibleCaches,
                        cacheDrawCallCounts,
                        cacheBufferCounts,
                        KRAKK_OVERLAY_TIMING_SPIKE_TOP_SECTION_LIMIT
                );
            }
            if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED) {
                drawNanos = System.nanoTime() - drawStartNanos;
            }
        } finally {
            long frameNanos = System.nanoTime() - overlayStartNanos;
            this.krakk$updateOverlayPressureState(frameNanos, drawCalls);
            if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED) {
                this.krakk$recordOverlayTimingSample(
                        frameNanos,
                        this.krakk$sectionCaches.size(),
                        this.krakk$pendingDirtySections.size(),
                        this.krakk$urgentDirtySections.size(),
                        syncNanos,
                        rebuildNanos,
                        sweepNanos,
                        drawNanos,
                        visibleCachesFound,
                        visibleCachesDrawn,
                        rebuiltSections,
                        drawCalls,
                        stagePasses,
                        adaptiveRangeChunks,
                        adaptiveVisibleCacheCap,
                        visibleBufferCountTotal,
                        visibleBufferCountMax,
                        topDrawSections
                );
            }
            profiler.pop();
        }
    }

    @Unique
    private void krakk$recordOverlayTimingSample(long frameNanos,
                                                 int cachedSections,
                                                 int pendingSections,
                                                 int urgentSections,
                                                 long syncNanos,
                                                 long rebuildNanos,
                                                 long sweepNanos,
                                                 long drawNanos,
                                                 int visibleCachesFound,
                                                 int visibleCachesDrawn,
                                                 int rebuiltSections,
                                                 int drawCalls,
                                                 int stagePasses,
                                                 int renderDistanceChunks,
                                                 int visibleCacheCap,
                                                 int visibleBufferCountTotal,
                                                 int visibleBufferCountMax,
                                                 String topDrawSections) {
        long nowNanos = System.nanoTime();
        if (this.krakk$overlayTimingWindowStartNanos == Long.MIN_VALUE) {
            this.krakk$overlayTimingWindowStartNanos = nowNanos;
            this.krakk$overlayTimingNextLogNanos = nowNanos + KRAKK_OVERLAY_TIMING_LOG_INTERVAL_NANOS;
            this.krakk$overlayTimingNextHitchLogNanos = nowNanos;
        }

        this.krakk$overlayTimingSamples++;
        this.krakk$overlayTimingTotalNanos += frameNanos;
        this.krakk$overlayTimingSyncTotalNanos += syncNanos;
        this.krakk$overlayTimingRebuildTotalNanos += rebuildNanos;
        this.krakk$overlayTimingSweepTotalNanos += sweepNanos;
        this.krakk$overlayTimingDrawTotalNanos += drawNanos;
        if (syncNanos > this.krakk$overlayTimingSyncWorstNanos) {
            this.krakk$overlayTimingSyncWorstNanos = syncNanos;
        }
        if (rebuildNanos > this.krakk$overlayTimingRebuildWorstNanos) {
            this.krakk$overlayTimingRebuildWorstNanos = rebuildNanos;
        }
        if (sweepNanos > this.krakk$overlayTimingSweepWorstNanos) {
            this.krakk$overlayTimingSweepWorstNanos = sweepNanos;
        }
        if (drawNanos > this.krakk$overlayTimingDrawWorstNanos) {
            this.krakk$overlayTimingDrawWorstNanos = drawNanos;
            this.krakk$overlayTimingWorstVisibleSectionBufferMax = visibleBufferCountMax;
            this.krakk$overlayTimingWorstTopSections = topDrawSections == null ? "none" : topDrawSections;
        }
        if (frameNanos > this.krakk$overlayTimingWorstNanos) {
            this.krakk$overlayTimingWorstNanos = frameNanos;
            this.krakk$overlayTimingWorstCachedSections = cachedSections;
            this.krakk$overlayTimingWorstPendingSections = pendingSections;
            this.krakk$overlayTimingWorstUrgentSections = urgentSections;
            this.krakk$overlayTimingWorstVisibleCachesFound = visibleCachesFound;
            this.krakk$overlayTimingWorstVisibleCachesDrawn = visibleCachesDrawn;
            this.krakk$overlayTimingWorstRebuilds = rebuiltSections;
            this.krakk$overlayTimingWorstDrawCalls = drawCalls;
            this.krakk$overlayTimingWorstStagePasses = stagePasses;
            this.krakk$overlayTimingWorstRangeChunks = renderDistanceChunks;
            this.krakk$overlayTimingWorstVisibleCap = visibleCacheCap;
        }
        if (frameNanos >= KRAKK_OVERLAY_TIMING_WARN_NANOS) {
            this.krakk$overlayTimingSlowSamples++;
        }
        if (frameNanos >= KRAKK_OVERLAY_TIMING_HITCH_NANOS && nowNanos >= this.krakk$overlayTimingNextHitchLogNanos) {
            KRAKK_LOGGER.warn(
                    "Krakk overlay hitch: frameMs={} thresholdMs={} phaseMs[sync={} rebuild={} sweep={} draw={}] "
                            + "context[caches={} pending={} urgent={} visibleFound={} visibleDrawn={} rebuilds={} drawCalls={} stagePasses={} rangeChunks={} visibleCap={} topSections={} topSectionBufferMax={}]",
                    krakk$nanosToMillis(frameNanos),
                    krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_HITCH_NANOS),
                    krakk$nanosToMillis(syncNanos),
                    krakk$nanosToMillis(rebuildNanos),
                    krakk$nanosToMillis(sweepNanos),
                    krakk$nanosToMillis(drawNanos),
                    cachedSections,
                    pendingSections,
                    urgentSections,
                    visibleCachesFound,
                    visibleCachesDrawn,
                    rebuiltSections,
                    drawCalls,
                    stagePasses,
                    renderDistanceChunks,
                    visibleCacheCap,
                    topDrawSections,
                    visibleBufferCountMax
            );
            this.krakk$overlayTimingNextHitchLogNanos = nowNanos + KRAKK_OVERLAY_TIMING_HITCH_LOG_INTERVAL_NANOS;
        }

        if (nowNanos < this.krakk$overlayTimingNextLogNanos) {
            return;
        }

        long intervalNanos = nowNanos - this.krakk$overlayTimingWindowStartNanos;
        double averageMs = this.krakk$overlayTimingSamples > 0L
                ? (this.krakk$overlayTimingTotalNanos / (double) this.krakk$overlayTimingSamples) / 1_000_000.0D
                : 0.0D;
        double syncAverageMs = this.krakk$overlayTimingSamples > 0L
                ? (this.krakk$overlayTimingSyncTotalNanos / (double) this.krakk$overlayTimingSamples) / 1_000_000.0D
                : 0.0D;
        double rebuildAverageMs = this.krakk$overlayTimingSamples > 0L
                ? (this.krakk$overlayTimingRebuildTotalNanos / (double) this.krakk$overlayTimingSamples) / 1_000_000.0D
                : 0.0D;
        double sweepAverageMs = this.krakk$overlayTimingSamples > 0L
                ? (this.krakk$overlayTimingSweepTotalNanos / (double) this.krakk$overlayTimingSamples) / 1_000_000.0D
                : 0.0D;
        double drawAverageMs = this.krakk$overlayTimingSamples > 0L
                ? (this.krakk$overlayTimingDrawTotalNanos / (double) this.krakk$overlayTimingSamples) / 1_000_000.0D
                : 0.0D;
        double uploadAverageMs = this.krakk$overlayTimingSamples > 0L
                ? (this.krakk$overlayTimingUploadTotalNanos / (double) this.krakk$overlayTimingSamples) / 1_000_000.0D
                : 0.0D;
        double uploadKiB = this.krakk$overlayTimingUploadedBytes / 1024.0D;
        double sectionBufferMean = 0.0D;
        int sectionBufferP95 = 0;
        int sectionBufferMax = visibleBufferCountMax;
        if (!this.krakk$overlayTimingVisibleSectionBufferSamples.isEmpty()) {
            int sampleCount = this.krakk$overlayTimingVisibleSectionBufferSamples.size();
            long totalBuffers = 0L;
            int[] sortedSamples = this.krakk$overlayTimingVisibleSectionBufferSamples.toIntArray();
            for (int bufferCount : sortedSamples) {
                totalBuffers += bufferCount;
            }
            Arrays.sort(sortedSamples);
            sectionBufferMean = totalBuffers / (double) sampleCount;
            int p95Index = Math.max(0, (int) Math.ceil(sampleCount * 0.95D) - 1);
            sectionBufferP95 = sortedSamples[p95Index];
            sectionBufferMax = sortedSamples[sortedSamples.length - 1];
        } else if (visibleCachesDrawn > 0) {
            sectionBufferMean = visibleBufferCountTotal / (double) visibleCachesDrawn;
            sectionBufferP95 = visibleBufferCountMax;
            sectionBufferMax = visibleBufferCountMax;
        }

        if (this.krakk$overlayTimingSlowSamples > 0L) {
            KRAKK_LOGGER.warn(
                    "Krakk overlay timing: slowFrames={}/{} intervalMs={} avgMs={} worstMs={} thresholdMs={} "
                            + "phaseAvgMs[sync={} rebuild={} sweep={} draw={}] phaseWorstMs[sync={} rebuild={} sweep={} draw={}] "
                            + "upload[avgMs={} worstMs={} buffers={} bytesKiB={} vboCreates={} vboCloses={}] "
                            + "visibleBuffersPerSection[mean={} p95={} max={}] "
                            + "worstContext[caches={} pending={} urgent={} visibleFound={} visibleDrawn={} rebuilds={} drawCalls={} stagePasses={} rangeChunks={} visibleCap={} topSections={} topSectionBufferMax={}]",
                    this.krakk$overlayTimingSlowSamples,
                    this.krakk$overlayTimingSamples,
                    krakk$nanosToMillis(intervalNanos),
                    averageMs,
                    krakk$nanosToMillis(this.krakk$overlayTimingWorstNanos),
                    krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_WARN_NANOS),
                    syncAverageMs,
                    rebuildAverageMs,
                    sweepAverageMs,
                    drawAverageMs,
                    krakk$nanosToMillis(this.krakk$overlayTimingSyncWorstNanos),
                    krakk$nanosToMillis(this.krakk$overlayTimingRebuildWorstNanos),
                    krakk$nanosToMillis(this.krakk$overlayTimingSweepWorstNanos),
                    krakk$nanosToMillis(this.krakk$overlayTimingDrawWorstNanos),
                    uploadAverageMs,
                    krakk$nanosToMillis(this.krakk$overlayTimingUploadWorstNanos),
                    this.krakk$overlayTimingUploadedBuffers,
                    uploadKiB,
                    this.krakk$overlayTimingCreatedVbos,
                    this.krakk$overlayTimingClosedVbos,
                    sectionBufferMean,
                    sectionBufferP95,
                    sectionBufferMax,
                    this.krakk$overlayTimingWorstCachedSections,
                    this.krakk$overlayTimingWorstPendingSections,
                    this.krakk$overlayTimingWorstUrgentSections,
                    this.krakk$overlayTimingWorstVisibleCachesFound,
                    this.krakk$overlayTimingWorstVisibleCachesDrawn,
                    this.krakk$overlayTimingWorstRebuilds,
                    this.krakk$overlayTimingWorstDrawCalls,
                    this.krakk$overlayTimingWorstStagePasses,
                    this.krakk$overlayTimingWorstRangeChunks,
                    this.krakk$overlayTimingWorstVisibleCap,
                    this.krakk$overlayTimingWorstTopSections,
                    this.krakk$overlayTimingWorstVisibleSectionBufferMax
            );
        } else {
            KRAKK_LOGGER.info(
                    "Krakk overlay timing: slowFrames={}/{} intervalMs={} avgMs={} worstMs={} thresholdMs={} "
                            + "phaseAvgMs[sync={} rebuild={} sweep={} draw={}] phaseWorstMs[sync={} rebuild={} sweep={} draw={}] "
                            + "upload[avgMs={} worstMs={} buffers={} bytesKiB={} vboCreates={} vboCloses={}] "
                            + "visibleBuffersPerSection[mean={} p95={} max={}] "
                            + "peakContext[caches={} pending={} urgent={} visibleFound={} visibleDrawn={} rebuilds={} drawCalls={} stagePasses={} rangeChunks={} visibleCap={} topSections={} topSectionBufferMax={}]",
                    this.krakk$overlayTimingSlowSamples,
                    this.krakk$overlayTimingSamples,
                    krakk$nanosToMillis(intervalNanos),
                    averageMs,
                    krakk$nanosToMillis(this.krakk$overlayTimingWorstNanos),
                    krakk$nanosToMillis(KRAKK_OVERLAY_TIMING_WARN_NANOS),
                    syncAverageMs,
                    rebuildAverageMs,
                    sweepAverageMs,
                    drawAverageMs,
                    krakk$nanosToMillis(this.krakk$overlayTimingSyncWorstNanos),
                    krakk$nanosToMillis(this.krakk$overlayTimingRebuildWorstNanos),
                    krakk$nanosToMillis(this.krakk$overlayTimingSweepWorstNanos),
                    krakk$nanosToMillis(this.krakk$overlayTimingDrawWorstNanos),
                    uploadAverageMs,
                    krakk$nanosToMillis(this.krakk$overlayTimingUploadWorstNanos),
                    this.krakk$overlayTimingUploadedBuffers,
                    uploadKiB,
                    this.krakk$overlayTimingCreatedVbos,
                    this.krakk$overlayTimingClosedVbos,
                    sectionBufferMean,
                    sectionBufferP95,
                    sectionBufferMax,
                    this.krakk$overlayTimingWorstCachedSections,
                    this.krakk$overlayTimingWorstPendingSections,
                    this.krakk$overlayTimingWorstUrgentSections,
                    this.krakk$overlayTimingWorstVisibleCachesFound,
                    this.krakk$overlayTimingWorstVisibleCachesDrawn,
                    this.krakk$overlayTimingWorstRebuilds,
                    this.krakk$overlayTimingWorstDrawCalls,
                    this.krakk$overlayTimingWorstStagePasses,
                    this.krakk$overlayTimingWorstRangeChunks,
                    this.krakk$overlayTimingWorstVisibleCap,
                    this.krakk$overlayTimingWorstTopSections,
                    this.krakk$overlayTimingWorstVisibleSectionBufferMax
            );
        }

        this.krakk$overlayTimingWindowStartNanos = nowNanos;
        this.krakk$overlayTimingNextLogNanos = nowNanos + KRAKK_OVERLAY_TIMING_LOG_INTERVAL_NANOS;
        if (this.krakk$overlayTimingNextHitchLogNanos == Long.MIN_VALUE || this.krakk$overlayTimingNextHitchLogNanos < nowNanos) {
            this.krakk$overlayTimingNextHitchLogNanos = nowNanos;
        }
        this.krakk$overlayTimingSamples = 0L;
        this.krakk$overlayTimingSlowSamples = 0L;
        this.krakk$overlayTimingTotalNanos = 0L;
        this.krakk$overlayTimingSyncTotalNanos = 0L;
        this.krakk$overlayTimingRebuildTotalNanos = 0L;
        this.krakk$overlayTimingSweepTotalNanos = 0L;
        this.krakk$overlayTimingDrawTotalNanos = 0L;
        this.krakk$overlayTimingUploadTotalNanos = 0L;
        this.krakk$overlayTimingUploadWorstNanos = 0L;
        this.krakk$overlayTimingUploadedBytes = 0L;
        this.krakk$overlayTimingUploadedBuffers = 0L;
        this.krakk$overlayTimingCreatedVbos = 0L;
        this.krakk$overlayTimingClosedVbos = 0L;
        this.krakk$overlayTimingVisibleSectionBufferSamples.clear();
        this.krakk$overlayTimingWorstNanos = 0L;
        this.krakk$overlayTimingSyncWorstNanos = 0L;
        this.krakk$overlayTimingRebuildWorstNanos = 0L;
        this.krakk$overlayTimingSweepWorstNanos = 0L;
        this.krakk$overlayTimingDrawWorstNanos = 0L;
        this.krakk$overlayTimingWorstCachedSections = 0;
        this.krakk$overlayTimingWorstPendingSections = 0;
        this.krakk$overlayTimingWorstUrgentSections = 0;
        this.krakk$overlayTimingWorstVisibleCachesFound = 0;
        this.krakk$overlayTimingWorstVisibleCachesDrawn = 0;
        this.krakk$overlayTimingWorstRebuilds = 0;
        this.krakk$overlayTimingWorstDrawCalls = 0;
        this.krakk$overlayTimingWorstStagePasses = 0;
        this.krakk$overlayTimingWorstRangeChunks = 0;
        this.krakk$overlayTimingWorstVisibleCap = 0;
        this.krakk$overlayTimingWorstVisibleSectionBufferMax = 0;
        this.krakk$overlayTimingWorstTopSections = "none";
    }

    @Unique
    private void krakk$resetOverlayTimingStats() {
        this.krakk$overlayTimingWindowStartNanos = Long.MIN_VALUE;
        this.krakk$overlayTimingNextLogNanos = Long.MIN_VALUE;
        this.krakk$overlayTimingNextHitchLogNanos = Long.MIN_VALUE;
        this.krakk$overlayTimingSamples = 0L;
        this.krakk$overlayTimingSlowSamples = 0L;
        this.krakk$overlayTimingTotalNanos = 0L;
        this.krakk$overlayTimingSyncTotalNanos = 0L;
        this.krakk$overlayTimingRebuildTotalNanos = 0L;
        this.krakk$overlayTimingSweepTotalNanos = 0L;
        this.krakk$overlayTimingDrawTotalNanos = 0L;
        this.krakk$overlayTimingUploadTotalNanos = 0L;
        this.krakk$overlayTimingUploadWorstNanos = 0L;
        this.krakk$overlayTimingUploadedBytes = 0L;
        this.krakk$overlayTimingUploadedBuffers = 0L;
        this.krakk$overlayTimingCreatedVbos = 0L;
        this.krakk$overlayTimingClosedVbos = 0L;
        this.krakk$overlayTimingVisibleSectionBufferSamples.clear();
        this.krakk$overlayTimingWorstNanos = 0L;
        this.krakk$overlayTimingSyncWorstNanos = 0L;
        this.krakk$overlayTimingRebuildWorstNanos = 0L;
        this.krakk$overlayTimingSweepWorstNanos = 0L;
        this.krakk$overlayTimingDrawWorstNanos = 0L;
        this.krakk$overlayTimingWorstCachedSections = 0;
        this.krakk$overlayTimingWorstPendingSections = 0;
        this.krakk$overlayTimingWorstUrgentSections = 0;
        this.krakk$overlayTimingWorstVisibleCachesFound = 0;
        this.krakk$overlayTimingWorstVisibleCachesDrawn = 0;
        this.krakk$overlayTimingWorstRebuilds = 0;
        this.krakk$overlayTimingWorstDrawCalls = 0;
        this.krakk$overlayTimingWorstStagePasses = 0;
        this.krakk$overlayTimingWorstRangeChunks = 0;
        this.krakk$overlayTimingWorstVisibleCap = 0;
        this.krakk$overlayTimingWorstVisibleSectionBufferMax = 0;
        this.krakk$overlayTimingWorstTopSections = "none";
    }

    @Unique
    private void krakk$enqueueVisibleDamageSections(ClientLevel level, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        if (this.krakk$activeDimensionId == null) {
            return;
        }

        long gameTick = level.getGameTime();
        boolean shouldSnapshot = this.krakk$lastSectionSnapshotTick == Long.MIN_VALUE
                || gameTick < this.krakk$lastSectionSnapshotTick
                || (gameTick - this.krakk$lastSectionSnapshotTick) >= KRAKK_SECTION_SNAPSHOT_INTERVAL_TICKS;
        boolean shouldDiscover = this.krakk$lastSectionDiscoveryTick == Long.MIN_VALUE
                || gameTick < this.krakk$lastSectionDiscoveryTick
                || (gameTick - this.krakk$lastSectionDiscoveryTick) >= KRAKK_SECTION_DISCOVERY_INTERVAL_TICKS;

        if (!shouldSnapshot && !shouldDiscover) {
            return;
        }

        LongOpenHashSet visibleSet = null;
        if (shouldSnapshot) {
            long[] visibleSections = KrakkApi.clientOverlay().snapshotSectionsInChunkRange(
                    this.krakk$activeDimensionId,
                    minChunkX,
                    maxChunkX,
                    minChunkZ,
                    maxChunkZ
            );
            if (shouldDiscover) {
                visibleSet = new LongOpenHashSet(visibleSections.length);
                for (long sectionKey : visibleSections) {
                    visibleSet.add(sectionKey);
                }
            } else {
                for (long sectionKey : visibleSections) {
                    this.krakk$queueSectionForRebuildIfMissing(sectionKey);
                }
            }
            this.krakk$lastSectionSnapshotTick = gameTick;
        }

        if (shouldDiscover) {
            if (visibleSet == null) {
                visibleSet = new LongOpenHashSet();
            }
            // Safety net for chunk load/unload race windows: discover damage directly from loaded chunk sections.
            KrakkChunkSectionDamageBridge.collectSectionKeysInChunkRange(
                    level,
                    minChunkX,
                    maxChunkX,
                    minChunkZ,
                    maxChunkZ,
                    visibleSet
            );
            this.krakk$lastSectionDiscoveryTick = gameTick;
            LongIterator visibleIterator = visibleSet.iterator();
            while (visibleIterator.hasNext()) {
                long sectionKey = visibleIterator.nextLong();
                this.krakk$queueSectionForRebuildIfMissing(sectionKey);
            }
        }

        // Keep in-range cache entries stable. They are invalidated by dirty section rebuilds,
        // and out-of-range/unloaded entries are swept separately.
    }

    @Unique
    private void krakk$queueSectionForRebuildIfMissing(long sectionKey) {
        if (!this.krakk$sectionCaches.containsKey(sectionKey)) {
            this.krakk$pendingDirtySections.add(sectionKey);
            this.krakk$urgentDirtySections.add(sectionKey);
        }
    }

    @Unique
    private int krakk$rebuildDirtySections(ClientLevel level, BlockRenderDispatcher blockRenderer,
                                           int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ,
                                           int cameraChunkX, int cameraChunkZ,
                                           LongOpenHashSet activeVisibleSections) {
        if (this.krakk$pendingDirtySections.isEmpty()) {
            return 0;
        }

        boolean hasVisibilityFilter = activeVisibleSections != null && !activeVisibleSections.isEmpty();
        boolean backlog = this.krakk$pendingDirtySections.size() >= KRAKK_SECTION_REBUILD_BACKLOG_THRESHOLD;
        int maxRebuilds = backlog
                ? KRAKK_MAX_SECTION_REBUILDS_PER_FRAME_BACKLOG
                : KRAKK_MAX_SECTION_REBUILDS_PER_FRAME;
        int blocksPerPass = backlog
                ? KRAKK_SECTION_REBUILD_BLOCKS_PER_PASS_BACKLOG
                : KRAKK_SECTION_REBUILD_BLOCKS_PER_PASS;
        long rebuildBudgetNanos = backlog
                ? KRAKK_SECTION_REBUILD_BACKLOG_BUDGET_NANOS
                : KRAKK_SECTION_REBUILD_BUDGET_NANOS;
        long rebuildDeadlineNanos = System.nanoTime() + rebuildBudgetNanos;
        int rebuilds = 0;
        Long2ByteOpenHashMap chunkLoadedCache = this.krakk$chunkLoadedScratch;
        chunkLoadedCache.clear();
        chunkLoadedCache.defaultReturnValue((byte) -1);

        LongArrayList urgentCandidates = this.krakk$urgentCandidatesScratch;
        urgentCandidates.clear();
        LongIterator urgentIterator = this.krakk$urgentDirtySections.iterator();
        int urgentScans = 0;
        while (urgentIterator.hasNext()) {
            long sectionKey = urgentIterator.nextLong();
            urgentIterator.remove();
            urgentScans++;
            if (urgentScans > KRAKK_MAX_DIRTY_SCANS_PER_FRAME) {
                // Defer remaining urgent scans to subsequent frames.
                this.krakk$urgentDirtySections.add(sectionKey);
                break;
            }

            int sectionX = SectionPos.x(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);
            boolean inRange = sectionX >= minChunkX && sectionX <= maxChunkX
                    && sectionZ >= minChunkZ && sectionZ <= maxChunkZ;
            if (!inRange || !krakk$isChunkLoaded(level, sectionX, sectionZ, chunkLoadedCache)) {
                this.krakk$pendingDirtySections.remove(sectionKey);
                this.krakk$discardSection(sectionKey);
                continue;
            }
            if (hasVisibilityFilter && !activeVisibleSections.contains(sectionKey)) {
                // Keep pending for when the section enters the visible set.
                continue;
            }

            urgentCandidates.add(sectionKey);
        }
        krakk$sortSectionKeysByDistance(urgentCandidates, cameraChunkX, cameraChunkZ);

        LongArrayList candidates = this.krakk$candidatesScratch;
        candidates.clear();
        LongOpenHashSet urgentCandidateSet = this.krakk$urgentCandidateSetScratch;
        urgentCandidateSet.clear();
        for (int i = 0; i < urgentCandidates.size(); i++) {
            urgentCandidateSet.add(urgentCandidates.getLong(i));
        }
        LongIterator iterator = this.krakk$pendingDirtySections.iterator();
        int candidateScans = 0;
        while (iterator.hasNext()) {
            long sectionKey = iterator.nextLong();
            candidateScans++;
            if (candidateScans > KRAKK_MAX_DIRTY_SCANS_PER_FRAME) {
                break;
            }

            int sectionX = SectionPos.x(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);
            boolean inRange = sectionX >= minChunkX && sectionX <= maxChunkX
                    && sectionZ >= minChunkZ && sectionZ <= maxChunkZ;
            if (!inRange) {
                // Drop stale pending work outside the current render window.
                // Visible sections are re-queued from overlay dirty events when needed.
                iterator.remove();
                this.krakk$discardSection(sectionKey);
                continue;
            }
            if (!krakk$isChunkLoaded(level, sectionX, sectionZ, chunkLoadedCache)) {
                // Drop work for unloaded chunks to avoid starvation of valid sections.
                iterator.remove();
                this.krakk$discardSection(sectionKey);
                continue;
            }
            if (urgentCandidateSet.contains(sectionKey)) {
                continue;
            }
            if (hasVisibilityFilter && !activeVisibleSections.contains(sectionKey)) {
                // Keep pending for when the section enters the visible set.
                continue;
            }

            candidates.add(sectionKey);
        }
        krakk$sortSectionKeysByDistance(candidates, cameraChunkX, cameraChunkZ);

        int urgentQuota;
        int backgroundQuota;
        if (urgentCandidates.isEmpty()) {
            urgentQuota = 0;
            backgroundQuota = maxRebuilds;
        } else if (candidates.isEmpty()) {
            urgentQuota = maxRebuilds;
            backgroundQuota = 0;
        } else if (maxRebuilds <= 1) {
            urgentQuota = 1;
            backgroundQuota = 0;
        } else {
            urgentQuota = Math.max(1, (maxRebuilds * KRAKK_URGENT_REBUILD_SHARE_PERCENT) / 100);
            urgentQuota = Math.min(maxRebuilds - 1, urgentQuota);
            backgroundQuota = maxRebuilds - urgentQuota;
        }

        int urgentRebuilds = 0;
        int urgentIndex = 0;
        while (urgentIndex < urgentCandidates.size() && rebuilds < maxRebuilds && urgentRebuilds < urgentQuota) {
            long sectionKey = urgentCandidates.getLong(urgentIndex++);
            int advanceStatus = this.krakk$advanceQueuedSection(level, blockRenderer, sectionKey, blocksPerPass);
            if (advanceStatus != KRAKK_REBUILD_ADVANCE_IDLE) {
                rebuilds++;
                urgentRebuilds++;
            }
            if (System.nanoTime() >= rebuildDeadlineNanos) {
                return rebuilds;
            }
        }

        int backgroundRebuilds = 0;
        int candidateIndex = 0;
        while (candidateIndex < candidates.size() && rebuilds < maxRebuilds && backgroundRebuilds < backgroundQuota) {
            long sectionKey = candidates.getLong(candidateIndex++);
            int advanceStatus = this.krakk$advanceQueuedSection(level, blockRenderer, sectionKey, blocksPerPass);
            if (advanceStatus != KRAKK_REBUILD_ADVANCE_IDLE) {
                rebuilds++;
                backgroundRebuilds++;
            }
            if (System.nanoTime() >= rebuildDeadlineNanos) {
                return rebuilds;
            }
        }

        while (urgentIndex < urgentCandidates.size() && rebuilds < maxRebuilds) {
            long sectionKey = urgentCandidates.getLong(urgentIndex++);
            int advanceStatus = this.krakk$advanceQueuedSection(level, blockRenderer, sectionKey, blocksPerPass);
            if (advanceStatus != KRAKK_REBUILD_ADVANCE_IDLE) {
                rebuilds++;
            }
            if (System.nanoTime() >= rebuildDeadlineNanos) {
                return rebuilds;
            }
        }

        while (candidateIndex < candidates.size() && rebuilds < maxRebuilds) {
            long sectionKey = candidates.getLong(candidateIndex++);
            int advanceStatus = this.krakk$advanceQueuedSection(level, blockRenderer, sectionKey, blocksPerPass);
            if (advanceStatus != KRAKK_REBUILD_ADVANCE_IDLE) {
                rebuilds++;
            }
            if (System.nanoTime() >= rebuildDeadlineNanos) {
                return rebuilds;
            }
        }
        return rebuilds;
    }

    @Unique
    private int krakk$advanceQueuedSection(ClientLevel level,
                                           BlockRenderDispatcher blockRenderer,
                                           long sectionKey,
                                           int blocksPerPass) {
        int advanceStatus = this.krakk$advanceSectionRebuild(level, blockRenderer, sectionKey, blocksPerPass);
        if (advanceStatus == KRAKK_REBUILD_ADVANCE_COMPLETE) {
            this.krakk$pendingDirtySections.remove(sectionKey);
        } else {
            this.krakk$urgentDirtySections.add(sectionKey);
        }
        return advanceStatus;
    }

    @Unique
    private static void krakk$sortSectionKeysByDistance(LongArrayList sectionKeys, int centerChunkX, int centerChunkZ) {
        for (int i = 1; i < sectionKeys.size(); i++) {
            long currentKey = sectionKeys.getLong(i);
            long currentDistance = krakk$sectionChunkDistanceSquared(currentKey, centerChunkX, centerChunkZ);
            int j = i - 1;
            while (j >= 0) {
                long previousKey = sectionKeys.getLong(j);
                if (krakk$sectionChunkDistanceSquared(previousKey, centerChunkX, centerChunkZ) <= currentDistance) {
                    break;
                }
                sectionKeys.set(j + 1, previousKey);
                j--;
            }
            sectionKeys.set(j + 1, currentKey);
        }
    }

    @Unique
    private static long krakk$sectionChunkDistanceSquared(long sectionKey, int centerChunkX, int centerChunkZ) {
        long dx = SectionPos.x(sectionKey) - (long) centerChunkX;
        long dz = SectionPos.z(sectionKey) - (long) centerChunkZ;
        return (dx * dx) + (dz * dz);
    }

    @Unique
    private int krakk$advanceSectionRebuild(ClientLevel level, BlockRenderDispatcher blockRenderer, long sectionKey, int blocksPerPass) {
        SectionRebuildWork rebuildWork = this.krakk$sectionRebuildWork.get(sectionKey);

        if (rebuildWork == null) {
            Long2ByteOpenHashMap snapshot = KrakkApi.clientOverlay().snapshotSection(this.krakk$activeDimensionId, sectionKey);
            if (snapshot.isEmpty()) {
                snapshot = KrakkChunkSectionDamageBridge.snapshotSection(level, sectionKey);
            }
            if (snapshot.isEmpty()) {
                this.krakk$discardSection(sectionKey);
                return KRAKK_REBUILD_ADVANCE_COMPLETE;
            }

            rebuildWork = new SectionRebuildWork(
                    sectionKey,
                    snapshot,
                    KRAKK_SECTION_REBUILD_STAGE_EXTEND_LIMIT,
                    KRAKK_SECTION_REBUILD_PASS_HARD_LIMIT_MULTIPLIER
            );
            this.krakk$sectionRebuildWork.put(sectionKey, rebuildWork);
        }

        if (rebuildWork.tryCollectCompletedPass()) {
            Throwable passFailure = rebuildWork.takeCompletedFailure();
            if (passFailure != null) {
                if (krakk$isLegacyRandomThreadingFailure(passFailure)) {
                    this.krakk$disableAsyncRebuildRuntime(passFailure);
                } else {
                    KRAKK_LOGGER.warn(
                            "Krakk overlay async rebuild pass failed for section ({}, {}, {}); forcing fresh rebuild.",
                            SectionPos.x(sectionKey),
                            SectionPos.y(sectionKey),
                            SectionPos.z(sectionKey),
                            passFailure
                    );
                }
                this.krakk$cancelSectionRebuild(sectionKey);
                this.krakk$pendingDirtySections.add(sectionKey);
                this.krakk$urgentDirtySections.add(sectionKey);
                return KRAKK_REBUILD_ADVANCE_IDLE;
            }
            try {
                this.krakk$uploadCompletedPassBuffers(rebuildWork);
            } catch (RuntimeException exception) {
                KRAKK_LOGGER.warn(
                        "Krakk overlay upload failed for section ({}, {}, {}); preserving current cache and retrying rebuild.",
                        SectionPos.x(sectionKey),
                        SectionPos.y(sectionKey),
                        SectionPos.z(sectionKey),
                        exception
                );
                this.krakk$cancelSectionRebuild(sectionKey);
                this.krakk$pendingDirtySections.add(sectionKey);
                this.krakk$urgentDirtySections.add(sectionKey);
                return KRAKK_REBUILD_ADVANCE_IDLE;
            }
        }

        if (rebuildWork.isFullyComplete()) {
            this.krakk$finalizeSectionRebuild(sectionKey, rebuildWork);
            return KRAKK_REBUILD_ADVANCE_COMPLETE;
        }

        if (rebuildWork.hasInFlightPass() || rebuildWork.hasCompletedPass()) {
            return KRAKK_REBUILD_ADVANCE_IDLE;
        }

        if (!this.krakk$isAsyncRebuildEnabled()) {
            LongArrayList[] positionsByStage = rebuildWork.consumeNextPass(Math.max(1, blocksPerPass));
            if (krakk$hasStagePositions(positionsByStage)) {
                BufferBuilder.RenderedBuffer[] compiledBuffers;
                try {
                    compiledBuffers = this.krakk$compileStagePass(
                            level,
                            blockRenderer,
                            positionsByStage,
                            rebuildWork.originBlockX(),
                            rebuildWork.originBlockY(),
                            rebuildWork.originBlockZ()
                    );
                } catch (RuntimeException exception) {
                    KRAKK_LOGGER.warn(
                            "Krakk overlay sync rebuild pass failed for section ({}, {}, {}); forcing fresh rebuild.",
                            SectionPos.x(sectionKey),
                            SectionPos.y(sectionKey),
                            SectionPos.z(sectionKey),
                            exception
                    );
                    this.krakk$cancelSectionRebuild(sectionKey);
                    this.krakk$pendingDirtySections.add(sectionKey);
                    this.krakk$urgentDirtySections.add(sectionKey);
                    return KRAKK_REBUILD_ADVANCE_IDLE;
                }
                rebuildWork.setCompletedStageBuffers(compiledBuffers);
                try {
                    this.krakk$uploadCompletedPassBuffers(rebuildWork);
                } catch (RuntimeException exception) {
                    KRAKK_LOGGER.warn(
                            "Krakk overlay upload failed for section ({}, {}, {}); preserving current cache and retrying rebuild.",
                            SectionPos.x(sectionKey),
                            SectionPos.y(sectionKey),
                            SectionPos.z(sectionKey),
                            exception
                    );
                    this.krakk$cancelSectionRebuild(sectionKey);
                    this.krakk$pendingDirtySections.add(sectionKey);
                    this.krakk$urgentDirtySections.add(sectionKey);
                    return KRAKK_REBUILD_ADVANCE_IDLE;
                }
            }
            if (rebuildWork.isFullyComplete()) {
                this.krakk$finalizeSectionRebuild(sectionKey, rebuildWork);
                return KRAKK_REBUILD_ADVANCE_COMPLETE;
            }
            return KRAKK_REBUILD_ADVANCE_PROGRESSED;
        }

        if (this.krakk$countInFlightRebuildPasses() < KRAKK_OVERLAY_MAX_ASYNC_REBUILD_TASKS) {
            LongArrayList[] positionsByStage = rebuildWork.consumeNextPass(Math.max(1, blocksPerPass));
            if (krakk$hasStagePositions(positionsByStage)) {
                this.krakk$captureAsyncRendererDependencies(blockRenderer);
                final int originBlockX = rebuildWork.originBlockX();
                final int originBlockY = rebuildWork.originBlockY();
                final int originBlockZ = rebuildWork.originBlockZ();
                CompletableFuture<BufferBuilder.RenderedBuffer[]> compilePassFuture = CompletableFuture.supplyAsync(() -> this.krakk$compileStagePass(
                        level,
                        this.krakk$getAsyncThreadBlockRendererOrFallback(blockRenderer),
                        positionsByStage,
                        originBlockX,
                        originBlockY,
                        originBlockZ
                ));
                rebuildWork.setInFlightPass(compilePassFuture);
            }
            if (rebuildWork.isFullyComplete()) {
                this.krakk$finalizeSectionRebuild(sectionKey, rebuildWork);
                return KRAKK_REBUILD_ADVANCE_COMPLETE;
            }
            return KRAKK_REBUILD_ADVANCE_PROGRESSED;
        }

        return KRAKK_REBUILD_ADVANCE_IDLE;
    }

    @Unique
    private boolean krakk$isAsyncRebuildEnabled() {
        return KRAKK_OVERLAY_ASYNC_REBUILD_ENABLED && this.krakk$asyncRebuildRuntimeEnabled;
    }

    @Unique
    private void krakk$captureAsyncRendererDependencies(BlockRenderDispatcher blockRenderer) {
        Minecraft minecraft = Minecraft.getInstance();
        KRAKK_ASYNC_SHARED_MODEL_SHAPER = blockRenderer.getBlockModelShaper();
        KRAKK_ASYNC_SHARED_BLOCK_COLORS = minecraft.getBlockColors();
        KRAKK_ASYNC_SHARED_BLOCK_ENTITY_RENDER_DISPATCHER = minecraft.getBlockEntityRenderDispatcher();
        KRAKK_ASYNC_SHARED_ENTITY_MODEL_SET = minecraft.getEntityModels();
    }

    @Unique
    private BlockRenderDispatcher krakk$getAsyncThreadBlockRendererOrFallback(BlockRenderDispatcher fallback) {
        BlockModelShaper sharedModelShaper = KRAKK_ASYNC_SHARED_MODEL_SHAPER;
        BlockColors sharedBlockColors = KRAKK_ASYNC_SHARED_BLOCK_COLORS;
        BlockEntityRenderDispatcher sharedBlockEntityDispatcher = KRAKK_ASYNC_SHARED_BLOCK_ENTITY_RENDER_DISPATCHER;
        EntityModelSet sharedEntityModelSet = KRAKK_ASYNC_SHARED_ENTITY_MODEL_SET;

        if (sharedModelShaper == null
                || sharedBlockColors == null
                || sharedBlockEntityDispatcher == null
                || sharedEntityModelSet == null) {
            return fallback;
        }

        BlockModelShaper threadShaper = KRAKK_ASYNC_THREAD_MODEL_SHAPER.get();
        BlockRenderDispatcher threadRenderer = KRAKK_ASYNC_THREAD_BLOCK_RENDERER.get();
        if (threadRenderer != null && threadShaper == sharedModelShaper) {
            return threadRenderer;
        }

        BlockEntityWithoutLevelRenderer threadItemRenderer =
                new BlockEntityWithoutLevelRenderer(sharedBlockEntityDispatcher, sharedEntityModelSet);
        BlockRenderDispatcher threadLocalRenderer =
                new BlockRenderDispatcher(sharedModelShaper, threadItemRenderer, sharedBlockColors);
        KRAKK_ASYNC_THREAD_BLOCK_RENDERER.set(threadLocalRenderer);
        KRAKK_ASYNC_THREAD_MODEL_SHAPER.set(sharedModelShaper);
        return threadLocalRenderer;
    }

    @Unique
    private void krakk$disableAsyncRebuildRuntime(Throwable cause) {
        if (!this.krakk$asyncRebuildRuntimeEnabled) {
            return;
        }
        this.krakk$asyncRebuildRuntimeEnabled = false;
        if (!this.krakk$loggedAsyncRebuildDisabled) {
            KRAKK_LOGGER.warn(
                    "Krakk overlay async rebuild disabled at runtime due to thread-unsafe block tessellation; falling back to single-threaded rebuilds.",
                    cause
            );
            this.krakk$loggedAsyncRebuildDisabled = true;
        }
    }

    @Unique
    private static boolean krakk$isLegacyRandomThreadingFailure(Throwable throwable) {
        Throwable cursor = throwable;
        while (cursor != null) {
            String message = cursor.getMessage();
            if (message != null && message.contains("LegacyRandomSource from multiple threads")) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    @Unique
    private void krakk$finalizeSectionRebuild(long sectionKey, SectionRebuildWork rebuildWork) {
        this.krakk$sectionRebuildWork.remove(sectionKey);
        SectionRenderCache completedCache = this.krakk$drainPendingSectionCache(sectionKey, rebuildWork);
        if (completedCache.isEmpty()) {
            this.krakk$recordClosedVboCount(completedCache.close());
            this.krakk$clearSectionCacheEntry(sectionKey);
            return;
        }

        SectionRenderCache previousCache = this.krakk$sectionCaches.put(sectionKey, completedCache);
        if (previousCache != null) {
            this.krakk$recordClosedVboCount(previousCache.close());
        }
    }

    @Unique
    private SectionRenderCache krakk$drainPendingSectionCache(long sectionKey, SectionRebuildWork rebuildWork) {
        SectionRenderCache completedCache = new SectionRenderCache(sectionKey);
        for (int stage = 1; stage <= 9; stage++) {
            ArrayList<VertexBuffer> pendingStageBuffers = rebuildWork.takePendingStageBuffers(stage);
            if (pendingStageBuffers == null || pendingStageBuffers.isEmpty()) {
                continue;
            }
            for (int bufferIndex = 0; bufferIndex < pendingStageBuffers.size(); bufferIndex++) {
                completedCache.addStageBuffer(stage, pendingStageBuffers.get(bufferIndex));
            }
        }
        return completedCache;
    }

    @Unique
    private void krakk$uploadCompletedPassBuffers(SectionRebuildWork rebuildWork) {
        for (int stage = 1; stage <= 9; stage++) {
            BufferBuilder.RenderedBuffer renderedBuffer = rebuildWork.takeCompletedStageBuffer(stage);
            if (renderedBuffer == null) {
                continue;
            }
            long estimatedBytes = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? krakk$estimateRenderedBufferBytes(renderedBuffer) : 0L;
            long uploadStartNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? System.nanoTime() : 0L;
            VertexBuffer stageBuffer = this.krakk$uploadStageBuffer(renderedBuffer);
            long uploadNanos = KRAKK_OVERLAY_TIMING_LOGGING_ENABLED ? (System.nanoTime() - uploadStartNanos) : 0L;
            if (stageBuffer != null) {
                rebuildWork.addPendingStageBuffer(stage, stageBuffer);
                if (KRAKK_OVERLAY_TIMING_LOGGING_ENABLED) {
                    this.krakk$overlayTimingUploadTotalNanos += uploadNanos;
                    if (uploadNanos > this.krakk$overlayTimingUploadWorstNanos) {
                        this.krakk$overlayTimingUploadWorstNanos = uploadNanos;
                    }
                    this.krakk$overlayTimingUploadedBuffers++;
                    this.krakk$overlayTimingUploadedBytes += estimatedBytes;
                    this.krakk$overlayTimingCreatedVbos++;
                }
            }
        }
        rebuildWork.clearCompletedPass();
        VertexBuffer.unbind();
    }

    @Unique
    private int krakk$countInFlightRebuildPasses() {
        int inFlight = 0;
        for (SectionRebuildWork rebuildWork : this.krakk$sectionRebuildWork.values()) {
            if (rebuildWork.hasInFlightPass()) {
                inFlight++;
            }
        }
        return inFlight;
    }

    @Unique
    private BufferBuilder.RenderedBuffer[] krakk$compileStagePass(ClientLevel level,
                                                                  BlockRenderDispatcher blockRenderer,
                                                                  LongArrayList[] positionsByStage,
                                                                  int sectionOriginX,
                                                                  int sectionOriginY,
                                                                  int sectionOriginZ) {
        BufferBuilder.RenderedBuffer[] stageBuffers = new BufferBuilder.RenderedBuffer[10];
        try {
            for (int stage = 1; stage <= 9; stage++) {
                LongArrayList stagePositions = positionsByStage[stage];
                if (stagePositions == null || stagePositions.isEmpty()) {
                    continue;
                }
                BufferBuilder.RenderedBuffer renderedBuffer = this.krakk$buildStageRenderedBuffer(
                        level,
                        blockRenderer,
                        stagePositions,
                        sectionOriginX,
                        sectionOriginY,
                        sectionOriginZ
                );
                if (renderedBuffer != null) {
                    stageBuffers[stage] = renderedBuffer;
                }
            }
            return stageBuffers;
        } catch (RuntimeException exception) {
            krakk$discardRenderedBuffers(stageBuffers);
            throw exception;
        }
    }

    @Unique
    private static boolean krakk$hasStagePositions(LongArrayList[] positionsByStage) {
        for (int stage = 1; stage <= 9; stage++) {
            LongArrayList stagePositions = positionsByStage[stage];
            if (stagePositions != null && !stagePositions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Unique
    private VertexBuffer krakk$uploadStageBuffer(BufferBuilder.RenderedBuffer renderedBuffer) {
        if (renderedBuffer == null) {
            return null;
        }
        VertexBuffer vertexBuffer = new VertexBuffer(VertexBuffer.Usage.STATIC);
        try {
            vertexBuffer.bind();
            vertexBuffer.upload(renderedBuffer);
            return vertexBuffer;
        } catch (RuntimeException exception) {
            vertexBuffer.close();
            throw exception;
        }
    }

    @Unique
    private static void krakk$discardRenderedBuffer(BufferBuilder.RenderedBuffer renderedBuffer) {
        if (renderedBuffer == null) {
            return;
        }
        try {
            Method releaseMethod = renderedBuffer.getClass().getMethod("release");
            releaseMethod.invoke(renderedBuffer);
        } catch (Exception ignored) {
            // Best-effort release for dropped async pass buffers.
        }
    }

    @Unique
    private static void krakk$discardRenderedBuffers(BufferBuilder.RenderedBuffer[] renderedBuffers) {
        if (renderedBuffers == null) {
            return;
        }
        for (int stage = 1; stage <= 9; stage++) {
            BufferBuilder.RenderedBuffer renderedBuffer = renderedBuffers[stage];
            renderedBuffers[stage] = null;
            krakk$discardRenderedBuffer(renderedBuffer);
        }
    }

    @Unique
    private static long krakk$estimateRenderedBufferBytes(BufferBuilder.RenderedBuffer renderedBuffer) {
        if (renderedBuffer == null) {
            return 0L;
        }
        long totalBytes = 0L;
        totalBytes += krakk$remainingBytesFromRenderedBufferPart(renderedBuffer, "vertexBuffer");
        totalBytes += krakk$remainingBytesFromRenderedBufferPart(renderedBuffer, "indexBuffer");
        return totalBytes;
    }

    @Unique
    private static long krakk$remainingBytesFromRenderedBufferPart(BufferBuilder.RenderedBuffer renderedBuffer, String accessorName) {
        try {
            Method accessor = renderedBuffer.getClass().getMethod(accessorName);
            Object value = accessor.invoke(renderedBuffer);
            if (value instanceof ByteBuffer byteBuffer) {
                return byteBuffer.remaining();
            }
        } catch (Exception ignored) {
            // Best-effort size estimate only.
        }
        return 0L;
    }

    @Unique
    private static String krakk$buildTopSectionDrawSummary(ArrayList<SectionRenderCache> visibleCaches,
                                                           int[] cacheDrawCallCounts,
                                                           int[] cacheBufferCounts,
                                                           int limit) {
        int scanCount = Math.min(
                visibleCaches.size(),
                Math.min(cacheDrawCallCounts.length, cacheBufferCounts.length)
        );
        if (scanCount <= 0 || limit <= 0) {
            return "none";
        }

        int topCount = Math.min(limit, scanCount);
        int[] topIndices = new int[topCount];
        int[] topScores = new int[topCount];
        Arrays.fill(topIndices, -1);
        Arrays.fill(topScores, -1);

        for (int cacheIndex = 0; cacheIndex < scanCount; cacheIndex++) {
            int score = cacheDrawCallCounts[cacheIndex];
            if (score <= 0) {
                continue;
            }

            for (int slot = 0; slot < topCount; slot++) {
                if (score <= topScores[slot]) {
                    continue;
                }
                for (int shift = topCount - 1; shift > slot; shift--) {
                    topScores[shift] = topScores[shift - 1];
                    topIndices[shift] = topIndices[shift - 1];
                }
                topScores[slot] = score;
                topIndices[slot] = cacheIndex;
                break;
            }
        }

        StringBuilder summary = new StringBuilder();
        for (int slot = 0; slot < topCount; slot++) {
            int cacheIndex = topIndices[slot];
            if (cacheIndex < 0 || cacheIndex >= scanCount) {
                continue;
            }

            SectionRenderCache cache = visibleCaches.get(cacheIndex);
            if (summary.length() > 0) {
                summary.append(" | ");
            }
            summary.append(cache.sectionX())
                    .append(",")
                    .append(cache.sectionY())
                    .append(",")
                    .append(cache.sectionZ())
                    .append(":draw=")
                    .append(cacheDrawCallCounts[cacheIndex])
                    .append(",buf=")
                    .append(cacheBufferCounts[cacheIndex]);
        }

        return summary.length() == 0 ? "none" : summary.toString();
    }

    @Unique
    private BufferBuilder.RenderedBuffer krakk$buildStageRenderedBuffer(ClientLevel level,
                                                                        BlockRenderDispatcher blockRenderer,
                                                                        LongArrayList stagePositions,
                                                                        int sectionOriginX,
                                                                        int sectionOriginY,
                                                                        int sectionOriginZ) {
        BufferBuilder bufferBuilder = new BufferBuilder(krakk$estimateStageBufferCapacity(stagePositions.size()));
        bufferBuilder.begin(VertexFormat.Mode.QUADS, DefaultVertexFormat.BLOCK);
        BlockPos.MutableBlockPos mutableBlockPos = new BlockPos.MutableBlockPos();
        PoseStack blockPose = new PoseStack();
        Matrix4f rotatedDecalPose = new Matrix4f();
        Matrix3f rotatedDecalNormal = new Matrix3f();

        for (int positionIndex = 0; positionIndex < stagePositions.size(); positionIndex++) {
            long posLong = stagePositions.getLong(positionIndex);
            int blockX = BlockPos.getX(posLong);
            int blockY = BlockPos.getY(posLong);
            int blockZ = BlockPos.getZ(posLong);
            mutableBlockPos.set(blockX, blockY, blockZ);
            BlockState blockState = level.getBlockState(mutableBlockPos);
            if (blockState.isAir()) {
                continue;
            }

            blockPose.pushPose();
            blockPose.translate(
                    blockX - sectionOriginX,
                    blockY - sectionOriginY,
                    blockZ - sectionOriginZ
            );

            PoseStack.Pose pose = blockPose.last();
            Matrix4f decalPose = pose.pose();
            Matrix3f decalNormal = pose.normal();
            int quarterTurns = krakk$getDecalQuarterTurns(posLong);
            if (quarterTurns != 0) {
                float angle = quarterTurns * ((float) (Math.PI / 2.0D));
                rotatedDecalPose.set(decalPose).rotateZ(angle);
                rotatedDecalNormal.set(decalNormal).rotateZ(angle);
                decalPose = rotatedDecalPose;
                decalNormal = rotatedDecalNormal;
            }

            VertexConsumer consumer = new SheetedDecalTextureGenerator(bufferBuilder, decalPose, decalNormal, 1.0F);
            blockRenderer.renderBreakingTexture(blockState, mutableBlockPos, level, blockPose, consumer);
            blockPose.popPose();
        }

        return bufferBuilder.endOrDiscardIfEmpty();
    }

    @Unique
    private VertexBuffer krakk$buildStageBuffer(ClientLevel level, BlockRenderDispatcher blockRenderer, LongArrayList stagePositions,
                                                int sectionOriginX, int sectionOriginY, int sectionOriginZ) {
        BufferBuilder.RenderedBuffer renderedBuffer = this.krakk$buildStageRenderedBuffer(
                level,
                blockRenderer,
                stagePositions,
                sectionOriginX,
                sectionOriginY,
                sectionOriginZ
        );
        if (renderedBuffer == null) {
            return null;
        }
        return this.krakk$uploadStageBuffer(renderedBuffer);
    }

    @Unique
    private static int krakk$estimateStageBufferCapacity(int stageBlockCount) {
        if (stageBlockCount <= 0) {
            return KRAKK_BUFFER_BUILDER_CAPACITY;
        }
        long estimated = (long) stageBlockCount * KRAKK_BUFFER_BUILDER_BYTES_PER_STAGE_BLOCK;
        if (estimated < KRAKK_BUFFER_BUILDER_CAPACITY) {
            return KRAKK_BUFFER_BUILDER_CAPACITY;
        }
        if (estimated > KRAKK_BUFFER_BUILDER_MAX_CAPACITY) {
            return KRAKK_BUFFER_BUILDER_MAX_CAPACITY;
        }
        return (int) estimated;
    }

    @Unique
    private void krakk$sweepSectionCache(ClientLevel level, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        long gameTick = level.getGameTime();
        if (this.krakk$lastCacheSweepTick != Long.MIN_VALUE
                && (gameTick - this.krakk$lastCacheSweepTick) < KRAKK_CACHE_SWEEP_INTERVAL_TICKS) {
            return;
        }
        this.krakk$lastCacheSweepTick = gameTick;
        Long2ByteOpenHashMap chunkLoadedCache = new Long2ByteOpenHashMap();
        chunkLoadedCache.defaultReturnValue((byte) -1);

        LongArrayList staleSections = new LongArrayList();
        LongIterator iterator = this.krakk$sectionCaches.keySet().iterator();
        while (iterator.hasNext()) {
            long sectionKey = iterator.nextLong();
            int sectionX = SectionPos.x(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);
            if (sectionX < minChunkX || sectionX > maxChunkX || sectionZ < minChunkZ || sectionZ > maxChunkZ) {
                staleSections.add(sectionKey);
                continue;
            }
            if (!krakk$isChunkLoaded(level, sectionX, sectionZ, chunkLoadedCache)) {
                staleSections.add(sectionKey);
            }
        }

        for (int i = 0; i < staleSections.size(); i++) {
            long sectionKey = staleSections.getLong(i);
            this.krakk$discardSection(sectionKey);
            this.krakk$pendingDirtySections.remove(sectionKey);
            this.krakk$urgentDirtySections.remove(sectionKey);
        }
    }

    @Unique
    private void krakk$clearSectionCacheEntry(long sectionKey) {
        SectionRenderCache cache = this.krakk$sectionCaches.remove(sectionKey);
        if (cache != null) {
            this.krakk$recordClosedVboCount(cache.close());
        }
    }

    @Unique
    private void krakk$recordClosedVboCount(int closedCount) {
        if (closedCount <= 0) {
            return;
        }
        this.krakk$overlayTimingClosedVbos += closedCount;
    }

    @Unique
    private VecRange krakk$computeOverlayViewBudget(double cameraX, double cameraZ, Minecraft minecraft) {
        int baseRenderDistanceChunks = krakk$baseOverlayRenderDistanceChunks(minecraft);
        int baseVisibleCacheCap = Math.max(1, KRAKK_OVERLAY_MAX_VISIBLE_SECTION_CACHES);
        if (!KRAKK_OVERLAY_DYNAMIC_LOD_ENABLED) {
            this.krakk$overlayAdaptiveRangeChunks = baseRenderDistanceChunks;
            this.krakk$overlayAdaptiveVisibleCacheCap = baseVisibleCacheCap;
            this.krakk$lastOverlayCameraX = cameraX;
            this.krakk$lastOverlayCameraZ = cameraZ;
            return krakk$visibleChunkRange(cameraX, cameraZ, baseRenderDistanceChunks);
        }

        int configuredMinRange = Math.min(baseRenderDistanceChunks, KRAKK_OVERLAY_DYNAMIC_MIN_RENDER_DISTANCE_CHUNKS);
        int minRenderDistanceChunks = Math.max(1, configuredMinRange);
        int configuredMinVisibleCap = Math.min(baseVisibleCacheCap, KRAKK_OVERLAY_DYNAMIC_MIN_VISIBLE_SECTION_CACHES);
        int minVisibleCacheCap = Math.max(1, configuredMinVisibleCap);

        int pendingSections = this.krakk$pendingDirtySections.size();
        int backlogSteps = pendingSections / KRAKK_OVERLAY_DYNAMIC_BACKLOG_STEP_SECTIONS;
        int backlogRangeReduction = Math.min(KRAKK_OVERLAY_DYNAMIC_MAX_RANGE_REDUCTION_CHUNKS, backlogSteps);
        int maxCapReductionPerStep = Math.max(
                1,
                KRAKK_OVERLAY_DYNAMIC_MAX_CAP_REDUCTION_PERCENT
                        / Math.max(1, KRAKK_OVERLAY_DYNAMIC_MAX_RANGE_REDUCTION_CHUNKS)
        );
        int backlogCapReductionPercent = Math.min(
                KRAKK_OVERLAY_DYNAMIC_MAX_CAP_REDUCTION_PERCENT,
                backlogSteps * maxCapReductionPerStep
        );

        double cameraDeltaX = Double.isNaN(this.krakk$lastOverlayCameraX)
                ? 0.0D
                : (cameraX - this.krakk$lastOverlayCameraX);
        double cameraDeltaZ = Double.isNaN(this.krakk$lastOverlayCameraZ)
                ? 0.0D
                : (cameraZ - this.krakk$lastOverlayCameraZ);
        double movementDistanceBlocks = Math.sqrt((cameraDeltaX * cameraDeltaX) + (cameraDeltaZ * cameraDeltaZ));
        boolean movementLoad = movementDistanceBlocks >= KRAKK_OVERLAY_DYNAMIC_MOVEMENT_THRESHOLD_BLOCKS;
        int movementRangeReduction = movementLoad ? KRAKK_OVERLAY_DYNAMIC_MOVEMENT_RANGE_REDUCTION_CHUNKS : 0;
        int movementCapReductionPercent = movementLoad ? KRAKK_OVERLAY_DYNAMIC_MOVEMENT_CAP_REDUCTION_PERCENT : 0;

        int targetRenderDistanceChunks = Mth.clamp(
                baseRenderDistanceChunks - backlogRangeReduction - movementRangeReduction,
                minRenderDistanceChunks,
                baseRenderDistanceChunks
        );
        int targetVisibleCapReductionPercent = Math.min(
                90,
                backlogCapReductionPercent + movementCapReductionPercent
        );
        int targetVisibleCacheCap = Math.max(
                minVisibleCacheCap,
                Math.max(1, (baseVisibleCacheCap * (100 - targetVisibleCapReductionPercent)) / 100)
        );

        if (this.krakk$overlayAdaptiveRangeChunks == Integer.MIN_VALUE) {
            this.krakk$overlayAdaptiveRangeChunks = targetRenderDistanceChunks;
        } else if (targetRenderDistanceChunks < this.krakk$overlayAdaptiveRangeChunks) {
            this.krakk$overlayAdaptiveRangeChunks = targetRenderDistanceChunks;
        } else {
            this.krakk$overlayAdaptiveRangeChunks = Math.min(
                    targetRenderDistanceChunks,
                    this.krakk$overlayAdaptiveRangeChunks + 1
            );
        }

        if (this.krakk$overlayAdaptiveVisibleCacheCap == Integer.MIN_VALUE) {
            this.krakk$overlayAdaptiveVisibleCacheCap = targetVisibleCacheCap;
        } else if (targetVisibleCacheCap < this.krakk$overlayAdaptiveVisibleCacheCap) {
            this.krakk$overlayAdaptiveVisibleCacheCap = targetVisibleCacheCap;
        } else {
            this.krakk$overlayAdaptiveVisibleCacheCap = Math.min(
                    targetVisibleCacheCap,
                    this.krakk$overlayAdaptiveVisibleCacheCap + KRAKK_OVERLAY_DYNAMIC_RECOVERY_VISIBLE_CACHES_PER_FRAME
            );
        }

        int effectiveRenderDistanceChunks = Mth.clamp(
                this.krakk$overlayAdaptiveRangeChunks,
                minRenderDistanceChunks,
                baseRenderDistanceChunks
        );
        int effectiveVisibleCacheCap = Mth.clamp(
                this.krakk$overlayAdaptiveVisibleCacheCap,
                minVisibleCacheCap,
                baseVisibleCacheCap
        );

        this.krakk$overlayAdaptiveRangeChunks = effectiveRenderDistanceChunks;
        this.krakk$overlayAdaptiveVisibleCacheCap = effectiveVisibleCacheCap;
        this.krakk$lastOverlayCameraX = cameraX;
        this.krakk$lastOverlayCameraZ = cameraZ;
        return krakk$visibleChunkRange(cameraX, cameraZ, effectiveRenderDistanceChunks);
    }

    @Unique
    private int krakk$computeOverlayDrawCallBudget() {
        int baseDrawCallBudget = Math.max(1, KRAKK_OVERLAY_DRAW_CALL_BUDGET);
        int minDrawCallBudget = Math.max(1, Math.min(baseDrawCallBudget, KRAKK_OVERLAY_DRAW_CALL_BUDGET_MIN));
        int targetReductionPercent = 0;

        if (this.krakk$overlayPreviousFrameNanos >= KRAKK_OVERLAY_TIMING_HITCH_NANOS) {
            targetReductionPercent += KRAKK_OVERLAY_DRAW_CALL_HITCH_REDUCTION_PERCENT;
        }
        if (this.krakk$overlayPreviousDrawCalls >= KRAKK_OVERLAY_DRAW_CALL_PRESSURE_THRESHOLD) {
            targetReductionPercent += KRAKK_OVERLAY_DRAW_CALL_PRESSURE_REDUCTION_PERCENT;
        }

        int clampedReductionPercent = Math.min(90, targetReductionPercent);
        int targetDrawCallBudget = Math.max(
                minDrawCallBudget,
                Math.max(1, (baseDrawCallBudget * (100 - clampedReductionPercent)) / 100)
        );

        if (this.krakk$overlayAdaptiveDrawCallBudget == Integer.MIN_VALUE) {
            this.krakk$overlayAdaptiveDrawCallBudget = targetDrawCallBudget;
        } else if (targetDrawCallBudget < this.krakk$overlayAdaptiveDrawCallBudget) {
            this.krakk$overlayAdaptiveDrawCallBudget = targetDrawCallBudget;
        } else {
            this.krakk$overlayAdaptiveDrawCallBudget = Math.min(
                    targetDrawCallBudget,
                    this.krakk$overlayAdaptiveDrawCallBudget + KRAKK_OVERLAY_DRAW_CALL_BUDGET_RECOVERY_PER_FRAME
            );
        }

        this.krakk$overlayAdaptiveDrawCallBudget = Mth.clamp(
                this.krakk$overlayAdaptiveDrawCallBudget,
                minDrawCallBudget,
                baseDrawCallBudget
        );
        return this.krakk$overlayAdaptiveDrawCallBudget;
    }

    @Unique
    private static int krakk$drawBudgetPressurePercent(int drawCallBudget) {
        int baseDrawCallBudget = Math.max(1, KRAKK_OVERLAY_DRAW_CALL_BUDGET);
        int clampedBudget = Math.max(1, Math.min(baseDrawCallBudget, drawCallBudget));
        return (int) Math.round((1.0D - (clampedBudget / (double) baseDrawCallBudget)) * 100.0D);
    }

    @Unique
    private static int krakk$stageCapForSectionDistance(long sectionDistanceSquared, int drawBudgetPressurePercent) {
        if (drawBudgetPressurePercent < 20) {
            return 9;
        }
        if (drawBudgetPressurePercent < 40) {
            if (sectionDistanceSquared >= 64L) {
                return 6;
            }
            if (sectionDistanceSquared >= 25L) {
                return 7;
            }
            return 9;
        }
        if (sectionDistanceSquared >= 64L) {
            return 3;
        }
        if (sectionDistanceSquared >= 25L) {
            return 4;
        }
        if (sectionDistanceSquared >= 9L) {
            return 6;
        }
        return 9;
    }

    @Unique
    private void krakk$updateOverlayPressureState(long frameNanos, int drawCalls) {
        this.krakk$overlayPreviousFrameNanos = Math.max(0L, frameNanos);
        this.krakk$overlayPreviousDrawCalls = Math.max(0, drawCalls);
    }

    @Unique
    private static int krakk$baseOverlayRenderDistanceChunks(Minecraft minecraft) {
        return Math.min(
                minecraft.options.getEffectiveRenderDistance(),
                KRAKK_OVERLAY_RENDER_DISTANCE_CHUNKS
        );
    }

    @Unique
    private static VecRange krakk$visibleChunkRange(double cameraX, double cameraZ, int renderDistanceChunks) {
        int cameraChunkX = Mth.floor(cameraX) >> 4;
        int cameraChunkZ = Mth.floor(cameraZ) >> 4;
        return new VecRange(
                cameraChunkX - renderDistanceChunks,
                cameraChunkX + renderDistanceChunks,
                cameraChunkZ - renderDistanceChunks,
                cameraChunkZ + renderDistanceChunks
        );
    }

    @Unique
    private ArrayList<SectionRenderCache> krakk$collectVisibleCaches(ClientLevel level,
                                                                      int minChunkX,
                                                                      int maxChunkX,
                                                                      int minChunkZ,
                                                                      int maxChunkZ,
                                                                      LongOpenHashSet activeVisibleSections) {
        ArrayList<SectionRenderCache> visibleCaches = new ArrayList<>(this.krakk$sectionCaches.size());
        Long2ByteOpenHashMap chunkLoadedCache = this.krakk$chunkLoadedScratch;
        chunkLoadedCache.clear();
        chunkLoadedCache.defaultReturnValue((byte) -1);
        LongArrayList staleSections = this.krakk$staleSectionKeysScratch;
        staleSections.clear();
        boolean hasVisibilityFilter = activeVisibleSections != null && !activeVisibleSections.isEmpty();
        LongIterator sectionKeyIterator = this.krakk$sectionCaches.keySet().iterator();
        while (sectionKeyIterator.hasNext()) {
            long sectionKey = sectionKeyIterator.nextLong();
            SectionRenderCache cache = this.krakk$sectionCaches.get(sectionKey);
            if (cache == null) {
                continue;
            }
            if (cache.inChunkRange(minChunkX, maxChunkX, minChunkZ, maxChunkZ)) {
                if (hasVisibilityFilter && !activeVisibleSections.contains(cache.sectionKey())) {
                    continue;
                }
                if (!krakk$isChunkLoaded(level, cache.sectionX(), cache.sectionZ(), chunkLoadedCache)) {
                    staleSections.add(sectionKey);
                    continue;
                }
                visibleCaches.add(cache);
            }
        }
        for (int i = 0; i < staleSections.size(); i++) {
            long sectionKey = staleSections.getLong(i);
            this.krakk$discardSection(sectionKey);
            this.krakk$pendingDirtySections.remove(sectionKey);
            this.krakk$urgentDirtySections.remove(sectionKey);
        }
        return visibleCaches;
    }

    @Unique
    private void krakk$cancelSectionRebuild(long sectionKey) {
        SectionRebuildWork rebuildWork = this.krakk$sectionRebuildWork.remove(sectionKey);
        if (rebuildWork != null) {
            this.krakk$recordClosedVboCount(rebuildWork.cancel());
        }
    }

    @Unique
    private void krakk$discardSection(long sectionKey) {
        this.krakk$cancelSectionRebuild(sectionKey);
        this.krakk$clearSectionCacheEntry(sectionKey);
    }

    @Unique
    private LongOpenHashSet krakk$collectActiveVisibleSectionKeys() {
        LongOpenHashSet visibleSections = this.krakk$visibleSectionsScratch;
        visibleSections.clear();
        if (KRAKK_SODIUM_VISIBILITY_ENABLED) {
            if (this.krakk$collectSodiumVisibleSectionKeys(visibleSections)) {
                if (!this.krakk$loggedSodiumVisibilitySource) {
                    KRAKK_LOGGER.info("Krakk overlay visibility source: sodium");
                    this.krakk$loggedSodiumVisibilitySource = true;
                }
                return visibleSections;
            }
        }

        this.krakk$collectVanillaVisibleSectionKeys(visibleSections);
        if (!this.krakk$loggedVanillaVisibilitySource && !visibleSections.isEmpty()) {
            KRAKK_LOGGER.info("Krakk overlay visibility source: vanilla");
            this.krakk$loggedVanillaVisibilitySource = true;
        }
        return visibleSections;
    }

    @Unique
    private boolean krakk$collectSodiumVisibleSectionKeys(LongOpenHashSet visibleSections) {
        visibleSections.clear();
        if (!krakk$initializeSodiumVisibilityReflection()) {
            return false;
        }

        try {
            Object sodiumWorldRenderer = KRAKK_SODIUM_INSTANCE_NULLABLE_METHOD.invoke(null);
            if (sodiumWorldRenderer == null) {
                return false;
            }

            Object renderSectionManager = KRAKK_SODIUM_RENDER_SECTION_MANAGER_FIELD.get(sodiumWorldRenderer);
            if (renderSectionManager == null) {
                return false;
            }

            Object sortedRenderLists = KRAKK_SODIUM_GET_RENDER_LISTS_METHOD.invoke(renderSectionManager);
            if (sortedRenderLists == null) {
                return false;
            }

            Object chunkRenderListIteratorObject = KRAKK_SODIUM_SORTED_LISTS_ITERATOR_METHOD.invoke(sortedRenderLists, Boolean.FALSE);
            if (!(chunkRenderListIteratorObject instanceof Iterator<?> chunkRenderListIterator)) {
                return false;
            }

            while (chunkRenderListIterator.hasNext()) {
                Object chunkRenderList = chunkRenderListIterator.next();
                if (chunkRenderList == null) {
                    continue;
                }

                Object renderRegion = KRAKK_SODIUM_CHUNK_LIST_GET_REGION_METHOD.invoke(chunkRenderList);
                if (renderRegion == null) {
                    continue;
                }

                Object geometrySectionsIterator = KRAKK_SODIUM_CHUNK_LIST_SECTIONS_WITH_GEOMETRY_ITERATOR_METHOD.invoke(chunkRenderList, Boolean.FALSE);
                while (geometrySectionsIterator != null
                        && Boolean.TRUE.equals(KRAKK_SODIUM_BYTE_ITERATOR_HAS_NEXT_METHOD.invoke(geometrySectionsIterator))) {
                    int localSectionIndex = ((Number) KRAKK_SODIUM_BYTE_ITERATOR_NEXT_BYTE_AS_INT_METHOD.invoke(geometrySectionsIterator)).intValue();
                    Object renderSection = KRAKK_SODIUM_RENDER_REGION_GET_SECTION_METHOD.invoke(renderRegion, localSectionIndex);
                    if (renderSection == null) {
                        continue;
                    }

                    int sectionX = ((Number) KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_X_METHOD.invoke(renderSection)).intValue();
                    int sectionY = ((Number) KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_Y_METHOD.invoke(renderSection)).intValue();
                    int sectionZ = ((Number) KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_Z_METHOD.invoke(renderSection)).intValue();
                    visibleSections.add(SectionPos.asLong(sectionX, sectionY, sectionZ));
                }
            }

            return !visibleSections.isEmpty();
        } catch (Exception exception) {
            KRAKK_SODIUM_REFLECTION_AVAILABLE = false;
            KRAKK_LOGGER.warn("Krakk Sodium visibility reflection failed; falling back to vanilla visibility.", exception);
            visibleSections.clear();
            return false;
        }
    }

    @Unique
    private static boolean krakk$initializeSodiumVisibilityReflection() {
        if (KRAKK_SODIUM_REFLECTION_INITIALIZED) {
            return KRAKK_SODIUM_REFLECTION_AVAILABLE;
        }

        synchronized (KrakkLevelRendererMixin.class) {
            if (KRAKK_SODIUM_REFLECTION_INITIALIZED) {
                return KRAKK_SODIUM_REFLECTION_AVAILABLE;
            }

            try {
                ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                if (contextClassLoader == null) {
                    contextClassLoader = KrakkLevelRendererMixin.class.getClassLoader();
                }
                Class<?> sodiumWorldRendererClass = Class.forName(KRAKK_SODIUM_WORLD_RENDERER_CLASS, false, contextClassLoader);
                Class<?> renderSectionManagerClass = Class.forName(KRAKK_SODIUM_RENDER_SECTION_MANAGER_CLASS, false, contextClassLoader);
                Class<?> sortedRenderListsClass = Class.forName(KRAKK_SODIUM_SORTED_RENDER_LISTS_CLASS, false, contextClassLoader);
                Class<?> chunkRenderListClass = Class.forName(KRAKK_SODIUM_CHUNK_RENDER_LIST_CLASS, false, contextClassLoader);
                Class<?> renderRegionClass = Class.forName(KRAKK_SODIUM_RENDER_REGION_CLASS, false, contextClassLoader);
                Class<?> renderSectionClass = Class.forName(KRAKK_SODIUM_RENDER_SECTION_CLASS, false, contextClassLoader);
                Class<?> byteIteratorClass = Class.forName(KRAKK_SODIUM_BYTE_ITERATOR_CLASS, false, contextClassLoader);

                KRAKK_SODIUM_INSTANCE_NULLABLE_METHOD = sodiumWorldRendererClass.getMethod("instanceNullable");
                KRAKK_SODIUM_RENDER_SECTION_MANAGER_FIELD = sodiumWorldRendererClass.getDeclaredField("renderSectionManager");
                KRAKK_SODIUM_RENDER_SECTION_MANAGER_FIELD.setAccessible(true);
                KRAKK_SODIUM_GET_RENDER_LISTS_METHOD = renderSectionManagerClass.getMethod("getRenderLists");
                KRAKK_SODIUM_SORTED_LISTS_ITERATOR_METHOD = sortedRenderListsClass.getMethod("iterator", boolean.class);
                KRAKK_SODIUM_CHUNK_LIST_SECTIONS_WITH_GEOMETRY_ITERATOR_METHOD =
                        chunkRenderListClass.getMethod("sectionsWithGeometryIterator", boolean.class);
                KRAKK_SODIUM_CHUNK_LIST_GET_REGION_METHOD = chunkRenderListClass.getMethod("getRegion");
                KRAKK_SODIUM_RENDER_REGION_GET_SECTION_METHOD = renderRegionClass.getMethod("getSection", int.class);
                KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_X_METHOD = renderSectionClass.getMethod("getChunkX");
                KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_Y_METHOD = renderSectionClass.getMethod("getChunkY");
                KRAKK_SODIUM_RENDER_SECTION_GET_CHUNK_Z_METHOD = renderSectionClass.getMethod("getChunkZ");
                KRAKK_SODIUM_BYTE_ITERATOR_HAS_NEXT_METHOD = byteIteratorClass.getMethod("hasNext");
                KRAKK_SODIUM_BYTE_ITERATOR_NEXT_BYTE_AS_INT_METHOD = byteIteratorClass.getMethod("nextByteAsInt");

                KRAKK_SODIUM_REFLECTION_AVAILABLE = true;
            } catch (Exception ignored) {
                KRAKK_SODIUM_REFLECTION_AVAILABLE = false;
            }

            KRAKK_SODIUM_REFLECTION_INITIALIZED = true;
            return KRAKK_SODIUM_REFLECTION_AVAILABLE;
        }
    }

    @Unique
    private void krakk$collectVanillaVisibleSectionKeys(LongOpenHashSet visibleSections) {
        visibleSections.clear();
        if (this.renderChunksInFrustum == null || this.renderChunksInFrustum.isEmpty()) {
            return;
        }

        for (int i = 0; i < this.renderChunksInFrustum.size(); i++) {
            Object info = this.renderChunksInFrustum.get(i);
            ChunkRenderDispatcher.RenderChunk renderChunk = krakk$getRenderChunkFromInfo(info);
            if (renderChunk == null) {
                continue;
            }

            BlockPos origin = renderChunk.getOrigin();
            int sectionX = SectionPos.blockToSectionCoord(origin.getX());
            int sectionY = SectionPos.blockToSectionCoord(origin.getY());
            int sectionZ = SectionPos.blockToSectionCoord(origin.getZ());
            visibleSections.add(SectionPos.asLong(sectionX, sectionY, sectionZ));
        }
    }

    @Unique
    private void krakk$ensureVisibleCacheScratchCapacity(int visibleCacheCount) {
        if (this.krakk$visibleCacheOffsetXScratch.length < visibleCacheCount) {
            this.krakk$visibleCacheOffsetXScratch = Arrays.copyOf(this.krakk$visibleCacheOffsetXScratch, visibleCacheCount);
            this.krakk$visibleCacheOffsetYScratch = Arrays.copyOf(this.krakk$visibleCacheOffsetYScratch, visibleCacheCount);
            this.krakk$visibleCacheOffsetZScratch = Arrays.copyOf(this.krakk$visibleCacheOffsetZScratch, visibleCacheCount);
            this.krakk$visibleCacheDrawCallsScratch = Arrays.copyOf(this.krakk$visibleCacheDrawCallsScratch, visibleCacheCount);
            this.krakk$visibleCacheBufferCountsScratch = Arrays.copyOf(this.krakk$visibleCacheBufferCountsScratch, visibleCacheCount);
            this.krakk$visibleCacheStageCapScratch = Arrays.copyOf(this.krakk$visibleCacheStageCapScratch, visibleCacheCount);
        }
    }

    @Unique
    private static ChunkRenderDispatcher.RenderChunk krakk$getRenderChunkFromInfo(Object info) {
        if (info == null) {
            return null;
        }

        Class<?> infoClass = info.getClass();
        if (KRAKK_RENDER_CHUNK_INFO_NO_CHUNK_FIELD.contains(infoClass)) {
            return null;
        }

        Field chunkField = KRAKK_RENDER_CHUNK_INFO_CHUNK_FIELDS.get(infoClass);
        if (chunkField == null) {
            Field[] declaredFields = infoClass.getDeclaredFields();
            for (Field declaredField : declaredFields) {
                if (ChunkRenderDispatcher.RenderChunk.class.isAssignableFrom(declaredField.getType())) {
                    chunkField = declaredField;
                    chunkField.setAccessible(true);
                    KRAKK_RENDER_CHUNK_INFO_CHUNK_FIELDS.put(infoClass, chunkField);
                    break;
                }
            }
            if (chunkField == null) {
                KRAKK_RENDER_CHUNK_INFO_NO_CHUNK_FIELD.add(infoClass);
                return null;
            }
        }

        try {
            Object chunk = chunkField.get(info);
            return chunk instanceof ChunkRenderDispatcher.RenderChunk renderChunk ? renderChunk : null;
        } catch (IllegalAccessException ignored) {
            KRAKK_RENDER_CHUNK_INFO_CHUNK_FIELDS.remove(infoClass);
            KRAKK_RENDER_CHUNK_INFO_NO_CHUNK_FIELD.add(infoClass);
            return null;
        }
    }

    @Unique
    private static void krakk$sortVisibleCachesByDistance(ArrayList<SectionRenderCache> caches,
                                                          int centerChunkX,
                                                          int centerChunkZ) {
        for (int i = 1; i < caches.size(); i++) {
            SectionRenderCache current = caches.get(i);
            long currentDistance = krakk$sectionChunkDistanceSquared(current.sectionX(), current.sectionZ(), centerChunkX, centerChunkZ);
            int j = i - 1;
            while (j >= 0) {
                SectionRenderCache previous = caches.get(j);
                if (krakk$sectionChunkDistanceSquared(previous.sectionX(), previous.sectionZ(), centerChunkX, centerChunkZ) <= currentDistance) {
                    break;
                }
                caches.set(j + 1, previous);
                j--;
            }
            caches.set(j + 1, current);
        }
    }

    @Unique
    private static boolean krakk$isChunkLoaded(ClientLevel level, int chunkX, int chunkZ, Long2ByteOpenHashMap chunkLoadedCache) {
        long chunkKey = (((long) chunkX) << 32) ^ (chunkZ & 0xFFFFFFFFL);
        byte cached = chunkLoadedCache.get(chunkKey);
        if (cached == -1) {
            cached = (byte) (level.getChunkSource().getChunkNow(chunkX, chunkZ) == null ? 0 : 1);
            chunkLoadedCache.put(chunkKey, cached);
        }
        return cached == 1;
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
    private static double krakk$nanosToMillis(long nanos) {
        return nanos / 1_000_000.0D;
    }

    @Unique
    private static long krakk$sectionChunkDistanceSquared(int sectionX, int sectionZ, int centerChunkX, int centerChunkZ) {
        long dx = sectionX - (long) centerChunkX;
        long dz = sectionZ - (long) centerChunkZ;
        return (dx * dx) + (dz * dz);
    }

}
