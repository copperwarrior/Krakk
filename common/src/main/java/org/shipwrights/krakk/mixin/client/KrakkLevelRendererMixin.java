package org.shipwrights.krakk.mixin.client;

import com.mojang.blaze3d.vertex.BufferBuilder;
import com.mojang.blaze3d.vertex.DefaultVertexFormat;
import com.mojang.blaze3d.vertex.PoseStack;
import com.mojang.blaze3d.vertex.SheetedDecalTextureGenerator;
import com.mojang.blaze3d.vertex.VertexBuffer;
import com.mojang.blaze3d.vertex.VertexConsumer;
import com.mojang.blaze3d.vertex.VertexFormat;
import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.client.Camera;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.renderer.GameRenderer;
import net.minecraft.client.renderer.LevelRenderer;
import net.minecraft.client.renderer.LightTexture;
import net.minecraft.client.renderer.RenderType;
import net.minecraft.client.renderer.ShaderInstance;
import net.minecraft.client.renderer.block.BlockRenderDispatcher;
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
import org.shipwrights.krakk.state.chunk.KrakkChunkSectionDamageBridge;
import org.slf4j.Logger;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(value = LevelRenderer.class, priority = 900)
public abstract class KrakkLevelRendererMixin {
    @Unique
    private static final Logger KRAKK_LOGGER = LogUtils.getLogger();
    @Unique
    private static final int KRAKK_MAX_SECTION_REBUILDS_PER_FRAME = 12;
    @Unique
    private static final int KRAKK_MAX_DIRTY_SCANS_PER_FRAME = 256;
    @Unique
    private static final long KRAKK_CACHE_SWEEP_INTERVAL_TICKS = 40L;
    @Unique
    private static final long KRAKK_SECTION_DISCOVERY_INTERVAL_TICKS = 20L;
    @Unique
    private static final int KRAKK_BUFFER_BUILDER_CAPACITY = 256;

    @Unique
    private final LongOpenHashSet krakk$pendingDirtySections = new LongOpenHashSet();
    @Unique
    private final LongLinkedOpenHashSet krakk$urgentDirtySections = new LongLinkedOpenHashSet();
    @Unique
    private final Long2ObjectOpenHashMap<SectionRenderCache> krakk$sectionCaches = new Long2ObjectOpenHashMap<>();
    private ResourceLocation krakk$activeDimensionId = null;
    @Unique
    private ClientLevel krakk$activeLevel = null;
    @Unique
    private long krakk$lastCacheSweepTick = Long.MIN_VALUE;
    @Unique
    private long krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
    @Unique
    private boolean krakk$loggedRendererInit = false;

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
        for (SectionRenderCache cache : this.krakk$sectionCaches.values()) {
            cache.close();
        }
        this.krakk$sectionCaches.clear();
        this.krakk$pendingDirtySections.clear();
        this.krakk$urgentDirtySections.clear();
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
            this.krakk$clearRenderCache();
            return;
        }

        if (level != this.krakk$activeLevel) {
            this.krakk$activeLevel = level;
            this.krakk$activeDimensionId = level.dimension().location();
            this.krakk$lastCacheSweepTick = Long.MIN_VALUE;
            this.krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
            this.krakk$clearRenderCache();
            this.krakk$loggedRendererInit = false;
        }

        ResourceLocation dimensionId = level.dimension().location();
        if (!dimensionId.equals(this.krakk$activeDimensionId)) {
            this.krakk$activeDimensionId = dimensionId;
            this.krakk$lastCacheSweepTick = Long.MIN_VALUE;
            this.krakk$lastSectionDiscoveryTick = Long.MIN_VALUE;
            this.krakk$clearRenderCache();
            this.krakk$loggedRendererInit = false;
        }

        long[] dirtySections = KrakkApi.clientOverlay().consumeDirtySections(dimensionId);
        for (long sectionKey : dirtySections) {
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
        Minecraft minecraft = Minecraft.getInstance();
        ClientLevel level = minecraft.level;
        if (level == null || this.krakk$activeDimensionId == null) {
            return;
        }
        if (!this.krakk$loggedRendererInit) {
            KRAKK_LOGGER.info("Krakk renderer: chunk-layer overlay integration active");
            this.krakk$loggedRendererInit = true;
        }

        BlockRenderDispatcher blockRenderer = minecraft.getBlockRenderer();
        VecRange range = krakk$visibleChunkRange(cameraX, cameraZ, minecraft);
        this.krakk$enqueueVisibleDamageSections(level, range.minChunkX(), range.maxChunkX(), range.minChunkZ(), range.maxChunkZ());
        this.krakk$rebuildDirtySections(level, blockRenderer, range.minChunkX(), range.maxChunkX(), range.minChunkZ(), range.maxChunkZ());
        this.krakk$sweepSectionCache(level, range.minChunkX(), range.maxChunkX(), range.minChunkZ(), range.maxChunkZ());
        ShaderInstance overlayShader = KrakkClientShaders.damageOverlayShaderOrFallback();
        if (overlayShader == null) {
            return;
        }

        for (int stage = 1; stage <= 9; stage++) {
            RenderType stageRenderType = ModelBakery.DESTROY_TYPES.get(stage);
            boolean stageRendered = false;

            for (SectionRenderCache cache : this.krakk$sectionCaches.values()) {
                if (!cache.inChunkRange(range.minChunkX(), range.maxChunkX(), range.minChunkZ(), range.maxChunkZ())) {
                    continue;
                }

                VertexBuffer buffer = cache.getStageBuffer(stage);
                if (buffer == null) {
                    continue;
                }

                if (!stageRendered) {
                    stageRenderType.setupRenderState();
                    stageRendered = true;
                    KrakkClientShaders.syncFogUniforms(overlayShader);
                }

                Matrix4f modelViewMatrix = new Matrix4f(poseStack.last().pose())
                        .translate(
                                (float) (cache.originX() - cameraX),
                                (float) (cache.originY() - cameraY),
                                (float) (cache.originZ() - cameraZ)
                        );
                buffer.bind();
                try {
                    buffer.drawWithShader(modelViewMatrix, projectionMatrix, overlayShader);
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
                    KrakkClientShaders.syncFogUniforms(overlayShader);
                    buffer.drawWithShader(modelViewMatrix, projectionMatrix, overlayShader);
                }
            }

            if (stageRendered) {
                VertexBuffer.unbind();
                stageRenderType.clearRenderState();
            }
        }
    }

    @Unique
    private void krakk$enqueueVisibleDamageSections(ClientLevel level, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        if (this.krakk$activeDimensionId == null) {
            return;
        }

        long[] visibleSections = KrakkApi.clientOverlay().snapshotSectionsInChunkRange(
                this.krakk$activeDimensionId,
                minChunkX,
                maxChunkX,
                minChunkZ,
                maxChunkZ
        );
        LongOpenHashSet visibleSet = new LongOpenHashSet(visibleSections.length);
        for (long sectionKey : visibleSections) {
            visibleSet.add(sectionKey);
        }
        long gameTick = level.getGameTime();
        if (this.krakk$lastSectionDiscoveryTick == Long.MIN_VALUE
                || gameTick < this.krakk$lastSectionDiscoveryTick
                || (gameTick - this.krakk$lastSectionDiscoveryTick) >= KRAKK_SECTION_DISCOVERY_INTERVAL_TICKS) {
            // Safety net for chunk-load timing races: discover damage directly from chunk section state,
            // not only overlay cache.
            KrakkChunkSectionDamageBridge.collectSectionKeysInChunkRange(
                    level,
                    minChunkX,
                    maxChunkX,
                    minChunkZ,
                    maxChunkZ,
                    visibleSet
            );
            this.krakk$lastSectionDiscoveryTick = gameTick;
        }

        // Only queue sections that do not already have a built cache.
        LongIterator visibleIterator = visibleSet.iterator();
        while (visibleIterator.hasNext()) {
            long sectionKey = visibleIterator.nextLong();
            if (!this.krakk$sectionCaches.containsKey(sectionKey)) {
                this.krakk$pendingDirtySections.add(sectionKey);
            }
        }

        // Keep in-range cache entries stable. They are invalidated by dirty section rebuilds,
        // and out-of-range/unloaded entries are swept separately.
    }

    @Unique
    private void krakk$rebuildDirtySections(ClientLevel level, BlockRenderDispatcher blockRenderer,
                                            int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        if (this.krakk$pendingDirtySections.isEmpty()) {
            return;
        }

        int rebuilds = 0;
        int scanned = 0;

        LongIterator urgentIterator = this.krakk$urgentDirtySections.iterator();
        while (urgentIterator.hasNext() && rebuilds < KRAKK_MAX_SECTION_REBUILDS_PER_FRAME && scanned < KRAKK_MAX_DIRTY_SCANS_PER_FRAME) {
            long sectionKey = urgentIterator.nextLong();
            urgentIterator.remove();
            scanned++;

            int sectionX = SectionPos.x(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);
            boolean inRange = sectionX >= minChunkX && sectionX <= maxChunkX
                    && sectionZ >= minChunkZ && sectionZ <= maxChunkZ;
            if (!inRange || level.getChunkSource().getChunkNow(sectionX, sectionZ) == null) {
                this.krakk$pendingDirtySections.remove(sectionKey);
                continue;
            }

            this.krakk$rebuildSectionCache(level, blockRenderer, sectionKey);
            this.krakk$pendingDirtySections.remove(sectionKey);
            rebuilds++;
        }

        if (rebuilds >= KRAKK_MAX_SECTION_REBUILDS_PER_FRAME) {
            return;
        }

        LongArrayList candidates = new LongArrayList(Math.min(this.krakk$pendingDirtySections.size(), KRAKK_MAX_DIRTY_SCANS_PER_FRAME));
        LongIterator iterator = this.krakk$pendingDirtySections.iterator();
        while (iterator.hasNext() && scanned < KRAKK_MAX_DIRTY_SCANS_PER_FRAME) {
            long sectionKey = iterator.nextLong();
            scanned++;

            int sectionX = SectionPos.x(sectionKey);
            int sectionZ = SectionPos.z(sectionKey);
            boolean inRange = sectionX >= minChunkX && sectionX <= maxChunkX
                    && sectionZ >= minChunkZ && sectionZ <= maxChunkZ;
            if (!inRange) {
                // Drop stale pending work outside the current render window.
                // Visible sections are re-queued by discovery when needed.
                iterator.remove();
                continue;
            }
            if (level.getChunkSource().getChunkNow(sectionX, sectionZ) == null) {
                // Drop work for unloaded chunks to avoid starvation of valid sections.
                iterator.remove();
                continue;
            }

            candidates.add(sectionKey);
        }

        for (int i = 0; i < candidates.size() && rebuilds < KRAKK_MAX_SECTION_REBUILDS_PER_FRAME; i++) {
            long sectionKey = candidates.getLong(i);
            this.krakk$rebuildSectionCache(level, blockRenderer, sectionKey);
            this.krakk$pendingDirtySections.remove(sectionKey);
            rebuilds++;
        }
    }

    @Unique
    private void krakk$rebuildSectionCache(ClientLevel level, BlockRenderDispatcher blockRenderer, long sectionKey) {
        this.krakk$clearSectionCacheEntry(sectionKey);

        Long2ByteOpenHashMap snapshot = KrakkApi.clientOverlay().snapshotSection(this.krakk$activeDimensionId, sectionKey);
        if (snapshot.isEmpty()) {
            snapshot = KrakkChunkSectionDamageBridge.snapshotSection(level, sectionKey);
        }
        if (snapshot.isEmpty()) {
            return;
        }

        SectionRenderCache cache = new SectionRenderCache(sectionKey);
        LongOpenHashSet[] positionsByStage = new LongOpenHashSet[10];
        for (Long2ByteMap.Entry entry : snapshot.long2ByteEntrySet()) {
            int stage = krakk$toDestroyStage(entry.getByteValue());
            if (stage <= 0) {
                continue;
            }
            LongOpenHashSet stagePositions = positionsByStage[stage];
            if (stagePositions == null) {
                stagePositions = new LongOpenHashSet();
                positionsByStage[stage] = stagePositions;
            }
            stagePositions.add(entry.getLongKey());
        }

        for (int stage = 1; stage <= 9; stage++) {
            LongOpenHashSet stagePositions = positionsByStage[stage];
            if (stagePositions == null || stagePositions.isEmpty()) {
                continue;
            }
            VertexBuffer stageBuffer = this.krakk$buildStageBuffer(
                    level,
                    blockRenderer,
                    stagePositions,
                    cache.originBlockX(),
                    cache.originBlockY(),
                    cache.originBlockZ()
            );
            if (stageBuffer != null) {
                cache.setStageBuffer(stage, stageBuffer);
            }
        }
        VertexBuffer.unbind();

        if (cache.isEmpty()) {
            cache.close();
            return;
        }
        this.krakk$sectionCaches.put(sectionKey, cache);
    }

    @Unique
    private VertexBuffer krakk$buildStageBuffer(ClientLevel level, BlockRenderDispatcher blockRenderer, LongOpenHashSet stagePositions,
                                                int sectionOriginX, int sectionOriginY, int sectionOriginZ) {
        BufferBuilder bufferBuilder = new BufferBuilder(KRAKK_BUFFER_BUILDER_CAPACITY);
        bufferBuilder.begin(VertexFormat.Mode.QUADS, DefaultVertexFormat.BLOCK);

        LongIterator iterator = stagePositions.iterator();
        while (iterator.hasNext()) {
            long posLong = iterator.nextLong();
            BlockPos blockPos = BlockPos.of(posLong);
            BlockState blockState = level.getBlockState(blockPos);
            if (blockState.isAir()) {
                continue;
            }

            PoseStack blockPose = new PoseStack();
            blockPose.translate(
                    blockPos.getX() - sectionOriginX,
                    blockPos.getY() - sectionOriginY,
                    blockPos.getZ() - sectionOriginZ
            );

            PoseStack.Pose pose = blockPose.last();
            Matrix4f decalPose = pose.pose();
            Matrix3f decalNormal = pose.normal();
            int quarterTurns = krakk$getDecalQuarterTurns(posLong);
            if (quarterTurns != 0) {
                float angle = quarterTurns * ((float) (Math.PI / 2.0D));
                decalPose = new Matrix4f(decalPose).rotateZ(angle);
                decalNormal = new Matrix3f(decalNormal).rotateZ(angle);
            }

            VertexConsumer consumer = new SheetedDecalTextureGenerator(bufferBuilder, decalPose, decalNormal, 1.0F);
            blockRenderer.renderBreakingTexture(blockState, blockPos, level, blockPose, consumer);
        }

        BufferBuilder.RenderedBuffer renderedBuffer = bufferBuilder.endOrDiscardIfEmpty();
        if (renderedBuffer == null) {
            return null;
        }

        VertexBuffer vertexBuffer = new VertexBuffer(VertexBuffer.Usage.STATIC);
        vertexBuffer.bind();
        vertexBuffer.upload(renderedBuffer);
        return vertexBuffer;
    }

    @Unique
    private void krakk$sweepSectionCache(ClientLevel level, int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        long gameTick = level.getGameTime();
        if (this.krakk$lastCacheSweepTick != Long.MIN_VALUE
                && (gameTick - this.krakk$lastCacheSweepTick) < KRAKK_CACHE_SWEEP_INTERVAL_TICKS) {
            return;
        }
        this.krakk$lastCacheSweepTick = gameTick;

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
            if (level.getChunkSource().getChunkNow(sectionX, sectionZ) == null) {
                staleSections.add(sectionKey);
            }
        }

        for (int i = 0; i < staleSections.size(); i++) {
            this.krakk$clearSectionCacheEntry(staleSections.getLong(i));
        }
    }

    @Unique
    private void krakk$clearSectionCacheEntry(long sectionKey) {
        SectionRenderCache cache = this.krakk$sectionCaches.remove(sectionKey);
        if (cache != null) {
            cache.close();
        }
    }

    @Unique
    private static VecRange krakk$visibleChunkRange(double cameraX, double cameraZ, Minecraft minecraft) {
        int renderDistanceChunks = minecraft.options.getEffectiveRenderDistance();
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
    private record VecRange(int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
    }

    @Unique
    private static final class SectionRenderCache {
        private final int sectionX;
        private final int sectionY;
        private final int sectionZ;
        private final int originBlockX;
        private final int originBlockY;
        private final int originBlockZ;
        private final VertexBuffer[] stageBuffers = new VertexBuffer[10];

        private SectionRenderCache(long sectionKey) {
            this.sectionX = SectionPos.x(sectionKey);
            this.sectionY = SectionPos.y(sectionKey);
            this.sectionZ = SectionPos.z(sectionKey);
            this.originBlockX = this.sectionX << 4;
            this.originBlockY = this.sectionY << 4;
            this.originBlockZ = this.sectionZ << 4;
        }

        private int originBlockX() {
            return this.originBlockX;
        }

        private int originBlockY() {
            return this.originBlockY;
        }

        private int originBlockZ() {
            return this.originBlockZ;
        }

        private double originX() {
            return this.originBlockX;
        }

        private double originY() {
            return this.originBlockY;
        }

        private double originZ() {
            return this.originBlockZ;
        }

        private void setStageBuffer(int stage, VertexBuffer buffer) {
            this.stageBuffers[stage] = buffer;
        }

        private VertexBuffer getStageBuffer(int stage) {
            return this.stageBuffers[stage];
        }

        private boolean isEmpty() {
            for (int stage = 1; stage <= 9; stage++) {
                if (this.stageBuffers[stage] != null) {
                    return false;
                }
            }
            return true;
        }

        private boolean inChunkRange(int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
            return this.sectionX >= minChunkX && this.sectionX <= maxChunkX
                    && this.sectionZ >= minChunkZ && this.sectionZ <= maxChunkZ;
        }

        private void close() {
            for (int stage = 1; stage <= 9; stage++) {
                VertexBuffer buffer = this.stageBuffers[stage];
                if (buffer != null) {
                    buffer.close();
                    this.stageBuffers[stage] = null;
                }
            }
        }
    }
}
