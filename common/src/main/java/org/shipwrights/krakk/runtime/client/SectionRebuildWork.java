package org.shipwrights.krakk.runtime.client;

import com.mojang.blaze3d.vertex.BufferBuilder;
import com.mojang.blaze3d.vertex.VertexBuffer;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.minecraft.core.SectionPos;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class SectionRebuildWork {
    private final int originBlockX;
    private final int originBlockY;
    private final int originBlockZ;
    private final long[] blockPositions;
    private final byte[] blockStages;
    private final int stageExtendLimit;
    private final int hardLimitMultiplier;
    @SuppressWarnings("unchecked")
    private final ArrayList<VertexBuffer>[] pendingStageBuffers = (ArrayList<VertexBuffer>[]) new ArrayList<?>[10];
    private int cursor = 0;
    private CompletableFuture<BufferBuilder.RenderedBuffer[]> inFlightPass;
    private BufferBuilder.RenderedBuffer[] completedStageBuffers;
    private Throwable completedFailure;

    public SectionRebuildWork(long sectionKey,
                              Long2ByteOpenHashMap snapshot,
                              int stageExtendLimit,
                              int hardLimitMultiplier) {
        int sectionX = SectionPos.x(sectionKey);
        int sectionY = SectionPos.y(sectionKey);
        int sectionZ = SectionPos.z(sectionKey);
        this.originBlockX = sectionX << 4;
        this.originBlockY = sectionY << 4;
        this.originBlockZ = sectionZ << 4;
        this.stageExtendLimit = Math.max(1, stageExtendLimit);
        this.hardLimitMultiplier = Math.max(1, hardLimitMultiplier);

        LongArrayList[] positionsByStage = new LongArrayList[10];
        int entryCount = 0;
        for (Long2ByteMap.Entry entry : snapshot.long2ByteEntrySet()) {
            int stage = toDestroyStage(entry.getByteValue());
            if (stage <= 0) {
                continue;
            }
            LongArrayList stagePositions = positionsByStage[stage];
            if (stagePositions == null) {
                stagePositions = new LongArrayList();
                positionsByStage[stage] = stagePositions;
            }
            stagePositions.add(entry.getLongKey());
            entryCount++;
        }

        this.blockPositions = new long[entryCount];
        this.blockStages = new byte[entryCount];
        int cursor = 0;
        for (int stage = 1; stage <= 9; stage++) {
            LongArrayList stagePositions = positionsByStage[stage];
            if (stagePositions == null || stagePositions.isEmpty()) {
                continue;
            }
            for (int i = 0; i < stagePositions.size(); i++) {
                this.blockPositions[cursor] = stagePositions.getLong(i);
                this.blockStages[cursor] = (byte) stage;
                cursor++;
            }
        }
    }

    public int originBlockX() {
        return this.originBlockX;
    }

    public int originBlockY() {
        return this.originBlockY;
    }

    public int originBlockZ() {
        return this.originBlockZ;
    }

    public void addPendingStageBuffer(int stage, VertexBuffer buffer) {
        ArrayList<VertexBuffer> stageBuffers = this.pendingStageBuffers[stage];
        if (stageBuffers == null) {
            stageBuffers = new ArrayList<>();
            this.pendingStageBuffers[stage] = stageBuffers;
        }
        stageBuffers.add(buffer);
    }

    public ArrayList<VertexBuffer> takePendingStageBuffers(int stage) {
        ArrayList<VertexBuffer> stageBuffers = this.pendingStageBuffers[stage];
        this.pendingStageBuffers[stage] = null;
        return stageBuffers;
    }

    public boolean isComplete() {
        return this.cursor >= this.blockPositions.length;
    }

    public boolean hasInFlightPass() {
        return this.inFlightPass != null;
    }

    public void setInFlightPass(CompletableFuture<BufferBuilder.RenderedBuffer[]> inFlightPass) {
        this.inFlightPass = inFlightPass;
    }

    public boolean hasCompletedPass() {
        return this.completedStageBuffers != null || this.completedFailure != null;
    }

    public boolean tryCollectCompletedPass() {
        if (this.inFlightPass == null || !this.inFlightPass.isDone()) {
            return false;
        }

        try {
            this.completedStageBuffers = this.inFlightPass.join();
            this.completedFailure = null;
        } catch (CompletionException exception) {
            this.completedStageBuffers = null;
            this.completedFailure = exception.getCause() == null ? exception : exception.getCause();
        } catch (RuntimeException exception) {
            this.completedStageBuffers = null;
            this.completedFailure = exception;
        }
        this.inFlightPass = null;
        return true;
    }

    public void setCompletedStageBuffers(BufferBuilder.RenderedBuffer[] completedStageBuffers) {
        this.completedStageBuffers = completedStageBuffers;
        this.completedFailure = null;
    }

    public BufferBuilder.RenderedBuffer takeCompletedStageBuffer(int stage) {
        if (this.completedStageBuffers == null) {
            return null;
        }
        BufferBuilder.RenderedBuffer renderedBuffer = this.completedStageBuffers[stage];
        this.completedStageBuffers[stage] = null;
        return renderedBuffer;
    }

    public Throwable takeCompletedFailure() {
        Throwable failure = this.completedFailure;
        this.completedFailure = null;
        return failure;
    }

    public void clearCompletedPass() {
        if (this.completedStageBuffers != null) {
            discardRenderedBuffers(this.completedStageBuffers);
            this.completedStageBuffers = null;
        }
        this.completedFailure = null;
    }

    public boolean isFullyComplete() {
        return this.isComplete() && this.inFlightPass == null && this.completedStageBuffers == null && this.completedFailure == null;
    }

    public int cancel() {
        int closedVbos = 0;
        if (this.inFlightPass != null) {
            this.inFlightPass.cancel(false);
            this.inFlightPass = null;
        }
        this.clearCompletedPass();
        for (int stage = 1; stage <= 9; stage++) {
            ArrayList<VertexBuffer> stageBuffers = this.pendingStageBuffers[stage];
            if (stageBuffers == null) {
                continue;
            }
            for (int i = 0; i < stageBuffers.size(); i++) {
                stageBuffers.get(i).close();
                closedVbos++;
            }
            stageBuffers.clear();
            this.pendingStageBuffers[stage] = null;
        }
        return closedVbos;
    }

    public LongArrayList[] consumeNextPass(int maxBlocks) {
        LongArrayList[] positionsByStage = new LongArrayList[10];
        int basePassLimit = Math.max(1, maxBlocks);
        int limit = Math.min(this.blockPositions.length, this.cursor + basePassLimit);
        int hardLimit = Math.min(
                this.blockPositions.length,
                this.cursor + (basePassLimit * this.hardLimitMultiplier)
        );
        // Avoid splitting a stage into many tiny pass fragments; this lowers draw-call fan-out.
        if (limit < this.blockPositions.length && limit > this.cursor) {
            int trailingStage = this.blockStages[limit - 1] & 0xFF;
            int extension = 0;
            while (limit < this.blockPositions.length
                    && limit < hardLimit
                    && extension < this.stageExtendLimit
                    && (this.blockStages[limit] & 0xFF) == trailingStage) {
                limit++;
                extension++;
            }
        }
        while (this.cursor < limit) {
            int stage = this.blockStages[this.cursor] & 0xFF;
            if (stage > 0) {
                LongArrayList stagePositions = positionsByStage[stage];
                if (stagePositions == null) {
                    stagePositions = new LongArrayList();
                    positionsByStage[stage] = stagePositions;
                }
                stagePositions.add(this.blockPositions[this.cursor]);
            }
            this.cursor++;
        }
        return positionsByStage;
    }

    private static int toDestroyStage(int damageState) {
        int clamped = Math.max(0, Math.min(15, damageState));
        if (clamped <= 0) {
            return 0;
        }
        return Math.max(1, Math.min(9, (int) Math.ceil((clamped * 9.0D) / 15.0D)));
    }

    private static void discardRenderedBuffers(BufferBuilder.RenderedBuffer[] renderedBuffers) {
        if (renderedBuffers == null) {
            return;
        }
        for (int stage = 1; stage <= 9; stage++) {
            BufferBuilder.RenderedBuffer renderedBuffer = renderedBuffers[stage];
            renderedBuffers[stage] = null;
            discardRenderedBuffer(renderedBuffer);
        }
    }

    private static void discardRenderedBuffer(BufferBuilder.RenderedBuffer renderedBuffer) {
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
}
