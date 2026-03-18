package org.shipwrights.krakk.runtime.client;

import com.mojang.blaze3d.vertex.VertexBuffer;
import net.minecraft.core.SectionPos;

import java.util.ArrayList;

public final class SectionRenderCache {
    private final int sectionX;
    private final int sectionY;
    private final int sectionZ;
    private final int originBlockX;
    private final int originBlockY;
    private final int originBlockZ;
    @SuppressWarnings("unchecked")
    private final ArrayList<VertexBuffer>[] stageBuffers = (ArrayList<VertexBuffer>[]) new ArrayList<?>[10];
    private int stageMask = 0;
    private int stageBufferCount = 0;

    public SectionRenderCache(long sectionKey) {
        this.sectionX = SectionPos.x(sectionKey);
        this.sectionY = SectionPos.y(sectionKey);
        this.sectionZ = SectionPos.z(sectionKey);
        this.originBlockX = this.sectionX << 4;
        this.originBlockY = this.sectionY << 4;
        this.originBlockZ = this.sectionZ << 4;
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

    public int sectionX() {
        return this.sectionX;
    }

    public int sectionY() {
        return this.sectionY;
    }

    public long sectionKey() {
        return SectionPos.asLong(this.sectionX, this.sectionY, this.sectionZ);
    }

    public int sectionZ() {
        return this.sectionZ;
    }

    public void addStageBuffer(int stage, VertexBuffer buffer) {
        ArrayList<VertexBuffer> buffers = this.stageBuffers[stage];
        if (buffers == null) {
            buffers = new ArrayList<>();
            this.stageBuffers[stage] = buffers;
        }
        buffers.add(buffer);
        this.stageMask |= (1 << stage);
        this.stageBufferCount++;
    }

    public ArrayList<VertexBuffer> getStageBuffers(int stage) {
        return this.stageBuffers[stage];
    }

    public boolean hasStage(int stage) {
        return (this.stageMask & (1 << stage)) != 0;
    }

    public boolean isEmpty() {
        return this.stageMask == 0;
    }

    public boolean inChunkRange(int minChunkX, int maxChunkX, int minChunkZ, int maxChunkZ) {
        return this.sectionX >= minChunkX && this.sectionX <= maxChunkX
                && this.sectionZ >= minChunkZ && this.sectionZ <= maxChunkZ;
    }

    public int totalStageBufferCount() {
        return this.stageBufferCount;
    }

    public int close() {
        int closed = 0;
        for (int stage = 1; stage <= 9; stage++) {
            ArrayList<VertexBuffer> buffers = this.stageBuffers[stage];
            if (buffers == null) {
                continue;
            }
            for (int i = 0; i < buffers.size(); i++) {
                buffers.get(i).close();
                closed++;
            }
            buffers.clear();
            this.stageBuffers[stage] = null;
        }
        this.stageMask = 0;
        this.stageBufferCount = 0;
        return closed;
    }
}
