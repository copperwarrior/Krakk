package org.shipwrights.krakk.runtime.explosion;

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.minecraft.core.BlockPos;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.PriorityQueue;

// ─── Float indexed access ─────────────────────────────────────────────────────

interface FloatIndexedAccess {
    int size();

    float getFloat(int index);
}

interface MutableFloatIndexedAccess extends FloatIndexedAccess {
    void setFloat(int index, float value);

    void fill(float value);
}

final class HeapFloatArrayStorage implements MutableFloatIndexedAccess {
    private final float[] values;

    HeapFloatArrayStorage(int size) {
        this.values = new float[size];
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public float getFloat(int index) {
        return values[index];
    }

    @Override
    public void setFloat(int index, float value) {
        values[index] = value;
    }

    @Override
    public void fill(float value) {
        Arrays.fill(values, value);
    }
}

final class SparseAirSolidSlownessStorage implements MutableFloatIndexedAccess {
    private final int size;
    private final float airSlowness;
    private final BitSet activeMask;
    private final Int2FloatOpenHashMap solidSlownessByIndex;

    SparseAirSolidSlownessStorage(int size, float airSlowness) {
        this.size = Math.max(1, size);
        this.airSlowness = airSlowness;
        this.activeMask = new BitSet(this.size);
        this.solidSlownessByIndex = new Int2FloatOpenHashMap(1024);
        this.solidSlownessByIndex.defaultReturnValue(Float.NaN);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public float getFloat(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
        }
        if (!activeMask.get(index)) {
            return 0.0F;
        }
        float solidSlowness = solidSlownessByIndex.get(index);
        return Float.isFinite(solidSlowness) ? solidSlowness : airSlowness;
    }

    @Override
    public void setFloat(int index, float value) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
        }
        if (!(value > 0.0F)) {
            activeMask.clear(index);
            solidSlownessByIndex.remove(index);
            return;
        }
        activeMask.set(index);
        if (Math.abs(value - airSlowness) <= 1.0E-6F) {
            solidSlownessByIndex.remove(index);
            return;
        }
        solidSlownessByIndex.put(index, value);
    }

    @Override
    public void fill(float value) {
        solidSlownessByIndex.clear();
        if (!(value > 0.0F)) {
            activeMask.clear();
            return;
        }
        activeMask.set(0, size);
        if (Math.abs(value - airSlowness) <= 1.0E-6F) {
            return;
        }
        for (int index = 0; index < size; index++) {
            solidSlownessByIndex.put(index, value);
        }
    }

    BitSet activeMask() {
        return activeMask;
    }
}

// ─── Float chunk (chunked off-heap / heap storage) ───────────────────────────

interface FloatChunk {
    float getFloat(int index);

    void setFloat(int index, float value);

    void fill(float value);
}

final class HeapFloatChunk implements FloatChunk {
    private final float[] values;

    HeapFloatChunk(int size) {
        this.values = new float[size];
    }

    @Override
    public float getFloat(int index) {
        return values[index];
    }

    @Override
    public void setFloat(int index, float value) {
        values[index] = value;
    }

    @Override
    public void fill(float value) {
        Arrays.fill(values, value);
    }
}

final class DirectFloatChunk implements FloatChunk {
    private static final sun.misc.Unsafe UNSAFE;
    private static final long BUFFER_ADDRESS_OFFSET;
    static {
        try {
            java.lang.reflect.Field fu = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            fu.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) fu.get(null);
            java.lang.reflect.Field fa = java.nio.Buffer.class.getDeclaredField("address");
            BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(fa);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private final ByteBuffer values; // held to prevent GC of native memory
    private final long address;

    DirectFloatChunk(int size) {
        this.values = ByteBuffer.allocateDirect(size * Float.BYTES);
        this.address = UNSAFE.getLong(values, BUFFER_ADDRESS_OFFSET);
    }

    @Override
    public float getFloat(int index) {
        return UNSAFE.getFloat(address + (long) index * Float.BYTES);
    }

    @Override
    public void setFloat(int index, float value) {
        UNSAFE.putFloat(address + (long) index * Float.BYTES, value);
    }

    @Override
    public void fill(float value) {
        int limit = values.capacity() / Float.BYTES;
        for (int i = 0; i < limit; i++) {
            UNSAFE.putFloat(address + (long) i * Float.BYTES, value);
        }
    }
}

final class AdaptiveFloatArrayStorage implements MutableFloatIndexedAccess {
    private static final int CHUNK_SHIFT = 20;
    private static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;

    // Plain array instead of ArrayList: eliminates ArrayList.get() overhead (bounds check +
    // virtual dispatch) on every getFloat/setFloat call in the solveKrakkCell hot path.
    private final FloatChunk[] chunks;
    private final int size;

    AdaptiveFloatArrayStorage(int size, boolean preferOffHeapFirst) {
        this.size = size;
        int expectedChunks = Math.max(1, (size + CHUNK_SIZE - 1) / CHUNK_SIZE);
        this.chunks = new FloatChunk[expectedChunks];
        int remaining = size;
        for (int i = 0; i < expectedChunks; i++) {
            int chunkSize = Math.min(CHUNK_SIZE, remaining);
            chunks[i] = createChunk(chunkSize, preferOffHeapFirst);
            remaining -= chunkSize;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public float getFloat(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
        }
        int chunkIndex = index >>> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        return chunks[chunkIndex].getFloat(chunkOffset);
    }

    @Override
    public void setFloat(int index, float value) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
        }
        int chunkIndex = index >>> CHUNK_SHIFT;
        int chunkOffset = index & CHUNK_MASK;
        chunks[chunkIndex].setFloat(chunkOffset, value);
    }

    @Override
    public void fill(float value) {
        for (FloatChunk chunk : chunks) {
            chunk.fill(value);
        }
    }

    private static FloatChunk createChunk(int chunkSize, boolean preferOffHeapFirst) {
        if (preferOffHeapFirst) {
            try {
                return new DirectFloatChunk(chunkSize);
            } catch (OutOfMemoryError directAllocationFailure) {
                // Fall back to heap for this chunk.
            }
            return new HeapFloatChunk(chunkSize);
        }

        try {
            return new HeapFloatChunk(chunkSize);
        } catch (OutOfMemoryError heapAllocationFailure) {
            // Fall back to direct for this chunk.
        }
        return new DirectFloatChunk(chunkSize);
    }
}

final class PagedFloatArrayStorage implements MutableFloatIndexedAccess {
    private static final int PAGE_SHIFT = 16;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final Cleaner CLEANER = Cleaner.create();

    private final int size;
    private final int pageCount;
    private final int maxCachedPages;
    private final float[][] cachePages;
    private final int[] cachePageIds;
    private final boolean[] cacheDirty;
    private final long[] cacheAccessTicks;
    private final Int2IntOpenHashMap pageToSlot;
    private final boolean[] persistedPages;
    private final FileChannel channel;
    private final ByteBuffer ioBuffer;
    private float defaultFillValue;
    private long accessTick;

    PagedFloatArrayStorage(int size, int requestedCachePages) {
        this.size = Math.max(1, size);
        this.pageCount = Math.max(1, (this.size + PAGE_SIZE - 1) / PAGE_SIZE);
        this.maxCachedPages = Math.max(4, Math.min(this.pageCount, requestedCachePages));
        this.cachePages = new float[this.maxCachedPages][PAGE_SIZE];
        this.cachePageIds = new int[this.maxCachedPages];
        Arrays.fill(this.cachePageIds, -1);
        this.cacheDirty = new boolean[this.maxCachedPages];
        this.cacheAccessTicks = new long[this.maxCachedPages];
        this.pageToSlot = new Int2IntOpenHashMap(Math.max(16, this.maxCachedPages * 2));
        this.pageToSlot.defaultReturnValue(-1);
        this.persistedPages = new boolean[this.pageCount];
        this.ioBuffer = ByteBuffer.allocate(PAGE_SIZE * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        this.defaultFillValue = 0.0F;
        this.accessTick = 1L;

        Path backingFile = null;
        FileChannel openedChannel = null;
        try {
            backingFile = Files.createTempFile("krakk-float-pages-", ".bin");
            openedChannel = FileChannel.open(
                    backingFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            CLEANER.register(this, new PagedFloatStorageCleanup(openedChannel, backingFile));
        } catch (IOException exception) {
            if (backingFile != null) {
                try {
                    Files.deleteIfExists(backingFile);
                } catch (IOException ignored) {
                }
            }
            throw new IllegalStateException("Unable to create paged float storage.", exception);
        }
        this.channel = openedChannel;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public float getFloat(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
        }
        int pageIndex = index >>> PAGE_SHIFT;
        int pageOffset = index & PAGE_MASK;
        int slot = ensurePageLoaded(pageIndex);
        cacheAccessTicks[slot] = accessTick++;
        return cachePages[slot][pageOffset];
    }

    @Override
    public void setFloat(int index, float value) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
        }
        int pageIndex = index >>> PAGE_SHIFT;
        int pageOffset = index & PAGE_MASK;
        int slot = ensurePageLoaded(pageIndex);
        cacheAccessTicks[slot] = accessTick++;
        cachePages[slot][pageOffset] = value;
        cacheDirty[slot] = true;
    }

    @Override
    public void fill(float value) {
        defaultFillValue = value;
        Arrays.fill(persistedPages, false);
        pageToSlot.clear();
        Arrays.fill(cachePageIds, -1);
        Arrays.fill(cacheDirty, false);
        Arrays.fill(cacheAccessTicks, 0L);
        accessTick = 1L;
        try {
            channel.truncate(0L);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to clear paged float storage.", exception);
        }
    }

    private int ensurePageLoaded(int pageIndex) {
        int existingSlot = pageToSlot.get(pageIndex);
        if (existingSlot >= 0) {
            return existingSlot;
        }
        int slot = claimSlot();
        int previousPageIndex = cachePageIds[slot];
        if (previousPageIndex >= 0) {
            if (cacheDirty[slot]) {
                writePage(previousPageIndex, cachePages[slot]);
                cacheDirty[slot] = false;
            }
            pageToSlot.remove(previousPageIndex);
        }

        int length = pageLength(pageIndex);
        if (persistedPages[pageIndex]) {
            readPage(pageIndex, cachePages[slot], length);
        } else {
            Arrays.fill(cachePages[slot], 0, length, defaultFillValue);
        }
        cachePageIds[slot] = pageIndex;
        cacheAccessTicks[slot] = accessTick++;
        cacheDirty[slot] = false;
        pageToSlot.put(pageIndex, slot);
        return slot;
    }

    private int claimSlot() {
        for (int i = 0; i < maxCachedPages; i++) {
            if (cachePageIds[i] < 0) {
                return i;
            }
        }
        int lruSlot = 0;
        long lruTick = cacheAccessTicks[0];
        for (int i = 1; i < maxCachedPages; i++) {
            long tick = cacheAccessTicks[i];
            if (tick < lruTick) {
                lruTick = tick;
                lruSlot = i;
            }
        }
        return lruSlot;
    }

    private void writePage(int pageIndex, float[] pageData) {
        int length = pageLength(pageIndex);
        int byteLength = length * Float.BYTES;
        ioBuffer.clear();
        ioBuffer.limit(byteLength);
        for (int i = 0; i < length; i++) {
            ioBuffer.putFloat(pageData[i]);
        }
        ioBuffer.flip();
        long writePos = pageStartOffset(pageIndex);
        try {
            while (ioBuffer.hasRemaining()) {
                int written = channel.write(ioBuffer, writePos);
                if (written <= 0) {
                    throw new IOException("Unable to write paged float page.");
                }
                writePos += written;
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to write paged float storage.", exception);
        }
        persistedPages[pageIndex] = true;
    }

    private void readPage(int pageIndex, float[] pageData, int length) {
        int byteLength = length * Float.BYTES;
        ioBuffer.clear();
        ioBuffer.limit(byteLength);
        long readPos = pageStartOffset(pageIndex);
        int totalRead = 0;
        try {
            while (totalRead < byteLength) {
                int read = channel.read(ioBuffer, readPos + totalRead);
                if (read <= 0) {
                    break;
                }
                totalRead += read;
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read paged float storage.", exception);
        }
        ioBuffer.flip();
        int readableFloats = totalRead / Float.BYTES;
        for (int i = 0; i < readableFloats; i++) {
            pageData[i] = ioBuffer.getFloat();
        }
        if (readableFloats < length) {
            Arrays.fill(pageData, readableFloats, length, defaultFillValue);
        }
    }

    private int pageLength(int pageIndex) {
        int pageStart = pageIndex << PAGE_SHIFT;
        int remaining = size - pageStart;
        return Math.max(0, Math.min(PAGE_SIZE, remaining));
    }

    private long pageStartOffset(int pageIndex) {
        return (long) pageIndex * (long) PAGE_SIZE * (long) Float.BYTES;
    }
}

final class PagedFloatStorageCleanup implements Runnable {
    private final FileChannel channel;
    private final Path filePath;

    PagedFloatStorageCleanup(FileChannel channel, Path filePath) {
        this.channel = channel;
        this.filePath = filePath;
    }

    @Override
    public void run() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
        if (filePath != null) {
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException ignored) {
            }
        }
    }
}

// ─── Source index set ─────────────────────────────────────────────────────────

@FunctionalInterface
interface IntIndexConsumer {
    void accept(int index);
}

interface SourceIndexSet {
    boolean contains(int index);

    boolean add(int index);

    void forEach(IntIndexConsumer consumer);
}

final class BitSetSourceIndexSet implements SourceIndexSet {
    private final BitSet indices;

    BitSetSourceIndexSet(int approximateVolume) {
        this.indices = new BitSet(Math.max(1, approximateVolume));
    }

    @Override
    public boolean contains(int index) {
        return indices.get(index);
    }

    @Override
    public boolean add(int index) {
        if (indices.get(index)) {
            return false;
        }
        indices.set(index);
        return true;
    }

    @Override
    public void forEach(IntIndexConsumer consumer) {
        if (consumer == null) {
            return;
        }
        for (int index = indices.nextSetBit(0); index >= 0; index = indices.nextSetBit(index + 1)) {
            consumer.accept(index);
        }
    }
}

final class SparseSourceIndexSet implements SourceIndexSet {
    private final IntOpenHashSet membership;
    private final IntArrayList insertionOrder;

    SparseSourceIndexSet() {
        this.membership = new IntOpenHashSet(32);
        this.insertionOrder = new IntArrayList(32);
    }

    @Override
    public boolean contains(int index) {
        return membership.contains(index);
    }

    @Override
    public boolean add(int index) {
        if (!membership.add(index)) {
            return false;
        }
        insertionOrder.add(index);
        return true;
    }

    @Override
    public void forEach(IntIndexConsumer consumer) {
        if (consumer == null) {
            return;
        }
        for (int i = 0; i < insertionOrder.size(); i++) {
            consumer.accept(insertionOrder.getInt(i));
        }
    }
}

// ─── Long indexed access ──────────────────────────────────────────────────────

interface LongIndexedAccess {
    int size();

    boolean isEmpty();

    long getLong(int index);
}

final class EmptyLongIndexedAccess implements LongIndexedAccess {
    static final EmptyLongIndexedAccess INSTANCE = new EmptyLongIndexedAccess();

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public long getLong(int index) {
        throw new IndexOutOfBoundsException("index=" + index + " size=0");
    }
}

final class LongArrayListIndexedAccess implements LongIndexedAccess {
    private final LongArrayList values;

    LongArrayListIndexedAccess(LongArrayList values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public long getLong(int index) {
        return values.getLong(index);
    }
}

final class PackedKrakkSolidPositions implements LongIndexedAccess {
    private final IntArrayList indices;
    private final int minX;
    private final int minY;
    private final int minZ;
    private final int strideX;
    private final int strideY;

    PackedKrakkSolidPositions(int minX, int minY, int minZ, int sizeY, int sizeZ, int expectedEntries) {
        this.indices = new IntArrayList(Math.max(16, expectedEntries));
        this.minX = minX;
        this.minY = minY;
        this.minZ = minZ;
        this.strideX = sizeY * sizeZ;
        this.strideY = sizeZ;
    }

    void addIndex(int index) {
        indices.add(index);
    }

    void clear() {
        indices.clear();
    }

    @Override
    public int size() {
        return indices.size();
    }

    @Override
    public boolean isEmpty() {
        return indices.isEmpty();
    }

    @Override
    public long getLong(int index) {
        int gridIndex = indices.getInt(index);
        int xOffset = gridIndex / strideX;
        int yz = gridIndex - (xOffset * strideX);
        int yOffset = yz / strideY;
        int zOffset = yz - (yOffset * strideY);
        return BlockPos.asLong(minX + xOffset, minY + yOffset, minZ + zOffset);
    }
}

// ─── Priority queues ──────────────────────────────────────────────────────────

final class KrakkMinHeap {
    private int[] indices;
    private double[] priorities;
    private int size;
    private double polledPriority;

    KrakkMinHeap(int initialCapacity) {
        int capacity = Math.max(16, initialCapacity);
        this.indices = new int[capacity];
        this.priorities = new double[capacity];
        this.size = 0;
        this.polledPriority = Double.POSITIVE_INFINITY;
    }

    boolean isEmpty() {
        return size <= 0;
    }

    void add(int index, double priority) {
        ensureCapacity(size + 1);
        int cursor = size++;
        while (cursor > 0) {
            int parent = (cursor - 1) >>> 1;
            if (priority >= priorities[parent]) {
                break;
            }
            indices[cursor] = indices[parent];
            priorities[cursor] = priorities[parent];
            cursor = parent;
        }
        indices[cursor] = index;
        priorities[cursor] = priority;
    }

    int pollIndex() {
        int rootIndex = indices[0];
        polledPriority = priorities[0];
        size--;
        if (size > 0) {
            int tailIndex = indices[size];
            double tailPriority = priorities[size];
            int cursor = 0;
            while (true) {
                int left = (cursor << 1) + 1;
                if (left >= size) {
                    break;
                }
                int right = left + 1;
                int child = right < size && priorities[right] < priorities[left] ? right : left;
                if (tailPriority <= priorities[child]) {
                    break;
                }
                indices[cursor] = indices[child];
                priorities[cursor] = priorities[child];
                cursor = child;
            }
            indices[cursor] = tailIndex;
            priorities[cursor] = tailPriority;
        }
        return rootIndex;
    }

    double pollPriority() {
        return polledPriority;
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity <= indices.length) {
            return;
        }
        int newCapacity = Math.max(minCapacity, indices.length << 1);
        indices = Arrays.copyOf(indices, newCapacity);
        priorities = Arrays.copyOf(priorities, newCapacity);
    }
}

final class KrakkDeltaBucketQueue {
    private final double bucketWidth;
    private final Int2ObjectOpenHashMap<IntArrayList> buckets;
    private final PriorityQueue<Integer> bucketOrder;
    private final IntOpenHashSet queuedBuckets;
    private final BitSet pendingEntries;

    KrakkDeltaBucketQueue(double bucketWidth, int indexCapacity) {
        this.bucketWidth = Math.max(1.0E-9D, bucketWidth);
        this.buckets = new Int2ObjectOpenHashMap<>();
        this.bucketOrder = new PriorityQueue<>();
        this.queuedBuckets = new IntOpenHashSet();
        this.pendingEntries = new BitSet(Math.max(1, indexCapacity));
    }

    boolean isEmpty() {
        return buckets.isEmpty();
    }

    int bucketCount() {
        return buckets.size();
    }

    void add(int index, double arrival) {
        if (index < 0) {
            return;
        }
        if (pendingEntries.get(index)) {
            return;
        }
        int bucketIndex = KrakkExplosionRuntime.krakkDeltaBucketIndex(arrival, bucketWidth);
        IntArrayList entries = buckets.get(bucketIndex);
        if (entries == null) {
            entries = new IntArrayList(16);
            buckets.put(bucketIndex, entries);
        }
        entries.add(index);
        pendingEntries.set(index);
        if (queuedBuckets.add(bucketIndex)) {
            bucketOrder.add(bucketIndex);
        }
    }

    boolean claim(int index) {
        if (index < 0 || !pendingEntries.get(index)) {
            return false;
        }
        pendingEntries.clear(index);
        return true;
    }

    int pollBucketIndex() {
        while (!bucketOrder.isEmpty()) {
            int bucketIndex = bucketOrder.poll();
            queuedBuckets.remove(bucketIndex);
            IntArrayList entries = buckets.get(bucketIndex);
            if (entries != null && !entries.isEmpty()) {
                return bucketIndex;
            }
        }
        return -1;
    }

    IntArrayList takeBucket(int bucketIndex) {
        return buckets.remove(bucketIndex);
    }
}
