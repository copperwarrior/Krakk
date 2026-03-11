package org.shipwrights.krakk.state.chunk;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2LongOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.nbt.CompoundTag;

public final class KrakkBlockDamageChunkStorage {
    public static final String ROOT_TAG = "KrakkDamage";
    public static final String LEGACY_ROOT_TAG = "CannonicalDamage";

    private static final String SECTION_YS_TAG = "SectionYs";
    private static final String LOCAL_INDICES_TAG = "LocalIndices";
    private static final String STATES_TAG = "States";
    private static final String LAST_UPDATE_TICKS_TAG = "LastUpdateTicks";

    private static final int MAX_DAMAGE_STATE = 15;
    private static final int NO_DAMAGE_STATE = -1;
    private static final long NO_LAST_UPDATE_TICK = -1L;

    private final Int2ObjectOpenHashMap<SectionDamageState> sectionsByY = new Int2ObjectOpenHashMap<>();

    public void load(CompoundTag tag) {
        this.sectionsByY.clear();

        int[] sectionYs = tag.getIntArray(SECTION_YS_TAG);
        int[] localIndices = tag.getIntArray(LOCAL_INDICES_TAG);
        byte[] states = tag.getByteArray(STATES_TAG);
        long[] lastUpdateTicks = tag.getLongArray(LAST_UPDATE_TICKS_TAG);

        int len = Math.min(sectionYs.length, Math.min(localIndices.length, states.length));
        for (int i = 0; i < len; i++) {
            int state = clamp(states[i], 0, MAX_DAMAGE_STATE);
            if (state <= 0) {
                continue;
            }

            int sectionY = sectionYs[i];
            short localIndex = (short) (localIndices[i] & 0x0FFF);
            long updateTick = i < lastUpdateTicks.length ? lastUpdateTicks[i] : NO_LAST_UPDATE_TICK;
            putInternal(sectionY, localIndex, (byte) state, updateTick);
        }
    }

    public CompoundTag save() {
        CompoundTag tag = new CompoundTag();
        int totalEntries = 0;
        for (SectionDamageState section : this.sectionsByY.values()) {
            totalEntries += section.damageStates.size();
        }

        if (totalEntries <= 0) {
            return tag;
        }

        int[] sectionYs = new int[totalEntries];
        int[] localIndices = new int[totalEntries];
        byte[] states = new byte[totalEntries];
        long[] lastUpdateTicks = new long[totalEntries];

        int i = 0;
        for (Int2ObjectMap.Entry<SectionDamageState> sectionEntry : this.sectionsByY.int2ObjectEntrySet()) {
            int sectionY = sectionEntry.getIntKey();
            SectionDamageState section = sectionEntry.getValue();
            for (Short2ByteMap.Entry stateEntry : section.damageStates.short2ByteEntrySet()) {
                short localIndex = stateEntry.getShortKey();
                sectionYs[i] = sectionY;
                localIndices[i] = localIndex & 0x0FFF;
                states[i] = stateEntry.getByteValue();
                lastUpdateTicks[i] = section.lastUpdateTicks.get(localIndex);
                i++;
            }
        }

        tag.putIntArray(SECTION_YS_TAG, sectionYs);
        tag.putIntArray(LOCAL_INDICES_TAG, localIndices);
        tag.putByteArray(STATES_TAG, states);
        tag.putLongArray(LAST_UPDATE_TICKS_TAG, lastUpdateTicks);
        return tag;
    }

    public void copyFrom(KrakkBlockDamageChunkStorage other) {
        if (other == this) {
            return;
        }
        this.sectionsByY.clear();
        for (Int2ObjectMap.Entry<SectionDamageState> entry : other.sectionsByY.int2ObjectEntrySet()) {
            this.sectionsByY.put(entry.getIntKey(), entry.getValue().copy());
        }
    }

    public boolean isEmpty() {
        return this.sectionsByY.isEmpty();
    }

    public int getDamageState(long posLong) {
        SectionDamageState section = this.sectionsByY.get(sectionYFromPosLong(posLong));
        if (section == null) {
            return 0;
        }

        int state = section.damageStates.get(localIndexFromPosLong(posLong));
        return state == NO_DAMAGE_STATE ? 0 : state;
    }

    public long getLastUpdateTick(long posLong) {
        SectionDamageState section = this.sectionsByY.get(sectionYFromPosLong(posLong));
        if (section == null) {
            return NO_LAST_UPDATE_TICK;
        }
        return section.lastUpdateTicks.get(localIndexFromPosLong(posLong));
    }

    public boolean setLastUpdateTick(long posLong, long lastUpdateTick) {
        SectionDamageState section = this.sectionsByY.get(sectionYFromPosLong(posLong));
        if (section == null) {
            return false;
        }

        short localIndex = localIndexFromPosLong(posLong);
        if (!section.damageStates.containsKey(localIndex)) {
            return false;
        }

        long previous = section.lastUpdateTicks.put(localIndex, lastUpdateTick);
        return previous != lastUpdateTick;
    }

    public boolean setDamageState(long posLong, int state, long lastUpdateTick) {
        int clamped = clamp(state, 0, MAX_DAMAGE_STATE);
        if (clamped <= 0) {
            return removeDamageState(posLong) != NO_DAMAGE_STATE;
        }

        int sectionY = sectionYFromPosLong(posLong);
        short localIndex = localIndexFromPosLong(posLong);
        SectionDamageState section = this.sectionsByY.get(sectionY);
        if (section == null) {
            section = new SectionDamageState();
            this.sectionsByY.put(sectionY, section);
        }

        int previousState = section.damageStates.put(localIndex, (byte) clamped);
        long previousTick = section.lastUpdateTicks.put(localIndex, lastUpdateTick);
        return previousState != clamped || previousTick != lastUpdateTick;
    }

    public int removeDamageState(long posLong) {
        int sectionY = sectionYFromPosLong(posLong);
        SectionDamageState section = this.sectionsByY.get(sectionY);
        if (section == null) {
            return NO_DAMAGE_STATE;
        }

        short localIndex = localIndexFromPosLong(posLong);
        int previous = section.damageStates.remove(localIndex);
        section.lastUpdateTicks.remove(localIndex);

        if (section.damageStates.isEmpty()) {
            this.sectionsByY.remove(sectionY);
        }

        return previous;
    }

    public void forEachSection(SectionSnapshotConsumer consumer) {
        for (Int2ObjectMap.Entry<SectionDamageState> entry : this.sectionsByY.int2ObjectEntrySet()) {
            SectionDamageState section = entry.getValue();
            if (section.damageStates.isEmpty()) {
                continue;
            }
            consumer.accept(entry.getIntKey(), section.snapshotDamageStates());
        }
    }

    public Short2ByteOpenHashMap snapshotSection(int sectionY) {
        SectionDamageState section = this.sectionsByY.get(sectionY);
        if (section == null || section.damageStates.isEmpty()) {
            return new Short2ByteOpenHashMap();
        }
        return section.snapshotDamageStates();
    }

    private void putInternal(int sectionY, short localIndex, byte damageState, long updateTick) {
        SectionDamageState section = this.sectionsByY.get(sectionY);
        if (section == null) {
            section = new SectionDamageState();
            this.sectionsByY.put(sectionY, section);
        }
        section.damageStates.put(localIndex, damageState);
        section.lastUpdateTicks.put(localIndex, updateTick);
    }

    private static int sectionYFromPosLong(long posLong) {
        return BlockPos.getY(posLong) >> 4;
    }

    private static short localIndexFromPosLong(long posLong) {
        int x = BlockPos.getX(posLong);
        int y = BlockPos.getY(posLong);
        int z = BlockPos.getZ(posLong);
        return (short) (((y & 15) << 8) | ((z & 15) << 4) | (x & 15));
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private static final class SectionDamageState {
        private final Short2ByteOpenHashMap damageStates = new Short2ByteOpenHashMap();
        private final Short2LongOpenHashMap lastUpdateTicks = new Short2LongOpenHashMap();

        private SectionDamageState() {
            this.damageStates.defaultReturnValue((byte) NO_DAMAGE_STATE);
            this.lastUpdateTicks.defaultReturnValue(NO_LAST_UPDATE_TICK);
        }

        private SectionDamageState copy() {
            SectionDamageState copy = new SectionDamageState();
            copy.damageStates.putAll(this.damageStates);
            copy.lastUpdateTicks.putAll(this.lastUpdateTicks);
            return copy;
        }

        private Short2ByteOpenHashMap snapshotDamageStates() {
            return new Short2ByteOpenHashMap(this.damageStates);
        }
    }

    @FunctionalInterface
    public interface SectionSnapshotConsumer {
        void accept(int sectionY, Short2ByteOpenHashMap states);
    }
}
