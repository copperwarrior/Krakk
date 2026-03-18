package org.shipwrights.krakk.state.chunk;

import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;

public interface KrakkBlockDamageSectionAccess {
    Short2ByteOpenHashMap krakk$getDamageStates();

    void krakk$clearDamageStates();

    void krakk$setDamageState(short localIndex, byte damageState);

    void krakk$removeDamageState(short localIndex);
}
