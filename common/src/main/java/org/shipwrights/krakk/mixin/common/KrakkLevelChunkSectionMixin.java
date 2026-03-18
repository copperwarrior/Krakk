package org.shipwrights.krakk.mixin.common;

import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.Registry;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.chunk.LevelChunkSection;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageSectionAccess;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

@Mixin(LevelChunkSection.class)
public abstract class KrakkLevelChunkSectionMixin implements KrakkBlockDamageSectionAccess {
    @Unique
    private Short2ByteOpenHashMap krakk$damageStates;

    @Inject(method = "<init>(Lnet/minecraft/core/Registry;)V", at = @At("RETURN"), require = 0)
    private void krakk$initDamageStateStorage(Registry<Biome> biomeRegistry, CallbackInfo ci) {
        this.krakk$ensureDamageStatesStorage();
    }

    @Inject(method = "getSerializedSize", at = @At("RETURN"), cancellable = true, require = 0)
    private void krakk$includeDamageStatePayloadSize(CallbackInfoReturnable<Integer> cir) {
        Short2ByteOpenHashMap states = this.krakk$ensureDamageStatesStorage();
        int extraBytes = Short.BYTES + states.size() * (Short.BYTES + Byte.BYTES);
        cir.setReturnValue(cir.getReturnValue() + extraBytes);
    }

    @Inject(method = "write", at = @At("TAIL"), require = 0)
    private void krakk$writeDamageStates(FriendlyByteBuf buf, CallbackInfo ci) {
        Short2ByteOpenHashMap states = this.krakk$ensureDamageStatesStorage();
        buf.writeShort(states.size());
        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            buf.writeShort(entry.getShortKey() & 0x0FFF);
            buf.writeByte(entry.getByteValue());
        }
    }

    @Inject(method = "read", at = @At("TAIL"), require = 0)
    private void krakk$readDamageStates(FriendlyByteBuf buf, CallbackInfo ci) {
        Short2ByteOpenHashMap states = this.krakk$ensureDamageStatesStorage();
        states.clear();

        int count = Short.toUnsignedInt(buf.readShort());
        for (int i = 0; i < count; i++) {
            short localIndex = (short) (buf.readShort() & 0x0FFF);
            byte damageState = (byte) Math.max(0, Math.min(15, buf.readByte()));
            if (damageState > 0) {
                states.put(localIndex, damageState);
            }
        }
    }

    @Override
    public Short2ByteOpenHashMap krakk$getDamageStates() {
        return this.krakk$ensureDamageStatesStorage();
    }

    @Override
    public void krakk$clearDamageStates() {
        this.krakk$ensureDamageStatesStorage().clear();
    }

    @Override
    public void krakk$setDamageState(short localIndex, byte damageState) {
        byte clamped = (byte) Math.max(0, Math.min(15, damageState));
        if (clamped <= 0) {
            this.krakk$removeDamageState(localIndex);
            return;
        }
        this.krakk$ensureDamageStatesStorage().put((short) (localIndex & 0x0FFF), clamped);
    }

    @Override
    public void krakk$removeDamageState(short localIndex) {
        this.krakk$ensureDamageStatesStorage().remove((short) (localIndex & 0x0FFF));
    }

    @Unique
    private Short2ByteOpenHashMap krakk$ensureDamageStatesStorage() {
        if (this.krakk$damageStates == null) {
            this.krakk$damageStates = new Short2ByteOpenHashMap();
            this.krakk$damageStates.defaultReturnValue((byte) 0);
        }
        return this.krakk$damageStates;
    }
}
