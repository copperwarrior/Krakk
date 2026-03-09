package org.shipwrights.krakk.network;

import dev.architectury.networking.NetworkManager;
import dev.architectury.platform.Platform;
import dev.architectury.utils.Env;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import io.netty.buffer.Unpooled;
import net.minecraft.core.BlockPos;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.network.KrakkNetworkApi;

public final class KrakkBlockDamageNetwork implements KrakkNetworkApi {
    private final ResourceLocation blockDamageSyncPacket;
    private final ResourceLocation blockDamageSectionPacket;
    private final ResourceLocation blockDamageChunkUnloadPacket;

    public KrakkBlockDamageNetwork(String namespace) {
        this.blockDamageSyncPacket = new ResourceLocation(namespace, "block_damage_sync");
        this.blockDamageSectionPacket = new ResourceLocation(namespace, "block_damage_section");
        this.blockDamageChunkUnloadPacket = new ResourceLocation(namespace, "block_damage_chunk_unload");
    }

    @Override
    public void initClientReceivers() {
        if (Platform.getEnvironment() != Env.CLIENT) {
            return;
        }

        NetworkManager.registerReceiver(NetworkManager.s2c(), blockDamageSyncPacket, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            long posLong = buf.readLong();
            int damageState = buf.readByte();

            context.queue(() -> {
                if (context.getPlayer() == null || context.getPlayer().level() == null) {
                    return;
                }
                if (!context.getPlayer().level().dimension().location().equals(dimensionId)) {
                    return;
                }
                KrakkApi.clientOverlay().applyDamage(dimensionId, posLong, damageState);
            });
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), blockDamageSectionPacket, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            int sectionX = buf.readVarInt();
            int sectionY = buf.readVarInt();
            int sectionZ = buf.readVarInt();
            int size = buf.readVarInt();
            Short2ByteOpenHashMap sectionStates = new Short2ByteOpenHashMap(Math.max(0, size));

            for (int i = 0; i < size; i++) {
                short localIndex = buf.readShort();
                int damageState = buf.readByte();
                int clampedState = clampDamageState(damageState);
                if (clampedState > 0) {
                    sectionStates.put((short) (localIndex & 0x0FFF), (byte) clampedState);
                }
            }

            context.queue(() -> {
                if (context.getPlayer() == null || context.getPlayer().level() == null) {
                    return;
                }
                if (!context.getPlayer().level().dimension().location().equals(dimensionId)) {
                    return;
                }
                KrakkApi.clientOverlay().applySection(dimensionId, sectionX, sectionY, sectionZ, sectionStates);
            });
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), blockDamageChunkUnloadPacket, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            int chunkX = buf.readVarInt();
            int chunkZ = buf.readVarInt();

            context.queue(() -> {
                if (context.getPlayer() == null || context.getPlayer().level() == null) {
                    return;
                }
                if (!context.getPlayer().level().dimension().location().equals(dimensionId)) {
                    return;
                }
                KrakkApi.clientOverlay().clearChunk(dimensionId, chunkX, chunkZ);
            });
        });
    }

    @Override
    public void sendDamageSync(ServerLevel level, BlockPos pos, int damageState) {
        int clampedState = clampDamageState(damageState);
        ResourceLocation dimensionId = level.dimension().location();
        ChunkPos targetChunk = new ChunkPos(pos);
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;

        for (ServerPlayer player : level.players()) {
            if (player.level() != level) {
                continue;
            }

            ChunkPos playerChunk = player.chunkPosition();
            if (Math.abs(playerChunk.x - targetChunk.x) > viewDistance || Math.abs(playerChunk.z - targetChunk.z) > viewDistance) {
                continue;
            }

            FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
            buf.writeResourceLocation(dimensionId);
            buf.writeLong(pos.asLong());
            buf.writeByte(clampedState);
            NetworkManager.sendToPlayer(player, blockDamageSyncPacket, buf);
        }
    }

    @Override
    public void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId,
                                    int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        if (states.isEmpty()) {
            return;
        }

        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeVarInt(sectionX);
        buf.writeVarInt(sectionY);
        buf.writeVarInt(sectionZ);
        buf.writeVarInt(states.size());

        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            int clampedState = clampDamageState(entry.getByteValue());
            buf.writeShort(entry.getShortKey() & 0x0FFF);
            buf.writeByte(clampedState);
        }

        NetworkManager.sendToPlayer(player, blockDamageSectionPacket, buf);
    }

    @Override
    public void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeVarInt(chunkX);
        buf.writeVarInt(chunkZ);
        NetworkManager.sendToPlayer(player, blockDamageChunkUnloadPacket, buf);
    }

    private static int clampDamageState(int damageState) {
        return Math.max(0, Math.min(15, damageState));
    }
}
