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

import java.util.List;

public final class KrakkBlockDamageNetwork implements KrakkNetworkApi {
    private final ResourceLocation blockDamageSyncPacket;
    private final ResourceLocation blockDamageSectionPacket;
    private final ResourceLocation blockDamageSectionDeltaPacket;
    private final ResourceLocation blockDamageChunkUnloadPacket;

    public KrakkBlockDamageNetwork(String namespace) {
        this.blockDamageSyncPacket = new ResourceLocation(namespace, "block_damage_sync");
        this.blockDamageSectionPacket = new ResourceLocation(namespace, "block_damage_section");
        this.blockDamageSectionDeltaPacket = new ResourceLocation(namespace, "block_damage_section_delta");
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
            Short2ByteOpenHashMap sectionStates = parseSectionStates(buf, size, false);

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

        NetworkManager.registerReceiver(NetworkManager.s2c(), blockDamageSectionDeltaPacket, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            int sectionX = buf.readVarInt();
            int sectionY = buf.readVarInt();
            int sectionZ = buf.readVarInt();
            int size = buf.readVarInt();
            Short2ByteOpenHashMap sectionStates = parseSectionStates(buf, size, true);

            context.queue(() -> {
                if (context.getPlayer() == null || context.getPlayer().level() == null) {
                    return;
                }
                if (!context.getPlayer().level().dimension().location().equals(dimensionId)) {
                    return;
                }
                KrakkApi.clientOverlay().applySectionDelta(dimensionId, sectionX, sectionY, sectionZ, sectionStates);
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
        java.util.ArrayList<ServerPlayer> recipients = new java.util.ArrayList<>();

        for (ServerPlayer player : level.players()) {
            if (player.level() != level) {
                continue;
            }

            ChunkPos playerChunk = player.chunkPosition();
            if (Math.abs(playerChunk.x - targetChunk.x) > viewDistance || Math.abs(playerChunk.z - targetChunk.z) > viewDistance) {
                continue;
            }
            recipients.add(player);
        }
        sendDamageSyncBatch(recipients, dimensionId, pos.asLong(), clampedState);
    }

    @Override
    public void sendDamageSyncBatch(List<ServerPlayer> players, ResourceLocation dimensionId, long posLong, int damageState) {
        if (players.isEmpty()) {
            return;
        }
        FriendlyByteBuf serialized = serializeDamageSync(dimensionId, posLong, clampDamageState(damageState));
        sendSharedPayload(players, this.blockDamageSyncPacket, serialized);
    }

    @Override
    public void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId,
                                    int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        FriendlyByteBuf buf = serializeSectionSnapshot(dimensionId, sectionX, sectionY, sectionZ, states);
        NetworkManager.sendToPlayer(player, blockDamageSectionPacket, buf);
    }

    @Override
    public void sendSectionSnapshotBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                                         int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        if (players.isEmpty()) {
            return;
        }

        FriendlyByteBuf serialized = serializeSectionSnapshot(dimensionId, sectionX, sectionY, sectionZ, states);
        sendSharedPayload(players, this.blockDamageSectionPacket, serialized);
    }

    @Override
    public void sendSectionDeltaBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                                      int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        if (players.isEmpty()) {
            return;
        }

        FriendlyByteBuf serialized = serializeSectionDelta(dimensionId, sectionX, sectionY, sectionZ, states);
        sendSharedPayload(players, this.blockDamageSectionDeltaPacket, serialized);
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

    private static FriendlyByteBuf serializeDamageSync(ResourceLocation dimensionId, long posLong, int clampedState) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeLong(posLong);
        buf.writeByte(clampedState);
        return buf;
    }

    private static FriendlyByteBuf serializeSectionSnapshot(ResourceLocation dimensionId,
                                                            int sectionX,
                                                            int sectionY,
                                                            int sectionZ,
                                                            Short2ByteOpenHashMap states) {
        return serializeSection(dimensionId, sectionX, sectionY, sectionZ, states, false);
    }

    private static FriendlyByteBuf serializeSectionDelta(ResourceLocation dimensionId,
                                                         int sectionX,
                                                         int sectionY,
                                                         int sectionZ,
                                                         Short2ByteOpenHashMap states) {
        return serializeSection(dimensionId, sectionX, sectionY, sectionZ, states, true);
    }

    private static FriendlyByteBuf serializeSection(ResourceLocation dimensionId,
                                                    int sectionX,
                                                    int sectionY,
                                                    int sectionZ,
                                                    Short2ByteOpenHashMap states,
                                                    boolean allowZeroStates) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeVarInt(sectionX);
        buf.writeVarInt(sectionY);
        buf.writeVarInt(sectionZ);
        int entryCount = 0;
        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            int clampedState = clampDamageState(entry.getByteValue());
            if (allowZeroStates || clampedState > 0) {
                entryCount++;
            }
        }
        buf.writeVarInt(entryCount);

        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            int clampedState = clampDamageState(entry.getByteValue());
            if (!allowZeroStates && clampedState <= 0) {
                continue;
            }
            buf.writeShort(entry.getShortKey() & 0x0FFF);
            buf.writeByte(clampedState);
        }
        return buf;
    }

    private static Short2ByteOpenHashMap parseSectionStates(FriendlyByteBuf buf, int size, boolean allowZeroStates) {
        Short2ByteOpenHashMap sectionStates = new Short2ByteOpenHashMap(Math.max(0, size));
        for (int i = 0; i < size; i++) {
            short localIndex = buf.readShort();
            int clampedState = clampDamageState(buf.readByte());
            if (!allowZeroStates && clampedState <= 0) {
                continue;
            }
            sectionStates.put((short) (localIndex & 0x0FFF), (byte) clampedState);
        }
        return sectionStates;
    }

    private static void sendSharedPayload(List<ServerPlayer> players, ResourceLocation packetId, FriendlyByteBuf serialized) {
        int startIndex = serialized.readerIndex();
        int byteLength = serialized.readableBytes();
        byte[] payload = new byte[byteLength];
        serialized.getBytes(startIndex, payload);

        for (ServerPlayer player : players) {
            FriendlyByteBuf playerBuf = new FriendlyByteBuf(Unpooled.wrappedBuffer(payload));
            NetworkManager.sendToPlayer(player, packetId, playerBuf);
        }
    }
}
