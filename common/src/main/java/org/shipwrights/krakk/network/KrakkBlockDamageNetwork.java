package org.shipwrights.krakk.network;

import dev.architectury.networking.NetworkManager;
import dev.architectury.platform.Platform;
import dev.architectury.utils.Env;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import io.netty.buffer.Unpooled;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.network.KrakkNetworkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;

import java.util.List;

public final class KrakkBlockDamageNetwork implements KrakkNetworkApi {
    private static final int CHUNK_RESYNC_REQUEST_COOLDOWN_TICKS = 20;
    private static volatile boolean commonReceiversInitialized = false;

    private final ResourceLocation blockDamageSyncPacket;
    private final ResourceLocation blockDamageSectionPacket;
    private final ResourceLocation blockDamageSectionDeltaPacket;
    private final ResourceLocation blockDamageChunkUnloadPacket;
    private final ResourceLocation blockDamageChunkInitPacket;
    private final ResourceLocation blockDamageChunkResyncRequestPacket;
    private final LongOpenHashSet clientInitializedChunks = new LongOpenHashSet();
    private final Long2LongOpenHashMap clientResyncCooldownByChunk = new Long2LongOpenHashMap();
    private ResourceLocation clientTrackingDimension = null;

    public KrakkBlockDamageNetwork(String namespace) {
        this.blockDamageSyncPacket = new ResourceLocation(namespace, "block_damage_sync");
        this.blockDamageSectionPacket = new ResourceLocation(namespace, "block_damage_section");
        this.blockDamageSectionDeltaPacket = new ResourceLocation(namespace, "block_damage_section_delta");
        this.blockDamageChunkUnloadPacket = new ResourceLocation(namespace, "block_damage_chunk_unload");
        this.blockDamageChunkInitPacket = new ResourceLocation(namespace, "block_damage_chunk_init");
        this.blockDamageChunkResyncRequestPacket = new ResourceLocation(namespace, "block_damage_chunk_resync_request");
        initCommonReceivers();
    }

    private void initCommonReceivers() {
        synchronized (KrakkBlockDamageNetwork.class) {
            if (commonReceiversInitialized) {
                return;
            }
            commonReceiversInitialized = true;
        }

        NetworkManager.registerReceiver(NetworkManager.c2s(), blockDamageChunkResyncRequestPacket, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            int chunkX = buf.readVarInt();
            int chunkZ = buf.readVarInt();

            context.queue(() -> {
                if (!(context.getPlayer() instanceof ServerPlayer player) || player.connection == null) {
                    return;
                }

                ServerLevel level = player.serverLevel();
                if (!level.dimension().location().equals(dimensionId)) {
                    return;
                }

                KrakkDamageRuntime.recordRecoveryResyncRequested(player, dimensionId, chunkX, chunkZ);
                if (!isChunkRelevantToPlayer(level, player, chunkX, chunkZ)) {
                    return;
                }
                if (level.getChunkSource().getChunkNow(chunkX, chunkZ) == null) {
                    return;
                }

                sendChunkUnload(player, dimensionId, chunkX, chunkZ);
                sendChunkInit(player, dimensionId, chunkX, chunkZ);
                KrakkApi.damage().syncChunkToPlayer(player, level, chunkX, chunkZ, false);
                KrakkDamageRuntime.recordRecoveryResyncServed(player, dimensionId, chunkX, chunkZ);
            });
        });
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
                ensureClientTrackingDimension(dimensionId);
                int chunkX = SectionPos.blockToSectionCoord(BlockPos.getX(posLong));
                int chunkZ = SectionPos.blockToSectionCoord(BlockPos.getZ(posLong));
                if (!isChunkInitialized(chunkX, chunkZ)) {
                    requestClientChunkResync(dimensionId, chunkX, chunkZ, context.getPlayer().level().getGameTime());
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
                ensureClientTrackingDimension(dimensionId);
                markChunkInitialized(sectionX, sectionZ);
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
                ensureClientTrackingDimension(dimensionId);
                if (!isChunkInitialized(sectionX, sectionZ)) {
                    requestClientChunkResync(dimensionId, sectionX, sectionZ, context.getPlayer().level().getGameTime());
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
                ensureClientTrackingDimension(dimensionId);
                clearChunkInitialized(chunkX, chunkZ);
                KrakkApi.clientOverlay().clearChunk(dimensionId, chunkX, chunkZ);
            });
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), blockDamageChunkInitPacket, (buf, context) -> {
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
                ensureClientTrackingDimension(dimensionId);
                markChunkInitialized(chunkX, chunkZ);
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

        SharedPayload payload = serializeSectionSnapshotPayload(dimensionId, sectionX, sectionY, sectionZ, states);
        sendSharedPayloadBatch(players, payload);
    }

    @Override
    public void sendSectionDeltaBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                                      int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        if (players.isEmpty()) {
            return;
        }

        SharedPayload payload = serializeSectionDeltaPayload(dimensionId, sectionX, sectionY, sectionZ, states);
        sendSharedPayloadBatch(players, payload);
    }

    @Override
    public SharedPayload serializeSectionSnapshotPayload(ResourceLocation dimensionId,
                                                         int sectionX,
                                                         int sectionY,
                                                         int sectionZ,
                                                         Short2ByteOpenHashMap states) {
        FriendlyByteBuf serialized = serializeSectionSnapshot(dimensionId, sectionX, sectionY, sectionZ, states);
        return new SharedPayload(this.blockDamageSectionPacket, copyPayload(serialized));
    }

    @Override
    public SharedPayload serializeSectionDeltaPayload(ResourceLocation dimensionId,
                                                      int sectionX,
                                                      int sectionY,
                                                      int sectionZ,
                                                      Short2ByteOpenHashMap states) {
        FriendlyByteBuf serialized = serializeSectionDelta(dimensionId, sectionX, sectionY, sectionZ, states);
        return new SharedPayload(this.blockDamageSectionDeltaPacket, copyPayload(serialized));
    }

    @Override
    public void sendSharedPayloadBatch(List<ServerPlayer> players, SharedPayload payload) {
        if (players.isEmpty() || payload == null || payload.payload() == null) {
            return;
        }
        sendSharedPayloadBytes(players, payload.packetId(), payload.payload());
    }

    @Override
    public void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeVarInt(chunkX);
        buf.writeVarInt(chunkZ);
        NetworkManager.sendToPlayer(player, blockDamageChunkUnloadPacket, buf);
    }

    @Override
    public void sendChunkInit(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeVarInt(chunkX);
        buf.writeVarInt(chunkZ);
        NetworkManager.sendToPlayer(player, blockDamageChunkInitPacket, buf);
    }

    private static boolean isChunkRelevantToPlayer(ServerLevel level, ServerPlayer player, int chunkX, int chunkZ) {
        if (player.level() != level) {
            return false;
        }
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;
        ChunkPos playerChunk = player.chunkPosition();
        return Math.abs(playerChunk.x - chunkX) <= viewDistance
                && Math.abs(playerChunk.z - chunkZ) <= viewDistance;
    }

    private void ensureClientTrackingDimension(ResourceLocation dimensionId) {
        if (dimensionId.equals(this.clientTrackingDimension)) {
            return;
        }
        this.clientTrackingDimension = dimensionId;
        this.clientInitializedChunks.clear();
        this.clientResyncCooldownByChunk.clear();
    }

    private boolean isChunkInitialized(int chunkX, int chunkZ) {
        return this.clientInitializedChunks.contains(ChunkPos.asLong(chunkX, chunkZ));
    }

    private void markChunkInitialized(int chunkX, int chunkZ) {
        this.clientInitializedChunks.add(ChunkPos.asLong(chunkX, chunkZ));
    }

    private void clearChunkInitialized(int chunkX, int chunkZ) {
        long chunkKey = ChunkPos.asLong(chunkX, chunkZ);
        this.clientInitializedChunks.remove(chunkKey);
        this.clientResyncCooldownByChunk.remove(chunkKey);
    }

    private void requestClientChunkResync(ResourceLocation dimensionId, int chunkX, int chunkZ, long gameTime) {
        long chunkKey = ChunkPos.asLong(chunkX, chunkZ);
        long allowedAt = this.clientResyncCooldownByChunk.get(chunkKey);
        if (allowedAt > gameTime) {
            return;
        }
        this.clientResyncCooldownByChunk.put(chunkKey, gameTime + CHUNK_RESYNC_REQUEST_COOLDOWN_TICKS);

        FriendlyByteBuf request = new FriendlyByteBuf(Unpooled.buffer());
        request.writeResourceLocation(dimensionId);
        request.writeVarInt(chunkX);
        request.writeVarInt(chunkZ);
        NetworkManager.sendToServer(blockDamageChunkResyncRequestPacket, request);
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
        sendSharedPayloadBytes(players, packetId, copyPayload(serialized));
    }

    private static byte[] copyPayload(FriendlyByteBuf serialized) {
        int startIndex = serialized.readerIndex();
        int byteLength = serialized.readableBytes();
        byte[] payload = new byte[byteLength];
        serialized.getBytes(startIndex, payload);
        return payload;
    }

    private static void sendSharedPayloadBytes(List<ServerPlayer> players, ResourceLocation packetId, byte[] payload) {
        for (ServerPlayer player : players) {
            FriendlyByteBuf playerBuf = new FriendlyByteBuf(Unpooled.wrappedBuffer(payload));
            NetworkManager.sendToPlayer(player, packetId, playerBuf);
        }
    }
}
