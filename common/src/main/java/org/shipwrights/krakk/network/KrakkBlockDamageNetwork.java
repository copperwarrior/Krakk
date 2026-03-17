package org.shipwrights.krakk.network;

import com.mojang.logging.LogUtils;
import dev.architectury.networking.NetworkManager;
import dev.architectury.platform.Platform;
import dev.architectury.utils.Env;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import org.shipwrights.krakk.api.network.KrakkNetworkApi;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.shipwrights.krakk.state.chunk.KrakkChunkSectionDamageBridge;
import org.slf4j.Logger;

import java.util.List;

/**
 * Dedicated Krakk damage sync packets sent from chunk broadcast hooks.
 */
public final class KrakkBlockDamageNetwork implements KrakkNetworkApi {
    private static final Logger LOGGER = LogUtils.getLogger();

    private final ResourceLocation damageSyncPacketId;
    private final ResourceLocation sectionSnapshotPacketId;
    private final ResourceLocation sectionDeltaPacketId;
    private final ResourceLocation chunkUnloadPacketId;
    private final ResourceLocation chunkInitPacketId;
    private boolean clientReceiversInitialized = false;

    public KrakkBlockDamageNetwork(String namespace) {
        this.damageSyncPacketId = new ResourceLocation(namespace, "damage_sync");
        this.sectionSnapshotPacketId = new ResourceLocation(namespace, "damage_section_snapshot");
        this.sectionDeltaPacketId = new ResourceLocation(namespace, "damage_section_delta");
        this.chunkUnloadPacketId = new ResourceLocation(namespace, "damage_chunk_unload");
        this.chunkInitPacketId = new ResourceLocation(namespace, "damage_chunk_init");
    }

    @Override
    public void initClientReceivers() {
        if (Platform.getEnvironment() != Env.CLIENT) {
            return;
        }
        if (this.clientReceiversInitialized) {
            return;
        }
        this.clientReceiversInitialized = true;

        NetworkManager.registerReceiver(NetworkManager.s2c(), this.damageSyncPacketId, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            long posLong = buf.readLong();
            int damageState = clampDamageState(buf.readByte());
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk net recv damage_sync: dim={} pos=({}, {}, {}) state={} playerPresent={}",
                        dimensionId,
                        BlockPos.getX(posLong),
                        BlockPos.getY(posLong),
                        BlockPos.getZ(posLong),
                        damageState,
                        context.getPlayer() != null
                );
            }
            context.queue(() -> {
                if (context.getPlayer() == null) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug("Krakk net recv damage_sync dropped: no client player context");
                    }
                    return;
                }
                Level level = context.getPlayer().level();
                if (level == null) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug("Krakk net recv damage_sync dropped: player level is null");
                    }
                    return;
                }
                if (!dimensionId.equals(level.dimension().location())) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug(
                                "Krakk net recv damage_sync dropped: dimension mismatch packetDim={} clientDim={}",
                                dimensionId,
                                level.dimension().location()
                        );
                    }
                    return;
                }
                KrakkChunkSectionDamageBridge.applyBlockDamage(level, posLong, damageState);
                KrakkApi.clientOverlay().applyDamage(dimensionId, posLong, damageState);
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk net recv damage_sync applied: dim={} pos=({}, {}, {}) state={}",
                            dimensionId,
                            BlockPos.getX(posLong),
                            BlockPos.getY(posLong),
                            BlockPos.getZ(posLong),
                            damageState
                    );
                }
            });
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), this.sectionSnapshotPacketId, (buf, context) -> {
            SectionPayload payload = readSectionPayload(buf);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk net recv damage_section_snapshot: dim={} section=({}, {}, {}) entries={} playerPresent={}",
                        payload.dimensionId(),
                        payload.sectionX(),
                        payload.sectionY(),
                        payload.sectionZ(),
                        payload.states().size(),
                        context.getPlayer() != null
                );
            }
            context.queue(() -> {
                if (context.getPlayer() == null) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug("Krakk net recv damage_section_snapshot dropped: no client player context");
                    }
                    return;
                }
                Level level = context.getPlayer().level();
                if (level == null) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug("Krakk net recv damage_section_snapshot dropped: player level is null");
                    }
                    return;
                }
                if (!payload.dimensionId().equals(level.dimension().location())) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug(
                                "Krakk net recv damage_section_snapshot dropped: dimension mismatch packetDim={} clientDim={}",
                                payload.dimensionId(),
                                level.dimension().location()
                        );
                    }
                    return;
                }
                KrakkChunkSectionDamageBridge.applySection(
                        level,
                        payload.sectionX(),
                        payload.sectionY(),
                        payload.sectionZ(),
                        payload.states(),
                        true
                );
                KrakkApi.clientOverlay().applySection(
                        payload.dimensionId(),
                        payload.sectionX(),
                        payload.sectionY(),
                        payload.sectionZ(),
                        payload.states()
                );
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk net recv damage_section_snapshot applied: dim={} section=({}, {}, {}) entries={}",
                            payload.dimensionId(),
                            payload.sectionX(),
                            payload.sectionY(),
                            payload.sectionZ(),
                            payload.states().size()
                    );
                }
            });
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), this.sectionDeltaPacketId, (buf, context) -> {
            SectionPayload payload = readSectionPayload(buf);
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk net recv damage_section_delta: dim={} section=({}, {}, {}) entries={} playerPresent={}",
                        payload.dimensionId(),
                        payload.sectionX(),
                        payload.sectionY(),
                        payload.sectionZ(),
                        payload.states().size(),
                        context.getPlayer() != null
                );
            }
            context.queue(() -> {
                if (context.getPlayer() == null) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug("Krakk net recv damage_section_delta dropped: no client player context");
                    }
                    return;
                }
                Level level = context.getPlayer().level();
                if (level == null) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug("Krakk net recv damage_section_delta dropped: player level is null");
                    }
                    return;
                }
                if (!payload.dimensionId().equals(level.dimension().location())) {
                    if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                        LOGGER.debug(
                                "Krakk net recv damage_section_delta dropped: dimension mismatch packetDim={} clientDim={}",
                                payload.dimensionId(),
                                level.dimension().location()
                        );
                    }
                    return;
                }
                KrakkChunkSectionDamageBridge.applySection(
                        level,
                        payload.sectionX(),
                        payload.sectionY(),
                        payload.sectionZ(),
                        payload.states(),
                        false
                );
                KrakkApi.clientOverlay().applySectionDelta(
                        payload.dimensionId(),
                        payload.sectionX(),
                        payload.sectionY(),
                        payload.sectionZ(),
                        payload.states()
                );
                if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                    LOGGER.debug(
                            "Krakk net recv damage_section_delta applied: dim={} section=({}, {}, {}) entries={}",
                            payload.dimensionId(),
                            payload.sectionX(),
                            payload.sectionY(),
                            payload.sectionZ(),
                            payload.states().size()
                    );
                }
            });
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), this.chunkUnloadPacketId, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            int chunkX = buf.readInt();
            int chunkZ = buf.readInt();
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk net recv damage_chunk_unload: dim={} chunk=({}, {})",
                        dimensionId,
                        chunkX,
                        chunkZ
                );
            }
            context.queue(() -> KrakkApi.clientOverlay().clearChunk(dimensionId, chunkX, chunkZ));
        });

        NetworkManager.registerReceiver(NetworkManager.s2c(), this.chunkInitPacketId, (buf, context) -> {
            ResourceLocation dimensionId = buf.readResourceLocation();
            int chunkX = buf.readInt();
            int chunkZ = buf.readInt();
            if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
                LOGGER.debug(
                        "Krakk net recv damage_chunk_init: dim={} chunk=({}, {})",
                        dimensionId,
                        chunkX,
                        chunkZ
                );
            }
        });
    }

    @Override
    public void sendDamageSync(ServerLevel level, BlockPos pos, int damageState) {
        int targetChunkX = SectionPos.blockToSectionCoord(pos.getX());
        int targetChunkZ = SectionPos.blockToSectionCoord(pos.getZ());
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;

        ResourceLocation dimensionId = level.dimension().location();
        int clampedState = clampDamageState(damageState);
        int sent = 0;
        for (ServerPlayer player : level.players()) {
            if (player.level() != level) {
                continue;
            }
            ChunkPos playerChunk = player.chunkPosition();
            if (Math.abs(playerChunk.x - targetChunkX) > viewDistance || Math.abs(playerChunk.z - targetChunkZ) > viewDistance) {
                continue;
            }

            FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
            buf.writeResourceLocation(dimensionId);
            buf.writeLong(pos.asLong());
            buf.writeByte(clampedState);
            NetworkManager.sendToPlayer(player, this.damageSyncPacketId, buf);
            sent++;
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_sync: dim={} pos=({}, {}, {}) state={} recipients={} viewDistance={}",
                    dimensionId,
                    pos.getX(),
                    pos.getY(),
                    pos.getZ(),
                    clampedState,
                    sent,
                    viewDistance
            );
        }
    }

    @Override
    public void sendDamageSyncBatch(List<ServerPlayer> players, ResourceLocation dimensionId, long posLong, int damageState) {
        if (players.isEmpty()) {
            return;
        }
        int clampedState = clampDamageState(damageState);
        for (ServerPlayer player : players) {
            FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
            buf.writeResourceLocation(dimensionId);
            buf.writeLong(posLong);
            buf.writeByte(clampedState);
            NetworkManager.sendToPlayer(player, this.damageSyncPacketId, buf);
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_sync batch: dim={} pos=({}, {}, {}) state={} recipients={}",
                    dimensionId,
                    BlockPos.getX(posLong),
                    BlockPos.getY(posLong),
                    BlockPos.getZ(posLong),
                    clampedState,
                    players.size()
            );
        }
    }

    @Override
    public void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId,
                                    int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        writeSectionPayload(buf, dimensionId, sectionX, sectionY, sectionZ, states);
        NetworkManager.sendToPlayer(player, this.sectionSnapshotPacketId, buf);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_section_snapshot: dim={} section=({}, {}, {}) entries={} recipient={}",
                    dimensionId,
                    sectionX,
                    sectionY,
                    sectionZ,
                    states.size(),
                    player.getGameProfile().getName()
            );
        }
    }

    @Override
    public void sendSectionSnapshotBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                                         int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        if (players.isEmpty()) {
            return;
        }
        byte[] payload = encodeSectionPayload(dimensionId, sectionX, sectionY, sectionZ, states);
        for (ServerPlayer player : players) {
            FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.wrappedBuffer(payload));
            NetworkManager.sendToPlayer(player, this.sectionSnapshotPacketId, buf);
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_section_snapshot batch: dim={} section=({}, {}, {}) entries={} recipients={} bytes={}",
                    dimensionId,
                    sectionX,
                    sectionY,
                    sectionZ,
                    states.size(),
                    players.size(),
                    payload.length
            );
        }
    }

    @Override
    public void sendSectionDeltaBatch(List<ServerPlayer> players, ResourceLocation dimensionId,
                                      int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        if (players.isEmpty()) {
            return;
        }
        byte[] payload = encodeSectionPayload(dimensionId, sectionX, sectionY, sectionZ, states);
        for (ServerPlayer player : players) {
            FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.wrappedBuffer(payload));
            NetworkManager.sendToPlayer(player, this.sectionDeltaPacketId, buf);
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_section_delta batch: dim={} section=({}, {}, {}) entries={} recipients={} bytes={}",
                    dimensionId,
                    sectionX,
                    sectionY,
                    sectionZ,
                    states.size(),
                    players.size(),
                    payload.length
            );
        }
    }

    @Override
    public SharedPayload serializeSectionSnapshotPayload(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        return new SharedPayload(this.sectionSnapshotPacketId, encodeSectionPayload(dimensionId, sectionX, sectionY, sectionZ, states));
    }

    @Override
    public SharedPayload serializeSectionDeltaPayload(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        return new SharedPayload(this.sectionDeltaPacketId, encodeSectionPayload(dimensionId, sectionX, sectionY, sectionZ, states));
    }

    @Override
    public void sendSharedPayloadBatch(List<ServerPlayer> players, SharedPayload payload) {
        if (players.isEmpty() || payload == null || payload.payload() == null) {
            return;
        }
        for (ServerPlayer player : players) {
            FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.wrappedBuffer(payload.payload()));
            NetworkManager.sendToPlayer(player, payload.packetId(), buf);
        }
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send shared payload batch: packet={} recipients={} bytes={}",
                    payload.packetId(),
                    players.size(),
                    payload.payload().length
            );
        }
    }

    @Override
    public void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeInt(chunkX);
        buf.writeInt(chunkZ);
        NetworkManager.sendToPlayer(player, this.chunkUnloadPacketId, buf);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_chunk_unload: dim={} chunk=({}, {}) recipient={}",
                    dimensionId,
                    chunkX,
                    chunkZ,
                    player.getGameProfile().getName()
            );
        }
    }

    @Override
    public void sendChunkInit(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        buf.writeResourceLocation(dimensionId);
        buf.writeInt(chunkX);
        buf.writeInt(chunkZ);
        NetworkManager.sendToPlayer(player, this.chunkInitPacketId, buf);
        if (KrakkDamageRuntime.isSyncDebugLoggingEnabled()) {
            LOGGER.debug(
                    "Krakk net send damage_chunk_init: dim={} chunk=({}, {}) recipient={}",
                    dimensionId,
                    chunkX,
                    chunkZ,
                    player.getGameProfile().getName()
            );
        }
    }

    private static int clampDamageState(int damageState) {
        return Math.max(0, Math.min(15, damageState));
    }

    private static byte[] encodeSectionPayload(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states) {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        writeSectionPayload(buf, dimensionId, sectionX, sectionY, sectionZ, states);
        byte[] payload = new byte[buf.readableBytes()];
        buf.getBytes(0, payload);
        return payload;
    }

    private static void writeSectionPayload(FriendlyByteBuf buf,
                                            ResourceLocation dimensionId,
                                            int sectionX,
                                            int sectionY,
                                            int sectionZ,
                                            Short2ByteOpenHashMap states) {
        buf.writeResourceLocation(dimensionId);
        buf.writeInt(sectionX);
        buf.writeInt(sectionY);
        buf.writeInt(sectionZ);
        buf.writeVarInt(states.size());
        for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
            buf.writeShort(entry.getShortKey() & 0x0FFF);
            buf.writeByte(clampDamageState(entry.getByteValue()));
        }
    }

    private static SectionPayload readSectionPayload(FriendlyByteBuf buf) {
        ResourceLocation dimensionId = buf.readResourceLocation();
        int sectionX = buf.readInt();
        int sectionY = buf.readInt();
        int sectionZ = buf.readInt();
        int count = Math.max(0, buf.readVarInt());
        Short2ByteOpenHashMap states = new Short2ByteOpenHashMap(count);
        for (int i = 0; i < count; i++) {
            short localIndex = (short) (buf.readShort() & 0x0FFF);
            byte damageState = (byte) clampDamageState(buf.readByte());
            states.put(localIndex, damageState);
        }
        return new SectionPayload(dimensionId, sectionX, sectionY, sectionZ, states);
    }

    private record SectionPayload(ResourceLocation dimensionId,
                                  int sectionX,
                                  int sectionY,
                                  int sectionZ,
                                  Short2ByteOpenHashMap states) {
    }
}
