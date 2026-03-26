package org.shipwrights.krakk.command;

import com.mojang.logging.LogUtils;
import com.mojang.brigadier.arguments.DoubleArgumentType;
import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.mojang.brigadier.arguments.StringArgumentType;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import dev.architectury.event.events.common.CommandRegistrationEvent;
import it.unimi.dsi.fastutil.shorts.Short2ByteMap;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.commands.arguments.EntityArgument;
import net.minecraft.commands.arguments.coordinates.BlockPosArgument;
import net.minecraft.commands.arguments.coordinates.Vec3Argument;
import net.minecraft.core.BlockPos;
import net.minecraft.network.chat.Component;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.phys.Vec3;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.api.explosion.KrakkExplosionProfile;
import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkStorage;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KrakkDebugCommands {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final int DEFAULT_MAX_POWER = 1_000_000;
    private static final int DEFAULT_DECAY_TICKS = 24_000;

    private KrakkDebugCommands() {
    }

    public static void register() {
        CommandRegistrationEvent.EVENT.register((dispatcher, registry, selection) -> dispatcher.register(
                Commands.literal("krakk")
                        .requires(source -> source.hasPermission(2))
                        .then(Commands.literal("setblockdamage")
                                .then(Commands.argument("pos", BlockPosArgument.blockPos())
                                        .then(Commands.argument("value", IntegerArgumentType.integer(0, KrakkApi.damage().getMaxDamageState()))
                                                .executes(context -> setBlockDamage(
                                                        context.getSource(),
                                                        BlockPosArgument.getBlockPos(context, "pos"),
                                                        IntegerArgumentType.getInteger(context, "value"))))))
                        .then(Commands.literal("getblockdamage")
                                .then(Commands.argument("pos", BlockPosArgument.blockPos())
                                        .executes(context -> getBlockDamage(
                                                context.getSource(),
                                                BlockPosArgument.getBlockPos(context, "pos")))))
                        .then(Commands.literal("clearblockdamage")
                                .then(Commands.argument("pos", BlockPosArgument.blockPos())
                                        .executes(context -> clearBlockDamage(
                                                context.getSource(),
                                                BlockPosArgument.getBlockPos(context, "pos")))))
                        .then(Commands.literal("clearareadamage")
                                .then(Commands.argument("from", BlockPosArgument.blockPos())
                                        .then(Commands.argument("to", BlockPosArgument.blockPos())
                                                .executes(context -> clearAreaDamage(
                                                        context.getSource(),
                                                        BlockPosArgument.getBlockPos(context, "from"),
                                                        BlockPosArgument.getBlockPos(context, "to"))))))
                        .then(Commands.literal("explodemag")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("magnitude", StringArgumentType.word())
                                                .executes(context -> explode(
                                                        context.getSource(),
                                                        Vec3Argument.getVec3(context, "pos"),
                                                        StringArgumentType.getString(context, "magnitude"),
                                                        0,
                                                        KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS))
                                                .then(Commands.argument("blastTransmittance", IntegerArgumentType.integer(0, 100))
                                                        .executes(context -> explode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                StringArgumentType.getString(context, "magnitude"),
                                                                IntegerArgumentType.getInteger(context, "blastTransmittance"),
                                                                KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS))
                                                        .then(Commands.argument("heat", DoubleArgumentType.doubleArg())
                                                                .executes(context -> explode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        StringArgumentType.getString(context, "magnitude"),
                                                                        IntegerArgumentType.getInteger(context, "blastTransmittance"),
                                                                        DoubleArgumentType.getDouble(context, "heat"))))))))
                        .then(Commands.literal("exploderad")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(0.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> krakkExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                0,
                                                                KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS))
                                                        .then(Commands.argument("blastTransmittance", IntegerArgumentType.integer(0, 100))
                                                                .executes(context -> krakkExplode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                        IntegerArgumentType.getInteger(context, "blastTransmittance"),
                                                                        KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS))
                                                                .then(Commands.argument("heat", DoubleArgumentType.doubleArg())
                                                                        .executes(context -> krakkExplode(
                                                                                context.getSource(),
                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                IntegerArgumentType.getInteger(context, "blastTransmittance"),
                                                                                DoubleArgumentType.getDouble(context, "heat")))))))))
                        .then(Commands.literal("fillblockdamage")
                                .then(Commands.argument("from", BlockPosArgument.blockPos())
                                        .then(Commands.argument("to", BlockPosArgument.blockPos())
                                                .then(Commands.argument("value", IntegerArgumentType.integer(0, KrakkApi.damage().getMaxDamageState()))
                                                        .executes(context -> fillBlockDamage(
                                                                context.getSource(),
                                                                BlockPosArgument.getBlockPos(context, "from"),
                                                                BlockPosArgument.getBlockPos(context, "to"),
                                                                IntegerArgumentType.getInteger(context, "value")))))))
                        .then(Commands.literal("damage")
                                .then(Commands.argument("pos", BlockPosArgument.blockPos())
                                        .then(Commands.argument("amount", IntegerArgumentType.integer(0))
                                                .executes(context -> damageBlock(
                                                        context.getSource(),
                                                        BlockPosArgument.getBlockPos(context, "pos"),
                                                        IntegerArgumentType.getInteger(context, "amount"),
                                                        KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS))
                                                .then(Commands.argument("heat", DoubleArgumentType.doubleArg())
                                                        .executes(context -> damageBlock(
                                                                context.getSource(),
                                                                BlockPosArgument.getBlockPos(context, "pos"),
                                                                IntegerArgumentType.getInteger(context, "amount"),
                                                                DoubleArgumentType.getDouble(context, "heat")))))))
                        .then(Commands.literal("areadamage")
                                .then(Commands.argument("from", BlockPosArgument.blockPos())
                                        .then(Commands.argument("to", BlockPosArgument.blockPos())
                                                .then(Commands.argument("amount", IntegerArgumentType.integer(1))
                                                        .executes(context -> areaDamage(
                                                                context.getSource(),
                                                                BlockPosArgument.getBlockPos(context, "from"),
                                                                BlockPosArgument.getBlockPos(context, "to"),
                                                                IntegerArgumentType.getInteger(context, "amount")))))))
                        .then(Commands.literal("decaytick")
                                .executes(context -> decayTick(context.getSource(), DEFAULT_DECAY_TICKS, null))
                                .then(Commands.argument("ticks", IntegerArgumentType.integer(1))
                                        .executes(context -> decayTick(
                                                context.getSource(),
                                                IntegerArgumentType.getInteger(context, "ticks"),
                                                null))
                                        .then(Commands.argument("player", EntityArgument.player())
                                                .executes(context -> decayTick(
                                                        context.getSource(),
                                                        IntegerArgumentType.getInteger(context, "ticks"),
                                                        EntityArgument.getPlayer(context, "player")))))
                                .then(Commands.argument("player", EntityArgument.player())
                                        .executes(context -> decayTick(
                                                context.getSource(),
                                                DEFAULT_DECAY_TICKS,
                                                EntityArgument.getPlayer(context, "player")))))
                        .then(Commands.literal("syncchunk")
                                .executes(context -> syncLoadedChunks(context.getSource(), null))
                                .then(Commands.argument("player", EntityArgument.player())
                                        .executes(context -> syncLoadedChunks(
                                                context.getSource(),
                                                EntityArgument.getPlayer(context, "player")))))
                        .then(Commands.literal("stats")
                                .executes(context -> stats(context.getSource(), null))
                                .then(Commands.argument("player", EntityArgument.player())
                                        .executes(context -> stats(
                                                context.getSource(),
                                                EntityArgument.getPlayer(context, "player")))))
                        .then(Commands.literal("validatedamage")
                                .executes(context -> validateDamageSections(context.getSource(), null, 8))
                                .then(Commands.argument("top", IntegerArgumentType.integer(1, 64))
                                        .executes(context -> validateDamageSections(
                                                context.getSource(),
                                                null,
                                                IntegerArgumentType.getInteger(context, "top"))))
                                .then(Commands.argument("player", EntityArgument.player())
                                        .executes(context -> validateDamageSections(
                                                context.getSource(),
                                                EntityArgument.getPlayer(context, "player"),
                                                8))
                                        .then(Commands.argument("top", IntegerArgumentType.integer(1, 64))
                                                .executes(context -> validateDamageSections(
                                                        context.getSource(),
                                                        EntityArgument.getPlayer(context, "player"),
                                                        IntegerArgumentType.getInteger(context, "top"))))))
                        .then(Commands.literal("overlay")
                                .then(Commands.literal("refresh")
                                        .executes(context -> overlayRefresh(context.getSource(), null))
                                        .then(Commands.argument("player", EntityArgument.player())
                                                .executes(context -> overlayRefresh(
                                                        context.getSource(),
                                                        EntityArgument.getPlayer(context, "player")))))
                                .then(Commands.literal("clear")
                                        .executes(context -> overlayClear(context.getSource(), null))
                                        .then(Commands.argument("player", EntityArgument.player())
                                                .executes(context -> overlayClear(
                                                        context.getSource(),
                                                        EntityArgument.getPlayer(context, "player"))))))));
    }

    private static int setBlockDamage(CommandSourceStack source, BlockPos pos, int value) {
        ServerLevel level = source.getLevel();
        boolean changed = KrakkApi.damage().setDamageStateForDebug(level, pos, value);
        if (!changed) {
            source.sendFailure(Component.literal("Unable to set damage state at target block."));
            return 0;
        }

        source.sendSuccess(() -> Component.literal(String.format(
                "Set block damage at %d %d %d to %d",
                pos.getX(), pos.getY(), pos.getZ(), value
        )), true);
        return 1;
    }

    private static int getBlockDamage(CommandSourceStack source, BlockPos pos) {
        ServerLevel level = source.getLevel();
        int state = KrakkApi.damage().getDamageState(level, pos);
        float baseline = KrakkApi.damage().getMiningBaseline(level, pos);
        int max = KrakkApi.damage().getMaxDamageState();
        source.sendSuccess(() -> Component.literal(String.format(
                "Block %d %d %d damage=%d/%d baseline=%.3f",
                pos.getX(), pos.getY(), pos.getZ(), state, max, baseline
        )), false);
        return 1;
    }

    private static int clearBlockDamage(CommandSourceStack source, BlockPos pos) {
        ServerLevel level = source.getLevel();
        int previous = KrakkApi.damage().getDamageState(level, pos);
        KrakkApi.damage().clearDamage(level, pos);
        if (previous <= 0) {
            source.sendSuccess(() -> Component.literal("Block had no tracked damage state."), false);
            return 0;
        }

        source.sendSuccess(() -> Component.literal(String.format(
                "Cleared block damage at %d %d %d (previous=%d)",
                pos.getX(), pos.getY(), pos.getZ(), previous
        )), true);
        return 1;
    }

    private static int clearAreaDamage(CommandSourceStack source, BlockPos from, BlockPos to) {
        ServerLevel level = source.getLevel();
        Bounds bounds = Bounds.of(from, to);
        int cleared = 0;
        int untouched = 0;
        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();

        for (int x = bounds.minX; x <= bounds.maxX; x++) {
            for (int y = bounds.minY; y <= bounds.maxY; y++) {
                for (int z = bounds.minZ; z <= bounds.maxZ; z++) {
                    cursor.set(x, y, z);
                    int previous = KrakkApi.damage().getDamageState(level, cursor);
                    KrakkApi.damage().clearDamage(level, cursor);
                    if (previous > 0) {
                        cleared++;
                    } else {
                        untouched++;
                    }
                }
            }
        }

        int total = bounds.volume();
        int finalCleared = cleared;
        int finalUntouched = untouched;
        source.sendSuccess(() -> Component.literal(String.format(
                "Cleared area damage (%d blocks): cleared=%d untouched=%d",
                total, finalCleared, finalUntouched
        )), true);
        return cleared;
    }

    private static DetonationMagnitude parseDetonationMagnitude(String rawInput) {
        if (rawInput == null) {
            throw new IllegalArgumentException("Magnitude is required.");
        }
        String trimmed = rawInput.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Magnitude is required.");
        }
        String normalized = trimmed.toLowerCase();

        try {
            if (normalized.endsWith("mt")) {
                double value = Double.parseDouble(normalized.substring(0, normalized.length() - 2));
                if (!Double.isFinite(value) || value <= 0.0D) {
                    throw new IllegalArgumentException("Magnitude must be > 0.");
                }
                double tons = value * KrakkExplosionProfile.MEGATON_TONS;
                return new DetonationMagnitude(trimmed, KrakkExplosionProfile.powerFromTonnage(tons), tons);
            }
            if (normalized.endsWith("kt")) {
                double value = Double.parseDouble(normalized.substring(0, normalized.length() - 2));
                if (!Double.isFinite(value) || value <= 0.0D) {
                    throw new IllegalArgumentException("Magnitude must be > 0.");
                }
                double tons = value * KrakkExplosionProfile.KILOTON_TONS;
                return new DetonationMagnitude(trimmed, KrakkExplosionProfile.powerFromTonnage(tons), tons);
            }
            if (normalized.endsWith("t")) {
                double tons = Double.parseDouble(normalized.substring(0, normalized.length() - 1));
                if (!Double.isFinite(tons) || tons <= 0.0D) {
                    throw new IllegalArgumentException("Magnitude must be > 0.");
                }
                return new DetonationMagnitude(trimmed, KrakkExplosionProfile.powerFromTonnage(tons), tons);
            }

            double power = Double.parseDouble(normalized);
            if (!Double.isFinite(power) || power <= 0.0D) {
                throw new IllegalArgumentException("Magnitude must be > 0.");
            }
            double tonnage = KrakkExplosionProfile.tonnageFromPower(power);
            return new DetonationMagnitude(trimmed, power, tonnage);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid magnitude. Use power or tonnage suffixes like 1t, 1kt, 1mt.");
        }
    }

    private static int explode(CommandSourceStack source, Vec3 pos, String magnitudeInput, int blastTransmittanceCommand, double impactHeatCelsius) {
        double blastTransmittance = (blastTransmittanceCommand / 100.0D) * KrakkExplosionProfile.DEFAULT_BLAST_TRANSMITTANCE;
        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        DetonationMagnitude magnitude;
        try {
            magnitude = parseDetonationMagnitude(magnitudeInput);
        } catch (IllegalArgumentException exception) {
            source.sendFailure(Component.literal(exception.getMessage()));
            return 0;
        }
        double power = magnitude.power();
        double tonnage = magnitude.tonnageTons();
        // TNT reference point: vanilla TNT is roughly equivalent to a Krakk explosion at radius=4, power=25.
        double derivedRadius = KrakkExplosionCurves.computeBlastRadius(power);
        try {
            KrakkApi.explosions().detonate(
                    level,
                    pos.x,
                    pos.y,
                    pos.z,
                    sourceEntity,
                    owner,
                    KrakkExplosionProfile.fromPower(power, impactHeatCelsius).withBlastTransmittance(blastTransmittance)
            );
        } catch (Throwable exception) {
            LOGGER.error(
                    "Failed to execute /krakk explode at {} {} {} (input={}, power={}, heat={}).",
                    pos.x,
                    pos.y,
                    pos.z,
                    magnitude.rawInput(),
                    power,
                    impactHeatCelsius,
                    exception
            );
            if (exception instanceof OutOfMemoryError) {
                System.gc();
            }
            source.sendFailure(Component.literal("Failed to execute Krakk explosion command. See logs for details."));
            return 0;
        }
        try {
            source.sendSuccess(() -> Component.literal(String.format(
                    "Triggered Krakk explosion at %.2f %.2f %.2f (input=%s, power=%.2f, tonnage=%.4ft, blastTransmittance=%d%%, heat=%.2fC, derivedRadius=%.2f)",
                    pos.x, pos.y, pos.z, magnitude.rawInput(), power, tonnage, blastTransmittanceCommand, impactHeatCelsius, derivedRadius
            )), true);
        } catch (RuntimeException exception) {
            LOGGER.error(
                    "Explosion executed but /krakk explode feedback failed at {} {} {} (input={}).",
                    pos.x,
                    pos.y,
                    pos.z,
                    magnitude.rawInput(),
                    exception
            );
        }
        return 1;
    }

    @SuppressWarnings("unused")
    public static void tickQueuedDetonations(MinecraftServer server) {
    }


    private static int krakkExplode(CommandSourceStack source, Vec3 pos, double radius, double energy,
                                    int blastTransmittanceCommand, double impactHeatCelsius) {
        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        double blastTransmittance = blastTransmittanceCommand > 0
                ? blastTransmittanceCommand / 100.0D
                : KrakkExplosionProfile.DEFAULT_BLAST_TRANSMITTANCE;
        KrakkExplosionProfile profile = new KrakkExplosionProfile(radius, energy, impactHeatCelsius)
                .withBlastTransmittance(blastTransmittance);
        try {
            KrakkApi.explosions().detonate(
                    level,
                    pos.x,
                    pos.y,
                    pos.z,
                    sourceEntity,
                    owner,
                    profile
            );
        } catch (RuntimeException exception) {
            LOGGER.error(
                    "Failed to execute /krakk exploderad at {} {} {} (radius={}, energy={}).",
                    pos.x,
                    pos.y,
                    pos.z,
                    radius,
                    energy,
                    exception
            );
            source.sendFailure(Component.literal("Failed to execute Krakk explosion command. See logs for details."));
            return 0;
        }
        try {
            source.sendSuccess(() -> Component.literal(String.format(
                    "Triggered Krakk explosion at %.2f %.2f %.2f (radius=%.2f, energy=%.2f, transmittance=%.2f, heat=%.1f)",
                    pos.x, pos.y, pos.z, radius, energy, profile.blastTransmittance(), impactHeatCelsius
            )), true);
        } catch (RuntimeException exception) {
            LOGGER.error(
                    "Explosion executed but /krakk exploderad feedback failed at {} {} {} (radius={}, energy={}).",
                    pos.x,
                    pos.y,
                    pos.z,
                    radius,
                    energy,
                    exception
            );
        }
        return 1;
    }


    private static int fillBlockDamage(CommandSourceStack source, BlockPos from, BlockPos to, int value) {
        ServerLevel level = source.getLevel();
        Bounds bounds = Bounds.of(from, to);
        int updated = 0;
        int skipped = 0;
        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();

        for (int x = bounds.minX; x <= bounds.maxX; x++) {
            for (int y = bounds.minY; y <= bounds.maxY; y++) {
                for (int z = bounds.minZ; z <= bounds.maxZ; z++) {
                    cursor.set(x, y, z);
                    if (KrakkApi.damage().setDamageStateForDebug(level, cursor, value)) {
                        updated++;
                    } else {
                        skipped++;
                    }
                }
            }
        }

        int total = bounds.volume();
        int finalUpdated = updated;
        int finalSkipped = skipped;
        source.sendSuccess(() -> Component.literal(String.format(
                "Set block damage in area (%d blocks): updated=%d skipped=%d value=%d",
                total, finalUpdated, finalSkipped, value
        )), true);
        return updated;
    }

    private static int damageBlock(CommandSourceStack source, BlockPos pos, int amount, double impactHeatCelsius) {
        ServerLevel level = source.getLevel();
        BlockState blockState = level.getBlockState(pos);
        if (blockState.isAir()) {
            source.sendFailure(Component.literal("Target block is air."));
            return 0;
        }
        Entity sourceEntity = source.getEntity();

        KrakkImpactResult result;
        boolean converted = false;
        boolean ignited = false;
        if (KrakkApi.damage() instanceof KrakkDamageRuntime runtime) {
            if (amount <= 0) {
                KrakkDamageRuntime.ImpactExecutionResult executionResult = runtime.applyThermalImpactPrevalidatedWithEvents(
                        level,
                        pos,
                        blockState,
                        sourceEntity,
                        0.0D,
                        impactHeatCelsius,
                        null
                );
                result = executionResult.impactResult();
                converted = executionResult.converted();
                ignited = executionResult.ignited();
            } else {
                KrakkDamageRuntime.ImpactExecutionResult executionResult = runtime.applyImpactPrevalidatedWithEvents(
                        level,
                        pos,
                        blockState,
                        sourceEntity,
                        amount,
                        impactHeatCelsius,
                        false,
                        null
                );
                result = executionResult.impactResult();
                converted = executionResult.converted();
                ignited = executionResult.ignited();
            }
        } else {
            result = KrakkApi.damage().applyImpact(level, pos, blockState, sourceEntity, amount,
                    impactHeatCelsius, false, null);
        }

        int finalDamageState = Math.max(0, result.damageState());
        boolean finalConverted = converted;
        boolean finalIgnited = ignited;
        source.sendSuccess(() -> Component.literal(String.format(
                "Damaged block at %d %d %d by %.2f heat %.2fC (broken=%s, damageState=%d, converted=%s, ignited=%s)",
                pos.getX(), pos.getY(), pos.getZ(), (double) amount, impactHeatCelsius,
                result.broken(), finalDamageState, finalConverted, finalIgnited
        )), true);
        return 1;
    }

    private static int areaDamage(CommandSourceStack source, BlockPos from, BlockPos to, int amount) {
        ServerLevel level = source.getLevel();
        Bounds bounds = Bounds.of(from, to);
        int updated = 0;
        int skipped = 0;
        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
        int maxDamage = KrakkApi.damage().getMaxDamageState();

        for (int x = bounds.minX; x <= bounds.maxX; x++) {
            for (int y = bounds.minY; y <= bounds.maxY; y++) {
                for (int z = bounds.minZ; z <= bounds.maxZ; z++) {
                    cursor.set(x, y, z);
                    int current = KrakkApi.damage().getDamageState(level, cursor);
                    int next = Math.min(maxDamage, current + amount);
                    if (KrakkApi.damage().setDamageStateForDebug(level, cursor, next)) {
                        updated++;
                    } else {
                        skipped++;
                    }
                }
            }
        }

        int total = bounds.volume();
        int finalUpdated = updated;
        int finalSkipped = skipped;
        source.sendSuccess(() -> Component.literal(String.format(
                "Damaged area (%d blocks) by %d: updated=%d skipped=%d",
                total, amount, finalUpdated, finalSkipped
        )), true);
        return updated;
    }

    private static int decayTick(CommandSourceStack source, int ticks, ServerPlayer explicitTarget) {
        ServerPlayer target = resolveTargetPlayer(source, explicitTarget);
        if (target == null) {
            return 0;
        }

        //noinspection resource
        ServerLevel level = target.serverLevel();
        long shiftTicks = Math.max(1L, ticks);
        final int[] touched = {0};
        final int[] changed = {0};
        final int[] unsavedChunks = {0};

        ChunkLoop loop = iterateLoadedChunksForPlayer(target, (chunk, chunkX, chunkZ) -> {
            if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
                return;
            }

            KrakkBlockDamageChunkStorage storage = access.krakk$getBlockDamageStorage();
            final boolean[] chunkDirty = {false};
            storage.forEachSection((sectionY, states) -> {
                if (states.isEmpty()) {
                    return;
                }

                for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
                    short localIndex = entry.getShortKey();
                    int localX = localIndex & 15;
                    int localZ = (localIndex >> 4) & 15;
                    int localY = (localIndex >> 8) & 15;

                    int worldX = (chunkX << 4) | localX;
                    int worldY = (sectionY << 4) | localY;
                    int worldZ = (chunkZ << 4) | localZ;
                    long posLong = BlockPos.asLong(worldX, worldY, worldZ);

                    long lastTick = storage.getLastUpdateTick(posLong);
                    long newLastTick = lastTick >= 0L ? (lastTick - shiftTicks) : (level.getGameTime() - shiftTicks);
                    if (storage.setLastUpdateTick(posLong, newLastTick)) {
                        chunkDirty[0] = true;
                    }

                    int before = storage.getDamageState(posLong);
                    int after = KrakkApi.damage().getDamageState(level, BlockPos.of(posLong));
                    touched[0]++;
                    if (after != before) {
                        changed[0]++;
                    }
                }
            });

            if (chunkDirty[0]) {
                chunk.setUnsaved(true);
                unsavedChunks[0]++;
            }
        });
        int finalLoadedChunks = loop.loadedChunks();
        int finalTouched = touched[0];
        int finalChanged = changed[0];
        int finalUnsavedChunks = unsavedChunks[0];
        source.sendSuccess(() -> Component.literal(String.format(
                "Applied decay tick shift=%d for %s: loadedChunks=%d touched=%d changed=%d unsavedChunks=%d",
                ticks, target.getGameProfile().getName(), finalLoadedChunks, finalTouched, finalChanged, finalUnsavedChunks
        )), true);
        return finalChanged;
    }

    private static int syncLoadedChunks(CommandSourceStack source, ServerPlayer explicitTarget) {
        ServerPlayer target = resolveTargetPlayer(source, explicitTarget);
        if (target == null) {
            return 0;
        }

        //noinspection resource
        ServerLevel level = target.serverLevel();
        ChunkLoop loop = iterateLoadedChunksForPlayer(target, (chunk, chunkX, chunkZ) ->
                KrakkApi.damage().syncChunkToPlayer(target, level, chunkX, chunkZ, false)
        );

        int loadedChunks = loop.loadedChunks();
        source.sendSuccess(() -> Component.literal(String.format(
                "Synced Krakk damage state to %s for %d loaded chunks",
                target.getGameProfile().getName(), loadedChunks
        )), true);
        return loadedChunks;
    }

    private static int stats(CommandSourceStack source, ServerPlayer explicitTarget) {
        ServerPlayer target = resolveTargetPlayer(source, explicitTarget);
        if (target == null) {
            return 0;
        }

        final int[] chunksWithDamage = {0};
        final int[] damagedSections = {0};
        final int[] damagedBlocks = {0};

        ChunkLoop loop = iterateLoadedChunksForPlayer(target, (chunk, chunkX, chunkZ) -> {
            if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
                return;
            }

            final int[] sectionCountThisChunk = {0};
            access.krakk$getBlockDamageStorage().forEachSection((sectionY, states) -> {
                if (states.isEmpty()) {
                    return;
                }
                sectionCountThisChunk[0]++;
                damagedSections[0]++;
                damagedBlocks[0] += states.size();
            });

            if (sectionCountThisChunk[0] > 0) {
                chunksWithDamage[0]++;
            }
        });

        int loadedChunks = loop.loadedChunks();
        int finalChunksWithDamage = chunksWithDamage[0];
        int finalDamagedSections = damagedSections[0];
        int finalDamagedBlocks = damagedBlocks[0];
        source.sendSuccess(() -> Component.literal(String.format(
                "Krakk stats for %s: loadedChunks=%d chunksWithDamage=%d damagedSections=%d damagedBlocks=%d",
                target.getGameProfile().getName(),
                loadedChunks,
                finalChunksWithDamage,
                finalDamagedSections,
                finalDamagedBlocks
        )), false);
        return finalDamagedBlocks;
    }

    private static int validateDamageSections(CommandSourceStack source, ServerPlayer explicitTarget, int topChunks) {
        ServerPlayer target = resolveTargetPlayer(source, explicitTarget);
        if (target == null) {
            return 0;
        }

        final int[] chunksWithDamage = {0};
        final int[] damagedSections = {0};
        final int[] damagedBlocks = {0};
        final long[] worldYMod4Counts = new long[4];
        final long[] chunkXMod4WithDamage = new long[4];
        Map<Integer, Long> sectionEntriesByY = new HashMap<>();
        Map<Integer, Integer> sectionCountsByY = new HashMap<>();
        Map<Integer, Long> entriesByChunkX = new HashMap<>();
        List<ChunkDamageSummary> chunkSummaries = new ArrayList<>();

        ChunkLoop loop = iterateLoadedChunksForPlayer(target, (chunk, chunkX, chunkZ) -> {
            if (!(chunk instanceof KrakkBlockDamageChunkAccess access)) {
                return;
            }

            KrakkBlockDamageChunkStorage storage = access.krakk$getBlockDamageStorage();
            final int[] chunkEntryCount = {0};
            final int[] chunkSectionCount = {0};
            storage.forEachSection((sectionY, states) -> {
                if (states.isEmpty()) {
                    return;
                }

                int sectionEntries = states.size();
                chunkEntryCount[0] += sectionEntries;
                chunkSectionCount[0]++;
                sectionEntriesByY.merge(sectionY, (long) sectionEntries, Long::sum);
                sectionCountsByY.merge(sectionY, 1, Integer::sum);
                for (Short2ByteMap.Entry entry : states.short2ByteEntrySet()) {
                    int localIndex = entry.getShortKey() & 0x0FFF;
                    int localY = (localIndex >> 8) & 15;
                    int worldY = (sectionY << 4) | localY;
                    worldYMod4Counts[worldY & 3]++;
                }
            });

            if (chunkEntryCount[0] <= 0) {
                return;
            }

            chunksWithDamage[0]++;
            damagedSections[0] += chunkSectionCount[0];
            damagedBlocks[0] += chunkEntryCount[0];
            chunkXMod4WithDamage[Math.floorMod(chunkX, 4)]++;
            entriesByChunkX.merge(chunkX, (long) chunkEntryCount[0], Long::sum);
            chunkSummaries.add(new ChunkDamageSummary(chunkX, chunkZ, chunkSectionCount[0], chunkEntryCount[0]));
        });

        source.sendSuccess(() -> Component.literal(String.format(
                "Krakk validate damage for %s: loadedChunks=%d chunksWithDamage=%d damagedSections=%d damagedBlocks=%d",
                target.getGameProfile().getName(),
                loop.loadedChunks(),
                chunksWithDamage[0],
                damagedSections[0],
                damagedBlocks[0]
        )), false);

        if (damagedBlocks[0] <= 0) {
            source.sendSuccess(() -> Component.literal("No loaded chunk damage states found for this player view."), false);
            return 0;
        }

        source.sendSuccess(() -> Component.literal(String.format(
                "chunkX%%4 with damage chunks: [0]=%d [1]=%d [2]=%d [3]=%d",
                chunkXMod4WithDamage[0],
                chunkXMod4WithDamage[1],
                chunkXMod4WithDamage[2],
                chunkXMod4WithDamage[3]
        )), false);
        source.sendSuccess(() -> Component.literal(String.format(
                "worldY%%4 with damage blocks: [0]=%d [1]=%d [2]=%d [3]=%d",
                worldYMod4Counts[0],
                worldYMod4Counts[1],
                worldYMod4Counts[2],
                worldYMod4Counts[3]
        )), false);

        List<Integer> sortedSectionY = new ArrayList<>(sectionEntriesByY.keySet());
        sortedSectionY.sort(Integer::compareTo);
        for (int sectionY : sortedSectionY) {
            long entries = sectionEntriesByY.getOrDefault(sectionY, 0L);
            int sections = sectionCountsByY.getOrDefault(sectionY, 0);
            source.sendSuccess(() -> Component.literal(String.format(
                    "sectionY=%d entries=%d sections=%d",
                    sectionY,
                    entries,
                    sections
            )), false);
        }

        List<Map.Entry<Integer, Long>> sortedChunkXColumns = new ArrayList<>(entriesByChunkX.entrySet());
        sortedChunkXColumns.sort((left, right) -> Long.compare(right.getValue(), left.getValue()));
        int topColumns = Math.min(8, sortedChunkXColumns.size());
        for (int i = 0; i < topColumns; i++) {
            Map.Entry<Integer, Long> column = sortedChunkXColumns.get(i);
            int rank = i + 1;
            source.sendSuccess(() -> Component.literal(String.format(
                    "topChunkX[%d] x=%d entries=%d",
                    rank,
                    column.getKey(),
                    column.getValue()
            )), false);
        }

        chunkSummaries.sort(Comparator.comparingInt(ChunkDamageSummary::entries).reversed());
        int topChunkCount = Math.min(Math.max(1, topChunks), chunkSummaries.size());
        for (int i = 0; i < topChunkCount; i++) {
            ChunkDamageSummary summary = chunkSummaries.get(i);
            int rank = i + 1;
            source.sendSuccess(() -> Component.literal(String.format(
                    "topChunk[%d] chunk=(%d, %d) entries=%d sections=%d",
                    rank,
                    summary.chunkX(),
                    summary.chunkZ(),
                    summary.entries(),
                    summary.sections()
            )), false);
        }

        return damagedBlocks[0];
    }

    private static int overlayRefresh(CommandSourceStack source, ServerPlayer explicitTarget) {
        return syncLoadedChunks(source, explicitTarget);
    }

    private static int overlayClear(CommandSourceStack source, ServerPlayer explicitTarget) {
        ServerPlayer target = resolveTargetPlayer(source, explicitTarget);
        if (target == null) {
            return 0;
        }

        //noinspection resource
        ServerLevel level = target.serverLevel();
        ChunkLoop loop = iterateLoadedChunksForPlayer(target, (chunk, chunkX, chunkZ) ->
                KrakkApi.network().sendChunkUnload(target, level.dimension().location(), chunkX, chunkZ)
        );

        int loadedChunks = loop.loadedChunks();
        source.sendSuccess(() -> Component.literal(String.format(
                "Cleared Krakk overlay cache for %s across %d loaded chunks",
                target.getGameProfile().getName(), loadedChunks
        )), true);
        return loadedChunks;
    }

    private static ServerPlayer resolveTargetPlayer(CommandSourceStack source, ServerPlayer explicitTarget) {
        if (explicitTarget != null) {
            return explicitTarget;
        }

        try {
            return source.getPlayerOrException();
        } catch (CommandSyntaxException ignored) {
            source.sendFailure(Component.literal("This command requires a target player when run from console."));
            return null;
        }
    }

    private static ChunkLoop iterateLoadedChunksForPlayer(ServerPlayer player, LoadedChunkConsumer consumer) {
        //noinspection resource
        ServerLevel level = player.serverLevel();
        ChunkPos center = player.chunkPosition();
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;
        int loaded = 0;

        for (int chunkX = center.x - viewDistance; chunkX <= center.x + viewDistance; chunkX++) {
            for (int chunkZ = center.z - viewDistance; chunkZ <= center.z + viewDistance; chunkZ++) {
                LevelChunk chunk = level.getChunkSource().getChunkNow(chunkX, chunkZ);
                if (chunk == null) {
                    continue;
                }
                loaded++;
                consumer.accept(chunk, chunkX, chunkZ);
            }
        }

        return new ChunkLoop(loaded);
    }

    @FunctionalInterface
    private interface LoadedChunkConsumer {
        void accept(LevelChunk chunk, int chunkX, int chunkZ);
    }

    private record ChunkLoop(int loadedChunks) {
    }

    private record ChunkDamageSummary(int chunkX, int chunkZ, int sections, int entries) {
    }

    private record DetonationMagnitude(String rawInput, double power, double tonnageTons) {
    }

    private record Bounds(int minX, int minY, int minZ, int maxX, int maxY, int maxZ) {
        private static Bounds of(BlockPos from, BlockPos to) {
            return new Bounds(
                    Math.min(from.getX(), to.getX()),
                    Math.min(from.getY(), to.getY()),
                    Math.min(from.getZ(), to.getZ()),
                    Math.max(from.getX(), to.getX()),
                    Math.max(from.getY(), to.getY()),
                    Math.max(from.getZ(), to.getZ())
            );
        }

        private int volume() {
            return (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1);
        }
    }
}
