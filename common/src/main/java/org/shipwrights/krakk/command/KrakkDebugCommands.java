package org.shipwrights.krakk.command;

import com.mojang.logging.LogUtils;
import com.mojang.brigadier.arguments.BoolArgumentType;
import com.mojang.brigadier.arguments.DoubleArgumentType;
import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.mojang.brigadier.arguments.LongArgumentType;
import com.mojang.brigadier.arguments.StringArgumentType;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import dev.architectury.event.events.common.CommandRegistrationEvent;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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
import org.shipwrights.krakk.runtime.explosion.KrakkExplosionRuntime;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KrakkDebugCommands {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final int DEFAULT_MAX_POWER = 1_000_000;
    private static final int DEFAULT_DECAY_TICKS = 24_000;
    private static final int DEFAULT_PROFILE_RUNS = 20;
    private static final int DEFAULT_PROFILE_WARMUP = 3;
    private static final double QPROF_KRAKK_RADIUS = 96.0D;
    private static final double QPROF_KRAKK_ENERGY = 1_000_000.0D;
    private static final int QPROF_RUNS = 7;
    private static final int QPROF_WARMUP = 2;
    private static final long QPROF_SEED = 12_345L;
    private static final double QPROF_DAMAGE_RADIUS = 96.0D;
    private static final int QPROF_DAMAGE_RUNS = 1;
    private static final int QPROF_DAMAGE_WARMUP = 0;
    private static final long QPROF_DAMAGE_SEED = 12_345L;
    private static final double QPROF_DAMAGE_MAX_IMPACT = 12.0D;
    private static final int QPROF_DAMAGE_SAMPLE_STEP = 3;
    private static final boolean QPROF_DAMAGE_DROP_ON_BREAK = false;
    private static final int DAMAGE_PROFILE_PROGRESS_INTERVAL = 20_000;

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
                        .then(Commands.literal("explode")
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
                        .then(Commands.literal("phaselogging")
                                .executes(context -> reportPhaseLogging(context.getSource()))
                                .then(Commands.argument("enabled", BoolArgumentType.bool())
                                        .executes(context -> setPhaseLogging(
                                                context.getSource(),
                                                BoolArgumentType.getBool(context, "enabled")
                                        ))))
                        .then(Commands.literal("krakkexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(0.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> krakkExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy")))))))
                        .then(Commands.literal("profexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("magnitude", StringArgumentType.word())
                                                .executes(context -> profExplode(
                                                        context.getSource(),
                                                        Vec3Argument.getVec3(context, "pos"),
                                                        StringArgumentType.getString(context, "magnitude"),
                                                        DEFAULT_PROFILE_RUNS,
                                                        DEFAULT_PROFILE_WARMUP,
                                                        Long.MIN_VALUE,
                                                        false))
                                                .then(Commands.argument("runs", IntegerArgumentType.integer(1, 200))
                                                        .executes(context -> profExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                StringArgumentType.getString(context, "magnitude"),
                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                DEFAULT_PROFILE_WARMUP,
                                                                Long.MIN_VALUE,
                                                                false))
                                                        .then(Commands.argument("warmup", IntegerArgumentType.integer(0, 200))
                                                                .executes(context -> profExplode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        StringArgumentType.getString(context, "magnitude"),
                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                        Long.MIN_VALUE,
                                                                        false))
                                                                .then(Commands.argument("seed", LongArgumentType.longArg())
                                                                        .executes(context -> profExplode(
                                                                                context.getSource(),
                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                StringArgumentType.getString(context, "magnitude"),
                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                LongArgumentType.getLong(context, "seed"),
                                                                                false))
                                                                        .then(Commands.argument("apply", BoolArgumentType.bool())
                                                                                .executes(context -> profExplode(
                                                                                        context.getSource(),
                                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                                        StringArgumentType.getString(context, "magnitude"),
                                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                                        LongArgumentType.getLong(context, "seed"),
                                                                                        BoolArgumentType.getBool(context, "apply"))))))))))
                        .then(Commands.literal("profkrakkexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(0.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> profKrakkExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                DEFAULT_PROFILE_RUNS,
                                                                DEFAULT_PROFILE_WARMUP,
                                                                Long.MIN_VALUE,
                                                                false))
                                                        .then(Commands.argument("runs", IntegerArgumentType.integer(1, 200))
                                                                .executes(context -> profKrakkExplode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                        DEFAULT_PROFILE_WARMUP,
                                                                        Long.MIN_VALUE,
                                                                        false))
                                                                .then(Commands.argument("warmup", IntegerArgumentType.integer(0, 200))
                                                                        .executes(context -> profKrakkExplode(
                                                                                context.getSource(),
                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                Long.MIN_VALUE,
                                                                                false))
                                                                        .then(Commands.argument("seed", LongArgumentType.longArg())
                                                                                .executes(context -> profKrakkExplode(
                                                                                        context.getSource(),
                                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                                        LongArgumentType.getLong(context, "seed"),
                                                                                        false))
                                                                                .then(Commands.argument("apply", BoolArgumentType.bool())
                                                                                        .executes(context -> profKrakkExplode(
                                                                                                context.getSource(),
                                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                                LongArgumentType.getLong(context, "seed"),
                                                                                                BoolArgumentType.getBool(context, "apply")))))))))))
                        .then(Commands.literal("qprof")
                                .executes(context -> quickProfileKrakk(context.getSource(), false))
                                .then(Commands.argument("apply", BoolArgumentType.bool())
                                        .executes(context -> quickProfileKrakk(
                                                context.getSource(),
                                                BoolArgumentType.getBool(context, "apply")))))
                        .then(Commands.literal("profdamage")
                                .executes(context -> profDamageStates(
                                        context.getSource(),
                                        QPROF_DAMAGE_RUNS,
                                        QPROF_DAMAGE_WARMUP,
                                        QPROF_DAMAGE_SEED,
                                        false
                                ))
                                .then(Commands.argument("runs", IntegerArgumentType.integer(1, 200))
                                        .executes(context -> profDamageStates(
                                                context.getSource(),
                                                IntegerArgumentType.getInteger(context, "runs"),
                                                QPROF_DAMAGE_WARMUP,
                                                QPROF_DAMAGE_SEED,
                                                false
                                        ))
                                        .then(Commands.argument("warmup", IntegerArgumentType.integer(0, 200))
                                                .executes(context -> profDamageStates(
                                                        context.getSource(),
                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                        QPROF_DAMAGE_SEED,
                                                        false
                                                ))
                                                .then(Commands.argument("seed", LongArgumentType.longArg())
                                                        .executes(context -> profDamageStates(
                                                                context.getSource(),
                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                LongArgumentType.getLong(context, "seed"),
                                                                false
                                                        ))))))
                        .then(Commands.literal("qprofdamage")
                                .executes(context -> profDamageStates(
                                        context.getSource(),
                                        QPROF_DAMAGE_RUNS,
                                        QPROF_DAMAGE_WARMUP,
                                        QPROF_DAMAGE_SEED,
                                        false
                                )))
                        .then(Commands.literal("qprofdamageclean")
                                .executes(context -> profDamageStates(
                                        context.getSource(),
                                        QPROF_DAMAGE_RUNS,
                                        QPROF_DAMAGE_WARMUP,
                                        QPROF_DAMAGE_SEED,
                                        true
                                )))
                        .then(Commands.literal("qprofdamageapply")
                                .executes(context -> profDamageStates(
                                        context.getSource(),
                                        QPROF_DAMAGE_RUNS,
                                        QPROF_DAMAGE_WARMUP,
                                        QPROF_DAMAGE_SEED,
                                        false
                                )))
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

    private static int reportPhaseLogging(CommandSourceStack source) {
        boolean enabled = KrakkExplosionRuntime.isKrakkPhaseTimingLoggingEnabled();
        source.sendSuccess(() -> Component.literal("Krakk phase timing logging: " + enabled), false);
        return 1;
    }

    private static int setPhaseLogging(CommandSourceStack source, boolean enabled) {
        KrakkExplosionRuntime.setKrakkPhaseTimingLoggingEnabled(enabled);
        source.sendSuccess(() -> Component.literal("Set Krakk phase timing logging to: " + enabled), true);
        return 1;
    }

    @SuppressWarnings("unused")
    public static void tickQueuedDetonations(MinecraftServer server) {
    }


    private static int krakkExplode(CommandSourceStack source, Vec3 pos, double radius, double energy) {
        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        try {
            KrakkApi.explosions().detonate(
                    level,
                    pos.x,
                    pos.y,
                    pos.z,
                    sourceEntity,
                    owner,
                    radius,
                    energy
            );
        } catch (RuntimeException exception) {
            LOGGER.error(
                    "Failed to execute /krakk krakkexplode at {} {} {} (radius={}, energy={}).",
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
                    "Triggered Krakk explosion at %.2f %.2f %.2f (radius=%.2f, energy=%.2f)",
                    pos.x, pos.y, pos.z, radius, energy
            )), true);
        } catch (RuntimeException exception) {
            LOGGER.error(
                    "Explosion executed but /krakk krakkexplode feedback failed at {} {} {} (radius={}, energy={}).",
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

    private static int profExplode(CommandSourceStack source, Vec3 pos, String magnitudeInput,
                                   int runs, int warmup, long seed, boolean apply) {
        if (!(KrakkApi.explosions() instanceof KrakkExplosionRuntime runtime)) {
            source.sendFailure(Component.literal("Krakk explosion runtime does not support profiling."));
            return 0;
        }

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
        KrakkExplosionProfile profile = KrakkExplosionProfile.fromPower(magnitude.power());
        long effectiveSeed = seed != Long.MIN_VALUE ? seed : (level.getGameTime() ^ BlockPos.containing(pos).asLong());
        int effectiveRuns = resolveProfileRuns(apply, runs);
        int effectiveWarmup = resolveProfileWarmup(apply, warmup);
        emitApplyProfileIterationNotice(source, apply, runs, warmup, effectiveRuns, effectiveWarmup);

        ProfileAggregate aggregate = profileMode(
                runtime, level, sourceEntity, owner, pos, profile, apply, effectiveRuns, effectiveWarmup, effectiveSeed
        );
        emitProfileAggregate(source, "profexplode", apply, effectiveRuns, effectiveWarmup, effectiveSeed, aggregate);
        return effectiveRuns;
    }

    private static int profKrakkExplode(CommandSourceStack source, Vec3 pos, double radius, double energy,
                                        int runs, int warmup, long seed, boolean apply) {
        if (!(KrakkApi.explosions() instanceof KrakkExplosionRuntime runtime)) {
            source.sendFailure(Component.literal("Krakk explosion runtime does not support profiling."));
            return 0;
        }

        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        KrakkExplosionProfile profile = KrakkExplosionProfile.krakk(radius, energy);
        long effectiveSeed = seed != Long.MIN_VALUE ? seed : (level.getGameTime() ^ BlockPos.containing(pos).asLong());
        int effectiveRuns = resolveProfileRuns(apply, runs);
        int effectiveWarmup = resolveProfileWarmup(apply, warmup);
        emitApplyProfileIterationNotice(source, apply, runs, warmup, effectiveRuns, effectiveWarmup);

        ProfileAggregate aggregate = profileMode(
                runtime, level, sourceEntity, owner, pos, profile, apply, effectiveRuns, effectiveWarmup, effectiveSeed
        );
        emitProfileAggregate(source, "profkrakkexplode", apply, effectiveRuns, effectiveWarmup, effectiveSeed, aggregate);
        return effectiveRuns;
    }

    private static int quickProfileKrakk(CommandSourceStack source, boolean apply) {
        return profKrakkExplode(
                source,
                source.getPosition(),
                QPROF_KRAKK_RADIUS,
                QPROF_KRAKK_ENERGY,
                QPROF_RUNS,
                QPROF_WARMUP,
                QPROF_SEED,
                apply
        );
    }

    private static int profDamageStates(CommandSourceStack source, int runs, int warmup, long seed, boolean cleanup) {
        ServerLevel level = source.getLevel();
        Vec3 center = source.getPosition();
        // Live applyImpact profiling mutates the world, so this profile mode is single-pass by design.
        int effectiveRuns = 1;
        int effectiveWarmup = 0;
        emitDamageProfileIterationNotice(source, runs, warmup, effectiveRuns, effectiveWarmup);

        DamageProfileTargets targets = buildDamageProfileTargets(level, center);
        if (targets.positions().isEmpty()) {
            source.sendFailure(Component.literal("No spherical impact targets were generated."));
            return 0;
        }

        long[] nanos = new long[effectiveRuns];
        long sumNanos = 0L;
        long minNanos = Long.MAX_VALUE;
        long maxNanos = Long.MIN_VALUE;
        long totalAttempted = 0L;
        long totalBroken = 0L;
        long totalDamaged = 0L;
        long totalNoEffect = 0L;
        long totalSkipped = 0L;
        long totalPreClearNanos = 0L;
        long totalApplyNanos = 0L;
        long totalPostClearNanos = 0L;
        KrakkDamageRuntime.DamageRuntimeProfileSnapshot runtimeSnapshot = null;
        boolean captureRuntimeProfile = KrakkApi.damage() instanceof KrakkDamageRuntime;
        if (captureRuntimeProfile) {
            KrakkDamageRuntime.beginRuntimeProfiling();
        }
        try {
            for (int i = 0; i < effectiveRuns; i++) {
                DamageProfileRunResult result = runDamageProfilePass(
                        level,
                        targets,
                        seed + effectiveWarmup + i,
                        true,
                        cleanup
                );
                nanos[i] = result.elapsedNanos();
                sumNanos += result.elapsedNanos();
                minNanos = result.elapsedNanos();
                maxNanos = result.elapsedNanos();
                totalAttempted += result.attempted();
                totalBroken += result.broken();
                totalDamaged += result.damaged();
                totalNoEffect += result.noEffect();
                totalSkipped += result.skipped();
                totalPreClearNanos += result.preClearNanos();
                totalApplyNanos += result.applyNanos();
                totalPostClearNanos += result.postClearNanos();
            }
        } finally {
            if (captureRuntimeProfile) {
                runtimeSnapshot = KrakkDamageRuntime.endRuntimeProfiling();
            }
        }

        long[] sorted = Arrays.copyOf(nanos, nanos.length);
        Arrays.sort(sorted);
        double avgNanos = sumNanos / (double) effectiveRuns;
        double varianceNanos = 0.0D;
        for (long sampleNanos : nanos) {
            double delta = sampleNanos - avgNanos;
            varianceNanos += delta * delta;
        }
        varianceNanos /= effectiveRuns;
        double stddevNanos = Math.sqrt(Math.max(0.0D, varianceNanos));
        double covPercent = avgNanos > 0.0D ? ((stddevNanos / avgNanos) * 100.0D) : 0.0D;
        long p95 = sorted[percentileIndex(sorted.length, 0.95D)];
        long p99 = sorted[percentileIndex(sorted.length, 0.99D)];
        double avgMs = nanosToMs(avgNanos);
        double stddevMs = nanosToMs(stddevNanos);
        double p95Ms = nanosToMs(p95);
        double p99Ms = nanosToMs(p99);
        double minMs = nanosToMs(minNanos);
        double maxMs = nanosToMs(maxNanos);
        double avgCandidates = totalAttempted / (double) effectiveRuns;
        double avgBroken = totalBroken / (double) effectiveRuns;
        double avgDamaged = totalDamaged / (double) effectiveRuns;
        double avgNoEffect = totalNoEffect / (double) effectiveRuns;
        double avgSkipped = totalSkipped / (double) effectiveRuns;
        final double avgPreClearMs = nanosToMs(totalPreClearNanos / (double) effectiveRuns);
        final double avgApplyMs = nanosToMs(totalApplyNanos / (double) effectiveRuns);
        final double avgPostClearMs = nanosToMs(totalPostClearNanos / (double) effectiveRuns);
        final int reportRuns = effectiveRuns;
        final int reportWarmup = effectiveWarmup;
        final long reportSeed = seed;

        source.sendSuccess(() -> Component.literal(String.format(
                "Krakk profdamage: test=damage-impact-live-r96-step%d-maxImpact=%.1f-drop=%s-cleanup=%s runs=%d warmup=%d seed=%d avg=%.3fms stddev=%.3fms cov=%.2f%% p95=%.3fms p99=%.3fms min=%.3fms max=%.3fms",
                QPROF_DAMAGE_SAMPLE_STEP,
                QPROF_DAMAGE_MAX_IMPACT,
                QPROF_DAMAGE_DROP_ON_BREAK,
                cleanup,
                reportRuns,
                reportWarmup,
                reportSeed,
                avgMs,
                stddevMs,
                covPercent,
                p95Ms,
                p99Ms,
                minMs,
                maxMs
        )), false);
        source.sendSuccess(() -> Component.literal(
                "profileQuality: low confidence; recommended runs>=7 warmup>=2 for parity/perf comparisons."
        ), false);
        source.sendSuccess(() -> Component.literal(
                "profileScope: avg is impactApply->visible sync; preClear/postClear are reported separately."
        ), false);
        source.sendSuccess(() -> Component.literal(String.format(
                "avgDamage: radius=%.1f candidates=%.1f broken=%.1f damaged=%.1f noEffect=%.1f skipped=%.1f maxImpact=%.1f dropOnBreak=%s sampleStep=%d",
                QPROF_DAMAGE_RADIUS,
                avgCandidates,
                avgBroken,
                avgDamaged,
                avgNoEffect,
                avgSkipped,
                QPROF_DAMAGE_MAX_IMPACT,
                QPROF_DAMAGE_DROP_ON_BREAK,
                QPROF_DAMAGE_SAMPLE_STEP
        )), false);
        source.sendSuccess(() -> Component.literal(String.format(
                "avgDamageStages: preClear=%.3fms apply=%.3fms postClear=%.3fms",
                avgPreClearMs,
                avgApplyMs,
                avgPostClearMs
        )), false);
        if (!cleanup) {
            source.sendSuccess(() -> Component.literal(
                    "profdamage cleanup disabled: damage states and break outcomes were left in-world for inspection."
            ), false);
        }
        if (runtimeSnapshot != null) {
            final double perRunDebugMs = nanosToMs(runtimeSnapshot.debugMethodNanos() / (double) effectiveRuns);
            final double perRunClearMs = nanosToMs(runtimeSnapshot.clearMethodNanos() / (double) effectiveRuns);
            final double perRunSetMs = nanosToMs(runtimeSnapshot.setMethodNanos() / (double) effectiveRuns);
            final double perRunSyncMs = nanosToMs(runtimeSnapshot.syncMethodNanos() / (double) effectiveRuns);
            final double perRunMarkMs = nanosToMs(runtimeSnapshot.markMethodNanos() / (double) effectiveRuns);
            final double perRunFlushMs = nanosToMs(runtimeSnapshot.flushMethodNanos() / (double) effectiveRuns);
            final double perRunDebugLiveCheckMs = nanosToMs(runtimeSnapshot.debugLiveCheckNanos() / (double) effectiveRuns);
            final double perRunClearLookupMs = nanosToMs(runtimeSnapshot.clearLookupNanos() / (double) effectiveRuns);
            final double perRunClearRemoveMs = nanosToMs(runtimeSnapshot.clearRemoveNanos() / (double) effectiveRuns);
            final double perRunSetLookupMs = nanosToMs(runtimeSnapshot.setLookupNanos() / (double) effectiveRuns);
            final double perRunSetStorageMs = nanosToMs(runtimeSnapshot.setStorageNanos() / (double) effectiveRuns);
            final double perRunSetConversionMs = nanosToMs(runtimeSnapshot.setConversionNanos() / (double) effectiveRuns);
            final long perRunDebugCalls = Math.round(runtimeSnapshot.debugCalls() / (double) effectiveRuns);
            final long perRunClearCalls = Math.round(runtimeSnapshot.clearCalls() / (double) effectiveRuns);
            final long perRunSetCalls = Math.round(runtimeSnapshot.setCalls() / (double) effectiveRuns);
            final long perRunSyncCalls = Math.round(runtimeSnapshot.syncCalls() / (double) effectiveRuns);
            final long perRunSyncBatched = Math.round(runtimeSnapshot.syncBatched() / (double) effectiveRuns);
            final long perRunSyncCoalesced = Math.round(runtimeSnapshot.syncCoalesced() / (double) effectiveRuns);
            final long perRunSyncDirect = Math.round(runtimeSnapshot.syncDirect() / (double) effectiveRuns);
            final long perRunSyncSuppressed = Math.round(runtimeSnapshot.syncSuppressed() / (double) effectiveRuns);
            final long perRunMarkCalls = Math.round(runtimeSnapshot.markCalls() / (double) effectiveRuns);
            final long perRunPacketSends = Math.round(runtimeSnapshot.markPacketSends() / (double) effectiveRuns);
            final long perRunCacheNotifies = Math.round(runtimeSnapshot.markCacheNotifies() / (double) effectiveRuns);
            final long perRunFlushCalls = Math.round(runtimeSnapshot.flushCalls() / (double) effectiveRuns);
            final long perRunFlushEntries = Math.round(runtimeSnapshot.flushEntries() / (double) effectiveRuns);
            final long perRunFlushRouteChunkCache = Math.round(runtimeSnapshot.flushRouteChunkCache() / (double) effectiveRuns);
            final long perRunFlushRoutePerBlockFallback = Math.round(runtimeSnapshot.flushRoutePerBlockFallback() / (double) effectiveRuns);
            final long perRunFlushRouteSectionDelta = Math.round(runtimeSnapshot.flushRouteSectionDelta() / (double) effectiveRuns);
            final long perRunFlushRouteSectionSnapshot = Math.round(runtimeSnapshot.flushRouteSectionSnapshot() / (double) effectiveRuns);

            LOGGER.info(
                    "Krakk profdamage runtime: debug={}ms clear={}ms set={}ms sync={}ms mark={}ms flush={}ms",
                    String.format("%.3f", perRunDebugMs),
                    String.format("%.3f", perRunClearMs),
                    String.format("%.3f", perRunSetMs),
                    String.format("%.3f", perRunSyncMs),
                    String.format("%.3f", perRunMarkMs),
                    String.format("%.3f", perRunFlushMs)
            );
            LOGGER.info(
                    "Krakk profdamage runtime detail: debug(liveCheck={}ms calls={}) clear(lookup={}ms remove={}ms calls={}) set(lookup={}ms storage={}ms conversion={}ms calls={})",
                    String.format("%.3f", perRunDebugLiveCheckMs),
                    perRunDebugCalls,
                    String.format("%.3f", perRunClearLookupMs),
                    String.format("%.3f", perRunClearRemoveMs),
                    perRunClearCalls,
                    String.format("%.3f", perRunSetLookupMs),
                    String.format("%.3f", perRunSetStorageMs),
                    String.format("%.3f", perRunSetConversionMs),
                    perRunSetCalls
            );
            LOGGER.info(
                    "Krakk profdamage runtime sync: sync(calls={} batched={} coalesced={} direct={} suppressed={}) mark(calls={} packets={} cache={}) flush(calls={} entries={} route(cache={} perBlockFallback={} sectionDelta={} sectionSnapshot={}))",
                    perRunSyncCalls,
                    perRunSyncBatched,
                    perRunSyncCoalesced,
                    perRunSyncDirect,
                    perRunSyncSuppressed,
                    perRunMarkCalls,
                    perRunPacketSends,
                    perRunCacheNotifies,
                    perRunFlushCalls,
                    perRunFlushEntries,
                    perRunFlushRouteChunkCache,
                    perRunFlushRoutePerBlockFallback,
                    perRunFlushRouteSectionDelta,
                    perRunFlushRouteSectionSnapshot
            );

            source.sendSuccess(() -> Component.literal(String.format(
                    "avgDamageRuntime: debug=%.3fms clear=%.3fms set=%.3fms sync=%.3fms mark=%.3fms flush=%.3fms",
                    perRunDebugMs,
                    perRunClearMs,
                    perRunSetMs,
                    perRunSyncMs,
                    perRunMarkMs,
                    perRunFlushMs
            )), false);
            source.sendSuccess(() -> Component.literal(String.format(
                    "avgDamageRuntimeDetail: debug(liveCheck=%.3fms calls=%d) clear(lookup=%.3fms remove=%.3fms calls=%d) set(lookup=%.3fms storage=%.3fms conversion=%.3fms calls=%d)",
                    perRunDebugLiveCheckMs,
                    perRunDebugCalls,
                    perRunClearLookupMs,
                    perRunClearRemoveMs,
                    perRunClearCalls,
                    perRunSetLookupMs,
                    perRunSetStorageMs,
                    perRunSetConversionMs,
                    perRunSetCalls
            )), false);
            source.sendSuccess(() -> Component.literal(String.format(
                    "avgDamageRuntimeSync: sync(calls=%d batched=%d coalesced=%d direct=%d suppressed=%d) mark(calls=%d packets=%d cache=%d) flush(calls=%d entries=%d route(cache=%d perBlockFallback=%d sectionDelta=%d sectionSnapshot=%d))",
                    perRunSyncCalls,
                    perRunSyncBatched,
                    perRunSyncCoalesced,
                    perRunSyncDirect,
                    perRunSyncSuppressed,
                    perRunMarkCalls,
                    perRunPacketSends,
                    perRunCacheNotifies,
                    perRunFlushCalls,
                    perRunFlushEntries,
                    perRunFlushRouteChunkCache,
                    perRunFlushRoutePerBlockFallback,
                    perRunFlushRouteSectionDelta,
                    perRunFlushRouteSectionSnapshot
            )), false);
        }
        return effectiveRuns;
    }

    private static DamageProfileTargets buildDamageProfileTargets(ServerLevel level, Vec3 center) {
        double radius = QPROF_DAMAGE_RADIUS;
        int minX = (int) Math.floor(center.x - radius);
        int maxX = (int) Math.ceil(center.x + radius);
        int minY = (int) Math.floor(center.y - radius);
        int maxY = (int) Math.ceil(center.y + radius);
        int minZ = (int) Math.floor(center.z - radius);
        int maxZ = (int) Math.ceil(center.z + radius);
        double radiusSq = radius * radius;

        LongArrayList positions = new LongArrayList(Math.max(4096, (int) (radius * radius)));
        DoubleArrayList impactPowers = new DoubleArrayList(Math.max(4096, (int) (radius * radius)));
        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
        for (int x = minX; x <= maxX; x += QPROF_DAMAGE_SAMPLE_STEP) {
            double dx = (x + 0.5D) - center.x;
            for (int y = minY; y <= maxY; y += QPROF_DAMAGE_SAMPLE_STEP) {
                double dy = (y + 0.5D) - center.y;
                for (int z = minZ; z <= maxZ; z += QPROF_DAMAGE_SAMPLE_STEP) {
                    double dz = (z + 0.5D) - center.z;
                    double distSq = (dx * dx) + (dy * dy) + (dz * dz);
                    if (distSq > radiusSq) {
                        continue;
                    }
                    double dist = Math.sqrt(distSq);
                    double normalized = 1.0D - (dist / Math.max(radius, 1.0E-9D));
                    double impactPower = normalized * QPROF_DAMAGE_MAX_IMPACT;
                    if (impactPower <= 0.0D) {
                        continue;
                    }
                    cursor.set(x, y, z);
                    if (!level.isInWorldBounds(cursor)) {
                        continue;
                    }
                    var blockState = level.getBlockState(cursor);
                    if (blockState.isAir() || blockState.getDestroySpeed(level, cursor) < 0.0F) {
                        continue;
                    }
                    positions.add(BlockPos.asLong(x, y, z));
                    impactPowers.add(impactPower);
                }
            }
        }
        return new DamageProfileTargets(positions, impactPowers);
    }

    private static DamageProfileRunResult runDamageProfilePass(ServerLevel level, DamageProfileTargets targets, long seed,
                                                               boolean logProgress, boolean cleanup) {
        final long[] preClearNanos = {0L};
        final long[] applyNanos = {0L};
        final long[] postClearNanos = {0L};
        final int[] broken = {0};
        final int[] damaged = {0};
        final int[] noEffect = {0};
        final int[] skipped = {0};
        final int size = targets.positions().size();
        final boolean bulkSync = KrakkApi.damage() instanceof KrakkDamageRuntime;

        Runnable preClearWork = () -> {
            long preClearStart = System.nanoTime();
            clearDamageProfileTargets(level, targets, "preClear", logProgress);
            preClearNanos[0] = System.nanoTime() - preClearStart;
        };

        Runnable applyWork = () -> {
            LongArrayList positions = targets.positions();
            DoubleArrayList impactPowers = targets.impactPowers();
            int startOffset = size > 0 ? Math.floorMod(seed, size) : 0;
            long applyStart = System.nanoTime();
            BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
            for (int i = 0; i < size; i++) {
                int index = startOffset + i;
                if (index >= size) {
                    index -= size;
                }
                long posLong = positions.getLong(index);
                cursor.set(BlockPos.getX(posLong), BlockPos.getY(posLong), BlockPos.getZ(posLong));
                var liveState = level.getBlockState(cursor);
                if (liveState.isAir() || liveState.getDestroySpeed(level, cursor) < 0.0F) {
                    skipped[0]++;
                } else {
                    KrakkImpactResult result = KrakkApi.damage().applyImpact(
                            level,
                            cursor,
                            liveState,
                            null,
                            impactPowers.getDouble(index),
                            QPROF_DAMAGE_DROP_ON_BREAK
                    );
                    if (result.broken()) {
                        broken[0]++;
                    } else if (result.damageState() > 0) {
                        damaged[0]++;
                    } else {
                        noEffect[0]++;
                    }
                }
                int processed = i + 1;
                if (logProgress && (processed % DAMAGE_PROFILE_PROGRESS_INTERVAL) == 0) {
                    LOGGER.info(
                            "Krakk profdamage progress: stage=apply processed={}/{} elapsed={}ms broken={} damaged={} noEffect={} skipped={}",
                            processed,
                            size,
                            String.format("%.3f", nanosToMs(System.nanoTime() - applyStart)),
                            broken[0],
                            damaged[0],
                            noEffect[0],
                            skipped[0]
                    );
                }
            }
            if (logProgress && size > 0 && ((size % DAMAGE_PROFILE_PROGRESS_INTERVAL) != 0)) {
                LOGGER.info(
                        "Krakk profdamage progress: stage=apply processed={}/{} elapsed={}ms broken={} damaged={} noEffect={} skipped={}",
                        size,
                        size,
                        String.format("%.3f", nanosToMs(System.nanoTime() - applyStart)),
                        broken[0],
                        damaged[0],
                        noEffect[0],
                        skipped[0]
                );
            }
            applyNanos[0] = System.nanoTime() - applyStart;
        };

        Runnable postClearWork = () -> {
            long postClearStart = System.nanoTime();
            clearDamageProfileTargets(level, targets, "postClear", logProgress);
            postClearNanos[0] = System.nanoTime() - postClearStart;
        };

        if (logProgress) {
            LOGGER.info(
                    "Krakk profdamage pass mode: bulkSync={} pipeline=impactApply->visible cleanup={} dropOnBreak={} sampleStep={} maxImpact={}",
                    bulkSync,
                    cleanup,
                    dropOnBreak,
                    QPROF_DAMAGE_SAMPLE_STEP,
                    String.format("%.3f", QPROF_DAMAGE_MAX_IMPACT)
            );
        }
        preClearWork.run();
        if (bulkSync) {
            KrakkDamageRuntime.runInBulkSync(level, applyWork);
        } else {
            applyWork.run();
        }
        if (cleanup) {
            postClearWork.run();
        }

        if (logProgress) {
            LOGGER.info(
                    "Krakk profdamage pass complete: preClear={}ms apply={}ms postClear={}ms attempted={} broken={} damaged={} noEffect={} skipped={}",
                    String.format("%.3f", nanosToMs(preClearNanos[0])),
                    String.format("%.3f", nanosToMs(applyNanos[0])),
                    String.format("%.3f", nanosToMs(postClearNanos[0])),
                    size,
                    broken[0],
                    damaged[0],
                    noEffect[0],
                    skipped[0]
            );
        }

        return new DamageProfileRunResult(
                applyNanos[0],
                size,
                broken[0],
                damaged[0],
                noEffect[0],
                skipped[0],
                preClearNanos[0],
                applyNanos[0],
                postClearNanos[0]
        );
    }

    private static void emitDamageProfileIterationNotice(CommandSourceStack source,
                                                         int requestedRuns,
                                                         int requestedWarmup,
                                                         int effectiveRuns,
                                                         int effectiveWarmup) {
        if (requestedRuns == effectiveRuns && requestedWarmup == effectiveWarmup) {
            return;
        }
        source.sendSuccess(() -> Component.literal(String.format(
                "profdamage uses live impact/break pipeline and mutates world state; forcing runs=%d warmup=%d (requested runs=%d warmup=%d)",
                effectiveRuns,
                effectiveWarmup,
                requestedRuns,
                requestedWarmup
        )), false);
    }

    private static void clearDamageProfileTargets(ServerLevel level, DamageProfileTargets targets, String stage,
                                                  boolean logProgress) {
        LongArrayList positions = targets.positions();
        long stageStart = System.nanoTime();
        if (KrakkApi.damage() instanceof KrakkDamageRuntime runtime) {
            KrakkDamageRuntime.BulkDebugProgressListener progressListener = null;
            if (logProgress) {
                progressListener = (processed, total, cleared, ignored) -> LOGGER.info(
                        "Krakk profdamage progress: stage={} processed={}/{} elapsed={}ms",
                        stage,
                        processed,
                        total,
                        String.format("%.3f", nanosToMs(System.nanoTime() - stageStart))
                );
            }
            runtime.clearDamageStatesBulk(
                    level,
                    positions,
                    0,
                    logProgress ? DAMAGE_PROFILE_PROGRESS_INTERVAL : 0,
                    progressListener
            );
            return;
        }

        BlockPos.MutableBlockPos cursor = new BlockPos.MutableBlockPos();
        for (int i = 0; i < positions.size(); i++) {
            long posLong = positions.getLong(i);
            cursor.set(BlockPos.getX(posLong), BlockPos.getY(posLong), BlockPos.getZ(posLong));
            KrakkApi.damage().clearDamage(level, cursor);
            if (logProgress && ((i + 1) % DAMAGE_PROFILE_PROGRESS_INTERVAL) == 0) {
                LOGGER.info(
                        "Krakk profdamage progress: stage={} processed={}/{} elapsed={}ms",
                        stage,
                        i + 1,
                        positions.size(),
                        String.format("%.3f", nanosToMs(System.nanoTime() - stageStart))
                );
            }
        }
    }

    private static int resolveProfileRuns(boolean apply, int runs) {
        return apply ? 1 : runs;
    }

    private static int resolveProfileWarmup(boolean apply, int warmup) {
        return apply ? 0 : warmup;
    }

    private static void emitApplyProfileIterationNotice(CommandSourceStack source,
                                                        boolean apply,
                                                        int requestedRuns,
                                                        int requestedWarmup,
                                                        int effectiveRuns,
                                                        int effectiveWarmup) {
        if (!apply) {
            return;
        }
        if (requestedRuns == effectiveRuns && requestedWarmup == effectiveWarmup) {
            return;
        }
        source.sendSuccess(() -> Component.literal(String.format(
                "apply=true mutates world state; forcing runs=%d warmup=%d (requested runs=%d warmup=%d)",
                effectiveRuns,
                effectiveWarmup,
                requestedRuns,
                requestedWarmup
        )), false);
    }

    private static ProfileAggregate profileMode(KrakkExplosionRuntime runtime,
                                                ServerLevel level,
                                                Entity sourceEntity,
                                                LivingEntity owner,
                                                Vec3 pos,
                                                KrakkExplosionProfile profile,
                                                boolean apply,
                                                int runs,
                                                int warmup,
                                                long seed) {
        for (int i = 0; i < warmup; i++) {
            runtime.profileDetonate(level, pos.x, pos.y, pos.z, sourceEntity, owner, profile, apply, seed);
        }

        long[] nanos = new long[runs];
        long sumNanos = 0L;
        long minNanos = Long.MAX_VALUE;
        long maxNanos = Long.MIN_VALUE;

        long totalInitialRays = 0L;
        long totalProcessedRays = 0L;
        long totalRaySplits = 0L;
        long totalSplitChecks = 0L;
        long totalRaySteps = 0L;
        long totalRawImpacts = 0L;
        long totalPostAaImpacts = 0L;
        long totalBlocksEvaluated = 0L;
        long totalBroken = 0L;
        long totalDamaged = 0L;
        long totalPredictedBroken = 0L;
        long totalPredictedDamaged = 0L;
        long totalTnt = 0L;
        long totalSpecial = 0L;
        long totalLowImpactSkipped = 0L;
        long totalEntityCandidates = 0L;
        long totalEntityIntersectionTests = 0L;
        long totalEntityHits = 0L;
        long totalOctreeNodeTests = 0L;
        long totalOctreeLeafVisits = 0L;
        long totalEntityAffected = 0L;
        long totalEntityDamaged = 0L;
        long totalEntityKilled = 0L;
        long totalBroadphaseNanos = 0L;
        long totalRaycastNanos = 0L;
        long totalAntialiasNanos = 0L;
        long totalBlockResolveNanos = 0L;
        long totalSplitCheckNanos = 0L;
        long totalEntitySegmentNanos = 0L;
        long totalEntityApplyNanos = 0L;
        long totalVolumetricResistanceFieldNanos = 0L;
        long totalVolumetricDirectionSetupNanos = 0L;
        long totalVolumetricPressureSolveNanos = 0L;
        long totalKrakkSolveNanos = 0L;
        long totalVolumetricTargetScanNanos = 0L;
        long totalVolumetricTargetScanPrecheckNanos = 0L;
        long totalVolumetricTargetScanBlendNanos = 0L;
        long totalVolumetricImpactApplyNanos = 0L;
        long totalVolumetricImpactApplyDirectNanos = 0L;
        long totalVolumetricImpactApplyCollapseSeedNanos = 0L;
        long totalVolumetricImpactApplyCollapseBfsNanos = 0L;
        long totalVolumetricImpactApplyCollapseApplyNanos = 0L;
        long totalVolumetricSampledVoxels = 0L;
        long totalVolumetricSampledSolids = 0L;
        long totalVolumetricTargetBlocks = 0L;
        long totalVolumetricDirectionSamples = 0L;
        long totalVolumetricRadialSteps = 0L;
        long totalKrakkSourceCells = 0L;
        long totalKrakkSweepCycles = 0L;
        long totalPackets = 0L;
        long totalBytes = 0L;

        for (int i = 0; i < runs; i++) {
            KrakkExplosionRuntime.ExplosionProfileReport report = runtime.profileDetonate(
                    level, pos.x, pos.y, pos.z, sourceEntity, owner, profile, apply, seed
            );
            nanos[i] = report.elapsedNanos();
            sumNanos += report.elapsedNanos();
            minNanos = Math.min(minNanos, report.elapsedNanos());
            maxNanos = Math.max(maxNanos, report.elapsedNanos());
            totalInitialRays += report.initialRays();
            totalProcessedRays += report.processedRays();
            totalRaySplits += report.raySplits();
            totalSplitChecks += report.splitChecks();
            totalRaySteps += report.raySteps();
            totalRawImpacts += report.rawImpactedBlocks();
            totalPostAaImpacts += report.postAaImpactedBlocks();
            totalBlocksEvaluated += report.blocksEvaluated();
            totalBroken += report.brokenBlocks();
            totalDamaged += report.damagedBlocks();
            totalPredictedBroken += report.predictedBrokenBlocks();
            totalPredictedDamaged += report.predictedDamagedBlocks();
            totalTnt += report.tntTriggered();
            totalSpecial += report.specialHandled();
            totalLowImpactSkipped += report.lowImpactSkipped();
            totalEntityCandidates += report.entityCandidates();
            totalEntityIntersectionTests += report.entityIntersectionTests();
            totalEntityHits += report.entityHits();
            totalOctreeNodeTests += report.octreeNodeTests();
            totalOctreeLeafVisits += report.octreeLeafVisits();
            totalEntityAffected += report.entityAffected();
            totalEntityDamaged += report.entityDamaged();
            totalEntityKilled += report.entityKilled();
            totalBroadphaseNanos += report.broadphaseNanos();
            totalRaycastNanos += report.raycastNanos();
            totalAntialiasNanos += report.antialiasNanos();
            totalBlockResolveNanos += report.blockResolveNanos();
            totalSplitCheckNanos += report.splitCheckNanos();
            totalEntitySegmentNanos += report.entitySegmentNanos();
            totalEntityApplyNanos += report.entityApplyNanos();
            totalVolumetricResistanceFieldNanos += report.volumetricResistanceFieldNanos();
            totalVolumetricDirectionSetupNanos += report.volumetricDirectionSetupNanos();
            totalVolumetricPressureSolveNanos += report.volumetricPressureSolveNanos();
            totalKrakkSolveNanos += report.krakkSolveNanos();
            totalVolumetricTargetScanNanos += report.volumetricTargetScanNanos();
            totalVolumetricTargetScanPrecheckNanos += report.volumetricTargetScanPrecheckNanos();
            totalVolumetricTargetScanBlendNanos += report.volumetricTargetScanBlendNanos();
            totalVolumetricImpactApplyNanos += report.volumetricImpactApplyNanos();
            totalVolumetricImpactApplyDirectNanos += report.volumetricImpactApplyDirectNanos();
            totalVolumetricImpactApplyCollapseSeedNanos += report.volumetricImpactApplyCollapseSeedNanos();
            totalVolumetricImpactApplyCollapseBfsNanos += report.volumetricImpactApplyCollapseBfsNanos();
            totalVolumetricImpactApplyCollapseApplyNanos += report.volumetricImpactApplyCollapseApplyNanos();
            totalVolumetricSampledVoxels += report.volumetricSampledVoxels();
            totalVolumetricSampledSolids += report.volumetricSampledSolids();
            totalVolumetricTargetBlocks += report.volumetricTargetBlocks();
            totalVolumetricDirectionSamples += report.volumetricDirectionSamples();
            totalVolumetricRadialSteps += report.volumetricRadialSteps();
            totalKrakkSourceCells += report.krakkSourceCells();
            totalKrakkSweepCycles += report.krakkSweepCycles();
            totalPackets += report.estimatedSyncPackets();
            totalBytes += report.estimatedSyncBytes();
        }

        long[] sorted = Arrays.copyOf(nanos, nanos.length);
        Arrays.sort(sorted);
        double avgNanos = sumNanos / (double) runs;
        double varianceNanos = 0.0D;
        for (long sampleNanos : nanos) {
            double delta = sampleNanos - avgNanos;
            varianceNanos += delta * delta;
        }
        varianceNanos /= runs;
        double stddevNanos = Math.sqrt(Math.max(0.0D, varianceNanos));
        double covPercent = avgNanos > 0.0D ? ((stddevNanos / avgNanos) * 100.0D) : 0.0D;
        long p95 = sorted[percentileIndex(sorted.length, 0.95D)];
        long p99 = sorted[percentileIndex(sorted.length, 0.99D)];
        long brokenOut = apply ? totalBroken : totalPredictedBroken;
        long damagedOut = apply ? totalDamaged : totalPredictedDamaged;

        return new ProfileAggregate(
                nanosToMs(avgNanos),
                nanosToMs(minNanos),
                nanosToMs(maxNanos),
                nanosToMs(stddevNanos),
                covPercent,
                nanosToMs(p95),
                nanosToMs(p99),
                totalInitialRays / (double) runs,
                totalProcessedRays / (double) runs,
                totalRaySplits / (double) runs,
                totalSplitChecks / (double) runs,
                totalRaySteps / (double) runs,
                totalRawImpacts / (double) runs,
                totalPostAaImpacts / (double) runs,
                totalLowImpactSkipped / (double) runs,
                totalBlocksEvaluated / (double) runs,
                brokenOut / (double) runs,
                damagedOut / (double) runs,
                totalTnt / (double) runs,
                totalSpecial / (double) runs,
                totalEntityCandidates / (double) runs,
                totalEntityIntersectionTests / (double) runs,
                totalEntityHits / (double) runs,
                totalOctreeNodeTests / (double) runs,
                totalOctreeLeafVisits / (double) runs,
                totalEntityAffected / (double) runs,
                totalEntityDamaged / (double) runs,
                totalEntityKilled / (double) runs,
                nanosToMs(totalBroadphaseNanos / (double) runs),
                nanosToMs(totalRaycastNanos / (double) runs),
                nanosToMs(totalAntialiasNanos / (double) runs),
                nanosToMs(totalBlockResolveNanos / (double) runs),
                nanosToMs(totalSplitCheckNanos / (double) runs),
                nanosToMs(totalEntitySegmentNanos / (double) runs),
                nanosToMs(totalEntityApplyNanos / (double) runs),
                nanosToMs(totalVolumetricResistanceFieldNanos / (double) runs),
                nanosToMs(totalVolumetricDirectionSetupNanos / (double) runs),
                nanosToMs(totalVolumetricPressureSolveNanos / (double) runs),
                nanosToMs(totalKrakkSolveNanos / (double) runs),
                nanosToMs(totalVolumetricTargetScanNanos / (double) runs),
                nanosToMs(totalVolumetricTargetScanPrecheckNanos / (double) runs),
                nanosToMs(totalVolumetricTargetScanBlendNanos / (double) runs),
                nanosToMs(totalVolumetricImpactApplyNanos / (double) runs),
                nanosToMs(totalVolumetricImpactApplyDirectNanos / (double) runs),
                nanosToMs(totalVolumetricImpactApplyCollapseSeedNanos / (double) runs),
                nanosToMs(totalVolumetricImpactApplyCollapseBfsNanos / (double) runs),
                nanosToMs(totalVolumetricImpactApplyCollapseApplyNanos / (double) runs),
                totalVolumetricSampledVoxels / (double) runs,
                totalVolumetricSampledSolids / (double) runs,
                totalVolumetricTargetBlocks / (double) runs,
                totalVolumetricDirectionSamples / (double) runs,
                totalVolumetricRadialSteps / (double) runs,
                totalKrakkSourceCells / (double) runs,
                totalKrakkSweepCycles / (double) runs,
                totalPackets / (double) runs,
                totalBytes / (double) runs
        );
    }

    private static void emitProfileAggregate(CommandSourceStack source,
                                             String commandLabel,
                                             boolean apply,
                                             int runs,
                                             int warmup,
                                             long seed,
                                             ProfileAggregate aggregate) {
        String testName = KrakkExplosionRuntime.getProfilerTestName();
        source.sendSuccess(() -> Component.literal(String.format(
                "Krakk %s: test=%s apply=%s runs=%d warmup=%d seed=%d avg=%.3fms stddev=%.3fms cov=%.2f%% p95=%.3fms p99=%.3fms min=%.3fms max=%.3fms",
                commandLabel,
                testName,
                apply,
                runs,
                warmup,
                seed,
                aggregate.avgMs,
                aggregate.stddevMs,
                aggregate.covPercent,
                aggregate.p95Ms,
                aggregate.p99Ms,
                aggregate.minMs,
                aggregate.maxMs
        )), false);
        if (runs < 5 || warmup < 1) {
            source.sendSuccess(() -> Component.literal(
                    "profileQuality: low confidence; recommended runs>=7 warmup>=2 for parity/perf comparisons."
            ), false);
        }
        source.sendSuccess(() -> Component.literal(String.format(
                "avgMetrics: rays(initial=%.1f processed=%.1f splits=%.1f checks=%.1f steps=%.1f) impacts(raw=%.1f postAA=%.1f lowSkip=%.1f) blocks(eval=%.1f broken=%.1f damaged=%.1f tnt=%.1f special=%.1f) entities(candidates=%.1f tests=%.1f hits=%.1f octree(nodes=%.1f leaves=%.1f) affected=%.1f damaged=%.1f killed=%.1f) syncEst(packets=%.1f bytes=%.1f)",
                aggregate.avgInitialRays,
                aggregate.avgProcessedRays,
                aggregate.avgRaySplits,
                aggregate.avgSplitChecks,
                aggregate.avgRaySteps,
                aggregate.avgRawImpacts,
                aggregate.avgPostAaImpacts,
                aggregate.avgLowImpactSkipped,
                aggregate.avgBlocksEvaluated,
                aggregate.avgBroken,
                aggregate.avgDamaged,
                aggregate.avgTnt,
                aggregate.avgSpecial,
                aggregate.avgEntityCandidates,
                aggregate.avgEntityIntersectionTests,
                aggregate.avgEntityHits,
                aggregate.avgOctreeNodeTests,
                aggregate.avgOctreeLeafVisits,
                aggregate.avgEntityAffected,
                aggregate.avgEntityDamaged,
                aggregate.avgEntityKilled,
                aggregate.avgPackets,
                aggregate.avgBytes
        )), false);
        source.sendSuccess(() -> Component.literal(String.format(
                "avgStages: broadphase=%.3fms raycast=%.3fms antialias=%.3fms blockResolve=%.3fms splitChecks=%.3fms entitySegment=%.3fms entityApply=%.3fms",
                aggregate.avgBroadphaseMs,
                aggregate.avgRaycastMs,
                aggregate.avgAntialiasMs,
                aggregate.avgBlockResolveMs,
                aggregate.avgSplitCheckMs,
                aggregate.avgEntitySegmentMs,
                aggregate.avgEntityApplyMs
        )), false);
        source.sendSuccess(() -> Component.literal(String.format(
                "avgVolumetric: resistanceField=%.3fms directionSetup=%.3fms pressureSolve=%.3fms targetScan=%.3fms (precheck=%.3fms blend=%.3fms) impactApply=%.3fms (direct=%.3fms collapseSeed=%.3fms collapseBfs=%.3fms collapseApply=%.3fms) sampledVoxels=%.1f sampledSolids=%.1f targets=%.1f directions=%.1f radialSteps=%.1f",
                aggregate.avgVolumetricResistanceFieldMs,
                aggregate.avgVolumetricDirectionSetupMs,
                aggregate.avgVolumetricPressureSolveMs,
                aggregate.avgVolumetricTargetScanMs,
                aggregate.avgVolumetricTargetScanPrecheckMs,
                aggregate.avgVolumetricTargetScanBlendMs,
                aggregate.avgVolumetricImpactApplyMs,
                aggregate.avgVolumetricImpactApplyDirectMs,
                aggregate.avgVolumetricImpactApplyCollapseSeedMs,
                aggregate.avgVolumetricImpactApplyCollapseBfsMs,
                aggregate.avgVolumetricImpactApplyCollapseApplyMs,
                aggregate.avgVolumetricSampledVoxels,
                aggregate.avgVolumetricSampledSolids,
                aggregate.avgVolumetricTargetBlocks,
                aggregate.avgVolumetricDirectionSamples,
                aggregate.avgVolumetricRadialSteps
        )), false);
        if (aggregate.avgKrakkSolveMs > 0.0D || aggregate.avgKrakkSourceCells > 0.0D || aggregate.avgKrakkSweepCycles > 0.0D) {
            source.sendSuccess(() -> Component.literal(String.format(
                    "avgKrakk: solve=%.3fms sourceCells=%.1f sweepCycles=%.1f",
                    aggregate.avgKrakkSolveMs,
                    aggregate.avgKrakkSourceCells,
                    aggregate.avgKrakkSweepCycles
            )), false);
        }
    }

    private record ProfileAggregate(
            double avgMs,
            double minMs,
            double maxMs,
            double stddevMs,
            double covPercent,
            double p95Ms,
            double p99Ms,
            double avgInitialRays,
            double avgProcessedRays,
            double avgRaySplits,
            double avgSplitChecks,
            double avgRaySteps,
            double avgRawImpacts,
            double avgPostAaImpacts,
            double avgLowImpactSkipped,
            double avgBlocksEvaluated,
            double avgBroken,
            double avgDamaged,
            double avgTnt,
            double avgSpecial,
            double avgEntityCandidates,
            double avgEntityIntersectionTests,
            double avgEntityHits,
            double avgOctreeNodeTests,
            double avgOctreeLeafVisits,
            double avgEntityAffected,
            double avgEntityDamaged,
            double avgEntityKilled,
            double avgBroadphaseMs,
            double avgRaycastMs,
            double avgAntialiasMs,
            double avgBlockResolveMs,
            double avgSplitCheckMs,
            double avgEntitySegmentMs,
            double avgEntityApplyMs,
            double avgVolumetricResistanceFieldMs,
            double avgVolumetricDirectionSetupMs,
            double avgVolumetricPressureSolveMs,
            double avgKrakkSolveMs,
            double avgVolumetricTargetScanMs,
            double avgVolumetricTargetScanPrecheckMs,
            double avgVolumetricTargetScanBlendMs,
            double avgVolumetricImpactApplyMs,
            double avgVolumetricImpactApplyDirectMs,
            double avgVolumetricImpactApplyCollapseSeedMs,
            double avgVolumetricImpactApplyCollapseBfsMs,
            double avgVolumetricImpactApplyCollapseApplyMs,
            double avgVolumetricSampledVoxels,
            double avgVolumetricSampledSolids,
            double avgVolumetricTargetBlocks,
            double avgVolumetricDirectionSamples,
            double avgVolumetricRadialSteps,
            double avgKrakkSourceCells,
            double avgKrakkSweepCycles,
            double avgPackets,
            double avgBytes
    ) {
    }

    private record DamageProfileTargets(LongArrayList positions, DoubleArrayList impactPowers) {
    }

    private record DamageProfileRunResult(long elapsedNanos, int attempted, int broken, int damaged, int noEffect, int skipped,
                                          long preClearNanos, long applyNanos, long postClearNanos) {
    }

    private static int percentileIndex(int length, double percentile) {
        if (length <= 1) {
            return 0;
        }
        int idx = (int) Math.ceil(percentile * length) - 1;
        return Math.max(0, Math.min(length - 1, idx));
    }

    private static double nanosToMs(double nanos) {
        return nanos / 1_000_000.0D;
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
