package org.shipwrights.krakk.command;

import com.mojang.brigadier.arguments.BoolArgumentType;
import com.mojang.brigadier.arguments.DoubleArgumentType;
import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.mojang.brigadier.arguments.LongArgumentType;
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
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.phys.Vec3;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.explosion.KrakkExplosionProfile;
import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkAccess;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageChunkStorage;
import org.shipwrights.krakk.runtime.explosion.KrakkExplosionRuntime;

import java.util.Arrays;

public final class KrakkDebugCommands {
    private static final int DEFAULT_MAX_POWER = 1_000_000;
    private static final int DEFAULT_DECAY_TICKS = 24_000;
    private static final int DEFAULT_PROFILE_RUNS = 20;
    private static final int DEFAULT_PROFILE_WARMUP = 3;
    private static final double QPROF_EIKONAL_RADIUS = 96.0D;
    private static final double QPROF_EIKONAL_ENERGY = 1_000_000.0D;
    private static final int QPROF_RUNS = 1;
    private static final int QPROF_WARMUP = 0;
    private static final long QPROF_SEED = 12_345L;

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
                                        .then(Commands.argument("power", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                .executes(context -> explode(
                                                        context.getSource(),
                                                        Vec3Argument.getVec3(context, "pos"),
                                                        DoubleArgumentType.getDouble(context, "power"))))))
                        .then(Commands.literal("volexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(1.0D, 96.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> volumetricExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy")))))))
                        .then(Commands.literal("eikexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(0.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> eikonalExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy")))))))
                        .then(Commands.literal("profexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("power", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                .executes(context -> profExplode(
                                                        context.getSource(),
                                                        Vec3Argument.getVec3(context, "pos"),
                                                        DoubleArgumentType.getDouble(context, "power"),
                                                        DEFAULT_PROFILE_RUNS,
                                                        DEFAULT_PROFILE_WARMUP,
                                                        Long.MIN_VALUE,
                                                        false))
                                                .then(Commands.argument("runs", IntegerArgumentType.integer(1, 200))
                                                        .executes(context -> profExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "power"),
                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                DEFAULT_PROFILE_WARMUP,
                                                                Long.MIN_VALUE,
                                                                false))
                                                        .then(Commands.argument("warmup", IntegerArgumentType.integer(0, 200))
                                                                .executes(context -> profExplode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        DoubleArgumentType.getDouble(context, "power"),
                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                        Long.MIN_VALUE,
                                                                        false))
                                                                .then(Commands.argument("seed", LongArgumentType.longArg())
                                                                        .executes(context -> profExplode(
                                                                                context.getSource(),
                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                DoubleArgumentType.getDouble(context, "power"),
                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                LongArgumentType.getLong(context, "seed"),
                                                                                false))
                                                                        .then(Commands.argument("apply", BoolArgumentType.bool())
                                                                                .executes(context -> profExplode(
                                                                                        context.getSource(),
                                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                                        DoubleArgumentType.getDouble(context, "power"),
                                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                                        LongArgumentType.getLong(context, "seed"),
                                                                                        BoolArgumentType.getBool(context, "apply"))))))))))
                        .then(Commands.literal("profvolexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(1.0D, 96.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> profVolumetricExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                DEFAULT_PROFILE_RUNS,
                                                                DEFAULT_PROFILE_WARMUP,
                                                                Long.MIN_VALUE,
                                                                false))
                                                        .then(Commands.argument("runs", IntegerArgumentType.integer(1, 200))
                                                                .executes(context -> profVolumetricExplode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                        DEFAULT_PROFILE_WARMUP,
                                                                        Long.MIN_VALUE,
                                                                        false))
                                                                .then(Commands.argument("warmup", IntegerArgumentType.integer(0, 200))
                                                                        .executes(context -> profVolumetricExplode(
                                                                                context.getSource(),
                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                Long.MIN_VALUE,
                                                                                false))
                                                                        .then(Commands.argument("seed", LongArgumentType.longArg())
                                                                                .executes(context -> profVolumetricExplode(
                                                                                        context.getSource(),
                                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                                        LongArgumentType.getLong(context, "seed"),
                                                                                        false))
                                                                                .then(Commands.argument("apply", BoolArgumentType.bool())
                                                                                        .executes(context -> profVolumetricExplode(
                                                                                                context.getSource(),
                                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                                LongArgumentType.getLong(context, "seed"),
                                                                                                BoolArgumentType.getBool(context, "apply")))))))))))
                        .then(Commands.literal("profeikexplode")
                                .then(Commands.argument("pos", Vec3Argument.vec3())
                                        .then(Commands.argument("radius", DoubleArgumentType.doubleArg(0.0D))
                                                .then(Commands.argument("energy", DoubleArgumentType.doubleArg(0.1D, DEFAULT_MAX_POWER))
                                                        .executes(context -> profEikonalExplode(
                                                                context.getSource(),
                                                                Vec3Argument.getVec3(context, "pos"),
                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                DEFAULT_PROFILE_RUNS,
                                                                DEFAULT_PROFILE_WARMUP,
                                                                Long.MIN_VALUE,
                                                                false))
                                                        .then(Commands.argument("runs", IntegerArgumentType.integer(1, 200))
                                                                .executes(context -> profEikonalExplode(
                                                                        context.getSource(),
                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                        DEFAULT_PROFILE_WARMUP,
                                                                        Long.MIN_VALUE,
                                                                        false))
                                                                .then(Commands.argument("warmup", IntegerArgumentType.integer(0, 200))
                                                                        .executes(context -> profEikonalExplode(
                                                                                context.getSource(),
                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                Long.MIN_VALUE,
                                                                                false))
                                                                        .then(Commands.argument("seed", LongArgumentType.longArg())
                                                                                .executes(context -> profEikonalExplode(
                                                                                        context.getSource(),
                                                                                        Vec3Argument.getVec3(context, "pos"),
                                                                                        DoubleArgumentType.getDouble(context, "radius"),
                                                                                        DoubleArgumentType.getDouble(context, "energy"),
                                                                                        IntegerArgumentType.getInteger(context, "runs"),
                                                                                        IntegerArgumentType.getInteger(context, "warmup"),
                                                                                        LongArgumentType.getLong(context, "seed"),
                                                                                        false))
                                                                                .then(Commands.argument("apply", BoolArgumentType.bool())
                                                                                        .executes(context -> profEikonalExplode(
                                                                                                context.getSource(),
                                                                                                Vec3Argument.getVec3(context, "pos"),
                                                                                                DoubleArgumentType.getDouble(context, "radius"),
                                                                                                DoubleArgumentType.getDouble(context, "energy"),
                                                                                                IntegerArgumentType.getInteger(context, "runs"),
                                                                                                IntegerArgumentType.getInteger(context, "warmup"),
                                                                                                LongArgumentType.getLong(context, "seed"),
                                                                                                BoolArgumentType.getBool(context, "apply")))))))))))
                        .then(Commands.literal("qprof")
                                .executes(context -> quickProfileEikonal(context.getSource(), false))
                                .then(Commands.argument("apply", BoolArgumentType.bool())
                                        .executes(context -> quickProfileEikonal(
                                                context.getSource(),
                                                BoolArgumentType.getBool(context, "apply")))))
                        .then(Commands.literal("resistancefieldparallel")
                                .executes(context -> getResistanceFieldParallelSampling(context.getSource()))
                                .then(Commands.argument("enabled", BoolArgumentType.bool())
                                        .executes(context -> setResistanceFieldParallelSampling(
                                                context.getSource(),
                                                BoolArgumentType.getBool(context, "enabled")))))
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
                                        .then(Commands.argument("amount", IntegerArgumentType.integer(1))
                                                .executes(context -> damageBlock(
                                                        context.getSource(),
                                                        BlockPosArgument.getBlockPos(context, "pos"),
                                                        IntegerArgumentType.getInteger(context, "amount"))))))
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

    private static int explode(CommandSourceStack source, Vec3 pos, double power) {
        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        double derivedRadius = KrakkExplosionCurves.computeBlastRadius(power);
        KrakkApi.explosions().detonate(
                level,
                pos.x,
                pos.y,
                pos.z,
                sourceEntity,
                owner,
                new KrakkExplosionProfile(power)
        );

        source.sendSuccess(() -> Component.literal(String.format(
                "Triggered Krakk explosion at %.2f %.2f %.2f (power=%.2f, derivedRadius=%.2f)",
                pos.x, pos.y, pos.z, power, derivedRadius
        )), true);
        return 1;
    }

    private static int volumetricExplode(CommandSourceStack source, Vec3 pos, double radius, double energy) {
        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        KrakkApi.explosions().detonate(
                level,
                pos.x,
                pos.y,
                pos.z,
                sourceEntity,
                owner,
                KrakkExplosionProfile.volumetricDebug(radius, energy)
        );

        source.sendSuccess(() -> Component.literal(String.format(
                "Triggered Krakk volumetric explosion at %.2f %.2f %.2f (radius=%.2f, energy=%.2f)",
                pos.x, pos.y, pos.z, radius, energy
        )), true);
        return 1;
    }

    private static int eikonalExplode(CommandSourceStack source, Vec3 pos, double radius, double energy) {
        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        KrakkApi.explosions().detonate(
                level,
                pos.x,
                pos.y,
                pos.z,
                sourceEntity,
                owner,
                KrakkExplosionProfile.eikonalDebug(radius, energy)
        );

        source.sendSuccess(() -> Component.literal(String.format(
                "Triggered Krakk eikonal explosion at %.2f %.2f %.2f (radius=%.2f, energy=%.2f)",
                pos.x, pos.y, pos.z, radius, energy
        )), true);
        return 1;
    }

    private static int getResistanceFieldParallelSampling(CommandSourceStack source) {
        boolean enabled = KrakkExplosionRuntime.isParallelResistanceFieldSamplingEnabled();
        source.sendSuccess(() -> Component.literal(
                "Resistance-field off-thread sampling is " + (enabled ? "enabled" : "disabled")
        ), false);
        return 1;
    }

    private static int setResistanceFieldParallelSampling(CommandSourceStack source, boolean enabled) {
        boolean resolved = KrakkExplosionRuntime.setParallelResistanceFieldSamplingEnabled(enabled);
        source.sendSuccess(() -> Component.literal(
                "Set resistance-field off-thread sampling to " + (resolved ? "enabled" : "disabled")
        ), true);
        return 1;
    }

    private static int profExplode(CommandSourceStack source, Vec3 pos, double power,
                                   int runs, int warmup, long seed, boolean apply) {
        if (!(KrakkApi.explosions() instanceof KrakkExplosionRuntime runtime)) {
            source.sendFailure(Component.literal("Krakk explosion runtime does not support profiling."));
            return 0;
        }

        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        KrakkExplosionProfile profile = new KrakkExplosionProfile(power);
        long effectiveSeed = seed != Long.MIN_VALUE ? seed : (level.getGameTime() ^ BlockPos.containing(pos).asLong());

        ProfileAggregate aggregate = profileMode(
                runtime, level, sourceEntity, owner, pos, profile, apply, runs, warmup, effectiveSeed
        );
        emitProfileAggregate(source, "profexplode", apply, runs, warmup, effectiveSeed, aggregate);
        return runs;
    }

    private static int profVolumetricExplode(CommandSourceStack source, Vec3 pos, double radius, double energy,
                                             int runs, int warmup, long seed, boolean apply) {
        if (!(KrakkApi.explosions() instanceof KrakkExplosionRuntime runtime)) {
            source.sendFailure(Component.literal("Krakk explosion runtime does not support profiling."));
            return 0;
        }

        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        KrakkExplosionProfile profile = KrakkExplosionProfile.volumetric(radius, energy);
        long effectiveSeed = seed != Long.MIN_VALUE ? seed : (level.getGameTime() ^ BlockPos.containing(pos).asLong());

        ProfileAggregate aggregate = profileMode(
                runtime, level, sourceEntity, owner, pos, profile, apply, runs, warmup, effectiveSeed
        );
        emitProfileAggregate(source, "profvolexplode", apply, runs, warmup, effectiveSeed, aggregate);
        return runs;
    }

    private static int profEikonalExplode(CommandSourceStack source, Vec3 pos, double radius, double energy,
                                          int runs, int warmup, long seed, boolean apply) {
        if (!(KrakkApi.explosions() instanceof KrakkExplosionRuntime runtime)) {
            source.sendFailure(Component.literal("Krakk explosion runtime does not support profiling."));
            return 0;
        }

        ServerLevel level = source.getLevel();
        Entity sourceEntity = source.getEntity();
        LivingEntity owner = sourceEntity instanceof LivingEntity living ? living : null;
        KrakkExplosionProfile profile = KrakkExplosionProfile.eikonal(radius, energy);
        long effectiveSeed = seed != Long.MIN_VALUE ? seed : (level.getGameTime() ^ BlockPos.containing(pos).asLong());

        ProfileAggregate aggregate = profileMode(
                runtime, level, sourceEntity, owner, pos, profile, apply, runs, warmup, effectiveSeed
        );
        emitProfileAggregate(source, "profeikexplode", apply, runs, warmup, effectiveSeed, aggregate);
        return runs;
    }

    private static int quickProfileEikonal(CommandSourceStack source, boolean apply) {
        return profEikonalExplode(
                source,
                source.getPosition(),
                QPROF_EIKONAL_RADIUS,
                QPROF_EIKONAL_ENERGY,
                QPROF_RUNS,
                QPROF_WARMUP,
                QPROF_SEED,
                apply
        );
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
        long totalEikonalSolveNanos = 0L;
        long totalVolumetricTargetScanNanos = 0L;
        long totalVolumetricTargetScanPrecheckNanos = 0L;
        long totalVolumetricTargetScanBlendNanos = 0L;
        long totalVolumetricImpactApplyNanos = 0L;
        long totalVolumetricSampledVoxels = 0L;
        long totalVolumetricSampledSolids = 0L;
        long totalVolumetricTargetBlocks = 0L;
        long totalVolumetricDirectionSamples = 0L;
        long totalVolumetricRadialSteps = 0L;
        long totalEikonalSourceCells = 0L;
        long totalEikonalSweepCycles = 0L;
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
            totalEikonalSolveNanos += report.eikonalSolveNanos();
            totalVolumetricTargetScanNanos += report.volumetricTargetScanNanos();
            totalVolumetricTargetScanPrecheckNanos += report.volumetricTargetScanPrecheckNanos();
            totalVolumetricTargetScanBlendNanos += report.volumetricTargetScanBlendNanos();
            totalVolumetricImpactApplyNanos += report.volumetricImpactApplyNanos();
            totalVolumetricSampledVoxels += report.volumetricSampledVoxels();
            totalVolumetricSampledSolids += report.volumetricSampledSolids();
            totalVolumetricTargetBlocks += report.volumetricTargetBlocks();
            totalVolumetricDirectionSamples += report.volumetricDirectionSamples();
            totalVolumetricRadialSteps += report.volumetricRadialSteps();
            totalEikonalSourceCells += report.eikonalSourceCells();
            totalEikonalSweepCycles += report.eikonalSweepCycles();
            totalPackets += report.estimatedSyncPackets();
            totalBytes += report.estimatedSyncBytes();
        }

        long[] sorted = Arrays.copyOf(nanos, nanos.length);
        Arrays.sort(sorted);
        long p95 = sorted[percentileIndex(sorted.length, 0.95D)];
        long p99 = sorted[percentileIndex(sorted.length, 0.99D)];
        long brokenOut = apply ? totalBroken : totalPredictedBroken;
        long damagedOut = apply ? totalDamaged : totalPredictedDamaged;

        return new ProfileAggregate(
                nanosToMs(sumNanos / (double) runs),
                nanosToMs(minNanos),
                nanosToMs(maxNanos),
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
                nanosToMs(totalEikonalSolveNanos / (double) runs),
                nanosToMs(totalVolumetricTargetScanNanos / (double) runs),
                nanosToMs(totalVolumetricTargetScanPrecheckNanos / (double) runs),
                nanosToMs(totalVolumetricTargetScanBlendNanos / (double) runs),
                nanosToMs(totalVolumetricImpactApplyNanos / (double) runs),
                totalVolumetricSampledVoxels / (double) runs,
                totalVolumetricSampledSolids / (double) runs,
                totalVolumetricTargetBlocks / (double) runs,
                totalVolumetricDirectionSamples / (double) runs,
                totalVolumetricRadialSteps / (double) runs,
                totalEikonalSourceCells / (double) runs,
                totalEikonalSweepCycles / (double) runs,
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
        source.sendSuccess(() -> Component.literal(String.format(
                "Krakk %s: apply=%s runs=%d warmup=%d seed=%d avg=%.3fms p95=%.3fms p99=%.3fms min=%.3fms max=%.3fms",
                commandLabel,
                apply,
                runs,
                warmup,
                seed,
                aggregate.avgMs,
                aggregate.p95Ms,
                aggregate.p99Ms,
                aggregate.minMs,
                aggregate.maxMs
        )), false);
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
                "avgVolumetric: resistanceField=%.3fms directionSetup=%.3fms pressureSolve=%.3fms targetScan=%.3fms (precheck=%.3fms blend=%.3fms) impactApply=%.3fms sampledVoxels=%.1f sampledSolids=%.1f targets=%.1f directions=%.1f radialSteps=%.1f",
                aggregate.avgVolumetricResistanceFieldMs,
                aggregate.avgVolumetricDirectionSetupMs,
                aggregate.avgVolumetricPressureSolveMs,
                aggregate.avgVolumetricTargetScanMs,
                aggregate.avgVolumetricTargetScanPrecheckMs,
                aggregate.avgVolumetricTargetScanBlendMs,
                aggregate.avgVolumetricImpactApplyMs,
                aggregate.avgVolumetricSampledVoxels,
                aggregate.avgVolumetricSampledSolids,
                aggregate.avgVolumetricTargetBlocks,
                aggregate.avgVolumetricDirectionSamples,
                aggregate.avgVolumetricRadialSteps
        )), false);
        if (aggregate.avgEikonalSolveMs > 0.0D || aggregate.avgEikonalSourceCells > 0.0D || aggregate.avgEikonalSweepCycles > 0.0D) {
            source.sendSuccess(() -> Component.literal(String.format(
                    "avgEikonal: solve=%.3fms sourceCells=%.1f sweepCycles=%.1f",
                    aggregate.avgEikonalSolveMs,
                    aggregate.avgEikonalSourceCells,
                    aggregate.avgEikonalSweepCycles
            )), false);
        }
    }

    private record ProfileAggregate(
            double avgMs,
            double minMs,
            double maxMs,
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
            double avgEikonalSolveMs,
            double avgVolumetricTargetScanMs,
            double avgVolumetricTargetScanPrecheckMs,
            double avgVolumetricTargetScanBlendMs,
            double avgVolumetricImpactApplyMs,
            double avgVolumetricSampledVoxels,
            double avgVolumetricSampledSolids,
            double avgVolumetricTargetBlocks,
            double avgVolumetricDirectionSamples,
            double avgVolumetricRadialSteps,
            double avgEikonalSourceCells,
            double avgEikonalSweepCycles,
            double avgPackets,
            double avgBytes
    ) {
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

    private static int damageBlock(CommandSourceStack source, BlockPos pos, int amount) {
        ServerLevel level = source.getLevel();
        int current = KrakkApi.damage().getDamageState(level, pos);
        int next = Math.min(KrakkApi.damage().getMaxDamageState(), current + amount);
        boolean changed = KrakkApi.damage().setDamageStateForDebug(level, pos, next);
        if (!changed) {
            source.sendFailure(Component.literal("Unable to damage target block."));
            return 0;
        }

        source.sendSuccess(() -> Component.literal(String.format(
                "Damaged block at %d %d %d by %d (from %d to %d)",
                pos.getX(), pos.getY(), pos.getZ(), amount, current, next
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

    private static int overlayRefresh(CommandSourceStack source, ServerPlayer explicitTarget) {
        return syncLoadedChunks(source, explicitTarget);
    }

    private static int overlayClear(CommandSourceStack source, ServerPlayer explicitTarget) {
        ServerPlayer target = resolveTargetPlayer(source, explicitTarget);
        if (target == null) {
            return 0;
        }

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
