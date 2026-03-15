package org.shipwrights.krakk.runtime.explosion;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.core.BlockPos;
import net.minecraft.core.SectionPos;
import net.minecraft.core.particles.ParticleTypes;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.sounds.SoundEvents;
import net.minecraft.sounds.SoundSource;
import net.minecraft.util.Mth;
import net.minecraft.util.RandomSource;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.entity.ai.attributes.Attributes;
import net.minecraft.world.damagesource.DamageSource;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.LiquidBlock;
import net.minecraft.world.level.block.TntBlock;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.phys.AABB;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkDamageType;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.api.explosion.KrakkExplosionApi;
import org.shipwrights.krakk.api.explosion.KrakkExplosionProfile;
import org.shipwrights.krakk.engine.damage.KrakkDamageCurves;
import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;
import org.shipwrights.krakk.engine.explosion.KrakkRaySplitMath;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public final class KrakkExplosionRuntime implements KrakkExplosionApi {
    private static final int MIN_RAY_COUNT = 640;
    private static final int MAX_RAY_COUNT = 2048;
    private static final double DEFAULT_RAY_COUNT = 960.0D;
    private static final double GOLDEN_ANGLE = Math.PI * (3.0D - Math.sqrt(5.0D));
    private static final double INITIAL_RAY_VARIANCE_RADIANS = Math.toRadians(1.0D);
    private static final double RAY_STEP = 0.3D; // retained for parity tuning constants
    private static final double RAY_DECAY_PER_STEP = RAY_STEP * 0.45D;
    private static final double RAY_DECAY_PER_UNIT = RAY_DECAY_PER_STEP / RAY_STEP;
    private static final double FLUID_RAY_DAMPING = 0.90D;
    private static final double BLOCK_RESISTANCE_DAMPING = 0.17D;
    private static final double BLOCK_BASE_DAMPING = 0.08D;
    private static final double RAY_IMPACT_SCALE = 0.0012D;
    private static final double STONE_EQUIVALENT_FLUID_EXPLOSION_RESISTANCE = 6.0D;
    private static final float STONE_EQUIVALENT_FLUID_HARDNESS = 1.5F;
    private static final int FLUID_REMOVE_DAMAGE_THRESHOLD = 9;
    private static final double DEFAULT_RAY_SPLIT_DISTANCE_THRESHOLD = 0.5D;
    private static final double MIN_RAY_SPLIT_DISTANCE_THRESHOLD = 0.05D;
    private static final double MAX_RAY_SPLIT_DISTANCE_THRESHOLD = 64.0D;
    private static final double RAY_SPLIT_HYSTERESIS_RATIO = 0.06D;
    private static final double RAY_SPLIT_HALF_ANGLE_RADIANS = Math.toRadians(30.0D);
    private static final double RAY_SPLIT_VARIANCE_RADIANS = Math.toRadians(5.0D);
    private static final int RAY_SPLIT_CHILD_COUNT = 3;
    private static final int MAX_RAY_SPLIT_DEPTH = 8;
    private static final double MIN_RAY_SPLIT_ENERGY = 0.35D;
    private static final double RAY_SPLIT_MIN_TRAVEL_AFTER_SPLIT = 0.35D;
    private static final double SPLIT_PAYOFF_IMPACT_FLOOR = KrakkDamageCurves.MIN_IMPACT_FOR_ONE_DAMAGE_STATE;
    private static final double MIN_RESOLVED_RAY_IMPACT = 1.0E-4D;
    private static final double ENTITY_QUERY_MARGIN = 0.75D;
    private static final double ENTITY_HITBOX_INFLATE = 0.05D;
    private static final double ENTITY_MACRO_CELL_SIZE = 4.0D;
    private static final double ENTITY_HIT_EPSILON_DISTANCE = 1.0E-4D;
    private static final double ENTITY_BASE_RAY_DAMPING = 0.10D;
    private static final double ENTITY_ARMOR_RAY_DAMPING = 0.02D;
    private static final double ENTITY_TOUGHNESS_RAY_DAMPING = 0.015D;
    // Calibrated so a full-health 400 HP entity (e.g., Warden) saps roughly obsidian-equivalent ray energy.
    private static final double ENTITY_HEALTH_OBSIDIAN_EQUIVALENT = 400.0D;
    private static final double ENTITY_HEALTH_OBSIDIAN_EQUIVALENT_DAMPING = computeResistanceCostFromExplosionResistance(1200.0D);
    private static final double ENTITY_IMPACT_SCALE = 0.08D;
    private static final double ENTITY_MIN_APPLIED_IMPACT = 1.0E-4D;
    private static final double ENTITY_HP_STRENGTH_BASE = 0.60D;
    private static final double ENTITY_HP_STRENGTH_PER_HP = 0.08D;
    private static final double ENTITY_ARMOR_DAMAGE_DIVISOR = 0.06D;
    private static final double ENTITY_TOUGHNESS_DAMAGE_DIVISOR = 0.03D;
    private static final double ENTITY_KNOCKBACK_SCALE = 0.18D;
    private static final double ENTITY_KNOCKBACK_MAX = 6.0D;
    private static final double ENTITY_KNOCKBACK_UPWARD_BIAS = 0.12D;
    private static final int ENTITY_SEGMENT_CHECK_INTERVAL = 2;
    private static final double RAY_AA_SPREAD_SHARE = 0.16D;
    private static final double RAY_AA_MIN_IMPACT = 1.0E-3D;
    private static final double RAY_AA_FRONTIER_IMPACT_DELTA = 2.5E-3D;
    private static final double RAY_SMOOTHING_FRACTION = 0.72D;
    private static final double RAY_SMOOTHING_MIN_RADIUS = 0.75D;
    private static final double RAY_SMOOTHING_MAX_RADIUS = 2.40D;
    private static final double RAY_SMOOTHING_RANGE2_THRESHOLD = 0.95D;
    private static final double RAY_SMOOTHING_RANGE3_THRESHOLD = 1.75D;
    private static final double RAY_SMOKE_HEIGHT_OFFSET = 0.05D;
    private static final double RAY_SMOKE_JITTER = 0.06D;
    private static final int RAY_SMOKE_STEP_INTERVAL = 2;
    private static final double RAY_SMOKE_RAY_FRACTION = 0.22D;
    private static final int RAY_SMOKE_MIN_RAYS = 48;
    private static final int RAY_SMOKE_MAX_RAYS = 320;
    private static final double RAY_SMOKE_BUDGET_PER_RADIUS = 240.0D;
    private static final int RAY_SMOKE_MIN_BUDGET = 320;
    private static final int RAY_SMOKE_MAX_BUDGET = 2400;
    private static final double IMPACT_DIFFUSION_SHARE = 0.18D;
    private static final double IMPACT_DIFFUSION_RADIUS_THRESHOLD = 7.0D;
    private static final int IMPACT_DIFFUSION_MAX_PASSES = 2;
    private static final int POST_SMOOTH_PASSES = 2;
    private static final int POST_SMOOTH_RADIUS = 4; // 9x9x9 neighborhood
    private static final int POST_SMOOTH_MIN_NEIGHBORS = 1;
    private static final double POST_SMOOTH_SELF_WEIGHT = 0.35D;
    private static final double POST_SMOOTH_NEIGHBOR_WEIGHT = 1.25D;
    private static final double POST_SMOOTH_PEAK_BLEND = 0.60D;
    private static final int POST_SMOOTH_MAX_DAMAGE_STATE = 14;
    private static final int POST_EDGE_FRONTIER_RADIUS = 1; // grow one block past damaged edge each pass
    private static final int POST_EDGE_SAMPLE_RADIUS = 2;
    private static final int POST_EDGE_MIN_NEIGHBORS = 2;
    private static final double POST_EDGE_NEIGHBOR_WEIGHT = 1.0D;
    private static final double POST_EDGE_DAMAGE_SCALE = 0.42D;
    private static final double POST_EDGE_PEAK_SCALE = 0.10D;
    private static final int POST_EDGE_MAX_DAMAGE_STATE = 6;
    private static final double MAX_BLOCK_IMPACT_MULTIPLIER = 2.5D;
    private static final int VOLUMETRIC_MAX_RADIUS = 96;
    private static final double VOLUMETRIC_MIN_ENERGY = 1.0E-6D;
    private static final double VOLUMETRIC_DEFORM_SAMPLE_STEP = 0.5D;
    private static final double VOLUMETRIC_PRESSURE_AIR_DECAY_PER_BLOCK = 0.01D;
    private static final double VOLUMETRIC_PRESSURE_RESISTANCE_LOSS_SCALE = 0.04D;
    private static final double VOLUMETRIC_PRESSURE_DIFFUSION = 0.24D;
    private static final double VOLUMETRIC_PRESSURE_RECOVERY_PER_BLOCK = 0.08D;
    private static final int VOLUMETRIC_DIRECTION_NEIGHBOR_COUNT = 10;
    private static final int VOLUMETRIC_DIRECTION_BLEND_COUNT = 8;
    private static final double VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT = 1.7D;
    private static final int VOLUMETRIC_DIRECTION_LOOKUP_RESOLUTION = 64;
    private static final double VOLUMETRIC_RADIUS_SCALE_BASE = 6.0D;
    private static final double VOLUMETRIC_MAX_POWER_RADIUS_SCALE = 8.0D;
    private static final double VOLUMETRIC_MAX_POINT_RADIUS_SCALE = 8.0D;
    private static final double VOLUMETRIC_DIRECTION_SAMPLES_PER_RADIUS2 = 6.0D;
    private static final int VOLUMETRIC_MIN_DIRECTION_SAMPLES = 128;
    private static final int VOLUMETRIC_MAX_DIRECTION_SAMPLES = 3072;
    private static final int VOLUMETRIC_MAX_RADIAL_STEPS = 128;
    private static final double VOLUMETRIC_EDGE_POINT_BIAS_BLEND = 0.70D;
    private static final double VOLUMETRIC_EDGE_POINT_BIAS_EXPONENT = 2.25D;
    private static final double VOLUMETRIC_OUTER_SMOOTHING_RATIO = 0.06D;
    private static final double VOLUMETRIC_OUTER_SMOOTHING_VARIANCE = 0.010D;
    private static final double VOLUMETRIC_EDGE_SPIKE_START_FRACTION = 0.72D;
    private static final double VOLUMETRIC_EDGE_SPIKE_MAX_REDUCTION = 0.12D;
    private static final double VOLUMETRIC_EDGE_SPIKE_REDUCTION_EXPONENT = 2.6D;
    private static final double VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION = 96.0D;
    private static final double VOLUMETRIC_EDGE_SPIKE_MIN_STRENGTH = 0.08D;
    private static final double VOLUMETRIC_EDGE_SPIKE_RAMP_EXPONENT = 2.2D;
    private static final double VOLUMETRIC_OUTER_SMOOTHING_FULL_WEIGHT_FRACTION = 1.0D
            - Mth.clamp(VOLUMETRIC_OUTER_SMOOTHING_RATIO + VOLUMETRIC_OUTER_SMOOTHING_VARIANCE, 0.01D, 0.20D);
    private static final double VOLUMETRIC_RESISTANCE_FIELD_PREALLOC_SOLID_FRACTION = 0.85D;
    private static final double VOLUMETRIC_AIR_DISTRIBUTION_BLEND = 0.78D;
    private static final double VOLUMETRIC_MAX_AIR_NORMALIZATION_SCALE = 8.0D;
    private static final double VOLUMETRIC_IMPACT_POWER_PER_ENERGY = 500.0D;
    private static final double STRUCTURAL_COLLAPSE_IMPACT_WEIGHT_SCALE = 0.60D;
    private static final int STRUCTURAL_COLLAPSE_MAX_VOXELS = 24_000_000;
    private static final double KRAKK_AIR_SLOWNESS = 1.0D;
    private static final double KRAKK_SOLID_SLOWNESS_SCALE = 0.040D;
    private static final int KRAKK_BASE_SWEEP_CYCLES = 8;
    private static final int KRAKK_MAX_SWEEP_CYCLES = 24;
    private static final double KRAKK_CONVERGENCE_EPSILON = 1.0E-3D;
    private static final double KRAKK_MAX_ARRIVAL_MULTIPLIER = 1.20D;
    private static final double KRAKK_WEIGHT_EXPONENT = 1.35D;
    private static final double KRAKK_RESISTANCE_ATTENUATION_PER_OVERRUN = 3.0D;
    private static final double KRAKK_RESISTANCE_NORMALIZED_ATTENUATION = 3.5D;
    private static final double KRAKK_HARD_BLOCK_NORMALIZED_OVERRUN = 4.0D;
    private static final double KRAKK_OVERRUN_DEADZONE = 0.05D;
    private static final double KRAKK_SHADOW_SOLID_SLOWNESS_SCALE = 4.0D;
    private static final double KRAKK_SHADOW_ATTENUATION_PER_OVERRUN = 4.0D;
    private static final double KRAKK_SHADOW_NORMALIZED_ATTENUATION = 7.0D;
    private static final double KRAKK_HARD_BLOCK_SHADOW_NORMALIZED_OVERRUN = 2.5D;
    private static final double KRAKK_SHADOW_OVERRUN_DEADZONE = 0.02D;
    private static final boolean KRAKK_USE_MULTIRES_COARSE_SOLVE = true;
    private static final int KRAKK_MULTIRES_DOWNSAMPLE_FACTOR = 2;
    private static final int KRAKK_MULTIRES_FINE_REFINE_SWEEP_CYCLES = 2;
    private static final int KRAKK_MULTIRES_MIN_AXIS_FOR_COARSE = 24;
    private static final double KRAKK_TARGET_MIN_WEIGHT = 2.5E-5D;
    private static final boolean KRAKK_ENABLE_LOW_WEIGHT_STOCHASTIC_SAMPLING = true;
    private static final double KRAKK_LOW_WEIGHT_STOCHASTIC_THRESHOLD = 2.0E-4D;
    private static final double KRAKK_LOW_WEIGHT_STOCHASTIC_MIN_KEEP_PROBABILITY = 0.25D;
    private static final boolean KRAKK_ENABLE_VOLUMETRIC_BASELINE_SMOOTHING = true;
    private static final double KRAKK_MIN_TRANSMITTANCE = 1.0E-3D;
    private static final double KRAKK_ENVELOPE_TRANSMITTANCE_BLEND = 0.25D;
    private static final double KRAKK_BASELINE_SMOOTH_BLEND = 0.78D;
    private static final double KRAKK_VOLUMETRIC_MECHANICS_SELF_SMOOTH = 0.32D;
    private static final double KRAKK_CUTOFF_EDGE_START_NORMALIZED = 0.32D;
    private static final double KRAKK_CUTOFF_EDGE_CURVE_EXPONENT = 1.85D;
    private static final int VOLUMETRIC_TARGET_SCAN_PARALLELISM = Math.max(
            1,
            Math.min(8, Runtime.getRuntime().availableProcessors() - 1)
    );
    private static final int VOLUMETRIC_TARGET_SCAN_MIN_SOLIDS_FOR_PARALLEL = 262_144;
    private static final int VOLUMETRIC_TARGET_SCAN_SOLIDS_PER_TASK = 262_144;
    private static final int VOLUMETRIC_TARGET_SCAN_MAX_TASKS = 32;
    private static final int KRAKK_TARGET_SCAN_MIN_SOLIDS_FOR_PARALLEL = 131_072;
    private static final int KRAKK_TARGET_SCAN_SOLIDS_PER_TASK = 262_144;
    private static final int KRAKK_TARGET_SCAN_MAX_TASKS = 32;
    private static final int RESISTANCE_FIELD_MIN_VOXELS_FOR_PARALLEL = 262_144;
    private static final int RESISTANCE_FIELD_MIN_COLUMNS_FOR_PARALLEL = 32;
    private static final int RESISTANCE_FIELD_COLUMNS_PER_TASK = 16;
    private static final long RESISTANCE_FIELD_MIN_VOXELS_PER_TASK = 786_432L;
    private static final int RESISTANCE_FIELD_MAX_TASKS = 16;
    private static final int KRAKK_SWEEP_MIN_ROWS_FOR_PARALLEL = 144;
    private static final int KRAKK_SWEEP_ROWS_PER_TASK = 24;
    private static final int KRAKK_SWEEP_MAX_TASKS = 8;
    private static final int KRAKK_SOLVE_PARALLELISM = Math.max(
            1,
            Math.min(2, Runtime.getRuntime().availableProcessors() - 1)
    );
    private static final ConcurrentHashMap<Integer, VolumetricDirectionCache> VOLUMETRIC_DIRECTION_CACHE = new ConcurrentHashMap<>();
    private static final ForkJoinPool VOLUMETRIC_TARGET_SCAN_POOL = VOLUMETRIC_TARGET_SCAN_PARALLELISM > 1
            ? new ForkJoinPool(VOLUMETRIC_TARGET_SCAN_PARALLELISM)
            : null;
    private static final ForkJoinPool KRAKK_SOLVE_POOL = KRAKK_SOLVE_PARALLELISM > 1
            ? new ForkJoinPool(KRAKK_SOLVE_PARALLELISM)
            : null;
    private static final int[] CHILD_TRAVERSAL_OFFSETS = new int[]{0, 1, 2, 4, 3, 5, 6, 7};
    private static final int[][] CHILD_ORDER_BY_RAY_OCTANT = buildChildOrderByRayOctant();
    private static volatile SpecialBlockHandler specialBlockHandler = SpecialBlockHandler.NOOP;
    private static volatile boolean parallelResistanceFieldSamplingEnabled = true;
    private static volatile double raySplitDistanceThreshold = DEFAULT_RAY_SPLIT_DISTANCE_THRESHOLD;

    public KrakkExplosionRuntime() {
    }

    @FunctionalInterface
    public interface SpecialBlockHandler {
        SpecialBlockHandler NOOP = (level, blockPos, blockState, source, owner) -> false;

        boolean handle(ServerLevel level, BlockPos blockPos, BlockState blockState, Entity source, LivingEntity owner);
    }

    public static void setSpecialBlockHandler(SpecialBlockHandler handler) {
        specialBlockHandler = handler == null ? SpecialBlockHandler.NOOP : handler;
    }

    public static String getProfilerTestName() {
        return String.format(
                "mr%d-ref%d-rad%d-lw=%s-smooth=%s-idx=v1-frontier=v1-rfParallel=true-tsMath=v1",
                KRAKK_MULTIRES_DOWNSAMPLE_FACTOR,
                KRAKK_MULTIRES_FINE_REFINE_SWEEP_CYCLES,
                VOLUMETRIC_MAX_RADIAL_STEPS,
                KRAKK_ENABLE_LOW_WEIGHT_STOCHASTIC_SAMPLING,
                KRAKK_ENABLE_VOLUMETRIC_BASELINE_SMOOTHING
        );
    }

    public static double getRaySplitDistanceThreshold() {
        return raySplitDistanceThreshold;
    }

    public static double setRaySplitDistanceThreshold(double threshold) {
        if (!Double.isFinite(threshold)) {
            return raySplitDistanceThreshold;
        }
        double clamped = Mth.clamp(
                threshold,
                MIN_RAY_SPLIT_DISTANCE_THRESHOLD,
                MAX_RAY_SPLIT_DISTANCE_THRESHOLD
        );
        raySplitDistanceThreshold = clamped;
        return clamped;
    }

    public record ExplosionProfileReport(
            long elapsedNanos,
            boolean applied,
            long seed,
            int initialRays,
            int processedRays,
            int raySplits,
            int splitChecks,
            int raySteps,
            int rawImpactedBlocks,
            int postAaImpactedBlocks,
            int blocksEvaluated,
            int brokenBlocks,
            int damagedBlocks,
            int predictedBrokenBlocks,
            int predictedDamagedBlocks,
            int tntTriggered,
            int specialHandled,
            int lowImpactSkipped,
            int entityCandidates,
            int entityIntersectionTests,
            int entityHits,
            int octreeNodeTests,
            int octreeLeafVisits,
            int entityAffected,
            int entityDamaged,
            int entityKilled,
            long broadphaseNanos,
            long raycastNanos,
            long antialiasNanos,
            long blockResolveNanos,
            long splitCheckNanos,
            long entitySegmentNanos,
            long entityApplyNanos,
            long volumetricResistanceFieldNanos,
            long volumetricDirectionSetupNanos,
            long volumetricPressureSolveNanos,
            long krakkSolveNanos,
            long volumetricTargetScanNanos,
            long volumetricTargetScanPrecheckNanos,
            long volumetricTargetScanBlendNanos,
            long volumetricImpactApplyNanos,
            long volumetricImpactApplyDirectNanos,
            long volumetricImpactApplyCollapseSeedNanos,
            long volumetricImpactApplyCollapseBfsNanos,
            long volumetricImpactApplyCollapseApplyNanos,
            int volumetricSampledVoxels,
            int volumetricSampledSolids,
            int volumetricTargetBlocks,
            int volumetricDirectionSamples,
            int volumetricRadialSteps,
            int krakkSourceCells,
            int krakkSweepCycles,
            int estimatedSyncPackets,
            int estimatedSyncBytes,
            int smokeParticles
    ) {
    }

    private static void detonateRaycast(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner,
                                        double blastRadius, double blastPower, RandomSource random,
                                        boolean applyWorldChanges, boolean emitEffects,
                                        ExplosionProfileTrace trace) {
        if (blastRadius <= 1.0E-9D || blastPower <= 1.0E-9D) {
            return;
        }

        if (emitEffects) {
            emitExplosionEffects(level, x, y, z, random);
        }

        long broadphaseStart = trace != null ? System.nanoTime() : 0L;
        ExplosionEntityContext entityContext = buildExplosionEntityContext(level, x, y, z, blastRadius, trace);
        if (trace != null) {
            trace.broadphaseNanos += (System.nanoTime() - broadphaseStart);
        }
        SectionImpactAccumulator blockImpacts = collectRaycastImpacts(
                level,
                x,
                y,
                z,
                blastRadius,
                blastPower,
                random,
                emitEffects,
                trace,
                entityContext
        );
        long blockResolveStart = trace != null ? System.nanoTime() : 0L;
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        Runnable resolvePass = () -> {
            blockImpacts.forEachImpact((blockPosLong, resolvedImpactPower) -> {
                applySingleBlockImpact(
                        level,
                        mutablePos,
                        blockPosLong,
                        resolvedImpactPower,
                        source,
                        owner,
                        applyWorldChanges,
                        trace
                );
            });
        };
        if (applyWorldChanges) {
            KrakkDamageRuntime.runBatchedSync(level, resolvePass);
        } else {
            resolvePass.run();
        }
        if (trace != null) {
            trace.blockResolveNanos += (System.nanoTime() - blockResolveStart);
        }

        long entityApplyStart = trace != null ? System.nanoTime() : 0L;
        applyEntityImpacts(level, source, owner, entityContext, applyWorldChanges, trace);
        if (trace != null) {
            trace.entityApplyNanos += (System.nanoTime() - entityApplyStart);
        }

        if (trace != null) {
            if (applyWorldChanges) {
                trace.syncPacketsEstimated = trace.brokenBlocks + trace.damagedBlocks;
            } else {
                trace.syncPacketsEstimated = trace.predictedBrokenBlocks + trace.predictedDamagedBlocks;
            }
            trace.syncBytesEstimated = trace.syncPacketsEstimated * estimateDamageSyncPayloadBytes(level.dimension().location());
        }
    }

    private static void detonateVolumetric(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner,
                                           double blastRadius, double totalEnergy,
                                           boolean applyWorldChanges, boolean emitEffects,
                                           ExplosionProfileTrace trace) {
        if (blastRadius <= 1.0E-9D || totalEnergy <= VOLUMETRIC_MIN_ENERGY) {
            return;
        }

        if (emitEffects) {
            emitExplosionEffects(level, x, y, z, level.random);
        }

        long blockResolveStart = trace != null ? System.nanoTime() : 0L;
        Runnable propagationPass = () -> runVolumetricPropagation(
                level,
                x,
                y,
                z,
                blastRadius,
                totalEnergy,
                source,
                owner,
                applyWorldChanges,
                trace
        );
        if (applyWorldChanges) {
            KrakkDamageRuntime.runBatchedSync(level, propagationPass);
        } else {
            propagationPass.run();
        }
        if (trace != null) {
            trace.blockResolveNanos += (System.nanoTime() - blockResolveStart);
            if (applyWorldChanges) {
                trace.syncPacketsEstimated = trace.brokenBlocks + trace.damagedBlocks;
            } else {
                trace.syncPacketsEstimated = trace.predictedBrokenBlocks + trace.predictedDamagedBlocks;
            }
            trace.syncBytesEstimated = trace.syncPacketsEstimated * estimateDamageSyncPayloadBytes(level.dimension().location());
        }
    }

    private static void detonateKrakk(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner,
                                        double blastRadius, double totalEnergy,
                                        boolean applyWorldChanges, boolean emitEffects,
                                        ExplosionProfileTrace trace) {
        if (totalEnergy <= VOLUMETRIC_MIN_ENERGY) {
            return;
        }

        if (emitEffects) {
            emitExplosionEffects(level, x, y, z, level.random);
        }

        long blockResolveStart = trace != null ? System.nanoTime() : 0L;
        Runnable propagationPass = () -> runKrakkPropagation(
                level,
                x,
                y,
                z,
                blastRadius,
                totalEnergy,
                source,
                owner,
                applyWorldChanges,
                trace
        );
        if (applyWorldChanges) {
            KrakkDamageRuntime.runBatchedSync(level, propagationPass);
        } else {
            propagationPass.run();
        }
        if (trace != null) {
            trace.blockResolveNanos += (System.nanoTime() - blockResolveStart);
            if (applyWorldChanges) {
                trace.syncPacketsEstimated = trace.brokenBlocks + trace.damagedBlocks;
            } else {
                trace.syncPacketsEstimated = trace.predictedBrokenBlocks + trace.predictedDamagedBlocks;
            }
            trace.syncBytesEstimated = trace.syncPacketsEstimated * estimateDamageSyncPayloadBytes(level.dimension().location());
        }
    }

    private static void runKrakkPropagation(ServerLevel level,
                                              double centerX,
                                              double centerY,
                                              double centerZ,
                                              double blastRadius,
                                              double totalEnergy,
                                              Entity source,
                                              LivingEntity owner,
                                              boolean applyWorldChanges,
                                              ExplosionProfileTrace trace) {
        BlockPos centerPos = BlockPos.containing(centerX, centerY, centerZ);
        if (!level.isInWorldBounds(centerPos)) {
            return;
        }

        boolean energyLimitedRadius = blastRadius <= 1.0E-9D;
        double resolvedRadius = energyLimitedRadius
                ? resolveKrakkRadiusFromEnergyCutoff(totalEnergy)
                : Math.max(1.0D, blastRadius);
        double radiusSq = resolvedRadius * resolvedRadius;
        int minX = Mth.floor(centerX - resolvedRadius);
        int maxX = Mth.ceil(centerX + resolvedRadius);
        int minY = Mth.floor(centerY - resolvedRadius);
        int maxY = Mth.ceil(centerY + resolvedRadius);
        int minZ = Mth.floor(centerZ - resolvedRadius);
        int maxZ = Mth.ceil(centerZ + resolvedRadius);

        Int2DoubleOpenHashMap resistanceCostCache = new Int2DoubleOpenHashMap(256);
        resistanceCostCache.defaultReturnValue(Double.NaN);
        long fieldStart = trace != null ? System.nanoTime() : 0L;
        KrakkField krakkField = buildKrakkField(
                level,
                centerX,
                centerY,
                centerZ,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ,
                radiusSq,
                resistanceCostCache
        );
        if (trace != null) {
            trace.volumetricResistanceFieldNanos += (System.nanoTime() - fieldStart);
            trace.volumetricSampledVoxels += krakkField.sampledVoxelCount();
            trace.volumetricSampledSolids += krakkField.solidPositions().size();
        }
        if (krakkField.sampledVoxelCount() <= 0 || krakkField.solidPositions().isEmpty()) {
            return;
        }

        boolean profileSubstages = trace != null;
        Future<KrakkVolumetricBaselineResult> baselineFuture = null;
        if (KRAKK_SOLVE_POOL != null) {
            baselineFuture = KRAKK_SOLVE_POOL.submit(
                    () -> buildKrakkVolumetricBaselineByPos(
                            centerX,
                            centerY,
                            centerZ,
                            resolvedRadius,
                            krakkField,
                            profileSubstages
                    )
            );
        }

        long solveStart = trace != null ? System.nanoTime() : 0L;
        PairedKrakkSolveResult pairedSolveResult = solvePairedKrakkArrivalTimes(
                krakkField,
                centerX,
                centerY,
                centerZ,
                Float.NaN,
                1.0F,
                (float) KRAKK_SHADOW_SOLID_SLOWNESS_SCALE
        );
        KrakkSolveResult solveResult = pairedSolveResult.normal();
        KrakkSolveResult shadowSolveResult = pairedSolveResult.shadow();
        if (trace != null) {
            long solveNanos = System.nanoTime() - solveStart;
            trace.volumetricPressureSolveNanos += solveNanos;
            trace.krakkSolveNanos += solveNanos;
            trace.krakkSourceCells += solveResult.sourceCells() + shadowSolveResult.sourceCells();
            trace.krakkSweepCycles += solveResult.sweepCycles() + shadowSolveResult.sweepCycles();
        }

        KrakkVolumetricBaselineResult baselineResult;
        if (baselineFuture != null) {
            try {
                baselineResult = baselineFuture.get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                baselineFuture.cancel(true);
                baselineResult = buildKrakkVolumetricBaselineByPos(
                        centerX,
                        centerY,
                        centerZ,
                        resolvedRadius,
                        krakkField,
                        profileSubstages
                );
            } catch (ExecutionException exception) {
                baselineFuture.cancel(true);
                baselineResult = buildKrakkVolumetricBaselineByPos(
                        centerX,
                        centerY,
                        centerZ,
                        resolvedRadius,
                        krakkField,
                        profileSubstages
                );
            }
        } else {
            baselineResult = buildKrakkVolumetricBaselineByPos(
                    centerX,
                    centerY,
                    centerZ,
                    resolvedRadius,
                    krakkField,
                    profileSubstages
            );
        }
        if (trace != null) {
            trace.volumetricDirectionSetupNanos += baselineResult.directionSetupNanos();
            trace.volumetricPressureSolveNanos += baselineResult.pressureSolveNanos();
            trace.volumetricTargetScanNanos += baselineResult.targetScanNanos();
            trace.volumetricTargetScanPrecheckNanos += baselineResult.targetScanPrecheckNanos();
            trace.volumetricTargetScanBlendNanos += baselineResult.targetScanBlendNanos();
            trace.volumetricDirectionSamples += baselineResult.directionSamples();
            trace.volumetricRadialSteps += baselineResult.radialSteps();
        }

        float[] arrivalTimes = solveResult.arrivalTimes();
        float[] shadowArrivalTimes = shadowSolveResult.arrivalTimes();
        LongArrayList solidPositions = krakkField.solidPositions();
        Long2FloatOpenHashMap volumetricBaselineByPos = baselineResult.baselineByPos();
        float[] volumetricBaselineByIndex = buildKrakkBaselineByIndex(
                solidPositions,
                volumetricBaselineByPos
        );
        double maxArrival = Math.max(1.0E-6D, resolvedRadius * KRAKK_MAX_ARRIVAL_MULTIPLIER);
        long targetScanStart = trace != null ? System.nanoTime() : 0L;
        KrakkTargetScanResult targetScanResult = scanKrakkTargets(
                solidPositions,
                volumetricBaselineByIndex,
                arrivalTimes,
                shadowArrivalTimes,
                krakkField,
                centerX,
                centerY,
                centerZ,
                maxArrival,
                true,
                profileSubstages
        );
        LongArrayList targetPositions = targetScanResult.targetPositions();
        FloatArrayList targetWeights = targetScanResult.targetWeights();
        double solidWeight = targetScanResult.solidWeight();
        if (trace != null) {
            trace.volumetricTargetScanNanos += (System.nanoTime() - targetScanStart);
            trace.volumetricTargetScanPrecheckNanos += targetScanResult.precheckNanos();
        }
        if (targetPositions.isEmpty() || solidWeight <= VOLUMETRIC_MIN_ENERGY) {
            return;
        }
        if (trace != null) {
            trace.rawImpactedBlocks += targetPositions.size();
            trace.postAaImpactedBlocks += targetPositions.size();
            trace.volumetricTargetBlocks += targetPositions.size();
        }

        double powerScale = computeVolumetricRadiusScale(resolvedRadius, VOLUMETRIC_MAX_POWER_RADIUS_SCALE);
        double impactBudget = totalEnergy * VOLUMETRIC_IMPACT_POWER_PER_ENERGY * powerScale;
        double airNormalizationScale = computeVolumetricAirNormalizationScale(solidPositions.size(), krakkField.sampledVoxelCount());
        double normalizationWeight = solidWeight * airNormalizationScale;
        if (normalizationWeight <= VOLUMETRIC_MIN_ENERGY) {
            return;
        }

        Long2FloatOpenHashMap collapseWeightsByPos = new Long2FloatOpenHashMap(Math.max(16, targetPositions.size()));
        collapseWeightsByPos.defaultReturnValue(0.0F);
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        long impactApplyStart = trace != null ? System.nanoTime() : 0L;
        long impactApplyDirectStart = trace != null ? System.nanoTime() : 0L;
        for (int i = 0; i < targetPositions.size(); i++) {
            long targetPosLong = targetPositions.getLong(i);
            double weight = targetWeights.getFloat(i);
            if (weight <= KRAKK_TARGET_MIN_WEIGHT) {
                continue;
            }
            float existingCollapseWeight = collapseWeightsByPos.get(targetPosLong);
            if (weight > existingCollapseWeight) {
                collapseWeightsByPos.put(targetPosLong, (float) weight);
            }
            double impactPower = impactBudget * (weight / normalizationWeight);
            applySingleBlockImpact(
                    level,
                    mutablePos,
                    targetPosLong,
                    impactPower,
                    source,
                    owner,
                    applyWorldChanges,
                    trace
            );
        }
        if (trace != null) {
            trace.volumetricImpactApplyDirectNanos += (System.nanoTime() - impactApplyDirectStart);
        }
        applyStructuralCollapsePass(
                level,
                centerX,
                centerY,
                centerZ,
                radiusSq,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ,
                solidPositions,
                collapseWeightsByPos,
                impactBudget,
                normalizationWeight,
                source,
                owner,
                applyWorldChanges,
                trace
        );
        if (trace != null) {
            trace.volumetricImpactApplyNanos += (System.nanoTime() - impactApplyStart);
        }
    }

    private static KrakkField buildKrakkField(ServerLevel level,
                                                  double centerX,
                                                  double centerY,
                                                  double centerZ,
                                                  int minX,
                                                  int maxX,
                                                  int minY,
                                                  int maxY,
                                                  int minZ,
                                                  int maxZ,
                                                  double radiusSq,
                                                  Int2DoubleOpenHashMap resistanceCostCache) {
        int sizeX = maxX - minX + 1;
        int sizeY = maxY - minY + 1;
        int sizeZ = maxZ - minZ + 1;
        long volumeLong = (long) sizeX * (long) sizeY * (long) sizeZ;
        if (volumeLong <= 0L || volumeLong > Integer.MAX_VALUE - 8L) {
            return new KrakkField(
                    minX,
                    minY,
                    minZ,
                    sizeX,
                    sizeY,
                    sizeZ,
                    0,
                    new BitSet(1),
                    new float[1],
                    new LongArrayList(),
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0]
            );
        }
        int volume = (int) volumeLong;
        BitSet activeMask = new BitSet(volume);
        float[] slowness = new float[Math.max(1, volume)];
        LongArrayList solidPositions = new LongArrayList(Math.max(64, volume / 8));
        int rowCount = sizeX * sizeY;
        int[] activeCountByRow = new int[Math.max(1, rowCount)];
        int sampledVoxelCount;
        int resistanceTaskCount = resolveResistanceFieldTaskCount(sizeX, sizeY, sizeZ);
        if (resistanceTaskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
            KrakkResistanceFieldChunkResult chunkResult = sampleKrakkFieldChunk(
                    level,
                    centerX,
                    centerY,
                    centerZ,
                    minX,
                    minY,
                    minZ,
                    sizeY,
                    sizeZ,
                    radiusSq,
                    0,
                    sizeX,
                    slowness,
                    activeCountByRow,
                    resistanceCostCache
            );
            sampledVoxelCount = chunkResult.sampledVoxelCount();
            LongArrayList chunkPositions = chunkResult.solidPositions();
            for (int i = 0; i < chunkPositions.size(); i++) {
                solidPositions.add(chunkPositions.getLong(i));
            }
        } else {
            ResistanceFieldSnapshot resistanceFieldSnapshot = sampleResistanceFieldSnapshot(
                    level,
                    centerX,
                    centerY,
                    centerZ,
                    minX,
                    minY,
                    minZ,
                    sizeX,
                    sizeY,
                    sizeZ,
                    radiusSq
            );
            int chunkSize = (sizeX + resistanceTaskCount - 1) / resistanceTaskCount;
            ArrayList<Future<KrakkResistanceFieldChunkResult>> futures = new ArrayList<>(resistanceTaskCount);
            for (int taskIndex = 0; taskIndex < resistanceTaskCount; taskIndex++) {
                int startXOffset = taskIndex * chunkSize;
                int endXOffset = Math.min(sizeX, startXOffset + chunkSize);
                if (startXOffset >= endXOffset) {
                    break;
                }
                final int chunkStartX = startXOffset;
                final int chunkEndX = endXOffset;
                futures.add(VOLUMETRIC_TARGET_SCAN_POOL.submit(
                        () -> sampleKrakkFieldChunk(
                                resistanceFieldSnapshot,
                                minX,
                                minY,
                                minZ,
                                sizeY,
                                sizeZ,
                                chunkStartX,
                                chunkEndX,
                                slowness,
                                activeCountByRow,
                                null
                        )
                ));
            }

            int sampledCountAccumulator = 0;
            boolean mergeFailed = false;
            try {
                for (Future<KrakkResistanceFieldChunkResult> future : futures) {
                    KrakkResistanceFieldChunkResult chunkResult = future.get();
                    sampledCountAccumulator += chunkResult.sampledVoxelCount();
                    LongArrayList chunkPositions = chunkResult.solidPositions();
                    for (int i = 0; i < chunkPositions.size(); i++) {
                        solidPositions.add(chunkPositions.getLong(i));
                    }
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                cancelKrakkResistanceFieldFutures(futures);
                mergeFailed = true;
            } catch (ExecutionException exception) {
                cancelKrakkResistanceFieldFutures(futures);
                mergeFailed = true;
            }

            if (mergeFailed) {
                Arrays.fill(slowness, 0.0F);
                Arrays.fill(activeCountByRow, 0);
                solidPositions.clear();
                KrakkResistanceFieldChunkResult fallback = sampleKrakkFieldChunk(
                        resistanceFieldSnapshot,
                        minX,
                        minY,
                        minZ,
                        sizeY,
                        sizeZ,
                        0,
                        sizeX,
                        slowness,
                        activeCountByRow,
                        resistanceCostCache
                );
                sampledVoxelCount = fallback.sampledVoxelCount();
                LongArrayList chunkPositions = fallback.solidPositions();
                for (int i = 0; i < chunkPositions.size(); i++) {
                    solidPositions.add(chunkPositions.getLong(i));
                }
            } else {
                sampledVoxelCount = sampledCountAccumulator;
            }
        }

        int[] activeRowOffsets = new int[Math.max(1, rowCount)];
        int totalActiveIndices = 0;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            activeRowOffsets[rowIndex] = totalActiveIndices;
            totalActiveIndices += activeCountByRow[rowIndex];
        }
        int[] activeRowIndices = new int[Math.max(1, totalActiveIndices)];
        int[] writeCursorByRow = Arrays.copyOf(activeRowOffsets, activeRowOffsets.length);
        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
        for (int xOffset = 0; xOffset < sizeX; xOffset++) {
            int baseX = xOffset * strideX;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int rowIndex = krakkRowIndex(xOffset, yOffset, sizeY);
                int base = baseX + (yOffset * strideY);
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int index = base + zOffset;
                    if (slowness[index] <= 0.0F) {
                        continue;
                    }
                    activeMask.set(index);
                    activeRowIndices[writeCursorByRow[rowIndex]++] = index;
                }
            }
        }

        int[] neighborNegX = new int[Math.max(1, volume)];
        int[] neighborPosX = new int[Math.max(1, volume)];
        int[] neighborNegY = new int[Math.max(1, volume)];
        int[] neighborPosY = new int[Math.max(1, volume)];
        int[] neighborNegZ = new int[Math.max(1, volume)];
        int[] neighborPosZ = new int[Math.max(1, volume)];
        Arrays.fill(neighborNegX, -1);
        Arrays.fill(neighborPosX, -1);
        Arrays.fill(neighborNegY, -1);
        Arrays.fill(neighborPosY, -1);
        Arrays.fill(neighborNegZ, -1);
        Arrays.fill(neighborPosZ, -1);
        for (int index = activeMask.nextSetBit(0); index >= 0; index = activeMask.nextSetBit(index + 1)) {
            int xOffset = index / strideX;
            int yz = index - (xOffset * strideX);
            int yOffset = yz / strideY;
            int zOffset = yz - (yOffset * strideY);
            if (xOffset > 0) {
                int neighbor = index - strideX;
                if (activeMask.get(neighbor)) {
                    neighborNegX[index] = neighbor;
                }
            }
            if (xOffset + 1 < sizeX) {
                int neighbor = index + strideX;
                if (activeMask.get(neighbor)) {
                    neighborPosX[index] = neighbor;
                }
            }
            if (yOffset > 0) {
                int neighbor = index - strideY;
                if (activeMask.get(neighbor)) {
                    neighborNegY[index] = neighbor;
                }
            }
            if (yOffset + 1 < sizeY) {
                int neighbor = index + strideY;
                if (activeMask.get(neighbor)) {
                    neighborPosY[index] = neighbor;
                }
            }
            if (zOffset > 0) {
                int neighbor = index - 1;
                if (activeMask.get(neighbor)) {
                    neighborNegZ[index] = neighbor;
                }
            }
            if (zOffset + 1 < sizeZ) {
                int neighbor = index + 1;
                if (activeMask.get(neighbor)) {
                    neighborPosZ[index] = neighbor;
                }
            }
        }
        return new KrakkField(
                minX,
                minY,
                minZ,
                sizeX,
                sizeY,
                sizeZ,
                sampledVoxelCount,
                activeMask,
                slowness,
                solidPositions,
                activeRowOffsets,
                activeCountByRow,
                activeRowIndices,
                neighborNegX,
                neighborPosX,
                neighborNegY,
                neighborPosY,
                neighborNegZ,
                neighborPosZ
        );
    }

    private static KrakkVolumetricBaselineResult buildKrakkVolumetricBaselineByPos(double centerX,
                                                                                        double centerY,
                                                                                        double centerZ,
                                                                                        double resolvedRadius,
                                                                                        KrakkField krakkField,
                                                                                        boolean collectMetrics) {
        LongArrayList solidPositions = krakkField.solidPositions();
        Long2FloatOpenHashMap empty = new Long2FloatOpenHashMap(16);
        empty.defaultReturnValue(Float.NaN);
        if (solidPositions.isEmpty()) {
            return new KrakkVolumetricBaselineResult(empty, 0L, 0L, 0L, 0L, 0L, 0, 0);
        }

        float[] fieldSlowness = krakkField.slowness();
        int fieldMinX = krakkField.minX();
        int fieldMinY = krakkField.minY();
        int fieldMinZ = krakkField.minZ();
        int fieldSizeX = krakkField.sizeX();
        int fieldSizeY = krakkField.sizeY();
        int fieldSizeZ = krakkField.sizeZ();
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        float resistanceScaleInv = (float) (1.0D / Math.max(1.0E-6D, KRAKK_SOLID_SLOWNESS_SCALE));

        long directionSetupStart = collectMetrics ? System.nanoTime() : 0L;
        double pointScale = computeVolumetricRadiusScale(resolvedRadius, VOLUMETRIC_MAX_POINT_RADIUS_SCALE);
        int directionCount = computeVolumetricDirectionSampleCount(resolvedRadius, pointScale);
        VolumetricDirectionCache directionCache = getVolumetricDirectionCache(directionCount);
        double[] dirX = directionCache.dirX();
        double[] dirY = directionCache.dirY();
        double[] dirZ = directionCache.dirZ();
        int[][] directionNeighbors = directionCache.neighbors();
        int[] directionLookup = directionCache.directionLookup();
        int directionLookupResolution = directionCache.directionLookupResolution();
        int radialSteps = Mth.clamp(
                Mth.ceil(resolvedRadius / VOLUMETRIC_DEFORM_SAMPLE_STEP),
                1,
                VOLUMETRIC_MAX_RADIAL_STEPS
        );
        double radialStepSize = resolvedRadius / radialSteps;
        float[] pressureByShell = new float[(radialSteps + 1) * directionCount];
        Arrays.fill(pressureByShell, 0, directionCount, 1.0F);
        float[] maxPressureByShell = new float[radialSteps + 1];
        maxPressureByShell[0] = 1.0F;
        float[] previousPressure = new float[directionCount];
        Arrays.fill(previousPressure, 1.0F);
        float[] rawPressure = new float[directionCount];
        float[] currentPressure = new float[directionCount];
        double perStepAirDecay = Mth.clamp(VOLUMETRIC_PRESSURE_AIR_DECAY_PER_BLOCK * radialStepSize, 0.0D, 0.95D);
        double perStepRecovery = Math.max(0.0D, VOLUMETRIC_PRESSURE_RECOVERY_PER_BLOCK * radialStepSize);
        long directionSetupNanos = collectMetrics ? (System.nanoTime() - directionSetupStart) : 0L;

        long pressureSolveStart = collectMetrics ? System.nanoTime() : 0L;
        for (int shell = 1; shell <= radialSteps; shell++) {
            double t = shell * radialStepSize;
            for (int i = 0; i < directionCount; i++) {
                int sampleX = Mth.floor(centerX + (dirX[i] * t));
                int sampleY = Mth.floor(centerY + (dirY[i] * t));
                int sampleZ = Mth.floor(centerZ + (dirZ[i] * t));
                int sampleXOffset = sampleX - fieldMinX;
                int sampleYOffset = sampleY - fieldMinY;
                int sampleZOffset = sampleZ - fieldMinZ;
                float sampleResistance = 0.0F;
                if (sampleXOffset >= 0
                        && sampleXOffset < fieldSizeX
                        && sampleYOffset >= 0
                        && sampleYOffset < fieldSizeY
                        && sampleZOffset >= 0
                        && sampleZOffset < fieldSizeZ) {
                    int sampleIndex = krakkGridIndex(sampleXOffset, sampleYOffset, sampleZOffset, fieldSizeY, fieldSizeZ);
                    float sampleSlowness = fieldSlowness[sampleIndex];
                    if (sampleSlowness > airSlowness) {
                        sampleResistance = (sampleSlowness - airSlowness) * resistanceScaleInv;
                    }
                }
                double transmitted = previousPressure[i] * (1.0D - perStepAirDecay);
                transmitted -= sampleResistance * VOLUMETRIC_PRESSURE_RESISTANCE_LOSS_SCALE * radialStepSize;
                rawPressure[i] = (float) Mth.clamp(transmitted, 0.0D, 1.0D);
            }

            int rowOffset = shell * directionCount;
            float shellMaxPressure = 0.0F;
            for (int i = 0; i < directionCount; i++) {
                int[] neighbors = directionNeighbors[i];
                double neighborPressure = 0.0D;
                int validNeighbors = 0;
                for (int neighborIndex : neighbors) {
                    if (neighborIndex < 0) {
                        continue;
                    }
                    neighborPressure += rawPressure[neighborIndex];
                    validNeighbors++;
                }
                double neighborAverage = validNeighbors > 0 ? (neighborPressure / validNeighbors) : rawPressure[i];
                double diffusedPressure = Mth.lerp(VOLUMETRIC_PRESSURE_DIFFUSION, rawPressure[i], neighborAverage);
                double recoveredPressure = Math.min(diffusedPressure, previousPressure[i] + perStepRecovery);
                currentPressure[i] = (float) Mth.clamp(recoveredPressure, 0.0D, 1.0D);
                pressureByShell[rowOffset + i] = currentPressure[i];
                if (currentPressure[i] > shellMaxPressure) {
                    shellMaxPressure = currentPressure[i];
                }
            }
            maxPressureByShell[shell] = shellMaxPressure;
            if (shellMaxPressure <= 0.0F) {
                break;
            }

            float[] swap = previousPressure;
            previousPressure = currentPressure;
            currentPressure = swap;
        }
        long pressureSolveNanos = collectMetrics ? (System.nanoTime() - pressureSolveStart) : 0L;

        VolumetricTargetScanContext targetScanContext = new VolumetricTargetScanContext(
                centerX,
                centerY,
                centerZ,
                resolvedRadius,
                radialStepSize,
                radialSteps,
                pressureByShell,
                maxPressureByShell,
                directionCount,
                dirX,
                dirY,
                dirZ,
                directionNeighbors,
                directionLookup,
                directionLookupResolution
        );
        long targetScanStart = collectMetrics ? System.nanoTime() : 0L;
        VolumetricTargetScanResult targetScanResult = scanVolumetricTargets(
                solidPositions,
                targetScanContext,
                true,
                collectMetrics
        );
        long targetScanNanos = collectMetrics ? (System.nanoTime() - targetScanStart) : 0L;

        LongArrayList targetPositions = targetScanResult.targetPositions();
        FloatArrayList targetWeights = targetScanResult.targetWeights();
        Long2FloatOpenHashMap baselineByPos = new Long2FloatOpenHashMap(Math.max(64, (int) Math.ceil(targetPositions.size() / 0.75D)));
        baselineByPos.defaultReturnValue(Float.NaN);
        for (int i = 0; i < targetPositions.size(); i++) {
            baselineByPos.put(targetPositions.getLong(i), targetWeights.getFloat(i));
        }
        Long2FloatOpenHashMap smoothed = KRAKK_ENABLE_VOLUMETRIC_BASELINE_SMOOTHING
                ? smoothKrakkVolumetricMechanics(baselineByPos, solidPositions)
                : baselineByPos;
        return new KrakkVolumetricBaselineResult(
                smoothed,
                directionSetupNanos,
                pressureSolveNanos,
                targetScanNanos,
                collectMetrics ? targetScanResult.precheckNanos() : 0L,
                collectMetrics ? targetScanResult.blendNanos() : 0L,
                collectMetrics ? directionCount : 0,
                collectMetrics ? radialSteps : 0
        );
    }

    private static Long2FloatOpenHashMap smoothKrakkVolumetricMechanics(Long2FloatOpenHashMap pressureByPos,
                                                                          LongArrayList solidPositions) {
        Long2FloatOpenHashMap smoothed = new Long2FloatOpenHashMap(Math.max(64, (int) Math.ceil(pressureByPos.size() / 0.75D)));
        smoothed.defaultReturnValue(Float.NaN);
        double selfWeight = Mth.clamp(KRAKK_VOLUMETRIC_MECHANICS_SELF_SMOOTH, 0.0D, 1.0D);
        for (int i = 0; i < solidPositions.size(); i++) {
            long posLong = solidPositions.getLong(i);
            float self = pressureByPos.get(posLong);
            if (!Float.isFinite(self)) {
                continue;
            }
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            double neighborSum = 0.0D;
            int neighborCount = 0;
            float px = pressureByPos.get(BlockPos.asLong(x + 1, y, z));
            if (Float.isFinite(px)) {
                neighborSum += px;
                neighborCount++;
            }
            float nx = pressureByPos.get(BlockPos.asLong(x - 1, y, z));
            if (Float.isFinite(nx)) {
                neighborSum += nx;
                neighborCount++;
            }
            float py = pressureByPos.get(BlockPos.asLong(x, y + 1, z));
            if (Float.isFinite(py)) {
                neighborSum += py;
                neighborCount++;
            }
            float ny = pressureByPos.get(BlockPos.asLong(x, y - 1, z));
            if (Float.isFinite(ny)) {
                neighborSum += ny;
                neighborCount++;
            }
            float pz = pressureByPos.get(BlockPos.asLong(x, y, z + 1));
            if (Float.isFinite(pz)) {
                neighborSum += pz;
                neighborCount++;
            }
            float nz = pressureByPos.get(BlockPos.asLong(x, y, z - 1));
            if (Float.isFinite(nz)) {
                neighborSum += nz;
                neighborCount++;
            }
            double neighborAverage = neighborCount > 0 ? (neighborSum / neighborCount) : self;
            double value = (self * selfWeight) + (neighborAverage * (1.0D - selfWeight));
            smoothed.put(posLong, (float) Mth.clamp(value, 0.0D, 1.0D));
        }
        return smoothed;
    }

    private static double computeKrakkCutoffEdgeRetention(double normalizedArrival) {
        double normalized = Mth.clamp(normalizedArrival, 0.0D, 1.0D);
        double edgeStart = Mth.clamp(KRAKK_CUTOFF_EDGE_START_NORMALIZED, 1.0E-4D, 1.0D);
        if (normalized >= edgeStart) {
            return 1.0D;
        }
        double edgeProgress = normalized / edgeStart;
        return Math.pow(Math.max(0.0D, edgeProgress), KRAKK_CUTOFF_EDGE_CURVE_EXPONENT);
    }

    private static double computeAnalyticKrakkAirArrival(double centerX,
                                                           double centerY,
                                                           double centerZ,
                                                           int x,
                                                           int y,
                                                           int z) {
        double dx = (x + 0.5D) - centerX;
        double dy = (y + 0.5D) - centerY;
        double dz = (z + 0.5D) - centerZ;
        return Math.sqrt((dx * dx) + (dy * dy) + (dz * dz)) * KRAKK_AIR_SLOWNESS;
    }

    private static float[] buildKrakkBaselineByIndex(LongArrayList solidPositions,
                                                       Long2FloatOpenHashMap volumetricBaselineByPos) {
        int solidCount = solidPositions.size();
        float[] volumetricBaselineByIndex = new float[solidCount];
        for (int i = 0; i < solidCount; i++) {
            volumetricBaselineByIndex[i] = volumetricBaselineByPos.get(solidPositions.getLong(i));
        }
        return volumetricBaselineByIndex;
    }

    private static KrakkSolveResult solveKrakkArrivalTimes(KrakkField field,
                                                               double centerX,
                                                               double centerY,
                                                               double centerZ,
                                                               float uniformSlownessOverride,
                                                               float solidSlownessScale) {
        float[] arrivalTimes = new float[field.slowness().length];
        Arrays.fill(arrivalTimes, Float.POSITIVE_INFINITY);
        BitSet traversableMask = field.activeMask();
        float[] resolvedSlowness = resolveKrakkSlownessField(field, uniformSlownessOverride, solidSlownessScale);
        BitSet sourceMask = new BitSet(arrivalTimes.length);
        int sourceCount = initializeKrakkSources(
                arrivalTimes,
                sourceMask,
                field,
                traversableMask,
                resolvedSlowness,
                centerX,
                centerY,
                centerZ
        );
        return solveKrakkArrivalTimesPreparedMultires(
                arrivalTimes,
                resolvedSlowness,
                field,
                sourceMask,
                sourceCount,
                true
        );
    }

    private static PairedKrakkSolveResult solvePairedKrakkArrivalTimes(KrakkField field,
                                                                           double centerX,
                                                                           double centerY,
                                                                           double centerZ,
                                                                           float uniformSlownessOverride,
                                                                           float normalSolidSlownessScale,
                                                                           float shadowSolidSlownessScale) {
        float[] normalArrivalTimes = new float[field.slowness().length];
        float[] shadowArrivalTimes = new float[field.slowness().length];
        Arrays.fill(normalArrivalTimes, Float.POSITIVE_INFINITY);
        Arrays.fill(shadowArrivalTimes, Float.POSITIVE_INFINITY);
        BitSet traversableMask = field.activeMask();
        PairedKrakkSlownessResult slownessResult = resolvePairedKrakkSlownessField(
                field,
                uniformSlownessOverride,
                normalSolidSlownessScale,
                shadowSolidSlownessScale
        );
        BitSet normalSourceMask = new BitSet(normalArrivalTimes.length);
        BitSet shadowSourceMask = new BitSet(shadowArrivalTimes.length);
        PairedKrakkSourceResult sourceResult = initializePairedKrakkSources(
                normalArrivalTimes,
                normalSourceMask,
                slownessResult.normalSlowness(),
                shadowArrivalTimes,
                shadowSourceMask,
                slownessResult.shadowSlowness(),
                field,
                traversableMask,
                centerX,
                centerY,
                centerZ
        );

        KrakkSolveResult normalSolveResult;
        KrakkSolveResult shadowSolveResult;
        if (KRAKK_SOLVE_POOL != null) {
            Future<KrakkSolveResult> shadowFuture = KRAKK_SOLVE_POOL.submit(
                    () -> solveKrakkArrivalTimesPreparedMultires(
                            shadowArrivalTimes,
                            slownessResult.shadowSlowness(),
                            field,
                            shadowSourceMask,
                            sourceResult.shadowSourceCount(),
                            true
                    )
            );
            normalSolveResult = solveKrakkArrivalTimesPreparedMultires(
                    normalArrivalTimes,
                    slownessResult.normalSlowness(),
                    field,
                    normalSourceMask,
                    sourceResult.normalSourceCount(),
                    true
            );
            try {
                shadowSolveResult = shadowFuture.get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                shadowFuture.cancel(true);
                shadowSolveResult = solveKrakkArrivalTimesPreparedMultires(
                        shadowArrivalTimes,
                        slownessResult.shadowSlowness(),
                        field,
                        shadowSourceMask,
                        sourceResult.shadowSourceCount(),
                        true
                );
            } catch (ExecutionException exception) {
                shadowFuture.cancel(true);
                shadowSolveResult = solveKrakkArrivalTimesPreparedMultires(
                        shadowArrivalTimes,
                        slownessResult.shadowSlowness(),
                        field,
                        shadowSourceMask,
                        sourceResult.shadowSourceCount(),
                        true
                );
            }
        } else {
            normalSolveResult = solveKrakkArrivalTimesPreparedMultires(
                    normalArrivalTimes,
                    slownessResult.normalSlowness(),
                    field,
                    normalSourceMask,
                    sourceResult.normalSourceCount(),
                    true
            );
            shadowSolveResult = solveKrakkArrivalTimesPreparedMultires(
                    shadowArrivalTimes,
                    slownessResult.shadowSlowness(),
                    field,
                    shadowSourceMask,
                    sourceResult.shadowSourceCount(),
                    true
            );
        }
        return new PairedKrakkSolveResult(normalSolveResult, shadowSolveResult);
    }

    private static KrakkSolveResult solveKrakkArrivalTimesPrepared(float[] arrivalTimes,
                                                                       float[] resolvedSlowness,
                                                                       KrakkField field,
                                                                       BitSet sourceMask,
                                                                       int sourceCount) {
        return solveKrakkArrivalTimesPrepared(
                arrivalTimes,
                resolvedSlowness,
                field,
                sourceMask,
                sourceCount,
                true
        );
    }

    private static KrakkSolveResult solveKrakkArrivalTimesPrepared(float[] arrivalTimes,
                                                                       float[] resolvedSlowness,
                                                                       KrakkField field,
                                                                       BitSet sourceMask,
                                                                       int sourceCount,
                                                                       boolean allowParallelSweep) {
        if (sourceCount <= 0) {
            return new KrakkSolveResult(arrivalTimes, 0, 0);
        }
        int maxSweepCycles = computeKrakkMaxSweepCycles(field);
        int sweepCycles = refineKrakkArrivalTimes(
                arrivalTimes,
                resolvedSlowness,
                field,
                sourceMask,
                allowParallelSweep,
                maxSweepCycles
        );
        return new KrakkSolveResult(arrivalTimes, sourceCount, sweepCycles);
    }

    private static KrakkSolveResult solveKrakkArrivalTimesPreparedMultires(float[] arrivalTimes,
                                                                                float[] resolvedSlowness,
                                                                                KrakkField field,
                                                                                BitSet sourceMask,
                                                                                int sourceCount,
                                                                                boolean allowParallelSweep) {
        if (!KRAKK_USE_MULTIRES_COARSE_SOLVE || sourceCount <= 0) {
            return solveKrakkArrivalTimesPrepared(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    sourceCount,
                    allowParallelSweep
            );
        }

        KrakkCoarseSolveContext coarseContext = buildCoarseKrakkSolveContext(
                field,
                resolvedSlowness,
                sourceMask,
                arrivalTimes,
                KRAKK_MULTIRES_DOWNSAMPLE_FACTOR
        );
        if (coarseContext == null || coarseContext.coarseSourceCount() <= 0) {
            return solveKrakkArrivalTimesPrepared(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    sourceCount,
                    allowParallelSweep
            );
        }

        solveKrakkArrivalTimesPrepared(
                coarseContext.coarseArrivalTimes(),
                coarseContext.coarseSlowness(),
                coarseContext.coarseField(),
                coarseContext.coarseSourceMask(),
                coarseContext.coarseSourceCount(),
                allowParallelSweep
        );

        seedFineArrivalTimesFromCoarse(
                arrivalTimes,
                resolvedSlowness,
                field,
                sourceMask,
                coarseContext
        );
        int fineSweepCycles = refineKrakkArrivalTimes(
                arrivalTimes,
                resolvedSlowness,
                field,
                sourceMask,
                allowParallelSweep,
                KRAKK_MULTIRES_FINE_REFINE_SWEEP_CYCLES
        );
        return new KrakkSolveResult(arrivalTimes, sourceCount, fineSweepCycles);
    }

    private static int refineKrakkArrivalTimes(float[] arrivalTimes,
                                                 float[] resolvedSlowness,
                                                 KrakkField field,
                                                 BitSet sourceMask,
                                                 boolean allowParallelSweep,
                                                 int maxSweepCycles) {
        int boundedCycles = Math.max(0, Math.min(maxSweepCycles, computeKrakkMaxSweepCycles(field)));
        int rowCount = field.sizeX() * field.sizeY();
        BitSet sourceRows = buildKrakkSourceRowMask(field, sourceMask);
        BitSet dirtyRows = null;
        int sweepCycles = 0;
        for (int cycle = 0; cycle < boundedCycles; cycle++) {
            BitSet activeRowsMask = null;
            if (cycle > 0) {
                if (dirtyRows == null || dirtyRows.isEmpty()) {
                    break;
                }
                activeRowsMask = buildExpandedKrakkRowMask(dirtyRows, sourceRows, field.sizeX(), field.sizeY());
            }
            BitSet nextDirtyRows = new BitSet(Math.max(1, rowCount));
            double maxDelta = 0.0D;
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, true, true, true, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, true, true, false, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, true, false, true, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, true, false, false, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, false, true, true, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, false, true, false, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, false, false, true, activeRowsMask, nextDirtyRows));
            maxDelta = Math.max(maxDelta, sweepKrakkPass(arrivalTimes, resolvedSlowness, field, sourceMask, allowParallelSweep, false, false, false, activeRowsMask, nextDirtyRows));
            dirtyRows = nextDirtyRows;
            sweepCycles++;
            if (maxDelta <= KRAKK_CONVERGENCE_EPSILON) {
                break;
            }
        }
        return sweepCycles;
    }

    private static BitSet buildKrakkSourceRowMask(KrakkField field, BitSet sourceMask) {
        int rowCount = Math.max(1, field.sizeX() * field.sizeY());
        BitSet sourceRows = new BitSet(rowCount);
        int strideX = field.sizeY() * field.sizeZ();
        int strideY = field.sizeZ();
        for (int index = sourceMask.nextSetBit(0); index >= 0; index = sourceMask.nextSetBit(index + 1)) {
            int x = index / strideX;
            int yz = index - (x * strideX);
            int y = yz / strideY;
            sourceRows.set(krakkRowIndex(x, y, field.sizeY()));
        }
        return sourceRows;
    }

    private static BitSet buildExpandedKrakkRowMask(BitSet dirtyRows, BitSet sourceRows, int sizeX, int sizeY) {
        BitSet expanded = (BitSet) dirtyRows.clone();
        if (sourceRows != null) {
            expanded.or(sourceRows);
        }
        for (int rowIndex = dirtyRows.nextSetBit(0); rowIndex >= 0; rowIndex = dirtyRows.nextSetBit(rowIndex + 1)) {
            int x = rowIndex / sizeY;
            int y = rowIndex - (x * sizeY);
            if (x > 0) {
                expanded.set(krakkRowIndex(x - 1, y, sizeY));
            }
            if (x + 1 < sizeX) {
                expanded.set(krakkRowIndex(x + 1, y, sizeY));
            }
            if (y > 0) {
                expanded.set(krakkRowIndex(x, y - 1, sizeY));
            }
            if (y + 1 < sizeY) {
                expanded.set(krakkRowIndex(x, y + 1, sizeY));
            }
        }
        return expanded;
    }

    private static KrakkCoarseSolveContext buildCoarseKrakkSolveContext(KrakkField fineField,
                                                                             float[] fineSlowness,
                                                                             BitSet fineSourceMask,
                                                                             float[] fineArrivalTimes,
                                                                             int downsampleFactor) {
        if (downsampleFactor <= 1) {
            return null;
        }
        int fineSizeX = fineField.sizeX();
        int fineSizeY = fineField.sizeY();
        int fineSizeZ = fineField.sizeZ();
        int longestAxis = Math.max(fineSizeX, Math.max(fineSizeY, fineSizeZ));
        if (longestAxis < KRAKK_MULTIRES_MIN_AXIS_FOR_COARSE) {
            return null;
        }

        int coarseSizeX = (fineSizeX + downsampleFactor - 1) / downsampleFactor;
        int coarseSizeY = (fineSizeY + downsampleFactor - 1) / downsampleFactor;
        int coarseSizeZ = (fineSizeZ + downsampleFactor - 1) / downsampleFactor;
        if (coarseSizeX >= fineSizeX && coarseSizeY >= fineSizeY && coarseSizeZ >= fineSizeZ) {
            return null;
        }
        long coarseVolumeLong = (long) coarseSizeX * (long) coarseSizeY * (long) coarseSizeZ;
        if (coarseVolumeLong <= 0L || coarseVolumeLong > Integer.MAX_VALUE - 8L) {
            return null;
        }
        int coarseVolume = (int) coarseVolumeLong;
        float[] coarseSlowness = new float[Math.max(1, coarseVolume)];
        float[] coarseArrivalTimes = new float[Math.max(1, coarseVolume)];
        Arrays.fill(coarseArrivalTimes, Float.POSITIVE_INFINITY);
        BitSet coarseSourceMask = new BitSet(coarseArrivalTimes.length);
        int coarseSourceCount = 0;
        LongArrayList coarseSolidPositions = new LongArrayList(Math.max(16, fineField.solidPositions().size() / Math.max(1, downsampleFactor * downsampleFactor)));

        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        int fineStrideX = fineSizeY * fineSizeZ;
        int fineStrideY = fineSizeZ;
        for (int coarseX = 0; coarseX < coarseSizeX; coarseX++) {
            int fineStartX = coarseX * downsampleFactor;
            int fineEndX = Math.min(fineSizeX, fineStartX + downsampleFactor);
            for (int coarseY = 0; coarseY < coarseSizeY; coarseY++) {
                int fineStartY = coarseY * downsampleFactor;
                int fineEndY = Math.min(fineSizeY, fineStartY + downsampleFactor);
                for (int coarseZ = 0; coarseZ < coarseSizeZ; coarseZ++) {
                    int fineStartZ = coarseZ * downsampleFactor;
                    int fineEndZ = Math.min(fineSizeZ, fineStartZ + downsampleFactor);
                    float minSlowness = Float.POSITIVE_INFINITY;
                    float minSourceArrival = Float.POSITIVE_INFINITY;
                    boolean active = false;
                    boolean hasSolid = false;
                    boolean hasSource = false;
                    for (int fineX = fineStartX; fineX < fineEndX; fineX++) {
                        int baseX = fineX * fineStrideX;
                        for (int fineY = fineStartY; fineY < fineEndY; fineY++) {
                            int base = baseX + (fineY * fineStrideY);
                            for (int fineZ = fineStartZ; fineZ < fineEndZ; fineZ++) {
                                int fineIndex = base + fineZ;
                                float slowness = fineSlowness[fineIndex];
                                if (slowness <= 0.0F) {
                                    continue;
                                }
                                active = true;
                                minSlowness = Math.min(minSlowness, slowness);
                                hasSolid |= slowness > airSlowness + 1.0E-6F;
                                if (fineSourceMask.get(fineIndex)) {
                                    hasSource = true;
                                    minSourceArrival = Math.min(minSourceArrival, fineArrivalTimes[fineIndex]);
                                }
                            }
                        }
                    }

                    if (!active || !Float.isFinite(minSlowness)) {
                        continue;
                    }

                    int coarseIndex = krakkGridIndex(coarseX, coarseY, coarseZ, coarseSizeY, coarseSizeZ);
                    coarseSlowness[coarseIndex] = minSlowness;
                    if (hasSolid) {
                        coarseSolidPositions.add(BlockPos.asLong(
                                fineField.minX() + fineStartX,
                                fineField.minY() + fineStartY,
                                fineField.minZ() + fineStartZ
                        ));
                    }
                    if (hasSource && Float.isFinite(minSourceArrival)) {
                        if (!coarseSourceMask.get(coarseIndex)) {
                            coarseSourceMask.set(coarseIndex);
                            coarseSourceCount++;
                        }
                        coarseArrivalTimes[coarseIndex] = minSourceArrival;
                    }
                }
            }
        }

        if (coarseSourceCount <= 0) {
            return null;
        }
        int coarseSampledVoxels = Math.max(1, fineField.sampledVoxelCount() / Math.max(1, downsampleFactor * downsampleFactor * downsampleFactor));
        KrakkField coarseField = buildPreparedKrakkFieldFromSlowness(
                fineField.minX(),
                fineField.minY(),
                fineField.minZ(),
                coarseSizeX,
                coarseSizeY,
                coarseSizeZ,
                coarseSampledVoxels,
                coarseSlowness,
                coarseSolidPositions
        );
        if (coarseField.activeMask().isEmpty()) {
            return null;
        }
        return new KrakkCoarseSolveContext(
                coarseField,
                coarseArrivalTimes,
                coarseSlowness,
                coarseSourceMask,
                coarseSourceCount,
                downsampleFactor
        );
    }

    private static KrakkField buildPreparedKrakkFieldFromSlowness(int minX,
                                                                       int minY,
                                                                       int minZ,
                                                                       int sizeX,
                                                                       int sizeY,
                                                                       int sizeZ,
                                                                       int sampledVoxelCount,
                                                                       float[] slowness,
                                                                       LongArrayList solidPositions) {
        int volume = Math.max(1, sizeX * sizeY * sizeZ);
        BitSet activeMask = new BitSet(volume);
        int rowCount = Math.max(1, sizeX * sizeY);
        int[] activeCountByRow = new int[rowCount];

        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
        for (int xOffset = 0; xOffset < sizeX; xOffset++) {
            int baseX = xOffset * strideX;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int rowIndex = krakkRowIndex(xOffset, yOffset, sizeY);
                int base = baseX + (yOffset * strideY);
                int activeCount = 0;
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int index = base + zOffset;
                    if (slowness[index] <= 0.0F) {
                        continue;
                    }
                    activeMask.set(index);
                    activeCount++;
                }
                activeCountByRow[rowIndex] = activeCount;
            }
        }

        int[] activeRowOffsets = new int[rowCount];
        int totalActive = 0;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            activeRowOffsets[rowIndex] = totalActive;
            totalActive += activeCountByRow[rowIndex];
        }
        int[] activeRowIndices = new int[Math.max(1, totalActive)];
        int[] writeCursorByRow = Arrays.copyOf(activeRowOffsets, activeRowOffsets.length);
        for (int xOffset = 0; xOffset < sizeX; xOffset++) {
            int baseX = xOffset * strideX;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int rowIndex = krakkRowIndex(xOffset, yOffset, sizeY);
                int base = baseX + (yOffset * strideY);
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int index = base + zOffset;
                    if (slowness[index] <= 0.0F) {
                        continue;
                    }
                    activeRowIndices[writeCursorByRow[rowIndex]++] = index;
                }
            }
        }

        int[] neighborNegX = new int[Math.max(1, volume)];
        int[] neighborPosX = new int[Math.max(1, volume)];
        int[] neighborNegY = new int[Math.max(1, volume)];
        int[] neighborPosY = new int[Math.max(1, volume)];
        int[] neighborNegZ = new int[Math.max(1, volume)];
        int[] neighborPosZ = new int[Math.max(1, volume)];
        Arrays.fill(neighborNegX, -1);
        Arrays.fill(neighborPosX, -1);
        Arrays.fill(neighborNegY, -1);
        Arrays.fill(neighborPosY, -1);
        Arrays.fill(neighborNegZ, -1);
        Arrays.fill(neighborPosZ, -1);
        for (int index = activeMask.nextSetBit(0); index >= 0; index = activeMask.nextSetBit(index + 1)) {
            int xOffset = index / strideX;
            int yz = index - (xOffset * strideX);
            int yOffset = yz / strideY;
            int zOffset = yz - (yOffset * strideY);
            if (xOffset > 0) {
                int neighbor = index - strideX;
                if (activeMask.get(neighbor)) {
                    neighborNegX[index] = neighbor;
                }
            }
            if (xOffset + 1 < sizeX) {
                int neighbor = index + strideX;
                if (activeMask.get(neighbor)) {
                    neighborPosX[index] = neighbor;
                }
            }
            if (yOffset > 0) {
                int neighbor = index - strideY;
                if (activeMask.get(neighbor)) {
                    neighborNegY[index] = neighbor;
                }
            }
            if (yOffset + 1 < sizeY) {
                int neighbor = index + strideY;
                if (activeMask.get(neighbor)) {
                    neighborPosY[index] = neighbor;
                }
            }
            if (zOffset > 0) {
                int neighbor = index - 1;
                if (activeMask.get(neighbor)) {
                    neighborNegZ[index] = neighbor;
                }
            }
            if (zOffset + 1 < sizeZ) {
                int neighbor = index + 1;
                if (activeMask.get(neighbor)) {
                    neighborPosZ[index] = neighbor;
                }
            }
        }
        return new KrakkField(
                minX,
                minY,
                minZ,
                sizeX,
                sizeY,
                sizeZ,
                sampledVoxelCount,
                activeMask,
                slowness,
                solidPositions,
                activeRowOffsets,
                activeCountByRow,
                activeRowIndices,
                neighborNegX,
                neighborPosX,
                neighborNegY,
                neighborPosY,
                neighborNegZ,
                neighborPosZ
        );
    }

    private static void seedFineArrivalTimesFromCoarse(float[] fineArrivalTimes,
                                                       float[] fineSlowness,
                                                       KrakkField fineField,
                                                       BitSet fineSourceMask,
                                                       KrakkCoarseSolveContext coarseContext) {
        KrakkField coarseField = coarseContext.coarseField();
        float[] coarseArrivalTimes = coarseContext.coarseArrivalTimes();
        int downsampleFactor = coarseContext.downsampleFactor();
        int fineSizeY = fineField.sizeY();
        int fineSizeZ = fineField.sizeZ();
        int coarseSizeY = coarseField.sizeY();
        int coarseSizeZ = coarseField.sizeZ();
        int fineStrideX = fineSizeY * fineSizeZ;
        int fineStrideY = fineSizeZ;
        int maxCoarseX = coarseField.sizeX() - 1;
        int maxCoarseY = coarseField.sizeY() - 1;
        int maxCoarseZ = coarseField.sizeZ() - 1;
        int[] activeIndices = fineField.activeRowIndices();
        for (int fineIndex : activeIndices) {
            if (fineSourceMask.get(fineIndex) || fineSlowness[fineIndex] <= 0.0F) {
                continue;
            }
            int fineX = fineIndex / fineStrideX;
            int fineYZ = fineIndex - (fineX * fineStrideX);
            int fineY = fineYZ / fineStrideY;
            int fineZ = fineYZ - (fineY * fineStrideY);

            int coarseX = Math.min(maxCoarseX, fineX / downsampleFactor);
            int coarseY = Math.min(maxCoarseY, fineY / downsampleFactor);
            int coarseZ = Math.min(maxCoarseZ, fineZ / downsampleFactor);
            int coarseIndex = ((coarseX * coarseSizeY) + coarseY) * coarseSizeZ + coarseZ;
            float coarseArrival = coarseArrivalTimes[coarseIndex];
            if (!Float.isFinite(coarseArrival)) {
                continue;
            }

            double coarseCenterX = (coarseX * downsampleFactor) + (downsampleFactor * 0.5D);
            double coarseCenterY = (coarseY * downsampleFactor) + (downsampleFactor * 0.5D);
            double coarseCenterZ = (coarseZ * downsampleFactor) + (downsampleFactor * 0.5D);
            double dx = (fineX + 0.5D) - coarseCenterX;
            double dy = (fineY + 0.5D) - coarseCenterY;
            double dz = (fineZ + 0.5D) - coarseCenterZ;
            double localDistance = Math.sqrt((dx * dx) + (dy * dy) + (dz * dz));
            float seededArrival = (float) (coarseArrival + (localDistance * Math.max(1.0E-6F, fineSlowness[fineIndex])));
            if (seededArrival < fineArrivalTimes[fineIndex]) {
                fineArrivalTimes[fineIndex] = seededArrival;
            }
        }
    }

    private static float[] resolveKrakkSlownessField(KrakkField field,
                                                       float uniformSlownessOverride,
                                                       float solidSlownessScale) {
        return resolvePairedKrakkSlownessField(
                field,
                uniformSlownessOverride,
                solidSlownessScale,
                solidSlownessScale
        ).normalSlowness();
    }

    private static PairedKrakkSlownessResult resolvePairedKrakkSlownessField(KrakkField field,
                                                                                  float uniformSlownessOverride,
                                                                                  float normalSolidSlownessScale,
                                                                                  float shadowSolidSlownessScale) {
        float[] baseSlowness = field.slowness();
        boolean uniformOverrideActive = Float.isFinite(uniformSlownessOverride) && uniformSlownessOverride > 0.0F;
        float resolvedNormalScale = Float.isFinite(normalSolidSlownessScale) && normalSolidSlownessScale > 0.0F
                ? normalSolidSlownessScale
                : 1.0F;
        float resolvedShadowScale = Float.isFinite(shadowSolidSlownessScale) && shadowSolidSlownessScale > 0.0F
                ? shadowSolidSlownessScale
                : 1.0F;
        if (!uniformOverrideActive
                && Math.abs(resolvedNormalScale - 1.0F) <= 1.0E-6F
                && Math.abs(resolvedShadowScale - 1.0F) <= 1.0E-6F) {
            return new PairedKrakkSlownessResult(baseSlowness, baseSlowness);
        }

        int[] activeIndices = field.activeRowIndices();
        if (uniformOverrideActive) {
            float[] resolvedSlowness = new float[baseSlowness.length];
            for (int index : activeIndices) {
                if (baseSlowness[index] > 0.0F) {
                    resolvedSlowness[index] = uniformSlownessOverride;
                }
            }
            return new PairedKrakkSlownessResult(resolvedSlowness, resolvedSlowness);
        }

        boolean normalIsBase = Math.abs(resolvedNormalScale - 1.0F) <= 1.0E-6F;
        boolean shadowIsBase = Math.abs(resolvedShadowScale - 1.0F) <= 1.0E-6F;
        if (!normalIsBase && !shadowIsBase && Math.abs(resolvedNormalScale - resolvedShadowScale) <= 1.0E-6F) {
            float[] resolvedSlowness = new float[baseSlowness.length];
            applyKrakkSlownessScale(resolvedSlowness, baseSlowness, activeIndices, resolvedNormalScale);
            return new PairedKrakkSlownessResult(resolvedSlowness, resolvedSlowness);
        }

        float[] normalSlowness = normalIsBase ? baseSlowness : new float[baseSlowness.length];
        float[] shadowSlowness = shadowIsBase ? baseSlowness : new float[baseSlowness.length];
        if (!normalIsBase) {
            applyKrakkSlownessScale(normalSlowness, baseSlowness, activeIndices, resolvedNormalScale);
        }
        if (!shadowIsBase) {
            applyKrakkSlownessScale(shadowSlowness, baseSlowness, activeIndices, resolvedShadowScale);
        }
        return new PairedKrakkSlownessResult(normalSlowness, shadowSlowness);
    }

    private static void applyKrakkSlownessScale(float[] outputSlowness,
                                                  float[] baseSlowness,
                                                  int[] activeIndices,
                                                  float scale) {
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        for (int index : activeIndices) {
            float base = baseSlowness[index];
            if (base <= 0.0F) {
                continue;
            }
            float solidOverrun = Math.max(0.0F, base - airSlowness);
            if (solidOverrun <= 1.0E-6F) {
                outputSlowness[index] = base;
                continue;
            }
            outputSlowness[index] = airSlowness + (solidOverrun * scale);
        }
    }

    private static int initializeKrakkSources(float[] arrivalTimes,
                                                BitSet sourceMask,
                                                KrakkField field,
                                                BitSet traversableMask,
                                                float[] resolvedSlowness,
                                                double centerX,
                                                double centerY,
                                                double centerZ) {
        PairedKrakkSourceResult sourceResult = initializePairedKrakkSources(
                arrivalTimes,
                sourceMask,
                resolvedSlowness,
                null,
                null,
                null,
                field,
                traversableMask,
                centerX,
                centerY,
                centerZ
        );
        return sourceResult.normalSourceCount();
    }

    private static PairedKrakkSourceResult initializePairedKrakkSources(float[] normalArrivalTimes,
                                                                             BitSet normalSourceMask,
                                                                             float[] normalSlowness,
                                                                             float[] shadowArrivalTimes,
                                                                             BitSet shadowSourceMask,
                                                                             float[] shadowSlowness,
                                                                             KrakkField field,
                                                                             BitSet traversableMask,
                                                                             double centerX,
                                                                             double centerY,
                                                                             double centerZ) {
        boolean computeShadow = shadowArrivalTimes != null && shadowSourceMask != null && shadowSlowness != null;
        int baseX = Mth.floor(centerX) - field.minX();
        int baseY = Mth.floor(centerY) - field.minY();
        int baseZ = Mth.floor(centerZ) - field.minZ();
        int minX = Math.max(0, baseX - 1);
        int maxX = Math.min(field.sizeX() - 1, baseX + 1);
        int minY = Math.max(0, baseY - 1);
        int maxY = Math.min(field.sizeY() - 1, baseY + 1);
        int minZ = Math.max(0, baseZ - 1);
        int maxZ = Math.min(field.sizeZ() - 1, baseZ + 1);
        int normalSourceCount = 0;
        int shadowSourceCount = 0;
        for (int x = minX; x <= maxX; x++) {
            for (int y = minY; y <= maxY; y++) {
                for (int z = minZ; z <= maxZ; z++) {
                    int index = krakkGridIndex(x, y, z, field.sizeY(), field.sizeZ());
                    if (!traversableMask.get(index)) {
                        continue;
                    }
                    double worldX = field.minX() + x + 0.5D;
                    double worldY = field.minY() + y + 0.5D;
                    double worldZ = field.minZ() + z + 0.5D;
                    double dx = worldX - centerX;
                    double dy = worldY - centerY;
                    double dz = worldZ - centerZ;
                    double distance = Math.sqrt((dx * dx) + (dy * dy) + (dz * dz));

                    float normalCellSlowness = normalSlowness[index];
                    if (normalCellSlowness > 0.0F) {
                        double normalArrival = distance * normalCellSlowness;
                        if (normalArrival + 1.0E-6D < normalArrivalTimes[index]) {
                            if (!normalSourceMask.get(index)) {
                                normalSourceMask.set(index);
                                normalSourceCount++;
                            }
                            normalArrivalTimes[index] = (float) normalArrival;
                        }
                    }

                    if (computeShadow) {
                        float shadowCellSlowness = shadowSlowness[index];
                        if (shadowCellSlowness <= 0.0F) {
                            continue;
                        }
                        double shadowArrival = distance * shadowCellSlowness;
                        if (shadowArrival + 1.0E-6D < shadowArrivalTimes[index]) {
                            if (!shadowSourceMask.get(index)) {
                                shadowSourceMask.set(index);
                                shadowSourceCount++;
                            }
                            shadowArrivalTimes[index] = (float) shadowArrival;
                        }
                    }
                }
            }
        }

        int seedIndex = -1;
        if (normalSourceCount <= 0 || (computeShadow && shadowSourceCount <= 0)) {
            seedIndex = resolveKrakkSeedIndex(field, traversableMask, centerX, centerY, centerZ);
        }
        if (normalSourceCount <= 0 && seedIndex >= 0 && normalSlowness[seedIndex] > 0.0F) {
            normalArrivalTimes[seedIndex] = 0.0F;
            normalSourceMask.set(seedIndex);
            normalSourceCount = 1;
        }
        if (computeShadow && shadowSourceCount <= 0 && seedIndex >= 0 && shadowSlowness[seedIndex] > 0.0F) {
            shadowArrivalTimes[seedIndex] = 0.0F;
            shadowSourceMask.set(seedIndex);
            shadowSourceCount = 1;
        }
        return new PairedKrakkSourceResult(normalSourceCount, shadowSourceCount);
    }

    private static int computeKrakkMaxSweepCycles(KrakkField field) {
        int longestAxis = Math.max(field.sizeX(), Math.max(field.sizeY(), field.sizeZ()));
        int growthCycles = Math.max(0, Mth.ceil((longestAxis - 32) / 12.0D));
        return Mth.clamp(KRAKK_BASE_SWEEP_CYCLES + growthCycles, KRAKK_BASE_SWEEP_CYCLES, KRAKK_MAX_SWEEP_CYCLES);
    }

    private static int resolveKrakkSeedIndex(KrakkField field,
                                               BitSet traversableMask,
                                               double centerX,
                                               double centerY,
                                               double centerZ) {
        int seedX = Mth.clamp(Mth.floor(centerX), field.minX(), field.minX() + field.sizeX() - 1);
        int seedY = Mth.clamp(Mth.floor(centerY), field.minY(), field.minY() + field.sizeY() - 1);
        int seedZ = Mth.clamp(Mth.floor(centerZ), field.minZ(), field.minZ() + field.sizeZ() - 1);
        int seedIndex = krakkGridIndex(seedX - field.minX(), seedY - field.minY(), seedZ - field.minZ(), field.sizeY(), field.sizeZ());
        if (traversableMask.get(seedIndex)) {
            return seedIndex;
        }

        double bestDistanceSq = Double.POSITIVE_INFINITY;
        int bestIndex = -1;
        for (int x = 0; x < field.sizeX(); x++) {
            for (int y = 0; y < field.sizeY(); y++) {
                for (int z = 0; z < field.sizeZ(); z++) {
                    int index = krakkGridIndex(x, y, z, field.sizeY(), field.sizeZ());
                    if (!traversableMask.get(index)) {
                        continue;
                    }
                    double worldX = field.minX() + x + 0.5D;
                    double worldY = field.minY() + y + 0.5D;
                    double worldZ = field.minZ() + z + 0.5D;
                    double dx = worldX - centerX;
                    double dy = worldY - centerY;
                    double dz = worldZ - centerZ;
                    double distSq = (dx * dx) + (dy * dy) + (dz * dz);
                    if (distSq < bestDistanceSq) {
                        bestDistanceSq = distSq;
                        bestIndex = index;
                    }
                }
            }
        }
        return bestIndex;
    }

    private static double sweepKrakkPass(float[] arrivalTimes,
                                           float[] resolvedSlowness,
                                           KrakkField field,
                                           BitSet sourceMask,
                                           boolean allowParallelSweep,
                                           boolean xForward,
                                           boolean yForward,
                                           boolean zForward,
                                           BitSet activeRowsMask,
                                           BitSet dirtyRowsOut) {
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        int diagonalCount = sizeX + sizeY - 1;
        int[] activeRowOffsets = field.activeRowOffsets();
        int[] activeRowLengths = field.activeRowLengths();
        int[] activeRowIndices = field.activeRowIndices();
        int[] neighborNegX = field.neighborNegX();
        int[] neighborPosX = field.neighborPosX();
        int[] neighborNegY = field.neighborNegY();
        int[] neighborPosY = field.neighborPosY();
        int[] neighborNegZ = field.neighborNegZ();
        int[] neighborPosZ = field.neighborPosZ();
        double maxDelta = 0.0D;
        for (int diagonal = 0; diagonal < diagonalCount; diagonal++) {
            int minXOrder = Math.max(0, diagonal - (sizeY - 1));
            int maxXOrder = Math.min(sizeX - 1, diagonal);
            int rowSpan = maxXOrder - minXOrder + 1;
            int taskCount = resolveKrakkSweepTaskCount(rowSpan);
            if (!allowParallelSweep || taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
                KrakkSweepChunkResult chunkResult = sweepKrakkDiagonalChunk(
                        arrivalTimes,
                        resolvedSlowness,
                        field,
                        sourceMask,
                        xForward,
                        yForward,
                        zForward,
                        diagonal,
                        minXOrder,
                        maxXOrder + 1,
                        activeRowOffsets,
                        activeRowLengths,
                        activeRowIndices,
                        neighborNegX,
                        neighborPosX,
                        neighborNegY,
                        neighborPosY,
                        neighborNegZ,
                        neighborPosZ,
                        activeRowsMask
                );
                maxDelta = Math.max(maxDelta, chunkResult.maxDelta());
                mergeKrakkDirtyRows(dirtyRowsOut, chunkResult.dirtyRows());
                continue;
            }

            int chunkSize = (rowSpan + taskCount - 1) / taskCount;
            ArrayList<Future<KrakkSweepChunkResult>> futures = new ArrayList<>(taskCount);
            final int diagonalIndex = diagonal;
            for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
                int startXOrder = minXOrder + (taskIndex * chunkSize);
                int endXOrder = Math.min(maxXOrder + 1, startXOrder + chunkSize);
                if (startXOrder >= endXOrder) {
                    break;
                }
                final int chunkStartXOrder = startXOrder;
                final int chunkEndXOrder = endXOrder;
                futures.add(VOLUMETRIC_TARGET_SCAN_POOL.submit(
                        () -> sweepKrakkDiagonalChunk(
                                arrivalTimes,
                                resolvedSlowness,
                                field,
                                sourceMask,
                                xForward,
                                yForward,
                                zForward,
                                diagonalIndex,
                                chunkStartXOrder,
                                chunkEndXOrder,
                                activeRowOffsets,
                                activeRowLengths,
                                activeRowIndices,
                                neighborNegX,
                                neighborPosX,
                                neighborNegY,
                                neighborPosY,
                                neighborNegZ,
                                neighborPosZ,
                                activeRowsMask
                        )
                ));
            }

            try {
                for (Future<KrakkSweepChunkResult> future : futures) {
                    KrakkSweepChunkResult chunkResult = future.get();
                    maxDelta = Math.max(maxDelta, chunkResult.maxDelta());
                    mergeKrakkDirtyRows(dirtyRowsOut, chunkResult.dirtyRows());
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                cancelKrakkSweepFutures(futures);
            } catch (ExecutionException exception) {
                cancelKrakkSweepFutures(futures);
            }
        }
        return maxDelta;
    }

    private static KrakkSweepChunkResult sweepKrakkDiagonalChunk(float[] arrivalTimes,
                                                                     float[] resolvedSlowness,
                                                                     KrakkField field,
                                                                     BitSet sourceMask,
                                                                     boolean xForward,
                                                                     boolean yForward,
                                                                     boolean zForward,
                                                                     int diagonal,
                                                                     int startXOrderInclusive,
                                                                     int endXOrderExclusive,
                                                                     int[] activeRowOffsets,
                                                                     int[] activeRowLengths,
                                                                     int[] activeRowIndices,
                                                                     int[] neighborNegX,
                                                                     int[] neighborPosX,
                                                                     int[] neighborNegY,
                                                                     int[] neighborPosY,
                                                                     int[] neighborNegZ,
                                                                     int[] neighborPosZ,
                                                                     BitSet activeRowsMask) {
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        double maxDelta = 0.0D;
        IntArrayList dirtyRows = new IntArrayList(Math.max(4, endXOrderExclusive - startXOrderInclusive));
        for (int xOrder = startXOrderInclusive; xOrder < endXOrderExclusive; xOrder++) {
            int yOrder = diagonal - xOrder;
            if (yOrder < 0 || yOrder >= sizeY) {
                continue;
            }
            int x = xForward ? xOrder : (sizeX - 1 - xOrder);
            int y = yForward ? yOrder : (sizeY - 1 - yOrder);
            int rowIndex = krakkRowIndex(x, y, sizeY);
            if (activeRowsMask != null && !activeRowsMask.get(rowIndex)) {
                continue;
            }
            int rowOffset = activeRowOffsets[rowIndex];
            int rowLength = activeRowLengths[rowIndex];
            if (rowLength <= 0) {
                continue;
            }
            boolean rowChanged = false;
            if (zForward) {
                int rowEnd = rowOffset + rowLength;
                for (int i = rowOffset; i < rowEnd; i++) {
                    int index = activeRowIndices[i];
                    if (sourceMask.get(index)) {
                        continue;
                    }
                    double candidate = solveKrakkCell(
                            arrivalTimes,
                            resolvedSlowness,
                            index,
                            neighborNegX,
                            neighborPosX,
                            neighborNegY,
                            neighborPosY,
                            neighborNegZ,
                            neighborPosZ
                    );
                    if (!Double.isFinite(candidate)) {
                        continue;
                    }
                    double previous = arrivalTimes[index];
                    if (candidate + 1.0E-9D < previous) {
                        arrivalTimes[index] = (float) candidate;
                        rowChanged = true;
                        if (Double.isFinite(previous)) {
                            maxDelta = Math.max(maxDelta, previous - candidate);
                        } else {
                            maxDelta = Math.max(maxDelta, candidate);
                        }
                    }
                }
                if (rowChanged) {
                    dirtyRows.add(rowIndex);
                }
                continue;
            }
            for (int i = rowOffset + rowLength - 1; i >= rowOffset; i--) {
                int index = activeRowIndices[i];
                if (sourceMask.get(index)) {
                    continue;
                }
                double candidate = solveKrakkCell(
                        arrivalTimes,
                        resolvedSlowness,
                        index,
                        neighborNegX,
                        neighborPosX,
                        neighborNegY,
                        neighborPosY,
                        neighborNegZ,
                        neighborPosZ
                );
                if (!Double.isFinite(candidate)) {
                    continue;
                }
                double previous = arrivalTimes[index];
                if (candidate + 1.0E-9D < previous) {
                    arrivalTimes[index] = (float) candidate;
                    rowChanged = true;
                    if (Double.isFinite(previous)) {
                        maxDelta = Math.max(maxDelta, previous - candidate);
                    } else {
                        maxDelta = Math.max(maxDelta, candidate);
                    }
                }
            }
            if (rowChanged) {
                dirtyRows.add(rowIndex);
            }
        }
        return new KrakkSweepChunkResult(maxDelta, dirtyRows);
    }

    private static void mergeKrakkDirtyRows(BitSet dirtyRowsOut, IntArrayList dirtyRows) {
        if (dirtyRowsOut == null || dirtyRows == null || dirtyRows.isEmpty()) {
            return;
        }
        for (int i = 0; i < dirtyRows.size(); i++) {
            dirtyRowsOut.set(dirtyRows.getInt(i));
        }
    }

    private static double solveKrakkCell(float[] arrivalTimes,
                                           float[] resolvedSlowness,
                                           int index,
                                           int[] neighborNegX,
                                           int[] neighborPosX,
                                           int[] neighborNegY,
                                           int[] neighborPosY,
                                           int[] neighborNegZ,
                                           int[] neighborPosZ) {
        double a = Double.POSITIVE_INFINITY;
        int negX = neighborNegX[index];
        int posX = neighborPosX[index];
        if (negX >= 0) {
            a = Math.min(a, arrivalTimes[negX]);
        }
        if (posX >= 0) {
            a = Math.min(a, arrivalTimes[posX]);
        }

        double b = Double.POSITIVE_INFINITY;
        int negY = neighborNegY[index];
        int posY = neighborPosY[index];
        if (negY >= 0) {
            b = Math.min(b, arrivalTimes[negY]);
        }
        if (posY >= 0) {
            b = Math.min(b, arrivalTimes[posY]);
        }

        double c = Double.POSITIVE_INFINITY;
        int negZ = neighborNegZ[index];
        int posZ = neighborPosZ[index];
        if (negZ >= 0) {
            c = Math.min(c, arrivalTimes[negZ]);
        }
        if (posZ >= 0) {
            c = Math.min(c, arrivalTimes[posZ]);
        }

        if (!Double.isFinite(a) && !Double.isFinite(b) && !Double.isFinite(c)) {
            return Double.POSITIVE_INFINITY;
        }
        return solveKrakkDistance(a, b, c, resolvedSlowness[index]);
    }

    private static double solveKrakkCellFromAccepted(float[] arrivalTimes,
                                                       float[] resolvedSlowness,
                                                       BitSet accepted,
                                                       int index,
                                                       int[] neighborNegX,
                                                       int[] neighborPosX,
                                                       int[] neighborNegY,
                                                       int[] neighborPosY,
                                                       int[] neighborNegZ,
                                                       int[] neighborPosZ) {
        double a = minAcceptedKrakkAxisNeighbor(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                neighborNegX[index],
                neighborPosX[index]
        );
        double b = minAcceptedKrakkAxisNeighbor(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                neighborNegY[index],
                neighborPosY[index]
        );
        double c = minAcceptedKrakkAxisNeighbor(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                neighborNegZ[index],
                neighborPosZ[index]
        );
        if (!Double.isFinite(a) && !Double.isFinite(b) && !Double.isFinite(c)) {
            return Double.POSITIVE_INFINITY;
        }
        return solveKrakkDistance(a, b, c, resolvedSlowness[index]);
    }

    private static void updateKrakkNeighborFromAccepted(float[] arrivalTimes,
                                                          float[] resolvedSlowness,
                                                          BitSet accepted,
                                                          KrakkMinHeap heap,
                                                          int neighborIndex,
                                                          int[] neighborNegX,
                                                          int[] neighborPosX,
                                                          int[] neighborNegY,
                                                          int[] neighborPosY,
                                                          int[] neighborNegZ,
                                                          int[] neighborPosZ) {
        if (neighborIndex < 0 || accepted.get(neighborIndex) || resolvedSlowness[neighborIndex] <= 0.0F) {
            return;
        }
        double candidate = solveKrakkCellFromAccepted(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                neighborIndex,
                neighborNegX,
                neighborPosX,
                neighborNegY,
                neighborPosY,
                neighborNegZ,
                neighborPosZ
        );
        if (!Double.isFinite(candidate)) {
            return;
        }
        if (candidate + 1.0E-9D < arrivalTimes[neighborIndex]) {
            arrivalTimes[neighborIndex] = (float) candidate;
            heap.add(neighborIndex, candidate);
        }
    }

    private static double minAcceptedKrakkAxisNeighbor(float[] arrivalTimes,
                                                         float[] resolvedSlowness,
                                                         BitSet accepted,
                                                         int negativeIndex,
                                                         int positiveIndex) {
        double min = Double.POSITIVE_INFINITY;
        if (negativeIndex >= 0 && accepted.get(negativeIndex) && resolvedSlowness[negativeIndex] > 0.0F) {
            min = Math.min(min, arrivalTimes[negativeIndex]);
        }
        if (positiveIndex >= 0 && accepted.get(positiveIndex) && resolvedSlowness[positiveIndex] > 0.0F) {
            min = Math.min(min, arrivalTimes[positiveIndex]);
        }
        return min;
    }

    private static double solveKrakkDistance(double a, double b, double c, double slowness) {
        double t0 = a;
        double t1 = b;
        double t2 = c;
        if (t0 > t1) {
            double swap = t0;
            t0 = t1;
            t1 = swap;
        }
        if (t1 > t2) {
            double swap = t1;
            t1 = t2;
            t2 = swap;
        }
        if (t0 > t1) {
            double swap = t0;
            t0 = t1;
            t1 = swap;
        }
        if (!Double.isFinite(t0)) {
            return Double.POSITIVE_INFINITY;
        }

        double s = Math.max(1.0E-6D, slowness);
        if (!Double.isFinite(t1)) {
            return t0 + s;
        }
        double result = t0 + s;
        if (result > t1) {
            double discriminant2 = (2.0D * s * s) - square(t0 - t1);
            if (discriminant2 > 0.0D) {
                result = (t0 + t1 + Math.sqrt(discriminant2)) * 0.5D;
            } else {
                result = t1 + s;
            }
            if (Double.isFinite(t2) && result > t2) {
                double discriminant3 = (3.0D * s * s)
                        - square(t0 - t1)
                        - square(t0 - t2)
                        - square(t1 - t2);
                if (discriminant3 > 0.0D) {
                    result = (t0 + t1 + t2 + Math.sqrt(discriminant3)) / 3.0D;
                } else {
                    result = t2 + s;
                }
            }
        }
        return result;
    }

    private static double square(double value) {
        return value * value;
    }

    private static int krakkGridIndex(int xOffset, int yOffset, int zOffset, int sizeY, int sizeZ) {
        return ((xOffset * sizeY) + yOffset) * sizeZ + zOffset;
    }

    private static int krakkRowIndex(int xOffset, int yOffset, int sizeY) {
        return (xOffset * sizeY) + yOffset;
    }

    private static void applyStructuralCollapsePass(ServerLevel level,
                                                    double centerX,
                                                    double centerY,
                                                    double centerZ,
                                                    double radiusSq,
                                                    int minX,
                                                    int maxX,
                                                    int minY,
                                                    int maxY,
                                                    int minZ,
                                                    int maxZ,
                                                    LongArrayList candidateSolidPositions,
                                                    Long2FloatOpenHashMap collapseWeightsByPos,
                                                    double impactBudget,
                                                    double normalizationWeight,
                                                    Entity source,
                                                    LivingEntity owner,
                                                    boolean applyWorldChanges,
                                                    ExplosionProfileTrace trace) {
        if (!applyWorldChanges && trace == null) {
            return;
        }
        if (candidateSolidPositions.isEmpty()) {
            return;
        }
        if (collapseWeightsByPos == null
                || collapseWeightsByPos.isEmpty()
                || impactBudget <= MIN_RESOLVED_RAY_IMPACT
                || normalizationWeight <= VOLUMETRIC_MIN_ENERGY) {
            return;
        }

        int sizeX = maxX - minX + 1;
        int sizeY = maxY - minY + 1;
        int sizeZ = maxZ - minZ + 1;
        long volumeLong = (long) sizeX * (long) sizeY * (long) sizeZ;
        if (volumeLong <= 0L || volumeLong > STRUCTURAL_COLLAPSE_MAX_VOXELS) {
            return;
        }

        int yzStride = sizeY * sizeZ;
        BitSet unsupportedMask = new BitSet((int) volumeLong);
        IntArrayList bfsQueue = new IntArrayList(Math.max(64, candidateSolidPositions.size() / 32));
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        long collapseSeedStart = trace != null ? System.nanoTime() : 0L;
        for (int i = 0; i < candidateSolidPositions.size(); i++) {
            long posLong = candidateSolidPositions.getLong(i);
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            if (x < minX || x > maxX || y < minY || y > maxY || z < minZ || z > maxZ) {
                continue;
            }

            int index = krakkGridIndex(x - minX, y - minY, z - minZ, sizeY, sizeZ);
            if (unsupportedMask.get(index)) {
                continue;
            }

            mutablePos.set(x, y, z);
            if (level.getBlockState(mutablePos).isAir()) {
                continue;
            }

            unsupportedMask.set(index);
            if (isStructuralBoundaryVoxel(x, y, z, centerX, centerY, centerZ, radiusSq)) {
                unsupportedMask.clear(index);
                bfsQueue.add(index);
            }
        }
        if (trace != null) {
            trace.volumetricImpactApplyCollapseSeedNanos += (System.nanoTime() - collapseSeedStart);
        }

        long collapseBfsStart = trace != null ? System.nanoTime() : 0L;
        for (int head = 0; head < bfsQueue.size(); head++) {
            int index = bfsQueue.getInt(head);
            int xOffset = index / yzStride;
            int rem = index - (xOffset * yzStride);
            int yOffset = rem / sizeZ;
            int zOffset = rem - (yOffset * sizeZ);

            if (xOffset > 0) {
                int neighbor = index - yzStride;
                if (unsupportedMask.get(neighbor)) {
                    unsupportedMask.clear(neighbor);
                    bfsQueue.add(neighbor);
                }
            }
            if (xOffset + 1 < sizeX) {
                int neighbor = index + yzStride;
                if (unsupportedMask.get(neighbor)) {
                    unsupportedMask.clear(neighbor);
                    bfsQueue.add(neighbor);
                }
            }
            if (yOffset > 0) {
                int neighbor = index - sizeZ;
                if (unsupportedMask.get(neighbor)) {
                    unsupportedMask.clear(neighbor);
                    bfsQueue.add(neighbor);
                }
            }
            if (yOffset + 1 < sizeY) {
                int neighbor = index + sizeZ;
                if (unsupportedMask.get(neighbor)) {
                    unsupportedMask.clear(neighbor);
                    bfsQueue.add(neighbor);
                }
            }
            if (zOffset > 0) {
                int neighbor = index - 1;
                if (unsupportedMask.get(neighbor)) {
                    unsupportedMask.clear(neighbor);
                    bfsQueue.add(neighbor);
                }
            }
            if (zOffset + 1 < sizeZ) {
                int neighbor = index + 1;
                if (unsupportedMask.get(neighbor)) {
                    unsupportedMask.clear(neighbor);
                    bfsQueue.add(neighbor);
                }
            }
        }
        if (trace != null) {
            trace.volumetricImpactApplyCollapseBfsNanos += (System.nanoTime() - collapseBfsStart);
        }

        if (unsupportedMask.isEmpty()) {
            return;
        }

        double collapseImpactScale = (impactBudget / normalizationWeight) * STRUCTURAL_COLLAPSE_IMPACT_WEIGHT_SCALE;
        BlockPos.MutableBlockPos collapsePos = new BlockPos.MutableBlockPos();
        long collapseApplyStart = trace != null ? System.nanoTime() : 0L;
        for (int index = unsupportedMask.nextSetBit(0); index >= 0; index = unsupportedMask.nextSetBit(index + 1)) {
            int xOffset = index / yzStride;
            int rem = index - (xOffset * yzStride);
            int yOffset = rem / sizeZ;
            int zOffset = rem - (yOffset * sizeZ);
            long posLong = BlockPos.asLong(minX + xOffset, minY + yOffset, minZ + zOffset);
            float collapseWeight = collapseWeightsByPos.get(posLong);
            if (collapseWeight <= VOLUMETRIC_MIN_ENERGY) {
                continue;
            }
            double collapseImpactPower = collapseImpactScale * collapseWeight;
            if (collapseImpactPower <= MIN_RESOLVED_RAY_IMPACT) {
                continue;
            }
            applySingleBlockImpact(
                    level,
                    collapsePos,
                    posLong,
                    collapseImpactPower,
                    source,
                    owner,
                    applyWorldChanges,
                    trace
            );
        }
        if (trace != null) {
            trace.volumetricImpactApplyCollapseApplyNanos += (System.nanoTime() - collapseApplyStart);
        }
    }

    private static boolean isStructuralBoundaryVoxel(int x,
                                                     int y,
                                                     int z,
                                                     double centerX,
                                                     double centerY,
                                                     double centerZ,
                                                     double radiusSq) {
        double cx = (x + 0.5D) - centerX;
        double cy = (y + 0.5D) - centerY;
        double cz = (z + 0.5D) - centerZ;
        if ((square(cx + 1.0D) + square(cy) + square(cz)) > radiusSq) {
            return true;
        }
        if ((square(cx - 1.0D) + square(cy) + square(cz)) > radiusSq) {
            return true;
        }
        if ((square(cx) + square(cy + 1.0D) + square(cz)) > radiusSq) {
            return true;
        }
        if ((square(cx) + square(cy - 1.0D) + square(cz)) > radiusSq) {
            return true;
        }
        if ((square(cx) + square(cy) + square(cz + 1.0D)) > radiusSq) {
            return true;
        }
        return (square(cx) + square(cy) + square(cz - 1.0D)) > radiusSq;
    }

    private static void runVolumetricPropagation(ServerLevel level,
                                                 double centerX,
                                                 double centerY,
                                                 double centerZ,
                                                 double blastRadius,
                                                 double totalEnergy,
                                                 Entity source,
                                                 LivingEntity owner,
                                                 boolean applyWorldChanges,
                                                 ExplosionProfileTrace trace) {
        BlockPos centerPos = BlockPos.containing(centerX, centerY, centerZ);
        if (!level.isInWorldBounds(centerPos)) {
            return;
        }

        double resolvedRadius = Mth.clamp(blastRadius, 1.0D, VOLUMETRIC_MAX_RADIUS);
        double radiusSq = resolvedRadius * resolvedRadius;
        int minX = Mth.floor(centerX - resolvedRadius);
        int maxX = Mth.ceil(centerX + resolvedRadius);
        int minY = Mth.floor(centerY - resolvedRadius);
        int maxY = Mth.ceil(centerY + resolvedRadius);
        int minZ = Mth.floor(centerZ - resolvedRadius);
        int maxZ = Mth.ceil(centerZ + resolvedRadius);
        Int2DoubleOpenHashMap resistanceCostCache = new Int2DoubleOpenHashMap(256);
        resistanceCostCache.defaultReturnValue(Double.NaN);
        long resistanceFieldStart = trace != null ? System.nanoTime() : 0L;
        VolumetricResistanceField resistanceFieldData = buildVolumetricResistanceField(
                level,
                centerX,
                centerY,
                centerZ,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ,
                radiusSq,
                resistanceCostCache
        );
        Long2FloatOpenHashMap resistanceField = resistanceFieldData.resistanceByPos();
        LongArrayList solidPositions = resistanceFieldData.solidPositions();
        int sampledVoxelCount = resistanceFieldData.sampledVoxelCount();
        if (trace != null) {
            trace.volumetricResistanceFieldNanos += (System.nanoTime() - resistanceFieldStart);
        }
        if (resistanceField.isEmpty()) {
            return;
        }

        long directionSetupStart = trace != null ? System.nanoTime() : 0L;
        double pointScale = computeVolumetricRadiusScale(resolvedRadius, VOLUMETRIC_MAX_POINT_RADIUS_SCALE);
        int directionCount = computeVolumetricDirectionSampleCount(resolvedRadius, pointScale);
        VolumetricDirectionCache directionCache = getVolumetricDirectionCache(directionCount);
        double[] dirX = directionCache.dirX();
        double[] dirY = directionCache.dirY();
        double[] dirZ = directionCache.dirZ();
        int[][] directionNeighbors = directionCache.neighbors();
        int[] directionLookup = directionCache.directionLookup();
        int directionLookupResolution = directionCache.directionLookupResolution();
        int radialSteps = Mth.clamp(
                Mth.ceil(resolvedRadius / VOLUMETRIC_DEFORM_SAMPLE_STEP),
                1,
                VOLUMETRIC_MAX_RADIAL_STEPS
        );
        double radialStepSize = resolvedRadius / radialSteps;
        float[] pressureByShell = new float[(radialSteps + 1) * directionCount];
        Arrays.fill(pressureByShell, 0, directionCount, 1.0F);
        float[] maxPressureByShell = new float[radialSteps + 1];
        maxPressureByShell[0] = 1.0F;
        float[] previousPressure = new float[directionCount];
        Arrays.fill(previousPressure, 1.0F);
        float[] rawPressure = new float[directionCount];
        float[] currentPressure = new float[directionCount];
        double perStepAirDecay = Mth.clamp(VOLUMETRIC_PRESSURE_AIR_DECAY_PER_BLOCK * radialStepSize, 0.0D, 0.95D);
        double perStepRecovery = Math.max(0.0D, VOLUMETRIC_PRESSURE_RECOVERY_PER_BLOCK * radialStepSize);
        if (trace != null) {
            trace.volumetricDirectionSetupNanos += (System.nanoTime() - directionSetupStart);
            trace.volumetricDirectionSamples += directionCount;
            trace.volumetricRadialSteps += radialSteps;
        }

        long pressureSolveStart = trace != null ? System.nanoTime() : 0L;
        for (int shell = 1; shell <= radialSteps; shell++) {
            double t = shell * radialStepSize;
            for (int i = 0; i < directionCount; i++) {
                int sampleX = Mth.floor(centerX + (dirX[i] * t));
                int sampleY = Mth.floor(centerY + (dirY[i] * t));
                int sampleZ = Mth.floor(centerZ + (dirZ[i] * t));
                long samplePosLong = BlockPos.asLong(sampleX, sampleY, sampleZ);
                float sampleResistance = resistanceField.get(samplePosLong);
                double transmitted = previousPressure[i] * (1.0D - perStepAirDecay);
                transmitted -= sampleResistance * VOLUMETRIC_PRESSURE_RESISTANCE_LOSS_SCALE * radialStepSize;
                rawPressure[i] = (float) Mth.clamp(transmitted, 0.0D, 1.0D);
            }

            int rowOffset = shell * directionCount;
            float shellMaxPressure = 0.0F;
            for (int i = 0; i < directionCount; i++) {
                int[] neighbors = directionNeighbors[i];
                double neighborPressure = 0.0D;
                int validNeighbors = 0;
                for (int neighborIndex : neighbors) {
                    if (neighborIndex < 0) {
                        continue;
                    }
                    neighborPressure += rawPressure[neighborIndex];
                    validNeighbors++;
                }
                double neighborAverage = validNeighbors > 0 ? (neighborPressure / validNeighbors) : rawPressure[i];
                double diffusedPressure = Mth.lerp(VOLUMETRIC_PRESSURE_DIFFUSION, rawPressure[i], neighborAverage);
                double recoveredPressure = Math.min(diffusedPressure, previousPressure[i] + perStepRecovery);
                currentPressure[i] = (float) Mth.clamp(recoveredPressure, 0.0D, 1.0D);
                pressureByShell[rowOffset + i] = currentPressure[i];
                if (currentPressure[i] > shellMaxPressure) {
                    shellMaxPressure = currentPressure[i];
                }
            }
            maxPressureByShell[shell] = shellMaxPressure;

            float[] swap = previousPressure;
            previousPressure = currentPressure;
            currentPressure = swap;
        }
        if (trace != null) {
            trace.volumetricPressureSolveNanos += (System.nanoTime() - pressureSolveStart);
        }

        if (trace != null) {
            trace.initialRays = directionCount;
            trace.processedRays += directionCount;
        }
        LongArrayList targetPositions;
        FloatArrayList targetWeights;
        double solidWeight = 0.0D;
        int sampledSolidCount = solidPositions.size();
        long targetScanStart = trace != null ? System.nanoTime() : 0L;
        VolumetricTargetScanContext targetScanContext = new VolumetricTargetScanContext(
                centerX,
                centerY,
                centerZ,
                resolvedRadius,
                radialStepSize,
                radialSteps,
                pressureByShell,
                maxPressureByShell,
                directionCount,
                dirX,
                dirY,
                dirZ,
                directionNeighbors,
                directionLookup,
                directionLookupResolution
        );
        VolumetricTargetScanResult targetScanResult = scanVolumetricTargets(
                solidPositions,
                targetScanContext,
                true,
                trace != null
        );
        targetPositions = targetScanResult.targetPositions();
        targetWeights = targetScanResult.targetWeights();
        for (int i = 0; i < targetWeights.size(); i++) {
            solidWeight += targetWeights.getFloat(i);
        }
        if (trace != null) {
            trace.volumetricTargetScanNanos += (System.nanoTime() - targetScanStart);
            trace.volumetricTargetScanPrecheckNanos += targetScanResult.precheckNanos();
            trace.volumetricTargetScanBlendNanos += targetScanResult.blendNanos();
            trace.volumetricSampledVoxels += sampledVoxelCount;
            trace.volumetricSampledSolids += sampledSolidCount;
        }

        if (targetPositions.isEmpty()
                || solidWeight <= VOLUMETRIC_MIN_ENERGY
                || sampledSolidCount <= 0
                || sampledVoxelCount <= 0) {
            return;
        }
        if (trace != null) {
            trace.rawImpactedBlocks += targetPositions.size();
            trace.postAaImpactedBlocks += targetPositions.size();
            trace.volumetricTargetBlocks += targetPositions.size();
        }

        double powerScale = computeVolumetricRadiusScale(resolvedRadius, VOLUMETRIC_MAX_POWER_RADIUS_SCALE);
        double impactBudget = totalEnergy * VOLUMETRIC_IMPACT_POWER_PER_ENERGY * powerScale;
        double airNormalizationScale = computeVolumetricAirNormalizationScale(sampledSolidCount, sampledVoxelCount);
        double normalizationWeight = solidWeight * airNormalizationScale;
        Long2FloatOpenHashMap collapseWeightsByPos = new Long2FloatOpenHashMap(Math.max(16, targetPositions.size()));
        collapseWeightsByPos.defaultReturnValue(0.0F);
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        long impactApplyStart = trace != null ? System.nanoTime() : 0L;
        long impactApplyDirectStart = trace != null ? System.nanoTime() : 0L;
        for (int i = 0; i < targetPositions.size(); i++) {
            long targetPosLong = targetPositions.getLong(i);
            double weight = targetWeights.getFloat(i);
            if (weight <= VOLUMETRIC_MIN_ENERGY) {
                continue;
            }
            float existingCollapseWeight = collapseWeightsByPos.get(targetPosLong);
            if (weight > existingCollapseWeight) {
                collapseWeightsByPos.put(targetPosLong, (float) weight);
            }
            double impactPower = impactBudget * (weight / normalizationWeight);
            applySingleBlockImpact(
                    level,
                    mutablePos,
                    targetPosLong,
                    impactPower,
                    source,
                    owner,
                    applyWorldChanges,
                    trace
            );
        }
        if (trace != null) {
            trace.volumetricImpactApplyDirectNanos += (System.nanoTime() - impactApplyDirectStart);
        }
        applyStructuralCollapsePass(
                level,
                centerX,
                centerY,
                centerZ,
                radiusSq,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ,
                solidPositions,
                collapseWeightsByPos,
                impactBudget,
                normalizationWeight,
                source,
                owner,
                applyWorldChanges,
                trace
        );
        if (trace != null) {
            trace.volumetricImpactApplyNanos += (System.nanoTime() - impactApplyStart);
        }
    }

    private static VolumetricResistanceField buildVolumetricResistanceField(ServerLevel level,
                                                                            double centerX,
                                                                            double centerY,
                                                                            double centerZ,
                                                                            int minX,
                                                                            int maxX,
                                                                            int minY,
                                                                            int maxY,
                                                                            int minZ,
                                                                            int maxZ,
                                                                            double radiusSq,
                                                                            Int2DoubleOpenHashMap resistanceCostCache) {
        int estimatedEntries = estimateVolumetricResistanceFieldEntries(radiusSq);
        Long2FloatOpenHashMap resistanceField = new Long2FloatOpenHashMap(estimatedEntries);
        resistanceField.defaultReturnValue(0.0F);
        LongArrayList solidPositions = new LongArrayList(Math.max(64, (int) Math.ceil(estimatedEntries * 0.75D)));
        int sizeX = maxX - minX + 1;
        int sizeY = maxY - minY + 1;
        int sizeZ = maxZ - minZ + 1;
        int taskCount = resolveResistanceFieldTaskCount(sizeX, sizeY, sizeZ);
        if (taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
            VolumetricResistanceFieldChunkResult chunkResult = sampleVolumetricResistanceFieldChunk(
                    level,
                    centerX,
                    centerY,
                    centerZ,
                    minX,
                    minY,
                    minZ,
                    sizeY,
                    sizeZ,
                    radiusSq,
                    0,
                    sizeX,
                    resistanceCostCache
            );
            LongArrayList chunkPositions = chunkResult.solidPositions();
            FloatArrayList chunkResistance = chunkResult.solidResistance();
            for (int i = 0; i < chunkPositions.size(); i++) {
                long posLong = chunkPositions.getLong(i);
                resistanceField.put(posLong, chunkResistance.getFloat(i));
                solidPositions.add(posLong);
            }
            return solidPositions.isEmpty()
                    ? createEmptyVolumetricResistanceField(chunkResult.sampledVoxelCount())
                    : new VolumetricResistanceField(resistanceField, solidPositions, chunkResult.sampledVoxelCount());
        }

        ResistanceFieldSnapshot resistanceFieldSnapshot = sampleResistanceFieldSnapshot(
                level,
                centerX,
                centerY,
                centerZ,
                minX,
                minY,
                minZ,
                sizeX,
                sizeY,
                sizeZ,
                radiusSq
        );
        int chunkSize = (sizeX + taskCount - 1) / taskCount;
        ArrayList<Future<VolumetricResistanceFieldChunkResult>> futures = new ArrayList<>(taskCount);
        for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
            int startXOffset = taskIndex * chunkSize;
            int endXOffset = Math.min(sizeX, startXOffset + chunkSize);
            if (startXOffset >= endXOffset) {
                break;
            }
            final int chunkStartX = startXOffset;
            final int chunkEndX = endXOffset;
            futures.add(VOLUMETRIC_TARGET_SCAN_POOL.submit(
                    () -> sampleVolumetricResistanceFieldChunk(
                            resistanceFieldSnapshot,
                            minX,
                            minY,
                            minZ,
                            sizeY,
                            sizeZ,
                            chunkStartX,
                            chunkEndX,
                            null
                    )
            ));
        }

        int sampledVoxelCount = 0;
        try {
            for (Future<VolumetricResistanceFieldChunkResult> future : futures) {
                VolumetricResistanceFieldChunkResult chunkResult = future.get();
                sampledVoxelCount += chunkResult.sampledVoxelCount();
                LongArrayList chunkPositions = chunkResult.solidPositions();
                FloatArrayList chunkResistance = chunkResult.solidResistance();
                for (int i = 0; i < chunkPositions.size(); i++) {
                    long posLong = chunkPositions.getLong(i);
                    resistanceField.put(posLong, chunkResistance.getFloat(i));
                    solidPositions.add(posLong);
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            cancelVolumetricResistanceFieldFutures(futures);
            VolumetricResistanceFieldChunkResult fallback = sampleVolumetricResistanceFieldChunk(
                    resistanceFieldSnapshot,
                    minX,
                    minY,
                    minZ,
                    sizeY,
                    sizeZ,
                    0,
                    sizeX,
                    resistanceCostCache
            );
            LongArrayList fallbackPositions = fallback.solidPositions();
            FloatArrayList fallbackResistance = fallback.solidResistance();
            resistanceField.clear();
            solidPositions.clear();
            for (int i = 0; i < fallbackPositions.size(); i++) {
                long posLong = fallbackPositions.getLong(i);
                resistanceField.put(posLong, fallbackResistance.getFloat(i));
                solidPositions.add(posLong);
            }
            return solidPositions.isEmpty()
                    ? createEmptyVolumetricResistanceField(fallback.sampledVoxelCount())
                    : new VolumetricResistanceField(resistanceField, solidPositions, fallback.sampledVoxelCount());
        } catch (ExecutionException exception) {
            cancelVolumetricResistanceFieldFutures(futures);
            VolumetricResistanceFieldChunkResult fallback = sampleVolumetricResistanceFieldChunk(
                    resistanceFieldSnapshot,
                    minX,
                    minY,
                    minZ,
                    sizeY,
                    sizeZ,
                    0,
                    sizeX,
                    resistanceCostCache
            );
            LongArrayList fallbackPositions = fallback.solidPositions();
            FloatArrayList fallbackResistance = fallback.solidResistance();
            resistanceField.clear();
            solidPositions.clear();
            for (int i = 0; i < fallbackPositions.size(); i++) {
                long posLong = fallbackPositions.getLong(i);
                resistanceField.put(posLong, fallbackResistance.getFloat(i));
                solidPositions.add(posLong);
            }
            return solidPositions.isEmpty()
                    ? createEmptyVolumetricResistanceField(fallback.sampledVoxelCount())
                    : new VolumetricResistanceField(resistanceField, solidPositions, fallback.sampledVoxelCount());
        }

        if (solidPositions.isEmpty()) {
            return createEmptyVolumetricResistanceField(sampledVoxelCount);
        }
        return new VolumetricResistanceField(resistanceField, solidPositions, sampledVoxelCount);
    }

    private static int estimateVolumetricResistanceFieldEntries(double radiusSq) {
        double clampedRadiusSq = Math.max(0.0D, radiusSq);
        double radius = Math.sqrt(clampedRadiusSq);
        double sphereVolume = (4.0D / 3.0D) * Math.PI * radius * radius * radius;
        double estimatedSolidCount = Math.max(1.0D, sphereVolume * VOLUMETRIC_RESISTANCE_FIELD_PREALLOC_SOLID_FRACTION);
        return Math.max(64, (int) Math.min(Integer.MAX_VALUE - 8L, Math.ceil(estimatedSolidCount / 0.75D)));
    }

    private static VolumetricResistanceField createEmptyVolumetricResistanceField(int sampledVoxelCount) {
        Long2FloatOpenHashMap empty = new Long2FloatOpenHashMap(16);
        empty.defaultReturnValue(0.0F);
        return new VolumetricResistanceField(empty, new LongArrayList(), sampledVoxelCount);
    }

    private static VolumetricResistanceFieldChunkResult sampleVolumetricResistanceFieldChunk(ServerLevel level,
                                                                                              double centerX,
                                                                                              double centerY,
                                                                                              double centerZ,
                                                                                              int minX,
                                                                                              int minY,
                                                                                              int minZ,
                                                                                              int sizeY,
                                                                                              int sizeZ,
                                                                                              double radiusSq,
                                                                                              int startXOffsetInclusive,
                                                                                              int endXOffsetExclusive,
                                                                                              Int2DoubleOpenHashMap sharedResistanceCostCache) {
        int chunkWidth = Math.max(1, endXOffsetExclusive - startXOffsetInclusive);
        int expectedChunkEntries = Math.max(
                16,
                (int) Math.ceil((double) (chunkWidth * sizeY * sizeZ) * VOLUMETRIC_RESISTANCE_FIELD_PREALLOC_SOLID_FRACTION)
        );
        LongArrayList localPositions = new LongArrayList(expectedChunkEntries);
        FloatArrayList localResistance = new FloatArrayList(expectedChunkEntries);
        Int2DoubleOpenHashMap localResistanceCache = sharedResistanceCostCache != null
                ? sharedResistanceCostCache
                : new Int2DoubleOpenHashMap(128);
        if (sharedResistanceCostCache == null) {
            localResistanceCache.defaultReturnValue(Double.NaN);
        }
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        int sampledVoxelCount = 0;
        for (int xOffset = startXOffsetInclusive; xOffset < endXOffsetExclusive; xOffset++) {
            int x = minX + xOffset;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int y = minY + yOffset;
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int z = minZ + zOffset;
                    double dx = (x + 0.5D) - centerX;
                    double dy = (y + 0.5D) - centerY;
                    double dz = (z + 0.5D) - centerZ;
                    double distSq = (dx * dx) + (dy * dy) + (dz * dz);
                    if (distSq > radiusSq) {
                        continue;
                    }
                    mutablePos.set(x, y, z);
                    if (!level.isInWorldBounds(mutablePos)) {
                        continue;
                    }
                    sampledVoxelCount++;
                    BlockState blockState = level.getBlockState(mutablePos);
                    if (blockState.isAir()) {
                        continue;
                    }
                    localPositions.add(mutablePos.asLong());
                    localResistance.add((float) getCachedResistanceCost(localResistanceCache, blockState));
                }
            }
        }
        return new VolumetricResistanceFieldChunkResult(localPositions, localResistance, sampledVoxelCount);
    }

    private static KrakkResistanceFieldChunkResult sampleKrakkFieldChunk(ServerLevel level,
                                                                             double centerX,
                                                                             double centerY,
                                                                             double centerZ,
                                                                             int minX,
                                                                             int minY,
                                                                             int minZ,
                                                                             int sizeY,
                                                                             int sizeZ,
                                                                             double radiusSq,
                                                                             int startXOffsetInclusive,
                                                                             int endXOffsetExclusive,
                                                                             float[] slowness,
                                                                             int[] activeCountByRow,
                                                                             Int2DoubleOpenHashMap sharedResistanceCostCache) {
        Int2DoubleOpenHashMap localResistanceCache = sharedResistanceCostCache != null
                ? sharedResistanceCostCache
                : new Int2DoubleOpenHashMap(128);
        if (sharedResistanceCostCache == null) {
            localResistanceCache.defaultReturnValue(Double.NaN);
        }
        LongArrayList localSolidPositions = new LongArrayList(Math.max(16, (endXOffsetExclusive - startXOffsetInclusive) * sizeY));
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        int sampledVoxelCount = 0;
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        float solidScale = (float) KRAKK_SOLID_SLOWNESS_SCALE;
        for (int xOffset = startXOffsetInclusive; xOffset < endXOffsetExclusive; xOffset++) {
            int x = minX + xOffset;
            int baseX = xOffset * sizeY * sizeZ;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int y = minY + yOffset;
                int rowIndex = krakkRowIndex(xOffset, yOffset, sizeY);
                int baseIndex = baseX + (yOffset * sizeZ);
                int rowActiveCount = 0;
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int z = minZ + zOffset;
                    double dx = (x + 0.5D) - centerX;
                    double dy = (y + 0.5D) - centerY;
                    double dz = (z + 0.5D) - centerZ;
                    double distSq = (dx * dx) + (dy * dy) + (dz * dz);
                    if (distSq > radiusSq) {
                        continue;
                    }
                    mutablePos.set(x, y, z);
                    if (!level.isInWorldBounds(mutablePos)) {
                        continue;
                    }
                    sampledVoxelCount++;
                    int index = baseIndex + zOffset;
                    BlockState blockState = level.getBlockState(mutablePos);
                    if (blockState.isAir()) {
                        slowness[index] = airSlowness;
                        rowActiveCount++;
                        continue;
                    }
                    double resistance = getCachedResistanceCost(localResistanceCache, blockState);
                    slowness[index] = airSlowness + ((float) resistance * solidScale);
                    rowActiveCount++;
                    localSolidPositions.add(mutablePos.asLong());
                }
                activeCountByRow[rowIndex] = rowActiveCount;
            }
        }
        return new KrakkResistanceFieldChunkResult(localSolidPositions, sampledVoxelCount);
    }

    private static ResistanceFieldSnapshot sampleResistanceFieldSnapshot(ServerLevel level,
                                                                         double centerX,
                                                                         double centerY,
                                                                         double centerZ,
                                                                         int minX,
                                                                         int minY,
                                                                         int minZ,
                                                                         int sizeX,
                                                                         int sizeY,
                                                                         int sizeZ,
                                                                         double radiusSq) {
        long volumeLong = (long) sizeX * (long) sizeY * (long) sizeZ;
        if (volumeLong <= 0L || volumeLong > Integer.MAX_VALUE - 8L) {
            return new ResistanceFieldSnapshot(new BlockState[1], 0);
        }
        int volume = (int) volumeLong;
        BlockState[] sampledStates = new BlockState[Math.max(1, volume)];
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        int sampledVoxelCount = 0;
        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
        for (int xOffset = 0; xOffset < sizeX; xOffset++) {
            int x = minX + xOffset;
            int baseX = xOffset * strideX;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int y = minY + yOffset;
                int baseIndex = baseX + (yOffset * strideY);
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int z = minZ + zOffset;
                    double dx = (x + 0.5D) - centerX;
                    double dy = (y + 0.5D) - centerY;
                    double dz = (z + 0.5D) - centerZ;
                    double distSq = (dx * dx) + (dy * dy) + (dz * dz);
                    if (distSq > radiusSq) {
                        continue;
                    }
                    mutablePos.set(x, y, z);
                    if (!level.isInWorldBounds(mutablePos)) {
                        continue;
                    }
                    sampledVoxelCount++;
                    sampledStates[baseIndex + zOffset] = level.getBlockState(mutablePos);
                }
            }
        }
        return new ResistanceFieldSnapshot(sampledStates, sampledVoxelCount);
    }

    private static VolumetricResistanceFieldChunkResult sampleVolumetricResistanceFieldChunk(ResistanceFieldSnapshot resistanceFieldSnapshot,
                                                                                              int minX,
                                                                                              int minY,
                                                                                              int minZ,
                                                                                              int sizeY,
                                                                                              int sizeZ,
                                                                                              int startXOffsetInclusive,
                                                                                              int endXOffsetExclusive,
                                                                                              Int2DoubleOpenHashMap sharedResistanceCostCache) {
        int chunkWidth = Math.max(1, endXOffsetExclusive - startXOffsetInclusive);
        int expectedChunkEntries = Math.max(
                16,
                (int) Math.ceil((double) (chunkWidth * sizeY * sizeZ) * VOLUMETRIC_RESISTANCE_FIELD_PREALLOC_SOLID_FRACTION)
        );
        LongArrayList localPositions = new LongArrayList(expectedChunkEntries);
        FloatArrayList localResistance = new FloatArrayList(expectedChunkEntries);
        Int2DoubleOpenHashMap localResistanceCache = sharedResistanceCostCache != null
                ? sharedResistanceCostCache
                : new Int2DoubleOpenHashMap(128);
        if (sharedResistanceCostCache == null) {
            localResistanceCache.defaultReturnValue(Double.NaN);
        }
        BlockState[] sampledStates = resistanceFieldSnapshot.sampledStates();
        int sampledVoxelCount = 0;
        for (int xOffset = startXOffsetInclusive; xOffset < endXOffsetExclusive; xOffset++) {
            int x = minX + xOffset;
            int baseX = xOffset * sizeY * sizeZ;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int y = minY + yOffset;
                int baseIndex = baseX + (yOffset * sizeZ);
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int z = minZ + zOffset;
                    BlockState blockState = sampledStates[baseIndex + zOffset];
                    if (blockState == null) {
                        continue;
                    }
                    sampledVoxelCount++;
                    if (blockState.isAir()) {
                        continue;
                    }
                    localPositions.add(BlockPos.asLong(x, y, z));
                    localResistance.add((float) getCachedResistanceCost(localResistanceCache, blockState));
                }
            }
        }
        return new VolumetricResistanceFieldChunkResult(localPositions, localResistance, sampledVoxelCount);
    }

    private static KrakkResistanceFieldChunkResult sampleKrakkFieldChunk(ResistanceFieldSnapshot resistanceFieldSnapshot,
                                                                             int minX,
                                                                             int minY,
                                                                             int minZ,
                                                                             int sizeY,
                                                                             int sizeZ,
                                                                             int startXOffsetInclusive,
                                                                             int endXOffsetExclusive,
                                                                             float[] slowness,
                                                                             int[] activeCountByRow,
                                                                             Int2DoubleOpenHashMap sharedResistanceCostCache) {
        Int2DoubleOpenHashMap localResistanceCache = sharedResistanceCostCache != null
                ? sharedResistanceCostCache
                : new Int2DoubleOpenHashMap(128);
        if (sharedResistanceCostCache == null) {
            localResistanceCache.defaultReturnValue(Double.NaN);
        }
        LongArrayList localSolidPositions = new LongArrayList(Math.max(16, (endXOffsetExclusive - startXOffsetInclusive) * sizeY));
        BlockState[] sampledStates = resistanceFieldSnapshot.sampledStates();
        int sampledVoxelCount = 0;
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        float solidScale = (float) KRAKK_SOLID_SLOWNESS_SCALE;
        for (int xOffset = startXOffsetInclusive; xOffset < endXOffsetExclusive; xOffset++) {
            int x = minX + xOffset;
            int baseX = xOffset * sizeY * sizeZ;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int y = minY + yOffset;
                int rowIndex = krakkRowIndex(xOffset, yOffset, sizeY);
                int baseIndex = baseX + (yOffset * sizeZ);
                int rowActiveCount = 0;
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    int z = minZ + zOffset;
                    BlockState blockState = sampledStates[baseIndex + zOffset];
                    if (blockState == null) {
                        continue;
                    }
                    sampledVoxelCount++;
                    int index = baseIndex + zOffset;
                    if (blockState.isAir()) {
                        slowness[index] = airSlowness;
                        rowActiveCount++;
                        continue;
                    }
                    double resistance = getCachedResistanceCost(localResistanceCache, blockState);
                    slowness[index] = airSlowness + ((float) resistance * solidScale);
                    rowActiveCount++;
                    localSolidPositions.add(BlockPos.asLong(x, y, z));
                }
                activeCountByRow[rowIndex] = rowActiveCount;
            }
        }
        return new KrakkResistanceFieldChunkResult(localSolidPositions, sampledVoxelCount);
    }

    private static int resolveResistanceFieldTaskCount(int sizeX, int sizeY, int sizeZ) {
        if (!parallelResistanceFieldSamplingEnabled || VOLUMETRIC_TARGET_SCAN_POOL == null) {
            return 1;
        }
        if (sizeX < RESISTANCE_FIELD_MIN_COLUMNS_FOR_PARALLEL) {
            return 1;
        }
        long voxelCount = (long) sizeX * (long) sizeY * (long) sizeZ;
        if (voxelCount < RESISTANCE_FIELD_MIN_VOXELS_FOR_PARALLEL) {
            return 1;
        }
        int tasksByColumns = Math.max(1, sizeX / RESISTANCE_FIELD_COLUMNS_PER_TASK);
        int tasksByVoxels = (int) Math.max(1L, voxelCount / RESISTANCE_FIELD_MIN_VOXELS_PER_TASK);
        int tasksByWorkers = Math.max(1, VOLUMETRIC_TARGET_SCAN_PARALLELISM);
        int taskCount = Math.min(tasksByColumns, Math.min(tasksByVoxels, tasksByWorkers));
        taskCount = Math.min(taskCount, RESISTANCE_FIELD_MAX_TASKS);
        return taskCount >= 2 ? taskCount : 1;
    }

    private static void cancelVolumetricResistanceFieldFutures(List<Future<VolumetricResistanceFieldChunkResult>> futures) {
        for (Future<VolumetricResistanceFieldChunkResult> future : futures) {
            future.cancel(true);
        }
    }

    private static void cancelKrakkResistanceFieldFutures(List<Future<KrakkResistanceFieldChunkResult>> futures) {
        for (Future<KrakkResistanceFieldChunkResult> future : futures) {
            future.cancel(true);
        }
    }

    private static int resolveKrakkSweepTaskCount(int rowSpan) {
        if (VOLUMETRIC_TARGET_SCAN_POOL == null || rowSpan < KRAKK_SWEEP_MIN_ROWS_FOR_PARALLEL) {
            return 1;
        }
        int tasksBySize = Math.max(1, (rowSpan + KRAKK_SWEEP_ROWS_PER_TASK - 1) / KRAKK_SWEEP_ROWS_PER_TASK);
        int tasksByWorkers = Math.max(1, VOLUMETRIC_TARGET_SCAN_PARALLELISM);
        int taskCount = Math.min(tasksBySize, tasksByWorkers);
        return Math.max(1, Math.min(KRAKK_SWEEP_MAX_TASKS, taskCount));
    }

    private static void cancelKrakkSweepFutures(List<? extends Future<?>> futures) {
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    private static VolumetricTargetScanResult scanVolumetricTargets(LongArrayList solidPositions,
                                                                     VolumetricTargetScanContext context,
                                                                     boolean allowParallel,
                                                                     boolean profileSubstages) {
        int solidCount = solidPositions.size();
        int taskCount = resolveVolumetricTargetScanTaskCount(solidCount);
        if (!allowParallel || taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
            return scanVolumetricTargetScanChunk(solidPositions, 0, solidCount, context, profileSubstages);
        }

        int chunkSize = (solidCount + taskCount - 1) / taskCount;
        ArrayList<Future<VolumetricTargetScanResult>> futures = new ArrayList<>(taskCount);
        for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
            int startIndex = taskIndex * chunkSize;
            int endIndex = Math.min(solidCount, startIndex + chunkSize);
            if (startIndex >= endIndex) {
                break;
            }
            final int chunkStart = startIndex;
            final int chunkEnd = endIndex;
            futures.add(VOLUMETRIC_TARGET_SCAN_POOL.submit(
                    () -> scanVolumetricTargetScanChunk(solidPositions, chunkStart, chunkEnd, context, profileSubstages)
            ));
        }

        LongArrayList mergedPositions = new LongArrayList(Math.max(64, solidCount / 8));
        FloatArrayList mergedWeights = new FloatArrayList(Math.max(64, solidCount / 8));
        long precheckNanos = 0L;
        long blendNanos = 0L;
        try {
            for (Future<VolumetricTargetScanResult> future : futures) {
                VolumetricTargetScanResult chunkResult = future.get();
                precheckNanos = Math.max(precheckNanos, chunkResult.precheckNanos());
                blendNanos = Math.max(blendNanos, chunkResult.blendNanos());
                LongArrayList chunkPositions = chunkResult.targetPositions();
                FloatArrayList chunkWeights = chunkResult.targetWeights();
                for (int i = 0; i < chunkPositions.size(); i++) {
                    mergedPositions.add(chunkPositions.getLong(i));
                    mergedWeights.add(chunkWeights.getFloat(i));
                }
            }
            return new VolumetricTargetScanResult(mergedPositions, mergedWeights, precheckNanos, blendNanos);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            cancelVolumetricTargetScanFutures(futures);
        } catch (ExecutionException exception) {
            cancelVolumetricTargetScanFutures(futures);
        }
        return scanVolumetricTargetScanChunk(solidPositions, 0, solidCount, context, profileSubstages);
    }

    private static VolumetricTargetScanResult scanVolumetricTargetScanChunk(LongArrayList solidPositions,
                                                                            int startInclusive,
                                                                            int endExclusive,
                                                                            VolumetricTargetScanContext context,
                                                                            boolean profileSubstages) {
        int expectedTargets = Math.max(16, (endExclusive - startInclusive) / 8);
        LongArrayList targetPositions = new LongArrayList(expectedTargets);
        FloatArrayList targetWeights = new FloatArrayList(expectedTargets);
        long precheckNanos = 0L;
        long blendNanos = 0L;
        double centerX = context.centerX();
        double centerY = context.centerY();
        double centerZ = context.centerZ();
        double radialStepSize = Math.max(context.radialStepSize(), 1.0E-9D);
        double invRadialStepSize = 1.0D / radialStepSize;
        double resolvedRadius = Math.max(context.resolvedRadius(), 1.0E-9D);
        double invResolvedRadius = 1.0D / resolvedRadius;
        int radialSteps = context.radialSteps();
        float[] maxPressureByShell = context.maxPressureByShell();
        float[] pressureByShell = context.pressureByShell();
        int directionCount = context.directionCount();
        double[] dirX = context.dirX();
        double[] dirY = context.dirY();
        double[] dirZ = context.dirZ();
        int[][] directionNeighbors = context.directionNeighbors();
        int[] directionLookup = context.directionLookup();
        int directionLookupResolution = context.directionLookupResolution();
        for (int i = startInclusive; i < endExclusive; i++) {
            long posLong = solidPositions.getLong(i);
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            long precheckStart = profileSubstages ? System.nanoTime() : 0L;

            double dx = (x + 0.5D) - centerX;
            double dy = (y + 0.5D) - centerY;
            double dz = (z + 0.5D) - centerZ;
            double distSq = (dx * dx) + (dy * dy) + (dz * dz);
            double dist = Math.sqrt(distSq);
            double shellPosition = dist * invRadialStepSize;
            double nx;
            double ny;
            double nz;
            if (dist <= 1.0E-9D) {
                nx = 0.0D;
                ny = 1.0D;
                nz = 0.0D;
            } else {
                double invDist = 1.0D / dist;
                nx = dx * invDist;
                ny = dy * invDist;
                nz = dz * invDist;
            }
            double radialFraction = dist * invResolvedRadius;
            if (radialFraction >= VOLUMETRIC_EDGE_SPIKE_START_FRACTION) {
                double edgeRadiusScale = computeVolumetricEdgeRadiusScale(
                        centerX,
                        centerY,
                        centerZ,
                        radialFraction,
                        nx,
                        ny,
                        nz
                );
                radialFraction = dist / Math.max(resolvedRadius * edgeRadiusScale, 1.0E-9D);
            }
            if (radialFraction >= 1.0D) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            radialFraction = Mth.clamp(radialFraction, 0.0D, 1.0D);
            double centerGradient = 1.0D - radialFraction;
            double edgeGradient = Math.pow(radialFraction, VOLUMETRIC_EDGE_POINT_BIAS_EXPONENT);
            double gradient = Mth.lerp(VOLUMETRIC_EDGE_POINT_BIAS_BLEND, centerGradient, edgeGradient);
            if (gradient <= 0.0D) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }

            double outerSmoothFactor = radialFraction <= VOLUMETRIC_OUTER_SMOOTHING_FULL_WEIGHT_FRACTION
                    ? 1.0D
                    : computeVolumetricOuterSmoothFactor(radialFraction, x, y, z);
            double maxWeight = gradient * outerSmoothFactor;
            if (maxWeight <= VOLUMETRIC_MIN_ENERGY) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }

            double clampedShell = shellPosition;
            if (clampedShell < 0.0D) {
                clampedShell = 0.0D;
            } else if (clampedShell > radialSteps) {
                clampedShell = radialSteps;
            }
            int lowerShell = (int) clampedShell;
            int upperShell = lowerShell < radialSteps ? (lowerShell + 1) : radialSteps;
            double shellAlpha = clampedShell - lowerShell;
            double lowerShellPressure = maxPressureByShell[lowerShell];
            double shellPressureUpper = lowerShellPressure + ((maxPressureByShell[upperShell] - lowerShellPressure) * shellAlpha);
            if ((maxWeight * shellPressureUpper) <= VOLUMETRIC_MIN_ENERGY) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }

            if (profileSubstages) {
                precheckNanos += (System.nanoTime() - precheckStart);
            }

            long blendStart = profileSubstages ? System.nanoTime() : 0L;
            double localPressure = sampleBlendedVolumetricPressure(
                    pressureByShell,
                    directionCount,
                    lowerShell * directionCount,
                    upperShell * directionCount,
                    shellAlpha,
                    nx,
                    ny,
                    nz,
                    dirX,
                    dirY,
                    dirZ,
                    directionNeighbors,
                    directionLookup,
                    directionLookupResolution
            );
            double pressureFactor = localPressure;
            if (pressureFactor < 0.0D) {
                pressureFactor = 0.0D;
            } else if (pressureFactor > 1.0D) {
                pressureFactor = 1.0D;
            }
            double weight = maxWeight * pressureFactor;
            if (profileSubstages) {
                blendNanos += (System.nanoTime() - blendStart);
            }
            if (weight <= VOLUMETRIC_MIN_ENERGY) {
                continue;
            }

            targetPositions.add(posLong);
            targetWeights.add((float) weight);
        }
        return new VolumetricTargetScanResult(targetPositions, targetWeights, precheckNanos, blendNanos);
    }

    private static int resolveVolumetricTargetScanTaskCount(int solidCount) {
        if (VOLUMETRIC_TARGET_SCAN_POOL == null || solidCount < VOLUMETRIC_TARGET_SCAN_MIN_SOLIDS_FOR_PARALLEL) {
            return 1;
        }
        int tasksBySize = Math.max(1, (solidCount + VOLUMETRIC_TARGET_SCAN_SOLIDS_PER_TASK - 1) / VOLUMETRIC_TARGET_SCAN_SOLIDS_PER_TASK);
        int tasksByWorkers = Math.max(1, VOLUMETRIC_TARGET_SCAN_PARALLELISM * 2);
        int taskCount = Math.min(tasksBySize, tasksByWorkers);
        return Math.max(1, Math.min(VOLUMETRIC_TARGET_SCAN_MAX_TASKS, taskCount));
    }

    private static void cancelVolumetricTargetScanFutures(List<Future<VolumetricTargetScanResult>> futures) {
        for (Future<VolumetricTargetScanResult> future : futures) {
            future.cancel(true);
        }
    }

    private static KrakkTargetScanResult scanKrakkTargets(LongArrayList solidPositions,
                                                              float[] volumetricBaselineByIndex,
                                                              float[] arrivalTimes,
                                                              float[] shadowArrivalTimes,
                                                              KrakkField krakkField,
                                                              double centerX,
                                                              double centerY,
                                                              double centerZ,
                                                              double maxArrival,
                                                              boolean allowParallel,
                                                              boolean profileSubstages) {
        int solidCount = solidPositions.size();
        int taskCount = resolveKrakkTargetScanTaskCount(solidCount);
        if (!allowParallel || taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
            return scanKrakkTargetScanChunk(
                    solidPositions,
                    0,
                    solidCount,
                    volumetricBaselineByIndex,
                    arrivalTimes,
                    shadowArrivalTimes,
                    krakkField,
                    centerX,
                    centerY,
                    centerZ,
                    maxArrival,
                    profileSubstages
            );
        }

        int chunkSize = (solidCount + taskCount - 1) / taskCount;
        ArrayList<Future<KrakkTargetScanResult>> futures = new ArrayList<>(taskCount);
        for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
            int startIndex = taskIndex * chunkSize;
            int endIndex = Math.min(solidCount, startIndex + chunkSize);
            if (startIndex >= endIndex) {
                break;
            }
            final int chunkStart = startIndex;
            final int chunkEnd = endIndex;
            futures.add(VOLUMETRIC_TARGET_SCAN_POOL.submit(
                    () -> scanKrakkTargetScanChunk(
                            solidPositions,
                            chunkStart,
                            chunkEnd,
                            volumetricBaselineByIndex,
                            arrivalTimes,
                            shadowArrivalTimes,
                            krakkField,
                            centerX,
                            centerY,
                            centerZ,
                            maxArrival,
                            profileSubstages
                    )
            ));
        }

        LongArrayList mergedPositions = new LongArrayList(Math.max(64, solidCount / 8));
        FloatArrayList mergedWeights = new FloatArrayList(Math.max(64, solidCount / 8));
        double solidWeight = 0.0D;
        long precheckNanos = 0L;
        try {
            for (Future<KrakkTargetScanResult> future : futures) {
                KrakkTargetScanResult chunkResult = future.get();
                precheckNanos += chunkResult.precheckNanos();
                solidWeight += chunkResult.solidWeight();
                LongArrayList chunkPositions = chunkResult.targetPositions();
                FloatArrayList chunkWeights = chunkResult.targetWeights();
                for (int i = 0; i < chunkPositions.size(); i++) {
                    mergedPositions.add(chunkPositions.getLong(i));
                    mergedWeights.add(chunkWeights.getFloat(i));
                }
            }
            return new KrakkTargetScanResult(mergedPositions, mergedWeights, solidWeight, precheckNanos);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            cancelKrakkTargetScanFutures(futures);
        } catch (ExecutionException exception) {
            cancelKrakkTargetScanFutures(futures);
        }
        return scanKrakkTargetScanChunk(
                solidPositions,
                0,
                solidCount,
                volumetricBaselineByIndex,
                arrivalTimes,
                shadowArrivalTimes,
                krakkField,
                centerX,
                centerY,
                centerZ,
                maxArrival,
                profileSubstages
        );
    }

    private static KrakkTargetScanResult scanKrakkTargetScanChunk(LongArrayList solidPositions,
                                                                       int startInclusive,
                                                                       int endExclusive,
                                                                       float[] volumetricBaselineByIndex,
                                                                       float[] arrivalTimes,
                                                                       float[] shadowArrivalTimes,
                                                                       KrakkField krakkField,
                                                                       double centerX,
                                                                       double centerY,
                                                                       double centerZ,
                                                                       double maxArrival,
                                                                       boolean profileSubstages) {
        int expectedTargets = Math.max(16, (endExclusive - startInclusive) / 8);
        LongArrayList targetPositions = new LongArrayList(expectedTargets);
        FloatArrayList targetWeights = new FloatArrayList(expectedTargets);
        long precheckNanos = 0L;
        double solidWeight = 0.0D;
        int fieldMinX = krakkField.minX();
        int fieldMinY = krakkField.minY();
        int fieldMinZ = krakkField.minZ();
        int fieldSizeY = krakkField.sizeY();
        int fieldSizeZ = krakkField.sizeZ();
        for (int i = startInclusive; i < endExclusive; i++) {
            long posLong = solidPositions.getLong(i);
            long precheckStart = profileSubstages ? System.nanoTime() : 0L;
            float baselineWeight = i >= 0 && i < volumetricBaselineByIndex.length
                    ? volumetricBaselineByIndex[i]
                    : Float.NaN;
            if (!Float.isFinite(baselineWeight) || baselineWeight <= KRAKK_TARGET_MIN_WEIGHT) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            int index = krakkGridIndex(
                    x - fieldMinX,
                    y - fieldMinY,
                    z - fieldMinZ,
                    fieldSizeY,
                    fieldSizeZ
            );
            double arrival = arrivalTimes[index];
            if (!Double.isFinite(arrival) || arrival > maxArrival) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double normalized = 1.0D - (arrival / maxArrival);
            if (normalized <= 0.0D) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double airArrival = computeAnalyticKrakkAirArrival(centerX, centerY, centerZ, x, y, z);
            if (!Double.isFinite(airArrival)) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double resistanceOverrun = Math.max(0.0D, arrival - airArrival);
            double effectiveOverrun = Math.max(0.0D, resistanceOverrun - KRAKK_OVERRUN_DEADZONE);
            double normalizedOverrun = effectiveOverrun / Math.max(1.0D, airArrival);
            if (normalizedOverrun >= KRAKK_HARD_BLOCK_NORMALIZED_OVERRUN) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double transmittance = Math.exp(
                    -(effectiveOverrun * KRAKK_RESISTANCE_ATTENUATION_PER_OVERRUN)
                            - (normalizedOverrun * KRAKK_RESISTANCE_NORMALIZED_ATTENUATION)
            );
            if (transmittance <= KRAKK_MIN_TRANSMITTANCE) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double shadowArrival = shadowArrivalTimes[index];
            if (!Double.isFinite(shadowArrival)) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double shadowOverrun = Math.max(0.0D, shadowArrival - arrival);
            double effectiveShadowOverrun = Math.max(0.0D, shadowOverrun - KRAKK_SHADOW_OVERRUN_DEADZONE);
            double normalizedShadowOverrun = effectiveShadowOverrun / Math.max(0.25D, airArrival);
            if (normalizedShadowOverrun >= KRAKK_HARD_BLOCK_SHADOW_NORMALIZED_OVERRUN) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double shadowTransmittance = Math.exp(
                    -(effectiveShadowOverrun * KRAKK_SHADOW_ATTENUATION_PER_OVERRUN)
                            - (normalizedShadowOverrun * KRAKK_SHADOW_NORMALIZED_ATTENUATION)
            );
            if (shadowTransmittance <= KRAKK_MIN_TRANSMITTANCE) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double eikEnvelope = Math.pow(normalized, KRAKK_WEIGHT_EXPONENT);
            double krakkTransFactor = Mth.lerp(
                    KRAKK_ENVELOPE_TRANSMITTANCE_BLEND,
                    1.0D,
                    transmittance * shadowTransmittance
            );
            double smoothingFactor = Mth.lerp(
                    KRAKK_BASELINE_SMOOTH_BLEND,
                    1.0D,
                    eikEnvelope * krakkTransFactor
            );
            double cutoffRetention = computeKrakkCutoffEdgeRetention(normalized);
            double weight = baselineWeight * smoothingFactor * cutoffRetention;
            if (weight <= KRAKK_TARGET_MIN_WEIGHT) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double sampledWeight = weight;
            if (KRAKK_ENABLE_LOW_WEIGHT_STOCHASTIC_SAMPLING
                    && weight < KRAKK_LOW_WEIGHT_STOCHASTIC_THRESHOLD) {
                double keepProbability = Mth.clamp(
                        weight / KRAKK_LOW_WEIGHT_STOCHASTIC_THRESHOLD,
                        KRAKK_LOW_WEIGHT_STOCHASTIC_MIN_KEEP_PROBABILITY,
                        1.0D
                );
                double keepRoll = sampleKrakkLowWeightKeepNoise(x, y, z, centerX, centerY, centerZ);
                if (keepRoll > keepProbability) {
                    if (profileSubstages) {
                        precheckNanos += (System.nanoTime() - precheckStart);
                    }
                    continue;
                }
                sampledWeight = weight / keepProbability;
            }
            if (profileSubstages) {
                precheckNanos += (System.nanoTime() - precheckStart);
            }
            targetPositions.add(posLong);
            targetWeights.add((float) sampledWeight);
            solidWeight += sampledWeight;
        }
        return new KrakkTargetScanResult(targetPositions, targetWeights, solidWeight, precheckNanos);
    }

    private static double sampleKrakkLowWeightKeepNoise(int x, int y, int z, double centerX, double centerY, double centerZ) {
        int cx = Mth.floor(centerX * 2.0D);
        int cy = Mth.floor(centerY * 2.0D);
        int cz = Mth.floor(centerZ * 2.0D);
        long hash = 0xA24BAED4963EE407L;
        hash ^= ((long) x) * 0x9E3779B97F4A7C15L;
        hash ^= ((long) y) * 0xC2B2AE3D27D4EB4FL;
        hash ^= ((long) z) * 0x165667B19E3779F9L;
        hash ^= ((long) cx) * 0x94D049BB133111EBL;
        hash ^= ((long) cy) * 0xFF51AFD7ED558CCDL;
        hash ^= ((long) cz) * 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= (hash >>> 33);
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        return ((hash >>> 11) & 0x1FFFFFL) / 2097151.0D;
    }

    private static int resolveKrakkTargetScanTaskCount(int solidCount) {
        if (VOLUMETRIC_TARGET_SCAN_POOL == null || solidCount < KRAKK_TARGET_SCAN_MIN_SOLIDS_FOR_PARALLEL) {
            return 1;
        }
        int tasksBySize = Math.max(1, (solidCount + KRAKK_TARGET_SCAN_SOLIDS_PER_TASK - 1) / KRAKK_TARGET_SCAN_SOLIDS_PER_TASK);
        int tasksByWorkers = Math.max(1, VOLUMETRIC_TARGET_SCAN_PARALLELISM * 2);
        int taskCount = Math.min(tasksBySize, tasksByWorkers);
        return Math.max(1, Math.min(KRAKK_TARGET_SCAN_MAX_TASKS, taskCount));
    }

    private static void cancelKrakkTargetScanFutures(List<Future<KrakkTargetScanResult>> futures) {
        for (Future<KrakkTargetScanResult> future : futures) {
            future.cancel(true);
        }
    }

    private static int computeVolumetricDirectionSampleCount(double radius, double pointScale) {
        int computed = (int) Math.round(VOLUMETRIC_DIRECTION_SAMPLES_PER_RADIUS2 * radius * radius * pointScale);
        return Mth.clamp(computed, VOLUMETRIC_MIN_DIRECTION_SAMPLES, VOLUMETRIC_MAX_DIRECTION_SAMPLES);
    }

    private static double computeVolumetricAirNormalizationScale(int solidCount, int sampledVoxelCount) {
        if (solidCount <= 0 || sampledVoxelCount <= 0) {
            return 1.0D;
        }
        double solidFraction = Mth.clamp(solidCount / (double) sampledVoxelCount, 1.0E-3D, 1.0D);
        double inverseFraction = 1.0D / solidFraction;
        double blended = Mth.lerp(VOLUMETRIC_AIR_DISTRIBUTION_BLEND, 1.0D, inverseFraction);
        return Mth.clamp(blended, 1.0D, VOLUMETRIC_MAX_AIR_NORMALIZATION_SCALE);
    }

    private static VolumetricDirectionCache getVolumetricDirectionCache(int directionCount) {
        return VOLUMETRIC_DIRECTION_CACHE.computeIfAbsent(directionCount, count -> {
            double[] dirX = new double[count];
            double[] dirY = new double[count];
            double[] dirZ = new double[count];
            for (int i = 0; i < count; i++) {
                Direction direction = fibonacciDirection(i, count);
                dirX[i] = direction.x;
                dirY[i] = direction.y;
                dirZ[i] = direction.z;
            }
            int[][] neighbors = computeVolumetricDirectionNeighbors(
                    dirX,
                    dirY,
                    dirZ,
                    VOLUMETRIC_DIRECTION_NEIGHBOR_COUNT
            );
            int lookupResolution = Math.max(8, VOLUMETRIC_DIRECTION_LOOKUP_RESOLUTION);
            int[] directionLookup = buildVolumetricDirectionLookup(dirX, dirY, dirZ, lookupResolution);
            return new VolumetricDirectionCache(dirX, dirY, dirZ, neighbors, directionLookup, lookupResolution);
        });
    }

    private static int[] buildVolumetricDirectionLookup(double[] dirX,
                                                        double[] dirY,
                                                        double[] dirZ,
                                                        int resolution) {
        int entryCount = resolution * resolution;
        int[] lookup = new int[Math.max(1, entryCount)];
        int directionCount = dirX.length;
        for (int v = 0; v < resolution; v++) {
            for (int u = 0; u < resolution; u++) {
                Direction direction = decodeOctahedralDirection(u, v, resolution);
                double nx = direction.x();
                double ny = direction.y();
                double nz = direction.z();
                int bestIndex = 0;
                double bestDot = -Double.MAX_VALUE;
                for (int i = 0; i < directionCount; i++) {
                    double dot = (nx * dirX[i]) + (ny * dirY[i]) + (nz * dirZ[i]);
                    if (dot > bestDot) {
                        bestDot = dot;
                        bestIndex = i;
                    }
                }
                lookup[(v * resolution) + u] = bestIndex;
            }
        }
        return lookup;
    }

    private static Direction decodeOctahedralDirection(int u, int v, int resolution) {
        double ox = (((u + 0.5D) / resolution) * 2.0D) - 1.0D;
        double oy = (((v + 0.5D) / resolution) * 2.0D) - 1.0D;
        double oz = 1.0D - Math.abs(ox) - Math.abs(oy);
        if (oz < 0.0D) {
            double oldX = ox;
            double oldY = oy;
            ox = (1.0D - Math.abs(oldY)) * Math.signum(oldX);
            oy = (1.0D - Math.abs(oldX)) * Math.signum(oldY);
            oz = -oz;
        }
        double length = Math.sqrt((ox * ox) + (oy * oy) + (oz * oz));
        if (length <= 1.0E-9D) {
            return new Direction(0.0D, 1.0D, 0.0D);
        }
        double invLength = 1.0D / length;
        return new Direction(ox * invLength, oy * invLength, oz * invLength);
    }

    private static double computeVolumetricRadiusScale(double radius, double maxScale) {
        double normalized = radius / VOLUMETRIC_RADIUS_SCALE_BASE;
        return Mth.clamp(normalized, 1.0D, maxScale);
    }

    private static double computeVolumetricOuterSmoothFactor(double radialFraction, int x, int y, int z) {
        double smoothingRatio = VOLUMETRIC_OUTER_SMOOTHING_RATIO
                + (sampleVolumetricOuterSmoothingVariance(x, y, z) * VOLUMETRIC_OUTER_SMOOTHING_VARIANCE);
        smoothingRatio = Mth.clamp(smoothingRatio, 0.01D, 0.20D);
        double start = 1.0D - smoothingRatio;
        if (radialFraction <= start) {
            return 1.0D;
        }
        double t = Mth.clamp((radialFraction - start) / Math.max(smoothingRatio, 1.0E-9D), 0.0D, 1.0D);
        double smoothStep = t * t * (3.0D - (2.0D * t));
        return 1.0D - smoothStep;
    }

    private static double computeVolumetricEdgeRadiusScale(double centerX,
                                                           double centerY,
                                                           double centerZ,
                                                           double radialFraction,
                                                           double nx,
                                                           double ny,
                                                           double nz) {
        int qx = Mth.floor((nx + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
        int qy = Mth.floor((ny + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
        int qz = Mth.floor((nz + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
        int cx = Mth.floor(centerX * 2.0D);
        int cy = Mth.floor(centerY * 2.0D);
        int cz = Mth.floor(centerZ * 2.0D);

        double primary = sampleVolumetricEdgeSpikeNoise(qx, qy, qz, cx, cy, cz);
        double secondary = sampleVolumetricEdgeSpikeNoise((qx * 2) + 17, (qy * 2) + 31, (qz * 2) + 47, cx, cy, cz);
        double noise = Mth.clamp((primary * 0.72D) + (secondary * 0.28D), 0.0D, 1.0D);
        double outerSpan = Math.max(1.0E-6D, 1.0D - VOLUMETRIC_EDGE_SPIKE_START_FRACTION);
        double edgeProgress = Mth.clamp((radialFraction - VOLUMETRIC_EDGE_SPIKE_START_FRACTION) / outerSpan, 0.0D, 1.0D);
        double smoothStep = edgeProgress * edgeProgress * (3.0D - (2.0D * edgeProgress));
        double ramp = Math.pow(smoothStep, VOLUMETRIC_EDGE_SPIKE_RAMP_EXPONENT);
        double strength = Mth.lerp(ramp, VOLUMETRIC_EDGE_SPIKE_MIN_STRENGTH, 1.0D);
        double reduction = VOLUMETRIC_EDGE_SPIKE_MAX_REDUCTION
                * strength
                * Math.pow(noise, VOLUMETRIC_EDGE_SPIKE_REDUCTION_EXPONENT);
        return Mth.clamp(1.0D - reduction, 0.55D, 1.0D);
    }

    private static double sampleVolumetricEdgeSpikeNoise(int x, int y, int z, int cx, int cy, int cz) {
        long hash = 0xD6E8FEB86659FD93L;
        hash ^= ((long) x) * 0x9E3779B97F4A7C15L;
        hash ^= ((long) y) * 0xC2B2AE3D27D4EB4FL;
        hash ^= ((long) z) * 0x165667B19E3779F9L;
        hash ^= ((long) cx) * 0x94D049BB133111EBL;
        hash ^= ((long) cy) * 0xFF51AFD7ED558CCDL;
        hash ^= ((long) cz) * 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= (hash >>> 33);
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        return ((hash >>> 11) & 0x1FFFFFL) / 2097151.0D;
    }

    private static double sampleVolumetricOuterSmoothingVariance(int x, int y, int z) {
        long hash = 0x9E3779B97F4A7C15L;
        hash ^= ((long) x) * 0xC2B2AE3D27D4EB4FL;
        hash ^= ((long) y) * 0x165667B19E3779F9L;
        hash ^= ((long) z) * 0x85EBCA77C2B2AE63L;
        hash ^= (hash >>> 33);
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= (hash >>> 33);
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        double unit = ((hash >>> 11) & 0x1FFFFFL) / 2097151.0D;
        return (unit * 2.0D) - 1.0D;
    }

    private static double sampleBlendedVolumetricPressure(float[] pressureByShell,
                                                          int directionCount,
                                                          int lowerRowOffset,
                                                          int upperRowOffset,
                                                          double shellAlpha,
                                                          double nx,
                                                          double ny,
                                                          double nz,
                                                          double[] dirX,
                                                          double[] dirY,
                                                          double[] dirZ,
                                                          int[][] directionNeighbors,
                                                          int[] directionLookup,
                                                          int directionLookupResolution) {
        int blendCount = Math.min(VOLUMETRIC_DIRECTION_BLEND_COUNT, directionCount);
        int best0 = -1;
        int best1 = -1;
        int best2 = -1;
        int best3 = -1;
        int best4 = -1;
        int best5 = -1;
        int best6 = -1;
        int best7 = -1;
        double dot0 = -Double.MAX_VALUE;
        double dot1 = -Double.MAX_VALUE;
        double dot2 = -Double.MAX_VALUE;
        double dot3 = -Double.MAX_VALUE;
        double dot4 = -Double.MAX_VALUE;
        double dot5 = -Double.MAX_VALUE;
        double dot6 = -Double.MAX_VALUE;
        double dot7 = -Double.MAX_VALUE;
        for (int i = 0; i < directionCount; i++) {
            double dot = (nx * dirX[i]) + (ny * dirY[i]) + (nz * dirZ[i]);
            if (dot > dot0) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot5;
                best6 = best5;
                dot5 = dot4;
                best5 = best4;
                dot4 = dot3;
                best4 = best3;
                dot3 = dot2;
                best3 = best2;
                dot2 = dot1;
                best2 = best1;
                dot1 = dot0;
                best1 = best0;
                dot0 = dot;
                best0 = i;
            } else if (dot > dot1) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot5;
                best6 = best5;
                dot5 = dot4;
                best5 = best4;
                dot4 = dot3;
                best4 = best3;
                dot3 = dot2;
                best3 = best2;
                dot2 = dot1;
                best2 = best1;
                dot1 = dot;
                best1 = i;
            } else if (dot > dot2) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot5;
                best6 = best5;
                dot5 = dot4;
                best5 = best4;
                dot4 = dot3;
                best4 = best3;
                dot3 = dot2;
                best3 = best2;
                dot2 = dot;
                best2 = i;
            } else if (dot > dot3) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot5;
                best6 = best5;
                dot5 = dot4;
                best5 = best4;
                dot4 = dot3;
                best4 = best3;
                dot3 = dot;
                best3 = i;
            } else if (dot > dot4) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot5;
                best6 = best5;
                dot5 = dot4;
                best5 = best4;
                dot4 = dot;
                best4 = i;
            } else if (dot > dot5) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot5;
                best6 = best5;
                dot5 = dot;
                best5 = i;
            } else if (dot > dot6) {
                dot7 = dot6;
                best7 = best6;
                dot6 = dot;
                best6 = i;
            } else if (dot > dot7) {
                dot7 = dot;
                best7 = i;
            }
        }

        double weightedPressure = 0.0D;
        double weightSum = 0.0D;
        if (best0 >= 0) {
            double weight0 = Math.pow(Math.max(0.0D, dot0), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight0 > 0.0D) {
                double pressure0 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best0], pressureByShell[upperRowOffset + best0]);
                weightedPressure += pressure0 * weight0;
                weightSum += weight0;
            }
        }
        if (blendCount >= 2 && best1 >= 0) {
            double weight1 = Math.pow(Math.max(0.0D, dot1), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight1 > 0.0D) {
                double pressure1 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best1], pressureByShell[upperRowOffset + best1]);
                weightedPressure += pressure1 * weight1;
                weightSum += weight1;
            }
        }
        if (blendCount >= 3 && best2 >= 0) {
            double weight2 = Math.pow(Math.max(0.0D, dot2), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight2 > 0.0D) {
                double pressure2 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best2], pressureByShell[upperRowOffset + best2]);
                weightedPressure += pressure2 * weight2;
                weightSum += weight2;
            }
        }
        if (blendCount >= 4 && best3 >= 0) {
            double weight3 = Math.pow(Math.max(0.0D, dot3), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight3 > 0.0D) {
                double pressure3 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best3], pressureByShell[upperRowOffset + best3]);
                weightedPressure += pressure3 * weight3;
                weightSum += weight3;
            }
        }
        if (blendCount >= 5 && best4 >= 0) {
            double weight4 = Math.pow(Math.max(0.0D, dot4), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight4 > 0.0D) {
                double pressure4 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best4], pressureByShell[upperRowOffset + best4]);
                weightedPressure += pressure4 * weight4;
                weightSum += weight4;
            }
        }
        if (blendCount >= 6 && best5 >= 0) {
            double weight5 = Math.pow(Math.max(0.0D, dot5), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight5 > 0.0D) {
                double pressure5 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best5], pressureByShell[upperRowOffset + best5]);
                weightedPressure += pressure5 * weight5;
                weightSum += weight5;
            }
        }
        if (blendCount >= 7 && best6 >= 0) {
            double weight6 = Math.pow(Math.max(0.0D, dot6), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight6 > 0.0D) {
                double pressure6 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best6], pressureByShell[upperRowOffset + best6]);
                weightedPressure += pressure6 * weight6;
                weightSum += weight6;
            }
        }
        if (blendCount >= 8 && best7 >= 0) {
            double weight7 = Math.pow(Math.max(0.0D, dot7), VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
            if (weight7 > 0.0D) {
                double pressure7 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best7], pressureByShell[upperRowOffset + best7]);
                weightedPressure += pressure7 * weight7;
                weightSum += weight7;
            }
        }
        if (weightSum > 1.0E-9D) {
            return weightedPressure / weightSum;
        }
        if (best0 >= 0) {
            return Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best0], pressureByShell[upperRowOffset + best0]);
        }
        return 0.0D;
    }

    private static int octahedralLookupIndex(double nx, double ny, double nz, int resolution) {
        double ax = Math.abs(nx);
        double ay = Math.abs(ny);
        double az = Math.abs(nz);
        double invL1 = 1.0D / Math.max(1.0E-9D, ax + ay + az);
        double ox = nx * invL1;
        double oy = ny * invL1;
        double oz = nz * invL1;
        if (oz < 0.0D) {
            double oldX = ox;
            double oldY = oy;
            ox = (1.0D - Math.abs(oldY)) * Math.signum(oldX);
            oy = (1.0D - Math.abs(oldX)) * Math.signum(oldY);
        }
        int u = Mth.clamp(Mth.floor(((ox * 0.5D) + 0.5D) * resolution), 0, resolution - 1);
        int v = Mth.clamp(Mth.floor(((oy * 0.5D) + 0.5D) * resolution), 0, resolution - 1);
        return (v * resolution) + u;
    }

    private static int[][] computeVolumetricDirectionNeighbors(double[] dirX,
                                                               double[] dirY,
                                                               double[] dirZ,
                                                               int neighborCount) {
        int directionCount = dirX.length;
        int[][] neighbors = new int[directionCount][neighborCount];
        for (int i = 0; i < directionCount; i++) {
            Arrays.fill(neighbors[i], -1);
            double[] bestDots = new double[neighborCount];
            Arrays.fill(bestDots, -Double.MAX_VALUE);
            for (int j = 0; j < directionCount; j++) {
                if (i == j) {
                    continue;
                }
                double dot = (dirX[i] * dirX[j]) + (dirY[i] * dirY[j]) + (dirZ[i] * dirZ[j]);
                int insertIndex = -1;
                for (int k = 0; k < neighborCount; k++) {
                    if (dot > bestDots[k]) {
                        insertIndex = k;
                        break;
                    }
                }
                if (insertIndex < 0) {
                    continue;
                }
                for (int k = neighborCount - 1; k > insertIndex; k--) {
                    bestDots[k] = bestDots[k - 1];
                    neighbors[i][k] = neighbors[i][k - 1];
                }
                bestDots[insertIndex] = dot;
                neighbors[i][insertIndex] = j;
            }
        }
        return neighbors;
    }

    private static Direction fibonacciDirection(int index, int count) {
        double t = (index + 0.5D) / count;
        double y = 1.0D - (2.0D * t);
        double radial = Math.sqrt(Math.max(0.0D, 1.0D - (y * y)));
        double theta = index * GOLDEN_ANGLE;
        double x = Math.cos(theta) * radial;
        double z = Math.sin(theta) * radial;
        return new Direction(x, y, z);
    }

    private static void emitExplosionEffects(ServerLevel level, double x, double y, double z, RandomSource random) {
        level.playSound(
                null,
                x,
                y,
                z,
                SoundEvents.GENERIC_EXPLODE,
                SoundSource.BLOCKS,
                1.0F,
                0.95F + (random.nextFloat() * 0.1F)
        );
        level.sendParticles(ParticleTypes.EXPLOSION_EMITTER, x, y, z, 1, 0.0D, 0.0D, 0.0D, 0.0D);
    }

    private static void applySingleBlockImpact(ServerLevel level,
                                               BlockPos.MutableBlockPos mutablePos,
                                               long blockPosLong,
                                               double resolvedImpactPower,
                                               Entity source,
                                               LivingEntity owner,
                                               boolean applyWorldChanges,
                                               ExplosionProfileTrace trace) {
        mutablePos.set(BlockPos.getX(blockPosLong), BlockPos.getY(blockPosLong), BlockPos.getZ(blockPosLong));
        if (!level.isInWorldBounds(mutablePos)) {
            return;
        }

        BlockState blockState = level.getBlockState(mutablePos);
        if (blockState.isAir()) {
            return;
        }
        if (trace != null) {
            trace.blocksEvaluated++;
        }

        if (applyWorldChanges && specialBlockHandler.handle(level, mutablePos, blockState, source, owner)) {
            if (trace != null) {
                trace.specialHandled++;
            }
            return;
        }

        if (blockState.is(Blocks.TNT)) {
            if (applyWorldChanges) {
                TntBlock.explode(level, mutablePos);
                level.removeBlock(mutablePos, false);
            }
            if (trace != null) {
                trace.tntTriggered++;
            }
            return;
        }

        if (resolvedImpactPower <= MIN_RESOLVED_RAY_IMPACT) {
            if (trace != null) {
                trace.lowImpactSkipped++;
            }
            return;
        }

        if (applyWorldChanges) {
            var damageApi = KrakkApi.damage();
            KrakkImpactResult result;
            if (damageApi instanceof KrakkDamageRuntime damageRuntime) {
                result = damageRuntime.applyImpactPrevalidated(
                        level,
                        mutablePos,
                        blockState,
                        source,
                        resolvedImpactPower,
                        false,
                        KrakkDamageType.KRAKK_DAMAGE_EXPLOSION
                );
            } else {
                result = damageApi.applyImpact(
                        level,
                        mutablePos,
                        blockState,
                        source,
                        resolvedImpactPower,
                        false,
                        KrakkDamageType.KRAKK_DAMAGE_EXPLOSION
                );
            }
            if (trace != null) {
                if (result.broken()) {
                    trace.brokenBlocks++;
                } else if (result.damageState() > 0) {
                    trace.damagedBlocks++;
                }
            }
            return;
        }

        predictImpactOutcome(level, mutablePos, blockState, resolvedImpactPower, trace);
    }

    private static SectionImpactAccumulator collectRaycastImpacts(ServerLevel level, double centerX, double centerY, double centerZ,
                                                                  double blastRadius,
                                                                  double blastPower, RandomSource random,
                                                                  boolean emitSmoke, ExplosionProfileTrace trace,
                                                                  ExplosionEntityContext entityContext) {
        int rayCount = KrakkExplosionCurves.computeRayCount(blastRadius, DEFAULT_RAY_COUNT, MIN_RAY_COUNT, MAX_RAY_COUNT);
        SectionImpactAccumulator impactAccumulator = new SectionImpactAccumulator(Math.max(64, rayCount >> 1));
        if (trace != null) {
            trace.initialRays = rayCount;
        }
        double splitDistanceThreshold = Mth.clamp(
                raySplitDistanceThreshold,
                MIN_RAY_SPLIT_DISTANCE_THRESHOLD,
                MAX_RAY_SPLIT_DISTANCE_THRESHOLD
        );
        double[] splitOuterRadiusSquaredByDepth = computeSplitRadiusSquaredByDepth(
                rayCount,
                splitDistanceThreshold * (1.0D + RAY_SPLIT_HYSTERESIS_RATIO)
        );
        double[] splitInnerRadiusSquaredByDepth = computeSplitRadiusSquaredByDepth(
                rayCount,
                splitDistanceThreshold * (1.0D - RAY_SPLIT_HYSTERESIS_RATIO)
        );
        double splitPayoffEnergyFloor = computeSplitPayoffEnergyFloor(blastRadius, blastPower);
        int smokeRays = Mth.clamp((int) Math.round(rayCount * RAY_SMOKE_RAY_FRACTION), RAY_SMOKE_MIN_RAYS, RAY_SMOKE_MAX_RAYS);
        int smokeRayStride = Math.max(1, rayCount / Math.max(1, smokeRays));
        int smokeBudget = Mth.clamp(
                (int) Math.round(blastRadius * RAY_SMOKE_BUDGET_PER_RADIUS),
                RAY_SMOKE_MIN_BUDGET,
                RAY_SMOKE_MAX_BUDGET
        );
        ArrayDeque<RaycastState> rayQueue = new ArrayDeque<>(Math.max(32, rayCount * 4));
        BlockPos.MutableBlockPos currentPos = new BlockPos.MutableBlockPos();
        Int2DoubleOpenHashMap resistanceCostCache = new Int2DoubleOpenHashMap(Math.max(128, rayCount / 2));
        resistanceCostCache.defaultReturnValue(Double.NaN);

        for (int i = 0; i < rayCount; i++) {
            double t = (i + 0.5D) / rayCount;
            double dy = 1.0D - (2.0D * t);
            double radial = Math.sqrt(Math.max(0.0D, 1.0D - (dy * dy)));
            double theta = i * GOLDEN_ANGLE;
            double dx = Math.cos(theta) * radial;
            double dz = Math.sin(theta) * radial;
            if (INITIAL_RAY_VARIANCE_RADIANS > 0.0D) {
                Direction coneU = perpendicularAxis(dx, dy, dz);
                Direction coneV = secondaryPerpendicularAxis(dx, dy, dz, coneU);
                double polarAngle = Math.abs((random.nextDouble() * 2.0D - 1.0D) * INITIAL_RAY_VARIANCE_RADIANS);
                double azimuthAngle = random.nextDouble() * (Math.PI * 2.0D);
                Direction jitteredDirection = directionOnCone(dx, dy, dz, coneU, coneV, polarAngle, azimuthAngle);
                dx = jitteredDirection.x;
                dy = jitteredDirection.y;
                dz = jitteredDirection.z;
            }
            double rayEnergy = blastRadius * (0.8D + (random.nextDouble() * 0.6D));
            boolean emitSmokeOnRay = (i % smokeRayStride) == 0;
            boolean canHitEntities = entityContext != null && entityContext.mayRayEverHitEntities(
                    centerX,
                    centerY,
                    centerZ,
                    dx,
                    dy,
                    dz,
                    rayEnergy / Math.max(RAY_DECAY_PER_UNIT, 1.0E-9D)
            );
            RaycastState ray = new RaycastState(
                    centerX, centerY, centerZ,
                    dx, dy, dz,
                    rayEnergy,
                    Long.MIN_VALUE,
                    0,
                    0,
                    emitSmokeOnRay,
                    entityContext != null ? entityContext.nextRayId() : (i + 1),
                    canHitEntities,
                    0.0D
            );
            ray.nextSplitEventT = computeNextSplitEventTravelT(
                    ray,
                    centerX,
                    centerY,
                    centerZ,
                    splitOuterRadiusSquaredByDepth,
                    splitInnerRadiusSquaredByDepth
            );
            rayQueue.addLast(ray);
        }

        long raycastStart = trace != null ? System.nanoTime() : 0L;
        while (!rayQueue.isEmpty()) {
            RaycastState ray = rayQueue.removeFirst();
            if (trace != null) {
                trace.processedRays++;
            }
            while (ray.energy > 0.0D) {
                currentPos.set(ray.cellX, ray.cellY, ray.cellZ);
                if (!level.isInWorldBounds(currentPos)) {
                    break;
                }
                if (trace != null) {
                    trace.raySteps++;
                }

                long currentPosLong = currentPos.asLong();
                if (currentPosLong != ray.lastPosLong) {
                    if (emitSmoke && ray.emitSmokeOnRay && smokeBudget > 0 && (ray.stepIndex % RAY_SMOKE_STEP_INTERVAL) == 0) {
                        emitRaySmoke(level, ray.x, ray.y, ray.z, random, trace);
                        smokeBudget--;
                    }

                    BlockState state = level.getBlockState(currentPos);
                    boolean hasFluid = !state.getFluidState().isEmpty();
                    boolean fluidBlock = hasFluid && isDamageableFluidBlock(state);
                    if (!state.isAir()) {
                        double normalizedEnergy = ray.energy / Math.max(blastRadius, 1.0E-6D);
                        if (!hasFluid || fluidBlock) {
                            double impact = blastPower * normalizedEnergy * RAY_IMPACT_SCALE;
                            if (impact > MIN_RESOLVED_RAY_IMPACT) {
                                impactAccumulator.add(ray.cellX, ray.cellY, ray.cellZ, impact);
                            }
                        }

                        ray.energy -= getCachedResistanceCost(resistanceCostCache, state);
                    } else if (hasFluid) {
                        ray.energy -= FLUID_RAY_DAMPING;
                    }

                    ray.lastPosLong = currentPosLong;
                }

                int axisMask = ray.chooseAxisMask();
                if (axisMask == 0) {
                    break;
                }

                double nextT = ray.nextBoundaryT(axisMask);

                double advanceDistance = nextT - ray.travelT;
                if (advanceDistance < 0.0D) {
                    advanceDistance = 0.0D;
                }

                int dynamicSplitDepthLimit = computeDynamicSplitDepthLimit(ray.energy, ray.splitDepth, splitPayoffEnergyFloor);
                if (ray.splitDepth < dynamicSplitDepthLimit
                        && ray.energy > MIN_RAY_SPLIT_ENERGY
                        && advanceDistance > 1.0E-9D
                        && ray.nextSplitEventT <= nextT + 1.0E-9D) {
                    long splitCheckStart = trace != null ? System.nanoTime() : 0L;
                    if (trace != null) {
                        trace.splitChecks++;
                    }
                    double splitDistance = Math.max(0.0D, ray.nextSplitEventT - ray.travelT);
                    if (trace != null) {
                        trace.splitCheckNanos += (System.nanoTime() - splitCheckStart);
                    }
                    if (splitDistance <= advanceDistance + 1.0E-9D) {
                        double splitEnergy = ray.energy - (splitDistance * RAY_DECAY_PER_UNIT);
                        if (splitEnergy > MIN_RAY_SPLIT_ENERGY) {
                            double splitX = ray.x + (ray.dirX * splitDistance);
                            double splitY = ray.y + (ray.dirY * splitDistance);
                            double splitZ = ray.z + (ray.dirZ * splitDistance);
                            splitRay(
                                    ray,
                                    splitX,
                                    splitY,
                                    splitZ,
                                    splitEnergy,
                                    rayQueue,
                                    random,
                                    trace,
                                    entityContext,
                                    blastRadius,
                                    centerX,
                                    centerY,
                                    centerZ,
                                    splitOuterRadiusSquaredByDepth,
                                    splitInnerRadiusSquaredByDepth
                            );
                            break;
                        }
                        ray.nextSplitEventT = Double.POSITIVE_INFINITY;
                    }
                }

                if (entityContext != null
                        && ray.canHitEntities
                        && (ray.stepIndex % ENTITY_SEGMENT_CHECK_INTERVAL) == 0) {
                    long entitySegmentStart = trace != null ? System.nanoTime() : 0L;
                    ray.energy = applyEntitySegmentInteractions(
                            entityContext,
                            ray,
                            blastRadius,
                            blastPower,
                            advanceDistance,
                            trace
                    );
                    if (trace != null) {
                        trace.entitySegmentNanos += (System.nanoTime() - entitySegmentStart);
                    }
                } else {
                    ray.energy -= advanceDistance * RAY_DECAY_PER_UNIT;
                }
                if (ray.energy <= 0.0D) {
                    break;
                }

                ray.advanceTo(nextT, axisMask);
                ray.stepIndex++;
            }

        }
        if (trace != null) {
            trace.raycastNanos += (System.nanoTime() - raycastStart);
        }
        if (trace != null) {
            trace.rawImpactedBlocks = impactAccumulator.countActiveImpacts();
        }
        long aaStart = trace != null ? System.nanoTime() : 0L;
        SectionImpactAccumulator antiAliasedImpacts = applyLightRaycastAntialiasing(impactAccumulator);
        if (trace != null) {
            trace.antialiasNanos += (System.nanoTime() - aaStart);
            trace.postAaImpactedBlocks = antiAliasedImpacts.countActiveImpacts();
        }
        double maxImpact = blastPower * MAX_BLOCK_IMPACT_MULTIPLIER;
        antiAliasedImpacts.clampMaxImpact(maxImpact);
        return antiAliasedImpacts;
    }

    private static double[] computeSplitRadiusSquaredByDepth(int initialRayCount, double threshold) {
        double[] splitRadiusSquaredByDepth = new double[MAX_RAY_SPLIT_DEPTH];
        double n0 = Math.max(1.0D, initialRayCount);
        double thresholdSquared = threshold * threshold;
        for (int depth = 0; depth < MAX_RAY_SPLIT_DEPTH; depth++) {
            double effectiveRayCount = n0 * Math.pow(RAY_SPLIT_CHILD_COUNT, depth);
            splitRadiusSquaredByDepth[depth] = thresholdSquared * (effectiveRayCount / (4.0D * Math.PI));
        }
        return splitRadiusSquaredByDepth;
    }

    private static double computeSplitPayoffEnergyFloor(double blastRadius, double blastPower) {
        double safeRadius = Math.max(blastRadius, 1.0E-6D);
        double safeImpactScale = Math.max(blastPower * RAY_IMPACT_SCALE, 1.0E-9D);
        double energyForOneDamageState = (SPLIT_PAYOFF_IMPACT_FLOOR * safeRadius) / safeImpactScale;
        double postSplitSurvivalFloor = MIN_RAY_SPLIT_ENERGY + (RAY_SPLIT_MIN_TRAVEL_AFTER_SPLIT * RAY_DECAY_PER_UNIT);
        return Math.max(energyForOneDamageState, postSplitSurvivalFloor);
    }

    private static int computeDynamicSplitDepthLimit(double rayEnergy, int currentDepth, double splitPayoffEnergyFloor) {
        if (currentDepth >= MAX_RAY_SPLIT_DEPTH || rayEnergy <= 0.0D) {
            return currentDepth;
        }

        double requiredChildEnergy = Math.max(splitPayoffEnergyFloor, MIN_RAY_SPLIT_ENERGY);
        if (rayEnergy < requiredChildEnergy * RAY_SPLIT_CHILD_COUNT) {
            return currentDepth;
        }

        int depthLimit = currentDepth;
        double projectedEnergy = rayEnergy;
        while (depthLimit < MAX_RAY_SPLIT_DEPTH) {
            projectedEnergy /= RAY_SPLIT_CHILD_COUNT;
            if (projectedEnergy < requiredChildEnergy) {
                break;
            }
            depthLimit++;
        }
        return depthLimit;
    }

    private static double distanceSquaredFromCenter(double x, double y, double z, double centerX, double centerY, double centerZ) {
        double dx = x - centerX;
        double dy = y - centerY;
        double dz = z - centerZ;
        return (dx * dx) + (dy * dy) + (dz * dz);
    }

    private static double findSplitCrossingDistanceOnRay(double startX, double startY, double startZ,
                                                         double dirX, double dirY, double dirZ,
                                                         double centerX, double centerY, double centerZ,
                                                         double splitRadiusSquared) {
        double mX = startX - centerX;
        double mY = startY - centerY;
        double mZ = startZ - centerZ;
        double startDistSquared = (mX * mX) + (mY * mY) + (mZ * mZ);
        double b = (mX * dirX) + (mY * dirY) + (mZ * dirZ);
        double c = startDistSquared - splitRadiusSquared;
        double discriminant = (b * b) - c;
        if (discriminant < 0.0D) {
            return Double.NaN;
        }
        double sqrt = Math.sqrt(discriminant);
        double t1 = -b - sqrt;
        double t2 = -b + sqrt;
        double candidate = Double.POSITIVE_INFINITY;
        if (t1 >= -1.0E-9D) {
            candidate = Math.min(candidate, Math.max(0.0D, t1));
        }
        if (t2 >= -1.0E-9D) {
            candidate = Math.min(candidate, Math.max(0.0D, t2));
        }
        if (!Double.isFinite(candidate)) {
            return Double.NaN;
        }
        return Math.max(0.0D, candidate);
    }

    private static double computeNextSplitEventTravelT(RaycastState ray,
                                                        double centerX, double centerY, double centerZ,
                                                        double[] splitOuterRadiusSquaredByDepth,
                                                        double[] splitInnerRadiusSquaredByDepth) {
        if (ray.splitDepth >= MAX_RAY_SPLIT_DEPTH || ray.energy <= MIN_RAY_SPLIT_ENERGY) {
            return Double.POSITIVE_INFINITY;
        }

        double minOffset = Math.max(0.0D, ray.minSplitTravelT - ray.travelT);
        double probeX = ray.x + (ray.dirX * minOffset);
        double probeY = ray.y + (ray.dirY * minOffset);
        double probeZ = ray.z + (ray.dirZ * minOffset);
        double outerRadiusSquared = splitOuterRadiusSquaredByDepth[ray.splitDepth];
        double innerRadiusSquared = splitInnerRadiusSquaredByDepth[ray.splitDepth];

        double probeDistSquared = distanceSquaredFromCenter(probeX, probeY, probeZ, centerX, centerY, centerZ);
        if (probeDistSquared >= innerRadiusSquared && probeDistSquared < outerRadiusSquared) {
            minOffset = Math.max(minOffset, RAY_SPLIT_MIN_TRAVEL_AFTER_SPLIT * 0.5D);
            probeX = ray.x + (ray.dirX * minOffset);
            probeY = ray.y + (ray.dirY * minOffset);
            probeZ = ray.z + (ray.dirZ * minOffset);
            probeDistSquared = distanceSquaredFromCenter(probeX, probeY, probeZ, centerX, centerY, centerZ);
        }

        if (probeDistSquared >= outerRadiusSquared) {
            return ray.travelT + minOffset;
        }

        double crossingOffset = findSplitCrossingDistanceOnRay(
                probeX,
                probeY,
                probeZ,
                ray.dirX,
                ray.dirY,
                ray.dirZ,
                centerX,
                centerY,
                centerZ,
                outerRadiusSquared
        );
        if (Double.isNaN(crossingOffset)) {
            return Double.POSITIVE_INFINITY;
        }
        return ray.travelT + minOffset + crossingOffset;
    }

    private static int[][] buildChildOrderByRayOctant() {
        int[][] orders = new int[8][8];
        for (int nearMask = 0; nearMask < 8; nearMask++) {
            int[] order = new int[8];
            for (int i = 0; i < CHILD_TRAVERSAL_OFFSETS.length; i++) {
                order[i] = nearMask ^ CHILD_TRAVERSAL_OFFSETS[i];
            }
            orders[nearMask] = order;
        }
        return orders;
    }

    private static int rayOctantMask(double dirX, double dirY, double dirZ) {
        int mask = 0;
        if (dirX < 0.0D) {
            mask |= 1;
        }
        if (dirY < 0.0D) {
            mask |= 2;
        }
        if (dirZ < 0.0D) {
            mask |= 4;
        }
        return mask;
    }

    private static ExplosionEntityContext buildExplosionEntityContext(ServerLevel level,
                                                                      double centerX, double centerY, double centerZ,
                                                                      double blastRadius,
                                                                      ExplosionProfileTrace trace) {
        AABB query = new AABB(
                centerX - blastRadius,
                centerY - blastRadius,
                centerZ - blastRadius,
                centerX + blastRadius,
                centerY + blastRadius,
                centerZ + blastRadius
        ).inflate(ENTITY_QUERY_MARGIN);
        List<LivingEntity> candidates = level.getEntitiesOfClass(
                LivingEntity.class,
                query,
                entity -> entity.isAlive() && !entity.isSpectator() && entity.isPickable()
        );
        if (candidates.isEmpty()) {
            return null;
        }

        candidates.sort(Comparator.comparingInt(Entity::getId));
        List<EntityHitProxy> all = new ArrayList<>(candidates.size());

        for (LivingEntity entity : candidates) {
            AABB aabb = entity.getBoundingBox().inflate(ENTITY_HITBOX_INFLATE);
            if (!aabb.intersects(query)) {
                continue;
            }
            EntityHitProxy proxy = new EntityHitProxy(entity, aabb, computeEntityResistanceCost(entity));
            all.add(proxy);
        }

        if (all.isEmpty()) {
            return null;
        }
        if (trace != null) {
            trace.entityCandidates += all.size();
        }
        PackedEntityOctree octree = PackedEntityOctree.build(all, query);
        EntityMacroGrid macroGrid = EntityMacroGrid.build(all, query, ENTITY_MACRO_CELL_SIZE);
        double maxEntityRadius = 0.0D;
        for (int i = 0; i < all.size(); i++) {
            double radius = Math.sqrt(all.get(i).radiusSquared);
            if (radius > maxEntityRadius) {
                maxEntityRadius = radius;
            }
        }

        return new ExplosionEntityContext(all, octree, macroGrid, query, maxEntityRadius);
    }

    private static double applyEntitySegmentInteractions(ExplosionEntityContext context,
                                                         RaycastState ray,
                                                         double blastRadius, double blastPower,
                                                         double segmentDistance,
                                                         ExplosionProfileTrace trace) {
        return applyEntitySegmentInteractionsOctree(
                context,
                ray,
                blastRadius,
                blastPower,
                segmentDistance,
                trace
        );
    }

    private static double applyEntitySegmentInteractionsOctree(ExplosionEntityContext context,
                                                               RaycastState ray,
                                                               double blastRadius,
                                                               double blastPower,
                                                               double segmentDistance,
                                                               ExplosionProfileTrace trace) {
        if (context.octree == null || context.octree.nodeCount <= 0 || segmentDistance <= 1.0E-9D) {
            return ray.energy - (segmentDistance * RAY_DECAY_PER_UNIT);
        }

        PackedEntityOctree octree = context.octree;
        double startX = ray.x;
        double startY = ray.y;
        double startZ = ray.z;
        double remainingDistance = segmentDistance;
        double segmentStartEnergy = ray.energy;
        double energyNormDenominator = Math.max(blastRadius, 1.0E-6D);
        int nearMask = rayOctantMask(ray.dirX, ray.dirY, ray.dirZ);
        int[] childOrder = CHILD_ORDER_BY_RAY_OCTANT[nearMask];

        while (remainingDistance > 1.0E-6D && segmentStartEnergy > 0.0D) {
            if (context.macroGrid != null
                    && !context.macroGrid.mayIntersectSegment(startX, startY, startZ, ray.dirX, ray.dirY, ray.dirZ, remainingDistance)) {
                break;
            }
            context.resetTraversal();
            context.pushNode(octree.rootIndex);
            EntityHitProxy nearestProxy = null;
            double nearestDistance = Double.POSITIVE_INFINITY;

            while (context.hasNodes()) {
                int nodeIndex = context.popNode();
                if (trace != null) {
                    trace.octreeNodeTests++;
                }
                double nodeEntryDistance = intersectRaySegmentAabbEntryDistance(
                        startX, startY, startZ,
                        ray.dirX, ray.dirY, ray.dirZ,
                        remainingDistance,
                        octree.minX[nodeIndex] - octree.maxEntityRadius[nodeIndex],
                        octree.minY[nodeIndex] - octree.maxEntityRadius[nodeIndex],
                        octree.minZ[nodeIndex] - octree.maxEntityRadius[nodeIndex],
                        octree.maxX[nodeIndex] + octree.maxEntityRadius[nodeIndex],
                        octree.maxY[nodeIndex] + octree.maxEntityRadius[nodeIndex],
                        octree.maxZ[nodeIndex] + octree.maxEntityRadius[nodeIndex]
                );
                if (Double.isNaN(nodeEntryDistance) || nodeEntryDistance >= nearestDistance) {
                    continue;
                }

                if (octree.childCount[nodeIndex] <= 0) {
                    if (trace != null) {
                        trace.octreeLeafVisits++;
                    }
                    int leafStart = octree.leafEntityStart[nodeIndex];
                    int leafCount = octree.leafEntityCount[nodeIndex];
                    if (leafCount <= 0) {
                        continue;
                    }
                    for (int i = 0; i < leafCount; i++) {
                        EntityHitProxy proxy = context.all.get(octree.leafEntityIndices[leafStart + i]);
                        if (proxy.lastRayId == ray.rayId || !proxy.entity.isAlive()) {
                            continue;
                        }
                        if (!intersectsRaySegmentSphere(startX, startY, startZ, ray.dirX, ray.dirY, ray.dirZ, remainingDistance, proxy)) {
                            continue;
                        }
                        if (trace != null) {
                            trace.entityIntersectionTests++;
                        }
                        double hitDistance = intersectRaySegmentAabbDistance(
                                startX,
                                startY,
                                startZ,
                                ray.dirX,
                                ray.dirY,
                                ray.dirZ,
                                remainingDistance,
                                proxy.aabb
                        );
                        if (Double.isNaN(hitDistance) || hitDistance >= nearestDistance) {
                            continue;
                        }
                        nearestDistance = hitDistance;
                        nearestProxy = proxy;
                    }
                } else {
                    int sortedChildCount = 0;
                    int childMask = octree.childMask[nodeIndex];
                    for (int i = 0; i < childOrder.length; i++) {
                        int childOctant = childOrder[i];
                        if ((childMask & (1 << childOctant)) == 0) {
                            continue;
                        }
                        int childNodeIndex = octree.findChildNodeForOctant(nodeIndex, childOctant);
                        if (childNodeIndex < 0) {
                            continue;
                        }
                        double childEntryDistance = intersectRaySegmentAabbEntryDistance(
                                startX, startY, startZ,
                                ray.dirX, ray.dirY, ray.dirZ,
                                remainingDistance,
                                octree.minX[childNodeIndex] - octree.maxEntityRadius[childNodeIndex],
                                octree.minY[childNodeIndex] - octree.maxEntityRadius[childNodeIndex],
                                octree.minZ[childNodeIndex] - octree.maxEntityRadius[childNodeIndex],
                                octree.maxX[childNodeIndex] + octree.maxEntityRadius[childNodeIndex],
                                octree.maxY[childNodeIndex] + octree.maxEntityRadius[childNodeIndex],
                                octree.maxZ[childNodeIndex] + octree.maxEntityRadius[childNodeIndex]
                        );
                        if (Double.isNaN(childEntryDistance) || childEntryDistance >= nearestDistance) {
                            continue;
                        }

                        int insert = sortedChildCount;
                        while (insert > 0 && childEntryDistance < context.childEntryDistances[insert - 1]) {
                            context.childEntryDistances[insert] = context.childEntryDistances[insert - 1];
                            context.childNodeIndices[insert] = context.childNodeIndices[insert - 1];
                            insert--;
                        }
                        context.childEntryDistances[insert] = childEntryDistance;
                        context.childNodeIndices[insert] = childNodeIndex;
                        sortedChildCount++;
                    }
                    for (int i = sortedChildCount - 1; i >= 0; i--) {
                        context.pushNode(context.childNodeIndices[i]);
                    }
                }
            }

            if (nearestProxy == null) {
                break;
            }
            if (trace != null) {
                trace.entityHits++;
            }

            double energyAtHit = segmentStartEnergy - (nearestDistance * RAY_DECAY_PER_UNIT);
            if (energyAtHit <= 0.0D) {
                return 0.0D;
            }

            double impact = blastPower * (energyAtHit / energyNormDenominator) * ENTITY_IMPACT_SCALE;
            if (impact > ENTITY_MIN_APPLIED_IMPACT) {
                nearestProxy.accumulatedImpact += impact;
                nearestProxy.pressureX += ray.dirX * impact;
                nearestProxy.pressureY += ray.dirY * impact;
                nearestProxy.pressureZ += ray.dirZ * impact;
            }
            nearestProxy.lastRayId = ray.rayId;

            segmentStartEnergy = energyAtHit - nearestProxy.rayResistanceCost;
            if (segmentStartEnergy <= 0.0D) {
                return 0.0D;
            }

            double advance = nearestDistance + ENTITY_HIT_EPSILON_DISTANCE;
            if (advance > remainingDistance) {
                advance = remainingDistance;
            }
            startX += ray.dirX * advance;
            startY += ray.dirY * advance;
            startZ += ray.dirZ * advance;
            remainingDistance -= advance;
        }

        return segmentStartEnergy - (remainingDistance * RAY_DECAY_PER_UNIT);
    }

    private static boolean intersectsRaySegmentSphere(double startX, double startY, double startZ,
                                                      double dirX, double dirY, double dirZ,
                                                      double maxDistance,
                                                      EntityHitProxy proxy) {
        double toCenterX = proxy.centerX - startX;
        double toCenterY = proxy.centerY - startY;
        double toCenterZ = proxy.centerZ - startZ;
        double t = (toCenterX * dirX) + (toCenterY * dirY) + (toCenterZ * dirZ);
        if (t < 0.0D) {
            t = 0.0D;
        } else if (t > maxDistance) {
            t = maxDistance;
        }
        double closestX = startX + (dirX * t);
        double closestY = startY + (dirY * t);
        double closestZ = startZ + (dirZ * t);
        double dx = proxy.centerX - closestX;
        double dy = proxy.centerY - closestY;
        double dz = proxy.centerZ - closestZ;
        return (dx * dx) + (dy * dy) + (dz * dz) <= proxy.radiusSquared;
    }

    private static double intersectRaySegmentAabbEntryDistance(double startX, double startY, double startZ,
                                                               double dirX, double dirY, double dirZ,
                                                               double maxDistance,
                                                               double minX, double minY, double minZ,
                                                               double maxX, double maxY, double maxZ) {
        double minT = 0.0D;
        double maxT = maxDistance;

        if (Math.abs(dirX) <= 1.0E-9D) {
            if (startX < minX || startX > maxX) {
                return Double.NaN;
            }
        } else {
            double invDir = 1.0D / dirX;
            double t1 = (minX - startX) * invDir;
            double t2 = (maxX - startX) * invDir;
            if (t1 > t2) {
                double swap = t1;
                t1 = t2;
                t2 = swap;
            }
            minT = Math.max(minT, t1);
            maxT = Math.min(maxT, t2);
            if (minT > maxT) {
                return Double.NaN;
            }
        }

        if (Math.abs(dirY) <= 1.0E-9D) {
            if (startY < minY || startY > maxY) {
                return Double.NaN;
            }
        } else {
            double invDir = 1.0D / dirY;
            double t1 = (minY - startY) * invDir;
            double t2 = (maxY - startY) * invDir;
            if (t1 > t2) {
                double swap = t1;
                t1 = t2;
                t2 = swap;
            }
            minT = Math.max(minT, t1);
            maxT = Math.min(maxT, t2);
            if (minT > maxT) {
                return Double.NaN;
            }
        }

        if (Math.abs(dirZ) <= 1.0E-9D) {
            if (startZ < minZ || startZ > maxZ) {
                return Double.NaN;
            }
        } else {
            double invDir = 1.0D / dirZ;
            double t1 = (minZ - startZ) * invDir;
            double t2 = (maxZ - startZ) * invDir;
            if (t1 > t2) {
                double swap = t1;
                t1 = t2;
                t2 = swap;
            }
            minT = Math.max(minT, t1);
            maxT = Math.min(maxT, t2);
            if (minT > maxT) {
                return Double.NaN;
            }
        }

        if (maxT < 0.0D || minT > maxDistance) {
            return Double.NaN;
        }
        return Math.max(0.0D, minT);
    }

    private static double intersectRaySegmentAabbDistance(double startX, double startY, double startZ,
                                                          double dirX, double dirY, double dirZ,
                                                          double maxDistance, AABB aabb) {
        double minT = 0.0D;
        double maxT = maxDistance;

        if (Math.abs(dirX) <= 1.0E-9D) {
            if (startX < aabb.minX || startX > aabb.maxX) {
                return Double.NaN;
            }
        } else {
            double invDir = 1.0D / dirX;
            double t1 = (aabb.minX - startX) * invDir;
            double t2 = (aabb.maxX - startX) * invDir;
            if (t1 > t2) {
                double swap = t1;
                t1 = t2;
                t2 = swap;
            }
            minT = Math.max(minT, t1);
            maxT = Math.min(maxT, t2);
            if (minT > maxT) {
                return Double.NaN;
            }
        }

        if (Math.abs(dirY) <= 1.0E-9D) {
            if (startY < aabb.minY || startY > aabb.maxY) {
                return Double.NaN;
            }
        } else {
            double invDir = 1.0D / dirY;
            double t1 = (aabb.minY - startY) * invDir;
            double t2 = (aabb.maxY - startY) * invDir;
            if (t1 > t2) {
                double swap = t1;
                t1 = t2;
                t2 = swap;
            }
            minT = Math.max(minT, t1);
            maxT = Math.min(maxT, t2);
            if (minT > maxT) {
                return Double.NaN;
            }
        }

        if (Math.abs(dirZ) <= 1.0E-9D) {
            if (startZ < aabb.minZ || startZ > aabb.maxZ) {
                return Double.NaN;
            }
        } else {
            double invDir = 1.0D / dirZ;
            double t1 = (aabb.minZ - startZ) * invDir;
            double t2 = (aabb.maxZ - startZ) * invDir;
            if (t1 > t2) {
                double swap = t1;
                t1 = t2;
                t2 = swap;
            }
            minT = Math.max(minT, t1);
            maxT = Math.min(maxT, t2);
            if (minT > maxT) {
                return Double.NaN;
            }
        }

        if (maxT < 0.0D || minT > maxDistance) {
            return Double.NaN;
        }
        return Math.max(0.0D, minT);
    }

    private static void applyEntityImpacts(ServerLevel level,
                                           Entity source,
                                           LivingEntity owner,
                                           ExplosionEntityContext context,
                                           boolean applyWorldChanges,
                                           ExplosionProfileTrace trace) {
        if (context == null || context.all.isEmpty()) {
            return;
        }
        DamageSource explosionDamage = level.damageSources().explosion(source, owner);
        for (EntityHitProxy proxy : context.all) {
            applySingleEntityImpact(
                    explosionDamage,
                    proxy.entity,
                    proxy.accumulatedImpact,
                    proxy.pressureX,
                    proxy.pressureY,
                    proxy.pressureZ,
                    proxy.massScale,
                    applyWorldChanges,
                    trace
            );
        }
    }

    private static void applySingleEntityImpact(DamageSource explosionDamage,
                                                LivingEntity entity,
                                                double accumulatedImpact,
                                                double pressureX,
                                                double pressureY,
                                                double pressureZ,
                                                double massScale,
                                                boolean applyWorldChanges,
                                                ExplosionProfileTrace trace) {
        if (accumulatedImpact <= ENTITY_MIN_APPLIED_IMPACT || !entity.isAlive()) {
            return;
        }

        double maxHealth = Math.max(1.0D, entity.getMaxHealth());
        double absorption = Math.max(0.0D, entity.getAbsorptionAmount());
        double strength = ENTITY_HP_STRENGTH_BASE + ((maxHealth + absorption) * ENTITY_HP_STRENGTH_PER_HP);
        double rawDamage = accumulatedImpact / Math.max(strength, 1.0E-6D);
        double armorDivisor = 1.0D
                + (entity.getArmorValue() * ENTITY_ARMOR_DAMAGE_DIVISOR)
                + (entity.getAttributeValue(Attributes.ARMOR_TOUGHNESS) * ENTITY_TOUGHNESS_DAMAGE_DIVISOR);
        double finalDamage = rawDamage / Math.max(1.0D, armorDivisor);
        if (!Double.isFinite(finalDamage) || finalDamage <= 0.0D) {
            return;
        }
        if (finalDamage > Float.MAX_VALUE) {
            finalDamage = Float.MAX_VALUE;
        }
        if (trace != null) {
            trace.entityAffected++;
        }

        if (!applyWorldChanges) {
            return;
        }
        boolean hurt = entity.hurt(explosionDamage, (float) finalDamage);
        if (hurt && trace != null) {
            trace.entityDamaged++;
        }
        if (!entity.isAlive()) {
            if (trace != null) {
                trace.entityKilled++;
            }
            return;
        }

        double pressureMagnitudeSq = (pressureX * pressureX) + (pressureY * pressureY) + (pressureZ * pressureZ);
        if (pressureMagnitudeSq <= 1.0E-8D || !Double.isFinite(pressureMagnitudeSq)) {
            return;
        }
        double pressureMagnitude = Math.sqrt(pressureMagnitudeSq);
        double knockbackResistance = Mth.clamp(entity.getAttributeValue(Attributes.KNOCKBACK_RESISTANCE), 0.0D, 1.0D);
        double knockbackStrength = Math.log1p(pressureMagnitude)
                * ENTITY_KNOCKBACK_SCALE
                * (1.0D - knockbackResistance)
                / massScale;
        if (!Double.isFinite(knockbackStrength) || knockbackStrength <= 1.0E-6D) {
            return;
        }
        knockbackStrength = Math.min(knockbackStrength, ENTITY_KNOCKBACK_MAX);
        double invMagnitude = 1.0D / pressureMagnitude;
        double knockbackX = pressureX * invMagnitude * knockbackStrength;
        double knockbackY = (pressureY * invMagnitude * knockbackStrength) + (knockbackStrength * ENTITY_KNOCKBACK_UPWARD_BIAS);
        double knockbackZ = pressureZ * invMagnitude * knockbackStrength;
        if (!Double.isFinite(knockbackX) || !Double.isFinite(knockbackY) || !Double.isFinite(knockbackZ)) {
            return;
        }
        entity.push(knockbackX, knockbackY, knockbackZ);
        entity.hurtMarked = true;
    }

    private static double computeEntityResistanceCost(LivingEntity entity) {
        double effectiveHealth = Math.max(0.0D, entity.getHealth() + entity.getAbsorptionAmount());
        double healthSapping = (effectiveHealth / ENTITY_HEALTH_OBSIDIAN_EQUIVALENT) * ENTITY_HEALTH_OBSIDIAN_EQUIVALENT_DAMPING;
        double armor = Math.max(0.0D, entity.getArmorValue());
        double toughness = Math.max(0.0D, entity.getAttributeValue(Attributes.ARMOR_TOUGHNESS));
        return ENTITY_BASE_RAY_DAMPING
                + (armor * ENTITY_ARMOR_RAY_DAMPING)
                + (toughness * ENTITY_TOUGHNESS_RAY_DAMPING)
                + healthSapping;
    }

    private static SectionImpactAccumulator applyLightRaycastAntialiasing(SectionImpactAccumulator source) {
        if (source.isEmpty() || RAY_AA_SPREAD_SHARE <= 0.0D) {
            return source;
        }

        SectionImpactAccumulator result = new SectionImpactAccumulator(Math.max(16, source.sectionCount() * 2));
        source.forEachImpactCoordinates((posX, posY, posZ, impact) -> {
            if (impact <= 0.0D) {
                return;
            }

            double spreadShare = impact >= RAY_AA_MIN_IMPACT ? RAY_AA_SPREAD_SHARE : 0.0D;
            double retainedImpact = impact * (1.0D - spreadShare);
            result.add(posX, posY, posZ, retainedImpact);

            if (spreadShare <= 0.0D) {
                return;
            }
            if (!isAaFrontierCell(source, posX, posY, posZ, impact)) {
                return;
            }

            double neighborImpact = (impact - retainedImpact) / 6.0D;
            result.add(posX + 1, posY, posZ, neighborImpact);
            result.add(posX - 1, posY, posZ, neighborImpact);
            result.add(posX, posY + 1, posZ, neighborImpact);
            result.add(posX, posY - 1, posZ, neighborImpact);
            result.add(posX, posY, posZ + 1, neighborImpact);
            result.add(posX, posY, posZ - 1, neighborImpact);
        });

        return result;
    }

    private static boolean isAaFrontierCell(SectionImpactAccumulator source, int x, int y, int z, double impact) {
        double px = source.getImpact(x + 1, y, z);
        if (px <= 0.0D || Math.abs(px - impact) >= RAY_AA_FRONTIER_IMPACT_DELTA) {
            return true;
        }
        double nx = source.getImpact(x - 1, y, z);
        if (nx <= 0.0D || Math.abs(nx - impact) >= RAY_AA_FRONTIER_IMPACT_DELTA) {
            return true;
        }
        double py = source.getImpact(x, y + 1, z);
        if (py <= 0.0D || Math.abs(py - impact) >= RAY_AA_FRONTIER_IMPACT_DELTA) {
            return true;
        }
        double ny = source.getImpact(x, y - 1, z);
        if (ny <= 0.0D || Math.abs(ny - impact) >= RAY_AA_FRONTIER_IMPACT_DELTA) {
            return true;
        }
        double pz = source.getImpact(x, y, z + 1);
        if (pz <= 0.0D || Math.abs(pz - impact) >= RAY_AA_FRONTIER_IMPACT_DELTA) {
            return true;
        }
        double nz = source.getImpact(x, y, z - 1);
        return nz <= 0.0D || Math.abs(nz - impact) >= RAY_AA_FRONTIER_IMPACT_DELTA;
    }

    private static double getCachedResistanceCost(Int2DoubleOpenHashMap resistanceCostCache, BlockState state) {
        int stateId = Block.getId(state);
        double cached = resistanceCostCache.get(stateId);
        if (!Double.isNaN(cached)) {
            return cached;
        }

        double resistance = isDamageableFluidBlock(state)
                ? STONE_EQUIVALENT_FLUID_EXPLOSION_RESISTANCE
                : Math.max(0.0D, state.getBlock().getExplosionResistance());
        double resistanceCost = computeResistanceCostFromExplosionResistance(resistance);
        resistanceCostCache.put(stateId, resistanceCost);
        return resistanceCost;
    }

    private static double computeResistanceCostFromExplosionResistance(double resistance) {
        return (Math.pow(resistance + 0.3D, 0.78D) * BLOCK_RESISTANCE_DAMPING) + BLOCK_BASE_DAMPING;
    }

    private static void splitRay(RaycastState parent,
                                 double splitX, double splitY, double splitZ, double parentEnergy,
                                 ArrayDeque<RaycastState> queue, RandomSource random, ExplosionProfileTrace trace,
                                 ExplosionEntityContext entityContext,
                                 double blastRadius,
                                 double centerX, double centerY, double centerZ,
                                 double[] splitOuterRadiusSquaredByDepth,
                                 double[] splitInnerRadiusSquaredByDepth) {
        double splitEnergy = parentEnergy / RAY_SPLIT_CHILD_COUNT;
        if (splitEnergy <= 0.0D) {
            return;
        }
        if (trace != null) {
            trace.raySplits++;
        }

        Direction coneU = perpendicularAxis(parent.dirX, parent.dirY, parent.dirZ);
        Direction coneV = secondaryPerpendicularAxis(parent.dirX, parent.dirY, parent.dirZ, coneU);
        double azimuthBase = random.nextDouble() * (Math.PI * 2.0D);
        double azimuthStep = (Math.PI * 2.0D) / RAY_SPLIT_CHILD_COUNT;
        int childDepth = parent.splitDepth + 1;

        for (int childIndex = RAY_SPLIT_CHILD_COUNT - 1; childIndex >= 0; childIndex--) {
            double polarAngle = RAY_SPLIT_HALF_ANGLE_RADIANS
                    + ((random.nextDouble() * 2.0D - 1.0D) * RAY_SPLIT_VARIANCE_RADIANS);
            polarAngle = Mth.clamp(polarAngle, 0.0D, Math.PI - 1.0E-4D);
            double azimuth = azimuthBase + (azimuthStep * childIndex);
            Direction childDirection = directionOnCone(
                    parent.dirX, parent.dirY, parent.dirZ,
                    coneU, coneV,
                    polarAngle, azimuth
            );
            int childRayId = entityContext != null ? entityContext.nextRayId() : parent.rayId;
            boolean childCanHitEntities = entityContext != null && entityContext.mayRayEverHitEntities(
                    splitX,
                    splitY,
                    splitZ,
                    childDirection.x,
                    childDirection.y,
                    childDirection.z,
                    splitEnergy / Math.max(RAY_DECAY_PER_UNIT, 1.0E-9D)
            );
            RaycastState childRay = new RaycastState(
                    splitX, splitY, splitZ,
                    childDirection.x, childDirection.y, childDirection.z,
                    splitEnergy,
                    parent.lastPosLong,
                    parent.stepIndex,
                    childDepth,
                    parent.emitSmokeOnRay,
                    childRayId,
                    childCanHitEntities,
                    RAY_SPLIT_MIN_TRAVEL_AFTER_SPLIT
            );
            childRay.nextSplitEventT = computeNextSplitEventTravelT(
                    childRay,
                    centerX,
                    centerY,
                    centerZ,
                    splitOuterRadiusSquaredByDepth,
                    splitInnerRadiusSquaredByDepth
            );
            queue.addFirst(childRay);
        }
    }

    private static Direction secondaryPerpendicularAxis(double dirX, double dirY, double dirZ, Direction coneU) {
        double vx = (dirY * coneU.z) - (dirZ * coneU.y);
        double vy = (dirZ * coneU.x) - (dirX * coneU.z);
        double vz = (dirX * coneU.y) - (dirY * coneU.x);
        double length = Math.sqrt((vx * vx) + (vy * vy) + (vz * vz));
        if (length <= 1.0E-8D) {
            return new Direction(0.0D, 1.0D, 0.0D);
        }
        return new Direction(vx / length, vy / length, vz / length);
    }

    private static Direction directionOnCone(double dirX, double dirY, double dirZ,
                                             Direction coneU, Direction coneV,
                                             double polarAngle, double azimuthAngle) {
        double sinPolar = Math.sin(polarAngle);
        double cosPolar = Math.cos(polarAngle);
        double cosAzimuth = Math.cos(azimuthAngle);
        double sinAzimuth = Math.sin(azimuthAngle);

        double ux = (coneU.x * cosAzimuth) + (coneV.x * sinAzimuth);
        double uy = (coneU.y * cosAzimuth) + (coneV.y * sinAzimuth);
        double uz = (coneU.z * cosAzimuth) + (coneV.z * sinAzimuth);

        double vx = (dirX * cosPolar) + (ux * sinPolar);
        double vy = (dirY * cosPolar) + (uy * sinPolar);
        double vz = (dirZ * cosPolar) + (uz * sinPolar);

        double length = Math.sqrt((vx * vx) + (vy * vy) + (vz * vz));
        if (length <= 1.0E-8D) {
            return new Direction(dirX, dirY, dirZ);
        }
        return new Direction(vx / length, vy / length, vz / length);
    }

    private static Direction perpendicularAxis(double x, double y, double z) {
        KrakkRaySplitMath.Vec3 axis = KrakkRaySplitMath.perpendicularAxis(x, y, z);
        return new Direction(axis.x(), axis.y(), axis.z());
    }

    private static Direction rotateAroundAxis(double vx, double vy, double vz, Direction axis, double angleRadians) {
        KrakkRaySplitMath.Vec3 rotated = KrakkRaySplitMath.rotateAroundAxis(
                vx, vy, vz,
                new KrakkRaySplitMath.Vec3(axis.x, axis.y, axis.z),
                angleRadians
        );
        return new Direction(rotated.x(), rotated.y(), rotated.z());
    }

    private static void emitRaySmoke(ServerLevel level, double x, double y, double z, RandomSource random, ExplosionProfileTrace trace) {
        double jitterX = (random.nextDouble() - 0.5D) * RAY_SMOKE_JITTER;
        double jitterY = (random.nextDouble() - 0.5D) * RAY_SMOKE_JITTER;
        double jitterZ = (random.nextDouble() - 0.5D) * RAY_SMOKE_JITTER;
        level.sendParticles(
                ParticleTypes.SMOKE,
                x + jitterX,
                y + RAY_SMOKE_HEIGHT_OFFSET + jitterY,
                z + jitterZ,
                1,
                0.0D,
                0.0D,
                0.0D,
                0.0D
        );
        if (trace != null) {
            trace.smokeParticles++;
        }
    }

    private static void predictImpactOutcome(ServerLevel level, BlockPos blockPos, BlockState blockState, double impactPower,
                                             ExplosionProfileTrace trace) {
        if (trace == null) {
            return;
        }

        if (blockState.isAir()) {
            return;
        }
        boolean fluidBlock = isDamageableFluidBlock(blockState);
        float hardness = fluidBlock ? STONE_EQUIVALENT_FLUID_HARDNESS : blockState.getDestroySpeed(level, blockPos);
        if (!fluidBlock && hardness < 0.0F) {
            return;
        }

        float resistance = fluidBlock
                ? (float) STONE_EQUIVALENT_FLUID_EXPLOSION_RESISTANCE
                : blockState.getBlock().getExplosionResistance();
        int delta = KrakkDamageCurves.computeImpactDamageDelta(
                blockState.getBlock() instanceof net.minecraft.world.level.block.FallingBlock,
                impactPower,
                resistance,
                hardness
        );
        if (delta <= 0) {
            return;
        }

        int existing = 0;
        if (KrakkApi.damage() instanceof KrakkDamageRuntime runtime) {
            existing = runtime.getRawDamageState(level, blockPos);
        } else {
            existing = KrakkApi.damage().getDamageState(level, blockPos);
        }

        int max = KrakkApi.damage().getMaxDamageState();
        int next = Math.min(max, existing + delta);
        if (fluidBlock && next >= FLUID_REMOVE_DAMAGE_THRESHOLD) {
            trace.predictedBrokenBlocks++;
        } else if (next >= max) {
            trace.predictedBrokenBlocks++;
        } else {
            trace.predictedDamagedBlocks++;
        }
    }

    private static boolean isDamageableFluidBlock(BlockState blockState) {
        if (!(blockState.getBlock() instanceof LiquidBlock)) {
            return false;
        }
        return !blockState.getFluidState().isEmpty();
    }

    private static int estimateDamageSyncPayloadBytes(ResourceLocation dimensionId) {
        String text = dimensionId.toString();
        int utf8Bytes = text.getBytes(StandardCharsets.UTF_8).length;
        return estimateVarIntBytes(utf8Bytes) + utf8Bytes + Long.BYTES + 1;
    }

    private static int estimateVarIntBytes(int value) {
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            bytes++;
            v >>>= 7;
        }
        return bytes;
    }

    private static Long2DoubleOpenHashMap diffuseImpactField(ServerLevel level, Long2DoubleOpenHashMap source, double blastRadius) {
        if (source.isEmpty()) {
            return source;
        }

        int passes = blastRadius >= IMPACT_DIFFUSION_RADIUS_THRESHOLD ? IMPACT_DIFFUSION_MAX_PASSES : 1;
        Long2DoubleOpenHashMap working = source;
        for (int pass = 0; pass < passes; pass++) {
            Long2DoubleOpenHashMap next = new Long2DoubleOpenHashMap(Math.max(working.size() * 2, 16));
            BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
            for (Long2DoubleMap.Entry entry : working.long2DoubleEntrySet()) {
                double impact = entry.getDoubleValue();
                if (impact <= 1.0E-6D) {
                    continue;
                }
                BlockPos originPos = BlockPos.of(entry.getLongKey());
                if (!level.isInWorldBounds(originPos)) {
                    continue;
                }

                double retained = impact * (1.0D - IMPACT_DIFFUSION_SHARE);
                next.addTo(originPos.asLong(), retained);
                double spread = impact - retained;
                if (spread <= 1.0E-6D) {
                    continue;
                }

                double totalWeight = 0.0D;
                for (int ox = -1; ox <= 1; ox++) {
                    for (int oy = -1; oy <= 1; oy++) {
                        for (int oz = -1; oz <= 1; oz++) {
                            if (ox == 0 && oy == 0 && oz == 0) {
                                continue;
                            }
                            mutablePos.set(originPos.getX() + ox, originPos.getY() + oy, originPos.getZ() + oz);
                            if (!level.isInWorldBounds(mutablePos)) {
                                continue;
                            }
                            int distSq = (ox * ox) + (oy * oy) + (oz * oz);
                            totalWeight += 1.0D / distSq;
                        }
                    }
                }

                if (totalWeight <= 1.0E-6D) {
                    next.addTo(originPos.asLong(), spread);
                    continue;
                }

                for (int ox = -1; ox <= 1; ox++) {
                    for (int oy = -1; oy <= 1; oy++) {
                        for (int oz = -1; oz <= 1; oz++) {
                            if (ox == 0 && oy == 0 && oz == 0) {
                                continue;
                            }
                            mutablePos.set(originPos.getX() + ox, originPos.getY() + oy, originPos.getZ() + oz);
                            if (!level.isInWorldBounds(mutablePos)) {
                                continue;
                            }
                            int distSq = (ox * ox) + (oy * oy) + (oz * oz);
                            double weight = 1.0D / distSq;
                            next.addTo(mutablePos.asLong(), spread * (weight / totalWeight));
                        }
                    }
                }
            }
            working = next;
        }
        return working;
    }

    private static void addSmoothedImpact(ServerLevel level, Long2DoubleOpenHashMap impacts,
                                          double centerX, double centerY, double centerZ, double blastRadius,
                                          double x, double y, double z, double impact) {
        if (impact <= 0.0D) {
            return;
        }

        double smoothing = Mth.clamp(RAY_SMOOTHING_FRACTION, 0.0D, 1.0D);
        double directImpact = impact * (1.0D - smoothing);
        double smoothedImpact = impact * smoothing;

        BlockPos directPos = BlockPos.containing(x, y, z);
        if (level.isInWorldBounds(directPos) && directImpact > 0.0D) {
            impacts.addTo(directPos.asLong(), directImpact);
        }

        if (smoothedImpact <= 0.0D) {
            return;
        }

        double radialDistance = Math.sqrt(
                ((x - centerX) * (x - centerX))
                        + ((y - centerY) * (y - centerY))
                        + ((z - centerZ) * (z - centerZ))
        );
        double normalizedDistance = Mth.clamp(radialDistance / Math.max(blastRadius, 1.0E-6D), 0.0D, 1.0D);
        double smoothingRadius = Mth.lerp(normalizedDistance, RAY_SMOOTHING_MIN_RADIUS, RAY_SMOOTHING_MAX_RADIUS);
        int kernelRange;
        if (smoothingRadius >= RAY_SMOOTHING_RANGE3_THRESHOLD) {
            kernelRange = 3;
        } else if (smoothingRadius >= RAY_SMOOTHING_RANGE2_THRESHOLD) {
            kernelRange = 2;
        } else {
            kernelRange = 1;
        }

        int baseX = Mth.floor(x);
        int baseY = Mth.floor(y);
        int baseZ = Mth.floor(z);

        double totalWeight = 0.0D;
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        for (int ox = -kernelRange; ox <= kernelRange; ox++) {
            for (int oy = -kernelRange; oy <= kernelRange; oy++) {
                for (int oz = -kernelRange; oz <= kernelRange; oz++) {
                    mutablePos.set(baseX + ox, baseY + oy, baseZ + oz);
                    if (!level.isInWorldBounds(mutablePos)) {
                        continue;
                    }

                    double cellCenterX = mutablePos.getX() + 0.5D;
                    double cellCenterY = mutablePos.getY() + 0.5D;
                    double cellCenterZ = mutablePos.getZ() + 0.5D;
                    double distance = Math.sqrt(
                            ((cellCenterX - x) * (cellCenterX - x))
                                    + ((cellCenterY - y) * (cellCenterY - y))
                                    + ((cellCenterZ - z) * (cellCenterZ - z))
                    );
                    double linearWeight = Math.max(0.0D, 1.0D - (distance / Math.max(smoothingRadius, 1.0E-6D)));
                    if (linearWeight <= 1.0E-6D) {
                        continue;
                    }
                    double weight = Math.sqrt(linearWeight);
                    totalWeight += weight;
                }
            }
        }

        if (totalWeight <= 1.0E-6D) {
            return;
        }

        for (int ox = -kernelRange; ox <= kernelRange; ox++) {
            for (int oy = -kernelRange; oy <= kernelRange; oy++) {
                for (int oz = -kernelRange; oz <= kernelRange; oz++) {
                    mutablePos.set(baseX + ox, baseY + oy, baseZ + oz);
                    if (!level.isInWorldBounds(mutablePos)) {
                        continue;
                    }

                    double cellCenterX = mutablePos.getX() + 0.5D;
                    double cellCenterY = mutablePos.getY() + 0.5D;
                    double cellCenterZ = mutablePos.getZ() + 0.5D;
                    double distance = Math.sqrt(
                            ((cellCenterX - x) * (cellCenterX - x))
                                    + ((cellCenterY - y) * (cellCenterY - y))
                                    + ((cellCenterZ - z) * (cellCenterZ - z))
                    );
                    double linearWeight = Math.max(0.0D, 1.0D - (distance / Math.max(smoothingRadius, 1.0E-6D)));
                    if (linearWeight <= 1.0E-6D) {
                        continue;
                    }
                    double weight = Math.sqrt(linearWeight);

                    impacts.addTo(mutablePos.asLong(), smoothedImpact * (weight / totalWeight));
                }
            }
        }
    }

    private static void applyPostExplosionDamageSmoothing(ServerLevel level, Long2ByteOpenHashMap survivingDamage) {
        if (survivingDamage.isEmpty()) {
            return;
        }

        Long2ByteOpenHashMap workingStates = new Long2ByteOpenHashMap(survivingDamage);
        BlockPos.MutableBlockPos neighborPos = new BlockPos.MutableBlockPos();
        for (int pass = 0; pass < POST_SMOOTH_PASSES; pass++) {
            Long2ByteOpenHashMap smoothedTargets = new Long2ByteOpenHashMap(Math.max(16, workingStates.size()));
            for (Long2ByteMap.Entry entry : workingStates.long2ByteEntrySet()) {
                long posLong = entry.getLongKey();
                int currentState = entry.getByteValue();
                if (currentState <= 0) {
                    continue;
                }

                BlockPos blockPos = BlockPos.of(posLong);
                if (!level.isInWorldBounds(blockPos)) {
                    continue;
                }

                BlockState centerState = level.getBlockState(blockPos);
                if (centerState.isAir() || centerState.getDestroySpeed(level, blockPos) < 0.0F) {
                    continue;
                }

                double weightedState = currentState * POST_SMOOTH_SELF_WEIGHT;
                double weightSum = POST_SMOOTH_SELF_WEIGHT;
                int contributingNeighbors = 0;
                int peakNeighborState = currentState;

                for (int ox = -POST_SMOOTH_RADIUS; ox <= POST_SMOOTH_RADIUS; ox++) {
                    for (int oy = -POST_SMOOTH_RADIUS; oy <= POST_SMOOTH_RADIUS; oy++) {
                        for (int oz = -POST_SMOOTH_RADIUS; oz <= POST_SMOOTH_RADIUS; oz++) {
                            if (ox == 0 && oy == 0 && oz == 0) {
                                continue;
                            }

                            neighborPos.set(blockPos.getX() + ox, blockPos.getY() + oy, blockPos.getZ() + oz);
                            if (!level.isInWorldBounds(neighborPos)) {
                                continue;
                            }

                            int neighborState = workingStates.get(neighborPos.asLong());
                            if (neighborState <= 0) {
                                continue;
                            }

                            BlockState neighborBlockState = level.getBlockState(neighborPos);
                            if (neighborBlockState.isAir() || neighborBlockState.getDestroySpeed(level, neighborPos) < 0.0F) {
                                continue;
                            }

                            int distSq = (ox * ox) + (oy * oy) + (oz * oz);
                            if (distSq <= 0) {
                                continue;
                            }
                            double distanceWeight = POST_SMOOTH_NEIGHBOR_WEIGHT / Math.sqrt(distSq);
                            weightedState += neighborState * distanceWeight;
                            weightSum += distanceWeight;
                            contributingNeighbors++;
                            if (neighborState > peakNeighborState) {
                                peakNeighborState = neighborState;
                            }
                        }
                    }
                }

                if (contributingNeighbors < POST_SMOOTH_MIN_NEIGHBORS || weightSum <= 1.0E-6D) {
                    continue;
                }

                double averageState = weightedState / weightSum;
                double blendedTarget = (averageState * (1.0D - POST_SMOOTH_PEAK_BLEND))
                        + (peakNeighborState * POST_SMOOTH_PEAK_BLEND);
                int targetState = Mth.clamp((int) Math.round(blendedTarget), currentState, POST_SMOOTH_MAX_DAMAGE_STATE);
                if (targetState > currentState) {
                    smoothedTargets.put(posLong, (byte) targetState);
                }
            }

            addEdgeFeatherTargets(level, workingStates, smoothedTargets);

            if (smoothedTargets.isEmpty()) {
                break;
            }

            for (Long2ByteMap.Entry entry : smoothedTargets.long2ByteEntrySet()) {
                BlockPos blockPos = BlockPos.of(entry.getLongKey());
                if (!level.isInWorldBounds(blockPos)) {
                    continue;
                }

                BlockState blockState = level.getBlockState(blockPos);
                if (blockState.isAir()) {
                    continue;
                }

                KrakkImpactResult result = KrakkApi.damage().accumulateTransferredDamageState(
                        level,
                        blockPos,
                        blockState,
                        entry.getByteValue(),
                        false
                );
                if (result.broken() || result.damageState() <= 0) {
                    workingStates.remove(entry.getLongKey());
                } else {
                    workingStates.put(entry.getLongKey(), (byte) result.damageState());
                }
            }
        }
    }

    private static void addEdgeFeatherTargets(ServerLevel level, Long2ByteOpenHashMap workingStates, Long2ByteOpenHashMap smoothedTargets) {
        if (workingStates.isEmpty()) {
            return;
        }

        LongOpenHashSet frontier = new LongOpenHashSet(Math.max(32, workingStates.size()));
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        for (Long2ByteMap.Entry entry : workingStates.long2ByteEntrySet()) {
            if (entry.getByteValue() <= 0) {
                continue;
            }
            BlockPos origin = BlockPos.of(entry.getLongKey());
            if (!level.isInWorldBounds(origin)) {
                continue;
            }

            for (int ox = -POST_EDGE_FRONTIER_RADIUS; ox <= POST_EDGE_FRONTIER_RADIUS; ox++) {
                for (int oy = -POST_EDGE_FRONTIER_RADIUS; oy <= POST_EDGE_FRONTIER_RADIUS; oy++) {
                    for (int oz = -POST_EDGE_FRONTIER_RADIUS; oz <= POST_EDGE_FRONTIER_RADIUS; oz++) {
                        if (ox == 0 && oy == 0 && oz == 0) {
                            continue;
                        }

                        mutablePos.set(origin.getX() + ox, origin.getY() + oy, origin.getZ() + oz);
                        if (!level.isInWorldBounds(mutablePos)) {
                            continue;
                        }
                        long candidateLong = mutablePos.asLong();
                        if (workingStates.get(candidateLong) > 0) {
                            continue;
                        }
                        frontier.add(candidateLong);
                    }
                }
            }
        }

        LongIterator iterator = frontier.iterator();
        while (iterator.hasNext()) {
            long candidateLong = iterator.nextLong();
            BlockPos candidatePos = BlockPos.of(candidateLong);
            if (!level.isInWorldBounds(candidatePos)) {
                continue;
            }
            BlockState candidateState = level.getBlockState(candidatePos);
            if (candidateState.isAir() || candidateState.getDestroySpeed(level, candidatePos) < 0.0F) {
                continue;
            }

            double weightedDamage = 0.0D;
            double totalWeight = 0.0D;
            int contributors = 0;
            int peakNeighbor = 0;
            for (int ox = -POST_EDGE_SAMPLE_RADIUS; ox <= POST_EDGE_SAMPLE_RADIUS; ox++) {
                for (int oy = -POST_EDGE_SAMPLE_RADIUS; oy <= POST_EDGE_SAMPLE_RADIUS; oy++) {
                    for (int oz = -POST_EDGE_SAMPLE_RADIUS; oz <= POST_EDGE_SAMPLE_RADIUS; oz++) {
                        if (ox == 0 && oy == 0 && oz == 0) {
                            continue;
                        }

                        mutablePos.set(candidatePos.getX() + ox, candidatePos.getY() + oy, candidatePos.getZ() + oz);
                        if (!level.isInWorldBounds(mutablePos)) {
                            continue;
                        }
                        int neighborDamage = workingStates.get(mutablePos.asLong());
                        if (neighborDamage <= 0) {
                            continue;
                        }

                        int distSq = (ox * ox) + (oy * oy) + (oz * oz);
                        if (distSq <= 0) {
                            continue;
                        }
                        double weight = POST_EDGE_NEIGHBOR_WEIGHT / Math.sqrt(distSq);
                        weightedDamage += neighborDamage * weight;
                        totalWeight += weight;
                        contributors++;
                        if (neighborDamage > peakNeighbor) {
                            peakNeighbor = neighborDamage;
                        }
                    }
                }
            }

            if (contributors < POST_EDGE_MIN_NEIGHBORS || totalWeight <= 1.0E-6D) {
                continue;
            }

            double averageDamage = weightedDamage / totalWeight;
            double featherDamage = (averageDamage * POST_EDGE_DAMAGE_SCALE) + (peakNeighbor * POST_EDGE_PEAK_SCALE);
            int targetState = Mth.clamp((int) Math.round(featherDamage), 1, POST_EDGE_MAX_DAMAGE_STATE);
            int existingTarget = smoothedTargets.get(candidateLong);
            if (targetState > existingTarget) {
                smoothedTargets.put(candidateLong, (byte) targetState);
            }
        }
    }

    private static final class ExplosionEntityContext {
        private final List<EntityHitProxy> all;
        private final PackedEntityOctree octree;
        private final EntityMacroGrid macroGrid;
        private final AABB queryBounds;
        private final double maxEntityRadius;
        private int[] nodeStack;
        private int nodeStackSize;
        private final int[] childNodeIndices;
        private final double[] childEntryDistances;
        private int nextRayId = 1;

        private ExplosionEntityContext(List<EntityHitProxy> all, PackedEntityOctree octree, EntityMacroGrid macroGrid,
                                       AABB queryBounds, double maxEntityRadius) {
            this.all = all;
            this.octree = octree;
            this.macroGrid = macroGrid;
            this.queryBounds = queryBounds;
            this.maxEntityRadius = maxEntityRadius;
            this.nodeStack = new int[64];
            this.nodeStackSize = 0;
            this.childNodeIndices = new int[8];
            this.childEntryDistances = new double[8];
        }

        private int nextRayId() {
            return this.nextRayId++;
        }

        private void resetTraversal() {
            this.nodeStackSize = 0;
        }

        private boolean hasNodes() {
            return this.nodeStackSize > 0;
        }

        private void pushNode(int nodeIndex) {
            if (this.nodeStackSize >= this.nodeStack.length) {
                this.nodeStack = java.util.Arrays.copyOf(this.nodeStack, this.nodeStack.length * 2);
            }
            this.nodeStack[this.nodeStackSize++] = nodeIndex;
        }

        private int popNode() {
            return this.nodeStack[--this.nodeStackSize];
        }

        private boolean mayRayEverHitEntities(double startX, double startY, double startZ,
                                              double dirX, double dirY, double dirZ,
                                              double maxDistance) {
            if (this.queryBounds == null || this.all.isEmpty() || maxDistance <= 1.0E-9D) {
                return false;
            }
            double expandedMinX = this.queryBounds.minX - this.maxEntityRadius;
            double expandedMinY = this.queryBounds.minY - this.maxEntityRadius;
            double expandedMinZ = this.queryBounds.minZ - this.maxEntityRadius;
            double expandedMaxX = this.queryBounds.maxX + this.maxEntityRadius;
            double expandedMaxY = this.queryBounds.maxY + this.maxEntityRadius;
            double expandedMaxZ = this.queryBounds.maxZ + this.maxEntityRadius;
            return !Double.isNaN(intersectRaySegmentAabbEntryDistance(
                    startX,
                    startY,
                    startZ,
                    dirX,
                    dirY,
                    dirZ,
                    maxDistance,
                    expandedMinX,
                    expandedMinY,
                    expandedMinZ,
                    expandedMaxX,
                    expandedMaxY,
                    expandedMaxZ
            ));
        }
    }

    private static final class EntityHitProxy {
        private final LivingEntity entity;
        private final AABB aabb;
        private final double rayResistanceCost;
        private final double massScale;
        private final double centerX;
        private final double centerY;
        private final double centerZ;
        private final double radiusSquared;
        private int lastRayId = -1;
        private double accumulatedImpact;
        private double pressureX;
        private double pressureY;
        private double pressureZ;

        private EntityHitProxy(LivingEntity entity, AABB aabb, double rayResistanceCost) {
            this.entity = entity;
            this.aabb = aabb;
            this.rayResistanceCost = rayResistanceCost;
            this.massScale = Math.max(0.75D, entity.getBbWidth() * entity.getBbHeight());
            this.centerX = (aabb.minX + aabb.maxX) * 0.5D;
            this.centerY = (aabb.minY + aabb.maxY) * 0.5D;
            this.centerZ = (aabb.minZ + aabb.maxZ) * 0.5D;
            double halfX = (aabb.maxX - aabb.minX) * 0.5D;
            double halfY = (aabb.maxY - aabb.minY) * 0.5D;
            double halfZ = (aabb.maxZ - aabb.minZ) * 0.5D;
            this.radiusSquared = (halfX * halfX) + (halfY * halfY) + (halfZ * halfZ);
        }
    }

    private static final class EntityMacroGrid {
        private static final int CELL_INDEX_BITS = 21;
        private static final long CELL_INDEX_MASK = (1L << CELL_INDEX_BITS) - 1L;

        private final double minX;
        private final double minY;
        private final double minZ;
        private final double maxX;
        private final double maxY;
        private final double maxZ;
        private final double cellSize;
        private final int sizeX;
        private final int sizeY;
        private final int sizeZ;
        private final LongOpenHashSet occupiedCells;

        private EntityMacroGrid(double minX, double minY, double minZ,
                                double maxX, double maxY, double maxZ,
                                double cellSize,
                                int sizeX, int sizeY, int sizeZ,
                                LongOpenHashSet occupiedCells) {
            this.minX = minX;
            this.minY = minY;
            this.minZ = minZ;
            this.maxX = maxX;
            this.maxY = maxY;
            this.maxZ = maxZ;
            this.cellSize = cellSize;
            this.sizeX = sizeX;
            this.sizeY = sizeY;
            this.sizeZ = sizeZ;
            this.occupiedCells = occupiedCells;
        }

        private static EntityMacroGrid build(List<EntityHitProxy> entities, AABB bounds, double cellSize) {
            if (entities.isEmpty() || cellSize <= 1.0E-6D) {
                return null;
            }
            int sizeX = Math.max(1, (int) Math.ceil((bounds.maxX - bounds.minX) / cellSize));
            int sizeY = Math.max(1, (int) Math.ceil((bounds.maxY - bounds.minY) / cellSize));
            int sizeZ = Math.max(1, (int) Math.ceil((bounds.maxZ - bounds.minZ) / cellSize));
            LongOpenHashSet occupied = new LongOpenHashSet(Math.max(32, entities.size() * 4));

            for (int i = 0; i < entities.size(); i++) {
                EntityHitProxy proxy = entities.get(i);
                int minCellX = toCellIndex(proxy.aabb.minX, bounds.minX, cellSize, sizeX);
                int minCellY = toCellIndex(proxy.aabb.minY, bounds.minY, cellSize, sizeY);
                int minCellZ = toCellIndex(proxy.aabb.minZ, bounds.minZ, cellSize, sizeZ);
                int maxCellX = toCellIndex(proxy.aabb.maxX, bounds.minX, cellSize, sizeX);
                int maxCellY = toCellIndex(proxy.aabb.maxY, bounds.minY, cellSize, sizeY);
                int maxCellZ = toCellIndex(proxy.aabb.maxZ, bounds.minZ, cellSize, sizeZ);

                for (int x = minCellX; x <= maxCellX; x++) {
                    for (int y = minCellY; y <= maxCellY; y++) {
                        for (int z = minCellZ; z <= maxCellZ; z++) {
                            occupied.add(packCellKey(x, y, z));
                        }
                    }
                }
            }

            if (occupied.isEmpty()) {
                return null;
            }
            return new EntityMacroGrid(
                    bounds.minX,
                    bounds.minY,
                    bounds.minZ,
                    bounds.maxX,
                    bounds.maxY,
                    bounds.maxZ,
                    cellSize,
                    sizeX,
                    sizeY,
                    sizeZ,
                    occupied
            );
        }

        private boolean mayIntersectSegment(double startX, double startY, double startZ,
                                            double dirX, double dirY, double dirZ,
                                            double segmentDistance) {
            if (segmentDistance <= 1.0E-9D) {
                return false;
            }

            double endX = startX + (dirX * segmentDistance);
            double endY = startY + (dirY * segmentDistance);
            double endZ = startZ + (dirZ * segmentDistance);
            double segMinX = Math.min(startX, endX);
            double segMinY = Math.min(startY, endY);
            double segMinZ = Math.min(startZ, endZ);
            double segMaxX = Math.max(startX, endX);
            double segMaxY = Math.max(startY, endY);
            double segMaxZ = Math.max(startZ, endZ);

            if (segMaxX < this.minX || segMinX > this.maxX
                    || segMaxY < this.minY || segMinY > this.maxY
                    || segMaxZ < this.minZ || segMinZ > this.maxZ) {
                return false;
            }

            int minCellX = toCellIndex(segMinX, this.minX, this.cellSize, this.sizeX);
            int minCellY = toCellIndex(segMinY, this.minY, this.cellSize, this.sizeY);
            int minCellZ = toCellIndex(segMinZ, this.minZ, this.cellSize, this.sizeZ);
            int maxCellX = toCellIndex(segMaxX, this.minX, this.cellSize, this.sizeX);
            int maxCellY = toCellIndex(segMaxY, this.minY, this.cellSize, this.sizeY);
            int maxCellZ = toCellIndex(segMaxZ, this.minZ, this.cellSize, this.sizeZ);

            for (int x = minCellX; x <= maxCellX; x++) {
                for (int y = minCellY; y <= maxCellY; y++) {
                    for (int z = minCellZ; z <= maxCellZ; z++) {
                        if (this.occupiedCells.contains(packCellKey(x, y, z))) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private static int toCellIndex(double coord, double origin, double cellSize, int size) {
            int idx = (int) Math.floor((coord - origin) / cellSize);
            if (idx < 0) {
                return 0;
            }
            if (idx >= size) {
                return size - 1;
            }
            return idx;
        }

        private static long packCellKey(int x, int y, int z) {
            return ((long) x & CELL_INDEX_MASK) << (CELL_INDEX_BITS * 2)
                    | (((long) y & CELL_INDEX_MASK) << CELL_INDEX_BITS)
                    | ((long) z & CELL_INDEX_MASK);
        }
    }

    private static final class PackedEntityOctree {
        private static final int MAX_DEPTH = 3;
        private static final int LEAF_ENTITY_CAP = 8;

        private final int rootIndex;
        private final int nodeCount;

        private final double[] minX;
        private final double[] minY;
        private final double[] minZ;
        private final double[] maxX;
        private final double[] maxY;
        private final double[] maxZ;
        private final double[] maxEntityRadius;
        private final int[] childBase;
        private final int[] childCount;
        private final int[] childMask;
        private final int[] leafEntityStart;
        private final int[] leafEntityCount;

        private final int[] childNodeIndices;
        private final byte[] childOctants;
        private final int[] leafEntityIndices;

        private PackedEntityOctree(
                int rootIndex,
                double[] minX,
                double[] minY,
                double[] minZ,
                double[] maxX,
                double[] maxY,
                double[] maxZ,
                double[] maxEntityRadius,
                int[] childBase,
                int[] childCount,
                int[] childMask,
                int[] leafEntityStart,
                int[] leafEntityCount,
                int[] childNodeIndices,
                byte[] childOctants,
                int[] leafEntityIndices
        ) {
            this.rootIndex = rootIndex;
            this.nodeCount = minX.length;
            this.minX = minX;
            this.minY = minY;
            this.minZ = minZ;
            this.maxX = maxX;
            this.maxY = maxY;
            this.maxZ = maxZ;
            this.maxEntityRadius = maxEntityRadius;
            this.childBase = childBase;
            this.childCount = childCount;
            this.childMask = childMask;
            this.leafEntityStart = leafEntityStart;
            this.leafEntityCount = leafEntityCount;
            this.childNodeIndices = childNodeIndices;
            this.childOctants = childOctants;
            this.leafEntityIndices = leafEntityIndices;
        }

        private static PackedEntityOctree build(List<EntityHitProxy> all, AABB bounds) {
            if (all.isEmpty()) {
                return null;
            }
            Builder builder = new Builder(Math.max(16, all.size() * 2), Math.max(16, all.size() * 4));
            IntArrayList indices = new IntArrayList(all.size());
            for (int i = 0; i < all.size(); i++) {
                indices.add(i);
            }
            int rootIndex = buildNode(
                    builder,
                    all,
                    indices,
                    bounds.minX,
                    bounds.minY,
                    bounds.minZ,
                    bounds.maxX,
                    bounds.maxY,
                    bounds.maxZ,
                    0
            );
            if (rootIndex < 0) {
                return null;
            }
            return builder.toPacked(rootIndex);
        }

        private static int buildNode(Builder builder,
                                     List<EntityHitProxy> all,
                                     IntArrayList indices,
                                     double minX, double minY, double minZ,
                                     double maxX, double maxY, double maxZ,
                                     int depth) {
            if (indices.isEmpty()) {
                return -1;
            }

            double maxRadius = 0.0D;
            for (int i = 0; i < indices.size(); i++) {
                EntityHitProxy proxy = all.get(indices.getInt(i));
                double radius = Math.sqrt(proxy.radiusSquared);
                if (radius > maxRadius) {
                    maxRadius = radius;
                }
            }

            int nodeIndex = builder.addNode(minX, minY, minZ, maxX, maxY, maxZ, maxRadius);
            if (depth >= MAX_DEPTH || indices.size() <= LEAF_ENTITY_CAP) {
                builder.setLeaf(nodeIndex, indices);
                return nodeIndex;
            }

            double midX = (minX + maxX) * 0.5D;
            double midY = (minY + maxY) * 0.5D;
            double midZ = (minZ + maxZ) * 0.5D;

            IntArrayList[] buckets = new IntArrayList[8];
            for (int i = 0; i < indices.size(); i++) {
                int entityIndex = indices.getInt(i);
                EntityHitProxy proxy = all.get(entityIndex);
                int octant = 0;
                if (proxy.centerX >= midX) {
                    octant |= 1;
                }
                if (proxy.centerY >= midY) {
                    octant |= 2;
                }
                if (proxy.centerZ >= midZ) {
                    octant |= 4;
                }
                IntArrayList bucket = buckets[octant];
                if (bucket == null) {
                    bucket = new IntArrayList();
                    buckets[octant] = bucket;
                }
                bucket.add(entityIndex);
            }

            int childCount = 0;
            int childMask = 0;
            int[] localChildNodeIndices = new int[8];
            byte[] localChildOctants = new byte[8];

            for (int octant = 0; octant < 8; octant++) {
                IntArrayList bucket = buckets[octant];
                if (bucket == null || bucket.isEmpty()) {
                    continue;
                }

                double childMinX = (octant & 1) == 0 ? minX : midX;
                double childMaxX = (octant & 1) == 0 ? midX : maxX;
                double childMinY = (octant & 2) == 0 ? minY : midY;
                double childMaxY = (octant & 2) == 0 ? midY : maxY;
                double childMinZ = (octant & 4) == 0 ? minZ : midZ;
                double childMaxZ = (octant & 4) == 0 ? midZ : maxZ;

                int childNodeIndex = buildNode(
                        builder,
                        all,
                        bucket,
                        childMinX,
                        childMinY,
                        childMinZ,
                        childMaxX,
                        childMaxY,
                        childMaxZ,
                        depth + 1
                );
                if (childNodeIndex >= 0) {
                    localChildNodeIndices[childCount] = childNodeIndex;
                    localChildOctants[childCount] = (byte) octant;
                    childMask |= (1 << octant);
                    childCount++;
                }
            }

            if (childCount <= 0) {
                builder.setLeaf(nodeIndex, indices);
                return nodeIndex;
            }

            int childBase = builder.childNodeIndices.size();
            for (int i = 0; i < childCount; i++) {
                builder.childNodeIndices.add(localChildNodeIndices[i]);
                builder.childOctants.add(localChildOctants[i]);
            }
            builder.setChildren(nodeIndex, childBase, childCount, childMask);
            return nodeIndex;
        }

        private int findChildNodeForOctant(int nodeIndex, int octant) {
            int base = this.childBase[nodeIndex];
            int count = this.childCount[nodeIndex];
            for (int i = 0; i < count; i++) {
                int childRef = base + i;
                if ((this.childOctants[childRef] & 0xFF) == octant) {
                    return this.childNodeIndices[childRef];
                }
            }
            return -1;
        }

        private static final class Builder {
            private final DoubleArrayList nodeMinX;
            private final DoubleArrayList nodeMinY;
            private final DoubleArrayList nodeMinZ;
            private final DoubleArrayList nodeMaxX;
            private final DoubleArrayList nodeMaxY;
            private final DoubleArrayList nodeMaxZ;
            private final DoubleArrayList nodeMaxEntityRadius;
            private final IntArrayList nodeChildBase;
            private final IntArrayList nodeChildCount;
            private final IntArrayList nodeChildMask;
            private final IntArrayList nodeLeafEntityStart;
            private final IntArrayList nodeLeafEntityCount;

            private final IntArrayList childNodeIndices;
            private final ByteArrayList childOctants;
            private final IntArrayList leafEntityIndices;

            private Builder(int estimatedNodes, int estimatedRefs) {
                this.nodeMinX = new DoubleArrayList(estimatedNodes);
                this.nodeMinY = new DoubleArrayList(estimatedNodes);
                this.nodeMinZ = new DoubleArrayList(estimatedNodes);
                this.nodeMaxX = new DoubleArrayList(estimatedNodes);
                this.nodeMaxY = new DoubleArrayList(estimatedNodes);
                this.nodeMaxZ = new DoubleArrayList(estimatedNodes);
                this.nodeMaxEntityRadius = new DoubleArrayList(estimatedNodes);
                this.nodeChildBase = new IntArrayList(estimatedNodes);
                this.nodeChildCount = new IntArrayList(estimatedNodes);
                this.nodeChildMask = new IntArrayList(estimatedNodes);
                this.nodeLeafEntityStart = new IntArrayList(estimatedNodes);
                this.nodeLeafEntityCount = new IntArrayList(estimatedNodes);

                this.childNodeIndices = new IntArrayList(estimatedRefs);
                this.childOctants = new ByteArrayList(estimatedRefs);
                this.leafEntityIndices = new IntArrayList(estimatedRefs);
            }

            private int addNode(double minX, double minY, double minZ,
                                double maxX, double maxY, double maxZ,
                                double maxEntityRadius) {
                int nodeIndex = this.nodeMinX.size();
                this.nodeMinX.add(minX);
                this.nodeMinY.add(minY);
                this.nodeMinZ.add(minZ);
                this.nodeMaxX.add(maxX);
                this.nodeMaxY.add(maxY);
                this.nodeMaxZ.add(maxZ);
                this.nodeMaxEntityRadius.add(maxEntityRadius);
                this.nodeChildBase.add(0);
                this.nodeChildCount.add(0);
                this.nodeChildMask.add(0);
                this.nodeLeafEntityStart.add(0);
                this.nodeLeafEntityCount.add(0);
                return nodeIndex;
            }

            private void setLeaf(int nodeIndex, IntArrayList entityIndices) {
                int leafStart = this.leafEntityIndices.size();
                for (int i = 0; i < entityIndices.size(); i++) {
                    this.leafEntityIndices.add(entityIndices.getInt(i));
                }
                this.nodeLeafEntityStart.set(nodeIndex, leafStart);
                this.nodeLeafEntityCount.set(nodeIndex, entityIndices.size());
                this.nodeChildBase.set(nodeIndex, 0);
                this.nodeChildCount.set(nodeIndex, 0);
                this.nodeChildMask.set(nodeIndex, 0);
            }

            private void setChildren(int nodeIndex, int childBase, int childCount, int childMask) {
                this.nodeChildBase.set(nodeIndex, childBase);
                this.nodeChildCount.set(nodeIndex, childCount);
                this.nodeChildMask.set(nodeIndex, childMask);
                this.nodeLeafEntityStart.set(nodeIndex, 0);
                this.nodeLeafEntityCount.set(nodeIndex, 0);
            }

            private PackedEntityOctree toPacked(int rootIndex) {
                return new PackedEntityOctree(
                        rootIndex,
                        this.nodeMinX.toDoubleArray(),
                        this.nodeMinY.toDoubleArray(),
                        this.nodeMinZ.toDoubleArray(),
                        this.nodeMaxX.toDoubleArray(),
                        this.nodeMaxY.toDoubleArray(),
                        this.nodeMaxZ.toDoubleArray(),
                        this.nodeMaxEntityRadius.toDoubleArray(),
                        this.nodeChildBase.toIntArray(),
                        this.nodeChildCount.toIntArray(),
                        this.nodeChildMask.toIntArray(),
                        this.nodeLeafEntityStart.toIntArray(),
                        this.nodeLeafEntityCount.toIntArray(),
                        this.childNodeIndices.toIntArray(),
                        this.childOctants.toByteArray(),
                        this.leafEntityIndices.toIntArray()
                );
            }
        }
    }

    private static final class SectionImpactAccumulator {
        private final Long2ObjectOpenHashMap<SectionImpactBuffer> sectionBuffers;

        private SectionImpactAccumulator(int estimatedSections) {
            this.sectionBuffers = new Long2ObjectOpenHashMap<>(Math.max(16, estimatedSections));
        }

        private boolean isEmpty() {
            return this.sectionBuffers.isEmpty();
        }

        private int sectionCount() {
            return this.sectionBuffers.size();
        }

        private void add(int x, int y, int z, double impact) {
            if (impact <= 0.0D) {
                return;
            }
            int sectionX = x >> 4;
            int sectionY = y >> 4;
            int sectionZ = z >> 4;
            long sectionKey = SectionPos.asLong(sectionX, sectionY, sectionZ);
            SectionImpactBuffer buffer = this.sectionBuffers.get(sectionKey);
            if (buffer == null) {
                buffer = new SectionImpactBuffer();
                this.sectionBuffers.put(sectionKey, buffer);
            }
            int localIndex = ((y & 15) << 8) | ((z & 15) << 4) | (x & 15);
            buffer.add(localIndex, impact);
        }

        private double getImpact(int x, int y, int z) {
            long sectionKey = SectionPos.asLong(x >> 4, y >> 4, z >> 4);
            SectionImpactBuffer buffer = this.sectionBuffers.get(sectionKey);
            if (buffer == null) {
                return 0.0D;
            }
            int localIndex = ((y & 15) << 8) | ((z & 15) << 4) | (x & 15);
            return buffer.values[localIndex];
        }

        private int countActiveImpacts() {
            int count = 0;
            for (var entry : this.sectionBuffers.long2ObjectEntrySet()) {
                SectionImpactBuffer buffer = entry.getValue();
                IntArrayList touched = buffer.touchedIndices;
                for (int i = 0; i < touched.size(); i++) {
                    int localIndex = touched.getInt(i);
                    if (buffer.values[localIndex] > 0.0D) {
                        count++;
                    }
                }
            }
            return count;
        }

        private void clampMaxImpact(double maxImpact) {
            for (var entry : this.sectionBuffers.long2ObjectEntrySet()) {
                SectionImpactBuffer buffer = entry.getValue();
                IntArrayList touched = buffer.touchedIndices;
                for (int i = 0; i < touched.size(); i++) {
                    int localIndex = touched.getInt(i);
                    if (buffer.values[localIndex] > maxImpact) {
                        buffer.values[localIndex] = maxImpact;
                    }
                }
            }
        }

        private void forEachImpact(ImpactConsumer consumer) {
            for (var entry : this.sectionBuffers.long2ObjectEntrySet()) {
                long sectionKey = entry.getLongKey();
                SectionImpactBuffer buffer = entry.getValue();
                int sectionX = SectionPos.x(sectionKey);
                int sectionY = SectionPos.y(sectionKey);
                int sectionZ = SectionPos.z(sectionKey);
                int baseX = sectionX << 4;
                int baseY = sectionY << 4;
                int baseZ = sectionZ << 4;

                IntArrayList touched = buffer.touchedIndices;
                for (int i = 0; i < touched.size(); i++) {
                    int localIndex = touched.getInt(i);
                    double impact = buffer.values[localIndex];
                    if (impact <= 0.0D) {
                        continue;
                    }
                    int localX = localIndex & 15;
                    int localZ = (localIndex >> 4) & 15;
                    int localY = (localIndex >> 8) & 15;
                    long posLong = BlockPos.asLong(baseX | localX, baseY | localY, baseZ | localZ);
                    consumer.accept(posLong, impact);
                }
            }
        }

        private void forEachImpactCoordinates(ImpactCoordinateConsumer consumer) {
            for (var entry : this.sectionBuffers.long2ObjectEntrySet()) {
                long sectionKey = entry.getLongKey();
                SectionImpactBuffer buffer = entry.getValue();
                int sectionX = SectionPos.x(sectionKey);
                int sectionY = SectionPos.y(sectionKey);
                int sectionZ = SectionPos.z(sectionKey);
                int baseX = sectionX << 4;
                int baseY = sectionY << 4;
                int baseZ = sectionZ << 4;

                IntArrayList touched = buffer.touchedIndices;
                for (int i = 0; i < touched.size(); i++) {
                    int localIndex = touched.getInt(i);
                    double impact = buffer.values[localIndex];
                    if (impact <= 0.0D) {
                        continue;
                    }
                    int localX = localIndex & 15;
                    int localZ = (localIndex >> 4) & 15;
                    int localY = (localIndex >> 8) & 15;
                    consumer.accept(baseX | localX, baseY | localY, baseZ | localZ, impact);
                }
            }
        }
    }

    private static final class SectionImpactBuffer {
        private final double[] values = new double[16 * 16 * 16];
        private final IntArrayList touchedIndices = new IntArrayList(96);

        private void add(int localIndex, double impact) {
            double previous = this.values[localIndex];
            if (previous == 0.0D) {
                this.touchedIndices.add(localIndex);
            }
            this.values[localIndex] = previous + impact;
        }
    }

    @FunctionalInterface
    private interface ImpactConsumer {
        void accept(long posLong, double impact);
    }

    @FunctionalInterface
    private interface ImpactCoordinateConsumer {
        void accept(int x, int y, int z, double impact);
    }

    private static final class RaycastState {
        private final int rayId;
        private double x;
        private double y;
        private double z;
        private final double dirX;
        private final double dirY;
        private final double dirZ;
        private double energy;
        private long lastPosLong;
        private int stepIndex;
        private final int splitDepth;
        private final boolean emitSmokeOnRay;
        private final boolean canHitEntities;
        private int cellX;
        private int cellY;
        private int cellZ;
        private final int stepX;
        private final int stepY;
        private final int stepZ;
        private final double tDeltaX;
        private final double tDeltaY;
        private final double tDeltaZ;
        private double tMaxX;
        private double tMaxY;
        private double tMaxZ;
        private double travelT;
        private final double minSplitTravelT;
        private double nextSplitEventT;

        private RaycastState(double x, double y, double z,
                             double dirX, double dirY, double dirZ,
                             double energy, long lastPosLong, int stepIndex, int splitDepth,
                             boolean emitSmokeOnRay, int rayId, boolean canHitEntities, double minSplitTravelT) {
            this.rayId = rayId;
            this.x = x;
            this.y = y;
            this.z = z;
            this.dirX = dirX;
            this.dirY = dirY;
            this.dirZ = dirZ;
            this.energy = energy;
            this.lastPosLong = lastPosLong;
            this.stepIndex = stepIndex;
            this.splitDepth = splitDepth;
            this.emitSmokeOnRay = emitSmokeOnRay;
            this.canHitEntities = canHitEntities;

            this.cellX = Mth.floor(x);
            this.cellY = Mth.floor(y);
            this.cellZ = Mth.floor(z);
            this.stepX = dirX > 0.0D ? 1 : (dirX < 0.0D ? -1 : 0);
            this.stepY = dirY > 0.0D ? 1 : (dirY < 0.0D ? -1 : 0);
            this.stepZ = dirZ > 0.0D ? 1 : (dirZ < 0.0D ? -1 : 0);
            this.tDeltaX = stepX == 0 ? Double.POSITIVE_INFINITY : Math.abs(1.0D / dirX);
            this.tDeltaY = stepY == 0 ? Double.POSITIVE_INFINITY : Math.abs(1.0D / dirY);
            this.tDeltaZ = stepZ == 0 ? Double.POSITIVE_INFINITY : Math.abs(1.0D / dirZ);
            this.tMaxX = initialTMax(x, cellX, stepX, dirX);
            this.tMaxY = initialTMax(y, cellY, stepY, dirY);
            this.tMaxZ = initialTMax(z, cellZ, stepZ, dirZ);
            this.travelT = 0.0D;
            this.minSplitTravelT = Math.max(0.0D, minSplitTravelT);
            this.nextSplitEventT = Double.POSITIVE_INFINITY;
        }

        private static double initialTMax(double origin, int cell, int step, double dir) {
            if (step == 0) {
                return Double.POSITIVE_INFINITY;
            }
            double nextBoundary = step > 0 ? (cell + 1.0D) : cell;
            return (nextBoundary - origin) / dir;
        }

        private int chooseAxisMask() {
            double min = Math.min(tMaxX, Math.min(tMaxY, tMaxZ));
            if (!Double.isFinite(min)) {
                return 0;
            }

            int mask = 0;
            if (tMaxX <= min) {
                mask |= 1;
            }
            if (tMaxY <= min) {
                mask |= 2;
            }
            if (tMaxZ <= min) {
                mask |= 4;
            }
            return mask;
        }

        private double nextBoundaryT(int axisMask) {
            double nextT = Double.POSITIVE_INFINITY;
            if ((axisMask & 1) != 0) {
                nextT = Math.min(nextT, tMaxX);
            }
            if ((axisMask & 2) != 0) {
                nextT = Math.min(nextT, tMaxY);
            }
            if ((axisMask & 4) != 0) {
                nextT = Math.min(nextT, tMaxZ);
            }
            return nextT;
        }

        private void advanceTo(double newT, int axisMask) {
            this.x += this.dirX * (newT - this.travelT);
            this.y += this.dirY * (newT - this.travelT);
            this.z += this.dirZ * (newT - this.travelT);
            this.travelT = newT;

            if ((axisMask & 1) != 0) {
                this.cellX += this.stepX;
                this.tMaxX += this.tDeltaX;
            }
            if ((axisMask & 2) != 0) {
                this.cellY += this.stepY;
                this.tMaxY += this.tDeltaY;
            }
            if ((axisMask & 4) != 0) {
                this.cellZ += this.stepZ;
                this.tMaxZ += this.tDeltaZ;
            }
        }
    }

    private record Direction(double x, double y, double z) {
    }

    private record VolumetricResistanceField(Long2FloatOpenHashMap resistanceByPos, LongArrayList solidPositions, int sampledVoxelCount) {
    }

    private static final class KrakkMinHeap {
        private int[] indices;
        private double[] priorities;
        private int size;
        private double polledPriority;

        private KrakkMinHeap(int initialCapacity) {
            int capacity = Math.max(16, initialCapacity);
            this.indices = new int[capacity];
            this.priorities = new double[capacity];
            this.size = 0;
            this.polledPriority = Double.POSITIVE_INFINITY;
        }

        private boolean isEmpty() {
            return size <= 0;
        }

        private void add(int index, double priority) {
            ensureCapacity(size + 1);
            int cursor = size++;
            while (cursor > 0) {
                int parent = (cursor - 1) >>> 1;
                if (priority >= priorities[parent]) {
                    break;
                }
                indices[cursor] = indices[parent];
                priorities[cursor] = priorities[parent];
                cursor = parent;
            }
            indices[cursor] = index;
            priorities[cursor] = priority;
        }

        private int pollIndex() {
            int rootIndex = indices[0];
            polledPriority = priorities[0];
            size--;
            if (size > 0) {
                int tailIndex = indices[size];
                double tailPriority = priorities[size];
                int cursor = 0;
                while (true) {
                    int left = (cursor << 1) + 1;
                    if (left >= size) {
                        break;
                    }
                    int right = left + 1;
                    int child = right < size && priorities[right] < priorities[left] ? right : left;
                    if (tailPriority <= priorities[child]) {
                        break;
                    }
                    indices[cursor] = indices[child];
                    priorities[cursor] = priorities[child];
                    cursor = child;
                }
                indices[cursor] = tailIndex;
                priorities[cursor] = tailPriority;
            }
            return rootIndex;
        }

        private double pollPriority() {
            return polledPriority;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity <= indices.length) {
                return;
            }
            int newCapacity = Math.max(minCapacity, indices.length << 1);
            indices = Arrays.copyOf(indices, newCapacity);
            priorities = Arrays.copyOf(priorities, newCapacity);
        }
    }

    private record VolumetricDirectionCache(double[] dirX,
                                            double[] dirY,
                                            double[] dirZ,
                                            int[][] neighbors,
                                            int[] directionLookup,
                                            int directionLookupResolution) {
    }

    private record VolumetricTargetScanContext(
            double centerX,
            double centerY,
            double centerZ,
            double resolvedRadius,
            double radialStepSize,
            int radialSteps,
            float[] pressureByShell,
            float[] maxPressureByShell,
            int directionCount,
            double[] dirX,
            double[] dirY,
            double[] dirZ,
            int[][] directionNeighbors,
            int[] directionLookup,
            int directionLookupResolution
    ) {
    }

    private record VolumetricTargetScanResult(
            LongArrayList targetPositions,
            FloatArrayList targetWeights,
            long precheckNanos,
            long blendNanos
    ) {
    }

    private record VolumetricResistanceFieldChunkResult(
            LongArrayList solidPositions,
            FloatArrayList solidResistance,
            int sampledVoxelCount
    ) {
    }

    private record ResistanceFieldSnapshot(BlockState[] sampledStates, int sampledVoxelCount) {
    }

    private record KrakkResistanceFieldChunkResult(
            LongArrayList solidPositions,
            int sampledVoxelCount
    ) {
    }

    private record KrakkTargetScanResult(
            LongArrayList targetPositions,
            FloatArrayList targetWeights,
            double solidWeight,
            long precheckNanos
    ) {
    }

    private record KrakkVolumetricBaselineResult(
            Long2FloatOpenHashMap baselineByPos,
            long directionSetupNanos,
            long pressureSolveNanos,
            long targetScanNanos,
            long targetScanPrecheckNanos,
            long targetScanBlendNanos,
            int directionSamples,
            int radialSteps
    ) {
    }

    private record KrakkSweepChunkResult(
            double maxDelta,
            IntArrayList dirtyRows
    ) {
    }

    private record KrakkField(
            int minX,
            int minY,
            int minZ,
            int sizeX,
            int sizeY,
            int sizeZ,
            int sampledVoxelCount,
            BitSet activeMask,
            float[] slowness,
            LongArrayList solidPositions,
            int[] activeRowOffsets,
            int[] activeRowLengths,
            int[] activeRowIndices,
            int[] neighborNegX,
            int[] neighborPosX,
            int[] neighborNegY,
            int[] neighborPosY,
            int[] neighborNegZ,
            int[] neighborPosZ
    ) {
    }

    private record KrakkCoarseSolveContext(
            KrakkField coarseField,
            float[] coarseArrivalTimes,
            float[] coarseSlowness,
            BitSet coarseSourceMask,
            int coarseSourceCount,
            int downsampleFactor
    ) {
    }

    private record KrakkSolveResult(float[] arrivalTimes, int sourceCells, int sweepCycles) {
    }

    private record PairedKrakkSolveResult(KrakkSolveResult normal, KrakkSolveResult shadow) {
    }

    private record PairedKrakkSlownessResult(float[] normalSlowness, float[] shadowSlowness) {
    }

    private record PairedKrakkSourceResult(int normalSourceCount, int shadowSourceCount) {
    }

    @Override
    public void detonate(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner, KrakkExplosionProfile profile) {
        ExplosionResolution resolution = resolveProfile(profile);
        detonateKrakk(
                level,
                x,
                y,
                z,
                source,
                owner,
                resolution.radius,
                resolution.energy,
                true,
                true,
                null
        );
    }

    public ExplosionProfileReport profileDetonate(ServerLevel level, double x, double y, double z, Entity source,
                                                  LivingEntity owner, KrakkExplosionProfile profile, boolean applyWorldChanges, long seed) {
        ExplosionResolution resolution = resolveProfile(profile);
        ExplosionProfileTrace trace = new ExplosionProfileTrace();
        long start = System.nanoTime();
        detonateKrakk(
                level,
                x,
                y,
                z,
                source,
                owner,
                resolution.radius,
                resolution.energy,
                applyWorldChanges,
                applyWorldChanges,
                trace
        );
        long elapsed = System.nanoTime() - start;
        return new ExplosionProfileReport(
                elapsed,
                applyWorldChanges,
                seed,
                trace.initialRays,
                trace.processedRays,
                trace.raySplits,
                trace.splitChecks,
                trace.raySteps,
                trace.rawImpactedBlocks,
                trace.postAaImpactedBlocks,
                trace.blocksEvaluated,
                trace.brokenBlocks,
                trace.damagedBlocks,
                trace.predictedBrokenBlocks,
                trace.predictedDamagedBlocks,
                trace.tntTriggered,
                trace.specialHandled,
                trace.lowImpactSkipped,
                trace.entityCandidates,
                trace.entityIntersectionTests,
                trace.entityHits,
                trace.octreeNodeTests,
                trace.octreeLeafVisits,
                trace.entityAffected,
                trace.entityDamaged,
                trace.entityKilled,
                trace.broadphaseNanos,
                trace.raycastNanos,
                trace.antialiasNanos,
                trace.blockResolveNanos,
                trace.splitCheckNanos,
                trace.entitySegmentNanos,
                trace.entityApplyNanos,
                trace.volumetricResistanceFieldNanos,
                trace.volumetricDirectionSetupNanos,
                trace.volumetricPressureSolveNanos,
                trace.krakkSolveNanos,
                trace.volumetricTargetScanNanos,
                trace.volumetricTargetScanPrecheckNanos,
                trace.volumetricTargetScanBlendNanos,
                trace.volumetricImpactApplyNanos,
                trace.volumetricImpactApplyDirectNanos,
                trace.volumetricImpactApplyCollapseSeedNanos,
                trace.volumetricImpactApplyCollapseBfsNanos,
                trace.volumetricImpactApplyCollapseApplyNanos,
                trace.volumetricSampledVoxels,
                trace.volumetricSampledSolids,
                trace.volumetricTargetBlocks,
                trace.volumetricDirectionSamples,
                trace.volumetricRadialSteps,
                trace.krakkSourceCells,
                trace.krakkSweepCycles,
                trace.syncPacketsEstimated,
                trace.syncBytesEstimated,
                trace.smokeParticles
        );
    }

    private static ExplosionResolution resolveProfile(KrakkExplosionProfile profile) {
        if (profile == null) {
            double fallbackPower = KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
            double resolvedRadius = sanitizeKrakkRadius(Double.NaN, fallbackPower);
            double resolvedEnergy = sanitizeKrakkEnergy(Double.NaN, fallbackPower);
            return new ExplosionResolution(resolvedRadius, resolvedEnergy);
        }

        double fallbackPower = KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
        double resolvedRadius = sanitizeKrakkRadius(profile.radius(), fallbackPower);
        double resolvedEnergy = sanitizeKrakkEnergy(profile.energy(), fallbackPower);
        return new ExplosionResolution(resolvedRadius, resolvedEnergy);
    }

    private static double sanitizeKrakkRadius(double radius, double fallbackPower) {
        if (Double.isFinite(radius)) {
            if (Math.abs(radius) <= 1.0E-9D) {
                return 0.0D;
            }
            if (radius > 0.0D) {
                return Math.max(1.0D, radius);
            }
        }
        double fallbackRadius = KrakkExplosionCurves.computeBlastRadius(fallbackPower);
        return Math.max(1.0D, fallbackRadius);
    }

    private static double resolveKrakkRadiusFromEnergyCutoff(double totalEnergy) {
        double normalizedEnergy = Math.max(totalEnergy, VOLUMETRIC_MIN_ENERGY);
        double decayPerBlock = Mth.clamp(VOLUMETRIC_PRESSURE_AIR_DECAY_PER_BLOCK, 1.0E-4D, 0.95D);
        double logDecay = Math.log1p(-decayPerBlock);
        double cutoffRatio = VOLUMETRIC_MIN_ENERGY / normalizedEnergy;
        double distance = Math.log(cutoffRatio) / logDecay;
        if (!Double.isFinite(distance)) {
            return 1.0D;
        }
        return Math.max(1.0D, distance);
    }

    private static double sanitizeKrakkEnergy(double energy, double fallbackPower) {
        if (Double.isFinite(energy) && energy > VOLUMETRIC_MIN_ENERGY) {
            return energy;
        }
        return Math.max(VOLUMETRIC_MIN_ENERGY, fallbackPower);
    }

    private record ExplosionResolution(double radius, double energy) {
    }

    private static final class ExplosionProfileTrace {
        private int initialRays;
        private int processedRays;
        private int raySplits;
        private int splitChecks;
        private int raySteps;
        private int rawImpactedBlocks;
        private int postAaImpactedBlocks;
        private int blocksEvaluated;
        private int brokenBlocks;
        private int damagedBlocks;
        private int predictedBrokenBlocks;
        private int predictedDamagedBlocks;
        private int tntTriggered;
        private int specialHandled;
        private int lowImpactSkipped;
        private int entityCandidates;
        private int entityIntersectionTests;
        private int entityHits;
        private int octreeNodeTests;
        private int octreeLeafVisits;
        private int entityAffected;
        private int entityDamaged;
        private int entityKilled;
        private long broadphaseNanos;
        private long raycastNanos;
        private long antialiasNanos;
        private long blockResolveNanos;
        private long splitCheckNanos;
        private long entitySegmentNanos;
        private long entityApplyNanos;
        private long volumetricResistanceFieldNanos;
        private long volumetricDirectionSetupNanos;
        private long volumetricPressureSolveNanos;
        private long krakkSolveNanos;
        private long volumetricTargetScanNanos;
        private long volumetricTargetScanPrecheckNanos;
        private long volumetricTargetScanBlendNanos;
        private long volumetricImpactApplyNanos;
        private long volumetricImpactApplyDirectNanos;
        private long volumetricImpactApplyCollapseSeedNanos;
        private long volumetricImpactApplyCollapseBfsNanos;
        private long volumetricImpactApplyCollapseApplyNanos;
        private int volumetricSampledVoxels;
        private int volumetricSampledSolids;
        private int volumetricTargetBlocks;
        private int volumetricDirectionSamples;
        private int volumetricRadialSteps;
        private int krakkSourceCells;
        private int krakkSweepCycles;
        private int syncPacketsEstimated;
        private int syncBytesEstimated;
        private int smokeParticles;
    }
}
