package org.shipwrights.krakk.runtime.explosion;

import com.mojang.logging.LogUtils;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.core.BlockPos;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.core.SectionPos;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.util.Mth;
import net.minecraft.util.RandomSource;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.entity.ai.attributes.Attributes;
import net.minecraft.world.entity.item.FallingBlockEntity;
import net.minecraft.world.entity.item.ItemEntity;
import net.minecraft.world.entity.item.PrimedTnt;
import net.minecraft.world.entity.projectile.Projectile;
import net.minecraft.world.entity.vehicle.AbstractMinecart;
import net.minecraft.world.damagesource.DamageSource;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.FallingBlock;
import net.minecraft.world.level.block.LiquidBlock;
import net.minecraft.world.level.block.TntBlock;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.Level;
import net.minecraft.world.phys.AABB;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.api.damage.KrakkDamageType;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.api.explosion.KrakkExplosionApi;
import org.shipwrights.krakk.api.explosion.KrakkExplosionProfile;
import org.shipwrights.krakk.engine.damage.KrakkDamageCurves;
import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;
import org.shipwrights.krakk.runtime.damage.KrakkDamageBlockConversions;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class KrakkExplosionRuntime implements KrakkExplosionApi {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final AtomicLong KRAKK_PHASE_TRACE_SEQUENCE = new AtomicLong(1L);
    private static volatile boolean krakkPhaseTimingLoggingEnabled = true;

    private static final double GOLDEN_ANGLE = Math.PI * (3.0D - Math.sqrt(5.0D));
    private static final double BLOCK_RESISTANCE_DAMPING = 0.17D;
    private static final double BLOCK_BASE_DAMPING = 0.08D;
    private static final double STONE_EQUIVALENT_FLUID_EXPLOSION_RESISTANCE = 6.0D;
    private static final float STONE_EQUIVALENT_FLUID_HARDNESS = 1.5F;
    private static final int FLUID_REMOVE_DAMAGE_THRESHOLD = 9;
    private static final double DEFAULT_RAY_SPLIT_DISTANCE_THRESHOLD = 0.5D;
    private static final double MIN_RAY_SPLIT_DISTANCE_THRESHOLD = 0.05D;
    private static final double MAX_RAY_SPLIT_DISTANCE_THRESHOLD = 64.0D;
    private static final double MIN_RESOLVED_RAY_IMPACT = 1.0E-4D;
    private static final double ENTITY_QUERY_MARGIN = 0.75D;
    private static final double ENTITY_HITBOX_INFLATE = 0.05D;
    // Entity impact power per unit radius for the volumetric detonation path, using eikonal arrival time sampling.
    // Tuned so a TNT-scale detonation (resolvedRadius ≈ 8) kills a full-health player at ground zero in open air.
    private static final double VOLUMETRIC_ENTITY_IMPACT_PER_RADIUS = 5.5D;
    private static final double ENTITY_MIN_APPLIED_IMPACT = 1.0E-4D;
    private static final double ENTITY_HP_STRENGTH_BASE = 0.60D;
    private static final double ENTITY_HP_STRENGTH_PER_HP = 0.08D;
    private static final double ENTITY_ARMOR_DAMAGE_DIVISOR = 0.06D;
    private static final double ENTITY_TOUGHNESS_DAMAGE_DIVISOR = 0.03D;
    private static final double ENTITY_KNOCKBACK_SCALE = 0.90D;
    private static final double ENTITY_KNOCKBACK_MAX = 6.0D;
    private static final double ENTITY_KNOCKBACK_UPWARD_BIAS = 0.7D;
    private static final double ENTITY_IGNITION_HEAT_CELSIUS = 100.0D;
    private static final double ENTITY_HEAT_PER_FIRE_SECOND = 100.0D;
    private static final int ENTITY_MAX_FIRE_SECONDS = 15;
    private static final double VOLUMETRIC_MIN_ENERGY = 1.0E-6D;
    private static final double VOLUMETRIC_DEFORM_SAMPLE_STEP = 0.5D;
    private static final double VOLUMETRIC_PRESSURE_AIR_DECAY_PER_BLOCK = 0.01D;
    private static final double VOLUMETRIC_PRESSURE_RESISTANCE_LOSS_SCALE = 0.04D;
    private static final double VOLUMETRIC_PRESSURE_DIFFUSION = 0.08D;
    private static final double VOLUMETRIC_PRESSURE_RECOVERY_PER_BLOCK = 0.08D;
    // 20 neighbors: worst-case top-8 is ~7° from seed; 20 neighbors span ~9.4° (safe margin).
    private static final int VOLUMETRIC_DIRECTION_NEIGHBOR_COUNT = 20;
    private static final int VOLUMETRIC_DIRECTION_BLEND_COUNT = 8;
    private static final double VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT = 1.7D;
    // LUT for pow(x, VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT) over x in [0, 1], resolution 1024.
    // Replaces 8 Math.pow calls per sampled block inside sampleBlendedVolumetricPressure.
    private static final float[] DOT_WEIGHT_LUT = buildDotWeightLut();
    private static float[] buildDotWeightLut() {
        float[] lut = new float[1024];
        for (int i = 0; i < 1024; i++) {
            lut[i] = (float) Math.pow(i / 1023.0, VOLUMETRIC_DIRECTION_BLEND_DOT_EXPONENT);
        }
        return lut;
    }
    private static final int VOLUMETRIC_DIRECTION_LOOKUP_RESOLUTION = 64;
    private static final double VOLUMETRIC_RADIUS_SCALE_BASE = 6.0D;
    private static final double VOLUMETRIC_MAX_POWER_RADIUS_SCALE = 8.0D;
    private static final double VOLUMETRIC_MAX_POINT_RADIUS_SCALE = 8.0D;
    private static final double VOLUMETRIC_DIRECTION_SAMPLES_PER_RADIUS2 = 6.0D;
    private static final int VOLUMETRIC_MIN_DIRECTION_SAMPLES = 128;
    private static final int VOLUMETRIC_MAX_DIRECTION_SAMPLES = 3072;
    private static final int VOLUMETRIC_MAX_RADIAL_STEPS = 128;
    private static final double VOLUMETRIC_EDGE_POINT_BIAS_BLEND = 0.70D;
    private static final double VOLUMETRIC_EDGE_SPIKE_START_FRACTION = 0.82D;
    private static final double VOLUMETRIC_EDGE_SPIKE_MAX_REDUCTION = 0.07D;
    private static final double VOLUMETRIC_EDGE_SPIKE_REDUCTION_EXPONENT = 2.6D;
    private static final double VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION = 72.0D;
    private static final double VOLUMETRIC_EDGE_SPIKE_MIN_STRENGTH = 0.08D;
    private static final double VOLUMETRIC_EDGE_SPIKE_RAMP_EXPONENT = 2.2D;
    private static final double VOLUMETRIC_AIR_DISTRIBUTION_BLEND = 0.78D;
    private static final double VOLUMETRIC_MAX_AIR_NORMALIZATION_SCALE = 8.0D;
    private static final double VOLUMETRIC_IMPACT_POWER_PER_ENERGY = 1500.0D;
    // Per-block damaging force = resolvedRadius * this * (blockWeight / maxBlockWeight).
    // Matches the ray system's rayEnergy = blastRadius * constant: larger blast → more force per
    // block, independent of how many solid blocks the volume contains.
    private static final double VOLUMETRIC_IMPACT_POWER_PER_RADIUS = 25.0D;
    private static final double STRUCTURAL_COLLAPSE_IMPACT_WEIGHT_SCALE = 0.60D;
    private static final int STRUCTURAL_COLLAPSE_MAX_VOXELS = 24_000_000;
    private static final double KRAKK_AIR_SLOWNESS = 1.0D;
    private static final double KRAKK_SOLID_SLOWNESS_SCALE = 0.040D;
    private static final double KRAKK_BLOCK_RESISTANCE_NOISE_MAX = 8.0D;
    private static final int KRAKK_BASE_SWEEP_CYCLES = 8;
    private static final int KRAKK_MAX_SWEEP_CYCLES = 24;
    private static final int KRAKK_MULTIRES_COARSE_MAX_SWEEP_CYCLES = 12;
    private static final double KRAKK_DELTA_STEPPING_BUCKET_WIDTH = 0.75D;
    private static final int KRAKK_DELTA_STEPPING_REFINE_SWEEP_CYCLES = 2;
    private static final int KRAKK_DELTA_STEPPING_PROGRESS_LOG_INTERVAL_BUCKETS = 2048;
    private static final int KRAKK_NB_JOBS_PER_TICK = 1;
    private static final long KRAKK_NB_TICK_BUDGET_NANOS = 35_000_000L;
    private static final int KRAKK_NB_POP_BUDGET = Integer.MAX_VALUE;
    private static final long KRAKK_NB_PROGRESS_LOG_INTERVAL_NANOS = 1_000_000_000L;
    private static final double KRAKK_NB_OVERRUN_ATTENUATION = 0.08D;
    private static final double KRAKK_NB_NORMALIZED_OVERRUN_ATTENUATION = 0.20D;
    private static final double KRAKK_NB_MAX_NORMALIZED_OVERRUN = 12.0D;
    private static final double KRAKK_NB_MIN_TRANSMITTANCE = 0.30D;
    private static final double KRAKK_NB_SHELL_IMPACT_NORMALIZATION = 24.0D;
    private static final double KRAKK_NB_MIN_SHELL_AREA = 16.0D;
    private static final int[][] KRAKK_STREAMING_WAVE_NEIGHBOR_OFFSETS = buildKrakkStreamingWaveNeighborOffsets();
    private static final int KRAKK_SHADOW_SOLVE_MAX_VOLUME = 180_000_000;
    private static final int KRAKK_SOLID_POSITIONS_PREALLOC_CAP = 4_000_000;
    private static final int KRAKK_CHUNK_SOLID_POSITIONS_PREALLOC_CAP = 1_000_000;
    private static final int KRAKK_TARGET_MERGE_PREALLOC_CAP = 2_000_000;
    private static final double KRAKK_CONVERGENCE_EPSILON = 1.0E-3D;
    private static final double KRAKK_MAX_ARRIVAL_MULTIPLIER = 1.20D;
    private static final double KRAKK_WEIGHT_EXPONENT = 2.0D;         // full transmittance (blastTransmittanceScale=1)
    private static final double KRAKK_WEIGHT_EXPONENT_SMOOTH = 0.7D; // smooth (blastTransmittanceScale=0)
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
    private static final boolean KRAKK_ENABLE_VOLUMETRIC_BASELINE_SMOOTHING = true;
    private static final double KRAKK_MIN_TRANSMITTANCE = 1.0E-3D;
    private static final double KRAKK_ENVELOPE_TRANSMITTANCE_BLEND = 0.25D;

    private static final double KRAKK_BASELINE_SMOOTH_BLEND = 0.92D;         // full transmittance (blastTransmittanceScale=1)
    private static final double KRAKK_BASELINE_SMOOTH_BLEND_FLOOR = 0.93D;  // smooth (blastTransmittanceScale=0)
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
    private static final int KRAKK_UNIFIED_MULTIRES_MAX_VOLUME = 120_000_000;
    private static final int RESISTANCE_FIELD_MIN_VOXELS_FOR_PARALLEL = 262_144;
    private static final int RESISTANCE_FIELD_MIN_COLUMNS_FOR_PARALLEL = 32;
    private static final int RESISTANCE_FIELD_COLUMNS_PER_TASK = 16;
    private static final long RESISTANCE_FIELD_MIN_VOXELS_PER_TASK = 786_432L;
    private static final long RESISTANCE_FIELD_MAX_PARALLEL_SNAPSHOT_VOXELS = 300_000_000L;
    private static final int RESISTANCE_FIELD_MAX_TASKS = 16;
    private static final int KRAKK_OFFHEAP_FLOAT_CHUNK_SHIFT = 20;
    private static final int KRAKK_OFFHEAP_FLOAT_CHUNK_SIZE = 1 << KRAKK_OFFHEAP_FLOAT_CHUNK_SHIFT;
    private static final int KRAKK_OFFHEAP_FLOAT_CHUNK_MASK = KRAKK_OFFHEAP_FLOAT_CHUNK_SIZE - 1;
    private static final int KRAKK_OFFHEAP_FLOAT_OFFLOAD_THRESHOLD = 4_194_304;
    private static final int KRAKK_SPARSE_SLOWNESS_OFFLOAD_THRESHOLD = 500_000_000;
    private static final int KRAKK_PAGED_FLOAT_OFFLOAD_THRESHOLD = Integer.MAX_VALUE;
    private static final int KRAKK_PAGED_FLOAT_PAGE_SHIFT = 16;
    private static final int KRAKK_PAGED_FLOAT_PAGE_SIZE = 1 << KRAKK_PAGED_FLOAT_PAGE_SHIFT;
    private static final int KRAKK_PAGED_FLOAT_PAGE_MASK = KRAKK_PAGED_FLOAT_PAGE_SIZE - 1;
    private static final int KRAKK_PAGED_FLOAT_CACHE_PAGES = 256;
    // Profiler (wevmKiLqWp): ForkJoinPool-5 workers spent 44% in awaitDone waiting for sweep
    // sub-tasks. Raised MIN_ROWS from 144→256 and ROWS_PER_TASK from 24→64 to reduce task
    // count ~2.7× and better amortize scheduling overhead against the 3 available workers.
    private static final int KRAKK_SWEEP_MIN_ROWS_FOR_PARALLEL = 64;
    private static final int KRAKK_SWEEP_ROWS_PER_TASK = 64;
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
    private static final Cleaner PAGED_FLOAT_STORAGE_CLEANER = Cleaner.create();
    private static final Direction[] THERMAL_EXPAND_DIRECTIONS = new Direction[]{
            new Direction(-1.0D, 0.0D, 0.0D),
            new Direction(1.0D, 0.0D, 0.0D),
            new Direction(0.0D, -1.0D, 0.0D),
            new Direction(0.0D, 1.0D, 0.0D),
            new Direction(0.0D, 0.0D, -1.0D),
            new Direction(0.0D, 0.0D, 1.0D)
    };
    private static final Map<Class<?>, Method> BLOCK_IGNITE_METHODS = new ConcurrentHashMap<>();
    private static final Set<Class<?>> BLOCK_NO_IGNITE_METHOD = ConcurrentHashMap.newKeySet();
    private static final Set<Class<?>> BLOCK_IGNITE_FAILURE_LOGGED = ConcurrentHashMap.newKeySet();
    private static volatile SpecialBlockHandler specialBlockHandler = SpecialBlockHandler.NOOP;
    private static final boolean parallelResistanceFieldSamplingEnabled = true;
    private static volatile double raySplitDistanceThreshold = DEFAULT_RAY_SPLIT_DISTANCE_THRESHOLD;
    private static final double EXTRA_RADIUS_FRACTION       = 0.30;
    // Outer spray must exceed stone's break threshold (durability≈3.5, need >52 for delta=15)
    private static final double NOISE_SPRAY_MAX_IMPACT = 80.0;
    private static final RuntimeExecutionPolicy DEFAULT_RUNTIME_EXECUTION_POLICY = new RuntimeExecutionPolicy(
            false,
            false,
            Integer.MAX_VALUE,
            KrakkArrivalSolver.SWEEP
    );
    private static final RuntimeExecutionPolicy UNIFIED_RUNTIME_EXECUTION_POLICY = new RuntimeExecutionPolicy(
            true,
            true,
            KRAKK_UNIFIED_MULTIRES_MAX_VOLUME,
            KrakkArrivalSolver.SWEEP
    );
    private static final RuntimeExecutionPolicy FAST_SWEEP_RUNTIME_EXECUTION_POLICY = new RuntimeExecutionPolicy(
            true,
            false,
            KRAKK_UNIFIED_MULTIRES_MAX_VOLUME,
            KrakkArrivalSolver.SWEEP
    );
    private static final RuntimeExecutionPolicy DELTA_STEPPING_RUNTIME_EXECUTION_POLICY = new RuntimeExecutionPolicy(
            true,
            false,
            KRAKK_UNIFIED_MULTIRES_MAX_VOLUME,
            KrakkArrivalSolver.DELTA_STEPPING
    );
    private static final RuntimeExecutionPolicy ORDERED_UPWIND_RUNTIME_EXECUTION_POLICY = new RuntimeExecutionPolicy(
            true,
            true,
            KRAKK_UNIFIED_MULTIRES_MAX_VOLUME,
            KrakkArrivalSolver.ORDERED_UPWIND
    );
    private static final ThreadLocal<RuntimeExecutionPolicy> ACTIVE_RUNTIME_EXECUTION_POLICY = ThreadLocal.withInitial(
            () -> DEFAULT_RUNTIME_EXECUTION_POLICY
    );
    private static final ArrayDeque<NarrowBandWavefrontJob> KRAKK_NB_JOB_QUEUE = new ArrayDeque<>();
    private static long nextNarrowBandWaveJobId = 1L;


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

    private static boolean tryTriggerNativeIgnite(ServerLevel level, BlockPos blockPos, BlockState blockState,
                                                  Entity source, LivingEntity owner) {
        Block block = blockState.getBlock();
        Method igniteMethod = resolveNativeIgniteMethod(block.getClass());
        if (igniteMethod == null) {
            return false;
        }

        Object[] args = buildIgniteArgs(igniteMethod, level, blockPos, blockState, source, owner);
        if (args == null) {
            BLOCK_NO_IGNITE_METHOD.add(block.getClass());
            BLOCK_IGNITE_METHODS.remove(block.getClass());
            return false;
        }

        try {
            Object result = igniteMethod.invoke(block, args);
            boolean handled = !returnsBoolean(igniteMethod) || Boolean.TRUE.equals(result);
            if (!handled) {
                return false;
            }
            if (isOnCaughtFireMethod(igniteMethod) && block instanceof TntBlock && level.getBlockState(blockPos).is(block)) {
                level.removeBlock(blockPos, false);
            }
            return true;
        } catch (IllegalAccessException | InvocationTargetException exception) {
            if (BLOCK_IGNITE_FAILURE_LOGGED.add(block.getClass())) {
                LOGGER.warn("Failed to invoke ignite handler {} on block {}", igniteMethod.getName(), block, exception);
            }
            return false;
        }
    }

    private static Method resolveNativeIgniteMethod(Class<?> blockClass) {
        Method cached = BLOCK_IGNITE_METHODS.get(blockClass);
        if (cached != null) {
            return cached;
        }
        if (BLOCK_NO_IGNITE_METHOD.contains(blockClass)) {
            return null;
        }

        Method resolved = findPublicIgniteMethod(blockClass, "ignite");
        if (resolved == null) {
            resolved = findPublicIgniteMethod(blockClass, "onCaughtFire");
        }
        if (resolved != null) {
            BLOCK_IGNITE_METHODS.put(blockClass, resolved);
            return resolved;
        }

        BLOCK_NO_IGNITE_METHOD.add(blockClass);
        return null;
    }

    private static Method findPublicIgniteMethod(Class<?> blockClass, String methodName) {
        for (Method method : blockClass.getMethods()) {
            if (!method.getName().equals(methodName)) {
                continue;
            }
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (!isIgniteCompatible(method)) {
                continue;
            }
            return method;
        }
        return null;
    }

    private static boolean isIgniteCompatible(Method method) {
        if (!returnsBoolean(method) && method.getReturnType() != Void.TYPE) {
            return false;
        }

        boolean hasLevel = false;
        boolean hasBlockPos = false;
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (BlockState.class.isAssignableFrom(parameterType)
                    || BlockPos.class.isAssignableFrom(parameterType)
                    || Level.class.isAssignableFrom(parameterType)
                    || net.minecraft.core.Direction.class.isAssignableFrom(parameterType)
                    || LivingEntity.class.isAssignableFrom(parameterType)
                    || Entity.class.isAssignableFrom(parameterType)) {
                if (BlockPos.class.isAssignableFrom(parameterType)) {
                    hasBlockPos = true;
                }
                if (Level.class.isAssignableFrom(parameterType)) {
                    hasLevel = true;
                }
                continue;
            }
            return false;
        }
        return hasLevel && hasBlockPos;
    }

    private static boolean returnsBoolean(Method method) {
        return method.getReturnType() == Boolean.TYPE || method.getReturnType() == Boolean.class;
    }

    private static boolean isOnCaughtFireMethod(Method method) {
        return "onCaughtFire".equals(method.getName());
    }

    private static Object[] buildIgniteArgs(Method method, ServerLevel level, BlockPos blockPos, BlockState blockState,
                                            Entity source, LivingEntity owner) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] args = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            if (BlockState.class.isAssignableFrom(parameterType)) {
                args[i] = blockState;
                continue;
            }
            if (BlockPos.class.isAssignableFrom(parameterType)) {
                args[i] = blockPos;
                continue;
            }
            if (Level.class.isAssignableFrom(parameterType)) {
                args[i] = level;
                continue;
            }
            if (net.minecraft.core.Direction.class.isAssignableFrom(parameterType)) {
                args[i] = null;
                continue;
            }
            if (LivingEntity.class.isAssignableFrom(parameterType)) {
                args[i] = owner;
                continue;
            }
            if (Entity.class.isAssignableFrom(parameterType)) {
                args[i] = owner != null ? owner : source;
                continue;
            }
            return null;
        }
        return args;
    }

    public static boolean isKrakkPhaseTimingLoggingEnabled() {
        return krakkPhaseTimingLoggingEnabled;
    }

    public static void setKrakkPhaseTimingLoggingEnabled(boolean enabled) {
        krakkPhaseTimingLoggingEnabled = enabled;
    }

    @SuppressWarnings("unused") // public API — available to dependent mods
    public static void runWithUnifiedExecutionPolicy(Runnable runnable) {
        runWithExecutionPolicy(UNIFIED_RUNTIME_EXECUTION_POLICY, runnable);
    }

    @SuppressWarnings("unused") // public API — available to dependent mods
    public static void runWithFastSweepingExecutionPolicy(Runnable runnable) {
        runWithExecutionPolicy(FAST_SWEEP_RUNTIME_EXECUTION_POLICY, runnable);
    }

    @SuppressWarnings("unused") // public API — available to dependent mods
    public static void runWithDeltaSteppingExecutionPolicy(Runnable runnable) {
        runWithExecutionPolicy(DELTA_STEPPING_RUNTIME_EXECUTION_POLICY, runnable);
    }

    @SuppressWarnings("unused") // public API — available to dependent mods
    public static void runWithOrderedUpwindExecutionPolicy(Runnable runnable) {
        runWithExecutionPolicy(ORDERED_UPWIND_RUNTIME_EXECUTION_POLICY, runnable);
    }

    private static void runWithExecutionPolicy(RuntimeExecutionPolicy policy, Runnable runnable) {
        if (runnable == null) {
            return;
        }
        RuntimeExecutionPolicy previous = ACTIVE_RUNTIME_EXECUTION_POLICY.get();
        ACTIVE_RUNTIME_EXECUTION_POLICY.set(policy == null ? DEFAULT_RUNTIME_EXECUTION_POLICY : policy);
        try {
            runnable.run();
        } finally {
            ACTIVE_RUNTIME_EXECUTION_POLICY.set(previous);
        }
    }

    private static RuntimeExecutionPolicy activeRuntimeExecutionPolicy() {
        RuntimeExecutionPolicy policy = ACTIVE_RUNTIME_EXECUTION_POLICY.get();
        return policy == null ? DEFAULT_RUNTIME_EXECUTION_POLICY : policy;
    }

    public static void tickQueuedWavefrontJobs(MinecraftServer server) {
        tickQueuedNarrowBandWavefrontJobs(server);
    }

    private static void tickQueuedNarrowBandWavefrontJobs(MinecraftServer server) {
        if (server == null) {
            return;
        }
        int processed = 0;
        while (processed < KRAKK_NB_JOBS_PER_TICK) {
            NarrowBandWavefrontJob job;
            synchronized (KRAKK_NB_JOB_QUEUE) {
                job = KRAKK_NB_JOB_QUEUE.peekFirst();
            }
            if (job == null) {
                return;
            }

            ServerLevel level = server.getLevel(job.dimension());
            if (level == null) {
                synchronized (KRAKK_NB_JOB_QUEUE) {
                    KRAKK_NB_JOB_QUEUE.pollFirst();
                }
                LOGGER.warn(
                        "Dropping queued Krakk narrow-band wavefront job {} because dimension {} is unavailable.",
                        job.jobId(),
                        job.dimension().location()
                );
                continue;
            }

            long deadlineNanos = System.nanoTime() + KRAKK_NB_TICK_BUDGET_NANOS;
            try {
                KrakkDamageRuntime.runBatchedSync(
                        level,
                        () -> advanceNarrowBandWavefrontJob(job, level, deadlineNanos)
                );
            } catch (Throwable throwable) {
                synchronized (KRAKK_NB_JOB_QUEUE) {
                    KRAKK_NB_JOB_QUEUE.pollFirst();
                }
                LOGGER.error(
                        "Queued Krakk narrow-band wavefront job {} failed (dimension={}, center=({}, {}, {}), radius={}, energy={}).",
                        job.jobId(),
                        job.dimension().location(),
                        job.centerX(),
                        job.centerY(),
                        job.centerZ(),
                        job.resolvedRadius(),
                        job.totalEnergy(),
                        throwable
                );
                if (throwable instanceof OutOfMemoryError) {
                    System.gc();
                }
                continue;
            }

            if (job.phase() == NarrowBandWavefrontPhase.COMPLETE || job.phase() == NarrowBandWavefrontPhase.FAILED) {
                synchronized (KRAKK_NB_JOB_QUEUE) {
                    KRAKK_NB_JOB_QUEUE.pollFirst();
                }
                if (job.phase() == NarrowBandWavefrontPhase.COMPLETE) {
                    LOGGER.info(
                            "Queued Krakk narrow-band wavefront job {} completed (dimension={}, center=({}, {}, {}), radius={}, elapsedMs={}, reason={}).",
                            job.jobId(),
                            job.dimension().location(),
                            job.centerX(),
                            job.centerY(),
                            job.centerZ(),
                            job.resolvedRadius(),
                            nanosToMillis(System.nanoTime() - job.queuedAtNanos()),
                            job.failureMessage() == null ? "completed" : job.failureMessage()
                    );
                } else {
                    LOGGER.warn(
                            "Queued Krakk narrow-band wavefront job {} ended in failed state (dimension={}, center=({}, {}, {}), radius={}, message={}).",
                            job.jobId(),
                            job.dimension().location(),
                            job.centerX(),
                            job.centerY(),
                            job.centerZ(),
                            job.resolvedRadius(),
                            job.failureMessage()
                    );
                }
            }
            maybeLogNarrowBandWavefrontProgress(job);
            processed++;
        }
    }

    private static void maybeLogNarrowBandWavefrontProgress(NarrowBandWavefrontJob job) {
        if (job == null || !job.phaseLoggingEnabled()) {
            return;
        }
        long now = System.nanoTime();
        if ((now - job.lastProgressLogNanos()) < KRAKK_NB_PROGRESS_LOG_INTERVAL_NANOS) {
            return;
        }
        job.lastProgressLogNanos(now);
        LOGGER.info(
                "Krakk phase trace #{} [nb-progress] jobId={} phase={} elapsedMs={} popped={} accepted={} queue={} sourceCount={} directApplied={} thermalApplied={} heap={}",
                job.phaseTraceId(),
                job.jobId(),
                job.phase(),
                nanosToMillis(now - job.queuedAtNanos()),
                job.poppedNodes,
                job.acceptedNodes,
                job.trialQueue().size(),
                job.sourceCount(),
                job.directImpactApplications,
                job.thermalImpactApplications,
                formatKrakkHeapSnapshot()
        );
    }


    private static <T> T callWithExecutionPolicy(RuntimeExecutionPolicy policy, Supplier<T> supplier) {
        if (supplier == null) {
            return null;
        }
        RuntimeExecutionPolicy previous = ACTIVE_RUNTIME_EXECUTION_POLICY.get();
        ACTIVE_RUNTIME_EXECUTION_POLICY.set(policy == null ? DEFAULT_RUNTIME_EXECUTION_POLICY : policy);
        try {
            return supplier.get();
        } finally {
            ACTIVE_RUNTIME_EXECUTION_POLICY.set(previous);
        }
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

    @SuppressWarnings("unused") // public API — available to dependent mods
    public static double getRaySplitDistanceThreshold() {
        return raySplitDistanceThreshold;
    }

    @SuppressWarnings("unused") // public API — available to dependent mods
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


    private static void detonateKrakk(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner,
                                      double blastRadius, double totalEnergy, double impactHeatCelsius, double blastTransmittance,
                                      double entityDamage, double entityKnockback, boolean blockDamage,
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
                impactHeatCelsius,
                blastTransmittance,
                entityDamage,
                entityKnockback,
                blockDamage,
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
                                              double impactHeatCelsius,
                                              double blastTransmittance,
                                              double entityDamage,
                                              double entityKnockback,
                                              boolean blockDamage,
                                              Entity source,
                                              LivingEntity owner,
                                              boolean applyWorldChanges,
                                              ExplosionProfileTrace trace) {
        boolean phaseLoggingEnabled = krakkPhaseTimingLoggingEnabled;
        long phaseTraceId = phaseLoggingEnabled ? KRAKK_PHASE_TRACE_SEQUENCE.getAndIncrement() : -1L;
        long phaseTotalStart = phaseLoggingEnabled ? System.nanoTime() : 0L;
        RuntimeExecutionPolicy executionPolicy = activeRuntimeExecutionPolicy();
        if (phaseLoggingEnabled) {
            LOGGER.info(
                    "Krakk phase trace #{} [start] center=({}, {}, {}) energy={} inputRadius={} heatC={} applyWorldChanges={} policy[solver={}, compactSourceTracking={}, forceDirectResistanceSampling={}, maxMultiresVolume={}] heap={}",
                    phaseTraceId,
                    centerX,
                    centerY,
                    centerZ,
                    totalEnergy,
                    blastRadius,
                    impactHeatCelsius,
                    applyWorldChanges,
                    executionPolicy.arrivalSolver(),
                    executionPolicy.compactSourceTracking(),
                    executionPolicy.forceDirectResistanceSampling(),
                    executionPolicy.maxMultiresVolume(),
                    formatKrakkHeapSnapshot()
            );
        }

        BlockPos centerPos = BlockPos.containing(centerX, centerY, centerZ);
        if (!level.isInWorldBounds(centerPos)) {
            if (phaseLoggingEnabled) {
                LOGGER.info(
                        "Krakk phase trace #{} [abort] out-of-world-bounds center={} heap={}",
                        phaseTraceId,
                        centerPos,
                        formatKrakkHeapSnapshot()
                );
            }
            return;
        }

        boolean explicitRadiusProvided = blastRadius > 1.0E-9D;
        boolean energyLimitedRadius = executionPolicy.arrivalSolver() == KrakkArrivalSolver.DELTA_STEPPING || !explicitRadiusProvided;
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
        int worldMinY = level.getMinBuildHeight();
        int worldMaxY = level.getMaxBuildHeight() - 1;
        minY = Math.max(minY, worldMinY);
        maxY = Math.min(maxY, worldMaxY);
        if (minY > maxY) {
            if (phaseLoggingEnabled) {
                LOGGER.info(
                        "Krakk phase trace #{} [abort] y-bounds-empty worldMinY={} worldMaxY={} resolvedBoundsY=[{}, {}] heap={}",
                        phaseTraceId,
                        worldMinY,
                        worldMaxY,
                        minY,
                        maxY,
                        formatKrakkHeapSnapshot()
                );
            }
            return;
        }
        long boundedVolume = Math.max(0L, (long) (maxX - minX + 1) * (long) (maxY - minY + 1) * (long) (maxZ - minZ + 1));
        if (phaseLoggingEnabled) {
            LOGGER.info(
                    "Krakk phase trace #{} [bounds] resolvedRadius={} radiusSq={} bounds[x={}..{}, y={}..{}, z={}..{}] boundedVolume={} heap={}",
                    phaseTraceId,
                    resolvedRadius,
                    radiusSq,
                    minX,
                    maxX,
                    minY,
                    maxY,
                    minZ,
                    maxZ,
                    boundedVolume,
                    formatKrakkHeapSnapshot()
            );
            if (executionPolicy.arrivalSolver() == KrakkArrivalSolver.DELTA_STEPPING) {
                LOGGER.info(
                        "Krakk phase trace #{} [bounds] delta-stepping energy-cutoff envelope active; explicitRadius={} ignored.",
                        phaseTraceId,
                        blastRadius
                );
            }
        }

        if (executionPolicy.arrivalSolver() == KrakkArrivalSolver.DELTA_STEPPING) {
            if (applyWorldChanges && trace == null) {
                enqueueNarrowBandWavefrontJob(
                        level,
                        centerX,
                        centerY,
                        centerZ,
                        resolvedRadius,
                        radiusSq,
                        minX,
                        maxX,
                        minY,
                        maxY,
                        minZ,
                        maxZ,
                        totalEnergy,
                        impactHeatCelsius,
                        source,
                        owner,
                        phaseLoggingEnabled,
                        phaseTraceId
                );
                return;
            }
            runNarrowBandWavefrontImmediate(
                    level,
                    centerX,
                    centerY,
                    centerZ,
                    resolvedRadius,
                    radiusSq,
                    minX,
                    maxX,
                    minY,
                    maxY,
                    minZ,
                    maxZ,
                    totalEnergy,
                    impactHeatCelsius,
                    source,
                    owner,
                    applyWorldChanges,
                    trace,
                    phaseLoggingEnabled,
                    phaseTraceId,
                    phaseTotalStart
            );
            return;
        }

        Int2DoubleOpenHashMap resistanceCostCache = new Int2DoubleOpenHashMap(256);
        resistanceCostCache.defaultReturnValue(Double.NaN);
        long fieldStart = (trace != null || phaseLoggingEnabled) ? System.nanoTime() : 0L;
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
        long fieldNanos = (trace != null || phaseLoggingEnabled) ? (System.nanoTime() - fieldStart) : 0L;
        if (trace != null) {
            trace.volumetricResistanceFieldNanos += fieldNanos;
            trace.volumetricSampledVoxels += krakkField.sampledVoxelCount();
            trace.volumetricSampledSolids += krakkField.solidPositions().size();
        }
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "field-build",
                    fieldNanos,
                    String.format(
                            "sampledVoxels=%d sampledSolids=%d fieldDims=[%d,%d,%d]",
                            krakkField.sampledVoxelCount(),
                            krakkField.solidPositions().size(),
                            krakkField.sizeX(),
                            krakkField.sizeY(),
                            krakkField.sizeZ()
                    )
            );
        }
        if (krakkField.sampledVoxelCount() <= 0 || krakkField.solidPositions().isEmpty()) {
            if (phaseLoggingEnabled) {
                LOGGER.info(
                        "Krakk phase trace #{} [abort] empty-field sampledVoxels={} sampledSolids={} totalMs={} heap={}",
                        phaseTraceId,
                        krakkField.sampledVoxelCount(),
                        krakkField.solidPositions().size(),
                        nanosToMillis(System.nanoTime() - phaseTotalStart),
                        formatKrakkHeapSnapshot()
                );
            }
            return;
        }

        boolean profileSubstages = trace != null;
        boolean requireSerializedFieldAccess = requiresSerializedFloatAccess(krakkField.slowness());
        Future<KrakkVolumetricBaselineResult> baselineFuture = null;
        if (KRAKK_SOLVE_POOL != null && !requireSerializedFieldAccess) {
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
        if (phaseLoggingEnabled) {
            LOGGER.info(
                    "Krakk phase trace #{} [baseline-schedule] mode={} serializedFieldAccess={} heap={}",
                    phaseTraceId,
                    baselineFuture == null ? "inline" : "async",
                    requireSerializedFieldAccess,
                    formatKrakkHeapSnapshot()
            );
        }

        long solveStart = (trace != null || phaseLoggingEnabled) ? System.nanoTime() : 0L;
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
        long solveNanos = (trace != null || phaseLoggingEnabled) ? (System.nanoTime() - solveStart) : 0L;
        if (trace != null) {
            trace.volumetricPressureSolveNanos += solveNanos;
            trace.krakkSolveNanos += solveNanos;
            trace.krakkSourceCells += solveResult.sourceCells();
            trace.krakkSweepCycles += solveResult.sweepCycles();
            if (shadowSolveResult != solveResult) {
                trace.krakkSourceCells += shadowSolveResult.sourceCells();
                trace.krakkSweepCycles += shadowSolveResult.sweepCycles();
            }
        }
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "krakk-solve",
                    solveNanos,
                    String.format(
                            "normal[sourceCells=%d,sweepCycles=%d] shadow[sourceCells=%d,sweepCycles=%d]",
                            solveResult.sourceCells(),
                            solveResult.sweepCycles(),
                            shadowSolveResult.sourceCells(),
                            shadowSolveResult.sweepCycles()
                    )
            );
        }

        long baselineResolveStart = phaseLoggingEnabled ? System.nanoTime() : 0L;
        String baselineResolveMode = baselineFuture == null ? "inline" : "future";
        KrakkVolumetricBaselineResult baselineResult;
        if (baselineFuture != null) {
            try {
                baselineResult = baselineFuture.get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                baselineFuture.cancel(true);
                baselineResolveMode = "future_fallback_interrupted";
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
                baselineResolveMode = "future_fallback_execution";
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
        long baselineResolveNanos = phaseLoggingEnabled ? (System.nanoTime() - baselineResolveStart) : 0L;
        if (trace != null) {
            trace.volumetricDirectionSetupNanos += baselineResult.directionSetupNanos();
            trace.volumetricPressureSolveNanos += baselineResult.pressureSolveNanos();
            trace.volumetricTargetScanNanos += baselineResult.targetScanNanos();
            trace.volumetricTargetScanPrecheckNanos += baselineResult.targetScanPrecheckNanos();
            trace.volumetricTargetScanBlendNanos += baselineResult.targetScanBlendNanos();
            trace.volumetricDirectionSamples += baselineResult.directionSamples();
            trace.volumetricRadialSteps += baselineResult.radialSteps();
        }
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "baseline-resolve",
                    baselineResolveNanos,
                    String.format(
                            "mode=%s directionSetupMs=%.3f pressureSolveMs=%.3f targetScanMs=%.3f targetPrecheckMs=%.3f targetBlendMs=%.3f directions=%d radialSteps=%d",
                            baselineResolveMode,
                            nanosToMillis(baselineResult.directionSetupNanos()),
                            nanosToMillis(baselineResult.pressureSolveNanos()),
                            nanosToMillis(baselineResult.targetScanNanos()),
                            nanosToMillis(baselineResult.targetScanPrecheckNanos()),
                            nanosToMillis(baselineResult.targetScanBlendNanos()),
                            baselineResult.directionSamples(),
                            baselineResult.radialSteps()
                    )
            );
        }

        FloatIndexedAccess arrivalTimes = solveResult.arrivalTimes();
        FloatIndexedAccess shadowArrivalTimes = shadowSolveResult.arrivalTimes();
        LongIndexedAccess solidPositions = krakkField.solidPositions();
        float[] baselineByIndex = baselineResult.baselineByIndex();
        double maxArrival = Math.max(1.0E-6D, resolvedRadius * KRAKK_MAX_ARRIVAL_MULTIPLIER);

        // Entity damage is applied before the block-damage target scan so that early-outs caused by
        // high-resistance surroundings (all blocks filtered out) do not also suppress entity damage.
        if (entityDamage > 0.0D || entityKnockback > 0.0D) {
            long entityApplyStart = phaseLoggingEnabled ? System.nanoTime() : 0L;
            ExplosionEntityContext entityContext = buildExplosionEntityContext(level, centerX, centerY, centerZ, resolvedRadius, trace);
            accumulateKrakkEntityImpacts(entityContext, arrivalTimes, shadowArrivalTimes, krakkField, maxArrival, resolvedRadius);
            applyEntityImpacts(level, source, owner, entityContext, impactHeatCelsius, entityDamage, entityKnockback, applyWorldChanges, trace);
            if (phaseLoggingEnabled) {
                logKrakkPhaseTiming(
                        phaseTraceId,
                        "entity-apply",
                        System.nanoTime() - entityApplyStart,
                        String.format("entityDamage=%.2f entityKnockback=%.2f", entityDamage, entityKnockback)
                );
            }
        }

        if (!blockDamage) {
            return;
        }
        long targetScanStart = (trace != null || phaseLoggingEnabled) ? System.nanoTime() : 0L;
        KrakkTargetScanResult targetScanResult = scanKrakkTargets(
                solidPositions,
                baselineByIndex,
                arrivalTimes,
                shadowArrivalTimes,
                krakkField,
                centerX,
                centerY,
                centerZ,
                maxArrival,
                blastTransmittance,
                !requiresSerializedFloatAccess(arrivalTimes) && !requiresSerializedFloatAccess(shadowArrivalTimes),
                profileSubstages
        );
        long targetScanNanos = (trace != null || phaseLoggingEnabled) ? (System.nanoTime() - targetScanStart) : 0L;
        LongArrayList targetPositions = targetScanResult.targetPositions();
        FloatArrayList targetWeights = targetScanResult.targetWeights();
        double solidWeight = targetScanResult.solidWeight();
        double maxWeight = Math.max(1.0E-12D, targetScanResult.maxWeight());
        if (trace != null) {
            trace.volumetricTargetScanNanos += targetScanNanos;
            trace.volumetricTargetScanPrecheckNanos += targetScanResult.precheckNanos();
        }
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "target-scan",
                    targetScanNanos,
                    String.format(
                            "targetCount=%d solidWeight=%s precheckMs=%.3f",
                            targetPositions.size(),
                            solidWeight,
                            nanosToMillis(targetScanResult.precheckNanos())
                    )
            );
        }
        if (targetPositions.isEmpty() || solidWeight <= VOLUMETRIC_MIN_ENERGY) {
            if (phaseLoggingEnabled) {
                LOGGER.info(
                        "Krakk phase trace #{} [abort] no-targets targetCount={} solidWeight={} totalMs={} heap={}",
                        phaseTraceId,
                        targetPositions.size(),
                        solidWeight,
                        nanosToMillis(System.nanoTime() - phaseTotalStart),
                        formatKrakkHeapSnapshot()
                );
            }
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
            if (phaseLoggingEnabled) {
                LOGGER.info(
                        "Krakk phase trace #{} [abort] normalization-weight-too-small normalizationWeight={} totalMs={} heap={}",
                        phaseTraceId,
                        normalizationWeight,
                        nanosToMillis(System.nanoTime() - phaseTotalStart),
                        formatKrakkHeapSnapshot()
                );
            }
            return;
        }

        Long2FloatOpenHashMap collapseWeightsByPos = new Long2FloatOpenHashMap(Math.max(16, targetPositions.size()));
        collapseWeightsByPos.defaultReturnValue(0.0F);
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        long impactApplyStart = (trace != null || phaseLoggingEnabled) ? System.nanoTime() : 0L;
        long impactApplyDirectStart = (trace != null || phaseLoggingEnabled) ? System.nanoTime() : 0L;
        int directImpactApplications = 0;
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
            double impactPower = resolvedRadius * VOLUMETRIC_IMPACT_POWER_PER_RADIUS * (weight / maxWeight);
            applySingleBlockImpact(
                    level,
                    mutablePos,
                    targetPosLong,
                    impactPower,
                    source,
                    owner,
                    impactHeatCelsius,
                    false,
                    applyWorldChanges,
                    trace
            );
            directImpactApplications++;
        }
        long impactApplyDirectNanos = (trace != null || phaseLoggingEnabled) ? (System.nanoTime() - impactApplyDirectStart) : 0L;
        if (trace != null) {
            trace.volumetricImpactApplyDirectNanos += impactApplyDirectNanos;
        }
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "impact-apply-direct",
                    impactApplyDirectNanos,
                    String.format(
                            "appliedBlocks=%d collapseSeedCount=%d impactBudget=%s normalizationWeight=%s",
                            directImpactApplications,
                            collapseWeightsByPos.size(),
                            impactBudget,
                            normalizationWeight
                    )
            );
        }
        applyExtraRadiusImpacts(
                level, centerX, centerY, centerZ, resolvedRadius,
                targetPositions, source, owner, impactHeatCelsius, blastTransmittance, applyWorldChanges, trace, mutablePos
        );
        long collapseApplyStart = phaseLoggingEnabled ? System.nanoTime() : 0L;
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
        long collapseApplyNanos = phaseLoggingEnabled ? (System.nanoTime() - collapseApplyStart) : 0L;
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "impact-collapse",
                    collapseApplyNanos,
                    String.format(
                            "seedCount=%d",
                            collapseWeightsByPos.size()
                    )
            );
        }
        int thermalImpactApplications = 0;
        if (hasThermalEffect(impactHeatCelsius)) {
            long thermalApplyStart = phaseLoggingEnabled ? System.nanoTime() : 0L;
            ThermalTargetShape thermalTargetShape = buildThermalTargetShape(targetPositions, targetWeights);
            if (!thermalTargetShape.positions().isEmpty()) {
                LongArrayList thermalPositions = thermalTargetShape.positions();
                Long2FloatOpenHashMap thermalWeightsByPos = thermalTargetShape.weightsByPos();
                long orderingSeed = mixOrderingSeed(centerX, centerY, centerZ, impactHeatCelsius, thermalPositions.size());
                int permutationStart = computePermutationStart(orderingSeed, thermalPositions.size());
                int permutationStep = computePermutationStep(orderingSeed, thermalPositions.size());
                for (int i = 0; i < thermalPositions.size(); i++) {
                    int index = Math.floorMod(permutationStart + (i * permutationStep), thermalPositions.size());
                    long thermalPosLong = thermalPositions.getLong(index);
                    float weight = thermalWeightsByPos.get(thermalPosLong);
                    if (weight <= 0.0F) {
                        continue;
                    }
                    double thermalHeat = computeHeatFalloff(impactHeatCelsius, weight, resolvedRadius);
                    applySingleBlockImpact(
                            level,
                            mutablePos,
                            thermalPosLong,
                            0.0D,
                            source,
                            owner,
                            thermalHeat,
                            true,
                            applyWorldChanges,
                            trace
                    );
                    thermalImpactApplications++;
                }
            }
            if (phaseLoggingEnabled) {
                long thermalApplyNanos = System.nanoTime() - thermalApplyStart;
                logKrakkPhaseTiming(
                        phaseTraceId,
                        "impact-thermal",
                        thermalApplyNanos,
                        String.format(
                                "thermalTargets=%d appliedBlocks=%d",
                                thermalTargetShape.positions().size(),
                                thermalImpactApplications
                        )
                );
            }
        }
        long impactApplyNanos = (trace != null || phaseLoggingEnabled) ? (System.nanoTime() - impactApplyStart) : 0L;
        if (trace != null) {
            trace.volumetricImpactApplyNanos += impactApplyNanos;
        }
        if (phaseLoggingEnabled) {
            logKrakkPhaseTiming(
                    phaseTraceId,
                    "impact-total",
                    impactApplyNanos,
                    String.format(
                            "directApplied=%d thermalApplied=%d",
                            directImpactApplications,
                            thermalImpactApplications
                    )
            );
            LOGGER.info(
                    "Krakk phase trace #{} [complete] totalMs={} blocksEvaluated={} broken={} damaged={} predictedBroken={} predictedDamaged={} heap={}",
                    phaseTraceId,
                    nanosToMillis(System.nanoTime() - phaseTotalStart),
                    trace == null ? -1 : trace.blocksEvaluated,
                    trace == null ? -1 : trace.brokenBlocks,
                    trace == null ? -1 : trace.damagedBlocks,
                    trace == null ? -1 : trace.predictedBrokenBlocks,
                    trace == null ? -1 : trace.predictedDamagedBlocks,
                    formatKrakkHeapSnapshot()
            );
        }
    }

    private static void enqueueNarrowBandWavefrontJob(ServerLevel level,
                                                      double centerX,
                                                      double centerY,
                                                      double centerZ,
                                                      double resolvedRadius,
                                                      double radiusSq,
                                                      int minX,
                                                      int maxX,
                                                      int minY,
                                                      int maxY,
                                                      int minZ,
                                                      int maxZ,
                                                      double totalEnergy,
                                                      double impactHeatCelsius,
                                                      Entity source,
                                                      LivingEntity owner,
                                                      boolean phaseLoggingEnabled,
                                                      long phaseTraceId) {
        long jobId;
        int queueDepth;
        synchronized (KRAKK_NB_JOB_QUEUE) {
            jobId = nextNarrowBandWaveJobId++;
            KRAKK_NB_JOB_QUEUE.addLast(new NarrowBandWavefrontJob(
                    jobId,
                    level.dimension(),
                    centerX,
                    centerY,
                    centerZ,
                    resolvedRadius,
                    radiusSq,
                    minX,
                    maxX,
                    minY,
                    maxY,
                    minZ,
                    maxZ,
                    totalEnergy,
                    impactHeatCelsius,
                    source == null ? null : source.getUUID(),
                    owner == null ? null : owner.getUUID(),
                    true,
                    null,
                    phaseLoggingEnabled,
                    phaseTraceId,
                    System.nanoTime()
            ));
            queueDepth = KRAKK_NB_JOB_QUEUE.size();
        }
        if (phaseLoggingEnabled) {
            LOGGER.info(
                    "Krakk phase trace #{} [queued] narrow-band jobId={} dim={} center=({}, {}, {}) resolvedRadius={} queueDepth={} heap={}",
                    phaseTraceId,
                    jobId,
                    level.dimension().location(),
                    centerX,
                    centerY,
                    centerZ,
                    resolvedRadius,
                    queueDepth,
                    formatKrakkHeapSnapshot()
            );
        }
    }

    private static void runNarrowBandWavefrontImmediate(ServerLevel level,
                                                        double centerX,
                                                        double centerY,
                                                        double centerZ,
                                                        double resolvedRadius,
                                                        double radiusSq,
                                                        int minX,
                                                        int maxX,
                                                        int minY,
                                                        int maxY,
                                                        int minZ,
                                                        int maxZ,
                                                        double totalEnergy,
                                                        double impactHeatCelsius,
                                                        Entity source,
                                                        LivingEntity owner,
                                                        boolean applyWorldChanges,
                                                        ExplosionProfileTrace trace,
                                                        boolean phaseLoggingEnabled,
                                                        long phaseTraceId,
                                                        long phaseTotalStart) {
        NarrowBandWavefrontJob job = new NarrowBandWavefrontJob(
                -1L,
                level.dimension(),
                centerX,
                centerY,
                centerZ,
                resolvedRadius,
                radiusSq,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ,
                totalEnergy,
                impactHeatCelsius,
                source == null ? null : source.getUUID(),
                owner == null ? null : owner.getUUID(),
                applyWorldChanges,
                trace,
                phaseLoggingEnabled,
                phaseTraceId,
                System.nanoTime()
        );

        while (job.phase() != NarrowBandWavefrontPhase.COMPLETE
                && job.phase() != NarrowBandWavefrontPhase.FAILED) {
            advanceNarrowBandWavefrontJob(job, level, Long.MAX_VALUE);
        }

        if (phaseLoggingEnabled) {
            LOGGER.info(
                    "Krakk phase trace #{} [{}] narrow-band immediate totalMs={} blocksEvaluated={} broken={} damaged={} predictedBroken={} predictedDamaged={} heap={}",
                    phaseTraceId,
                    job.phase() == NarrowBandWavefrontPhase.COMPLETE ? "complete" : "abort",
                    nanosToMillis(System.nanoTime() - phaseTotalStart),
                    trace == null ? -1 : trace.blocksEvaluated,
                    trace == null ? -1 : trace.brokenBlocks,
                    trace == null ? -1 : trace.damagedBlocks,
                    trace == null ? -1 : trace.predictedBrokenBlocks,
                    trace == null ? -1 : trace.predictedDamagedBlocks,
                    formatKrakkHeapSnapshot()
            );
        }
    }

    private static void advanceNarrowBandWavefrontJob(NarrowBandWavefrontJob job, ServerLevel level, long deadlineNanos) {
        while (job.phase() != NarrowBandWavefrontPhase.COMPLETE
                && job.phase() != NarrowBandWavefrontPhase.FAILED
                && System.nanoTime() < deadlineNanos) {
            if (job.phase() == NarrowBandWavefrontPhase.SEED) {
                seedNarrowBandWavefrontJob(job, level);
                continue;
            }

            if (job.phase() == NarrowBandWavefrontPhase.PROPAGATE) {
                WavefrontSolveStats stats = stepNarrowBandWavefront(level, job, deadlineNanos);
                job.poppedNodes += stats.poppedNodes();
                job.acceptedNodes += stats.acceptedNodes();
                if (job.trialQueue().isEmpty()) {
                    long now = System.nanoTime();
                    long solveNanos = now - job.solveStartNanos();
                    if (job.trace() != null) {
                        job.trace().volumetricPressureSolveNanos += solveNanos;
                        job.trace().krakkSolveNanos += solveNanos;
                        job.trace().krakkSourceCells += job.sourceCount();
                        job.trace().krakkSweepCycles += job.poppedNodes;
                        job.trace().rawImpactedBlocks += job.directImpactApplications;
                        job.trace().postAaImpactedBlocks += job.directImpactApplications;
                        job.trace().volumetricTargetBlocks += job.directImpactApplications;
                        job.trace().volumetricImpactApplyDirectNanos += (now - job.impactApplyStartNanos());
                    }
                    if (job.phaseLoggingEnabled()) {
                        logKrakkPhaseTiming(
                                job.phaseTraceId(),
                                "nb-wavefront-solve",
                                solveNanos,
                                String.format(
                                        "jobId=%d source=%d accepted=%d popped=%d directApplied=%d thermalApplied=%d weight=%s",
                                        job.jobId(),
                                        job.sourceCount(),
                                        job.acceptedNodes,
                                        job.poppedNodes,
                                        job.directImpactApplications,
                                        job.thermalImpactApplications,
                                        job.directWeight
                                )
                        );
                    }
                    finishNarrowBandWavefrontJob(job, null);
                }
            }
        }
    }

    private static void seedNarrowBandWavefrontJob(NarrowBandWavefrontJob job, ServerLevel level) {
        int baseX = Mth.floor(job.centerX());
        int baseY = Mth.floor(job.centerY());
        int baseZ = Mth.floor(job.centerZ());
        int seedMinX = Math.max(job.minX(), baseX - 1);
        int seedMaxX = Math.min(job.maxX(), baseX + 1);
        int seedMinY = Math.max(job.minY(), baseY - 1);
        int seedMaxY = Math.min(job.maxY(), baseY + 1);
        int seedMinZ = Math.max(job.minZ(), baseZ - 1);
        int seedMaxZ = Math.min(job.maxZ(), baseZ + 1);

        int sourceCount = 0;
        for (int x = seedMinX; x <= seedMaxX; x++) {
            for (int y = seedMinY; y <= seedMaxY; y++) {
                for (int z = seedMinZ; z <= seedMaxZ; z++) {
                    double dx = (x + 0.5D) - job.centerX();
                    double dy = (y + 0.5D) - job.centerY();
                    double dz = (z + 0.5D) - job.centerZ();
                    double distSq = (dx * dx) + (dy * dy) + (dz * dz);
                    if (distSq > job.radiusSq()) {
                        continue;
                    }
                    long posLong = BlockPos.asLong(x, y, z);
                    float slowness = resolveStreamingNormalSlowness(
                            level,
                            x,
                            y,
                            z,
                            posLong,
                            job.mutablePos(),
                            job.chunkStateCache(),
                            job.resistanceCostCache(),
                            job.slownessByPos(),
                            null
                    );
                    if (slowness <= 0.0F) {
                        continue;
                    }
                    double arrival = Math.sqrt(distSq) * slowness;
                    if (enqueueStreamingSource(posLong, arrival, job.arrivalByPos(), job.trialQueue())) {
                        sourceCount++;
                    }
                }
            }
        }

        int seedX = Mth.clamp(baseX, job.minX(), job.maxX());
        int seedY = Mth.clamp(baseY, job.minY(), job.maxY());
        int seedZ = Mth.clamp(baseZ, job.minZ(), job.maxZ());
        long seedPosLong = BlockPos.asLong(seedX, seedY, seedZ);
        float seedSlowness = resolveStreamingNormalSlowness(
                level,
                seedX,
                seedY,
                seedZ,
                seedPosLong,
                job.mutablePos(),
                job.chunkStateCache(),
                job.resistanceCostCache(),
                job.slownessByPos(),
                null
        );
        if (seedSlowness > 0.0F && enqueueStreamingSource(seedPosLong, 0.0D, job.arrivalByPos(), job.trialQueue())) {
            sourceCount++;
        }

        job.sourceCount(sourceCount);
        if (sourceCount <= 0) {
            finishNarrowBandWavefrontJob(job, "nb-wavefront-no-sources");
            return;
        }

        // Narrow-band propagation is already geometrically clipped by radiusSq in neighbor expansion.
        // Keep arrival uncapped to avoid solver-shape artifacts from material-dependent slowness.
        job.maxArrival(Double.POSITIVE_INFINITY);
        double powerScale = computeVolumetricRadiusScale(job.resolvedRadius(), VOLUMETRIC_MAX_POWER_RADIUS_SCALE);
        job.impactBudget = job.totalEnergy() * VOLUMETRIC_IMPACT_POWER_PER_ENERGY * powerScale;
        long now = System.nanoTime();
        job.solveStartNanos(now);
        job.impactApplyStartNanos(now);
        job.setPhase(NarrowBandWavefrontPhase.PROPAGATE);
    }

    private static WavefrontSolveStats stepNarrowBandWavefront(ServerLevel level,
                                                               NarrowBandWavefrontJob job,
                                                               long deadlineNanos) {
        int poppedNodes = 0;
        int acceptedNodes = 0;
        Entity source = job.resolveSource(level);
        LivingEntity owner = job.resolveOwner(level);
        boolean thermalEnabled = hasThermalEffect(job.impactHeatCelsius());

        while (!job.trialQueue().isEmpty() && poppedNodes < KRAKK_NB_POP_BUDGET) {
            if ((poppedNodes & 127) == 0 && System.nanoTime() >= deadlineNanos) {
                break;
            }
            long posLong = job.trialQueue().pollKey();
            double queuedArrival = job.trialQueue().pollPriority();
            poppedNodes++;

            double currentArrival = job.arrivalByPos().get(posLong);
            if (!Double.isFinite(currentArrival) || queuedArrival > currentArrival + 1.0E-9D) {
                continue;
            }
            if (currentArrival > job.maxArrival()) {
                continue;
            }
            if (!job.finalized().add(posLong)) {
                continue;
            }
            acceptedNodes++;

            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            float currentSlowness = resolveStreamingNormalSlowness(
                    level,
                    x,
                    y,
                    z,
                    posLong,
                    job.mutablePos(),
                    job.chunkStateCache(),
                    job.resistanceCostCache(),
                    job.slownessByPos(),
                    null
            );
            if (currentSlowness <= 0.0F) {
                continue;
            }

            if (currentSlowness > (float) (KRAKK_AIR_SLOWNESS + 1.0E-6D)) {
                StreamingImpactSample impactSample = resolveNarrowBandImpactSample(
                        job,
                        currentArrival,
                        currentSlowness,
                        x,
                        y,
                        z
                );
                if (impactSample.impactPower() > MIN_RESOLVED_RAY_IMPACT) {
                    applySingleBlockImpact(
                            level,
                            job.mutablePos(),
                            posLong,
                            impactSample.impactPower(),
                            source,
                            owner,
                            job.impactHeatCelsius(),
                            false,
                            job.applyWorldChanges(),
                            job.trace()
                    );
                    job.directImpactApplications++;
                    job.directWeight += impactSample.weight();

                    if (thermalEnabled) {
                        double thermalHeat = computeHeatFalloff(
                                job.impactHeatCelsius(),
                                impactSample.weight(),
                                job.resolvedRadius()
                        );
                        for (int axis = 0; axis < 6; axis++) {
                            int tx = x;
                            int ty = y;
                            int tz = z;
                            if (axis == 0) {
                                tx--;
                            } else if (axis == 1) {
                                tx++;
                            } else if (axis == 2) {
                                ty--;
                            } else if (axis == 3) {
                                ty++;
                            } else if (axis == 4) {
                                tz--;
                            } else {
                                tz++;
                            }
                            applySingleBlockImpact(
                                    level,
                                    job.mutablePos(),
                                    BlockPos.asLong(tx, ty, tz),
                                    0.0D,
                                    source,
                                    owner,
                                    thermalHeat,
                                    true,
                                    job.applyWorldChanges(),
                                    job.trace()
                            );
                            job.thermalImpactApplications++;
                        }
                    }
                }
            }

            for (int[] offset : KRAKK_STREAMING_WAVE_NEIGHBOR_OFFSETS) {
                int nx = x + offset[0];
                int ny = y + offset[1];
                int nz = z + offset[2];
                if (nx < job.minX() || nx > job.maxX()
                        || ny < job.minY() || ny > job.maxY()
                        || nz < job.minZ() || nz > job.maxZ()) {
                    continue;
                }
                double dx = (nx + 0.5D) - job.centerX();
                double dy = (ny + 0.5D) - job.centerY();
                double dz = (nz + 0.5D) - job.centerZ();
                if (((dx * dx) + (dy * dy) + (dz * dz)) > job.radiusSq()) {
                    continue;
                }
                long neighborPosLong = BlockPos.asLong(nx, ny, nz);
                if (job.finalized().contains(neighborPosLong)) {
                    continue;
                }
                double candidate = solveKrakkStreamingCellFromAccepted(
                        level,
                        nx,
                        ny,
                        nz,
                        job.minX(),
                        job.maxX(),
                        job.minY(),
                        job.maxY(),
                        job.minZ(),
                        job.maxZ(),
                        false,
                        job.arrivalByPos(),
                        job.finalized(),
                        job.slownessByPos(),
                        null,
                        job.mutablePos(),
                        job.chunkStateCache(),
                        job.resistanceCostCache()
                );
                if (!Double.isFinite(candidate) || candidate > job.maxArrival()) {
                    continue;
                }
                if (candidate + 1.0E-9D < job.arrivalByPos().get(neighborPosLong)) {
                    job.arrivalByPos().put(neighborPosLong, (float) candidate);
                    job.trialQueue().add(neighborPosLong, candidate);
                }
            }
        }

        return new WavefrontSolveStats(acceptedNodes, poppedNodes);
    }

    private static StreamingImpactSample resolveNarrowBandImpactSample(NarrowBandWavefrontJob job,
                                                                       double arrival,
                                                                       float localSlowness,
                                                                       int x,
                                                                       int y,
                                                                       int z) {
        double airArrival = computeAnalyticKrakkAirArrival(job.centerX(), job.centerY(), job.centerZ(), x, y, z);
        if (!Double.isFinite(airArrival) || airArrival > job.resolvedRadius()) {
            return StreamingImpactSample.NONE;
        }
        double normalized = 1.0D - (airArrival / Math.max(1.0E-6D, job.resolvedRadius()));
        if (normalized <= 0.0D) {
            return StreamingImpactSample.NONE;
        }

        double expectedArrival = airArrival * Math.max(KRAKK_AIR_SLOWNESS, localSlowness);
        double resistanceOverrun = Math.max(0.0D, arrival - expectedArrival);
        double normalizedOverrun = resistanceOverrun / Math.max(1.0D, expectedArrival);
        if (normalizedOverrun >= KRAKK_NB_MAX_NORMALIZED_OVERRUN) {
            return StreamingImpactSample.NONE;
        }
        double transmittance = Math.exp(
                -(resistanceOverrun * KRAKK_NB_OVERRUN_ATTENUATION)
                        - (normalizedOverrun * KRAKK_NB_NORMALIZED_OVERRUN_ATTENUATION)
        );
        transmittance = Mth.clamp(transmittance, KRAKK_NB_MIN_TRANSMITTANCE, 1.0D);

        double cutoffRetention = computeKrakkCutoffEdgeRetention(normalized);
        double weight = Math.pow(Mth.clamp(normalized, 0.0D, 1.0D), KRAKK_WEIGHT_EXPONENT)
                * transmittance
                * cutoffRetention;
        if (weight <= 1.0E-8D) {
            return StreamingImpactSample.NONE;
        }

        double shellRadius = Math.max(1.0D, airArrival);
        double shellArea = Math.max(
                KRAKK_NB_MIN_SHELL_AREA,
                4.0D * Math.PI * shellRadius * shellRadius
        );
        double impactPower = (job.impactBudget * weight) / (shellArea * KRAKK_NB_SHELL_IMPACT_NORMALIZATION);
        if (!Double.isFinite(impactPower) || impactPower <= MIN_RESOLVED_RAY_IMPACT) {
            return StreamingImpactSample.NONE;
        }
        return new StreamingImpactSample(impactPower, weight);
    }

    private static void finishNarrowBandWavefrontJob(NarrowBandWavefrontJob job, String abortReason) {
        long impactApplyNanos = job.impactApplyStartNanos() > 0L
                ? (System.nanoTime() - job.impactApplyStartNanos())
                : 0L;
        if (job.trace() != null) {
            job.trace().volumetricImpactApplyNanos += impactApplyNanos;
        }
        if (job.phaseLoggingEnabled() && job.impactApplyStartNanos() > 0L) {
            logKrakkPhaseTiming(
                    job.phaseTraceId(),
                    "nb-impact-total",
                    impactApplyNanos,
                    String.format(
                            "jobId=%d directApplied=%d thermalApplied=%d",
                            job.jobId(),
                            job.directImpactApplications,
                            job.thermalImpactApplications
                    )
            );
        }
        if (job.phaseLoggingEnabled()) {
            String phase = abortReason == null ? "complete" : "abort";
            LOGGER.info(
                    "Krakk phase trace #{} [{}] narrow-band jobId={} reason={} totalMs={} blocksEvaluated={} broken={} damaged={} predictedBroken={} predictedDamaged={} heap={}",
                    job.phaseTraceId(),
                    phase,
                    job.jobId(),
                    abortReason == null ? "n/a" : abortReason,
                    nanosToMillis(System.nanoTime() - job.queuedAtNanos()),
                    job.trace() == null ? -1 : job.trace().blocksEvaluated,
                    job.trace() == null ? -1 : job.trace().brokenBlocks,
                    job.trace() == null ? -1 : job.trace().damagedBlocks,
                    job.trace() == null ? -1 : job.trace().predictedBrokenBlocks,
                    job.trace() == null ? -1 : job.trace().predictedDamagedBlocks,
                    formatKrakkHeapSnapshot()
            );
        }
        job.failureMessage(abortReason);
        job.setPhase(abortReason == null ? NarrowBandWavefrontPhase.COMPLETE : NarrowBandWavefrontPhase.FAILED);
    }


    private static double solveKrakkStreamingCellFromAccepted(ServerLevel level,
                                                              int x,
                                                              int y,
                                                              int z,
                                                              int minX,
                                                              int maxX,
                                                              int minY,
                                                              int maxY,
                                                              int minZ,
                                                              int maxZ,
                                                              boolean shadowSolve,
                                                              Long2FloatOpenHashMap arrivalByPos,
                                                              LongOpenHashSet accepted,
                                                              Long2FloatOpenHashMap normalSlownessByPos,
                                                              LongOpenHashSet sampledSolidPositions,
                                                              BlockPos.MutableBlockPos mutablePos,
                                                              WaveChunkStateCache chunkStateCache,
                                                              Int2DoubleOpenHashMap resistanceCostCache) {
        double a = minStreamingAcceptedAxis(
                arrivalByPos,
                accepted,
                x - 1,
                y,
                z,
                x + 1,
                y,
                z,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ
        );
        double b = minStreamingAcceptedAxis(
                arrivalByPos,
                accepted,
                x,
                y - 1,
                z,
                x,
                y + 1,
                z,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ
        );
        double c = minStreamingAcceptedAxis(
                arrivalByPos,
                accepted,
                x,
                y,
                z - 1,
                x,
                y,
                z + 1,
                minX,
                maxX,
                minY,
                maxY,
                minZ,
                maxZ
        );
        if (!Double.isFinite(a) && !Double.isFinite(b) && !Double.isFinite(c)) {
            return Double.POSITIVE_INFINITY;
        }
        long posLong = BlockPos.asLong(x, y, z);
        float normalSlowness = resolveStreamingNormalSlowness(
                level,
                x,
                y,
                z,
                posLong,
                mutablePos,
                chunkStateCache,
                resistanceCostCache,
                normalSlownessByPos,
                sampledSolidPositions
        );
        if (normalSlowness <= 0.0F) {
            return Double.POSITIVE_INFINITY;
        }
        double slowness = shadowSolve
                ? resolveStreamingShadowSlowness(normalSlowness)
                : normalSlowness;
        return solveKrakkDistance(a, b, c, slowness);
    }

    private static double minStreamingAcceptedAxis(Long2FloatOpenHashMap arrivalByPos,
                                                   LongOpenHashSet accepted,
                                                   int x1,
                                                   int y1,
                                                   int z1,
                                                   int x2,
                                                   int y2,
                                                   int z2,
                                                   int minX,
                                                   int maxX,
                                                   int minY,
                                                   int maxY,
                                                   int minZ,
                                                   int maxZ) {
        double min = Double.POSITIVE_INFINITY;
        if (x1 >= minX && x1 <= maxX && y1 >= minY && y1 <= maxY && z1 >= minZ && z1 <= maxZ) {
            long pos = BlockPos.asLong(x1, y1, z1);
            if (accepted.contains(pos)) {
                min = Math.min(min, arrivalByPos.get(pos));
            }
        }
        if (x2 >= minX && x2 <= maxX && y2 >= minY && y2 <= maxY && z2 >= minZ && z2 <= maxZ) {
            long pos = BlockPos.asLong(x2, y2, z2);
            if (accepted.contains(pos)) {
                min = Math.min(min, arrivalByPos.get(pos));
            }
        }
        return min;
    }

    private static boolean enqueueStreamingSource(long posLong,
                                                  double arrival,
                                                  Long2FloatOpenHashMap arrivalByPos,
                                                  WaveLongMinHeap queue) {
        if (!Double.isFinite(arrival)) {
            return false;
        }
        double existing = arrivalByPos.get(posLong);
        boolean isNew = !arrivalByPos.containsKey(posLong);
        if (arrival + 1.0E-9D < existing) {
            arrivalByPos.put(posLong, (float) arrival);
            queue.add(posLong, arrival);
            return isNew;
        }
        return false;
    }

    private static float resolveStreamingNormalSlowness(ServerLevel level,
                                                        int x,
                                                        int y,
                                                        int z,
                                                        long posLong,
                                                        BlockPos.MutableBlockPos mutablePos,
                                                        WaveChunkStateCache chunkStateCache,
                                                        Int2DoubleOpenHashMap resistanceCostCache,
                                                        Long2FloatOpenHashMap normalSlownessByPos,
                                                        LongOpenHashSet sampledSolidPositions) {
        float cached = normalSlownessByPos.get(posLong);
        if (Float.isFinite(cached)) {
            return cached;
        }
        BlockState blockState = chunkStateCache.getBlockState(level, mutablePos, x, y, z);
        float resolvedSlowness = (float) KRAKK_AIR_SLOWNESS;
        if (!blockState.isAir()) {
            double resistanceCost = getCachedResistanceCost(resistanceCostCache, blockState);
            double resistanceNoise = sampleBlockResistanceNoise(x, y, z) * KRAKK_BLOCK_RESISTANCE_NOISE_MAX;
            resolvedSlowness = (float) (KRAKK_AIR_SLOWNESS + ((resistanceCost + resistanceNoise) * KRAKK_SOLID_SLOWNESS_SCALE));
            if (sampledSolidPositions != null) {
                sampledSolidPositions.add(posLong);
            }
        }
        normalSlownessByPos.put(posLong, resolvedSlowness);
        return resolvedSlowness;
    }

    private static double sampleBlockResistanceNoise(int x, int y, int z) {
        long hash = 0x6C62272E07BB0142L;
        hash ^= ((long) x) * 0x9E3779B97F4A7C15L;
        hash ^= ((long) y) * 0xC2B2AE3D27D4EB4FL;
        hash ^= ((long) z) * 0x165667B19E3779F9L;
        hash ^= (hash >>> 33);
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= (hash >>> 33);
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= (hash >>> 33);
        return ((hash >>> 11) & 0x1FFFFFL) / 2097151.0D;
    }

    private static float resolveStreamingShadowSlowness(float normalSlowness) {
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        float overrun = Math.max(0.0F, normalSlowness - airSlowness);
        return (float) (airSlowness + (overrun * KRAKK_SHADOW_SOLID_SLOWNESS_SCALE));
    }

    private static void logKrakkPhaseTiming(long traceId, String phase, long elapsedNanos, String detail) {
        if (!krakkPhaseTimingLoggingEnabled) {
            return;
        }
        LOGGER.info(
                "Krakk phase trace #{} [{}] dtMs={} detail={} heap={}",
                traceId,
                phase,
                nanosToMillis(elapsedNanos),
                detail,
                formatKrakkHeapSnapshot()
        );
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0D;
    }

    private static String formatKrakkHeapSnapshot() {
        Runtime runtime = Runtime.getRuntime();
        long total = runtime.totalMemory();
        long free = runtime.freeMemory();
        long max = runtime.maxMemory();
        long used = Math.max(0L, total - free);
        return String.format(
                "usedMiB=%.1f totalMiB=%.1f maxMiB=%.1f",
                used / 1048576.0D,
                total / 1048576.0D,
                max / 1048576.0D
        );
    }

    private static boolean hasThermalEffect(double impactHeatCelsius) {
        return Math.abs(impactHeatCelsius - KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS) > 1.0E-6D;
    }

    private static ThermalTargetShape buildThermalTargetShape(LongArrayList targetPositions, FloatArrayList targetWeights) {
        Long2FloatOpenHashMap weightsByPos = new Long2FloatOpenHashMap(Math.max(16, targetPositions.size() * 2));
        weightsByPos.defaultReturnValue(0.0F);
        for (int i = 0; i < targetPositions.size(); i++) {
            long posLong = targetPositions.getLong(i);
            float weight = targetWeights.getFloat(i);
            if (weight <= 0.0F) {
                continue;
            }
            mergeThermalTargetWeight(weightsByPos, posLong, weight);
            for (Direction direction : THERMAL_EXPAND_DIRECTIONS) {
                long neighborPosLong = BlockPos.asLong(
                        BlockPos.getX(posLong) + (int) direction.x(),
                        BlockPos.getY(posLong) + (int) direction.y(),
                        BlockPos.getZ(posLong) + (int) direction.z()
                );
                mergeThermalTargetWeight(weightsByPos, neighborPosLong, weight * 0.75F);
            }
        }

        LongArrayList positions = new LongArrayList(weightsByPos.size());
        for (LongIterator iterator = weightsByPos.keySet().iterator(); iterator.hasNext(); ) {
            positions.add(iterator.nextLong());
        }
        return new ThermalTargetShape(positions, weightsByPos);
    }

    private static double computeHeatFalloff(double impactHeatCelsius, double targetWeight, double radius) {
        return impactHeatCelsius;
    }

    private static void mergeThermalTargetWeight(Long2FloatOpenHashMap weightsByPos, long posLong, float weight) {
        if (weight <= 0.0F) {
            return;
        }
        float existing = weightsByPos.get(posLong);
        if (weight > existing) {
            weightsByPos.put(posLong, weight);
        }
    }

    private static long mixOrderingSeed(double centerX, double centerY, double centerZ, double impactHeatCelsius, int size) {
        long seed = Double.doubleToLongBits(centerX * 31.0D);
        seed = (seed * 31L) ^ Double.doubleToLongBits(centerY * 17.0D);
        seed = (seed * 31L) ^ Double.doubleToLongBits(centerZ * 13.0D);
        seed = (seed * 31L) ^ Double.doubleToLongBits(impactHeatCelsius);
        seed = (seed * 31L) ^ size;
        return seed;
    }

    private static int computePermutationStart(long seed, int size) {
        if (size <= 1) {
            return 0;
        }
        int start = (int) Math.floorMod(seed, size);
        return Math.max(0, Math.min(size - 1, start));
    }

    private static int computePermutationStep(long seed, int size) {
        if (size <= 1) {
            return 1;
        }
        int step = (int) Math.floorMod(seed >>> 17, size);
        if (step <= 0) {
            step = 1;
        }
        while (greatestCommonDivisor(step, size) != 1) {
            step++;
            if (step >= size) {
                step = 1;
            }
        }
        return step;
    }

    private static int greatestCommonDivisor(int left, int right) {
        int a = Math.abs(left);
        int b = Math.abs(right);
        while (b != 0) {
            int temp = a % b;
            a = b;
            b = temp;
        }
        return Math.max(1, a);
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
                    createKrakkSlownessStorage(1),
                    EmptyLongIndexedAccess.INSTANCE,
                    new int[0]
            );
        }
        int volume = (int) volumeLong;
        MutableFloatIndexedAccess slowness = createKrakkSlownessStorage(Math.max(1, volume));
        BitSet activeMask = tracksActiveMaskInSlowness(slowness)
                ? ((SparseAirSolidSlownessStorage) slowness).activeMask()
                : new BitSet(volume);
        PackedKrakkSolidPositions solidPositions = new PackedKrakkSolidPositions(
                minX,
                minY,
                minZ,
                sizeY,
                sizeZ,
                estimateKrakkSolidPositionsCapacity(volume)
        );
        int rowCount = sizeX * sizeY;
        int[] activeCountByRow = new int[Math.max(1, rowCount)];
        int sampledVoxelCount;
        int resistanceTaskCount = resolveResistanceFieldTaskCount(sizeX, sizeY, sizeZ);
        if (requiresSerializedFloatAccess(slowness)) {
            resistanceTaskCount = 1;
        }
        boolean rebuildActiveMaskFromSlowness = false;
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
                    activeMask,
                    activeCountByRow,
                    resistanceCostCache
            );
            sampledVoxelCount = chunkResult.sampledVoxelCount();
            IntArrayList chunkPositions = chunkResult.solidIndices();
            for (int i = 0; i < chunkPositions.size(); i++) {
                solidPositions.addIndex(chunkPositions.getInt(i));
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
                                null,
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
                    IntArrayList chunkPositions = chunkResult.solidIndices();
                    for (int i = 0; i < chunkPositions.size(); i++) {
                        solidPositions.addIndex(chunkPositions.getInt(i));
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
                slowness.fill(0.0F);
                activeMask.clear();
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
                        activeMask,
                        activeCountByRow,
                        resistanceCostCache
                );
                sampledVoxelCount = fallback.sampledVoxelCount();
                IntArrayList chunkPositions = fallback.solidIndices();
                for (int i = 0; i < chunkPositions.size(); i++) {
                    solidPositions.addIndex(chunkPositions.getInt(i));
                }
            } else {
                sampledVoxelCount = sampledCountAccumulator;
                rebuildActiveMaskFromSlowness = true;
            }
        }

        if (rebuildActiveMaskFromSlowness) {
            int strideX = sizeY * sizeZ;
            int strideY = sizeZ;
            for (int xOffset = 0; xOffset < sizeX; xOffset++) {
                int baseX = xOffset * strideX;
                for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                    int base = baseX + (yOffset * strideY);
                    for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                        int index = base + zOffset;
                        if (slowness.getFloat(index) <= 0.0F) {
                            continue;
                        }
                        activeMask.set(index);
                    }
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
                activeCountByRow
        );
    }

    private static KrakkVolumetricBaselineResult buildKrakkVolumetricBaselineByPos(double centerX,
                                                                                        double centerY,
                                                                                        double centerZ,
                                                                                        double resolvedRadius,
                                                                                        KrakkField krakkField,
                                                                                        boolean collectMetrics) {
        LongIndexedAccess solidPositions = krakkField.solidPositions();
        int fieldMinX = krakkField.minX();
        int fieldMinY = krakkField.minY();
        int fieldMinZ = krakkField.minZ();
        int fieldSizeX = krakkField.sizeX();
        int fieldSizeY = krakkField.sizeY();
        int fieldSizeZ = krakkField.sizeZ();
        if (solidPositions.isEmpty()) {
            return new KrakkVolumetricBaselineResult(new float[0], 0L, 0L, 0L, 0L, 0L, 0, 0);
        }

        FloatIndexedAccess fieldSlowness = krakkField.slowness();
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
                    float sampleSlowness = fieldSlowness.getFloat(sampleIndex);
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
                collectMetrics
        );
        long targetScanNanos = collectMetrics ? (System.nanoTime() - targetScanStart) : 0L;

        LongArrayList targetPositions = targetScanResult.targetPositions();
        FloatArrayList targetWeights = targetScanResult.targetWeights();
        int totalFieldSize = fieldSizeX * fieldSizeY * fieldSizeZ;
        float[] baselineByIndex = new float[totalFieldSize];
        Arrays.fill(baselineByIndex, Float.NaN);
        for (int i = 0; i < targetPositions.size(); i++) {
            long posLong = targetPositions.getLong(i);
            int bx = BlockPos.getX(posLong);
            int by = BlockPos.getY(posLong);
            int bz = BlockPos.getZ(posLong);
            baselineByIndex[krakkGridIndex(bx - fieldMinX, by - fieldMinY, bz - fieldMinZ, fieldSizeY, fieldSizeZ)] =
                    targetWeights.getFloat(i);
        }
        float[] smoothedByIndex = KRAKK_ENABLE_VOLUMETRIC_BASELINE_SMOOTHING
                ? smoothKrakkVolumetricMechanics(baselineByIndex, fieldSizeX, fieldSizeY, fieldSizeZ,
                                                 solidPositions, fieldMinX, fieldMinY, fieldMinZ)
                : baselineByIndex;
        return new KrakkVolumetricBaselineResult(
                smoothedByIndex,
                directionSetupNanos,
                pressureSolveNanos,
                targetScanNanos,
                collectMetrics ? targetScanResult.precheckNanos() : 0L,
                collectMetrics ? targetScanResult.blendNanos() : 0L,
                collectMetrics ? directionCount : 0,
                collectMetrics ? radialSteps : 0
        );
    }

    private static float[] smoothKrakkVolumetricMechanics(float[] baselineByIndex,
                                                            int fieldSizeX,
                                                            int fieldSizeY,
                                                            int fieldSizeZ,
                                                            LongIndexedAccess solidPositions,
                                                            int fieldMinX,
                                                            int fieldMinY,
                                                            int fieldMinZ) {
        float[] smoothed = new float[baselineByIndex.length];
        Arrays.fill(smoothed, Float.NaN);
        int strideX = fieldSizeY * fieldSizeZ;
        int strideY = fieldSizeZ;
        double selfWeight = Mth.clamp(KRAKK_VOLUMETRIC_MECHANICS_SELF_SMOOTH, 0.0D, 1.0D);
        for (int i = 0; i < solidPositions.size(); i++) {
            long posLong = solidPositions.getLong(i);
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            int ix = x - fieldMinX;
            int iy = y - fieldMinY;
            int iz = z - fieldMinZ;
            int gridIdx = krakkGridIndex(ix, iy, iz, fieldSizeY, fieldSizeZ);
            float self = baselineByIndex[gridIdx];
            if (!Float.isFinite(self)) {
                continue;
            }
            double neighborSum = 0.0D;
            int neighborCount = 0;
            if (ix + 1 < fieldSizeX) {
                float v = baselineByIndex[gridIdx + strideX];
                if (Float.isFinite(v)) { neighborSum += v; neighborCount++; }
            }
            if (ix > 0) {
                float v = baselineByIndex[gridIdx - strideX];
                if (Float.isFinite(v)) { neighborSum += v; neighborCount++; }
            }
            if (iy + 1 < fieldSizeY) {
                float v = baselineByIndex[gridIdx + strideY];
                if (Float.isFinite(v)) { neighborSum += v; neighborCount++; }
            }
            if (iy > 0) {
                float v = baselineByIndex[gridIdx - strideY];
                if (Float.isFinite(v)) { neighborSum += v; neighborCount++; }
            }
            if (iz + 1 < fieldSizeZ) {
                float v = baselineByIndex[gridIdx + 1];
                if (Float.isFinite(v)) { neighborSum += v; neighborCount++; }
            }
            if (iz > 0) {
                float v = baselineByIndex[gridIdx - 1];
                if (Float.isFinite(v)) { neighborSum += v; neighborCount++; }
            }
            double neighborAverage = neighborCount > 0 ? (neighborSum / neighborCount) : self;
            smoothed[gridIdx] = (float) Mth.clamp((self * selfWeight) + (neighborAverage * (1.0D - selfWeight)), 0.0D, 1.0D);
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


    private static int[][] buildKrakkStreamingWaveNeighborOffsets() {
        int[][] offsets = new int[26][3];
        int cursor = 0;
        for (int dx = -1; dx <= 1; dx++) {
            for (int dy = -1; dy <= 1; dy++) {
                for (int dz = -1; dz <= 1; dz++) {
                    if (dx == 0 && dy == 0 && dz == 0) {
                        continue;
                    }
                    offsets[cursor][0] = dx;
                    offsets[cursor][1] = dy;
                    offsets[cursor][2] = dz;
                    cursor++;
                }
            }
        }
        return offsets;
    }


    private static KrakkSolveResult solveKrakkArrivalTimes(KrakkField field,
                                                               double centerX,
                                                               double centerY,
                                                               double centerZ,
                                                               float uniformSlownessOverride,
                                                               float solidSlownessScale) {
        MutableFloatIndexedAccess arrivalTimes = createKrakkArrivalStorage(field.slowness().size());
        arrivalTimes.fill(Float.POSITIVE_INFINITY);
        BitSet traversableMask = field.activeMask();
        FloatIndexedAccess resolvedSlowness = resolveKrakkSlownessField(field, uniformSlownessOverride, solidSlownessScale);
        SourceIndexSet sourceMask = createSourceIndexSet(arrivalTimes.size());
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
        if (field.slowness().size() > KRAKK_SHADOW_SOLVE_MAX_VOLUME) {
            KrakkSolveResult normalSolveResult = solveKrakkArrivalTimes(
                    field,
                    centerX,
                    centerY,
                    centerZ,
                    uniformSlownessOverride,
                    normalSolidSlownessScale
            );
            return new PairedKrakkSolveResult(normalSolveResult, normalSolveResult);
        }

        MutableFloatIndexedAccess normalArrivalTimes = createKrakkArrivalStorage(field.slowness().size());
        MutableFloatIndexedAccess shadowArrivalTimes = createKrakkArrivalStorage(field.slowness().size());
        normalArrivalTimes.fill(Float.POSITIVE_INFINITY);
        shadowArrivalTimes.fill(Float.POSITIVE_INFINITY);
        BitSet traversableMask = field.activeMask();
        PairedKrakkSlownessResult slownessResult = resolvePairedKrakkSlownessField(
                field,
                uniformSlownessOverride,
                normalSolidSlownessScale,
                shadowSolidSlownessScale
        );
        SourceIndexSet normalSourceMask = createSourceIndexSet(normalArrivalTimes.size());
        SourceIndexSet shadowSourceMask = createSourceIndexSet(shadowArrivalTimes.size());
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
        RuntimeExecutionPolicy executionPolicy = activeRuntimeExecutionPolicy();
        boolean deltaSteppingMode = executionPolicy.arrivalSolver() == KrakkArrivalSolver.DELTA_STEPPING;
        boolean allowConcurrentPairedSolve = KRAKK_SOLVE_POOL != null
                && !deltaSteppingMode
                && !requiresSerializedFloatAccess(normalArrivalTimes)
                && !requiresSerializedFloatAccess(shadowArrivalTimes)
                && !requiresSerializedFloatAccess(slownessResult.normalSlowness())
                && !requiresSerializedFloatAccess(slownessResult.shadowSlowness());
        if (allowConcurrentPairedSolve) {
            Future<KrakkSolveResult> shadowFuture = KRAKK_SOLVE_POOL.submit(
                    () -> callWithExecutionPolicy(
                            executionPolicy,
                            () -> solveKrakkArrivalTimesPreparedMultires(
                                    shadowArrivalTimes,
                                    slownessResult.shadowSlowness(),
                                    field,
                                    shadowSourceMask,
                                    sourceResult.shadowSourceCount(),
                                    true
                            )
                    )
            );
            normalSolveResult = callWithExecutionPolicy(
                    executionPolicy,
                    () -> solveKrakkArrivalTimesPreparedMultires(
                            normalArrivalTimes,
                            slownessResult.normalSlowness(),
                            field,
                            normalSourceMask,
                            sourceResult.normalSourceCount(),
                            true
                    )
            );
            try {
                shadowSolveResult = shadowFuture.get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                shadowFuture.cancel(true);
                shadowSolveResult = callWithExecutionPolicy(
                        executionPolicy,
                        () -> solveKrakkArrivalTimesPreparedMultires(
                                shadowArrivalTimes,
                                slownessResult.shadowSlowness(),
                                field,
                                shadowSourceMask,
                                sourceResult.shadowSourceCount(),
                                true
                        )
                );
            } catch (ExecutionException exception) {
                shadowFuture.cancel(true);
                shadowSolveResult = callWithExecutionPolicy(
                        executionPolicy,
                        () -> solveKrakkArrivalTimesPreparedMultires(
                                shadowArrivalTimes,
                                slownessResult.shadowSlowness(),
                                field,
                                shadowSourceMask,
                                sourceResult.shadowSourceCount(),
                                true
                        )
                );
            }
        } else {
            normalSolveResult = callWithExecutionPolicy(
                    executionPolicy,
                    () -> solveKrakkArrivalTimesPreparedMultires(
                            normalArrivalTimes,
                            slownessResult.normalSlowness(),
                            field,
                            normalSourceMask,
                            sourceResult.normalSourceCount(),
                            true
                    )
            );
            shadowSolveResult = callWithExecutionPolicy(
                    executionPolicy,
                    () -> solveKrakkArrivalTimesPreparedMultires(
                            shadowArrivalTimes,
                            slownessResult.shadowSlowness(),
                            field,
                            shadowSourceMask,
                            sourceResult.shadowSourceCount(),
                            true
                    )
            );
        }
        return new PairedKrakkSolveResult(normalSolveResult, shadowSolveResult);
    }

    private static KrakkSolveResult solveKrakkArrivalTimesPrepared(MutableFloatIndexedAccess arrivalTimes,
                                                                       FloatIndexedAccess resolvedSlowness,
                                                                       KrakkField field,
                                                                       SourceIndexSet sourceMask,
                                                                       int sourceCount,
                                                                       boolean allowParallelSweep) {
        if (sourceCount <= 0) {
            return new KrakkSolveResult(arrivalTimes, 0, 0);
        }
        RuntimeExecutionPolicy executionPolicy = activeRuntimeExecutionPolicy();
        if (executionPolicy.arrivalSolver() == KrakkArrivalSolver.DELTA_STEPPING) {
            KrakkSolveResult deltaSteppingResult = solveKrakkArrivalTimesDeltaStepping(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    sourceCount
            );
            int maxSweepCycles = Math.min(
                    KRAKK_DELTA_STEPPING_REFINE_SWEEP_CYCLES,
                    computeKrakkMaxSweepCycles(field)
            );
            int sweepCycles = refineKrakkArrivalTimes(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    allowParallelSweep,
                    maxSweepCycles
            );
            return new KrakkSolveResult(arrivalTimes, sourceCount, Math.max(deltaSteppingResult.sweepCycles(), sweepCycles));
        }
        if (executionPolicy.arrivalSolver() == KrakkArrivalSolver.ORDERED_UPWIND) {
            KrakkSolveResult orderedUpwindResult = solveKrakkArrivalTimesOrderedUpwind(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    sourceCount
            );
            int maxSweepCycles = computeKrakkMaxSweepCycles(field);
            int sweepCycles = refineKrakkArrivalTimes(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    allowParallelSweep,
                    maxSweepCycles
            );
            return new KrakkSolveResult(arrivalTimes, sourceCount, Math.max(orderedUpwindResult.sweepCycles(), sweepCycles));
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

    private static KrakkSolveResult solveKrakkArrivalTimesPrepared(MutableFloatIndexedAccess arrivalTimes,
                                                                       FloatIndexedAccess resolvedSlowness,
                                                                       KrakkField field,
                                                                       SourceIndexSet sourceMask,
                                                                       int sourceCount,
                                                                       boolean allowParallelSweep,
                                                                       int maxSweepCyclesOverride) {
        if (sourceCount <= 0) {
            return new KrakkSolveResult(arrivalTimes, 0, 0);
        }
        int maxSweepCycles = Math.min(computeKrakkMaxSweepCycles(field), maxSweepCyclesOverride);
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

    private static KrakkSolveResult solveKrakkArrivalTimesPreparedMultires(MutableFloatIndexedAccess arrivalTimes,
                                                                                FloatIndexedAccess resolvedSlowness,
                                                                                KrakkField field,
                                                                                SourceIndexSet sourceMask,
                                                                                int sourceCount,
                                                                                boolean allowParallelSweep) {
        RuntimeExecutionPolicy executionPolicy = activeRuntimeExecutionPolicy();
        if (!KRAKK_USE_MULTIRES_COARSE_SOLVE
                || sourceCount <= 0) {
            return solveKrakkArrivalTimesPrepared(
                    arrivalTimes,
                    resolvedSlowness,
                    field,
                    sourceMask,
                    sourceCount,
                    allowParallelSweep
            );
        }
        long fieldVolume = (long) field.sizeX() * (long) field.sizeY() * (long) field.sizeZ();
        if (fieldVolume > executionPolicy.maxMultiresVolume()) {
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
                allowParallelSweep,
                KRAKK_MULTIRES_COARSE_MAX_SWEEP_CYCLES
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

    private static KrakkSolveResult solveKrakkArrivalTimesOrderedUpwind(MutableFloatIndexedAccess arrivalTimes,
                                                                            FloatIndexedAccess resolvedSlowness,
                                                                            KrakkField field,
                                                                            SourceIndexSet sourceMask,
                                                                            int sourceCount) {
        if (sourceCount <= 0) {
            return new KrakkSolveResult(arrivalTimes, 0, 0);
        }

        BitSet activeMask = field.activeMask();
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        int sizeZ = field.sizeZ();
        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
        BitSet accepted = new BitSet(Math.max(1, arrivalTimes.size()));
        KrakkMinHeap heap = new KrakkMinHeap(Math.max(32, sourceCount * 4));

        sourceMask.forEach(index -> {
            if (index < 0 || index >= arrivalTimes.size()) {
                return;
            }
            if (!activeMask.get(index) || resolvedSlowness.getFloat(index) <= 0.0F) {
                return;
            }
            double seedArrival = arrivalTimes.getFloat(index);
            if (Double.isFinite(seedArrival)) {
                heap.add(index, seedArrival);
            }
        });
        if (heap.isEmpty()) {
            return new KrakkSolveResult(arrivalTimes, sourceCount, 0);
        }

        int acceptedCount = 0;
        while (!heap.isEmpty()) {
            int index = heap.pollIndex();
            double queuedArrival = heap.pollPriority();
            double currentArrival = arrivalTimes.getFloat(index);
            if (!Double.isFinite(currentArrival) || queuedArrival > currentArrival + 1.0E-9D) {
                continue;
            }
            if (accepted.get(index)) {
                continue;
            }
            accepted.set(index);
            acceptedCount++;

            int x = index / strideX;
            int yz = index - (x * strideX);
            int y = yz / strideY;
            int z = yz - (y * strideY);

            if (x > 0) {
                updateKrakkNeighborFromAccepted(
                        arrivalTimes,
                        resolvedSlowness,
                        accepted,
                        heap,
                        index - strideX,
                        field
                );
            }
            if (x + 1 < sizeX) {
                updateKrakkNeighborFromAccepted(
                        arrivalTimes,
                        resolvedSlowness,
                        accepted,
                        heap,
                        index + strideX,
                        field
                );
            }
            if (y > 0) {
                updateKrakkNeighborFromAccepted(
                        arrivalTimes,
                        resolvedSlowness,
                        accepted,
                        heap,
                        index - strideY,
                        field
                );
            }
            if (y + 1 < sizeY) {
                updateKrakkNeighborFromAccepted(
                        arrivalTimes,
                        resolvedSlowness,
                        accepted,
                        heap,
                        index + strideY,
                        field
                );
            }
            if (z > 0) {
                updateKrakkNeighborFromAccepted(
                        arrivalTimes,
                        resolvedSlowness,
                        accepted,
                        heap,
                        index - 1,
                        field
                );
            }
            if (z + 1 < sizeZ) {
                updateKrakkNeighborFromAccepted(
                        arrivalTimes,
                        resolvedSlowness,
                        accepted,
                        heap,
                        index + 1,
                        field
                );
            }
        }

        return new KrakkSolveResult(arrivalTimes, sourceCount, acceptedCount > 0 ? 1 : 0);
    }

    private static KrakkSolveResult solveKrakkArrivalTimesDeltaStepping(MutableFloatIndexedAccess arrivalTimes,
                                                                            FloatIndexedAccess resolvedSlowness,
                                                                            KrakkField field,
                                                                            SourceIndexSet sourceMask,
                                                                            int sourceCount) {
        if (sourceCount <= 0) {
            return new KrakkSolveResult(arrivalTimes, 0, 0);
        }
        BitSet activeMask = field.activeMask();
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        int sizeZ = field.sizeZ();
        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
        double bucketWidth = Math.max(1.0E-4D, KRAKK_DELTA_STEPPING_BUCKET_WIDTH);
        KrakkDeltaBucketQueue bucketQueue = new KrakkDeltaBucketQueue(bucketWidth, arrivalTimes.size());
        BitSet accepted = new BitSet(Math.max(1, arrivalTimes.size()));

        sourceMask.forEach(index -> {
            if (index < 0 || index >= arrivalTimes.size()) {
                return;
            }
            if (!activeMask.get(index) || resolvedSlowness.getFloat(index) <= 0.0F) {
                return;
            }
            double seedArrival = arrivalTimes.getFloat(index);
            if (Double.isFinite(seedArrival)) {
                bucketQueue.add(index, seedArrival);
            }
        });

        if (bucketQueue.isEmpty()) {
            return new KrakkSolveResult(arrivalTimes, sourceCount, 0);
        }

        int bucketPasses = 0;
        int acceptedCount = 0;
        while (!bucketQueue.isEmpty()) {
            int bucketIndex = bucketQueue.pollBucketIndex();
            if (bucketIndex < 0) {
                break;
            }
            IntArrayList bucketEntries = bucketQueue.takeBucket(bucketIndex);
            if (bucketEntries == null || bucketEntries.isEmpty()) {
                continue;
            }
            bucketPasses++;
            if (krakkPhaseTimingLoggingEnabled
                    && (bucketPasses % KRAKK_DELTA_STEPPING_PROGRESS_LOG_INTERVAL_BUCKETS) == 0) {
                LOGGER.info(
                        "Krakk delta-stepping progress: bucketPasses={} accepted={} activeFrontierBuckets={} arrivalSize={} fieldDims=[{},{},{}] heap={}",
                        bucketPasses,
                        acceptedCount,
                        bucketQueue.bucketCount(),
                        arrivalTimes.size(),
                        sizeX,
                        sizeY,
                        sizeZ,
                        formatKrakkHeapSnapshot()
                );
            }
            for (int i = 0; i < bucketEntries.size(); i++) {
                int index = bucketEntries.getInt(i);
                if (!bucketQueue.claim(index)) {
                    continue;
                }
                if (index < 0 || index >= arrivalTimes.size()) {
                    continue;
                }
                if (!activeMask.get(index) || accepted.get(index)) {
                    continue;
                }
                double currentArrival = arrivalTimes.getFloat(index);
                if (!Double.isFinite(currentArrival)) {
                    continue;
                }
                int expectedBucket = krakkDeltaBucketIndex(currentArrival, bucketWidth);
                if (expectedBucket != bucketIndex) {
                    bucketQueue.add(index, currentArrival);
                    continue;
                }
                accepted.set(index);
                acceptedCount++;
                int x = index / strideX;
                int yz = index - (x * strideX);
                int y = yz / strideY;
                int z = yz - (y * strideY);

                if (x > 0) {
                    updateKrakkNeighborFromAcceptedDelta(
                            arrivalTimes,
                            resolvedSlowness,
                            accepted,
                            bucketQueue,
                            index - strideX,
                            field
                    );
                }
                if (x + 1 < sizeX) {
                    updateKrakkNeighborFromAcceptedDelta(
                            arrivalTimes,
                            resolvedSlowness,
                            accepted,
                            bucketQueue,
                            index + strideX,
                            field
                    );
                }
                if (y > 0) {
                    updateKrakkNeighborFromAcceptedDelta(
                            arrivalTimes,
                            resolvedSlowness,
                            accepted,
                            bucketQueue,
                            index - strideY,
                            field
                    );
                }
                if (y + 1 < sizeY) {
                    updateKrakkNeighborFromAcceptedDelta(
                            arrivalTimes,
                            resolvedSlowness,
                            accepted,
                            bucketQueue,
                            index + strideY,
                            field
                    );
                }
                if (z > 0) {
                    updateKrakkNeighborFromAcceptedDelta(
                            arrivalTimes,
                            resolvedSlowness,
                            accepted,
                            bucketQueue,
                            index - 1,
                            field
                    );
                }
                if (z + 1 < sizeZ) {
                    updateKrakkNeighborFromAcceptedDelta(
                            arrivalTimes,
                            resolvedSlowness,
                            accepted,
                            bucketQueue,
                            index + 1,
                            field
                    );
                }
            }
        }

        return new KrakkSolveResult(arrivalTimes, sourceCount, bucketPasses);
    }

    private static int refineKrakkArrivalTimes(MutableFloatIndexedAccess arrivalTimes,
                                                 FloatIndexedAccess resolvedSlowness,
                                                 KrakkField field,
                                                 SourceIndexSet sourceMask,
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
                if (dirtyRows.isEmpty()) {
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

    private static BitSet buildKrakkSourceRowMask(KrakkField field, SourceIndexSet sourceMask) {
        int rowCount = Math.max(1, field.sizeX() * field.sizeY());
        BitSet sourceRows = new BitSet(rowCount);
        int strideX = field.sizeY() * field.sizeZ();
        int strideY = field.sizeZ();
        sourceMask.forEach(index -> {
            int x = index / strideX;
            int yz = index - (x * strideX);
            int y = yz / strideY;
            sourceRows.set(krakkRowIndex(x, y, field.sizeY()));
        });
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
                                                                             FloatIndexedAccess fineSlowness,
                                                                             SourceIndexSet fineSourceMask,
                                                                             FloatIndexedAccess fineArrivalTimes,
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
        MutableFloatIndexedAccess coarseSlowness = createKrakkFloatStorage(Math.max(1, coarseVolume));
        MutableFloatIndexedAccess coarseArrivalTimes = createKrakkArrivalStorage(Math.max(1, coarseVolume));
        coarseArrivalTimes.fill(Float.POSITIVE_INFINITY);
        SourceIndexSet coarseSourceMask = createSourceIndexSet(coarseArrivalTimes.size());
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
                                float slowness = fineSlowness.getFloat(fineIndex);
                                if (slowness <= 0.0F) {
                                    continue;
                                }
                                active = true;
                                minSlowness = Math.min(minSlowness, slowness);
                                hasSolid |= slowness > airSlowness + 1.0E-6F;
                                if (fineSourceMask.contains(fineIndex)) {
                                    hasSource = true;
                                    minSourceArrival = Math.min(minSourceArrival, fineArrivalTimes.getFloat(fineIndex));
                                }
                            }
                        }
                    }

                    if (!active || !Float.isFinite(minSlowness)) {
                        continue;
                    }

                    int coarseIndex = krakkGridIndex(coarseX, coarseY, coarseZ, coarseSizeY, coarseSizeZ);
                    coarseSlowness.setFloat(coarseIndex, minSlowness);
                    if (hasSolid) {
                        coarseSolidPositions.add(BlockPos.asLong(
                                fineField.minX() + fineStartX,
                                fineField.minY() + fineStartY,
                                fineField.minZ() + fineStartZ
                        ));
                    }
                    if (hasSource && Float.isFinite(minSourceArrival)) {
                        if (coarseSourceMask.add(coarseIndex)) {
                            coarseSourceCount++;
                        }
                        coarseArrivalTimes.setFloat(coarseIndex, minSourceArrival);
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
                wrapLongArrayList(coarseSolidPositions)
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
                                                                       FloatIndexedAccess slowness,
                                                                       LongIndexedAccess solidPositions) {
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
                    if (slowness.getFloat(index) <= 0.0F) {
                        continue;
                    }
                    activeMask.set(index);
                    activeCount++;
                }
                activeCountByRow[rowIndex] = activeCount;
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
                activeCountByRow
        );
    }

    private static void seedFineArrivalTimesFromCoarse(MutableFloatIndexedAccess fineArrivalTimes,
                                                       FloatIndexedAccess fineSlowness,
                                                       KrakkField fineField,
                                                       SourceIndexSet fineSourceMask,
                                                       KrakkCoarseSolveContext coarseContext) {
        KrakkField coarseField = coarseContext.coarseField();
        FloatIndexedAccess coarseArrivalTimes = coarseContext.coarseArrivalTimes();
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
        BitSet activeMask = fineField.activeMask();
        for (int fineIndex = activeMask.nextSetBit(0); fineIndex >= 0; fineIndex = activeMask.nextSetBit(fineIndex + 1)) {
            if (fineSourceMask.contains(fineIndex) || fineSlowness.getFloat(fineIndex) <= 0.0F) {
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
            float coarseArrival = coarseArrivalTimes.getFloat(coarseIndex);
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
            float seededArrival = (float) (coarseArrival + (localDistance * Math.max(1.0E-6F, fineSlowness.getFloat(fineIndex))));
            if (seededArrival < fineArrivalTimes.getFloat(fineIndex)) {
                fineArrivalTimes.setFloat(fineIndex, seededArrival);
            }
        }
    }

    private static FloatIndexedAccess resolveKrakkSlownessField(KrakkField field,
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
        FloatIndexedAccess baseSlowness = field.slowness();
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

        if (uniformOverrideActive) {
            MutableFloatIndexedAccess resolvedSlowness = createKrakkFloatStorage(baseSlowness.size());
            BitSet activeMask = field.activeMask();
            for (int index = activeMask.nextSetBit(0); index >= 0; index = activeMask.nextSetBit(index + 1)) {
                if (baseSlowness.getFloat(index) > 0.0F) {
                    resolvedSlowness.setFloat(index, uniformSlownessOverride);
                }
            }
            return new PairedKrakkSlownessResult(resolvedSlowness, resolvedSlowness);
        }

        boolean normalIsBase = Math.abs(resolvedNormalScale - 1.0F) <= 1.0E-6F;
        boolean shadowIsBase = Math.abs(resolvedShadowScale - 1.0F) <= 1.0E-6F;
        if (!normalIsBase && !shadowIsBase && Math.abs(resolvedNormalScale - resolvedShadowScale) <= 1.0E-6F) {
            MutableFloatIndexedAccess resolvedSlowness = createKrakkFloatStorage(baseSlowness.size());
            applyKrakkSlownessScale(resolvedSlowness, baseSlowness, field.activeMask(), resolvedNormalScale);
            return new PairedKrakkSlownessResult(resolvedSlowness, resolvedSlowness);
        }

        MutableFloatIndexedAccess normalScaledSlowness = normalIsBase ? null : createKrakkFloatStorage(baseSlowness.size());
        MutableFloatIndexedAccess shadowScaledSlowness = shadowIsBase ? null : createKrakkFloatStorage(baseSlowness.size());
        if (normalScaledSlowness != null) {
            applyKrakkSlownessScale(normalScaledSlowness, baseSlowness, field.activeMask(), resolvedNormalScale);
        }
        if (shadowScaledSlowness != null) {
            applyKrakkSlownessScale(shadowScaledSlowness, baseSlowness, field.activeMask(), resolvedShadowScale);
        }
        FloatIndexedAccess normalSlowness = normalIsBase ? baseSlowness : normalScaledSlowness;
        FloatIndexedAccess shadowSlowness = shadowIsBase ? baseSlowness : shadowScaledSlowness;
        return new PairedKrakkSlownessResult(normalSlowness, shadowSlowness);
    }

    private static void applyKrakkSlownessScale(MutableFloatIndexedAccess outputSlowness,
                                                  FloatIndexedAccess baseSlowness,
                                                  BitSet activeMask,
                                                  float scale) {
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        for (int index = activeMask.nextSetBit(0); index >= 0; index = activeMask.nextSetBit(index + 1)) {
            float base = baseSlowness.getFloat(index);
            if (base <= 0.0F) {
                continue;
            }
            float solidOverrun = Math.max(0.0F, base - airSlowness);
            if (solidOverrun <= 1.0E-6F) {
                outputSlowness.setFloat(index, base);
                continue;
            }
            outputSlowness.setFloat(index, airSlowness + (solidOverrun * scale));
        }
    }

    private static int initializeKrakkSources(MutableFloatIndexedAccess arrivalTimes,
                                                SourceIndexSet sourceMask,
                                                KrakkField field,
                                                BitSet traversableMask,
                                                FloatIndexedAccess resolvedSlowness,
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

    private static PairedKrakkSourceResult initializePairedKrakkSources(MutableFloatIndexedAccess normalArrivalTimes,
                                                                             SourceIndexSet normalSourceMask,
                                                                             FloatIndexedAccess normalSlowness,
                                                                             MutableFloatIndexedAccess shadowArrivalTimes,
                                                                             SourceIndexSet shadowSourceMask,
                                                                             FloatIndexedAccess shadowSlowness,
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

                    float normalCellSlowness = normalSlowness.getFloat(index);
                    if (normalCellSlowness > 0.0F) {
                        double normalArrival = distance * normalCellSlowness;
                        if (normalArrival + 1.0E-6D < normalArrivalTimes.getFloat(index)) {
                            if (normalSourceMask.add(index)) {
                                normalSourceCount++;
                            }
                            normalArrivalTimes.setFloat(index, (float) normalArrival);
                        }
                    }

                    if (computeShadow) {
                        float shadowCellSlowness = shadowSlowness.getFloat(index);
                        if (shadowCellSlowness <= 0.0F) {
                            continue;
                        }
                        double shadowArrival = distance * shadowCellSlowness;
                        if (shadowArrival + 1.0E-6D < shadowArrivalTimes.getFloat(index)) {
                            if (shadowSourceMask.add(index)) {
                                shadowSourceCount++;
                            }
                            shadowArrivalTimes.setFloat(index, (float) shadowArrival);
                        }
                    }
                }
            }
        }

        int seedIndex = -1;
        if (normalSourceCount <= 0 || (computeShadow && shadowSourceCount <= 0)) {
            seedIndex = resolveKrakkSeedIndex(field, traversableMask, centerX, centerY, centerZ);
        }
        if (normalSourceCount <= 0 && seedIndex >= 0 && normalSlowness.getFloat(seedIndex) > 0.0F) {
            normalArrivalTimes.setFloat(seedIndex, 0.0F);
            if (normalSourceMask.add(seedIndex)) {
                normalSourceCount++;
            }
        }
        if (computeShadow && shadowSourceCount <= 0 && seedIndex >= 0 && shadowSlowness.getFloat(seedIndex) > 0.0F) {
            shadowArrivalTimes.setFloat(seedIndex, 0.0F);
            if (shadowSourceMask.add(seedIndex)) {
                shadowSourceCount++;
            }
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

    private static double sweepKrakkPass(MutableFloatIndexedAccess arrivalTimes,
                                           FloatIndexedAccess resolvedSlowness,
                                           KrakkField field,
                                           SourceIndexSet sourceMask,
                                           boolean allowParallelSweep,
                                           boolean xForward,
                                           boolean yForward,
                                           boolean zForward,
                                           BitSet activeRowsMask,
                                           BitSet dirtyRowsOut) {
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        int diagonalCount = sizeX + sizeY - 1;
        int[] activeRowLengths = field.activeRowLengths();
        long[] activeMaskWords = field.activeMask().toLongArray();
        int activeMaskWordCount = activeMaskWords.length;
        long[] activeRowsMaskWords = activeRowsMask != null ? activeRowsMask.toLongArray() : null;
        int activeRowsMaskWordCount = activeRowsMaskWords != null ? activeRowsMaskWords.length : 0;
        double maxDelta = 0.0D;
        boolean parallelSweepAllowed = allowParallelSweep
                && !requiresSerializedFloatAccess(arrivalTimes)
                && !requiresSerializedFloatAccess(resolvedSlowness);
        for (int diagonal = 0; diagonal < diagonalCount; diagonal++) {
            int minXOrder = Math.max(0, diagonal - (sizeY - 1));
            int maxXOrder = Math.min(sizeX - 1, diagonal);
            int rowSpan = maxXOrder - minXOrder + 1;
            int taskCount = resolveKrakkSweepTaskCount(rowSpan);
            if (!parallelSweepAllowed || taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
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
                        activeRowLengths,
                        activeMaskWords,
                        activeMaskWordCount,
                        activeRowsMaskWords,
                        activeRowsMaskWordCount
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
                                activeRowLengths,
                                activeMaskWords,
                                activeMaskWordCount,
                                activeRowsMaskWords,
                                activeRowsMaskWordCount
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

    private static KrakkSweepChunkResult sweepKrakkDiagonalChunk(MutableFloatIndexedAccess arrivalTimes,
                                                                     FloatIndexedAccess resolvedSlowness,
                                                                     KrakkField field,
                                                                     SourceIndexSet sourceMask,
                                                                     boolean xForward,
                                                                     boolean yForward,
                                                                     boolean zForward,
                                                                     int diagonal,
                                                                     int startXOrderInclusive,
                                                                     int endXOrderExclusive,
                                                                     int[] activeRowLengths,
                                                                     long[] activeMaskWords,
                                                                     int activeMaskWordCount,
                                                                     long[] activeRowsMaskWords,
                                                                     int activeRowsMaskWordCount) {
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        int sizeZ = field.sizeZ();
        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
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
            if (activeRowsMaskWords != null) {
                int rowWordIdx = rowIndex >> 6;
                if (rowWordIdx >= activeRowsMaskWordCount
                        || (activeRowsMaskWords[rowWordIdx] & (1L << (rowIndex & 63))) == 0L) {
                    continue;
                }
            }
            int rowLength = activeRowLengths[rowIndex];
            if (rowLength <= 0) {
                continue;
            }
            int baseIndex = (x * strideX) + (y * strideY);
            boolean rowChanged = false;
            if (zForward) {
                for (int z = 0; z < sizeZ; z++) {
                    int index = baseIndex + z;
                    int wordIdx = index >> 6;
                    if (wordIdx >= activeMaskWordCount
                            || (activeMaskWords[wordIdx] & (1L << (index & 63))) == 0L
                            || sourceMask.contains(index)) {
                        continue;
                    }
                    double candidate = solveKrakkCell(
                            arrivalTimes,
                            resolvedSlowness,
                            activeMaskWords,
                            activeMaskWordCount,
                            index, x, y, z,
                            sizeX, sizeY, sizeZ,
                            strideX, strideY
                    );
                    if (!Double.isFinite(candidate)) {
                        continue;
                    }
                    double previous = arrivalTimes.getFloat(index);
                    if (candidate + 1.0E-9D < previous) {
                        arrivalTimes.setFloat(index, (float) candidate);
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
            for (int z = sizeZ - 1; z >= 0; z--) {
                int index = baseIndex + z;
                int wordIdx = index >> 6;
                if (wordIdx >= activeMaskWordCount
                        || (activeMaskWords[wordIdx] & (1L << (index & 63))) == 0L
                        || sourceMask.contains(index)) {
                    continue;
                }
                double candidate = solveKrakkCell(
                        arrivalTimes,
                        resolvedSlowness,
                        activeMaskWords,
                        activeMaskWordCount,
                        index, x, y, z,
                        sizeX, sizeY, sizeZ,
                        strideX, strideY
                );
                if (!Double.isFinite(candidate)) {
                    continue;
                }
                double previous = arrivalTimes.getFloat(index);
                if (candidate + 1.0E-9D < previous) {
                    arrivalTimes.setFloat(index, (float) candidate);
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

    private static double solveKrakkCell(FloatIndexedAccess arrivalTimes,
                                           FloatIndexedAccess resolvedSlowness,
                                           long[] activeMaskWords,
                                           int activeMaskWordCount,
                                           int index, int x, int y, int z,
                                           int sizeX, int sizeY, int sizeZ,
                                           int strideX, int strideY) {
        double a = Double.POSITIVE_INFINITY;
        if (x > 0) {
            int negX = index - strideX;
            int negXWord = negX >> 6;
            if (negXWord < activeMaskWordCount && (activeMaskWords[negXWord] & (1L << (negX & 63))) != 0L) {
                a = Math.min(a, arrivalTimes.getFloat(negX));
            }
        }
        if (x + 1 < sizeX) {
            int posX = index + strideX;
            int posXWord = posX >> 6;
            if (posXWord < activeMaskWordCount && (activeMaskWords[posXWord] & (1L << (posX & 63))) != 0L) {
                a = Math.min(a, arrivalTimes.getFloat(posX));
            }
        }

        double b = Double.POSITIVE_INFINITY;
        if (y > 0) {
            int negY = index - strideY;
            int negYWord = negY >> 6;
            if (negYWord < activeMaskWordCount && (activeMaskWords[negYWord] & (1L << (negY & 63))) != 0L) {
                b = Math.min(b, arrivalTimes.getFloat(negY));
            }
        }
        if (y + 1 < sizeY) {
            int posY = index + strideY;
            int posYWord = posY >> 6;
            if (posYWord < activeMaskWordCount && (activeMaskWords[posYWord] & (1L << (posY & 63))) != 0L) {
                b = Math.min(b, arrivalTimes.getFloat(posY));
            }
        }

        double c = Double.POSITIVE_INFINITY;
        if (z > 0) {
            int negZ = index - 1;
            int negZWord = negZ >> 6;
            if (negZWord < activeMaskWordCount && (activeMaskWords[negZWord] & (1L << (negZ & 63))) != 0L) {
                c = Math.min(c, arrivalTimes.getFloat(negZ));
            }
        }
        if (z + 1 < sizeZ) {
            int posZ = index + 1;
            int posZWord = posZ >> 6;
            if (posZWord < activeMaskWordCount && (activeMaskWords[posZWord] & (1L << (posZ & 63))) != 0L) {
                c = Math.min(c, arrivalTimes.getFloat(posZ));
            }
        }

        if (!Double.isFinite(a) && !Double.isFinite(b) && !Double.isFinite(c)) {
            return Double.POSITIVE_INFINITY;
        }
        return solveKrakkDistance(a, b, c, resolvedSlowness.getFloat(index));
    }

    private static double solveKrakkCellFromAccepted(FloatIndexedAccess arrivalTimes,
                                                       FloatIndexedAccess resolvedSlowness,
                                                       BitSet accepted,
                                                       int index,
                                                       KrakkField field) {
        BitSet activeMask = field.activeMask();
        int sizeX = field.sizeX();
        int sizeY = field.sizeY();
        int sizeZ = field.sizeZ();
        int strideX = sizeY * sizeZ;
        int strideY = sizeZ;
        int x = index / strideX;
        int yz = index - (x * strideX);
        int y = yz / strideY;
        int z = yz - (y * strideY);
        int negX = x > 0 && activeMask.get(index - strideX) ? index - strideX : -1;
        int posX = x + 1 < sizeX && activeMask.get(index + strideX) ? index + strideX : -1;
        int negY = y > 0 && activeMask.get(index - strideY) ? index - strideY : -1;
        int posY = y + 1 < sizeY && activeMask.get(index + strideY) ? index + strideY : -1;
        int negZ = z > 0 && activeMask.get(index - 1) ? index - 1 : -1;
        int posZ = z + 1 < sizeZ && activeMask.get(index + 1) ? index + 1 : -1;

        double a = minAcceptedKrakkAxisNeighbor(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                negX,
                posX
        );
        double b = minAcceptedKrakkAxisNeighbor(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                negY,
                posY
        );
        double c = minAcceptedKrakkAxisNeighbor(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                negZ,
                posZ
        );
        if (!Double.isFinite(a) && !Double.isFinite(b) && !Double.isFinite(c)) {
            return Double.POSITIVE_INFINITY;
        }
        return solveKrakkDistance(a, b, c, resolvedSlowness.getFloat(index));
    }

    private static void updateKrakkNeighborFromAccepted(MutableFloatIndexedAccess arrivalTimes,
                                                          FloatIndexedAccess resolvedSlowness,
                                                          BitSet accepted,
                                                          KrakkMinHeap heap,
                                                          int neighborIndex,
                                                          KrakkField field) {
        if (neighborIndex < 0 || accepted.get(neighborIndex) || resolvedSlowness.getFloat(neighborIndex) <= 0.0F) {
            return;
        }
        double candidate = solveKrakkCellFromAccepted(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                neighborIndex,
                field
        );
        if (!Double.isFinite(candidate)) {
            return;
        }
        if (candidate + 1.0E-9D < arrivalTimes.getFloat(neighborIndex)) {
            arrivalTimes.setFloat(neighborIndex, (float) candidate);
            heap.add(neighborIndex, candidate);
        }
    }

    private static void updateKrakkNeighborFromAcceptedDelta(MutableFloatIndexedAccess arrivalTimes,
                                                               FloatIndexedAccess resolvedSlowness,
                                                               BitSet accepted,
                                                               KrakkDeltaBucketQueue bucketQueue,
                                                               int neighborIndex,
                                                               KrakkField field) {
        if (neighborIndex < 0 || accepted.get(neighborIndex) || resolvedSlowness.getFloat(neighborIndex) <= 0.0F) {
            return;
        }
        double candidate = solveKrakkCellFromAccepted(
                arrivalTimes,
                resolvedSlowness,
                accepted,
                neighborIndex,
                field
        );
        if (!Double.isFinite(candidate)) {
            return;
        }
        if (candidate + 1.0E-9D < arrivalTimes.getFloat(neighborIndex)) {
            arrivalTimes.setFloat(neighborIndex, (float) candidate);
            bucketQueue.add(neighborIndex, candidate);
        }
    }

    private static double minAcceptedKrakkAxisNeighbor(FloatIndexedAccess arrivalTimes,
                                                         FloatIndexedAccess resolvedSlowness,
                                                         BitSet accepted,
                                                         int negativeIndex,
                                                         int positiveIndex) {
        double min = Double.POSITIVE_INFINITY;
        if (negativeIndex >= 0 && accepted.get(negativeIndex) && resolvedSlowness.getFloat(negativeIndex) > 0.0F) {
            min = Math.min(min, arrivalTimes.getFloat(negativeIndex));
        }
        if (positiveIndex >= 0 && accepted.get(positiveIndex) && resolvedSlowness.getFloat(positiveIndex) > 0.0F) {
            min = Math.min(min, arrivalTimes.getFloat(positiveIndex));
        }
        return min;
    }

    private static int krakkDeltaBucketIndex(double arrival, double bucketWidth) {
        if (!Double.isFinite(arrival) || arrival <= 0.0D) {
            return 0;
        }
        double scaled = arrival / Math.max(1.0E-9D, bucketWidth);
        if (scaled >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) Math.floor(scaled);
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
        double sSq = s * s;
        if (!Double.isFinite(t1)) {
            return t0 + s;
        }
        double result = t0 + s;
        if (result > t1) {
            double sq01 = square(t0 - t1);
            double discriminant2 = (2.0D * sSq) - sq01;
            if (discriminant2 > 0.0D) {
                result = (t0 + t1 + Math.sqrt(discriminant2)) * 0.5D;
            } else {
                result = t1 + s;
            }
            if (Double.isFinite(t2) && result > t2) {
                double discriminant3 = (3.0D * sSq)
                        - sq01
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
                                                    LongIndexedAccess candidateSolidPositions,
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
                    KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS,
                    false,
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
                                                                             MutableFloatIndexedAccess slowness,
                                                                             BitSet activeMask,
                                                                             int[] activeCountByRow,
                                                                             Int2DoubleOpenHashMap sharedResistanceCostCache) {
        Int2DoubleOpenHashMap localResistanceCache = sharedResistanceCostCache != null
                ? sharedResistanceCostCache
                : new Int2DoubleOpenHashMap(128);
        if (sharedResistanceCostCache == null) {
            localResistanceCache.defaultReturnValue(Double.NaN);
        }
        IntArrayList localSolidIndices = new IntArrayList(
                estimateKrakkChunkSolidPositionsCapacity(endXOffsetExclusive - startXOffsetInclusive, sizeY)
        );
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        LevelChunk cachedChunk = null;
        int cachedChunkX = Integer.MIN_VALUE;
        int cachedChunkZ = Integer.MIN_VALUE;
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
                    if (activeMask != null) {
                        activeMask.set(index);
                    }
                    int chunkX = SectionPos.blockToSectionCoord(x);
                    int chunkZ = SectionPos.blockToSectionCoord(z);
                    if (chunkX != cachedChunkX || chunkZ != cachedChunkZ) {
                        cachedChunkX = chunkX;
                        cachedChunkZ = chunkZ;
                        cachedChunk = level.getChunkSource().getChunkNow(chunkX, chunkZ);
                    }
                    BlockState blockState = cachedChunk != null
                            ? cachedChunk.getBlockState(mutablePos)
                            : level.getBlockState(mutablePos);
                    if (blockState.isAir()) {
                        slowness.setFloat(index, airSlowness);
                        rowActiveCount++;
                        continue;
                    }
                    double resistance = getCachedResistanceCost(localResistanceCache, blockState);
                    slowness.setFloat(index, airSlowness + ((float) resistance * solidScale));
                    rowActiveCount++;
                    localSolidIndices.add(index);
                }
                activeCountByRow[rowIndex] = rowActiveCount;
            }
        }
        return new KrakkResistanceFieldChunkResult(localSolidIndices, sampledVoxelCount);
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

    private static KrakkResistanceFieldChunkResult sampleKrakkFieldChunk(ResistanceFieldSnapshot resistanceFieldSnapshot,
                                                                             int minX,
                                                                             int minY,
                                                                             int minZ,
                                                                             int sizeY,
                                                                             int sizeZ,
                                                                             int startXOffsetInclusive,
                                                                             int endXOffsetExclusive,
                                                                             MutableFloatIndexedAccess slowness,
                                                                             BitSet activeMask,
                                                                             int[] activeCountByRow,
                                                                             Int2DoubleOpenHashMap sharedResistanceCostCache) {
        Int2DoubleOpenHashMap localResistanceCache = sharedResistanceCostCache != null
                ? sharedResistanceCostCache
                : new Int2DoubleOpenHashMap(128);
        if (sharedResistanceCostCache == null) {
            localResistanceCache.defaultReturnValue(Double.NaN);
        }
        IntArrayList localSolidIndices = new IntArrayList(
                estimateKrakkChunkSolidPositionsCapacity(endXOffsetExclusive - startXOffsetInclusive, sizeY)
        );
        BlockState[] sampledStates = resistanceFieldSnapshot.sampledStates();
        int sampledVoxelCount = 0;
        float airSlowness = (float) KRAKK_AIR_SLOWNESS;
        float solidScale = (float) KRAKK_SOLID_SLOWNESS_SCALE;
        for (int xOffset = startXOffsetInclusive; xOffset < endXOffsetExclusive; xOffset++) {
            int baseX = xOffset * sizeY * sizeZ;
            for (int yOffset = 0; yOffset < sizeY; yOffset++) {
                int rowIndex = krakkRowIndex(xOffset, yOffset, sizeY);
                int baseIndex = baseX + (yOffset * sizeZ);
                int rowActiveCount = 0;
                for (int zOffset = 0; zOffset < sizeZ; zOffset++) {
                    BlockState blockState = sampledStates[baseIndex + zOffset];
                    if (blockState == null) {
                        continue;
                    }
                    sampledVoxelCount++;
                    int index = baseIndex + zOffset;
                    if (activeMask != null) {
                        activeMask.set(index);
                    }
                    if (blockState.isAir()) {
                        slowness.setFloat(index, airSlowness);
                        rowActiveCount++;
                        continue;
                    }
                    double resistance = getCachedResistanceCost(localResistanceCache, blockState);
                    slowness.setFloat(index, airSlowness + ((float) resistance * solidScale));
                    rowActiveCount++;
                    localSolidIndices.add(index);
                }
                activeCountByRow[rowIndex] = rowActiveCount;
            }
        }
        return new KrakkResistanceFieldChunkResult(localSolidIndices, sampledVoxelCount);
    }

    private static int resolveResistanceFieldTaskCount(int sizeX, int sizeY, int sizeZ) {
        RuntimeExecutionPolicy policy = activeRuntimeExecutionPolicy();
        if (policy.forceDirectResistanceSampling()) {
            return 1;
        }
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
        if (voxelCount > RESISTANCE_FIELD_MAX_PARALLEL_SNAPSHOT_VOXELS) {
            return 1;
        }
        int tasksByColumns = Math.max(1, sizeX / RESISTANCE_FIELD_COLUMNS_PER_TASK);
        int tasksByVoxels = (int) Math.max(1L, voxelCount / RESISTANCE_FIELD_MIN_VOXELS_PER_TASK);
        int tasksByWorkers = Math.max(1, VOLUMETRIC_TARGET_SCAN_PARALLELISM);
        int taskCount = Math.min(tasksByColumns, Math.min(tasksByVoxels, tasksByWorkers));
        taskCount = Math.min(taskCount, RESISTANCE_FIELD_MAX_TASKS);
        return taskCount >= 2 ? taskCount : 1;
    }

    private static int estimateKrakkSolidPositionsCapacity(int volume) {
        long requested = Math.max(64L, Math.max(1L, volume) / 64L);
        return clampKrakkPreallocation(requested, 64, KRAKK_SOLID_POSITIONS_PREALLOC_CAP);
    }

    private static int estimateKrakkChunkSolidPositionsCapacity(int chunkWidth, int sizeY) {
        long chunkCells = Math.max(1L, (long) Math.max(1, chunkWidth) * (long) Math.max(1, sizeY));
        long requested = Math.max(16L, chunkCells / 8L);
        return clampKrakkPreallocation(requested, 16, KRAKK_CHUNK_SOLID_POSITIONS_PREALLOC_CAP);
    }

    private static int estimateKrakkTargetMergeCapacity(int solidCount) {
        long requested = Math.max(64L, Math.max(1L, solidCount) / 16L);
        return clampKrakkPreallocation(requested, 64, KRAKK_TARGET_MERGE_PREALLOC_CAP);
    }

    private static int clampKrakkPreallocation(long requested, int min, int cap) {
        long bounded = Math.max(min, requested);
        bounded = Math.min(bounded, cap);
        return (int) Math.min(Integer.MAX_VALUE - 8L, bounded);
    }

    private static MutableFloatIndexedAccess createKrakkFloatStorage(int size) {
        return createKrakkFloatStorage(size, true);
    }

    private static SourceIndexSet createSourceIndexSet(int approximateVolume) {
        RuntimeExecutionPolicy policy = activeRuntimeExecutionPolicy();
        if (policy.compactSourceTracking()) {
            return new SparseSourceIndexSet();
        }
        int boundedSize = Math.max(1, approximateVolume);
        return new BitSetSourceIndexSet(boundedSize);
    }

    private static FloatChunk createAdaptiveFloatChunk(int chunkSize, boolean preferOffHeapFirst) {
        if (preferOffHeapFirst) {
            try {
                return new DirectFloatChunk(chunkSize);
            } catch (OutOfMemoryError directAllocationFailure) {
                // Fall back to heap for this chunk.
            }
            return new HeapFloatChunk(chunkSize);
        }

        try {
            return new HeapFloatChunk(chunkSize);
        } catch (OutOfMemoryError heapAllocationFailure) {
            // Fall back to direct for this chunk.
        }
        return new DirectFloatChunk(chunkSize);
    }

    private static MutableFloatIndexedAccess createKrakkFloatStorage(int size, boolean preferOffHeapForLarge) {
        int boundedSize = Math.max(1, size);
        if (boundedSize >= KRAKK_PAGED_FLOAT_OFFLOAD_THRESHOLD) {
            try {
                return new PagedFloatArrayStorage(boundedSize, KRAKK_PAGED_FLOAT_CACHE_PAGES);
            } catch (RuntimeException | OutOfMemoryError pagedStorageFailure) {
                // Fall through to adaptive in-memory storage if paging cannot be initialized.
            }
        }
        if (boundedSize >= KRAKK_OFFHEAP_FLOAT_OFFLOAD_THRESHOLD) {
            if (!preferOffHeapForLarge) {
                // Heap-first: try a single contiguous float[] before falling back to chunked.
                // HeapFloatArrayStorage.getFloat() = values[index] — a plain array access with no
                // chunk dispatch, bit-shift, or bounds-check overhead above the JVM's own array check.
                // For 50kt explosions (~4.2M cells, 16.8 MB), this fits comfortably in a modern JVM heap.
                try {
                    return new HeapFloatArrayStorage(boundedSize);
                } catch (OutOfMemoryError heapArrayOom) {
                    // Heap is fragmented; fall through to chunked adaptive storage.
                }
            }
            return new AdaptiveFloatArrayStorage(boundedSize, preferOffHeapForLarge);
        }
        return new HeapFloatArrayStorage(boundedSize);
    }

    private static MutableFloatIndexedAccess createKrakkSlownessStorage(int size) {
        int boundedSize = Math.max(1, size);
        if (boundedSize >= KRAKK_SPARSE_SLOWNESS_OFFLOAD_THRESHOLD) {
            return new SparseAirSolidSlownessStorage(boundedSize, (float) KRAKK_AIR_SLOWNESS);
        }
        // Heap-first for slowness lets arrival buffers consume more direct memory later.
        return createKrakkFloatStorage(boundedSize, false);
    }

    private static MutableFloatIndexedAccess createKrakkArrivalStorage(int size) {
        // Heap-first for arrival: HeapFloatChunk.getFloat() = values[index] is ~3x faster than
        // DirectFloatChunk.getFloat() = ByteBuffer.getFloat(), which incurs Buffer.checkIndex +
        // ScopedMemoryAccess + Unsafe overhead on every solveKrakkCell call (7x per cell).
        // For 50kt explosions, slowness uses SparseAirSolidSlownessStorage, so there is no
        // heap/direct memory competition that motivated the original direct-first choice.
        return createKrakkFloatStorage(size, false);
    }

    private static boolean requiresSerializedFloatAccess(FloatIndexedAccess indexedAccess) {
        return indexedAccess instanceof PagedFloatArrayStorage;
    }

    private static boolean tracksActiveMaskInSlowness(FloatIndexedAccess indexedAccess) {
        return indexedAccess instanceof SparseAirSolidSlownessStorage;
    }

    private static LongIndexedAccess wrapLongArrayList(LongArrayList values) {
        if (values == null || values.isEmpty()) {
            return EmptyLongIndexedAccess.INSTANCE;
        }
        return new LongArrayListIndexedAccess(values);
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
        return Math.min(KRAKK_SWEEP_MAX_TASKS, taskCount);
    }

    private static void cancelKrakkSweepFutures(List<? extends Future<?>> futures) {
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    private static VolumetricTargetScanResult scanVolumetricTargets(LongIndexedAccess solidPositions,
                                                                     VolumetricTargetScanContext context,
                                                                     boolean profileSubstages) {
        int solidCount = solidPositions.size();
        int taskCount = resolveVolumetricTargetScanTaskCount(solidCount);
        if (taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
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

        int mergedCapacity = estimateKrakkTargetMergeCapacity(solidCount);
        LongArrayList mergedPositions = new LongArrayList(mergedCapacity);
        FloatArrayList mergedWeights = new FloatArrayList(mergedCapacity);
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

    private static VolumetricTargetScanResult scanVolumetricTargetScanChunk(LongIndexedAccess solidPositions,
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
        double resolvedRadiusSq = resolvedRadius * resolvedRadius;
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
            if (distSq >= resolvedRadiusSq) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
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
                        centerX, centerY, centerZ, radialFraction, nx, ny, nz);
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
            // 2.25 = 2 + 1/4, so x^2.25 = x² · ⁴√x — avoids Math.pow in the hot loop
            double rfSq = radialFraction * radialFraction;
            double edgeGradient = rfSq * Math.sqrt(Math.sqrt(radialFraction));
            double gradient = Mth.lerp(VOLUMETRIC_EDGE_POINT_BIAS_BLEND, centerGradient, edgeGradient);
            if (gradient <= 0.0D) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }

            double maxWeight = gradient;
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
        return Math.min(VOLUMETRIC_TARGET_SCAN_MAX_TASKS, taskCount);
    }

    private static void cancelVolumetricTargetScanFutures(List<Future<VolumetricTargetScanResult>> futures) {
        for (Future<VolumetricTargetScanResult> future : futures) {
            future.cancel(true);
        }
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

    private static KrakkTargetScanResult scanKrakkTargets(LongIndexedAccess solidPositions,
                                                              float[] baselineByIndex,
                                                              FloatIndexedAccess arrivalTimes,
                                                              FloatIndexedAccess shadowArrivalTimes,
                                                              KrakkField krakkField,
                                                              double centerX,
                                                              double centerY,
                                                              double centerZ,
                                                              double maxArrival,
                                                              double blastTransmittance,
                                                              boolean allowParallel,
                                                              boolean profileSubstages) {
        int solidCount = solidPositions.size();
        boolean parallelTargetScanAllowed = allowParallel
                && !requiresSerializedFloatAccess(arrivalTimes)
                && !requiresSerializedFloatAccess(shadowArrivalTimes);
        int taskCount = resolveKrakkTargetScanTaskCount(solidCount);
        if (!parallelTargetScanAllowed || taskCount <= 1 || VOLUMETRIC_TARGET_SCAN_POOL == null) {
            return scanKrakkTargetScanChunk(
                    solidPositions,
                    0,
                    solidCount,
                    baselineByIndex,
                    arrivalTimes,
                    shadowArrivalTimes,
                    krakkField,
                    centerX,
                    centerY,
                    centerZ,
                    maxArrival,
                    blastTransmittance,
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
                            baselineByIndex,
                            arrivalTimes,
                            shadowArrivalTimes,
                            krakkField,
                            centerX,
                            centerY,
                            centerZ,
                            maxArrival,
                            blastTransmittance,
                            profileSubstages
                    )
            ));
        }

        int mergedCapacity = estimateKrakkTargetMergeCapacity(solidCount);
        LongArrayList mergedPositions = new LongArrayList(mergedCapacity);
        FloatArrayList mergedWeights = new FloatArrayList(mergedCapacity);
        double solidWeight = 0.0D;
        double maxWeight = 0.0D;
        long precheckNanos = 0L;
        try {
            for (Future<KrakkTargetScanResult> future : futures) {
                KrakkTargetScanResult chunkResult = future.get();
                precheckNanos += chunkResult.precheckNanos();
                solidWeight += chunkResult.solidWeight();
                maxWeight = Math.max(maxWeight, chunkResult.maxWeight());
                LongArrayList chunkPositions = chunkResult.targetPositions();
                FloatArrayList chunkWeights = chunkResult.targetWeights();
                for (int i = 0; i < chunkPositions.size(); i++) {
                    mergedPositions.add(chunkPositions.getLong(i));
                    mergedWeights.add(chunkWeights.getFloat(i));
                }
            }
            return new KrakkTargetScanResult(mergedPositions, mergedWeights, solidWeight, maxWeight, precheckNanos);
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
                baselineByIndex,
                arrivalTimes,
                shadowArrivalTimes,
                krakkField,
                centerX,
                centerY,
                centerZ,
                maxArrival,
                blastTransmittance,
                profileSubstages
        );
    }

    private static KrakkTargetScanResult scanKrakkTargetScanChunk(LongIndexedAccess solidPositions,
                                                                       int startInclusive,
                                                                       int endExclusive,
                                                                       float[] baselineByIndex,
                                                                       FloatIndexedAccess arrivalTimes,
                                                                       FloatIndexedAccess shadowArrivalTimes,
                                                                       KrakkField krakkField,
                                                                       double centerX,
                                                                       double centerY,
                                                                       double centerZ,
                                                                       double maxArrival,
                                                                       double blastTransmittance,
                                                                       boolean profileSubstages) {
        int expectedTargets = Math.max(16, (endExclusive - startInclusive) / 8);
        LongArrayList targetPositions = new LongArrayList(expectedTargets);
        FloatArrayList targetWeights = new FloatArrayList(expectedTargets);
        double maxWeight = 0.0D;
        long precheckNanos = 0L;
        double solidWeight = 0.0D;
        int fieldMinX = krakkField.minX();
        int fieldMinY = krakkField.minY();
        int fieldMinZ = krakkField.minZ();
        int fieldSizeY = krakkField.sizeY();
        int fieldSizeZ = krakkField.sizeZ();
        // Hoisted out of the per-block loop: depends only on the loop-invariant blastTransmittance.
        // At blastTransmittanceScale=0 the Fibonacci-sampled baseline is not used to cull blocks —
        // its irregular boundary coverage would produce a lumpy outer surface; the eikonal arrival
        // gate and final weight threshold are sufficient.
        double blastTransmittanceScale = Math.min(1.0D, blastTransmittance / KRAKK_BASELINE_SMOOTH_BLEND);
        for (int i = startInclusive; i < endExclusive; i++) {
            long posLong = solidPositions.getLong(i);
            long precheckStart = profileSubstages ? System.nanoTime() : 0L;
            int x = BlockPos.getX(posLong);
            int y = BlockPos.getY(posLong);
            int z = BlockPos.getZ(posLong);
            int index = krakkGridIndex(x - fieldMinX, y - fieldMinY, z - fieldMinZ, fieldSizeY, fieldSizeZ);
            float baselineWeight = baselineByIndex[index];
            if (blastTransmittanceScale > 0.0D && (!Float.isFinite(baselineWeight) || baselineWeight <= KRAKK_TARGET_MIN_WEIGHT)) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double arrival = arrivalTimes.getFloat(index);
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
            // At blastTransmittanceScale=0 (smooth) bypass material-resistance filters so the result is
            // a clean sphere unaffected by block density. At blastTransmittanceScale=1 the
            // original hard-block rules apply in full.
            if (blastTransmittanceScale > 0.0D && normalizedOverrun >= KRAKK_HARD_BLOCK_NORMALIZED_OVERRUN) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double transmittance = Math.exp(
                    -(effectiveOverrun * KRAKK_RESISTANCE_ATTENUATION_PER_OVERRUN)
                            - (normalizedOverrun * KRAKK_RESISTANCE_NORMALIZED_ATTENUATION)
            );
            if (blastTransmittanceScale > 0.0D && transmittance <= KRAKK_MIN_TRANSMITTANCE) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double shadowArrival = shadowArrivalTimes.getFloat(index);
            if (!Double.isFinite(shadowArrival)) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double shadowOverrun = Math.max(0.0D, shadowArrival - arrival);
            double effectiveShadowOverrun = Math.max(0.0D, shadowOverrun - KRAKK_SHADOW_OVERRUN_DEADZONE);
            double normalizedShadowOverrun = effectiveShadowOverrun / Math.max(0.25D, airArrival);
            if (blastTransmittanceScale > 0.0D && normalizedShadowOverrun >= KRAKK_HARD_BLOCK_SHADOW_NORMALIZED_OVERRUN) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double shadowTransmittance = computeShadowTransmittance(effectiveShadowOverrun, normalizedShadowOverrun);
            if (blastTransmittanceScale > 0.0D && shadowTransmittance <= KRAKK_MIN_TRANSMITTANCE) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double eikNormalized = normalized;
            boolean outerGatePassed = false;
            if (normalized < 0.5D) {
                long h = ((long) x * 0x9E3779B97F4A7C15L)
                       ^ ((long) y * 0x6C62272E07BB0142L)
                       ^ ((long) z * 0xCB9C59B3F9F87D4DL)
                       ^ ((long) Mth.floor(centerX) * 0xBF58476D1CE4E5B9L)
                       ^ ((long) Mth.floor(centerZ) * 0x94D049BB133111EBL);
                h ^= (h >>> 33);
                h *= 0xFF51AFD7ED558CCDL;
                h ^= (h >>> 33);
                double blockNoise = (h >>> 11 & 0x1FFFFFL) / 2097151.0D;
                // keepProbability: 1.0 when blastTransmittanceScale=0 (nothing skipped),
                // normalized*2.0 when blastTransmittanceScale=1 (original gate).
                double keepProbability = 1.0D - blastTransmittanceScale * (1.0D - normalized * 2.0D);
                if (blockNoise >= keepProbability) {
                    if (profileSubstages) {
                        precheckNanos += (System.nanoTime() - precheckStart);
                    }
                    continue;
                }
                outerGatePassed = blastTransmittanceScale > 0.0D;
            }
            double weightExponent = Mth.lerp(blastTransmittanceScale, KRAKK_WEIGHT_EXPONENT_SMOOTH, KRAKK_WEIGHT_EXPONENT);
            double eikEnvelope = Math.pow(eikNormalized, weightExponent);
            double rawTransFactor = Mth.lerp(
                    KRAKK_ENVELOPE_TRANSMITTANCE_BLEND,
                    1.0D,
                    transmittance * shadowTransmittance
            );
            // At blastTransmittanceScale=0 (smooth) transmittance has no effect (factor=1.0);
            // at blastTransmittanceScale=1 it applies fully.
            double krakkTransFactor = Mth.lerp(blastTransmittanceScale, 1.0D, rawTransFactor);
            double blendFloor = Mth.lerp(blastTransmittanceScale, KRAKK_BASELINE_SMOOTH_BLEND_FLOOR, KRAKK_BASELINE_SMOOTH_BLEND);
            double smoothingFactor = Mth.lerp(
                    blendFloor,
                    1.0D,
                    eikEnvelope * krakkTransFactor
            );
            // Gate-passing blocks blend cutoffRetention toward 1.0 proportionally to
            // blastTransmittanceScale: at full blastTransmittance the gate is the sole break/spare decision
            // (retention=1), at zero blastTransmittance the smooth curve is used in full.
            double smoothCutoff = computeKrakkCutoffEdgeRetention(eikNormalized);
            double cutoffRetention = outerGatePassed
                    ? Mth.lerp(blastTransmittanceScale, smoothCutoff, 1.0D)
                    : smoothCutoff;
            double rawWeight = baselineWeight * smoothingFactor * cutoffRetention;
            // At blastTransmittanceScale=0 (smooth) ignore baselineWeight/eikonal variation entirely —
            // all blocks inside the sphere get uniform weight; only the edge soft-falloff remains.
            // At blastTransmittanceScale=1 the full eikonal+fibonacci shaped weight is used.
            // Nonlinear (squared) so that moderate blastTransmittanceScale values lean smoothly quickly.
            double smoothT = blastTransmittanceScale * blastTransmittanceScale;
            double weight = Mth.lerp(smoothT, cutoffRetention, rawWeight);
            // Near the blast boundary (normalized < 0.20), suppress non-spike directions so
            // the inner side of each spike has a valley. Spike directions are left untouched;
            // the matching outward tips are added in applyExtraRadiusImpacts using the same noise.
            if (normalized < 0.20D && blastTransmittanceScale > 0.0D) {
                double ddx = x + 0.5D - centerX;
                double ddy = y + 0.5D - centerY;
                double ddz = z + 0.5D - centerZ;
                double ddist = Math.sqrt(ddx * ddx + ddy * ddy + ddz * ddz);
                double invDist = ddist > 1.0E-9D ? 1.0D / ddist : 0.0D;
                int qx = Mth.floor((ddx * invDist + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
                int qy = Mth.floor((ddy * invDist + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
                int qz = Mth.floor((ddz * invDist + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
                int cx = Mth.floor(centerX * 2.0D);
                int cy = Mth.floor(centerY * 2.0D);
                int cz = Mth.floor(centerZ * 2.0D);
                double primary   = sampleVolumetricEdgeSpikeNoise(qx, qy, qz, cx, cy, cz);
                double secondary = sampleVolumetricEdgeSpikeNoise((qx * 2) + 7, (qy * 2) + 13, (qz * 2) + 19, cx, cy, cz);
                double noiseVal  = Mth.clamp(primary * 0.72D + secondary * 0.28D, 0.0D, 1.0D);
                if (noiseVal < 0.5D) {
                    // Non-spike direction: suppress weight proportional to boundary proximity
                    // and how far the noise is below the threshold — hard suppression for valleys.
                    double boundaryT   = 1.0D - normalized / 0.20D; // 0 at normalized=0.20, 1 at boundary
                    double belowThresh = (0.5D - noiseVal) * 2.0D;  // 0 at threshold, 1 at min noise
                    weight *= Math.max(0.0D, 1.0D - blastTransmittanceScale * boundaryT * belowThresh);
                }
            }
            if (weight <= KRAKK_TARGET_MIN_WEIGHT) {
                if (profileSubstages) {
                    precheckNanos += (System.nanoTime() - precheckStart);
                }
                continue;
            }
            double sampledWeight = weight;
            if (profileSubstages) {
                precheckNanos += (System.nanoTime() - precheckStart);
            }
            targetPositions.add(posLong);
            targetWeights.add((float) sampledWeight);
            solidWeight += sampledWeight;
            if (sampledWeight > maxWeight) maxWeight = sampledWeight;
        }
        return new KrakkTargetScanResult(targetPositions, targetWeights, solidWeight, maxWeight, precheckNanos);
    }

    private static int resolveKrakkTargetScanTaskCount(int solidCount) {
        if (VOLUMETRIC_TARGET_SCAN_POOL == null || solidCount < KRAKK_TARGET_SCAN_MIN_SOLIDS_FOR_PARALLEL) {
            return 1;
        }
        int tasksBySize = Math.max(1, (solidCount + KRAKK_TARGET_SCAN_SOLIDS_PER_TASK - 1) / KRAKK_TARGET_SCAN_SOLIDS_PER_TASK);
        int tasksByWorkers = Math.max(1, VOLUMETRIC_TARGET_SCAN_PARALLELISM * 2);
        int taskCount = Math.min(tasksBySize, tasksByWorkers);
        return Math.min(KRAKK_TARGET_SCAN_MAX_TASKS, taskCount);
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
            int[] directionLookup = buildVolumetricDirectionCubeMap(dirX, dirY, dirZ, lookupResolution);
            return new VolumetricDirectionCache(dirX, dirY, dirZ, neighbors, directionLookup, lookupResolution);
        });
    }

    // Cube-map direction lookup: 6 faces × resolution² cells. Each face covers one
    // dominant-axis hemisphere (+x, -x, +y, -y, +z, -z). No fold-back, no signum(0)
    // singularity, no seam along any world axis. Total table size = 6 × resolution².
    private static int[] buildVolumetricDirectionCubeMap(double[] dirX,
                                                          double[] dirY,
                                                          double[] dirZ,
                                                          int resolution) {
        int faceSize = resolution * resolution;
        int[] cubeMap = new int[6 * faceSize];
        int directionCount = dirX.length;
        for (int face = 0; face < 6; face++) {
            for (int v = 0; v < resolution; v++) {
                for (int u = 0; u < resolution; u++) {
                    Direction dir = decodeCubeMapDirection(face, u, v, resolution);
                    double dnx = dir.x();
                    double dny = dir.y();
                    double dnz = dir.z();
                    int bestIndex = 0;
                    double bestDot = -Double.MAX_VALUE;
                    for (int i = 0; i < directionCount; i++) {
                        double dot = (dnx * dirX[i]) + (dny * dirY[i]) + (dnz * dirZ[i]);
                        if (dot > bestDot) {
                            bestDot = dot;
                            bestIndex = i;
                        }
                    }
                    cubeMap[face * faceSize + v * resolution + u] = bestIndex;
                }
            }
        }
        return cubeMap;
    }

    // Face layout (ou, ov are the two non-dominant Minecraft axes, ref is the dominant):
    //   0 = +x: ref=+1, ou=ny/nx,   ov=nz/nx   → decode (1,  ou, ov)
    //   1 = -x: ref=-1, ou=ny/|nx|, ov=nz/|nx| → decode (-1, ou, ov)
    //   2 = +y: ref=+1, ou=nx/ny,   ov=nz/ny   → decode (ou, 1,  ov)
    //   3 = -y: ref=-1, ou=nx/|ny|, ov=nz/|ny| → decode (ou, -1, ov)
    //   4 = +z: ref=+1, ou=nx/nz,   ov=ny/nz   → decode (ou, ov, 1 )
    //   5 = -z: ref=-1, ou=nx/|nz|, ov=ny/|nz| → decode (ou, ov, -1)
    private static Direction decodeCubeMapDirection(int face, int u, int v, int resolution) {
        double ou = (((u + 0.5D) / resolution) * 2.0D) - 1.0D;
        double ov = (((v + 0.5D) / resolution) * 2.0D) - 1.0D;
        double dx, dy, dz;
        switch (face) {
            case 0:  dx =  1.0D; dy = ou;  dz = ov;  break;
            case 1:  dx = -1.0D; dy = ou;  dz = ov;  break;
            case 2:  dx = ou;  dy =  1.0D; dz = ov;  break;
            case 3:  dx = ou;  dy = -1.0D; dz = ov;  break;
            case 4:  dx = ou;  dy = ov;  dz =  1.0D; break;
            default: dx = ou;  dy = ov;  dz = -1.0D; break;
        }
        double len = Math.sqrt((dx * dx) + (dy * dy) + (dz * dz));
        if (len <= 1.0E-9D) {
            return new Direction(0.0D, 1.0D, 0.0D);
        }
        double inv = 1.0D / len;
        return new Direction(dx * inv, dy * inv, dz * inv);
    }

    private static double computeVolumetricRadiusScale(double radius, double maxScale) {
        double normalized = radius / VOLUMETRIC_RADIUS_SCALE_BASE;
        return Mth.clamp(normalized, 1.0D, maxScale);
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
        // Fast-path: octahedral lookup gives nearest Fibonacci seed in O(1), then
        // search only the seed + its pre-computed 20 neighbors (~21 candidates total)
        // instead of the full O(directionCount=3072) scan. The decodeOctahedralDirection
        // fix ensures the lookup covers both hemispheres correctly.
        int seedCell = cubeMapLookupIndex(nx, ny, nz, directionLookupResolution);
        int seed = directionLookup[seedCell];
        int[] seedNeighbors = directionNeighbors[seed];
        for (int pass = 0; pass <= seedNeighbors.length; pass++) {
            int i = (pass == 0) ? seed : seedNeighbors[pass - 1];
            if (i < 0) break;
            double dot = (nx * dirX[i]) + (ny * dirY[i]) + (nz * dirZ[i]);
            if (dot > dot0) {
                dot7 = dot6; best7 = best6;
                dot6 = dot5; best6 = best5;
                dot5 = dot4; best5 = best4;
                dot4 = dot3; best4 = best3;
                dot3 = dot2; best3 = best2;
                dot2 = dot1; best2 = best1;
                dot1 = dot0; best1 = best0;
                dot0 = dot; best0 = i;
            } else if (dot > dot1) {
                dot7 = dot6; best7 = best6;
                dot6 = dot5; best6 = best5;
                dot5 = dot4; best5 = best4;
                dot4 = dot3; best4 = best3;
                dot3 = dot2; best3 = best2;
                dot2 = dot1; best2 = best1;
                dot1 = dot; best1 = i;
            } else if (dot > dot2) {
                dot7 = dot6; best7 = best6;
                dot6 = dot5; best6 = best5;
                dot5 = dot4; best5 = best4;
                dot4 = dot3; best4 = best3;
                dot3 = dot2; best3 = best2;
                dot2 = dot; best2 = i;
            } else if (dot > dot3) {
                dot7 = dot6; best7 = best6;
                dot6 = dot5; best6 = best5;
                dot5 = dot4; best5 = best4;
                dot4 = dot3; best4 = best3;
                dot3 = dot; best3 = i;
            } else if (dot > dot4) {
                dot7 = dot6; best7 = best6;
                dot6 = dot5; best6 = best5;
                dot5 = dot4; best5 = best4;
                dot4 = dot; best4 = i;
            } else if (dot > dot5) {
                dot7 = dot6; best7 = best6;
                dot6 = dot5; best6 = best5;
                dot5 = dot; best5 = i;
            } else if (dot > dot6) {
                dot7 = dot6; best7 = best6;
                dot6 = dot; best6 = i;
            } else if (dot > dot7) {
                dot7 = dot; best7 = i;
            }
        }

        double weightedPressure = 0.0D;
        double weightSum = 0.0D;
        if (best0 >= 0) {
            double weight0 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot0) * 1023.0)];
            if (weight0 > 0.0D) {
                double pressure0 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best0], pressureByShell[upperRowOffset + best0]);
                weightedPressure += pressure0 * weight0;
                weightSum += weight0;
            }
        }
        if (blendCount >= 2 && best1 >= 0) {
            double weight1 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot1) * 1023.0)];
            if (weight1 > 0.0D) {
                double pressure1 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best1], pressureByShell[upperRowOffset + best1]);
                weightedPressure += pressure1 * weight1;
                weightSum += weight1;
            }
        }
        if (blendCount >= 3 && best2 >= 0) {
            double weight2 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot2) * 1023.0)];
            if (weight2 > 0.0D) {
                double pressure2 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best2], pressureByShell[upperRowOffset + best2]);
                weightedPressure += pressure2 * weight2;
                weightSum += weight2;
            }
        }
        if (blendCount >= 4 && best3 >= 0) {
            double weight3 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot3) * 1023.0)];
            if (weight3 > 0.0D) {
                double pressure3 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best3], pressureByShell[upperRowOffset + best3]);
                weightedPressure += pressure3 * weight3;
                weightSum += weight3;
            }
        }
        if (blendCount >= 5 && best4 >= 0) {
            double weight4 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot4) * 1023.0)];
            if (weight4 > 0.0D) {
                double pressure4 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best4], pressureByShell[upperRowOffset + best4]);
                weightedPressure += pressure4 * weight4;
                weightSum += weight4;
            }
        }
        if (blendCount >= 6 && best5 >= 0) {
            double weight5 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot5) * 1023.0)];
            if (weight5 > 0.0D) {
                double pressure5 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best5], pressureByShell[upperRowOffset + best5]);
                weightedPressure += pressure5 * weight5;
                weightSum += weight5;
            }
        }
        if (blendCount >= 7 && best6 >= 0) {
            double weight6 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot6) * 1023.0)];
            if (weight6 > 0.0D) {
                double pressure6 = Mth.lerp(shellAlpha, pressureByShell[lowerRowOffset + best6], pressureByShell[upperRowOffset + best6]);
                weightedPressure += pressure6 * weight6;
                weightSum += weight6;
            }
        }
        if (blendCount >= 8 && best7 >= 0) {
            double weight7 = DOT_WEIGHT_LUT[(int) (Math.max(0.0D, dot7) * 1023.0)];
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

    private static int cubeMapLookupIndex(double nx, double ny, double nz, int resolution) {
        double ax = Math.abs(nx);
        double ay = Math.abs(ny);
        double az = Math.abs(nz);
        int face;
        double ou, ov;
        if (ax >= ay && ax >= az) {
            double inv = 1.0D / Math.max(ax, 1.0E-9D);
            face = (nx >= 0.0D) ? 0 : 1;
            ou = ny * inv;
            ov = nz * inv;
        } else if (ay >= az) {
            double inv = 1.0D / Math.max(ay, 1.0E-9D);
            face = (ny >= 0.0D) ? 2 : 3;
            ou = nx * inv;
            ov = nz * inv;
        } else {
            double inv = 1.0D / Math.max(az, 1.0E-9D);
            face = (nz >= 0.0D) ? 4 : 5;
            ou = nx * inv;
            ov = ny * inv;
        }
        int u = Mth.clamp(Mth.floor(((ou + 1.0D) * 0.5D) * resolution), 0, resolution - 1);
        int v = Mth.clamp(Mth.floor(((ov + 1.0D) * 0.5D) * resolution), 0, resolution - 1);
        return face * resolution * resolution + v * resolution + u;
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

    @SuppressWarnings("unused") // parameters reserved for future sound/particle effects
    private static void emitExplosionEffects(ServerLevel level, double x, double y, double z, RandomSource random) {
        // Intentionally disabled: Cannonical requests no Krakk explosion sound/particle effects.
    }

    private static void applySingleBlockImpact(ServerLevel level,
                                               BlockPos.MutableBlockPos mutablePos,
                                               long blockPosLong,
                                               double resolvedImpactPower,
                                               Entity source,
                                               LivingEntity owner,
                                               double impactHeatCelsius,
                                               boolean thermalOnly,
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

        if (!thermalOnly && applyWorldChanges && tryTriggerNativeIgnite(level, mutablePos, blockState, source, owner)) {
            if (trace != null) {
                trace.specialHandled++;
            }
            return;
        }

        if (!thermalOnly && applyWorldChanges && specialBlockHandler.handle(level, mutablePos, blockState, source, owner)) {
            if (trace != null) {
                trace.specialHandled++;
            }
            return;
        }

        if (!thermalOnly && blockState.is(Blocks.TNT)) {
            if (applyWorldChanges) {
                TntBlock.explode(level, mutablePos);
                level.removeBlock(mutablePos, false);
            }
            if (trace != null) {
                trace.tntTriggered++;
            }
            return;
        }

        if (!thermalOnly && resolvedImpactPower <= MIN_RESOLVED_RAY_IMPACT) {
            if (trace != null) {
                trace.lowImpactSkipped++;
            }
            return;
        }

        if (applyWorldChanges) {
            var damageApi = KrakkApi.damage();
            KrakkImpactResult result;
            if (damageApi instanceof KrakkDamageRuntime damageRuntime) {
                KrakkDamageRuntime.ImpactExecutionResult executionResult;
                if (thermalOnly) {
                    executionResult = damageRuntime.applyThermalImpactPrevalidatedWithEvents(
                            level, mutablePos, blockState, source,
                            resolvedImpactPower, impactHeatCelsius,
                            KrakkDamageType.KRAKK_DAMAGE_EXPLOSION);
                } else {
                    // Fast path: skip the full damage state machine for guaranteed-break blocks.
                    // A block is guaranteed to break if it is instant-mine (hardness == 0) or if
                    // the impact delta is >= MAX_DAMAGE_STATE regardless of current damage state.
                    KrakkDamageRuntime.ImpactExecutionResult fastResult = null;
                    boolean isFluid = blockState.getBlock() instanceof LiquidBlock
                            && !blockState.getFluidState().isEmpty();
                    if (!isFluid) {
                        float hardness = blockState.getDestroySpeed(level, mutablePos);
                        if (hardness >= 0.0F
                                && (hardness == 0.0F || KrakkDamageCurves.computeImpactDamageDelta(
                                blockState.getBlock() instanceof FallingBlock,
                                resolvedImpactPower,
                                blockState.getBlock().getExplosionResistance(),
                                hardness) >= KrakkDamageCurves.MAX_DAMAGE_STATE)) {
                            fastResult = damageRuntime.applyGuaranteedBreakExplosionImpact(
                                    level, mutablePos, blockState, source,
                                    resolvedImpactPower, impactHeatCelsius);
                        }
                    }
                    executionResult = fastResult != null ? fastResult
                            : damageRuntime.applyImpactPrevalidatedWithEvents(
                            level, mutablePos, blockState, source,
                            resolvedImpactPower, impactHeatCelsius,
                            false, KrakkDamageType.KRAKK_DAMAGE_EXPLOSION);
                }
                result = executionResult.impactResult();
            } else {
                result = damageApi.applyImpact(
                        level,
                        mutablePos,
                        blockState,
                        source,
                        resolvedImpactPower,
                        impactHeatCelsius,
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

        if (!thermalOnly) {
            predictImpactOutcome(level, mutablePos, blockState, resolvedImpactPower, trace);
        }
    }


    /**
     * Post-solve expansion pass: directly applies a synthetic impact to zero-hardness (instant-break)
     * and damageable blocks in an annular shell just beyond the normal blast radius.
     *
     * <p>Instant-break blocks (hardness == 0) receive a trivial synthetic impact — the fast path
     * in {@code applySingleBlockImpact} breaks them regardless of magnitude. Harder blocks receive
     * a scaled impact proportional to {@code blastPower}, applying damage states and triggering the
     * conversion handler at the periphery of the blast.
     */
    private static void applyExtraRadiusImpacts(
            ServerLevel level, double centerX, double centerY, double centerZ,
            double resolvedRadius, LongArrayList targetPositions,
            Entity source, LivingEntity owner, double impactHeatCelsius,
            double blastTransmittance,
            boolean applyWorldChanges, ExplosionProfileTrace trace,
            BlockPos.MutableBlockPos mutablePos) {
        if (targetPositions.isEmpty()) return;

        double extraRadius = resolvedRadius * EXTRA_RADIUS_FRACTION;
        int    maxSteps    = Math.max(1, (int) Math.ceil(extraRadius));

        // Build a fast lookup set so we can detect blast-surface blocks (targeted blocks
        // whose outward neighbor was not reached by the Eikonal solver). Resistance
        // blocking is already baked in — the solver simply never reached occluded cells.
        LongOpenHashSet targetSet = new LongOpenHashSet(targetPositions.size() * 2);
        for (int i = 0; i < targetPositions.size(); i++) {
            targetSet.add(targetPositions.getLong(i));
        }

        int scx = Mth.floor(centerX * 2.0D);
        int scy = Mth.floor(centerY * 2.0D);
        int scz = Mth.floor(centerZ * 2.0D);

        for (int i = 0; i < targetPositions.size(); i++) {
            long posLong = targetPositions.getLong(i);
            int px = BlockPos.getX(posLong);
            int py = BlockPos.getY(posLong);
            int pz = BlockPos.getZ(posLong);

            double dx   = px + 0.5D - centerX;
            double dy   = py + 0.5D - centerY;
            double dz   = pz + 0.5D - centerZ;
            double dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
            if (dist < 1.0E-9D) continue;
            double invDist = 1.0D / dist;
            double nx = dx * invDist, ny = dy * invDist, nz = dz * invDist;

            // Surface detection: the block just beyond this one in the outward direction
            // must not have been reached by the solver.
            int onx = Mth.floor(px + 0.5D + nx);
            int ony = Mth.floor(py + 0.5D + ny);
            int onz = Mth.floor(pz + 0.5D + nz);
            if (targetSet.contains(BlockPos.asLong(onx, ony, onz))) continue;

            int spikeDepth = 0;
            if (blastTransmittance > 0.0D) {
                // Directional spike noise — same quantization and seeds as the inner
                // suppression in scanKrakkTargetScanChunk.
                int qx = Mth.floor((nx + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
                int qy = Mth.floor((ny + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
                int qz = Mth.floor((nz + 1.0D) * VOLUMETRIC_EDGE_SPIKE_DIRECTION_QUANTIZATION);
                double spikePrimary   = sampleVolumetricEdgeSpikeNoise(qx, qy, qz, scx, scy, scz);
                double spikeSecondary = sampleVolumetricEdgeSpikeNoise((qx * 2) + 7, (qy * 2) + 13, (qz * 2) + 19, scx, scy, scz);
                double spikeNoise     = Mth.clamp(spikePrimary * 0.72D + spikeSecondary * 0.28D, 0.0D, 1.0D);
                double spikeStrength  = Math.max(0.0D, spikeNoise - 0.5D) * 2.0D * blastTransmittance;
                spikeDepth = (int) Math.ceil(maxSteps * spikeStrength);
            }
            // Always walk at least 1 step so block conversions and instant-break apply
            // across the full envelope, even in non-spike directions.  Spray damage only
            // fires within spikeDepth (which is 0 for non-spike directions).
            int totalDepth = Math.max(1, spikeDepth);

            // Walk outward from this surface block, applying envelope treatment.
            for (int step = 1; step <= totalDepth; step++) {
                int tx = Mth.floor(px + 0.5D + nx * step);
                int ty = Mth.floor(py + 0.5D + ny * step);
                int tz = Mth.floor(pz + 0.5D + nz * step);
                long tLong = BlockPos.asLong(tx, ty, tz);
                if (targetSet.contains(tLong)) continue; // already handled by main blast

                mutablePos.set(tx, ty, tz);
                if (!level.isInWorldBounds(mutablePos)) break;
                BlockState state = level.getBlockState(mutablePos);
                if (state.isAir()) continue;
                if (state.getBlock() instanceof LiquidBlock
                        && !state.getFluidState().isEmpty()) continue;
                float hardness = state.getDestroySpeed(level, mutablePos);
                if (hardness < 0.0F) break; // indestructible — stop the spike here

                double impactFraction = spikeDepth > 0 ? 1.0D - (double) step / (spikeDepth + 1) : 0.0D;

                if (hardness == 0.0F) {
                    applySingleBlockImpact(
                            level, mutablePos, tLong,
                            MIN_RESOLVED_RAY_IMPACT * 10.0, source, owner, impactHeatCelsius,
                            false, applyWorldChanges, trace
                    );
                } else if (applyWorldChanges && KrakkDamageBlockConversions.hasConversionRule(state)) {
                    KrakkDamageBlockConversions.applyConversionForDamageState(
                            level, mutablePos, state,
                            KrakkDamageCurves.MAX_DAMAGE_STATE,
                            MIN_RESOLVED_RAY_IMPACT * 10.0, impactHeatCelsius
                    );
                } else if (step <= spikeDepth) {
                    double impact = impactFraction * NOISE_SPRAY_MAX_IMPACT;
                    if (impact > MIN_RESOLVED_RAY_IMPACT) {
                        applySingleBlockImpact(
                                level, mutablePos, tLong,
                                impact, source, owner, impactHeatCelsius,
                                false, applyWorldChanges, trace
                        );
                    }
                }
            }
        }
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
        List<Entity> candidates = level.getEntitiesOfClass(
                Entity.class,
                query,
                entity -> entity.isAlive() && !entity.isSpectator()
                        && ((entity instanceof LivingEntity && entity.isPickable())
                         || entity instanceof ItemEntity
                         || entity instanceof PrimedTnt
                         || entity instanceof AbstractMinecart
                         || entity instanceof FallingBlockEntity
                         || entity instanceof Projectile)
        );
        if (candidates.isEmpty()) {
            return null;
        }

        candidates.sort(Comparator.comparingInt(Entity::getId));
        List<EntityHitProxy> all = new ArrayList<>(candidates.size());

        for (Entity entity : candidates) {
            AABB aabb = entity.getBoundingBox().inflate(ENTITY_HITBOX_INFLATE);
            if (!aabb.intersects(query)) {
                continue;
            }
            all.add(new EntityHitProxy(entity, aabb));
        }

        if (all.isEmpty()) {
            return null;
        }
        if (trace != null) {
            trace.entityCandidates += all.size();
        }

        return new ExplosionEntityContext(all);
    }



    private static void accumulateKrakkEntityImpacts(ExplosionEntityContext context,
                                                     FloatIndexedAccess arrivalTimes,
                                                     FloatIndexedAccess shadowArrivalTimes,
                                                     KrakkField krakkField,
                                                     double maxArrival,
                                                     double resolvedRadius) {
        if (context == null) {
            return;
        }
        int fieldMinX = krakkField.minX();
        int fieldMinY = krakkField.minY();
        int fieldMinZ = krakkField.minZ();
        int fieldSizeY = krakkField.sizeY();
        int fieldSizeZ = krakkField.sizeZ();
        int fieldMaxX = fieldMinX + krakkField.sizeX() - 1;
        int fieldMaxY = fieldMinY + krakkField.sizeY() - 1;
        int fieldMaxZ = fieldMinZ + krakkField.sizeZ() - 1;
        for (EntityHitProxy proxy : context.all) {
            int ix = Mth.floor(proxy.centerX);
            int iy = Mth.floor(proxy.centerY);
            int iz = Mth.floor(proxy.centerZ);
            if (ix < fieldMinX || ix > fieldMaxX || iy < fieldMinY || iy > fieldMaxY || iz < fieldMinZ || iz > fieldMaxZ) {
                continue;
            }
            int index = krakkGridIndex(ix - fieldMinX, iy - fieldMinY, iz - fieldMinZ, fieldSizeY, fieldSizeZ);
            float arrival = arrivalTimes.getFloat(index);
            if (!Float.isFinite(arrival) || arrival > maxArrival) {
                continue;
            }
            double normalized = 1.0D - (arrival / maxArrival);
            double weight = Math.pow(normalized, KRAKK_WEIGHT_EXPONENT_SMOOTH);
            float shadowArrival = shadowArrivalTimes.getFloat(index);
            if (Float.isFinite(shadowArrival)) {
                double shadowOverrun = Math.max(0.0D, shadowArrival - arrival);
                double effectiveShadowOverrun = Math.max(0.0D, shadowOverrun - KRAKK_SHADOW_OVERRUN_DEADZONE);
                double normalizedShadowOverrun = effectiveShadowOverrun / Math.max(0.25D, arrival);
                if (normalizedShadowOverrun >= KRAKK_HARD_BLOCK_SHADOW_NORMALIZED_OVERRUN) {
                    continue;
                }
                double shadowTransmittance = computeShadowTransmittance(effectiveShadowOverrun, normalizedShadowOverrun);
                if (shadowTransmittance <= KRAKK_MIN_TRANSMITTANCE) {
                    continue;
                }
                weight *= shadowTransmittance;
            }
            proxy.accumulatedImpact = resolvedRadius * VOLUMETRIC_ENTITY_IMPACT_PER_RADIUS * weight;

            // Derive knockback direction from the arrival-time gradient (∇T).
            // ∇T points in the direction the wavefront was travelling, which is the blast push direction.
            float axn = sampleNeighborArrival(arrivalTimes, ix - 1, iy, iz, fieldMinX, fieldMinY, fieldMinZ, fieldMaxX, fieldMaxY, fieldMaxZ, fieldSizeY, fieldSizeZ);
            float axp = sampleNeighborArrival(arrivalTimes, ix + 1, iy, iz, fieldMinX, fieldMinY, fieldMinZ, fieldMaxX, fieldMaxY, fieldMaxZ, fieldSizeY, fieldSizeZ);
            float ayn = sampleNeighborArrival(arrivalTimes, ix, iy - 1, iz, fieldMinX, fieldMinY, fieldMinZ, fieldMaxX, fieldMaxY, fieldMaxZ, fieldSizeY, fieldSizeZ);
            float ayp = sampleNeighborArrival(arrivalTimes, ix, iy + 1, iz, fieldMinX, fieldMinY, fieldMinZ, fieldMaxX, fieldMaxY, fieldMaxZ, fieldSizeY, fieldSizeZ);
            float azn = sampleNeighborArrival(arrivalTimes, ix, iy, iz - 1, fieldMinX, fieldMinY, fieldMinZ, fieldMaxX, fieldMaxY, fieldMaxZ, fieldSizeY, fieldSizeZ);
            float azp = sampleNeighborArrival(arrivalTimes, ix, iy, iz + 1, fieldMinX, fieldMinY, fieldMinZ, fieldMaxX, fieldMaxY, fieldMaxZ, fieldSizeY, fieldSizeZ);
            double gx = arrivalGradientComponent(axn, arrival, axp);
            double gy = arrivalGradientComponent(ayn, arrival, ayp);
            double gz = arrivalGradientComponent(azn, arrival, azp);
            double gMagSq = gx * gx + gy * gy + gz * gz;
            if (gMagSq > 1.0E-8D) {
                double invGMag = proxy.accumulatedImpact / Math.sqrt(gMagSq);
                proxy.pressureX = gx * invGMag;
                proxy.pressureY = gy * invGMag;
                proxy.pressureZ = gz * invGMag;
            }
        }
    }

    private static float sampleNeighborArrival(FloatIndexedAccess arrivalTimes,
                                               int nx, int ny, int nz,
                                               int fieldMinX, int fieldMinY, int fieldMinZ,
                                               int fieldMaxX, int fieldMaxY, int fieldMaxZ,
                                               int fieldSizeY, int fieldSizeZ) {
        if (nx < fieldMinX || nx > fieldMaxX || ny < fieldMinY || ny > fieldMaxY || nz < fieldMinZ || nz > fieldMaxZ) {
            return Float.NaN;
        }
        return arrivalTimes.getFloat(krakkGridIndex(nx - fieldMinX, ny - fieldMinY, nz - fieldMinZ, fieldSizeY, fieldSizeZ));
    }

    private static double computeShadowTransmittance(double effectiveShadowOverrun, double normalizedShadowOverrun) {
        return Math.exp(
                -(effectiveShadowOverrun * KRAKK_SHADOW_ATTENUATION_PER_OVERRUN)
                        - (normalizedShadowOverrun * KRAKK_SHADOW_NORMALIZED_ATTENUATION)
        );
    }

    private static double arrivalGradientComponent(float neg, float center, float pos) {
        boolean negValid = Float.isFinite(neg);
        boolean posValid = Float.isFinite(pos);
        if (negValid && posValid) {
            return (pos - neg) * 0.5D;
        } else if (posValid) {
            return pos - center;
        } else if (negValid) {
            return center - neg;
        }
        return 0.0D;
    }

    private static void applyEntityImpacts(ServerLevel level,
                                           Entity source,
                                           LivingEntity owner,
                                           ExplosionEntityContext context,
                                           double impactHeatCelsius,
                                           double entityDamage,
                                           double entityKnockback,
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
                    impactHeatCelsius,
                    entityDamage,
                    entityKnockback,
                    applyWorldChanges,
                    trace
            );
        }
    }

    private static void applySingleEntityImpact(DamageSource explosionDamage,
                                                Entity entity,
                                                double accumulatedImpact,
                                                double pressureX,
                                                double pressureY,
                                                double pressureZ,
                                                double massScale,
                                                double impactHeatCelsius,
                                                double entityDamage,
                                                double entityKnockback,
                                                boolean applyWorldChanges,
                                                ExplosionProfileTrace trace) {
        if (accumulatedImpact <= ENTITY_MIN_APPLIED_IMPACT || !entity.isAlive()) {
            return;
        }

        if (entity instanceof LivingEntity livingEntity) {
            double maxHealth = Math.max(1.0D, livingEntity.getMaxHealth());
            double absorption = Math.max(0.0D, livingEntity.getAbsorptionAmount());
            double strength = ENTITY_HP_STRENGTH_BASE + ((maxHealth + absorption) * ENTITY_HP_STRENGTH_PER_HP);
            double rawDamage = accumulatedImpact / Math.max(strength, 1.0E-6D);
            double armorDivisor = 1.0D
                    + (livingEntity.getArmorValue() * ENTITY_ARMOR_DAMAGE_DIVISOR)
                    + (livingEntity.getAttributeValue(Attributes.ARMOR_TOUGHNESS) * ENTITY_TOUGHNESS_DAMAGE_DIVISOR);
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
            if (entityDamage > 0.0D) {
                boolean hurt = livingEntity.hurt(explosionDamage, (float) (finalDamage * entityDamage));
                if (hurt && trace != null) {
                    trace.entityDamaged++;
                }
                if (!livingEntity.isAlive()) {
                    if (trace != null) {
                        trace.entityKilled++;
                    }
                    return;
                }
                igniteEntityFromHeat(livingEntity, impactHeatCelsius);
            } else if (entityKnockback > 0.0D) {
                livingEntity.hurt(explosionDamage, 0f);
            }
        } else {
            if (trace != null) {
                trace.entityAffected++;
            }
            if (!applyWorldChanges) {
                return;
            }
        }

        if (entityKnockback > 0.0D) {
            double pressureMagnitudeSq = (pressureX * pressureX) + (pressureY * pressureY) + (pressureZ * pressureZ);
            if (pressureMagnitudeSq <= 1.0E-8D || !Double.isFinite(pressureMagnitudeSq)) {
                return;
            }
            double pressureMagnitude = Math.sqrt(pressureMagnitudeSq);
            double knockbackResistance = entity instanceof LivingEntity le
                    ? Mth.clamp(le.getAttributeValue(Attributes.KNOCKBACK_RESISTANCE), 0.0D, 1.0D)
                    : 0.0D;
            double knockbackStrength = Math.log1p(pressureMagnitude)
                    * ENTITY_KNOCKBACK_SCALE
                    * entityKnockback
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
    }

    private static void igniteEntityFromHeat(LivingEntity entity, double impactHeatCelsius) {
        if (impactHeatCelsius <= ENTITY_IGNITION_HEAT_CELSIUS) {
            return;
        }
        int fireSeconds = (int) Math.min(ENTITY_MAX_FIRE_SECONDS,
                (impactHeatCelsius - ENTITY_IGNITION_HEAT_CELSIUS) / ENTITY_HEAT_PER_FIRE_SECOND);
        if (fireSeconds > 0) {
            entity.setSecondsOnFire(fireSeconds);
        }
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

        int existing;
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


    private static final class ExplosionEntityContext {
        private final List<EntityHitProxy> all;

        private ExplosionEntityContext(List<EntityHitProxy> all) {
            this.all = all;
        }
    }

    private static final class EntityHitProxy {
        private final Entity entity;
        private final double massScale;
        private final double centerX;
        private final double centerY;
        private final double centerZ;
        private double accumulatedImpact;
        private double pressureX;
        private double pressureY;
        private double pressureZ;

        private EntityHitProxy(Entity entity, AABB aabb) {
            this.entity = entity;
            this.massScale = Math.max(0.75D, entity.getBbWidth() * entity.getBbHeight());
            this.centerX = (aabb.minX + aabb.maxX) * 0.5D;
            this.centerY = (aabb.minY + aabb.maxY) * 0.5D;
            this.centerZ = (aabb.minZ + aabb.maxZ) * 0.5D;
        }
    }




    private record Direction(double x, double y, double z) {
    }

    private interface FloatIndexedAccess {
        int size();

        float getFloat(int index);
    }

    private interface MutableFloatIndexedAccess extends FloatIndexedAccess {
        void setFloat(int index, float value);

        void fill(float value);
    }

    private static final class HeapFloatArrayStorage implements MutableFloatIndexedAccess {
        private final float[] values;

        private HeapFloatArrayStorage(int size) {
            this.values = new float[size];
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public float getFloat(int index) {
            return values[index];
        }

        @Override
        public void setFloat(int index, float value) {
            values[index] = value;
        }

        @Override
        public void fill(float value) {
            Arrays.fill(values, value);
        }
    }

    private static final class SparseAirSolidSlownessStorage implements MutableFloatIndexedAccess {
        private final int size;
        private final float airSlowness;
        private final BitSet activeMask;
        private final Int2FloatOpenHashMap solidSlownessByIndex;

        private SparseAirSolidSlownessStorage(int size, float airSlowness) {
            this.size = Math.max(1, size);
            this.airSlowness = airSlowness;
            this.activeMask = new BitSet(this.size);
            this.solidSlownessByIndex = new Int2FloatOpenHashMap(1024);
            this.solidSlownessByIndex.defaultReturnValue(Float.NaN);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public float getFloat(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            }
            if (!activeMask.get(index)) {
                return 0.0F;
            }
            float solidSlowness = solidSlownessByIndex.get(index);
            return Float.isFinite(solidSlowness) ? solidSlowness : airSlowness;
        }

        @Override
        public void setFloat(int index, float value) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            }
            if (!(value > 0.0F)) {
                activeMask.clear(index);
                solidSlownessByIndex.remove(index);
                return;
            }
            activeMask.set(index);
            if (Math.abs(value - airSlowness) <= 1.0E-6F) {
                solidSlownessByIndex.remove(index);
                return;
            }
            solidSlownessByIndex.put(index, value);
        }

        @Override
        public void fill(float value) {
            solidSlownessByIndex.clear();
            if (!(value > 0.0F)) {
                activeMask.clear();
                return;
            }
            activeMask.set(0, size);
            if (Math.abs(value - airSlowness) <= 1.0E-6F) {
                return;
            }
            for (int index = 0; index < size; index++) {
                solidSlownessByIndex.put(index, value);
            }
        }

        private BitSet activeMask() {
            return activeMask;
        }
    }


    private interface FloatChunk {
        float getFloat(int index);

        void setFloat(int index, float value);

        void fill(float value);
    }

    private static final class HeapFloatChunk implements FloatChunk {
        private final float[] values;

        private HeapFloatChunk(int size) {
            this.values = new float[size];
        }

        @Override
        public float getFloat(int index) {
            return values[index];
        }

        @Override
        public void setFloat(int index, float value) {
            values[index] = value;
        }

        @Override
        public void fill(float value) {
            Arrays.fill(values, value);
        }
    }

    private static final class DirectFloatChunk implements FloatChunk {
        private static final sun.misc.Unsafe UNSAFE;
        private static final long BUFFER_ADDRESS_OFFSET;
        static {
            try {
                java.lang.reflect.Field fu = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                fu.setAccessible(true);
                UNSAFE = (sun.misc.Unsafe) fu.get(null);
                java.lang.reflect.Field fa = java.nio.Buffer.class.getDeclaredField("address");
                BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(fa);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final ByteBuffer values; // held to prevent GC of native memory
        private final long address;

        private DirectFloatChunk(int size) {
            this.values = ByteBuffer.allocateDirect(size * Float.BYTES);
            this.address = UNSAFE.getLong(values, BUFFER_ADDRESS_OFFSET);
        }

        @Override
        public float getFloat(int index) {
            return UNSAFE.getFloat(address + (long) index * Float.BYTES);
        }

        @Override
        public void setFloat(int index, float value) {
            UNSAFE.putFloat(address + (long) index * Float.BYTES, value);
        }

        @Override
        public void fill(float value) {
            int limit = values.capacity() / Float.BYTES;
            for (int i = 0; i < limit; i++) {
                UNSAFE.putFloat(address + (long) i * Float.BYTES, value);
            }
        }
    }

    private static final class AdaptiveFloatArrayStorage implements MutableFloatIndexedAccess {
        // Plain array instead of ArrayList: eliminates ArrayList.get() overhead (bounds check +
        // virtual dispatch) on every getFloat/setFloat call in the solveKrakkCell hot path.
        private final FloatChunk[] chunks;
        private final int size;

        private AdaptiveFloatArrayStorage(int size, boolean preferOffHeapFirst) {
            this.size = size;
            int expectedChunks = Math.max(
                    1,
                    (size + KRAKK_OFFHEAP_FLOAT_CHUNK_SIZE - 1) / KRAKK_OFFHEAP_FLOAT_CHUNK_SIZE
            );
            this.chunks = new FloatChunk[expectedChunks];
            int remaining = size;
            for (int i = 0; i < expectedChunks; i++) {
                int chunkSize = Math.min(KRAKK_OFFHEAP_FLOAT_CHUNK_SIZE, remaining);
                chunks[i] = createAdaptiveFloatChunk(chunkSize, preferOffHeapFirst);
                remaining -= chunkSize;
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public float getFloat(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            }
            int chunkIndex = index >>> KRAKK_OFFHEAP_FLOAT_CHUNK_SHIFT;
            int chunkOffset = index & KRAKK_OFFHEAP_FLOAT_CHUNK_MASK;
            return chunks[chunkIndex].getFloat(chunkOffset);
        }

        @Override
        public void setFloat(int index, float value) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            }
            int chunkIndex = index >>> KRAKK_OFFHEAP_FLOAT_CHUNK_SHIFT;
            int chunkOffset = index & KRAKK_OFFHEAP_FLOAT_CHUNK_MASK;
            chunks[chunkIndex].setFloat(chunkOffset, value);
        }

        @Override
        public void fill(float value) {
            for (FloatChunk chunk : chunks) {
                chunk.fill(value);
            }
        }
    }

    private static final class PagedFloatArrayStorage implements MutableFloatIndexedAccess {
        private final int size;
        private final int pageCount;
        private final int maxCachedPages;
        private final float[][] cachePages;
        private final int[] cachePageIds;
        private final boolean[] cacheDirty;
        private final long[] cacheAccessTicks;
        private final Int2IntOpenHashMap pageToSlot;
        private final boolean[] persistedPages;
        private final FileChannel channel;
        private final ByteBuffer ioBuffer;
        private float defaultFillValue;
        private long accessTick;

        private PagedFloatArrayStorage(int size, int requestedCachePages) {
            this.size = Math.max(1, size);
            this.pageCount = Math.max(1, (this.size + KRAKK_PAGED_FLOAT_PAGE_SIZE - 1) / KRAKK_PAGED_FLOAT_PAGE_SIZE);
            this.maxCachedPages = Math.max(4, Math.min(this.pageCount, requestedCachePages));
            this.cachePages = new float[this.maxCachedPages][KRAKK_PAGED_FLOAT_PAGE_SIZE];
            this.cachePageIds = new int[this.maxCachedPages];
            Arrays.fill(this.cachePageIds, -1);
            this.cacheDirty = new boolean[this.maxCachedPages];
            this.cacheAccessTicks = new long[this.maxCachedPages];
            this.pageToSlot = new Int2IntOpenHashMap(Math.max(16, this.maxCachedPages * 2));
            this.pageToSlot.defaultReturnValue(-1);
            this.persistedPages = new boolean[this.pageCount];
            this.ioBuffer = ByteBuffer.allocate(KRAKK_PAGED_FLOAT_PAGE_SIZE * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            this.defaultFillValue = 0.0F;
            this.accessTick = 1L;

            Path backingFile = null;
            FileChannel openedChannel = null;
            try {
                backingFile = Files.createTempFile("krakk-float-pages-", ".bin");
                openedChannel = FileChannel.open(
                        backingFile,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                );
                PAGED_FLOAT_STORAGE_CLEANER.register(
                        this,
                        new PagedFloatStorageCleanup(openedChannel, backingFile)
                );
            } catch (IOException exception) {
                if (openedChannel != null) {
                    try {
                        openedChannel.close();
                    } catch (IOException ignored) {
                    }
                }
                if (backingFile != null) {
                    try {
                        Files.deleteIfExists(backingFile);
                    } catch (IOException ignored) {
                    }
                }
                throw new IllegalStateException("Unable to create paged float storage.", exception);
            }
            this.channel = openedChannel;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public float getFloat(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            }
            int pageIndex = index >>> KRAKK_PAGED_FLOAT_PAGE_SHIFT;
            int pageOffset = index & KRAKK_PAGED_FLOAT_PAGE_MASK;
            int slot = ensurePageLoaded(pageIndex);
            cacheAccessTicks[slot] = accessTick++;
            return cachePages[slot][pageOffset];
        }

        @Override
        public void setFloat(int index, float value) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            }
            int pageIndex = index >>> KRAKK_PAGED_FLOAT_PAGE_SHIFT;
            int pageOffset = index & KRAKK_PAGED_FLOAT_PAGE_MASK;
            int slot = ensurePageLoaded(pageIndex);
            cacheAccessTicks[slot] = accessTick++;
            cachePages[slot][pageOffset] = value;
            cacheDirty[slot] = true;
        }

        @Override
        public void fill(float value) {
            defaultFillValue = value;
            Arrays.fill(persistedPages, false);
            pageToSlot.clear();
            Arrays.fill(cachePageIds, -1);
            Arrays.fill(cacheDirty, false);
            Arrays.fill(cacheAccessTicks, 0L);
            accessTick = 1L;
            try {
                channel.truncate(0L);
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to clear paged float storage.", exception);
            }
        }

        private int ensurePageLoaded(int pageIndex) {
            int existingSlot = pageToSlot.get(pageIndex);
            if (existingSlot >= 0) {
                return existingSlot;
            }
            int slot = claimSlot();
            int previousPageIndex = cachePageIds[slot];
            if (previousPageIndex >= 0) {
                if (cacheDirty[slot]) {
                    writePage(previousPageIndex, cachePages[slot]);
                    cacheDirty[slot] = false;
                }
                pageToSlot.remove(previousPageIndex);
            }

            int length = pageLength(pageIndex);
            if (persistedPages[pageIndex]) {
                readPage(pageIndex, cachePages[slot], length);
            } else {
                Arrays.fill(cachePages[slot], 0, length, defaultFillValue);
            }
            cachePageIds[slot] = pageIndex;
            cacheAccessTicks[slot] = accessTick++;
            cacheDirty[slot] = false;
            pageToSlot.put(pageIndex, slot);
            return slot;
        }

        private int claimSlot() {
            for (int i = 0; i < maxCachedPages; i++) {
                if (cachePageIds[i] < 0) {
                    return i;
                }
            }
            int lruSlot = 0;
            long lruTick = cacheAccessTicks[0];
            for (int i = 1; i < maxCachedPages; i++) {
                long tick = cacheAccessTicks[i];
                if (tick < lruTick) {
                    lruTick = tick;
                    lruSlot = i;
                }
            }
            return lruSlot;
        }

        private void writePage(int pageIndex, float[] pageData) {
            int length = pageLength(pageIndex);
            int byteLength = length * Float.BYTES;
            ioBuffer.clear();
            ioBuffer.limit(byteLength);
            for (int i = 0; i < length; i++) {
                ioBuffer.putFloat(pageData[i]);
            }
            ioBuffer.flip();
            long writePos = pageStartOffset(pageIndex);
            try {
                while (ioBuffer.hasRemaining()) {
                    int written = channel.write(ioBuffer, writePos);
                    if (written <= 0) {
                        throw new IOException("Unable to write paged float page.");
                    }
                    writePos += written;
                }
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to write paged float storage.", exception);
            }
            persistedPages[pageIndex] = true;
        }

        private void readPage(int pageIndex, float[] pageData, int length) {
            int byteLength = length * Float.BYTES;
            ioBuffer.clear();
            ioBuffer.limit(byteLength);
            long readPos = pageStartOffset(pageIndex);
            int totalRead = 0;
            try {
                while (totalRead < byteLength) {
                    int read = channel.read(ioBuffer, readPos + totalRead);
                    if (read <= 0) {
                        break;
                    }
                    totalRead += read;
                }
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to read paged float storage.", exception);
            }
            ioBuffer.flip();
            int readableFloats = totalRead / Float.BYTES;
            for (int i = 0; i < readableFloats; i++) {
                pageData[i] = ioBuffer.getFloat();
            }
            if (readableFloats < length) {
                Arrays.fill(pageData, readableFloats, length, defaultFillValue);
            }
        }

        private int pageLength(int pageIndex) {
            int pageStart = pageIndex << KRAKK_PAGED_FLOAT_PAGE_SHIFT;
            int remaining = size - pageStart;
            return Math.max(0, Math.min(KRAKK_PAGED_FLOAT_PAGE_SIZE, remaining));
        }

        private long pageStartOffset(int pageIndex) {
            return (long) pageIndex * (long) KRAKK_PAGED_FLOAT_PAGE_SIZE * (long) Float.BYTES;
        }
    }

    private static final class PagedFloatStorageCleanup implements Runnable {
        private final FileChannel channel;
        private final Path filePath;

        private PagedFloatStorageCleanup(FileChannel channel, Path filePath) {
            this.channel = channel;
            this.filePath = filePath;
        }

        @Override
        public void run() {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
            }
            if (filePath != null) {
                try {
                    Files.deleteIfExists(filePath);
                } catch (IOException ignored) {
                }
            }
        }
    }

    @FunctionalInterface
    private interface IntIndexConsumer {
        void accept(int index);
    }

    private interface SourceIndexSet {
        boolean contains(int index);

        boolean add(int index);

        void forEach(IntIndexConsumer consumer);
    }

    private static final class BitSetSourceIndexSet implements SourceIndexSet {
        private final BitSet indices;

        private BitSetSourceIndexSet(int approximateVolume) {
            this.indices = new BitSet(Math.max(1, approximateVolume));
        }

        @Override
        public boolean contains(int index) {
            return indices.get(index);
        }

        @Override
        public boolean add(int index) {
            if (indices.get(index)) {
                return false;
            }
            indices.set(index);
            return true;
        }

        @Override
        public void forEach(IntIndexConsumer consumer) {
            if (consumer == null) {
                return;
            }
            for (int index = indices.nextSetBit(0); index >= 0; index = indices.nextSetBit(index + 1)) {
                consumer.accept(index);
            }
        }
    }

    private static final class SparseSourceIndexSet implements SourceIndexSet {
        private final IntOpenHashSet membership;
        private final IntArrayList insertionOrder;

        private SparseSourceIndexSet() {
            this.membership = new IntOpenHashSet(32);
            this.insertionOrder = new IntArrayList(32);
        }

        @Override
        public boolean contains(int index) {
            return membership.contains(index);
        }

        @Override
        public boolean add(int index) {
            if (!membership.add(index)) {
                return false;
            }
            insertionOrder.add(index);
            return true;
        }

        @Override
        public void forEach(IntIndexConsumer consumer) {
            if (consumer == null) {
                return;
            }
            for (int i = 0; i < insertionOrder.size(); i++) {
                consumer.accept(insertionOrder.getInt(i));
            }
        }
    }

    private interface LongIndexedAccess {
        int size();

        boolean isEmpty();

        long getLong(int index);
    }

    private static final class EmptyLongIndexedAccess implements LongIndexedAccess {
        private static final EmptyLongIndexedAccess INSTANCE = new EmptyLongIndexedAccess();

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long getLong(int index) {
            throw new IndexOutOfBoundsException("index=" + index + " size=0");
        }
    }

    private static final class LongArrayListIndexedAccess implements LongIndexedAccess {
        private final LongArrayList values;

        private LongArrayListIndexedAccess(LongArrayList values) {
            this.values = values;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        public long getLong(int index) {
            return values.getLong(index);
        }
    }

    private static final class PackedKrakkSolidPositions implements LongIndexedAccess {
        private final IntArrayList indices;
        private final int minX;
        private final int minY;
        private final int minZ;
        private final int strideX;
        private final int strideY;

        private PackedKrakkSolidPositions(int minX, int minY, int minZ, int sizeY, int sizeZ, int expectedEntries) {
            this.indices = new IntArrayList(Math.max(16, expectedEntries));
            this.minX = minX;
            this.minY = minY;
            this.minZ = minZ;
            this.strideX = sizeY * sizeZ;
            this.strideY = sizeZ;
        }

        private void addIndex(int index) {
            indices.add(index);
        }

        private void clear() {
            indices.clear();
        }

        @Override
        public int size() {
            return indices.size();
        }

        @Override
        public boolean isEmpty() {
            return indices.isEmpty();
        }

        @Override
        public long getLong(int index) {
            int gridIndex = indices.getInt(index);
            int xOffset = gridIndex / strideX;
            int yz = gridIndex - (xOffset * strideX);
            int yOffset = yz / strideY;
            int zOffset = yz - (yOffset * strideY);
            return BlockPos.asLong(minX + xOffset, minY + yOffset, minZ + zOffset);
        }
    }


    private enum NarrowBandWavefrontPhase {
        SEED,
        PROPAGATE,
        COMPLETE,
        FAILED
    }

    private static final class NarrowBandWavefrontJob {
        private final long jobId;
        private final ResourceKey<Level> dimension;
        private final double centerX;
        private final double centerY;
        private final double centerZ;
        private final double resolvedRadius;
        private final double radiusSq;
        private final int minX;
        private final int maxX;
        private final int minY;
        private final int maxY;
        private final int minZ;
        private final int maxZ;
        private final double totalEnergy;
        private final double impactHeatCelsius;
        private final UUID sourceUuid;
        private final UUID ownerUuid;
        private final boolean applyWorldChanges;
        private final ExplosionProfileTrace trace;
        private final boolean phaseLoggingEnabled;
        private final long phaseTraceId;
        private final long queuedAtNanos;

        private NarrowBandWavefrontPhase phase;
        private String failureMessage;
        private Entity cachedSource;
        private LivingEntity cachedOwner;

        private final Int2DoubleOpenHashMap resistanceCostCache = new Int2DoubleOpenHashMap(256);
        private final WaveChunkStateCache chunkStateCache = new WaveChunkStateCache();
        private final BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        private final Long2FloatOpenHashMap slownessByPos = new Long2FloatOpenHashMap(2048);
        private final Long2FloatOpenHashMap arrivalByPos = new Long2FloatOpenHashMap(4096);
        private final LongOpenHashSet finalized = new LongOpenHashSet(4096);
        private final WaveLongMinHeap trialQueue = new WaveLongMinHeap(256);

        private int sourceCount;
        private double maxArrival;
        private long solveStartNanos;
        private long impactApplyStartNanos;
        private double impactBudget;
        private double directWeight;
        private int acceptedNodes;
        private int poppedNodes;
        private int directImpactApplications;
        private int thermalImpactApplications;
        private long lastProgressLogNanos;

        private NarrowBandWavefrontJob(long jobId,
                                       ResourceKey<Level> dimension,
                                       double centerX,
                                       double centerY,
                                       double centerZ,
                                       double resolvedRadius,
                                       double radiusSq,
                                       int minX,
                                       int maxX,
                                       int minY,
                                       int maxY,
                                       int minZ,
                                       int maxZ,
                                       double totalEnergy,
                                       double impactHeatCelsius,
                                       UUID sourceUuid,
                                       UUID ownerUuid,
                                       boolean applyWorldChanges,
                                       ExplosionProfileTrace trace,
                                       boolean phaseLoggingEnabled,
                                       long phaseTraceId,
                                       long queuedAtNanos) {
            this.jobId = jobId;
            this.dimension = dimension;
            this.centerX = centerX;
            this.centerY = centerY;
            this.centerZ = centerZ;
            this.resolvedRadius = resolvedRadius;
            this.radiusSq = radiusSq;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.minZ = minZ;
            this.maxZ = maxZ;
            this.totalEnergy = totalEnergy;
            this.impactHeatCelsius = impactHeatCelsius;
            this.sourceUuid = sourceUuid;
            this.ownerUuid = ownerUuid;
            this.applyWorldChanges = applyWorldChanges;
            this.trace = trace;
            this.phaseLoggingEnabled = phaseLoggingEnabled;
            this.phaseTraceId = phaseTraceId;
            this.queuedAtNanos = queuedAtNanos;
            this.phase = NarrowBandWavefrontPhase.SEED;
            this.failureMessage = null;
            this.lastProgressLogNanos = queuedAtNanos;

            this.resistanceCostCache.defaultReturnValue(Double.NaN);
            this.slownessByPos.defaultReturnValue(Float.NaN);
            this.arrivalByPos.defaultReturnValue(Float.POSITIVE_INFINITY);
        }

        private long jobId() {
            return jobId;
        }

        private ResourceKey<Level> dimension() {
            return dimension;
        }

        private double centerX() {
            return centerX;
        }

        private double centerY() {
            return centerY;
        }

        private double centerZ() {
            return centerZ;
        }

        private double resolvedRadius() {
            return resolvedRadius;
        }

        private double radiusSq() {
            return radiusSq;
        }

        private int minX() {
            return minX;
        }

        private int maxX() {
            return maxX;
        }

        private int minY() {
            return minY;
        }

        private int maxY() {
            return maxY;
        }

        private int minZ() {
            return minZ;
        }

        private int maxZ() {
            return maxZ;
        }

        private double totalEnergy() {
            return totalEnergy;
        }

        private double impactHeatCelsius() {
            return impactHeatCelsius;
        }

        private boolean applyWorldChanges() {
            return applyWorldChanges;
        }

        private ExplosionProfileTrace trace() {
            return trace;
        }

        private boolean phaseLoggingEnabled() {
            return phaseLoggingEnabled;
        }

        private long phaseTraceId() {
            return phaseTraceId;
        }

        private long queuedAtNanos() {
            return queuedAtNanos;
        }

        private long lastProgressLogNanos() {
            return lastProgressLogNanos;
        }

        private void lastProgressLogNanos(long value) {
            this.lastProgressLogNanos = value;
        }

        private NarrowBandWavefrontPhase phase() {
            return phase;
        }

        private void setPhase(NarrowBandWavefrontPhase phase) {
            this.phase = phase == null ? NarrowBandWavefrontPhase.FAILED : phase;
        }

        private String failureMessage() {
            return failureMessage;
        }

        private void failureMessage(String message) {
            this.failureMessage = message;
        }

        private Int2DoubleOpenHashMap resistanceCostCache() {
            return resistanceCostCache;
        }

        private WaveChunkStateCache chunkStateCache() {
            return chunkStateCache;
        }

        private BlockPos.MutableBlockPos mutablePos() {
            return mutablePos;
        }

        private Long2FloatOpenHashMap slownessByPos() {
            return slownessByPos;
        }

        private Long2FloatOpenHashMap arrivalByPos() {
            return arrivalByPos;
        }

        private LongOpenHashSet finalized() {
            return finalized;
        }

        private WaveLongMinHeap trialQueue() {
            return trialQueue;
        }

        private int sourceCount() {
            return sourceCount;
        }

        private void sourceCount(int sourceCount) {
            this.sourceCount = sourceCount;
        }

        private double maxArrival() {
            return maxArrival;
        }

        private void maxArrival(double maxArrival) {
            this.maxArrival = maxArrival;
        }

        private long solveStartNanos() {
            return solveStartNanos;
        }

        private void solveStartNanos(long solveStartNanos) {
            this.solveStartNanos = solveStartNanos;
        }

        private long impactApplyStartNanos() {
            return impactApplyStartNanos;
        }

        private void impactApplyStartNanos(long impactApplyStartNanos) {
            this.impactApplyStartNanos = impactApplyStartNanos;
        }

        private Entity resolveSource(ServerLevel level) {
            if (sourceUuid == null) {
                return null;
            }
            if (cachedSource != null && !cachedSource.isRemoved()) {
                return cachedSource;
            }
            Entity resolved = level.getEntity(sourceUuid);
            if (resolved != null && !resolved.isRemoved()) {
                cachedSource = resolved;
                return resolved;
            }
            cachedSource = null;
            return null;
        }

        private LivingEntity resolveOwner(ServerLevel level) {
            if (ownerUuid == null) {
                return null;
            }
            if (cachedOwner != null && !cachedOwner.isRemoved()) {
                return cachedOwner;
            }
            Entity resolved = level.getEntity(ownerUuid);
            if (resolved instanceof LivingEntity living && !living.isRemoved()) {
                cachedOwner = living;
                return living;
            }
            cachedOwner = null;
            return null;
        }
    }


    private static final class WaveChunkStateCache {
        private final Long2ObjectOpenHashMap<LevelChunk> chunksByPos = new Long2ObjectOpenHashMap<>();

        private BlockState getBlockState(ServerLevel level, BlockPos.MutableBlockPos mutablePos, int x, int y, int z) {
            int chunkX = x >> 4;
            int chunkZ = z >> 4;
            long chunkKey = ChunkPos.asLong(chunkX, chunkZ);
            LevelChunk chunk = chunksByPos.get(chunkKey);
            if (chunk == null) {
                chunk = level.getChunk(chunkX, chunkZ);
                chunksByPos.put(chunkKey, chunk);
            }
            mutablePos.set(x, y, z);
            return chunk.getBlockState(mutablePos);
        }
    }

    private static final class WaveLongMinHeap {
        private long[] keys;
        private double[] priorities;
        private int size;
        private double polledPriority;

        private WaveLongMinHeap(int initialCapacity) {
            int capacity = Math.max(16, initialCapacity);
            this.keys = new long[capacity];
            this.priorities = new double[capacity];
            this.size = 0;
            this.polledPriority = Double.POSITIVE_INFINITY;
        }

        private boolean isEmpty() {
            return size <= 0;
        }

        private int size() {
            return size;
        }

        private void add(long key, double priority) {
            ensureCapacity(size + 1);
            int cursor = size++;
            while (cursor > 0) {
                int parent = (cursor - 1) >>> 1;
                if (priority >= priorities[parent]) {
                    break;
                }
                keys[cursor] = keys[parent];
                priorities[cursor] = priorities[parent];
                cursor = parent;
            }
            keys[cursor] = key;
            priorities[cursor] = priority;
        }

        private long pollKey() {
            long rootKey = keys[0];
            polledPriority = priorities[0];
            size--;
            if (size > 0) {
                long tailKey = keys[size];
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
                    keys[cursor] = keys[child];
                    priorities[cursor] = priorities[child];
                    cursor = child;
                }
                keys[cursor] = tailKey;
                priorities[cursor] = tailPriority;
            }
            return rootKey;
        }

        private double pollPriority() {
            return polledPriority;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity <= keys.length) {
                return;
            }
            int newCapacity = Math.max(minCapacity, keys.length << 1);
            keys = Arrays.copyOf(keys, newCapacity);
            priorities = Arrays.copyOf(priorities, newCapacity);
        }
    }

    private record WavefrontSolveStats(int acceptedNodes, int poppedNodes) {
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

    private static final class KrakkDeltaBucketQueue {
        private final double bucketWidth;
        private final Int2ObjectOpenHashMap<IntArrayList> buckets;
        private final PriorityQueue<Integer> bucketOrder;
        private final IntOpenHashSet queuedBuckets;
        private final BitSet pendingEntries;

        private KrakkDeltaBucketQueue(double bucketWidth, int indexCapacity) {
            this.bucketWidth = Math.max(1.0E-9D, bucketWidth);
            this.buckets = new Int2ObjectOpenHashMap<>();
            this.bucketOrder = new PriorityQueue<>();
            this.queuedBuckets = new IntOpenHashSet();
            this.pendingEntries = new BitSet(Math.max(1, indexCapacity));
        }

        private boolean isEmpty() {
            return buckets.isEmpty();
        }

        private int bucketCount() {
            return buckets.size();
        }

        private void add(int index, double arrival) {
            if (index < 0) {
                return;
            }
            if (pendingEntries.get(index)) {
                return;
            }
            int bucketIndex = krakkDeltaBucketIndex(arrival, bucketWidth);
            IntArrayList entries = buckets.get(bucketIndex);
            if (entries == null) {
                entries = new IntArrayList(16);
                buckets.put(bucketIndex, entries);
            }
            entries.add(index);
            pendingEntries.set(index);
            if (queuedBuckets.add(bucketIndex)) {
                bucketOrder.add(bucketIndex);
            }
        }

        private boolean claim(int index) {
            if (index < 0 || !pendingEntries.get(index)) {
                return false;
            }
            pendingEntries.clear(index);
            return true;
        }

        private int pollBucketIndex() {
            while (!bucketOrder.isEmpty()) {
                int bucketIndex = bucketOrder.poll();
                queuedBuckets.remove(bucketIndex);
                IntArrayList entries = buckets.get(bucketIndex);
                if (entries != null && !entries.isEmpty()) {
                    return bucketIndex;
                }
            }
            return -1;
        }

        private IntArrayList takeBucket(int bucketIndex) {
            return buckets.remove(bucketIndex);
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

    private record ResistanceFieldSnapshot(BlockState[] sampledStates, int sampledVoxelCount) {
    }

    private record KrakkResistanceFieldChunkResult(
            IntArrayList solidIndices,
            int sampledVoxelCount
    ) {
    }

    private record KrakkTargetScanResult(
            LongArrayList targetPositions,
            FloatArrayList targetWeights,
            double solidWeight,
            double maxWeight,
            long precheckNanos
    ) {
    }

    private record KrakkVolumetricBaselineResult(
            float[] baselineByIndex,
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
            FloatIndexedAccess slowness,
            LongIndexedAccess solidPositions,
            int[] activeRowLengths
    ) {
    }

    private record KrakkCoarseSolveContext(
            KrakkField coarseField,
            MutableFloatIndexedAccess coarseArrivalTimes,
            MutableFloatIndexedAccess coarseSlowness,
            SourceIndexSet coarseSourceMask,
            int coarseSourceCount,
            int downsampleFactor
    ) {
    }

    private record KrakkSolveResult(MutableFloatIndexedAccess arrivalTimes, int sourceCells, int sweepCycles) {
    }

    private record PairedKrakkSolveResult(KrakkSolveResult normal, KrakkSolveResult shadow) {
    }

    private record PairedKrakkSlownessResult(FloatIndexedAccess normalSlowness, FloatIndexedAccess shadowSlowness) {
    }

    private record PairedKrakkSourceResult(int normalSourceCount, int shadowSourceCount) {
    }

    private enum KrakkArrivalSolver {
        SWEEP,
        DELTA_STEPPING,
        ORDERED_UPWIND
    }

    private record RuntimeExecutionPolicy(
            boolean compactSourceTracking,
            boolean forceDirectResistanceSampling,
            int maxMultiresVolume,
            KrakkArrivalSolver arrivalSolver
    ) {
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
                resolution.impactHeatCelsius,
                resolution.blastTransmittance,
                profile.entityDamage(),
                profile.entityKnockback(),
                profile.blockDamage(),
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
                resolution.impactHeatCelsius,
                resolution.blastTransmittance,
                profile.entityDamage(),
                profile.entityKnockback(),
                profile.blockDamage(),
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
            return new ExplosionResolution(resolvedRadius, resolvedEnergy, KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS, KrakkExplosionProfile.DEFAULT_BLAST_TRANSMITTANCE);
        }

        double fallbackPower = KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
        double resolvedRadius = sanitizeKrakkRadius(profile.radius(), fallbackPower);
        double resolvedEnergy = sanitizeKrakkEnergy(profile.energy(), fallbackPower);
        double resolvedImpactHeatCelsius = sanitizeImpactHeatCelsius(profile.impactHeatCelsius());
        return new ExplosionResolution(resolvedRadius, resolvedEnergy, resolvedImpactHeatCelsius, profile.blastTransmittance());
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
        // Use energy-driven blast calibration for the envelope radius so delta-stepping stays on
        // the same power scale as baseline Krakk explosions and remains practically solvable.
        double normalizedEnergy = Math.max(totalEnergy, VOLUMETRIC_MIN_ENERGY);
        double calibratedRadius = KrakkExplosionCurves.computeBlastRadius(normalizedEnergy);
        if (!Double.isFinite(calibratedRadius)) {
            return 1.0D;
        }
        return Math.max(1.0D, calibratedRadius);
    }

    private static double sanitizeKrakkEnergy(double energy, double fallbackPower) {
        if (Double.isFinite(energy) && energy > VOLUMETRIC_MIN_ENERGY) {
            return energy;
        }
        return Math.max(VOLUMETRIC_MIN_ENERGY, fallbackPower);
    }

    private static double sanitizeImpactHeatCelsius(double impactHeatCelsius) {
        if (!Double.isFinite(impactHeatCelsius)) {
            return KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS;
        }
        return impactHeatCelsius;
    }

    private record ThermalTargetShape(LongArrayList positions, Long2FloatOpenHashMap weightsByPos) {
    }

    private record StreamingImpactSample(double impactPower, double weight) {
        private static final StreamingImpactSample NONE = new StreamingImpactSample(0.0D, 0.0D);
    }

    private record ExplosionResolution(double radius, double energy, double impactHeatCelsius, double blastTransmittance) {
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
        @SuppressWarnings("unused") // emitRaySmoke is intentionally disabled for Cannonical; always 0 but kept for trace reporting
        private int smokeParticles;
    }
}
