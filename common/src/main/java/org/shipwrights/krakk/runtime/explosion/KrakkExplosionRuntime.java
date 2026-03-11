package org.shipwrights.krakk.runtime.explosion;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
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
import java.util.Comparator;
import java.util.List;

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
    private static final int[] CHILD_TRAVERSAL_OFFSETS = new int[]{0, 1, 2, 4, 3, 5, 6, 7};
    private static final int[][] CHILD_ORDER_BY_RAY_OCTANT = buildChildOrderByRayOctant();
    private static volatile SpecialBlockHandler specialBlockHandler = SpecialBlockHandler.NOOP;
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
            int estimatedSyncPackets,
            int estimatedSyncBytes,
            int smokeParticles
    ) {
    }

    private static void detonate(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner,
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
            KrakkImpactResult result = KrakkApi.damage().applyImpact(level, mutablePos, blockState, source, resolvedImpactPower, false);
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

    @Override
    public void detonate(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner, KrakkExplosionProfile profile) {
        ExplosionResolution resolution = resolveProfile(profile);
        detonate(
                level,
                x,
                y,
                z,
                source,
                owner,
                resolution.radius,
                resolution.power,
                level.random,
                true,
                true,
                null
        );
    }

    public ExplosionProfileReport profileDetonate(ServerLevel level, double x, double y, double z, Entity source,
                                                  LivingEntity owner, KrakkExplosionProfile profile, boolean applyWorldChanges, long seed) {
        ExplosionResolution resolution = resolveProfile(profile);
        ExplosionProfileTrace trace = new ExplosionProfileTrace();
        RandomSource random = RandomSource.create(seed);
        long start = System.nanoTime();
        detonate(
                level,
                x,
                y,
                z,
                source,
                owner,
                resolution.radius,
                resolution.power,
                random,
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
                trace.syncPacketsEstimated,
                trace.syncBytesEstimated,
                trace.smokeParticles
        );
    }

    private static ExplosionResolution resolveProfile(KrakkExplosionProfile profile) {
        double resolvedPower;
        if (profile == null) {
            resolvedPower = KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
        } else {
            resolvedPower = KrakkExplosionCurves.sanitizeImpactPower(profile.impactPower());
        }
        double resolvedRadius = KrakkExplosionCurves.computeBlastRadius(resolvedPower);
        return new ExplosionResolution(resolvedRadius, resolvedPower);
    }

    private record ExplosionResolution(double radius, double power) {
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
        private int syncPacketsEstimated;
        private int syncBytesEstimated;
        private int smokeParticles;
    }
}
