package org.shipwrights.krakk.runtime.explosion;

import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.core.BlockPos;
import net.minecraft.core.particles.ParticleTypes;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.sounds.SoundEvents;
import net.minecraft.sounds.SoundSource;
import net.minecraft.util.Mth;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.TntBlock;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.api.explosion.KrakkExplosionApi;
import org.shipwrights.krakk.api.explosion.KrakkExplosionProfile;
import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;
import org.shipwrights.krakk.engine.explosion.KrakkRaySplitMath;

import java.util.ArrayDeque;

public final class KrakkExplosionRuntime implements KrakkExplosionApi {
    private static final int MIN_RAY_COUNT = 640;
    private static final int MAX_RAY_COUNT = 2048;
    private static final double DEFAULT_RAY_COUNT = 960.0D;
    private static final double GOLDEN_ANGLE = Math.PI * (3.0D - Math.sqrt(5.0D));
    private static final double RAY_STEP = 0.3D;
    private static final double RAY_DECAY_PER_STEP = RAY_STEP * 0.45D;
    private static final double FLUID_RAY_DAMPING = 0.90D;
    private static final double BLOCK_RESISTANCE_DAMPING = 0.17D;
    private static final double BLOCK_BASE_DAMPING = 0.08D;
    private static final double RAY_IMPACT_SCALE = 0.0012D;
    private static final double RAY_SPLIT_DISTANCE_THRESHOLD = 1.0D;
    private static final double RAY_SPLIT_HALF_ANGLE_RADIANS = Math.toRadians(30.0D);
    private static final double RAY_SPLIT_VARIANCE_RADIANS = Math.toRadians(5.0D);
    private static final int MAX_RAY_SPLIT_DEPTH = 8;
    private static final double MIN_RAY_SPLIT_ENERGY = 0.35D;
    private static final double MIN_RESOLVED_RAY_IMPACT = 1.0E-4D;
    private static final double RAY_AA_SPREAD_SHARE = 0.16D;
    private static final double RAY_AA_MIN_IMPACT = 1.0E-3D;
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
    private static volatile double impactPower = KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
    private static volatile SpecialBlockHandler specialBlockHandler = SpecialBlockHandler.NOOP;

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

    public static void detonateGunpowderBarrel(ServerLevel level, double x, double y, double z, Entity source, LivingEntity barrelOwner) {
        double blastRadius = getBlastRadius();
        double blastPower = getImpactPower();

        level.playSound(
                null,
                x,
                y,
                z,
                SoundEvents.GENERIC_EXPLODE,
                SoundSource.BLOCKS,
                1.0F,
                0.95F + (level.random.nextFloat() * 0.1F)
        );
        level.sendParticles(ParticleTypes.EXPLOSION_EMITTER, x, y, z, 1, 0.0D, 0.0D, 0.0D, 0.0D);

        Long2DoubleOpenHashMap blockImpacts = collectRaycastImpacts(level, x, y, z, blastRadius, blastPower);
        for (Long2DoubleMap.Entry entry : blockImpacts.long2DoubleEntrySet()) {
            BlockPos blockPos = BlockPos.of(entry.getLongKey());
            if (!level.isInWorldBounds(blockPos)) {
                continue;
            }
            BlockState blockState = level.getBlockState(blockPos);
            if (blockState.isAir()) {
                continue;
            }

            if (specialBlockHandler.handle(level, blockPos, blockState, source, barrelOwner)) {
                continue;
            }
            if (blockState.is(Blocks.TNT)) {
                TntBlock.explode(level, blockPos);
                level.removeBlock(blockPos, false);
                continue;
            }

            double resolvedImpactPower = entry.getDoubleValue();
            if (resolvedImpactPower <= MIN_RESOLVED_RAY_IMPACT) {
                continue;
            }

            KrakkApi.damage().applyImpact(level, blockPos, blockState, source, resolvedImpactPower, false);
            // Raycast pass is authoritative in this mode; no post-smoothing pass.
        }
    }

    private static Long2DoubleOpenHashMap collectRaycastImpacts(ServerLevel level, double centerX, double centerY, double centerZ,
                                                                 double blastRadius, double blastPower) {
        Long2DoubleOpenHashMap impacts = new Long2DoubleOpenHashMap();
        int rayCount = KrakkExplosionCurves.computeRayCount(blastRadius, DEFAULT_RAY_COUNT, MIN_RAY_COUNT, MAX_RAY_COUNT);
        double baseAngularSpacing = Math.sqrt((4.0D * Math.PI) / Math.max(1, rayCount));
        int smokeRays = Mth.clamp((int) Math.round(rayCount * RAY_SMOKE_RAY_FRACTION), RAY_SMOKE_MIN_RAYS, RAY_SMOKE_MAX_RAYS);
        int smokeRayStride = Math.max(1, rayCount / Math.max(1, smokeRays));
        int smokeBudget = Mth.clamp(
                (int) Math.round(blastRadius * RAY_SMOKE_BUDGET_PER_RADIUS),
                RAY_SMOKE_MIN_BUDGET,
                RAY_SMOKE_MAX_BUDGET
        );
        ArrayDeque<RaycastState> rayQueue = new ArrayDeque<>(rayCount * 2);

        for (int i = 0; i < rayCount; i++) {
            double t = (i + 0.5D) / rayCount;
            double dy = 1.0D - (2.0D * t);
            double radial = Math.sqrt(Math.max(0.0D, 1.0D - (dy * dy)));
            double theta = i * GOLDEN_ANGLE;
            double dx = Math.cos(theta) * radial;
            double dz = Math.sin(theta) * radial;

            double rayEnergy = blastRadius * (0.8D + (level.random.nextDouble() * 0.6D));
            boolean emitSmokeOnRay = (i % smokeRayStride) == 0;
            rayQueue.addLast(new RaycastState(
                    centerX, centerY, centerZ,
                    dx, dy, dz,
                    rayEnergy,
                    Long.MIN_VALUE,
                    0,
                    0,
                    emitSmokeOnRay
            ));
        }

        while (!rayQueue.isEmpty()) {
            RaycastState ray = rayQueue.removeFirst();
            while (ray.energy > 0.0D) {
                BlockPos currentPos = BlockPos.containing(ray.x, ray.y, ray.z);
                if (!level.isInWorldBounds(currentPos)) {
                    break;
                }

                if (shouldSplitRay(ray, centerX, centerY, centerZ, baseAngularSpacing)) {
                    splitRay(level, ray, rayQueue);
                    break;
                }

                long currentPosLong = currentPos.asLong();
                if (currentPosLong != ray.lastPosLong) {
                    if (ray.emitSmokeOnRay && smokeBudget > 0 && (ray.stepIndex % RAY_SMOKE_STEP_INTERVAL) == 0) {
                        emitRaySmoke(level, ray.x, ray.y, ray.z);
                        smokeBudget--;
                    }

                    BlockState state = level.getBlockState(currentPos);
                    boolean hasFluid = !level.getFluidState(currentPos).isEmpty();
                    if (!state.isAir()) {
                        double normalizedEnergy = ray.energy / Math.max(blastRadius, 1.0E-6D);
                        if (!hasFluid) {
                            double impact = blastPower * normalizedEnergy * RAY_IMPACT_SCALE;
                            if (impact > MIN_RESOLVED_RAY_IMPACT) {
                                impacts.addTo(currentPosLong, impact);
                            }
                        }

                        double resistance = Math.max(0.0D, state.getBlock().getExplosionResistance());
                        double resistanceCost = (Math.pow(resistance + 0.3D, 0.78D) * BLOCK_RESISTANCE_DAMPING) + BLOCK_BASE_DAMPING;
                        ray.energy -= resistanceCost;
                    } else if (hasFluid) {
                        ray.energy -= FLUID_RAY_DAMPING;
                    }

                    ray.lastPosLong = currentPosLong;
                }

                ray.x += ray.dirX * RAY_STEP;
                ray.y += ray.dirY * RAY_STEP;
                ray.z += ray.dirZ * RAY_STEP;
                ray.energy -= RAY_DECAY_PER_STEP;
                ray.stepIndex++;
            }
        }

        Long2DoubleOpenHashMap antiAliasedImpacts = applyLightRaycastAntialiasing(level, impacts);
        double maxImpact = blastPower * MAX_BLOCK_IMPACT_MULTIPLIER;
        for (Long2DoubleMap.Entry entry : antiAliasedImpacts.long2DoubleEntrySet()) {
            if (entry.getDoubleValue() > maxImpact) {
                entry.setValue(maxImpact);
            }
        }

        return antiAliasedImpacts;
    }

    private static Long2DoubleOpenHashMap applyLightRaycastAntialiasing(ServerLevel level, Long2DoubleOpenHashMap source) {
        if (source.isEmpty() || RAY_AA_SPREAD_SHARE <= 0.0D) {
            return source;
        }

        Long2DoubleOpenHashMap result = new Long2DoubleOpenHashMap(Math.max(source.size() * 2, 16));
        BlockPos.MutableBlockPos mutablePos = new BlockPos.MutableBlockPos();
        long[] neighborKeys = new long[6];
        double[] neighborWeights = new double[6];

        for (Long2DoubleMap.Entry entry : source.long2DoubleEntrySet()) {
            double impact = entry.getDoubleValue();
            if (impact <= 0.0D) {
                continue;
            }

            BlockPos pos = BlockPos.of(entry.getLongKey());
            if (!level.isInWorldBounds(pos)) {
                continue;
            }

            double spreadShare = impact >= RAY_AA_MIN_IMPACT ? RAY_AA_SPREAD_SHARE : 0.0D;
            double retainedImpact = impact * (1.0D - spreadShare);
            result.addTo(pos.asLong(), retainedImpact);

            if (spreadShare <= 0.0D) {
                continue;
            }

            int candidateCount = 0;
            double weightTotal = 0.0D;
            candidateCount = collectAaNeighbor(level, mutablePos, neighborKeys, neighborWeights, candidateCount, pos.getX() + 1, pos.getY(), pos.getZ());
            candidateCount = collectAaNeighbor(level, mutablePos, neighborKeys, neighborWeights, candidateCount, pos.getX() - 1, pos.getY(), pos.getZ());
            candidateCount = collectAaNeighbor(level, mutablePos, neighborKeys, neighborWeights, candidateCount, pos.getX(), pos.getY() + 1, pos.getZ());
            candidateCount = collectAaNeighbor(level, mutablePos, neighborKeys, neighborWeights, candidateCount, pos.getX(), pos.getY() - 1, pos.getZ());
            candidateCount = collectAaNeighbor(level, mutablePos, neighborKeys, neighborWeights, candidateCount, pos.getX(), pos.getY(), pos.getZ() + 1);
            candidateCount = collectAaNeighbor(level, mutablePos, neighborKeys, neighborWeights, candidateCount, pos.getX(), pos.getY(), pos.getZ() - 1);
            for (int i = 0; i < candidateCount; i++) {
                weightTotal += neighborWeights[i];
            }

            double spreadImpact = impact - retainedImpact;
            if (candidateCount <= 0 || weightTotal <= 1.0E-8D) {
                result.addTo(pos.asLong(), spreadImpact);
                continue;
            }

            for (int i = 0; i < candidateCount; i++) {
                result.addTo(neighborKeys[i], spreadImpact * (neighborWeights[i] / weightTotal));
            }
        }

        return result;
    }

    private static int collectAaNeighbor(ServerLevel level, BlockPos.MutableBlockPos mutablePos,
                                         long[] neighborKeys, double[] neighborWeights, int index,
                                         int x, int y, int z) {
        mutablePos.set(x, y, z);
        if (!level.isInWorldBounds(mutablePos)) {
            return index;
        }

        BlockState neighborState = level.getBlockState(mutablePos);
        if (neighborState.isAir() || neighborState.getDestroySpeed(level, mutablePos) < 0.0F) {
            return index;
        }
        if (!level.getFluidState(mutablePos).isEmpty()) {
            return index;
        }

        if (index >= neighborKeys.length) {
            return index;
        }

        neighborKeys[index] = mutablePos.asLong();
        neighborWeights[index] = 1.0D;
        return index + 1;
    }

    private static boolean shouldSplitRay(RaycastState ray, double centerX, double centerY, double centerZ, double baseAngularSpacing) {
        return KrakkRaySplitMath.shouldSplitRay(
                ray.splitDepth,
                MAX_RAY_SPLIT_DEPTH,
                ray.energy,
                MIN_RAY_SPLIT_ENERGY,
                ray.x, ray.y, ray.z,
                centerX, centerY, centerZ,
                baseAngularSpacing,
                RAY_SPLIT_DISTANCE_THRESHOLD
        );
    }

    private static void splitRay(ServerLevel level, RaycastState parent, ArrayDeque<RaycastState> queue) {
        double splitEnergy = parent.energy * 0.5D;
        if (splitEnergy <= 0.0D) {
            return;
        }

        Direction axis = perpendicularAxis(parent.dirX, parent.dirY, parent.dirZ);
        double leftAngle = RAY_SPLIT_HALF_ANGLE_RADIANS
                + ((level.random.nextDouble() * 2.0D - 1.0D) * RAY_SPLIT_VARIANCE_RADIANS);
        double rightAngle = -RAY_SPLIT_HALF_ANGLE_RADIANS
                + ((level.random.nextDouble() * 2.0D - 1.0D) * RAY_SPLIT_VARIANCE_RADIANS);
        Direction left = rotateAroundAxis(parent.dirX, parent.dirY, parent.dirZ, axis, leftAngle);
        Direction right = rotateAroundAxis(parent.dirX, parent.dirY, parent.dirZ, axis, rightAngle);
        int childDepth = parent.splitDepth + 1;

        queue.addFirst(new RaycastState(
                parent.x, parent.y, parent.z,
                right.x, right.y, right.z,
                splitEnergy,
                parent.lastPosLong,
                parent.stepIndex,
                childDepth,
                parent.emitSmokeOnRay
        ));
        queue.addFirst(new RaycastState(
                parent.x, parent.y, parent.z,
                left.x, left.y, left.z,
                splitEnergy,
                parent.lastPosLong,
                parent.stepIndex,
                childDepth,
                parent.emitSmokeOnRay
        ));
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

    private static void emitRaySmoke(ServerLevel level, double x, double y, double z) {
        double jitterX = (level.random.nextDouble() - 0.5D) * RAY_SMOKE_JITTER;
        double jitterY = (level.random.nextDouble() - 0.5D) * RAY_SMOKE_JITTER;
        double jitterZ = (level.random.nextDouble() - 0.5D) * RAY_SMOKE_JITTER;
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

    private static final class RaycastState {
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

        private RaycastState(double x, double y, double z,
                             double dirX, double dirY, double dirZ,
                             double energy, long lastPosLong, int stepIndex, int splitDepth,
                             boolean emitSmokeOnRay) {
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
        }
    }

    private record Direction(double x, double y, double z) {
    }

    public static void setImpactPower(double newImpactPower) {
        impactPower = KrakkExplosionCurves.sanitizeImpactPower(newImpactPower);
    }

    public static double getImpactPower() {
        return impactPower;
    }

    public static double getDefaultImpactPower() {
        return KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
    }

    public static double getBlastRadius() {
        return KrakkExplosionCurves.computeBlastRadius(impactPower);
    }

    @Override
    public void detonate(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner, KrakkExplosionProfile profile) {
        if (profile == null) {
            detonateGunpowderBarrel(level, x, y, z, source, owner);
            return;
        }

        double previousPower = getImpactPower();
        setImpactPower(profile.impactPower());
        try {
            detonateGunpowderBarrel(level, x, y, z, source, owner);
        } finally {
            setImpactPower(previousPower);
        }
    }

    @Override
    public void setPower(double impactPower) {
        setImpactPower(impactPower);
    }

    @Override
    public double getPower() {
        return getImpactPower();
    }

    @Override
    public double getDefaultPower() {
        return getDefaultImpactPower();
    }

    @Override
    public double getRadius() {
        return getBlastRadius();
    }
}
