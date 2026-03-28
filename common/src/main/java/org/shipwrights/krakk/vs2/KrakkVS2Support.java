package org.shipwrights.krakk.vs2;

import com.mojang.logging.LogUtils;
import org.jetbrains.annotations.Nullable;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import net.minecraft.core.BlockPos;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import org.joml.Matrix4dc;
import org.joml.Vector3d;
import org.joml.Vector3dc;
import org.joml.primitives.AABBd;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.api.damage.KrakkDamageType;
import org.shipwrights.krakk.api.damage.KrakkImpactResult;
import org.shipwrights.krakk.runtime.explosion.KrakkExplosionRuntime;
import org.shipwrights.krakk.state.chunk.KrakkBlockDamageSectionAccess;
import org.valkyrienskies.core.api.physics.ContactPoint;
import org.valkyrienskies.core.api.ships.PhysShip;
import org.valkyrienskies.core.api.ships.Ship;
import org.valkyrienskies.core.api.ships.properties.ChunkClaim;
import org.valkyrienskies.mod.api.ValkyrienSkies;
import org.valkyrienskies.mod.common.ValkyrienSkiesMod;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;

/**
 * Optional VS2 integration for Krakk's explosion and damage systems.
 *
 * <p>All VS2 classes are {@code modCompileOnly}; this class is only instantiated
 * after {@link #isPresent()} confirms that VS2 is on the runtime classpath.
 *
 * <p>Registers three hooks with {@link KrakkExplosionRuntime}:
 * <ol>
 *   <li>{@link KrakkExplosionRuntime.BlockStateProvider} — injects the real shipyard block state
 *       for world-air positions that are occupied by a VS2 ship block. This lets the wavefront
 *       propagate through ship hulls with their actual blast resistance so ships cast physically
 *       correct shadows and the wave energy at each hit block is accurate.</li>
 *   <li>{@link KrakkExplosionRuntime.SpecialBlockHandler} — intercepts the block impact that
 *       would otherwise modify the world; redirects structural damage to shipyard coordinates and
 *       applies per-block impulse forces to the ship.</li>
 *   <li>Pre/post explosion hooks — store explosion center for force direction, then sync
 *       ship damage states to nearby players.</li>
 * </ol>
 */
public final class KrakkVS2Support {

    private static final Logger LOGGER = LogUtils.getLogger();

    // Per-block knockback force scale.
    // VS2's MixinExplosion applies ~250 000 N per hit block (explosionBlastForce 500 000
    // × distanceMult 0.5) via applyWorldForceToModelPos at each block's position.
    // Krakk impact power for a TNT-equivalent (power 25) direct hit is on the order of ~25
    // units, so BLOCK_FORCE_SCALE ≈ 250 000 / 25 = 10 000 gives comparable magnitudes.
    // Only the knockback-fraction of each block's impact power is multiplied by this constant;
    // the damage-fraction goes to structural damage instead.
    private static final double BLOCK_FORCE_SCALE = 20_000.0; // N per Krakk impact power unit

    /**
     * Thread-local storing the current explosion center (x, y, z) so that the
     * SpecialBlockHandler can compute per-block force direction without an additional parameter.
     * Written by the PreExplosionHook before any block iteration begins.
     */
    private static final ThreadLocal<double[]> CURRENT_EXPLOSION_CENTER =
            ThreadLocal.withInitial(() -> new double[3]);

    // Collision damage tuning.
    // Minimum relative velocity (m/s) below which no damage is applied.
    // Resting contact between bodies produces small but nonzero velocities — this
    // threshold filters that noise so only genuine impacts cause block damage.
    private static final double COLLISION_MIN_VELOCITY  = 0.5;
    // Converts VS2 kinetic energy (kg·blocks²/s²) → Krakk impactPower.
    // Tuning: 1000 kg ship at 5 m/s → KE=12500; 4 contacts → 3125/contact.
    // With massFactor=1.75 (stone), power = 3125/(100*1.75) ≈ 17.9 — significant hit.
    private static final double ENERGY_TO_POWER_SCALE   = 100.0;
    // Multiplied by block hardness to get massFactor. Higher = harder blocks take much less damage.
    // Combined with HARDNESS_CURVE for quadratic scaling: massFactor = 1 + h*scale*(1 + h*curve).
    private static final double HARDNESS_MASS_SCALE     = 0.5;
    // Quadratic hardness curve: makes strong blocks disproportionately tougher while
    // leaving weak blocks (glass, wood) nearly unchanged.
    // glass(0.3): ×1.16, wood(2): ×2.4, stone(1.5): ×1.97, iron(5): ×6.0, obsidian(50): ×276
    private static final double HARDNESS_CURVE          = 0.2;
    // Reference blast resistance for the transmission formula.
    // stone(6) transmits 33%, glass(0.3) transmits 91%, obsidian(1200) transmits 0.25%.
    private static final double PROPAGATION_BASE        = 3.0;
    // Energy-scaled propagation depth. Below this energy only the contact block is damaged.
    // Above it, depth grows logarithmically: depth = floor(log(energy / threshold) / log(step)).
    private static final double PROPAGATION_DEPTH_ENERGY_THRESHOLD = 2000.0;
    // Each multiplication of energy by this factor adds one depth layer.
    private static final double PROPAGATION_DEPTH_ENERGY_STEP      = 5.0;
    // Hard cap on propagation depth regardless of energy (performance guard).
    private static final int    MAX_PROPAGATION_DEPTH               = 10;
    // Raymarch grid size: hemisphere rays are cast from an N×N×6-face grid (surface cells only).
    // 6 → ~96 hemisphere rays; keeps cost bounded while giving decent angular coverage.
    private static final int    PROPAGATION_RAY_GRID    = 6;
    // Distance between ray sample points (blocks). Matches vanilla explosion step size.
    private static final double PROPAGATION_RAY_STEP    = 0.3;
    // When a ray damages a block, if the sample point is within this distance of a block
    // boundary, a fraction of damage bleeds into the adjacent block (antialiasing).
    private static final double PROPAGATION_AA_THRESHOLD = 0.3;
    // Fraction of the ray's energy applied to the adjacent block during antialiasing.
    private static final double PROPAGATION_AA_FRACTION  = 0.15;
    // Early-exit: energy below this threshold causes no meaningful Krakk damage.
    private static final double MIN_PROPAGATION_ENERGY  = 0.5;
    // Maximum contact points processed per collision event (performance guard).
    private static final int    COLLISION_MAX_CONTACTS  = 4;
    // Radius used when syncing ship damage to nearby players after a collision.
    private static final double COLLISION_SYNC_RADIUS  = 8.0;
    // Minimum game ticks between two damage applications for the same collision pair.
    // The persist event fires every physics tick (~60 Hz); this cooldown prevents
    // sustained contact from accumulating damage every tick.
    // 20 ticks = 1 second at 20 TPS.
    private static final long COLLISION_DAMAGE_COOLDOWN_TICKS = 20L;

    private record SnapshotContactPoint(
        double px, double py, double pz,
        double nx, double ny, double nz,
        double vx, double vy, double vz
    ) {
        double speed() { return Math.sqrt(vx * vx + vy * vy + vz * vz); }
    }

    private record CollisionSnapshot(
        String dimensionId,
        long shipIdA, long shipIdB,
        double massA, double massB,
        List<SnapshotContactPoint> contactPoints
    ) {}

    /**
     * A deferred GTPA force, scheduled to fire on a specific game tick so the physics engine
     * has time to process block removals before the follow-through impulse arrives.
     */
    private record DeferredForce(
        long fireTick,
        String dimensionId,
        long shipId,
        double fx, double fy, double fz
    ) {}

    /**
     * Collision snapshots queued from the physics thread, drained once per server tick on the
     * game thread.  Mirrors the ConcurrentLinkedQueue pattern from VS-Collision-damage.
     */
    private static final ConcurrentLinkedQueue<CollisionSnapshot> COLLISION_QUEUE =
            new ConcurrentLinkedQueue<>();

    /**
     * Breakthrough forces deferred by one game tick so the physics engine processes the
     * block removal before the follow-through impulse is applied.
     * Written and read exclusively on the game thread during {@link #drainCollisions}.
     */
    private static final java.util.ArrayDeque<DeferredForce> DEFERRED_FORCES =
            new java.util.ArrayDeque<>();

    /**
     * Tracks the last game tick at which damage was applied for each (shipIdA, shipIdB) pair.
     * Key = (shipIdA << 32 | shipIdB) encoded as a long; value = last damage tick.
     * Read/written exclusively on the game thread during drainCollisions.
     */
    private static final ConcurrentHashMap<Long, Long> COLLISION_DAMAGE_COOLDOWNS =
            new ConcurrentHashMap<>();

    private KrakkVS2Support() {
    }

    /**
     * Returns {@code true} when VS2 classes are present on the runtime classpath.
     * Safe to call before {@link #init()}.
     */
    public static boolean isPresent() {
        try {
            Class.forName("org.valkyrienskies.mod.api.ValkyrienSkies");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Returns {@code true} when VS2 is present AND the active physics backend is KRUNCH_CLASSIC.
     * Features that rely on VS2's internal Krunch physics entity API (e.g. barrel physics)
     * must gate on this rather than {@link #isPresent()} alone, because those APIs are
     * incompatible with PhysX or Jolt backends.
     *
     * <p>Queries the live VS2 pipeline via {@code VSGameUtilsKt.getVsPipeline(server).getPhysicsBackendType()}.
     * Uses reflection for the return-type chain because {@code VsiPipeline} and
     * {@code ConfigPhysicsBackendType} live in {@code vs-core:internal/impl} which are not on the
     * compile classpath. Defaults to {@code true} when the server or pipeline is unavailable
     * (e.g. called during world load before the pipeline is up).
     */
    public static boolean isKrunchClassicBackend() {
        try {
            net.minecraft.server.MinecraftServer server = ValkyrienSkiesMod.getCurrentServer();
            if (server == null) return true;
            // VSGameUtilsKt.getVsPipeline(server) → VsiPipeline (vs-core:internal, not on compile CP)
            // Invoke via reflection to avoid a compile-time dependency on the internal module.
            Object pipeline = org.valkyrienskies.mod.common.VSGameUtilsKt.class
                    .getMethod("getVsPipeline", net.minecraft.server.MinecraftServer.class)
                    .invoke(null, server);
            if (pipeline == null) return true;
            // VsiPipeline.getPhysicsBackendType() → ConfigPhysicsBackendType (vs-core:impl)
            Object backendType = pipeline.getClass().getMethod("getPhysicsBackendType").invoke(pipeline);
            return "KRUNCH_CLASSIC".equals(backendType.toString());
        } catch (Exception e) {
            LOGGER.warn("KrakkVS2Support: could not determine physics backend, assuming KRUNCH_CLASSIC: {}", e.getMessage());
            return true;
        }
    }

    /**
     * Registers all VS2 hooks with {@link KrakkExplosionRuntime}.
     * Must only be called after confirming {@link #isPresent()}.
     */
    public static void init() {
        KrakkExplosionRuntime.SpecialBlockHandler existingHandler =
                KrakkExplosionRuntime.getSpecialBlockHandler();

        // Hook 1: inject real ship block states into the wave solver.
        // WaveChunkStateCache calls this provider for every block it encounters. Returning the
        // actual shipyard block state for world-air positions that are occupied by a ship block
        // lets the Krakk wavefront slow down and attenuate correctly through ship hulls.
        // Returning null for non-air positions (real world blocks) and for ship positions that
        // have no block in the shipyard preserves normal world behavior everywhere else.
        KrakkExplosionRuntime.setBlockStateProvider((level, pos, worldState) -> {
            if (!worldState.isAir()) return null;
            // When multiple ships overlap, return the most resistant block so the
            // wavefront attenuates through the toughest hull at this position.
            BlockState best = null;
            float bestResistance = -1.0f;
            for (Ship ship : getShipsAt(level, pos)) {
                BlockState realState = getShipBlockState(level, ship, pos);
                if (realState == null || realState.isAir()) continue;
                float res = realState.getBlock().getExplosionResistance();
                if (best == null || res > bestResistance) {
                    best = realState;
                    bestResistance = res;
                }
            }
            return best;
        });

        // Hook 2: Ship block damage + per-block knockback.
        // Intercepts the block impact before Krakk applies world-side effects, redirecting
        // damage to the shipyard block and applying a physics force to the ship body.
        // Explosion energy at each block is split between structural damage and ship impulse:
        //   • blast resistance governs how much wavefront pressure is deflected into the ship
        //     (higher resistance → more knockback).
        //   • hardness governs how much energy is absorbed as structural damage
        //     (higher strength → more damage taken).
        // effectiveState is the real ship block state returned by the BlockStateProvider above;
        // it can be used for resistance/hardness lookups but blast resistance is re-fetched
        // from the shipyard block so we always use the authoritative server-side value.
        // Force is applied at the near face (the face that faces the explosion) so that
        // off-center hits produce realistic torque.
        KrakkExplosionRuntime.setSpecialBlockHandler((level, pos, effectiveState, source, owner) -> {
            if (existingHandler.handle(level, pos, effectiveState, source, owner)) return true;

            boolean anyShipHit = false;
            for (Ship ship : getShipsAt(level, pos)) {
                // Re-fetch from shipyard for authoritative block state (provider result may be stale
                // if a block was destroyed between wavefront propagation and impact application).
                BlockState realState = getShipBlockState(level, ship, pos);
                if (realState == null || realState.isAir()) continue; // ship present but slot empty

                anyShipHit = true;
                double impactPower = KrakkExplosionRuntime.currentBlockImpactPower();
                double impactHeat  = KrakkExplosionRuntime.currentBlockImpactHeat();

                // Energy split: blast resistance → knockback, hardness → damage.
                double blastRes = Math.max(0.0, realState.getBlock().getExplosionResistance());
                double hardness = Math.max(0.0, realState.getDestroySpeed(level, pos));
                double divisor  = blastRes + hardness + 1.0; // +1 prevents full 100% knockback for weak blocks
                double knockbackFrac = blastRes / divisor;
                double damageFrac    = hardness / divisor;

                // Structural damage (reduced by fraction going to knockback).
                applyShipBlockDamage(level, ship, pos, source, damageFrac * impactPower, impactHeat,
                        KrakkDamageType.KRAKK_DAMAGE_EXPLOSION);

                // Per-block impulse applied at the near face (face of the block facing the explosion).
                double forceMag = knockbackFrac * impactPower * BLOCK_FORCE_SCALE;
                if (forceMag < 1e-9) continue;

                double[] center = CURRENT_EXPLOSION_CENTER.get();
                double cx = center[0], cy = center[1], cz = center[2];
                double bx = pos.getX() + 0.5, by = pos.getY() + 0.5, bz = pos.getZ() + 0.5;
                double dx = bx - cx, dy = by - cy, dz = bz - cz;
                double dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
                if (dist < 1e-6) continue;
                double invDist = 1.0 / dist;
                // Unit wavefront normal: explosion → block.
                double nx = dx * invDist, ny = dy * invDist, nz = dz * invDist;
                // Near face = block center offset 0.5 blocks toward explosion (-N direction).
                double applyX = bx - 0.5 * nx;
                double applyY = by - 0.5 * ny;
                double applyZ = bz - 0.5 * nz;

                Vector3d force = new Vector3d(nx * forceMag, ny * forceMag, nz * forceMag);
                if (!force.isFinite()) continue;

                String shipyardDimId = ship.getChunkClaimDimension();
                ValkyrienSkiesMod.getOrCreateGTPA(shipyardDimId)
                        .applyWorldForce(ship.getId(), force, new Vector3d(applyX, applyY, applyZ));
            }
            return anyShipHit;
        });

        // Hook 3a: Store explosion center before block iteration so the SpecialBlockHandler
        // can compute per-block force direction without an additional parameter.
        KrakkExplosionRuntime.setPreExplosionHook(
                (level, x, y, z, resolvedRadius, resolvedEnergy, profile, source, owner) -> {
                    double[] center = CURRENT_EXPLOSION_CENTER.get();
                    center[0] = x; center[1] = y; center[2] = z;
                });

        // Hook 3b: Post-explosion damage sync — send shipyard section snapshots to nearby
        // overworld players so the crack overlay renders on the client.
        KrakkExplosionRuntime.setPostExplosionHook(
                (level, x, y, z, resolvedRadius, resolvedEnergy, profile, source, owner) -> {
                    if (resolvedRadius <= 0.0) return;
                    syncShipDamageToPlayers(level, x, y, z, resolvedRadius);
                });

        // Hook 4: VS2 collisionStart → queue for game-thread processing.
        // Events are captured on the physics thread into COLLISION_QUEUE and drained
        // once per server tick in drainCollisions().  This matches the pattern used by
        // VS-Collision-damage and avoids any per-event server.execute() overhead.
        //
        // NOTE on contact normal convention:
        // VS2/Krunch's normal is the outward normal of body B (points away from body B,
        // toward body A).  Therefore:
        //   posA = contact + normal * nudge   (along normal → into body A's geometry)
        //   posB = contact - normal * nudge   (opposite normal → into body B's geometry)
        LOGGER.info("KrakkVS2Support: registering collision event listener");
        try {
            ValkyrienSkies.api().getCollisionStartEvent().on(ev -> {
                PhysShip physA = ev.getPhysLevel().getShipById(ev.getShipIdA());
                PhysShip physB = ev.getPhysLevel().getShipById(ev.getShipIdB());
                double massA = (physA != null) ? physA.getMass() : Double.POSITIVE_INFINITY;
                double massB = (physB != null) ? physB.getMass() : Double.POSITIVE_INFINITY;
                if (Double.isInfinite(massA) && Double.isInfinite(massB)) return;
                if (physA != null && massA < 1.0) return;
                if (physB != null && massB < 1.0) return;

                List<SnapshotContactPoint> points = new java.util.ArrayList<>();
                int count = 0;
                for (ContactPoint cp : ev.getContactPoints()) {
                    if (count++ >= COLLISION_MAX_CONTACTS) break;
                    Vector3dc p = cp.getPosition(), n = cp.getNormal(), v = cp.getVelocity();
                    points.add(new SnapshotContactPoint(
                            p.x(), p.y(), p.z(), n.x(), n.y(), n.z(), v.x(), v.y(), v.z()));
                }
                if (points.isEmpty()) return;
                COLLISION_QUEUE.add(new CollisionSnapshot(
                        ev.getDimensionId(), ev.getShipIdA(), ev.getShipIdB(), massA, massB, points));
            });
            LOGGER.info("KrakkVS2Support: collision event listeners registered successfully");
        } catch (Exception e) {
            LOGGER.error("KrakkVS2Support: failed to register collision event listeners", e);
        }
    }

    // -------------------------------------------------------------------------
    // Block state / coordinate utilities
    // -------------------------------------------------------------------------

    /**
     * Returns the VS2 {@link Ship} whose world-space AABB contains the given block position,
     * or {@code null} if none.
     *
     * <p>{@code getShipManagingBlock} does a chunk-coordinate lookup and only matches chunks
     * that are part of a ship's claim (far-offset shipyard coords). World-space positions
     * near the explosion map to ordinary chunk indices, so that API always returns null here.
     * {@code getShipsIntersecting} is a world-space spatial query and correctly identifies
     * which ship visually occupies a given world position.
     */
    private static Iterable<Ship> getShipsAt(Level level, BlockPos worldPos) {
        return ValkyrienSkies.getShipsIntersecting(
                level, worldPos.getX() + 0.5, worldPos.getY() + 0.5, worldPos.getZ() + 0.5);
    }

    /**
     * Returns the first ship at the given world position, or null.
     * Use {@link #getShipsAt} when all overlapping ships must be processed.
     */
    @Nullable
    private static Ship getShipAt(Level level, BlockPos worldPos) {
        for (Ship ship : getShipsAt(level, worldPos)) {
            return ship;
        }
        return null;
    }

    /**
     * Transforms a world-space block position to the ship-local (Shipyard) position and
     * returns the block state stored there, or {@code null} if the shipyard level is unavailable.
     */
    private static BlockState getShipBlockState(ServerLevel level, Ship ship, BlockPos worldPos) {
        BlockPos shipyardPos = worldToShipyard(ship, worldPos);
        ServerLevel shipyardLevel = getShipyardLevel(level, ship);
        if (shipyardLevel == null) return null;
        return shipyardLevel.getBlockState(shipyardPos);
    }

    /**
     * Converts a world-space block position into the corresponding block position in
     * the ship's Shipyard chunk claim using the ship's world-to-model transform.
     */
    private static BlockPos worldToShipyard(Ship ship, BlockPos worldPos) {
        Matrix4dc toModel = ship.getTransform().getToModel();
        Vector3d local = toModel.transformPosition(
                worldPos.getX() + 0.5,
                worldPos.getY() + 0.5,
                worldPos.getZ() + 0.5,
                new Vector3d());
        return BlockPos.containing(local.x, local.y, local.z);
    }

    /**
     * Retrieves the {@link ServerLevel} for the Shipyard dimension that owns this ship's blocks.
     */
    private static ServerLevel getShipyardLevel(ServerLevel level, Ship ship) {
        String targetDimId = ship.getChunkClaimDimension();
        for (ServerLevel candidate : level.getServer().getAllLevels()) {
            if (targetDimId.equals(ValkyrienSkies.getDimensionId(candidate))) {
                return candidate;
            }
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Block damage
    // -------------------------------------------------------------------------

    /**
     * Applies Krakk block damage to the ship block at the Shipyard position that
     * corresponds to the given world-space position.
     *
     * @param impactPower the (already energy-split) impact power to deliver — caller is
     *                    responsible for multiplying by the damage fraction before passing.
     * @param impactHeat  the impact heat in °C.
     * @param damageType  the Krakk damage type to report ({@link KrakkDamageType#KRAKK_DAMAGE_EXPLOSION}
     *                    for wavefront hits, {@link KrakkDamageType#KRAKK_DAMAGE_COLLISION} for VS2 impacts).
     */
    private static void applyShipBlockDamage(ServerLevel level, Ship ship, BlockPos worldPos,
                                              Entity source, double impactPower, double impactHeat,
                                              KrakkDamageType damageType) {
        ServerLevel shipyardLevel = getShipyardLevel(level, ship);
        if (shipyardLevel == null) {
            LOGGER.info("KrakkVS2Support: shipyard level null for ship {}", ship.getId());
            return;
        }

        BlockPos shipyardPos = worldToShipyard(ship, worldPos);
        BlockState shipyardState = shipyardLevel.getBlockState(shipyardPos);
        if (shipyardState.isAir()) {
            shipyardPos = nearestNonAir(shipyardLevel, shipyardPos);
            if (shipyardPos == null) return;
            shipyardState = shipyardLevel.getBlockState(shipyardPos);
        }
        LOGGER.info("KrakkVS2Support: hit world={} → shipyard={} block={}",
                worldPos, shipyardPos, shipyardState.getBlock());

        KrakkApi.damage().applyImpact(
                shipyardLevel,
                shipyardPos,
                shipyardState,
                source,
                impactPower,
                impactHeat,
                false, // ship blocks do not drop items when damaged
                damageType);
    }

    // -------------------------------------------------------------------------
    // Collision damage
    // -------------------------------------------------------------------------

    /**
     * Drains {@link #COLLISION_QUEUE} and processes each queued collision snapshot on the
     * game thread.  Must be called once per server tick (at END phase).
     */
    public static void drainCollisions(MinecraftServer server) {
        // First, fire any deferred breakthrough forces whose tick has arrived.
        long currentTick = server.getTickCount();
        while (!DEFERRED_FORCES.isEmpty() && DEFERRED_FORCES.peek().fireTick() <= currentTick) {
            DeferredForce df = DEFERRED_FORCES.poll();
            ValkyrienSkiesMod.getOrCreateGTPA(df.dimensionId())
                    .applyWorldForce(df.shipId(), new Vector3d(df.fx(), df.fy(), df.fz()), null);
            LOGGER.info("KrakkVS2Support: fired deferred breakthrough force on ship {} = ({}, {}, {})",
                    df.shipId(), df.fx(), df.fy(), df.fz());
        }

        // Then process new collision snapshots.
        CollisionSnapshot snapshot;
        while ((snapshot = COLLISION_QUEUE.poll()) != null) {
            processCollisionDamage(snapshot, server);
        }
    }

    /**
     * Game-thread handler for a single collision snapshot.
     * Computes kinetic energy from reduced mass and impact velocity, then propagates
     * damage inward from each contact point using per-block material properties.
     */
    private static void processCollisionDamage(CollisionSnapshot snapshot, MinecraftServer server) {
        long shipIdA = snapshot.shipIdA();
        long shipIdB = snapshot.shipIdB();

        // Per-pair cooldown: sort IDs so (A,B) and (B,A) share the same entry.
        long lo = Math.min(shipIdA, shipIdB);
        long hi = Math.max(shipIdA, shipIdB);
        long pairKey = (lo * 0x9e3779b97f4a7c15L) ^ hi;
        long currentTick = server.getTickCount();
        Long lastTick = COLLISION_DAMAGE_COOLDOWNS.get(pairKey);
        if (lastTick != null && (currentTick - lastTick) < COLLISION_DAMAGE_COOLDOWN_TICKS) {
            return;
        }
        COLLISION_DAMAGE_COOLDOWNS.put(pairKey, currentTick);

        ServerLevel level = findLevelByDimId(server, snapshot.dimensionId());
        if (level == null) {
            LOGGER.info("KrakkVS2Support: no level for dimensionId={}", snapshot.dimensionId());
            return;
        }

        double maxSpeed = 0.0;
        for (SnapshotContactPoint cp : snapshot.contactPoints()) {
            double s = cp.speed();
            if (s > maxSpeed) maxSpeed = s;
        }
        if (maxSpeed < COLLISION_MIN_VELOCITY) return;

        boolean bodyAIsWorld = Double.isInfinite(snapshot.massA());
        boolean bodyBIsWorld = Double.isInfinite(snapshot.massB());

        LOGGER.info("KrakkVS2Support: collision damage shipA={} shipB={} vel={} worldA={} worldB={}",
                shipIdA, shipIdB, maxSpeed, bodyAIsWorld, bodyBIsWorld);

        boolean anyShipDamaged = false;
        double sumX = 0.0, sumY = 0.0, sumZ = 0.0;

        // Resolve game-thread Ship references once (null when body is the world).
        Ship shipA = bodyAIsWorld ? null : ValkyrienSkies.getShipById(level, shipIdA);
        Ship shipB = bodyBIsWorld ? null : ValkyrienSkies.getShipById(level, shipIdB);

        if (!bodyAIsWorld && shipA == null) {
            LOGGER.warn("KrakkVS2Support: body A (id={}) has finite mass but getShipById returned null", shipIdA);
        }
        if (!bodyBIsWorld && shipB == null) {
            LOGGER.warn("KrakkVS2Support: body B (id={}) has finite mass but getShipById returned null", shipIdB);
        }

        // Reduced mass: if one body is the world (infinite mass) use the ship mass directly.
        double massA = snapshot.massA();
        double massB = snapshot.massB();
        double mu = Double.isInfinite(massA) ? massB
                  : Double.isInfinite(massB) ? massA
                  : (massA * massB) / (massA + massB);

        int contactCount = snapshot.contactPoints().size();
        double perContact = 0.5 * mu * maxSpeed * maxSpeed / contactCount;

        // Accumulate breakthrough energy per body — when a contact block breaks,
        // the surviving ship should receive a follow-through push.
        double breakthroughFromA = 0.0; // energy freed by blocks breaking in body A
        double breakthroughFromB = 0.0; // energy freed by blocks breaking in body B

        for (SnapshotContactPoint cp : snapshot.contactPoints()) {
            BlockPos contactPos = BlockPos.containing(cp.px(), cp.py(), cp.pz());
            double nx = cp.nx(), ny = cp.ny(), nz = cp.nz();

            LOGGER.info("KrakkVS2Support: contact cp=({}, {}, {}) normal=({}, {}, {}) blockPos={} energy={}",
                    cp.px(), cp.py(), cp.pz(), nx, ny, nz, contactPos, perContact);

            // Propagate damage into body A (along +normal = into A).
            if (bodyAIsWorld) {
                breakthroughFromA += propagateWorldDamage(level, contactPos, nx, ny, nz, perContact);
            } else if (shipA != null) {
                breakthroughFromA += propagateShipDamage(level, shipA, contactPos, nx, ny, nz, perContact);
                anyShipDamaged = true;
            }

            // Propagate damage into body B (along −normal = into B).
            if (bodyBIsWorld) {
                breakthroughFromB += propagateWorldDamage(level, contactPos, -nx, -ny, -nz, perContact);
            } else if (shipB != null) {
                breakthroughFromB += propagateShipDamage(level, shipB, contactPos, -nx, -ny, -nz, perContact);
                anyShipDamaged = true;
            }

            sumX += cp.px(); sumY += cp.py(); sumZ += cp.pz();
        }

        // Breakthrough force disabled — block-mass energy model makes follow-through
        // pushes unnecessary for now.

        int cpCount = snapshot.contactPoints().size();
        if (anyShipDamaged && cpCount > 0) {
            syncShipDamageToPlayers(level,
                    sumX / cpCount, sumY / cpCount, sumZ / cpCount,
                    COLLISION_SYNC_RADIUS);
        }
    }

    /**
     * Queues a deferred follow-through force on a ship for the next game tick.
     * The one-tick delay ensures the physics engine has processed the block removal
     * before the impulse arrives, so the ship pushes through the gap cleanly.
     *
     * <p>Restores the ship's original impact velocity along the contact normal.
     * {@code F = mass × velocity × physicsTPS} gives the exact impulse needed to
     * impart that velocity in one physics tick, canceling the bounce.
     *
     * @param server   current server (for tick count)
     * @param ship     the ship to push
     * @param cp       a representative contact point (used for velocity)
     * @param nx,ny,nz direction the ship should be pushed (unit-length, into the broken body)
     * @param shipMass mass of the ship being pushed
     */
    private static void applyBreakthroughForce(MinecraftServer server, Ship ship,
                                                SnapshotContactPoint cp,
                                                double nx, double ny, double nz,
                                                double shipMass) {
        if (shipMass <= 0.0) return;

        // Project the relative velocity onto the push direction to get the impact speed.
        // Velocity is A−B; the ship we're pushing could be either A or B, so take the
        // absolute value of the projection to get speed regardless of sign convention.
        double impactSpeed = Math.abs(cp.vx() * nx + cp.vy() * ny + cp.vz() * nz);
        if (impactSpeed <= 0.0) return;

        // F = m * v * physicsTPS  →  exact impulse-equivalent for one physics tick.
        double physicsTPS = 60.0;
        double forceMag = shipMass * impactSpeed * physicsTPS;

        double fx = nx * forceMag, fy = ny * forceMag, fz = nz * forceMag;
        if (!Double.isFinite(fx) || !Double.isFinite(fy) || !Double.isFinite(fz)) return;

        long fireTick = server.getTickCount() + 1;
        DEFERRED_FORCES.add(new DeferredForce(
                fireTick, ship.getChunkClaimDimension(), ship.getId(), fx, fy, fz));

        LOGGER.info("KrakkVS2Support: queued breakthrough force on ship {} speed={} force=({}, {}, {}) fireTick={}",
                ship.getId(), impactSpeed, fx, fy, fz, fireTick);
    }

    /**
     * Computes propagation depth from collision energy. Below the threshold only
     * the contact block is damaged (depth 0). Each multiplication of energy by
     * {@link #PROPAGATION_DEPTH_ENERGY_STEP} adds one layer, up to {@link #MAX_PROPAGATION_DEPTH}.
     */
    private static int propagationDepthForEnergy(double energy) {
        if (energy < PROPAGATION_DEPTH_ENERGY_THRESHOLD) return 0;
        int depth = (int) (Math.log(energy / PROPAGATION_DEPTH_ENERGY_THRESHOLD)
                         / Math.log(PROPAGATION_DEPTH_ENERGY_STEP));
        return Math.min(Math.max(depth, 0) + 1, MAX_PROPAGATION_DEPTH);
    }

    /**
     * Applies damage to a single block and returns whether it was broken and
     * its transmission fraction (how much energy passes through).
     */
    private record BlockDamageResult(boolean broken, double transmissionFraction) {}

    private static BlockDamageResult damageBlock(ServerLevel level, BlockPos pos, double energy) {
        BlockState state = level.getBlockState(pos);
        if (state.isAir()) return new BlockDamageResult(false, 1.0);

        float rawHardness = state.getDestroySpeed(level, pos);
        // Negative hardness = unbreakable (bedrock, barriers, command blocks).
        if (rawHardness < 0.0f) return new BlockDamageResult(false, 0.0);

        double hardness = Math.max(0.0, rawHardness);
        double resistance = Math.max(0.0, state.getBlock().getExplosionResistance());


        // Hardness (structural density) reduces collision damage; blast resistance
        // only governs propagation (how much energy passes through to the next layer).
        // Quadratic curve: weak blocks (glass 0.3, wood 2) are barely affected;
        // strong blocks (iron 5, obsidian 50) get disproportionately more protection.
        double massFactor = 1.0 + hardness * HARDNESS_MASS_SCALE * (1.0 + hardness * HARDNESS_CURVE);
        double impactPower = energy / (ENERGY_TO_POWER_SCALE * massFactor);

        KrakkImpactResult result = KrakkApi.damage().applyImpact(level, pos, state, null,
                impactPower, KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS,
                false, KrakkDamageType.KRAKK_DAMAGE_COLLISION);

        double transmissionFraction = PROPAGATION_BASE / (resistance + PROPAGATION_BASE);
        return new BlockDamageResult(result.broken(), transmissionFraction);
    }

    /**
     * Propagates collision energy into a ship hull using radial raymarch.
     * Casts hemisphere rays from the contact point inward along the propagation normal,
     * stepping through air and damaging the first solid block each ray hits.
     *
     * @return breakthrough energy when the contact block was destroyed, 0.0 otherwise.
     */
    private static double propagateShipDamage(ServerLevel level, Ship ship,
                                              BlockPos contactWorldPos,
                                              double nx, double ny, double nz,
                                              double energy) {
        if (energy < MIN_PROPAGATION_ENERGY) return 0.0;
        ServerLevel shipyardLevel = getShipyardLevel(level, ship);
        if (shipyardLevel == null) return 0.0;

        // Transform the propagation normal from world space to shipyard space.
        Vector3d shipNormal = ship.getTransform().getToModel()
                .transformDirection(new Vector3d(nx, ny, nz));
        double snx = shipNormal.x, sny = shipNormal.y, snz = shipNormal.z;
        double len = Math.sqrt(snx * snx + sny * sny + snz * snz);
        if (len > 0.0) { snx /= len; sny /= len; snz /= len; }

        // Resolve starting position in shipyard coords.
        BlockPos startPos = worldToShipyard(ship, contactWorldPos);
        BlockState startState = shipyardLevel.getBlockState(startPos);
        LOGGER.info("KrakkVS2Support: ship propagate normal=({},{},{}) shipNormal=({},{},{}) worldPos={} shipyardPos={} state={}",
                nx, ny, nz, snx, sny, snz, contactWorldPos, startPos, startState);
        if (startState.isAir()) {
            BlockPos fallback = nearestNonAir(shipyardLevel, startPos);
            LOGGER.info("KrakkVS2Support: ship nearestNonAir from {} = {}", startPos,
                    fallback != null ? fallback + " state=" + shipyardLevel.getBlockState(fallback) : "null");
            startPos = fallback;
            if (startPos == null) return 0.0;
        }

        // Depth 0: damage the contact block.
        BlockDamageResult contactResult = damageBlock(shipyardLevel, startPos, energy);
        LOGGER.info("KrakkVS2Support: ship depth0 damage at {} energy={} broken={}", startPos, energy, contactResult.broken());
        double breakthrough = contactResult.broken()
                ? energy * contactResult.transmissionFraction() : 0.0;

        // Radial raymarch propagation.
        int maxDepth = propagationDepthForEnergy(energy);
        if (maxDepth > 0) {
            double propagationEnergy = energy * contactResult.transmissionFraction();
            castHemisphereRays(shipyardLevel,
                    startPos.getX() + 0.5, startPos.getY() + 0.5, startPos.getZ() + 0.5,
                    snx, sny, snz, propagationEnergy, maxDepth);
        }

        return breakthrough;
    }

    /**
     * Propagates collision energy into world blocks using radial raymarch.
     * Casts hemisphere rays from the contact point inward along the propagation normal,
     * stepping through air and damaging the first solid block each ray hits.
     *
     * @return breakthrough energy when the contact block was destroyed, 0.0 otherwise.
     */
    private static double propagateWorldDamage(ServerLevel level, BlockPos contactPos,
                                               double nx, double ny, double nz,
                                               double energy) {
        if (energy < MIN_PROPAGATION_ENERGY) return 0.0;

        // Nudge the contact position along the propagation normal (into the world).
        // VS2 contact points sit at the collision surface, so BlockPos.containing()
        // can land inside the ship's rendered hull — which is air in world-space.
        BlockPos startPos = BlockPos.containing(
                contactPos.getX() + 0.5 + nx * 0.5,
                contactPos.getY() + 0.5 + ny * 0.5,
                contactPos.getZ() + 0.5 + nz * 0.5);
        BlockState startState = level.getBlockState(startPos);
        LOGGER.info("KrakkVS2Support: world propagate normal=({},{},{}) contactPos={} nudgedPos={} nudgedState={}",
                nx, ny, nz, contactPos, startPos, startState);
        if (startState.isAir()) {
            // Fall back to the original position, then nearest non-air.
            startState = level.getBlockState(contactPos);
            if (!startState.isAir()) {
                startPos = contactPos;
                LOGGER.info("KrakkVS2Support: world fallback to contactPos={} state={}", contactPos, startState);
            } else {
                startPos = nearestNonAir(level, contactPos);
                LOGGER.info("KrakkVS2Support: world nearestNonAir from {} = {}", contactPos,
                        startPos != null ? startPos + " state=" + level.getBlockState(startPos) : "null");
                if (startPos == null) return 0.0;
            }
        }

        // Depth 0: damage the contact block.
        BlockDamageResult contactResult = damageBlock(level, startPos, energy);
        LOGGER.info("KrakkVS2Support: world depth0 damage at {} energy={} broken={}", startPos, energy, contactResult.broken());
        double breakthrough = contactResult.broken()
                ? energy * contactResult.transmissionFraction() : 0.0;

        // Radial raymarch propagation.
        int maxDepth = propagationDepthForEnergy(energy);
        if (maxDepth > 0) {
            double propagationEnergy = energy * contactResult.transmissionFraction();
            castHemisphereRays(level,
                    startPos.getX() + 0.5, startPos.getY() + 0.5, startPos.getZ() + 0.5,
                    nx, ny, nz, propagationEnergy, maxDepth);
        }

        return breakthrough;
    }

    /**
     * Casts rays in a hemisphere oriented along {@code (nx,ny,nz)} from the given center,
     * similar to vanilla explosion mechanics. Each ray steps at {@link #PROPAGATION_RAY_STEP}
     * intervals, traversing freely through air. The first solid block hit by each ray is
     * damaged, with light antialiasing bleeding a fraction of energy into edge-adjacent blocks.
     *
     * <p>Ray directions are generated from the surface cells of an N×N×N cube (like vanilla's
     * 16×16×16 grid but smaller), filtered to the hemisphere facing the propagation normal.
     *
     * @param level  the level to damage blocks in (shipyard or overworld)
     * @param cx     ray origin X (block center)
     * @param cy     ray origin Y (block center)
     * @param cz     ray origin Z (block center)
     * @param nx     propagation normal X
     * @param ny     propagation normal Y
     * @param nz     propagation normal Z
     * @param energy total propagation energy (split evenly across hemisphere rays)
     * @param maxRange maximum ray travel distance in blocks
     */
    private static void castHemisphereRays(ServerLevel level,
                                            double cx, double cy, double cz,
                                            double nx, double ny, double nz,
                                            double energy, int maxRange) {
        if (energy < MIN_PROPAGATION_ENERGY) return;
        int n = PROPAGATION_RAY_GRID;

        // Collect hemisphere ray directions from the cube surface.
        List<double[]> rayDirs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    // Only surface cells of the cube (at least one coordinate is 0 or n-1).
                    if (i > 0 && i < n - 1 && j > 0 && j < n - 1 && k > 0 && k < n - 1) continue;

                    // Map to [-1, 1] range centered on the cube.
                    double dx = (2.0 * i / (n - 1)) - 1.0;
                    double dy = (2.0 * j / (n - 1)) - 1.0;
                    double dz = (2.0 * k / (n - 1)) - 1.0;
                    double dirLen = Math.sqrt(dx * dx + dy * dy + dz * dz);
                    if (dirLen < 1.0e-6) continue;
                    dx /= dirLen; dy /= dirLen; dz /= dirLen;

                    // Filter to hemisphere: ray must point in the general direction of the normal.
                    if (dx * nx + dy * ny + dz * nz <= 0.0) continue;

                    rayDirs.add(new double[]{dx, dy, dz});
                }
            }
        }

        if (rayDirs.isEmpty()) return;
        double energyPerRay = energy / rayDirs.size();
        double step = PROPAGATION_RAY_STEP;
        int maxSteps = (int) Math.ceil(maxRange / step);

        // Track already-damaged positions to avoid double-hitting from overlapping rays.
        Set<Long> damaged = new HashSet<>();

        for (double[] dir : rayDirs) {
            double rdx = dir[0] * step, rdy = dir[1] * step, rdz = dir[2] * step;
            double rx = cx, ry = cy, rz = cz;
            double rayEnergy = energyPerRay;

            for (int s = 0; s < maxSteps; s++) {
                rx += rdx; ry += rdy; rz += rdz;
                BlockPos pos = BlockPos.containing(rx, ry, rz);
                BlockState state = level.getBlockState(pos);

                if (state.isAir()) continue; // traverse freely through air

                // Hit a solid block — damage it if not already hit by another ray.
                long posKey = pos.asLong();
                if (damaged.add(posKey)) {
                    // Distance attenuation: energy falls off with distance from origin.
                    double dist = Math.sqrt((rx - cx) * (rx - cx) + (ry - cy) * (ry - cy) + (rz - cz) * (rz - cz));
                    double attenuation = Math.max(0.0, 1.0 - dist / (maxRange + 1.0));
                    damageBlock(level, pos, rayEnergy * attenuation);

                    // Antialiasing: bleed a fraction of energy into the nearest adjacent block
                    // when the ray sample is close to a block boundary.
                    applyAntialiasing(level, pos, rx, ry, rz, rayEnergy * attenuation, damaged);
                }

                // Ray stops at the first solid block (does not pass through solids).
                break;
            }
        }
    }

    /**
     * Applies light antialiasing by checking if the sample point is near a block boundary.
     * If the fractional position within the block is within {@link #PROPAGATION_AA_THRESHOLD}
     * of an edge, a fraction of energy bleeds into the adjacent block on that axis.
     */
    private static void applyAntialiasing(ServerLevel level, BlockPos hitPos,
                                           double rx, double ry, double rz,
                                           double hitEnergy, Set<Long> damaged) {
        double fracX = rx - Math.floor(rx);
        double fracY = ry - Math.floor(ry);
        double fracZ = rz - Math.floor(rz);
        double aaEnergy = hitEnergy * PROPAGATION_AA_FRACTION;

        // Check each axis — if close to a boundary, bleed into the neighbor.
        if (fracX < PROPAGATION_AA_THRESHOLD) {
            BlockPos neighbor = hitPos.offset(-1, 0, 0);
            if (damaged.add(neighbor.asLong()) && !level.getBlockState(neighbor).isAir()) {
                damageBlock(level, neighbor, aaEnergy);
            }
        } else if (fracX > 1.0 - PROPAGATION_AA_THRESHOLD) {
            BlockPos neighbor = hitPos.offset(1, 0, 0);
            if (damaged.add(neighbor.asLong()) && !level.getBlockState(neighbor).isAir()) {
                damageBlock(level, neighbor, aaEnergy);
            }
        }

        if (fracY < PROPAGATION_AA_THRESHOLD) {
            BlockPos neighbor = hitPos.offset(0, -1, 0);
            if (damaged.add(neighbor.asLong()) && !level.getBlockState(neighbor).isAir()) {
                damageBlock(level, neighbor, aaEnergy);
            }
        } else if (fracY > 1.0 - PROPAGATION_AA_THRESHOLD) {
            BlockPos neighbor = hitPos.offset(0, 1, 0);
            if (damaged.add(neighbor.asLong()) && !level.getBlockState(neighbor).isAir()) {
                damageBlock(level, neighbor, aaEnergy);
            }
        }

        if (fracZ < PROPAGATION_AA_THRESHOLD) {
            BlockPos neighbor = hitPos.offset(0, 0, -1);
            if (damaged.add(neighbor.asLong()) && !level.getBlockState(neighbor).isAir()) {
                damageBlock(level, neighbor, aaEnergy);
            }
        } else if (fracZ > 1.0 - PROPAGATION_AA_THRESHOLD) {
            BlockPos neighbor = hitPos.offset(0, 0, 1);
            if (damaged.add(neighbor.asLong()) && !level.getBlockState(neighbor).isAir()) {
                damageBlock(level, neighbor, aaEnergy);
            }
        }
    }

    /**
     * Searches all 26 neighbours of {@code origin} for the nearest non-air block,
     * ranked by center-to-center distance (face-adjacent = 1, edge = √2, corner = √3).
     * Returns {@code null} if all neighbours are air.
     */
    @Nullable
    private static BlockPos nearestNonAir(ServerLevel level, BlockPos origin) {
        int bestDistSq = Integer.MAX_VALUE;
        BlockPos best = null;
        for (int dx = -1; dx <= 1; dx++) {
            for (int dy = -1; dy <= 1; dy++) {
                for (int dz = -1; dz <= 1; dz++) {
                    if (dx == 0 && dy == 0 && dz == 0) continue;
                    int distSq = dx * dx + dy * dy + dz * dz;
                    if (distSq >= bestDistSq) continue;
                    BlockPos candidate = origin.offset(dx, dy, dz);
                    if (!level.getBlockState(candidate).isAir()) {
                        bestDistSq = distSq;
                        best = candidate;
                    }
                }
            }
        }
        return best;
    }

    /**
     * Finds the {@link ServerLevel} whose VS2 dimension ID string matches {@code dimensionId},
     * or {@code null} if none is currently loaded.
     */
    private static ServerLevel findLevelByDimId(MinecraftServer server, String dimensionId) {
        for (ServerLevel candidate : server.getAllLevels()) {
            if (dimensionId.equals(ValkyrienSkies.getDimensionId(candidate))) {
                return candidate;
            }
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Ship damage sync
    // -------------------------------------------------------------------------

    /**
     * After an explosion, finds all ships whose chunk claims are within the blast radius,
     * reads the Krakk damage state from their shipyard-dimension chunks, and sends section
     * snapshots to nearby overworld players using the <em>overworld</em> dimension ID.
     *
     * <p>VS2 delivers ship chunk data to clients as part of the player's current level
     * (the overworld), so the client stores and renders those chunks under the overworld
     * dimension.  Packets labeled with the shipyard dimension ID would be dropped by the
     * client's dimension check; sending them with the overworld ID ensures they are applied
     * to the correct loaded chunk on the client.
     */
    private static void syncShipDamageToPlayers(ServerLevel level,
                                                double x, double y, double z,
                                                double resolvedRadius) {
        AABBd queryAABB = new AABBd(
                x - resolvedRadius, y - resolvedRadius, z - resolvedRadius,
                x + resolvedRadius, y + resolvedRadius, z + resolvedRadius);

        Iterable<Ship> ships = ValkyrienSkies.getShipsIntersecting(level, queryAABB);
        if (ships == null) return;

        ResourceLocation overworldDimId = level.dimension().location();
        int viewDistance = level.getServer().getPlayerList().getViewDistance() + 1;

        for (Ship ship : ships) {
            ServerLevel shipyardLevel = getShipyardLevel(level, ship);
            if (shipyardLevel == null) continue;

            ChunkClaim claim = ship.getChunkClaim();

            // Compute the ship's world-space chunk position for player proximity check.
            org.joml.Vector3dc com = ship.getTransform().getPosition();
            int shipChunkX = (int) Math.floor(com.x()) >> 4;
            int shipChunkZ = (int) Math.floor(com.z()) >> 4;

            // Gather overworld players close enough to the ship to receive the sync.
            java.util.List<ServerPlayer> targets = new java.util.ArrayList<>();
            for (ServerPlayer player : level.players()) {
                ChunkPos playerChunk = player.chunkPosition();
                if (Math.abs(playerChunk.x - shipChunkX) <= viewDistance
                        && Math.abs(playerChunk.z - shipChunkZ) <= viewDistance) {
                    targets.add(player);
                }
            }
            if (targets.isEmpty()) continue;

            // Iterate every chunk in this ship's claim and send damaged sections.
            for (int chunkX = claim.getXStart(); chunkX <= claim.getXEnd(); chunkX++) {
                for (int chunkZ = claim.getZStart(); chunkZ <= claim.getZEnd(); chunkZ++) {
                    LevelChunk chunk = shipyardLevel.getChunkSource().getChunkNow(chunkX, chunkZ);
                    if (chunk == null) continue;

                    LevelChunkSection[] sections = chunk.getSections();
                    int sectionY = chunk.getMinBuildHeight() >> 4;
                    for (LevelChunkSection section : sections) {
                        if (section instanceof KrakkBlockDamageSectionAccess access) {
                            Short2ByteOpenHashMap states = access.krakk$getDamageStates();
                            if (!states.isEmpty()) {
                                Short2ByteOpenHashMap snapshot = new Short2ByteOpenHashMap(states);
                                for (ServerPlayer player : targets) {
                                    KrakkApi.network().sendSectionSnapshot(
                                            player,
                                            overworldDimId,
                                            chunkX,
                                            sectionY,
                                            chunkZ,
                                            snapshot);
                                }
                            }
                        }
                        sectionY++;
                    }
                }
            }
        }
    }
}
