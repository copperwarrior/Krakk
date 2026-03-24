# Blast Shapes — Design Plan

This document describes the planned architecture for pluggable explosion geometry in Krakk via a `KrakkBlastShape` interface.

---

## Motivation

The explosion pipeline in `KrakkExplosionRuntime` hardcodes a sphere as the blast geometry. The shape currently controls:

- **Grid bounds** — `center ± resolvedRadius` (axis-aligned cube)
- **Cell filtering** — `distSq > radiusSq` (appears in ~6 places)
- **Ray generation** — golden-spiral uniform distribution on a unit sphere
- **Ray energy scaling** — `rayEnergy = blastRadius * (0.7 + rand * 0.6)`

The Eikonal solver itself (`sweepKrakkDiagonalChunk`, `solveKrakkCell`, `buildKrakkField`) is **fully shape-agnostic** — it operates on a 3D grid array and propagates from seed cells regardless of geometry. No changes are needed inside the solver.

---

## Target shapes

| Shape | Use case |
|---|---|
| Sphere | Default; existing behavior |
| Ellipsoid | Surface detonation (wide/flat), buried mine (tall/narrow), anisotropic charges |
| Directional cone | Shaped charge; penetrating round detonating inside armor; focused blast |
| Capsule | Torpedo, elongated barrel charge, depth charge column |
| Custom / arbitrary | Any caller-supplied implementation |

---

## `KrakkBlastShape` interface

**Proposed location:** `krakk/common/src/main/java/org/shipwrights/krakk/api/explosion/KrakkBlastShape.java`

```java
package org.shipwrights.krakk.api.explosion;

import java.util.Random;

/**
 * Defines the 3-D geometry of an explosion's blast volume.
 *
 * <p>All coordinates are offsets from the explosion center (i.e. block center minus
 * explosion origin). Unit-direction arguments (ux, uy, uz) must be pre-normalized
 * to length 1.0 by the caller.
 */
public interface KrakkBlastShape {

    /**
     * Returns the maximum extent of the shape in any direction.
     * Used to size the axis-aligned grid that the Eikonal solver operates on.
     * Must be > 0.
     */
    double boundingRadius();

    /**
     * Returns {@code true} if the point at offset {@code (dx, dy, dz)} from the
     * explosion center lies inside (or on the surface of) the blast volume.
     */
    boolean containsOffset(double dx, double dy, double dz);

    /**
     * Returns the effective radius of the shape in unit direction {@code (ux, uy, uz)}.
     *
     * <p>Used to scale ray energy: a direction that extends further through the shape
     * carries proportionally more energy. For a sphere this is always {@code r}.
     * Returns 0.0 for directions that do not intersect the shape at all (e.g. the
     * back half of a directional cone).
     */
    double effectiveRadius(double ux, double uy, double uz);

    /**
     * Generates {@code count} ray unit-direction vectors distributed over the shape's
     * surface, returning them as a flat {@code double[count * 3]} array in
     * {@code [dx0, dy0, dz0, dx1, dy1, dz1, ...]} order.
     *
     * <p>Implementations should produce a low-discrepancy or uniform distribution
     * over the solid angle subtended by the shape so that energy is applied evenly.
     *
     * @param random  used for any jitter or stochastic sampling
     */
    double[] sampleRayDirections(int count, Random random);

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /** Standard sphere with the given radius. Preserves all existing behavior. */
    static KrakkBlastShape sphere(double radius) {
        return new SphereBlastShape(radius);
    }

    /**
     * Axis-aligned ellipsoid with independent per-axis radii.
     *
     * @param rx  radius along X
     * @param ry  radius along Y
     * @param rz  radius along Z
     */
    static KrakkBlastShape ellipsoid(double rx, double ry, double rz) {
        return new EllipsoidBlastShape(rx, ry, rz);
    }

    /**
     * Spherical-cap (cone) shape: full sphere clipped to a forward cone.
     *
     * @param radius           maximum blast reach
     * @param halfAngleDegrees half-apex angle of the cone in degrees (0 = pencil beam,
     *                         180 = full sphere)
     * @param dirX/Y/Z         axis direction (need not be normalized; will be normalized internally)
     */
    static KrakkBlastShape cone(double radius, double halfAngleDegrees,
                                 double dirX, double dirY, double dirZ) {
        return new ConeBlastShape(radius, halfAngleDegrees, dirX, dirY, dirZ);
    }

    /**
     * Capsule: a cylinder of the given radius capped by hemispheres at each end.
     *
     * @param radius     cross-section radius
     * @param halfLength half the length of the cylindrical section
     * @param axisX/Y/Z  orientation axis (need not be normalized; will be normalized internally)
     */
    static KrakkBlastShape capsule(double radius, double halfLength,
                                    double axisX, double axisY, double axisZ) {
        return new CapsuleBlastShape(radius, halfLength, axisX, axisY, axisZ);
    }
}
```

---

## Built-in implementations

All implementations are package-private classes in the same package as `KrakkBlastShape`.

### `SphereBlastShape(double r)`

| Method | Implementation |
|---|---|
| `boundingRadius()` | `r` |
| `containsOffset(dx,dy,dz)` | `dx²+dy²+dz² ≤ r²` |
| `effectiveRadius(ux,uy,uz)` | `r` |
| `sampleRayDirections(n, rng)` | Extract existing golden-spiral algorithm from `runKrakkPropagation` lines ~8194–8209 into a static helper; call it here |

### `EllipsoidBlastShape(double rx, double ry, double rz)`

| Method | Implementation |
|---|---|
| `boundingRadius()` | `Math.max(rx, Math.max(ry, rz))` |
| `containsOffset(dx,dy,dz)` | `(dx/rx)² + (dy/ry)² + (dz/rz)² ≤ 1.0` |
| `effectiveRadius(ux,uy,uz)` | `1.0 / sqrt((ux/rx)² + (uy/ry)² + (uz/rz)²)` |
| `sampleRayDirections(n, rng)` | Golden spiral on unit sphere → scale each point by `(rx,ry,rz)` → renormalize to unit vector. Gives denser sampling toward larger axes. |

### `ConeBlastShape(double r, double halfAngle, Vec3 dir)`

The shape is the intersection of a sphere of radius `r` and a half-space defined by the cone opening.

| Method | Implementation |
|---|---|
| `boundingRadius()` | `r` |
| `containsOffset(dx,dy,dz)` | `dist ≤ r` AND `acos(dot(norm(dx,dy,dz), dir)) ≤ halfAngle` |
| `effectiveRadius(ux,uy,uz)` | `r` if `acos(dot(u,dir)) ≤ halfAngle`, else `0.0` |
| `sampleRayDirections(n, rng)` | Uniform sampling on spherical cap: `cosθ` uniform in `[cos(halfAngle), 1]`, `φ` uniform in `[0, 2π]`. Rotate from `(0,0,1)` frame to `dir` frame via Rodrigues' rotation. |

Special cases: `halfAngle = 180°` → full sphere. `halfAngle = 0°` → single ray (degenerate; clamp to ≥ 1°).

### `CapsuleBlastShape(double r, double halfLen, Vec3 axis)`

The shape is the Minkowski sum of a line segment and a sphere — equivalent to all points within distance `r` of the line segment `[-halfLen*axis, +halfLen*axis]`.

| Method | Implementation |
|---|---|
| `boundingRadius()` | `halfLen + r` |
| `containsOffset(dx,dy,dz)` | Project offset onto axis, clamp projection to `[-halfLen, halfLen]`, compute distance from nearest point on segment, check `≤ r` |
| `effectiveRadius(ux,uy,uz)` | `r + halfLen * abs(dot(u, axis))` — approximation that gives full sphere at equator and longer reach along axis |
| `sampleRayDirections(n, rng)` | Distribute proportionally by surface area: hemisphere caps cover `2πr²` each; cylinder side covers `2πr * 2halfLen`. Sample caps with hemispherical distribution rotated to `±axis`; sample side with uniform distribution on a cylinder |

---

## Changes to `KrakkExplosionProfile`

`KrakkExplosionProfile` is a record. Add one new component with a null default:

```java
public record KrakkExplosionProfile(
        double radius,
        double energy,
        double impactHeatCelsius,
        boolean debugVisuals,
        @Nullable KrakkBlastShape shape   // null = sphere using radius
) {
    // All existing factory methods remain unchanged — they call
    // the canonical constructor with shape = null.

    /** Returns a copy of this profile with the given blast shape. */
    public KrakkExplosionProfile withShape(KrakkBlastShape shape) {
        return new KrakkExplosionProfile(radius, energy, impactHeatCelsius, debugVisuals, shape);
    }
}
```

---

## Changes to `KrakkExplosionRuntime`

### `detonateKrakk`

After resolving `resolvedRadius`, synthesize a shape when none is provided:

```java
KrakkBlastShape shape = (profile.shape() != null)
        ? profile.shape()
        : KrakkBlastShape.sphere(resolvedRadius);
```

Pass `shape` to `runKrakkPropagation`.

### `runKrakkPropagation` — new parameter

```java
private static void runKrakkPropagation(
        ServerLevel level,
        double centerX, double centerY, double centerZ,
        double blastRadius,
        double totalEnergy,
        double impactHeatCelsius,
        KrakkBlastShape shape,       // NEW
        Entity source,
        LivingEntity owner,
        boolean applyWorldChanges,
        ExplosionProfileTrace trace)
```

### Grid bounds (currently lines ~904–910)

```java
// Before:
double resolvedRadius = ...;
int minX = Mth.floor(centerX - resolvedRadius);
int maxX = Mth.ceil (centerX + resolvedRadius);
// ...

// After:
double bound = shape.boundingRadius();
int minX = Mth.floor(centerX - bound);
int maxX = Mth.ceil (centerX + bound);
// same for Y, Z
```

### Cell filtering — all 6+ occurrences

Approximately at lines 1650, 2448, 2627, 6667, 6742, 6813, 7194 and inside `applyExtraRadiusImpacts`.

```java
// Before:
double distSq = dx * dx + dy * dy + dz * dz;
if (distSq > radiusSq) continue;

// After:
if (!shape.containsOffset(dx, dy, dz)) continue;
```

The `radiusSq` local variable can be removed at each site once all checks are converted.

### Ray generation (lines ~8194–8209)

```java
// Before: hardcoded golden-spiral loop
for (int i = 0; i < rayCount; i++) {
    double t = (i + 0.5D) / rayCount;
    double dy = 1.0D - 2.0D * t;
    double radial = Math.sqrt(Math.max(0.0D, 1.0D - dy * dy));
    double theta = i * GOLDEN_ANGLE;
    double rdx = Math.cos(theta) * radial;
    double rdz = Math.sin(theta) * radial;
    // ...
}

// After:
double[] dirs = shape.sampleRayDirections(rayCount, random);
for (int i = 0, j = 0; i < rayCount; i++, j += 3) {
    double rdx = dirs[j], rdy = dirs[j + 1], rdz = dirs[j + 2];
    // apply existing jitter (INITIAL_RAY_VARIANCE_RADIANS) as before
    // ...
}
```

Note: the golden-spiral algorithm extracted into `SphereBlastShape.sampleRayDirections` is identical to the current code — no behavioral change for sphere explosions.

### Ray energy scaling

```java
// Before:
double rayEnergy = blastRadius * (0.7D + random.nextDouble() * 0.6D);

// After:
double rayEnergy = shape.effectiveRadius(rdx, rdy, rdz) * (0.7D + random.nextDouble() * 0.6D);
```

### `applyExtraRadiusImpacts`

This method currently uses spherical inner/outer radius checks. Replace with shape-based checks:

- **Inner boundary** (blocks already inside main blast): `shape.containsOffset(dx, dy, dz)` → skip
- **Outer boundary** (blocks beyond the envelope): scale offsets by `1.0 / (1.0 + EXTRA_RADIUS_FRACTION)` and test `shape.containsOffset(scaledDx, scaledDy, scaledDz)` — if that returns false, the block is outside the inflated envelope → skip

---

## Scope notes

- **No changes to the Eikonal solver.** `sweepKrakkDiagonalChunk`, `solveKrakkCell`, `buildKrakkField`, and `initializePairedKrakkSources` are untouched. They already operate generically on any active-masked 3D grid.
- **Backward compatibility.** `null` shape in the profile synthesizes `KrakkBlastShape.sphere(resolvedRadius)` before entering `runKrakkPropagation`. All existing callers (including `GunpowderBarrelPrimedEntity` in Cannonical) need no changes.
- **Degenerate cases.** Cone(halfAngle=180) = sphere. Capsule(halfLen=0) = sphere. Ellipsoid(rx=ry=rz=r) = sphere. All should produce numerically identical results to `SphereBlastShape`.

---

## Verification

1. Default (no shape set): sphere detonation behavior and visual output unchanged.
2. `Ellipsoid(40, 15, 40)` on flat terrain: crater is wide and shallow rather than spherical.
3. `Cone(radius=30, halfAngle=45°, dir=south)`: only the forward hemisphere is destroyed; the behind-the-charge side is untouched.
4. `Capsule(radius=10, halfLen=20, axis=Y)`: a tall column is removed with hemispherical caps above and below.
5. `GunpowderBarrelPrimedEntity` (null shape): behavior identical to pre-change baseline.
