package org.shipwrights.krakk.api.explosion;

import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;

/**
 * Immutable input profile for a single explosion detonation.
 *
 * @param radius radius used by the Krakk algorithm; a value of {@code 0} enables
 *               energy-cutoff-limited propagation instead of explicit radius clipping
 * @param energy explicit energy pool used by the Krakk algorithm
 * @param debugVisuals enables debug visuals
 */
public record KrakkExplosionProfile(double radius, double energy,
                                    boolean debugVisuals) {
    private static final double MIN_ENERGY = 1.0E-6D;

    public KrakkExplosionProfile(double radius, double energy) {
        this(radius, energy, false);
    }

    public KrakkExplosionProfile(double power) {
        this(radiusFromPower(power), energyFromPower(power), false);
    }

    public static KrakkExplosionProfile fromPower(double power) {
        return new KrakkExplosionProfile(power);
    }

    public static KrakkExplosionProfile krakk(double impactPower) {
        return fromPower(impactPower);
    }

    public static KrakkExplosionProfile krakk(double radius, double energy) {
        return new KrakkExplosionProfile(radius, energy, false);
    }

    public static KrakkExplosionProfile krakkDebug(double radius, double energy) {
        return new KrakkExplosionProfile(radius, energy, true);
    }

    public static double radiusFromPower(double power) {
        double sanitizedPower = sanitizePower(power);
        return Math.max(1.0D, KrakkExplosionCurves.computeBlastRadius(sanitizedPower));
    }

    public static double energyFromPower(double power) {
        double sanitizedPower = sanitizePower(power);
        return Math.max(MIN_ENERGY, sanitizedPower);
    }

    private static double sanitizePower(double power) {
        if (!Double.isFinite(power) || power <= 0.0D) {
            return KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
        }
        return KrakkExplosionCurves.sanitizeImpactPower(power);
    }
}
