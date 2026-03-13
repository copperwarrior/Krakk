package org.shipwrights.krakk.api.explosion;

/**
 * Immutable input profile for a single explosion detonation.
 *
 * @param impactPower explosion strength used for ray energy and block damage
 * @param mode simulation mode
 * @param volumetricRadius radius used by volumetric/eikonal modes; for eikonal, a value of {@code 0} enables
 *                         energy-cutoff-limited propagation instead of explicit radius clipping
 * @param volumetricEnergy explicit energy pool used by volumetric/eikonal modes
 * @param debugVisuals enables debug visuals for supported modes
 */
public record KrakkExplosionProfile(double impactPower, Mode mode, double volumetricRadius, double volumetricEnergy,
                                    boolean debugVisuals) {
    public KrakkExplosionProfile {
        mode = mode == null ? Mode.RAYCAST : mode;
    }

    public KrakkExplosionProfile(double impactPower) {
        this(impactPower, Mode.RAYCAST, Double.NaN, Double.NaN, false);
    }

    public static KrakkExplosionProfile raycast(double impactPower) {
        return new KrakkExplosionProfile(impactPower, Mode.RAYCAST, Double.NaN, Double.NaN, false);
    }

    public static KrakkExplosionProfile volumetric(double radius, double energy) {
        return new KrakkExplosionProfile(Double.NaN, Mode.VOLUMETRIC, radius, energy, false);
    }

    public static KrakkExplosionProfile volumetricDebug(double radius, double energy) {
        return new KrakkExplosionProfile(Double.NaN, Mode.VOLUMETRIC, radius, energy, true);
    }

    public static KrakkExplosionProfile eikonal(double radius, double energy) {
        return new KrakkExplosionProfile(Double.NaN, Mode.EIKONAL, radius, energy, false);
    }

    public static KrakkExplosionProfile eikonalDebug(double radius, double energy) {
        return new KrakkExplosionProfile(Double.NaN, Mode.EIKONAL, radius, energy, true);
    }

    public enum Mode {
        RAYCAST,
        VOLUMETRIC,
        EIKONAL
    }
}
