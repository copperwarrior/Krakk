package org.shipwrights.krakk.api.explosion;

import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.engine.explosion.KrakkExplosionCurves;

/**
 * Immutable input profile for a single explosion detonation.
 *
 * @param radius radius used by the Krakk algorithm; a value of {@code 0} enables
 *               energy-cutoff-limited propagation instead of explicit radius clipping
 * @param energy explicit energy pool used by the Krakk algorithm
 * @param impactHeatCelsius thermal magnitude routed to impact conversion and placement systems
 * @param debugVisuals enables debug visuals
 */
public record KrakkExplosionProfile(double radius, double energy,
                                    double impactHeatCelsius,
                                    boolean debugVisuals,
                                    double blastTransmittance) {
    public static final double TONNAGE_PER_MINECRAFT_TNT = 0.1D;
    public static final double MINECRAFT_TNT_PER_TON = 1.0D / TONNAGE_PER_MINECRAFT_TNT;
    public static final double KILOTON_TONS = 1_000.0D;
    public static final double MEGATON_TONS = 1_000_000.0D;
    public static final double POWER_PER_MINECRAFT_TNT = 25.0D;
    public static final double DEFAULT_BLAST_TRANSMITTANCE = 0.92D;
    private static final double MIN_ENERGY = 1.0E-6D;

    public KrakkExplosionProfile {
        impactHeatCelsius = sanitizeHeat(impactHeatCelsius);
        blastTransmittance = Math.max(0.0D, Math.min(1.0D, blastTransmittance));
    }

    public KrakkExplosionProfile(double radius, double energy) {
        this(radius, energy, KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS, false, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public KrakkExplosionProfile(double radius, double energy, double impactHeatCelsius) {
        this(radius, energy, impactHeatCelsius, false, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public KrakkExplosionProfile(double power) {
        this(radiusFromPower(power), energyFromPower(power),
                KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS, false, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public KrakkExplosionProfile withBlastTransmittance(double newBlastTransmittance) {
        return new KrakkExplosionProfile(radius, energy, impactHeatCelsius, debugVisuals, newBlastTransmittance);
    }

    public static KrakkExplosionProfile fromPower(double power) {
        return new KrakkExplosionProfile(
                radiusFromPower(power),
                energyFromPower(power),
                KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS,
                false,
                DEFAULT_BLAST_TRANSMITTANCE
        );
    }

    public static KrakkExplosionProfile fromPower(double power, double impactHeatCelsius) {
        return new KrakkExplosionProfile(
                radiusFromPower(power),
                energyFromPower(power),
                impactHeatCelsius,
                false,
                DEFAULT_BLAST_TRANSMITTANCE
        );
    }

    public static KrakkExplosionProfile krakk(double impactPower) {
        return fromPower(impactPower);
    }

    public static KrakkExplosionProfile krakk(double radius, double energy) {
        return new KrakkExplosionProfile(radius, energy, KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS, false, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public static KrakkExplosionProfile krakk(double radius, double energy, double impactHeatCelsius) {
        return new KrakkExplosionProfile(radius, energy, impactHeatCelsius, false, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public static KrakkExplosionProfile krakkDebug(double radius, double energy) {
        return new KrakkExplosionProfile(radius, energy, KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS, true, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public static KrakkExplosionProfile krakkDebug(double radius, double energy, double impactHeatCelsius) {
        return new KrakkExplosionProfile(radius, energy, impactHeatCelsius, true, DEFAULT_BLAST_TRANSMITTANCE);
    }

    public static KrakkExplosionProfile fromTonnage(double tonnageTons) {
        return fromPower(powerFromTonnage(tonnageTons));
    }

    public static KrakkExplosionProfile fromTonnage(double tonnageTons, double impactHeatCelsius) {
        return fromPower(powerFromTonnage(tonnageTons), impactHeatCelsius);
    }

    public static double radiusFromPower(double power) {
        double sanitizedPower = sanitizePower(power);
        return Math.max(1.0D, KrakkExplosionCurves.computeBlastRadius(sanitizedPower));
    }

    public static double energyFromPower(double power) {
        double sanitizedPower = sanitizePower(power);
        return Math.max(MIN_ENERGY, sanitizedPower);
    }

    public static double radiusFromTonnage(double tonnageTons) {
        return radiusFromPower(powerFromTonnage(tonnageTons));
    }

    public static double energyFromTonnage(double tonnageTons) {
        return energyFromPower(powerFromTonnage(tonnageTons));
    }

    public static double powerFromTonnage(double tonnageTons) {
        if (!Double.isFinite(tonnageTons) || tonnageTons <= 0.0D) {
            return KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
        }
        double minecraftTntEquivalent = tonnageTons * MINECRAFT_TNT_PER_TON;
        return minecraftTntEquivalent * POWER_PER_MINECRAFT_TNT;
    }

    public static double tonnageFromPower(double power) {
        double sanitizedPower = sanitizePower(power);
        double minecraftTntEquivalent = sanitizedPower / POWER_PER_MINECRAFT_TNT;
        return minecraftTntEquivalent * TONNAGE_PER_MINECRAFT_TNT;
    }

    private static double sanitizePower(double power) {
        if (!Double.isFinite(power) || power <= 0.0D) {
            return KrakkExplosionCurves.DEFAULT_IMPACT_POWER;
        }
        return KrakkExplosionCurves.sanitizeImpactPower(power);
    }

    private static double sanitizeHeat(double impactHeatCelsius) {
        if (!Double.isFinite(impactHeatCelsius)) {
            return KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS;
        }
        return impactHeatCelsius;
    }
}
