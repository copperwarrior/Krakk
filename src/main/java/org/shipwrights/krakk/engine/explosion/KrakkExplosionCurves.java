package org.shipwrights.krakk.engine.explosion;

public final class KrakkExplosionCurves {
    public static final double DEFAULT_BLAST_RADIUS = 4.0D;
    public static final double DEFAULT_IMPACT_POWER = 420.0D;

    private KrakkExplosionCurves() {
    }

    public static double sanitizeImpactPower(double impactPower) {
        return Math.max(1.0D, impactPower);
    }

    public static double computeBlastRadius(double impactPower) {
        double normalized = Math.max(0.01D, sanitizeImpactPower(impactPower) / DEFAULT_IMPACT_POWER);
        return Math.max(1.0D, DEFAULT_BLAST_RADIUS * Math.cbrt(normalized));
    }

    public static int computeRayCount(double blastRadius, double defaultRayCount, int minRayCount, int maxRayCount) {
        int rayCount = (int) Math.round(defaultRayCount * (blastRadius / DEFAULT_BLAST_RADIUS));
        return Math.max(minRayCount, Math.min(maxRayCount, rayCount));
    }
}
