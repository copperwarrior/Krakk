package org.shipwrights.krakk.engine.damage;

public final class KrakkDamageCurves {
    public static final int MAX_DAMAGE_STATE = 15;
    public static final double MIN_IMPACT_FOR_ONE_DAMAGE_STATE = 350.0D; // speed 5 -> 5^2 * 14

    private KrakkDamageCurves() {
    }

    public static int computeImpactDamageDelta(boolean isFallingBlock, double impactPower, float resistance, float hardness) {
        double safeResistance = Math.max(0.0D, resistance);
        double safeHardness = Math.max(0.0D, hardness);

        // Tuned curve targets:
        // - sand-like blocks: instant break at speed 1
        // - dirt-like blocks: ~75% damage at speed 1
        // - stone-like blocks: start taking damage at speed 1, instant at speed 30
        // - obsidian-like blocks: damage starts at speed 5, instant at speed 80
        double baseDurability = 0.45D
                + (0.75D * Math.pow(safeHardness, 1.35D))
                + (0.18D * Math.pow(safeResistance, 0.60D))
                + (0.002D * safeHardness * safeResistance);

        double materialFactor = isFallingBlock ? 1.0D : 1.47D;
        double durability = baseDurability * materialFactor;

        double normalizedImpact = impactPower / durability;
        int delta = clampDamageState((int) Math.floor(normalizedImpact));
        if (delta <= 0 && impactPower >= MIN_IMPACT_FOR_ONE_DAMAGE_STATE) {
            return 1;
        }
        return delta;
    }

    public static int clampDamageState(int damageState) {
        return Math.max(0, Math.min(MAX_DAMAGE_STATE, damageState));
    }

    public static float toMiningBaseline(int damageState) {
        int clamped = clampDamageState(damageState);
        if (clamped <= 0) {
            return 0.0F;
        }
        return Math.min(1.0F, clamped / (float) MAX_DAMAGE_STATE);
    }
}
