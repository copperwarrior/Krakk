package org.shipwrights.krakk.api.explosion;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;

/**
 * Explosion simulation and global tuning API.
 */
public interface KrakkExplosionApi {
    /**
     * Executes a Krakk explosion simulation.
     */
    void detonate(ServerLevel level, double x, double y, double z, Entity source,
                  LivingEntity owner, KrakkExplosionProfile profile);

    /**
     * Sets global impact power used by command-driven or default explosions.
     */
    void setPower(double impactPower);

    /**
     * Returns current global impact power.
     */
    double getPower();

    /**
     * Returns default impact power before overrides.
     */
    double getDefaultPower();

    /**
     * Returns current effective explosion radius derived from power.
     */
    double getRadius();
}
