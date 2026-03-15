package org.shipwrights.krakk.api.explosion;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.phys.Vec3;

/**
 * Explosion simulation API.
 */
public interface KrakkExplosionApi {
    /**
     * Executes a Krakk explosion simulation.
     */
    void detonate(ServerLevel level, double x, double y, double z, Entity source,
                  LivingEntity owner, KrakkExplosionProfile profile);

    /**
     * Executes a simple Krakk explosion using power->radius/energy conversion.
     */
    default void detonate(ServerLevel level, double x, double y, double z, double power) {
        detonate(level, x, y, z, null, null, KrakkExplosionProfile.fromPower(power));
    }

    /**
     * Executes a simple Krakk explosion with explicit radius and energy.
     */
    default void detonate(ServerLevel level, double x, double y, double z, double radius, double energy) {
        detonate(level, x, y, z, null, null, KrakkExplosionProfile.krakk(radius, energy));
    }

    /**
     * Executes a simple Krakk explosion at a Vec3 using power->radius/energy conversion.
     */
    default void detonate(ServerLevel level, Vec3 pos, double power) {
        detonate(level, pos.x, pos.y, pos.z, power);
    }

    /**
     * Executes a simple Krakk explosion at a Vec3 with explicit radius and energy.
     */
    default void detonate(ServerLevel level, Vec3 pos, double radius, double energy) {
        detonate(level, pos.x, pos.y, pos.z, radius, energy);
    }

    /**
     * Executes a source-owned Krakk explosion using power->radius/energy conversion.
     */
    default void detonate(ServerLevel level, double x, double y, double z, Entity source,
                          LivingEntity owner, double power) {
        detonate(level, x, y, z, source, owner, KrakkExplosionProfile.fromPower(power));
    }

    /**
     * Executes a source-owned Krakk explosion with explicit radius and energy.
     */
    default void detonate(ServerLevel level, double x, double y, double z, Entity source,
                          LivingEntity owner, double radius, double energy) {
        detonate(level, x, y, z, source, owner, KrakkExplosionProfile.krakk(radius, energy));
    }
}
