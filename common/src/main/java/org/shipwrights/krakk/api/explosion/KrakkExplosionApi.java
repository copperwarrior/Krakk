package org.shipwrights.krakk.api.explosion;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;

/**
 * Explosion simulation API.
 */
public interface KrakkExplosionApi {
    /**
     * Executes a Krakk explosion simulation.
     */
    void detonate(ServerLevel level, double x, double y, double z, Entity source,
                  LivingEntity owner, KrakkExplosionProfile profile);
}
