package org.shipwrights.krakk.api.explosion;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.LivingEntity;

public interface KrakkExplosionApi {
    void detonate(ServerLevel level, double x, double y, double z, Entity source,
                  LivingEntity owner, KrakkExplosionProfile profile);

    void setPower(double impactPower);

    double getPower();

    double getDefaultPower();

    double getRadius();
}
