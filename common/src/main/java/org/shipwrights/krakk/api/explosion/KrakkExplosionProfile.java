package org.shipwrights.krakk.api.explosion;

/**
 * Immutable input profile for a single explosion detonation.
 *
 * @param impactPower explosion strength used for ray energy and block damage
 */
public record KrakkExplosionProfile(double impactPower) {
}
