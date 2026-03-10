package org.shipwrights.krakk.api.explosion;

/**
 * Immutable input profile for a single explosion detonation.
 *
 * @param impactPower explosion strength used for ray energy and block damage
 * @param blastRadius max sampling radius for the explosion pass
 */
public record KrakkExplosionProfile(double impactPower, double blastRadius) {
}
