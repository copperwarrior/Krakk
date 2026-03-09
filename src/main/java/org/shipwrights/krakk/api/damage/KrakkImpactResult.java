package org.shipwrights.krakk.api.damage;

/**
 * Result of a Krakk damage impact operation.
 *
 * @param broken true when the target block was broken by this operation
 * @param damageState resulting tracked damage state; callers may use negative or
 *                    zero values as "no stored state" depending on implementation
 */
public record KrakkImpactResult(boolean broken, int damageState) {
}
