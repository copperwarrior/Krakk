package org.shipwrights.krakk.runtime.explosion;

/**
 * Mutable trace object accumulating per-explosion diagnostic counters.
 * All fields are package-private so that {@code KrakkExplosionRuntime} driver
 * methods and {@code NarrowBandWavefrontJob} can write them directly.
 */
final class ExplosionProfileTrace {
    int initialRays;
    int processedRays;
    int raySplits;
    int splitChecks;
    int raySteps;
    int rawImpactedBlocks;
    int postAaImpactedBlocks;
    int blocksEvaluated;
    int brokenBlocks;
    int damagedBlocks;
    int predictedBrokenBlocks;
    int predictedDamagedBlocks;
    int tntTriggered;
    int specialHandled;
    int lowImpactSkipped;
    int entityCandidates;
    int entityIntersectionTests;
    int entityHits;
    int octreeNodeTests;
    int octreeLeafVisits;
    int entityAffected;
    int entityDamaged;
    int entityKilled;
    long broadphaseNanos;
    long raycastNanos;
    long antialiasNanos;
    long blockResolveNanos;
    long splitCheckNanos;
    long entitySegmentNanos;
    long entityApplyNanos;
    long volumetricResistanceFieldNanos;
    long volumetricDirectionSetupNanos;
    long volumetricPressureSolveNanos;
    long krakkSolveNanos;
    long volumetricTargetScanNanos;
    long volumetricTargetScanPrecheckNanos;
    long volumetricTargetScanBlendNanos;
    long volumetricImpactApplyNanos;
    long volumetricImpactApplyDirectNanos;
    long volumetricImpactApplyCollapseSeedNanos;
    long volumetricImpactApplyCollapseBfsNanos;
    long volumetricImpactApplyCollapseApplyNanos;
    int volumetricSampledVoxels;
    int volumetricSampledSolids;
    int volumetricTargetBlocks;
    int volumetricDirectionSamples;
    int volumetricRadialSteps;
    int krakkSourceCells;
    int krakkSweepCycles;
    int syncPacketsEstimated;
    int syncBytesEstimated;
    @SuppressWarnings("unused") // emitRaySmoke is intentionally disabled for Cannonical; always 0 but kept for trace reporting
    int smokeParticles;
}
