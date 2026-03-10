# Krakk API Reference

Version: `0.1.0-SNAPSHOT`

Base package: `org.shipwrights.krakk.api`

## Entry Point

Class: `KrakkApi`

Purpose:
1. Service locator for active Krakk subsystem implementations.
2. Stable integration boundary for consumers.

Accessors:
1. `KrakkDamageApi damage()`
2. `KrakkExplosionApi explosions()`
3. `KrakkClientOverlayApi clientOverlay()`
4. `KrakkNetworkApi network()`

Installers (runtime/bootstrap use):
1. `setDamageApi(KrakkDamageApi api)`
2. `setExplosionApi(KrakkExplosionApi api)`
3. `setClientOverlayApi(KrakkClientOverlayApi api)`
4. `setNetworkApi(KrakkNetworkApi api)`

## Damage API

Interface: `KrakkDamageApi`

Server-authoritative block damage, decay, transfer, and mining baseline behavior.

Methods:
1. `KrakkImpactResult applyImpact(ServerLevel level, BlockPos pos, BlockState state, Entity source, double impactPower, boolean dropOnBreak)`
2. `void clearDamage(ServerLevel level, BlockPos pos)`
3. `int repairDamage(ServerLevel level, BlockPos pos, int repairAmount)`
4. `int getDamageState(ServerLevel level, BlockPos pos)`
5. `float getMiningBaseline(ServerLevel level, BlockPos pos)`
6. `int takeDamageState(ServerLevel level, BlockPos pos)`
7. `int takeStoredDamageState(ServerLevel level, BlockPos pos)`
8. `boolean isLikelyPistonMoveSource(ServerLevel level, BlockPos sourcePos, BlockState sourceState)`
9. `boolean transferLikelyPistonCompletionDamage(ServerLevel level, BlockPos destinationPos, BlockState destinationState)`
10. `void applyTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState, int transferredState)`
11. `KrakkImpactResult accumulateTransferredDamageState(ServerLevel level, BlockPos pos, BlockState expectedState, int addedState, boolean dropOnBreak)`
12. `int getMaxDamageState()`
13. `boolean setDamageStateForDebug(ServerLevel level, BlockPos pos, int damageState)`
14. `void queuePlayerSync(ServerPlayer player)`
15. `void clearQueuedPlayerSync(ServerPlayer player)`
16. `void tickQueuedSyncs(MinecraftServer server)`
17. `void syncChunkToPlayer(ServerPlayer player, ServerLevel level, int chunkX, int chunkZ, boolean loadIfMissing)`

Related record:
1. `KrakkImpactResult(boolean broken, int damageState)`

## Explosion API

Interface: `KrakkExplosionApi`

Explosion simulation execution.

Methods:
1. `void detonate(ServerLevel level, double x, double y, double z, Entity source, LivingEntity owner, KrakkExplosionProfile profile)`

Related record:
1. `KrakkExplosionProfile(double impactPower, double blastRadius)`

## Client Overlay API

Interface: `KrakkClientOverlayApi`

Client-side cache for overlay rendering and mining prediction baseline.

Methods:
1. `void resetClientState()`
2. `void applyDamage(ResourceLocation dimensionId, long posLong, int damageState)`
3. `void applySection(ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap sectionStates)`
4. `void clearChunk(ResourceLocation dimensionId, int chunkX, int chunkZ)`
5. `float getMiningBaseline(ResourceLocation dimensionId, long posLong)`
6. `long[] consumeDirtySections(ResourceLocation dimensionId)`
7. `Long2ByteOpenHashMap snapshotSection(ResourceLocation dimensionId, long sectionKey)`

## Network API

Interface: `KrakkNetworkApi`

Packet boundary used by Krakk damage synchronization.

Methods:
1. `void initClientReceivers()`
2. `void sendDamageSync(ServerLevel level, BlockPos pos, int damageState)`
3. `void sendSectionSnapshot(ServerPlayer player, ResourceLocation dimensionId, int sectionX, int sectionY, int sectionZ, Short2ByteOpenHashMap states)`
4. `void sendChunkUnload(ServerPlayer player, ResourceLocation dimensionId, int chunkX, int chunkZ)`

## Integration Notes

1. Damage and explosion simulation are server-authoritative.
2. Client overlay API is cache/state only; do not treat it as authoritative world state.
3. Consumers should call APIs through `KrakkApi`, not concrete runtime classes.
4. Prefer adding new contract methods to `org.shipwrights.krakk.api.*` first, then implementing in runtime.
