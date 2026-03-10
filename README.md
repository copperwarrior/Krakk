# Krakk

Krakk is a standalone library mod focused on:
1. block damage state systems
2. explosion simulation utilities
3. chunk persistence/sync for damage overlays
4. shared API + runtime hooks for integrations

## Build
```bash
./gradlew build
```

## Javadocs
```bash
./gradlew javadoc javadocJar
```

## Publish locally (for downstream projects)
```bash
./gradlew publishToMavenLocal
```

Published coordinates:
- Group: `org.shipwrights.krakk`
- Artifacts:
  - `krakk-common`
  - `krakk-fabric`
  - `krakk-forge`
- Version: `0.1.0-SNAPSHOT` (default in `gradle.properties`)

## API Reference
- [KRAKK-API-REFERENCE.md](docs/KRAKK-API-REFERENCE.md)

## Debug Commands
All commands are under `/krakk` and require permission level 2.

1. `setblockdamage <pos> <value>`
2. `getblockdamage <pos>`
3. `clearblockdamage <pos>`
4. `fillblockdamage <from> <to> <value>`
5. `clearareadamage <from> <to>`
6. `damage <pos> <amount>`
7. `areadamage <from> <to> <amount>`
8. `explode <pos> <radius> <power>`
9. `profexplode <pos> <radius> <power> [runs] [warmup] [seed] [apply]`
10. `decaytick [ticks] [player]`
11. `syncchunk [player]`
12. `stats [player]`
13. `overlay refresh [player]`
14. `overlay clear [player]`
