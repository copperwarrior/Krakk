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
- Artifact: `krakk`
- Version: `0.1.0-SNAPSHOT` (default in `gradle.properties`)

## API Reference
- [KRAKK-API-REFERENCE.md](docs/KRAKK-API-REFERENCE.md)
