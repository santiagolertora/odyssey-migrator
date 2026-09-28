# Odyssey Migrator

Lightweight, resumable CQL → ScyllaDB migration — **no Spark**, no JVM, no assembly JAR.

**Hard rule:** never lose a row that was successfully read from the source. Delivery is
at-least-once (rewriting a PK is fine; skipping a failed write is not).

## Documentation

| Doc | |
|-----|--|
| **[docs/README.md](docs/README.md)** | Documentation hub |
| [Install](docs/install.md) | Build binary + Docker demo |
| [Quickstart](docs/quickstart.md) | First migrate → validate |
| [Commands](docs/commands.md) | Full CLI reference |
| [Configuration](docs/configuration.md) | TOML (`ferry.toml`) |
| [Live migration](docs/live-migration.md) | CDC, cutover, dual-write |
| [Known gaps](docs/known-gaps.md) | Honest limits |

```bash
odyssey-migrator --help
```

## Install (short)

```bash
cargo build --release -p odyssey-cli
./target/release/odyssey-migrator --help
cp examples/ferry.toml ./ferry.toml
```

Details: [docs/install.md](docs/install.md).

## Quick commands

```bash
# Local demo clusters
docker compose -f examples/demo/docker-compose.yml up -d

odyssey-migrator migrate -c ferry.toml --plan-only
odyssey-migrator migrate -c ferry.toml
odyssey-migrator status <migration-id> -c ferry.toml
odyssey-migrator validate <migration-id> -c ferry.toml
odyssey-migrator resume <migration-id> -c ferry.toml

# Live / cutover (Scylla CDC) or dual-write (Cassandra source)
odyssey-migrator migrate -c ferry.toml --with-live
odyssey-migrator cutover -c ferry.toml
odyssey-migrator dual-write -c ferry.toml
```

## What it does

- TOML config · schema discovery · Murmur3 / vnode planning
- Parallel page copy · AIMD concurrency · SQLite resume
- Digest / sample / full validation · Prometheus `/metrics` · HTML dashboard
- Scylla CDC live catch-up · cutover checklist · HTTP dual-write gateway
- Optional TTL/WRITETIME preserve (scalars, frozen collections, map/set elements)

## Integrity model

```text
read page → write rows → checkpoint progress → only then may the unit become completed
```

## Workspace

```text
crates/
  odyssey-types / odyssey-core / odyssey-cql / odyssey-planner
  odyssey-checkpoint / odyssey-engine / odyssey-validation / odyssey-metrics
  odyssey-cdc / odyssey-dualwrite / odyssey-cli / odyssey-integration
```

## License

Copyright (c) 2026 Santiago Lertora. All rights reserved.
See [`LICENSE`](LICENSE).
