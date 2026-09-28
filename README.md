# Odyssey Migrator

CQL table migration into ScyllaDB without Spark.

Odyssey copies Cassandra or Scylla tables over the native CQL protocol: token-range
reads, prepared writes, SQLite checkpoints, and an optional live path for catching up
mutations after the bulk copy. Delivery is at-least-once — Odyssey may rewrite a
primary key, but it will not skip a row that was already read from the source.

## Why it exists

Tools built around Spark add a heavy runtime for a job that is ultimately
“read ranges, write ranges, resume if interrupted.” Odyssey keeps that path in a
single Rust binary: plan → copy → checkpoint → validate → (optional) CDC / dual-write.

## Features

- Parallel token-range copy with adaptive concurrency and retries
- Resumable SQLite checkpoints (multi-process mesh optional)
- Digest / sample / full validation
- Scylla CDC live catch-up and an ops cutover checklist
- HTTP dual-write gateway for Cassandra→Scylla cutover windows
- Optional TTL / WRITETIME preserve (scalars, frozen collections, map/set elements)
- Prometheus metrics and a small HTML progress dashboard

## Quick start

```bash
cargo build --release -p odyssey-cli
cp examples/ferry.toml ./ferry.toml   # edit contact points and [[tables]]

./target/release/odyssey-migrator migrate -c ferry.toml --plan-only
./target/release/odyssey-migrator migrate -c ferry.toml
./target/release/odyssey-migrator validate <migration-id> -c ferry.toml
```

Install the binary on your `PATH`:

```bash
cargo install --path crates/odyssey-cli
odyssey-migrator --help
```

Local demo clusters: `docker compose -f examples/demo/docker-compose.yml up -d`
(see [docs/install.md](docs/install.md)).

## Documentation

| | |
|--|--|
| [docs/README.md](docs/README.md) | Documentation index |
| [Install](docs/install.md) | Build and Docker demo |
| [Quickstart](docs/quickstart.md) | First migration end-to-end |
| [Commands](docs/commands.md) | CLI reference |
| [Configuration](docs/configuration.md) | TOML settings |
| [Live migration](docs/live-migration.md) | CDC, cutover, dual-write |
| [Limitations](docs/known-gaps.md) | Current limits |

## Integrity model

```text
read page → write rows → checkpoint progress → only then may a unit complete
```

Failed writes fail the unit after retries. Interrupted runs resume from the last
durable checkpoint.

## License

Business Source License 1.1 (BSL). See [`LICENSE`](LICENSE).

Copyright (c) 2026 Santiago Lertora \<santiagolertora@gmail.com\>.
