# Odyssey Migrator documentation

Spark-free CQL → ScyllaDB migration: bulk copy, resume, validate, CDC live catch-up,
and an HTTP dual-write gateway for cutover.

**Durability:** at-least-once. Odyssey will rewrite a primary key rather than lose a
row that was already read from the source.

## Start here

| Doc | What it covers |
|-----|----------------|
| [Install](install.md) | Rust toolchain, build binary, local demo with Docker |
| [Quickstart](quickstart.md) | First successful migrate → validate loop |
| [Commands](commands.md) | Every CLI command and flag |
| [Configuration](configuration.md) | TOML reference (`ferry.toml`) |
| [Live migration](live-migration.md) | CDC catch-up, cutover, dual-write |
| [Limitations](known-gaps.md) | What is still partial or out of scope |
| [Benchmark](benchmark.md) | Local throughput notes vs Spark |

## Typical workflows

**Bulk only (snapshot race — no live traffic):**

```bash
odyssey-migrator migrate -c ferry.toml
odyssey-migrator validate <migration-id> -c ferry.toml
```

**Scylla → Scylla with CDC:**

```bash
# CDC enabled on source tables first
odyssey-migrator migrate -c ferry.toml --with-live
odyssey-migrator cutover -c ferry.toml
odyssey-migrator validate <migration-id> -c ferry.toml
```

**Cassandra → Scylla (no Scylla CDC on source):**

```bash
odyssey-migrator migrate -c ferry.toml
odyssey-migrator dual-write -c ferry.toml   # point app writers here
odyssey-migrator validate <migration-id> -c ferry.toml
```

## CLI help

```bash
odyssey-migrator --help
odyssey-migrator migrate --help
odyssey-migrator dual-write --help
```
