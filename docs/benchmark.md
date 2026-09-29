# Benchmark notes (Odyssey Migrator vs Spark / Scylla Migrator)

## What this repo measures locally

The demo compose is **1× Cassandra + 1× Scylla** on Docker Desktop. That is enough
for a **repeatable Odyssey baseline**, not a fair multi-node Spark shootout.

```bash
cargo build --release -p odyssey-cli
ROWS=100000 VALIDATE=1 ./examples/demo/benchmark.sh
```

The script truncates the target, uses a fresh SQLite checkpoint, disables the UI
hold, and prints wall-clock seconds + approx rows/s.

## Fair Spark comparison (separate lab)

Scylla Migrator needs:

1. Spark master + workers (or `local[*]` for a toy run)
2. The Scylla Migrator assembly JAR
3. Matching source/target schemas and row count
4. Similar consistency / batching knobs

On a laptop `local[*]` Spark run is usually **JVM + driver overhead dominated**
and understates Spark at cluster scale. For comparable numbers, run both
tools against the **same 3-node Cassandra → 3-node Scylla** lab and record:

| Tool | Rows | Wall s | rows/s | Notes |
|------|------|--------|--------|-------|
| Odyssey Migrator | | | | `benchmark.sh` |
| Spark Migrator | | | | `spark-submit …` |

Keep `preserve_ttls` / `preserve_writetimes` and consistency levels aligned when
comparing apples-to-apples.

## Why Odyssey can win on ops even when Spark is close on throughput

- No Spark master/workers/JAR/submit ceremony
- SQLite resume without rewriting a Spark job
- Built-in dashboard + Prometheus + validate in one binary

Document both **throughput** and **time-to-first-successful-migrate** (human ops).
