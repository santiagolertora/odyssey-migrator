# Lab smoke: SRC → OUR scylladb1

## Size on SRC (live measurement)

| Metric | Value |
|--------|--------|
| Table | `benchmark.key_value` |
| Live space (one SRC node tablestats) | **~46 GB** (`46060696971` bytes) |
| Partitions (estimate) | **~43.1M** |
| SRC node load (`nodetool status`) | ~43 GB × 3 nodes |
| OUR `scylladb1` free disk | **~4.7 GB** on `/` |

**Full copy will not fit** on the current target disk. The bundled `ferry.toml` sets `engine.max_units = 2` so only ~2 token ranges (~2/64 of the ring with 4 workers) are copied for smoke + validate.

## Schema

With `target.create_schema = true`, Odyssey creates a minimal keyspace/table on
**OUR target only** if missing (RF from `create_schema_rf`). SRC is never altered.
`./examples/lab/03-prepare-target.sh` is optional now.


| Cluster | Allowed |
|---------|---------|
| Lab SRC | **Read only** (tunnel + SELECT). No schema/DDL/DML writes. |
| Lab DST / `scylla-migrator` | **Do not touch** |
| OUR `scylladb1` (+ peers) | Create `benchmark.key_value` + INSERT via Odyssey only |

Odyssey itself only `SELECT`s on source and `INSERT`s on target.

## Run

From repo root:

```bash
chmod +x examples/lab/*.sh

# 1–3 + plan + migrate
./examples/lab/04-run-smoke.sh

# 4. validate (use id printed by migrate)
cargo run -p odyssey-cli --release -- validate <migration-id> --config examples/lab/ferry.toml
```

Step by step:

```bash
./examples/lab/01-tunnel-src.sh
./examples/lab/02-smoke-src-readonly.sh
./examples/lab/03-prepare-target.sh
cargo run -p odyssey-cli --release -- migrate --config examples/lab/ferry.toml --plan-only
cargo run -p odyssey-cli --release -- migrate --config examples/lab/ferry.toml
```

## After smoke

To copy more: raise `max_units` (or remove it) **only** after expanding disk on OUR nodes enough for ~46 GB × RF. Target schema today uses **RF=1** to save space during smoke.
