# Live migration with Odyssey (CDC catch-up)

> Docs index: [README](README.md) · [Commands](commands.md) · [Configuration](configuration.md)

Bulk copy alone is a snapshot race: any UPDATE or DELETE that lands while ranges
are still being read can leave the target wrong. Odyssey's live path uses **Scylla
CDC**, not CommitLog. For Cassandra→Scylla (no Scylla CDC on source), use the
**dual-write gateway** during the cutover window.

## Mental model

```text
1. Enable CDC on source tables (Scylla→Scylla) — or dual-write for Cassandra source
2. Note watermark (or use live.overlap_secs)
3. Bulk migrate (odyssey-migrator migrate)
4. Catch up CDC (odyssey-migrator live / migrate --with-live)
5. Quiesce app writes — or point writers at dual-write gateway when lag is small
6. Final catch-up + validate
7. Cut over reads to Scylla
```

This is the same shape as “base backup + binlog”, with CDC playing the binlog role.

## Prerequisites

Source table must have CDC enabled (Scylla→Scylla live):

```cql
ALTER TABLE ks.events WITH cdc = {'enabled': true};
```

Odyssey probes for `ks.events_scylla_cdc_log` and refuses to invent progress if it
is missing.

## Config

```toml
[source]
consistency = "local_quorum"

[target]
consistency = "local_quorum"

[live]
enabled = false
window_secs = 60
safety_secs = 30
sleep_secs = 10
overlap_secs = 60
# start_at = "2026-09-08T18:00:00Z"
target_lag_ms = 500

# Optional: app dual-write during cutover (Cassandra→Scylla or extra safety)
[dual_write]
enabled = true
listen_addr = "127.0.0.1:8091"
# auth_token = "shared-secret"
require_both = true
```

## Commands

One-shot catch-up until now (+ grace):

```bash
odyssey-migrator live --config ferry.toml
odyssey-migrator live --config ferry.toml --until 2026-09-08T20:00:00Z
```

Bulk then catch-up:

```bash
odyssey-migrator migrate --config ferry.toml --with-live
```

Ops checklist (CDC catch-up → quiesce → final catch-up):

```bash
odyssey-migrator cutover --config ferry.toml
```

HTTP dual-write gateway (source then target):

```bash
odyssey-migrator dual-write --config ferry.toml

curl -sS -X POST http://127.0.0.1:8091/v1/mutate \
  -H 'content-type: application/json' \
  -d '{"table":"ks.events","op":"insert","columns":{"pk":"a","v":"x"}}'
```

Supported `op` values: `insert`, `update`, `delete_row`, `delete_partition`.
Delivery is at-least-once: source is written first; if target fails the client
must retry (idempotent PK writes).

## What gets applied (CDC)

| CDC op | Target action |
|---|---|
| PreImage | ignored |
| RowInsert / RowUpdate / PostImage | INSERT (upsert) of PK/CK + present cells; cell deletes → `DELETE col` |
| RowDelete | DELETE row |
| PartitionDelete | DELETE partition |
| Range deletes | DELETE with CK bounds (composite CK: equality prefix + last non-null inequality) |

Delivery stays at-least-once. Prefer rewriting a PK over losing a mutate.

## Cluster topology / RF

Odyssey does not drain one node. Contact points are entry points; the driver is
token-aware. Reads/writes use configured consistency (default `LOCAL_QUORUM`).
With RF=3, a single replica never has to hold “the whole table” for the migration
to be correct.

## Limits

- Unfrozen **lists**: no per-element TTL/WRITETIME in CQL — Level-A uniform only.
- Unfrozen **maps/sets**: opt-in `engine.preserve_collection_elements` (needs cluster support for `WRITETIME(col[k])`).
- No automatic traffic flip — ops still owns DNS/LB cutover after validate.
- Cassandra-as-source CDC differs from Scylla CDC; use dual-write or a Cassandra-compatible change feed.

## Why not CommitLog?

CommitLog is a per-node crash-recovery WAL. It is not a supported cross-cluster
replication API. CDC is.
