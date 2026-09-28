# Current limitations

Limits of Odyssey Migrator V0.x. Prefer fail-closed behaviour over pretending
these are finished.

## Data model

| Gap | Status | Notes |
|-----|--------|-------|
| Per-cell TTL / WRITETIME on collections | Done (maps/sets) | Level-A max for scalars + frozen; `engine.preserve_collection_elements` overlays unfrozen map/set elements via `WRITETIME(col[k])` / element `UPDATE`. Lists stay Level-A (uniform) — CQL has no per-index TTL. |
| Counter tables | Done (opt-in) | `engine.allow_counters = true` migrates via `UPDATE … SET c = c + ?`. Default remains refuse. |
| UDTs / frozen nested edge cases | Partial | Basic types work; exotic schemas untested |

## CDC live catch-up

| Gap | Status | Notes |
|-----|--------|-------|
| Column-level deletes | Done | Emits `DELETE col FROM … WHERE pk/ck` when CDC marks cells deleted |
| Range deletes | Done | Single and composite CK bounds (equality prefix + last-non-null inequality); open-ended CDC markers → Ignore |
| Cassandra-as-source CDC | Out of scope | Path targets **Scylla CDC** on the source |
| Dual-write cutover | Done (MVP) | Checklist: `cutover`. App writes: `odyssey-migrator dual-write` HTTP gateway (`POST /v1/mutate`) |

See [`live-migration.md`](live-migration.md).

## Planner / scale-out

| Gap | Status | Notes |
|-----|--------|-------|
| Topology / vnode-aware planning | Done | `[planner] mode = "vnode"` discovers tokens and subdivides |
| Multi-host checkpoint mesh | Done (MVP) | Shared SQLite + `busy_timeout`, `claimed_by`, heartbeat reclaim via `[mesh]` |

## Notifications

| Gap | Status | Notes |
|-----|--------|-------|
| Webhook / Slack on complete or error | Done | `[notify] webhook_url`; verify with `odyssey-migrator notify-test -c ferry.toml` |

## Progress / UI

| Gap | Status | Notes |
|-----|--------|-------|
| Token progress | Done | `last_token` weighted % |
| ETA | Done | Dashboard ETA from progress % × uptime |

## Tests

| Gap | Status | Notes |
|-----|--------|-------|
| testcontainers integration | Done | `cargo test -p odyssey-integration -- --ignored` (needs Docker) |
