# Configuration

Odyssey is configured with a single TOML file (commonly `ferry.toml`).

```bash
cp examples/ferry.toml ./ferry.toml
odyssey-migrator migrate -c ferry.toml
```

Sections with `#[serde(default)]` may be omitted; Odyssey fills safe defaults.

## `[migration]`

| Key | Type | Notes |
|-----|------|-------|
| `name` | string | Human label for logs / UI |

## `[source]` / `[target]`

| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `kind` | string | `"cql"` | Source only; V0 only supports `cql` |
| `contact_points` | string[] | — | Host:port list |
| `datacenter` | string | — | Local DC for token-aware routing |
| `consistency` | string | `local_quorum` | `one`, `quorum`, `local_one`, `local_quorum`, … |
| `username` / `password` | string | unset | Optional auth |
| `contact_points_only` | bool | `false` | Ignore peer discovery (SSH tunnels) |
| `address_translations` | list | `[]` | `{ from = "ip:port", to = "127.0.0.1:port" }` |
| `create_schema` | bool | `false` | **Target only** — create missing KS/table from source |
| `create_schema_rf` | u32 | `1` | RF when creating keyspace |

## `[[tables]]`

Repeatable. Each entry:

```toml
[[tables]]
source = "ks.events"
target = "ks.events"
```

Fully-qualified `keyspace.table` required.

## `[engine]`

| Key | Default | Notes |
|-----|---------|-------|
| `workers` | `32` | Parallel unit workers |
| `page_size` | `5000` | Source page size |
| `write_batch_size` | `10` | UNLOGGED batch size (`1` = single-row) |
| `preserve_writetimes` | `false` | Level-A max `WRITETIME` → `USING TIMESTAMP` |
| `preserve_ttls` | `false` | Level-A max `TTL` → `USING TTL` |
| `preserve_frozen_collections` | `false` | Include frozen collections in Level-A max |
| `preserve_collection_elements` | `false` | Per-element meta for unfrozen maps/sets |
| `allow_counters` | `false` | Migrate counters via `UPDATE … + ?` |
| `max_units` | unset | Cap planned ranges (smoke tests) |

### `[engine.concurrency]`

| Key | Default |
|-----|---------|
| `initial` | `64` |
| `minimum` | `8` |
| `maximum` | `512` |

### `[engine.adaptive]`

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | AIMD concurrency |
| `target_p99_ms` | `15` | Latency target |

## `[checkpoint]`

| Key | Default | Notes |
|-----|---------|-------|
| `path` | `./.odyssey/migration.db` | SQLite file |
| `interval_secs` | `5` | Min seconds between mid-range progress saves |

## `[validation]`

| Key | Default | Notes |
|-----|---------|-------|
| `mode` | `digest` | `digest` / `sample` / `full` |
| `sample_size` | `1000` | Rows per range in sample mode |
| `compare_timestamps` | `false` | Include max WRITETIME/TTL in compare |
| `writetime_tolerance_us` | `1000000` | Sample pairwise tolerance |
| `ttl_tolerance_secs` | `60` | Sample pairwise tolerance |

## `[logging]`

| Key | Default | Notes |
|-----|---------|-------|
| `verbosity` | `info` | `error` / `warn` / `info` / `debug` / `trace` |
| `format` | `text` | `text` / `json` |
| `file` | unset | Optional log file path |

CLI `-v` / `-q` / `--log-format` / `--log-file` override these.

## `[metrics]`

| Key | Default |
|-----|---------|
| `enabled` | `false` (check example; demo often `true`) |
| `listen_addr` | `127.0.0.1:9100` |

Scrapes `GET /metrics` (Prometheus text).

## `[ui]`

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `false` | Dashboard during migrate/resume |
| `listen_addr` | `127.0.0.1:9080` | HTML + `/api/status` |
| `hold_secs` | `0` | Keep process alive N seconds after finish |

## `[live]`

Scylla CDC catch-up. See [live-migration.md](live-migration.md).

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `false` | Required for `cutover` unless `--force` |
| `window_secs` | `60` | Catch-up window sizing |
| `safety_secs` | `30` | Extra end margin |
| `sleep_secs` | `10` | Poll sleep |
| `overlap_secs` | `60` | Overlap before bulk end |
| `start_at` | unset | RFC3339 start watermark |
| `target_lag_ms` | `500` | Cutover lag hint |

## `[dual_write]`

HTTP gateway for app mutations during cutover.

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `false` | `dual-write` command checks this (or `--force`) |
| `listen_addr` | `127.0.0.1:8091` | Bind address |
| `auth_token` | `""` | If set, require `Authorization: Bearer …` |
| `require_both` | `true` | Fail request if target write fails |

## `[notify]`

| Key | Default | Notes |
|-----|---------|-------|
| `webhook_url` | `""` | Empty = disabled |
| `on_complete` | `true` | |
| `on_error` | `true` | |
| `timeout_secs` | `5` | |

Test with `odyssey-migrator notify-test -c ferry.toml`.

## `[planner]`

| Key | Default | Notes |
|-----|---------|-------|
| `mode` | `even` | `even` = ring split; `vnode` = topology-aware |

## `[mesh]`

Multi-process claim hardening on a **shared** SQLite checkpoint.

| Key | Default | Notes |
|-----|---------|-------|
| `worker_id` | hostname | Claim identity |
| `lease_secs` | (see code defaults) | Stale `running` reclaim |
| `busy_timeout_ms` | (see code defaults) | SQLite busy timeout |

## `[integrity]`

Fail-closed knobs. Do not weaken in production.

| Key | Default | Notes |
|-----|---------|-------|
| `guarantee` | `at_least_once` | |
| `fail_unit_on_write_error` | `true` | Must stay true |
| `require_checkpoint_before_complete` | `true` | |
| `checkpoint_on_cancel` | `true` | |

## Minimal example

```toml
[migration]
name = "demo"

[source]
contact_points = ["127.0.0.1:9042"]
datacenter = "datacenter1"

[target]
contact_points = ["127.0.0.1:9043"]
datacenter = "datacenter1"
create_schema = true

[[tables]]
source = "demo.events"
target = "demo.events"

[checkpoint]
path = "./.odyssey/migration.db"
```
