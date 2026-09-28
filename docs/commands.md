# Commands

Binary: **`odyssey-migrator`**

```bash
odyssey-migrator [GLOBAL OPTIONS] <COMMAND> …
odyssey-migrator <COMMAND> --help
```

## Global options

| Flag | Meaning |
|------|---------|
| `-v`, `-vv` | More verbose logs (`debug` / `trace`). Overrides `[logging].verbosity`. |
| `-q`, `--quiet` | Warnings and above only. Conflicts with `-v`. |
| `--log-format <text\|json>` | Log format (default from config / text). |
| `--log-file <PATH>` | Also write logs to this file. |
| `-h`, `--help` | Help. |
| `-V`, `--version` | Version. |

---

## `migrate`

Plan and run bulk token-range copy.

```bash
odyssey-migrator migrate -c ferry.toml
odyssey-migrator migrate -c ferry.toml --plan-only
odyssey-migrator migrate -c ferry.toml --with-live
odyssey-migrator migrate -c ferry.toml --ui-addr 127.0.0.1:19080 --ui-hold
```

| Flag | Meaning |
|------|---------|
| `-c, --config <FILE>` | TOML config (**required**). |
| `--plan-only` | Discover schema + plan ranges; do not copy. |
| `--with-live` | After bulk, run CDC catch-up (needs `[live]` + CDC on source). |
| `--ui-addr <ADDR>` | Override `[ui].listen_addr`. |
| `--ui-hold` | Keep dashboard until Ctrl+C after finish. |

---

## `status`

Print checkpoint progress for a migration id.

```bash
odyssey-migrator status <migration-id> -c ferry.toml
odyssey-migrator status <migration-id> --checkpoint ./.odyssey/migration.db
```

| Flag | Meaning |
|------|---------|
| `-c, --config <FILE>` | Optional; used to resolve checkpoint path + logging. |
| `--checkpoint <PATH>` | SQLite path override. |

---

## `resume`

Continue an interrupted bulk migration from its checkpoint.

```bash
odyssey-migrator resume <migration-id> -c ferry.toml
odyssey-migrator resume <migration-id> -c ferry.toml --ui-hold
```

| Flag | Meaning |
|------|---------|
| `-c, --config <FILE>` | TOML config (**required**). |
| `--checkpoint <PATH>` | SQLite path override. |
| `--ui-addr <ADDR>` | Override UI bind. |
| `--ui-hold` | Keep dashboard until Ctrl+C. |

---

## `ui`

Serve the HTML dashboard for an existing migration (Ctrl+C to exit).

```bash
odyssey-migrator ui -c ferry.toml
odyssey-migrator ui -c ferry.toml <migration-id>
```

If `migration_id` is omitted, uses the most recently created migration in the checkpoint.

---

## `validate`

Compare source vs target for the migration’s planned token ranges.

```bash
odyssey-migrator validate <migration-id> -c ferry.toml
```

Behavior is controlled by `[validation]` (`mode`, sample size, timestamp compare).

---

## `live`

One-shot Scylla CDC catch-up window.

```bash
odyssey-migrator live -c ferry.toml
odyssey-migrator live -c ferry.toml --until 2026-09-12T18:00:00Z
odyssey-migrator live -c ferry.toml --until-grace-secs 60
```

| Flag | Meaning |
|------|---------|
| `-c, --config <FILE>` | TOML (**required**); source tables need CDC. |
| `--until <RFC3339>` | End watermark. Default: now + grace. |
| `--until-grace-secs <N>` | Seconds ahead of now when `--until` omitted (default 30). |

---

## `cutover`

Interactive ops checklist: CDC catch-up → confirm quiesce / dual-write → final catch-up.

```bash
odyssey-migrator cutover -c ferry.toml
odyssey-migrator cutover -c ferry.toml --yes
odyssey-migrator cutover -c ferry.toml --force   # allow when [live] enabled = false
```

Does **not** flip application traffic. For app writes during cutover see `dual-write`.

---

## `dual-write`

HTTP gateway: structured mutations → **source then target** (at-least-once).

```bash
odyssey-migrator dual-write -c ferry.toml
odyssey-migrator dual-write -c ferry.toml --listen-addr 127.0.0.1:8091 --force
```

| Flag | Meaning |
|------|---------|
| `-c, --config <FILE>` | TOML (**required**). |
| `--listen-addr <ADDR>` | Override `[dual_write].listen_addr`. |
| `--force` | Start even if `[dual_write] enabled = false`. |

Endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness |
| `POST` | `/v1/mutate` | Apply mutation JSON |

Example body:

```json
{
  "table": "ks.events",
  "op": "insert",
  "columns": { "pk": "a", "v": "x" },
  "ttl_secs": 3600
}
```

`op`: `insert` | `update` | `delete_row` | `delete_partition`.  
`table` must match a `[[tables]].source` FQN. Optional `Authorization: Bearer <token>` when `[dual_write].auth_token` is set.

---

## `notify-test`

POST a sample payload to `[notify].webhook_url` (Slack-compatible JSON).

```bash
odyssey-migrator notify-test -c ferry.toml
odyssey-migrator notify-test -c ferry.toml --error
```

---

## Suggested order of operations

```text
migrate [--plan-only]
   → migrate / resume
   → status / ui
   → live  (or migrate --with-live)
   → dual-write  (Cassandra source / cutover window)
   → cutover
   → validate
   → flip reads to target
```
