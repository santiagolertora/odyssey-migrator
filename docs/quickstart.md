# Quickstart

Goal: copy one table, resume if interrupted, then validate digests.

## 1. Install and config

```bash
cargo build --release -p odyssey-cli
cp examples/ferry.toml ./ferry.toml
# edit contact points + [[tables]]
```

Ensure the **target** keyspace/table exist (or set `target.create_schema = true`).

## 2. Plan only (no writes)

```bash
./target/release/odyssey-migrator migrate -c ferry.toml --plan-only
```

Confirms connect + schema discovery + token-range planning without copying rows.

## 3. Bulk migrate

```bash
./target/release/odyssey-migrator migrate -c ferry.toml
```

Notes:

- Checkpoints land in `[checkpoint].path` (default `./.odyssey/migration.db`).
- If `[ui] enabled = true`, open the printed dashboard URL (default `http://127.0.0.1:9080/`).
- Keep the UI after finish: `--ui-hold` or `[ui].hold_secs`.

Copy the printed **migration id** for status / resume / validate.

## 4. Status

```bash
./target/release/odyssey-migrator status <migration-id> -c ferry.toml
```

## 5. Interrupt / resume

Ctrl+C mid-migrate is safe. Resume:

```bash
./target/release/odyssey-migrator resume <migration-id> -c ferry.toml
```

Resume re-reads incomplete token ranges (at-least-once: some PKs may be rewritten).

## 6. Validate

```bash
./target/release/odyssey-migrator validate <migration-id> -c ferry.toml
```

Modes come from `[validation].mode` (`digest` / `sample` / `full`). Digests must match
before you trust a cutover.

## 7. Live traffic (optional)

**Scylla source with CDC:**

```bash
ALTER TABLE ks.events WITH cdc = {'enabled': true};
# set [live] enabled = true in ferry.toml
./target/release/odyssey-migrator migrate -c ferry.toml --with-live
./target/release/odyssey-migrator cutover -c ferry.toml
```

**Cassandra source (no Scylla CDC):** run dual-write while finishing catch-up:

```bash
# [dual_write] enabled = true
./target/release/odyssey-migrator dual-write -c ferry.toml
```

Details: [live-migration.md](live-migration.md).

## Logging

```bash
odyssey-migrator -v migrate -c ferry.toml          # debug
odyssey-migrator -vv --log-format json migrate -c ferry.toml
odyssey-migrator -q status <id> -c ferry.toml      # quieter
```

## Metrics

When `[metrics] enabled = true`:

```text
http://127.0.0.1:9100/metrics
```
