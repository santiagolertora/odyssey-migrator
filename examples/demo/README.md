# Demo cluster (official images)

**Default:** 1× `cassandra:4.1` + 1× `scylladb/scylla:5.4` (fits a normal Docker Desktop).

## If `seed.sh` hangs

Docker Desktop is often stuck (API no longer answers `docker ps`). Then:

1. Ctrl+C the seed script  
2. Restart Docker Desktop  
3. Raise RAM if needed (Settings → Resources → Memory ≥ 4 GB for 1+1)  
4. Re-run `./examples/demo/seed.sh`

Six nodes (3+3) need ~8–10 GB inside Docker and healthcheck chains; skip until 1+1 works.

## Run

```bash
cd /Users/santiagolertora/Developement/odyssey-migrator
./examples/demo/seed.sh
# Open http://127.0.0.1:9080/ while migrate runs (demo holds the UI ~120s after Done)
cargo run -p odyssey-cli -- migrate --config examples/demo/ferry.toml
# Or reopen the last completed migration dashboard anytime:
cargo run -p odyssey-cli -- ui --config examples/demo/ferry.toml
cargo run -p odyssey-cli -- validate <migration-id> --config examples/demo/ferry.toml
```

See [`docs/benchmark.md`](../docs/benchmark.md) and:

```bash
ROWS=100000 VALIDATE=1 ./examples/demo/benchmark.sh
```


```bash
docker compose -f examples/demo/docker-compose.yml down -v
```

## Ports

| Role      | Port  |
|-----------|-------|
| Cassandra | 9042  |
| Scylla    | 9043  |

`ferry.toml` sets `contact_points_only = true` on both ends. Without that (or
without pinning), drivers discover Docker-internal peer IPs and queries from the
host fail (`empty plan` / connect timeout). Odyssey remaps peers to the published
localhost ports when `contact_points_only` is set without explicit translations.
