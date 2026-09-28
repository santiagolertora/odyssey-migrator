# Install

## Requirements

| Tool | Version |
|------|---------|
| Rust | **1.88+** (see workspace `rust-version`) |
| Cargo | comes with rustup |
| Docker | optional — local Cassandra + Scylla demo |
| OS | macOS / Linux (primary); Windows via WSL2 |

Network access to source and target CQL ports (default `9042`).

## Build from source

```bash
git clone <repo-url> odyssey-migrator
cd odyssey-migrator

# Release binary (recommended)
cargo build --release -p odyssey-cli

# Install onto PATH (optional)
cargo install --path crates/odyssey-cli --locked

./target/release/odyssey-migrator --version
./target/release/odyssey-migrator --help
```

The binary name is **`odyssey-migrator`**.

### Debug build (faster compile, slower migrate)

```bash
cargo build -p odyssey-cli
./target/debug/odyssey-migrator --help
```

## Verify the install

```bash
odyssey-migrator --help
odyssey-migrator migrate --help
```

You should see subcommands: `migrate`, `status`, `resume`, `ui`, `validate`,
`live`, `cutover`, `dual-write`, `notify-test`.

## Local demo clusters (Docker)

From the repo root:

```bash
docker compose -f examples/demo/docker-compose.yml up -d
# wait until both CQL ports accept connections (demo README has details)

# seed schema + rows (see examples/demo/README.md)
./examples/demo/seed.sh

cp examples/demo/ferry.toml ./ferry.toml
cargo run --release -p odyssey-cli -- migrate -c ferry.toml
```

Demo layout:

- Cassandra (source): `127.0.0.1:9042`
- Scylla (target): `127.0.0.1:9043`

There is also a thinner compose at `examples/docker-compose.yml` and a tunnel-oriented
lab under `examples/lab/`.

## Config file

Copy and edit:

```bash
cp examples/ferry.toml ./ferry.toml
```

Minimum you must set:

1. `[source].contact_points` / `datacenter`
2. `[target].contact_points` / `datacenter`
3. `[[tables]]` `source` / `target` as `keyspace.table`
4. `[checkpoint].path` (writable SQLite path)

Full reference: [configuration.md](configuration.md).

## Next

- [Quickstart](quickstart.md) — run your first migration
- [Commands](commands.md) — CLI reference
