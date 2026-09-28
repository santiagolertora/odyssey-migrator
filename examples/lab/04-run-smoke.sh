#!/usr/bin/env bash
# End-to-end lab smoke: tunnel → (assume schema) → plan-only → migrate → validate
# Writes ONLY to OUR cluster. SRC is read-only.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
CFG=examples/lab/ferry.toml

"$ROOT/examples/lab/01-tunnel-src.sh"
"$ROOT/examples/lab/02-smoke-src-readonly.sh"
"$ROOT/examples/lab/03-prepare-target.sh"

echo
echo "== plan-only (no data moved) =="
cargo run -p odyssey-cli --release -- migrate --config "$CFG" --plan-only

echo
echo "== migrate smoke (max_units from ferry.toml) =="
cargo run -p odyssey-cli --release -- migrate --config "$CFG"

echo
echo "When migrate prints a migration id, validate with:"
echo "  cargo run -p odyssey-cli --release -- validate <migration-id> --config $CFG"
