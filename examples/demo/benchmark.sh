#!/usr/bin/env bash
# Wall-clock Odyssey Migrator benchmark on the local 1+1 demo Dockers.
#
# Usage:
#   ROWS=100000 ./examples/demo/benchmark.sh
#   ROWS=100000 VALIDATE=1 ./examples/demo/benchmark.sh
#
# Spark Migrator comparison is documented in docs/benchmark.md (needs a Spark
# cluster + JAR — not part of this 1+1 compose).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ROWS="${ROWS:-100000}"
VALIDATE="${VALIDATE:-0}"
CFG="$ROOT/examples/demo/ferry-bench.toml"
CASS=ferry-demo-cassandra-1
SCYLLA=ferry-demo-scylla-1
BIN="${BIN:-$ROOT/target/release/odyssey-migrator}"

cd "$ROOT"

if [[ ! -x "$BIN" ]]; then
  echo "==> building release binary"
  cargo build --release -p odyssey-cli
fi

echo "==> ensure demo containers are up"
./examples/demo/seed.sh >/tmp/odyssey-bench-seed.log 2>&1 || {
  echo "seed failed; see /tmp/odyssey-bench-seed.log" >&2
  exit 1
}

# seed.sh loaded ROWS from its env; re-run load if we need a different size
if [[ "${ROWS}" != "2000" ]]; then
  echo "==> reseeding Cassandra with ROWS=$ROWS"
  ROWS="$ROWS" ./examples/demo/seed.sh >/tmp/odyssey-bench-seed2.log 2>&1
fi

echo "==> truncate target + fresh checkpoint"
docker exec "$SCYLLA" cqlsh -e "TRUNCATE ferry_demo.events;" >/dev/null
rm -f "$ROOT/.odyssey/bench-migration.db" "$ROOT/.odyssey/bench-migration.db-journal"

SRC_COUNT=$(docker exec "$CASS" cqlsh -e "SELECT COUNT(*) FROM ferry_demo.events;" | awk '/^[ ]*[0-9]+/ {print $1; exit}')
echo "    source rows≈ $SRC_COUNT"

echo "==> migrate (release)"
START=$(date +%s.%N)
OUT=$(mktemp)
set +e
"$BIN" migrate --config "$CFG" >"$OUT" 2>&1
RC=$?
set -e
END=$(date +%s.%N)
ELAPSED=$(awk -v s="$START" -v e="$END" 'BEGIN{printf "%.3f", e-s}')

MIG_ID=$(awk '/^  migration /{print $2; exit}' "$OUT" || true)
ROWS_W=$(awk '/^  rows written /{print $3; exit}' "$OUT" || true)
BYTES=$(awk '/^  bytes written /{print $3; exit}' "$OUT" || true)

echo
echo "Odyssey Migrator bench"
echo "  rows requested  $ROWS"
echo "  source count    $SRC_COUNT"
echo "  rows written    ${ROWS_W:-?}"
echo "  bytes written   ${BYTES:-?}"
echo "  wall seconds    $ELAPSED"
echo "  migration id    ${MIG_ID:-?}"
echo "  exit code       $RC"
if [[ -n "${ROWS_W:-}" && "$ELAPSED" != "0.000" ]]; then
  RPS=$(awk -v r="$ROWS_W" -v t="$ELAPSED" 'BEGIN{printf "%.0f", r/t}')
  echo "  approx rows/s   $RPS"
fi

if [[ "$RC" -ne 0 ]]; then
  echo "--- migrate output ---" >&2
  cat "$OUT" >&2
  exit "$RC"
fi

if [[ "$VALIDATE" == "1" && -n "${MIG_ID:-}" ]]; then
  echo
  echo "==> validate digest"
  "$BIN" validate "$MIG_ID" --config "$CFG"
fi

echo
echo "Full migrate log: $OUT"
echo "Spark comparison: see docs/benchmark.md"
