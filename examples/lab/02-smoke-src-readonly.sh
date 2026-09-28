#!/usr/bin/env bash
# Read-only smoke against lab SRC through the tunnel. No writes.
set -euo pipefail

PORT="${SRC_TUNNEL_PORT:-19042}"
USER="${SRC_USER:-scylla_user}"
PASS="${SRC_PASS:-SecretP@ssw0rd}"

if ! lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "no tunnel on :$PORT — run ./examples/lab/01-tunnel-src.sh first" >&2
  exit 1
fi

if ! command -v cqlsh >/dev/null 2>&1; then
  echo "cqlsh not found on PATH; skip local cqlsh or use Docker cqlsh" >&2
  echo "Trying python-free SSH check on SRC instead (nodetool only)..."
  ssh -o BatchMode=yes scylla-src-1 'nodetool tablestats benchmark.key_value 2>/dev/null | egrep "Space used \(live\)|partitions"'
  exit 0
fi

echo "== DESC KEYSPACE benchmark (read-only) =="
cqlsh 127.0.0.1 "$PORT" -u "$USER" -p "$PASS" -e "DESC KEYSPACE benchmark" | head -80

echo
echo "== sample keys (LIMIT 5) =="
cqlsh 127.0.0.1 "$PORT" -u "$USER" -p "$PASS" --request-timeout=60 \
  -e "SELECT key FROM benchmark.key_value LIMIT 5;"

echo
echo "OK — SRC readable. Do not COUNT(*) (times out on this table)."
