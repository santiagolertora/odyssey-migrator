#!/usr/bin/env bash
# Start 1× Cassandra + 1× Scylla (official images), schema, sample rows.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/examples/demo/docker-compose.yml")
ROWS="${ROWS:-2000}"
CASS=ferry-demo-cassandra-1
SCYLLA=ferry-demo-scylla-1

wait_up() {
  local c="$1"
  echo "waiting for $c ..."
  for i in $(seq 1 60); do
    if docker exec "$c" cqlsh -e "SELECT now() FROM system.local" >/dev/null 2>&1; then
      echo "  $c ready (${i}×5s)"
      return 0
    fi
    sleep 5
  done
  echo "timeout: $c — check: docker logs $c" >&2
  exit 1
}

echo "==> docker compose up -d (cassandra:4.1 + scylladb/scylla:5.4)"
"${COMPOSE[@]}" up -d --remove-orphans

wait_up "$CASS"
wait_up "$SCYLLA"

echo "==> schema"
docker exec -i "$CASS" cqlsh <"$ROOT/examples/demo/schema.cql"
docker exec -i "$SCYLLA" cqlsh <"$ROOT/examples/demo/schema.cql"

echo "==> loading $ROWS rows into Cassandra only"
docker exec "$CASS" cqlsh -e "TRUNCATE ferry_demo.events;" >/dev/null 2>&1 || true
TMP="$(mktemp)"
{
  echo "USE ferry_demo;"
  for i in $(seq 0 $((ROWS - 1))); do
    tenant=$((i % 50))
    if (( i % 10 == 0 )); then
      echo "INSERT INTO events (tenant_id, event_id, status, payload) VALUES ('tenant-${tenant}', ${i}, 'active', 'payload-${i}') USING TTL 86400;"
    else
      echo "INSERT INTO events (tenant_id, event_id, status, payload) VALUES ('tenant-${tenant}', ${i}, 'active', 'payload-${i}');"
    fi
  done
} >"$TMP"
docker exec -i "$CASS" cqlsh <"$TMP"
rm -f "$TMP"

echo "==> counts"
docker exec "$CASS" cqlsh -e "SELECT COUNT(*) FROM ferry_demo.events;"
docker exec "$SCYLLA" cqlsh -e "SELECT COUNT(*) FROM ferry_demo.events;"

echo
echo "Ready:"
echo "  cargo run -p odyssey-cli -- migrate --config examples/demo/ferry.toml"
