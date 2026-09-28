#!/usr/bin/env bash
# Create keyspace/table on OUR scylladb1 only. Never touches lab SRC/DST/migrator.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCHEMA="$ROOT/examples/lab/schema-target.cql"

echo "Applying schema on scylladb1 (161.22.44.52) via SSH + local cqlsh..."
# Pipe CQL over SSH into cqlsh on the node (AllowAll — no auth)
ssh -o BatchMode=yes scylladb1 "cqlsh -e \"$(cat "$SCHEMA" | tr '\n' ' ')\""

echo
echo "Verify on target:"
ssh -o BatchMode=yes scylladb1 \
  "cqlsh -e \"DESC KEYSPACE benchmark; SELECT COUNT(*) FROM benchmark.key_value;\""

echo
echo "Target ready. Disk reminder: full SRC table is ~46GB; smoke uses max_units only."
