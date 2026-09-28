#!/usr/bin/env bash
# Open read-only CQL tunnel: Mac :19042 → lab SRC 10.108.0.86:9042
set -euo pipefail

if lsof -nP -iTCP:19042 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "tunnel already up on 127.0.0.1:19042"
  lsof -nP -iTCP:19042 -sTCP:LISTEN
  exit 0
fi

echo "opening SRC tunnel (ssh -N -L 19042:10.108.0.86:9042 scylla-src-1)..."
ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L 19042:10.108.0.86:9042 \
  scylla-src-1

lsof -nP -iTCP:19042 -sTCP:LISTEN
echo "SRC CQL via 127.0.0.1:19042 (read path only)"
