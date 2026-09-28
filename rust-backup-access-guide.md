# Access & Environment Guide — Local Rust Backup / Migrator

**Audience:** team building / running our Rust backup tool (Scylla Migrator–like)  
**Constraint:** the tool runs **on our Mac (local)**. Do **not** install Spark, JARs, or our binary on the customer migrator VM (`scylla-migrator`).  
**Goal for this test:** read `benchmark.key_value` from the **lab SRC** cluster and write schema (+ optionally data) into **our** nodes `scylladb1` / `scylladb2` / `scylladb3` (one node is enough for a first test).

> Internal use only. Contains SSH aliases and CQL credentials for the interview lab.

---

## 1. Picture of the environments

```text
                         ┌─────────────────────────────────────┐
                         │  Interview LAB (customer AWS)       │
                         │                                     │
  Your Mac               │  SRC cluster (private 10.108.0.x)   │
  (Rust tool here)       │    scylla-src-1  10.108.0.86        │
       │                 │    scylla-src-2  10.108.0.97        │
       │  SSH + LocalForward                                   │
       │  127.0.0.1:19042 ──────► CQL 9042 on 10.108.0.86      │
       │                 │                                     │
       │                 │  DST lab cluster (10.63.0.x)         │
       │                 │    NOT required for this Rust test  │
       │                 │                                     │
       │                 │  scylla-migrator (dual-homed)       │
       │                 │    DO NOT install our tool here     │
                         └─────────────────────────────────────┘

       │
       │  CQL (direct to public IP, or SSH tunnel if needed)
       ▼
┌──────────────────────────────────────┐
│  OUR Scylla nodes (Binlogic)         │
│    scylladb1  161.22.44.52           │
│    scylladb2  103.23.61.242          │
│    scylladb3  103.23.61.242 (*)      │
└──────────────────────────────────────┘

(*) In ~/.ssh/config, scylladb2 and scylladb3 currently share the same
    HostName. Confirm with ops whether that is intentional (same host /
    different service) or a typo before treating them as three nodes.
```

---

## 2. SSH access (already in `~/.ssh/config`)

All lab hosts use key `~/.ssh/scylladb`, user `ubuntu`.  
Our nodes use key `~/.ssh/binlogic`, user `root`.

### 2.1 Lab — SOURCE (read from here)

| Alias | Public IP | Private CQL IP | Role |
|-------|-----------|----------------|------|
| `scylla-src-1` | `54.202.253.39` | `10.108.0.86` | Preferred tunnel jump + CQL |
| `scylla-src-2` | `44.245.3.86` | `10.108.0.97` | SRC node |
| `scylla-src-3` | `44.252.44.128` | `10.108.0.193` | SRC node |

```bash
ssh scylla-src-1
```

From the Mac you **cannot** open `10.108.0.86:9042` directly. Use a tunnel (section 3).

### 2.2 Lab — DESTINATION + migrator (context only)

| Alias | Public IP | Private IP | Notes |
|-------|-----------|------------|--------|
| `scylla-dst-1` | `16.146.251.234` | `10.63.0.165` | Lab DST; not our Rust target |
| `scylla-dst-2` | `52.12.213.58` | `10.63.0.245` | |
| `scylla-dst-3` | `34.217.17.169` | `10.63.0.8` | |
| `scylla-migrator` | `52.89.224.88` | `10.108.0.40` + `10.63.0.27` | Customer Spark migrator — **leave alone** |

Optional lab-DST tunnel (only if you need to compare against lab DST):

```bash
ssh -f -N -L 19043:10.63.0.165:9042 scylla-dst-1
# then CQL at 127.0.0.1:19043
```

### 2.3 OUR targets (write here)

| Alias | HostName | User | Key |
|-------|----------|------|-----|
| `scylladb1` | `161.22.44.52` | `root` | `~/.ssh/binlogic` |
| `scylladb2` | `103.23.61.242` | `root` | `~/.ssh/binlogic` |
| `scylladb3` | `103.23.61.242` | `root` | `~/.ssh/binlogic` |

```bash
ssh scylladb1
ssh scylladb2
# ssh scylladb3   # same IP as scylladb2 today — verify before use
```

**CQL auth on our nodes:** not the lab password. After SSH, check authenticator / create a test user, or use whatever is already configured on that box. Fill section 6 once confirmed.

---

## 3. CQL tunnels from the Mac (existing pattern)

Helper already in this repo: `scripts/tunnels.sh`  
(opens SRC → `:19042` and lab DST → `:19043`).

### 3.1 SRC only (what the Rust tool needs)

```bash
# Lab SRC private CQL via scylla-src-1
ssh -f -N -o ExitOnForwardFailure=yes \
  -L 19042:10.108.0.86:9042 \
  scylla-src-1

# Check
lsof -nP -iTCP:19042 -sTCP:LISTEN
```

From the Rust tool / cqlsh on the Mac:

| Logical endpoint | Connect to |
|------------------|------------|
| Lab SRC CQL | `127.0.0.1:19042` |

### 3.2 Optional tunnel to our `scylladb1` (if 9042 is not open publicly)

```bash
# Forward local 29042 -> scylladb1:9042 (adjust if Scylla listens elsewhere)
ssh -f -N -o ExitOnForwardFailure=yes \
  -L 29042:127.0.0.1:9042 \
  scylladb1
```

If CQL is reachable on the public IP, the tool can use `161.22.44.52:9042` directly and skip this.

### 3.3 Or use the repo script

```bash
cd /path/to/scylladb-exam-interview
./scripts/tunnels.sh
# SRC: 127.0.0.1:19042
# lab DST: 127.0.0.1:19043  (ignore for this test)
```

---

## 4. Lab SRC — CQL credentials & dataset

| Field | Value |
|-------|--------|
| Username | `scylla_user` |
| Password | `SecretP@ssw0rd` |
| Keyspace | `benchmark` |
| Table | `key_value` |
| Approx size | ~few GB / ~43M partitions / ~46 GB live on a SRC node (tablestats) |
| Build | ScyllaDB **2026.3** |

Do **not** use `cassandra` / `cassandra` for this dataset (lab instruction).

Smoke test through the tunnel:

```bash
# with cqlsh or any driver pointed at 127.0.0.1:19042
cqlsh 127.0.0.1 19042 -u scylla_user -p 'SecretP@ssw0rd' \
  -e "DESC KEYSPACE benchmark"

cqlsh 127.0.0.1 19042 -u scylla_user -p 'SecretP@ssw0rd' \
  --request-timeout=60 \
  -e "SELECT key FROM benchmark.key_value LIMIT 5;"
```

`SELECT COUNT(*)` on this table **times out** — expected. Use `LIMIT`, samples, or `nodetool tablestats` on a SRC host instead.

---

## 5. Schema to create on our nodes (`scylladb1` …)

Taken from lab SRC (`DESC KEYSPACE benchmark`). On a **single-node** test box, change replication to RF=1 (or whatever DC name that node uses).

### Lab original (RF=3, DC1, tablets)

```cql
CREATE KEYSPACE benchmark WITH replication = {
  'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy',
  'DC1': '3'
} AND durable_writes = true
  AND tablets = {'enabled': true, 'initial': 1024};

CREATE TABLE benchmark.key_value (
    key bigint,
    val blob,
    PRIMARY KEY (key)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4WithDictsCompressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND tombstone_gc = {
      'mode': 'repair',
      'propagation_delay_in_seconds': '3600'
    };
```

### Suggested for a single test node (adjust DC / RF)

```cql
-- Example: SimpleStrategy RF=1 — only if that matches how scylladb1 is set up
CREATE KEYSPACE IF NOT EXISTS benchmark
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS benchmark.key_value (
    key bigint PRIMARY KEY,
    val blob
);
```

Confirm datacenter name with `nodetool status` on `scylladb1` before using `NetworkTopologyStrategy`.

Raw dump also in this repo: `exports/benchmark-schema-src.cql`.

---

## 6. Suggested config for the Rust tool (local)

Fill destination auth after checking our nodes. Example shape:

```toml
# Example — adapt to odyssey-migrator / ferry CLI flags

[source]
name = "lab-src"
# via SSH tunnel (section 3.1)
hosts = ["127.0.0.1"]
port = 19042
username = "scylla_user"
password = "SecretP@ssw0rd"
keyspace = "benchmark"
table = "key_value"

[destination]
name = "scylladb1"
# Option A: direct (if 9042 open)
hosts = ["161.22.44.52"]
port = 9042
# Option B: via tunnel → hosts = ["127.0.0.1"], port = 29042
username = "TODO_OUR_USER"
password = "TODO_OUR_PASSWORD"
keyspace = "benchmark"
table = "key_value"

[job]
# First pass: schema-only is fine
create_schema = true
copy_data = true          # set false for schema-only dry run
# Full copy is large (~tens of millions of partitions) — start with a limit if supported
# row_limit = 10000
```

**Runtime rule:** open the SRC tunnel first, then run the binary on the Mac. No process on `scylla-migrator`.

---

## 7. Recommended test order

1. `ssh scylla-src-1` / `ssh scylladb1` — confirm SSH keys work.  
2. Open SRC tunnel `:19042`.  
3. From Mac, `DESC KEYSPACE benchmark` + `SELECT … LIMIT 5` against SRC.  
4. On `scylladb1`, create keyspace/table (or let the tool create schema).  
5. Run Rust tool: **schema only**, then small data sample / limited rows if available.  
6. Verify on target: same sample keys, then decide whether a full multi‑GB copy is worth it.  
7. Optionally repeat against `scylladb2` (after clarifying `scylladb3` IP).

---

## 8. What not to do

- Do **not** install Java/Spark/Migrator JAR or our Rust binary on `scylla-migrator`.  
- Do **not** point the Rust tool at lab private IPs without a tunnel.  
- Do **not** assume lab DST (`scylla-dst-*`) is the write target for this exercise — targets are **our** `scylladb1..3`.  
- Do **not** rely on `COUNT(*)` for progress on `benchmark.key_value`.  
- Treat lab credentials as temporary interview access; rotate/revoke mentally when the lab ends.

---

## 9. Quick reference card

| Need | Value |
|------|--------|
| SRC SSH | `ssh scylla-src-1` (`ubuntu`, `~/.ssh/scylladb`) |
| SRC CQL (from Mac) | `127.0.0.1:19042` ← tunnel to `10.108.0.86:9042` |
| SRC user/pass | `scylla_user` / `SecretP@ssw0rd` |
| Dataset | `benchmark.key_value` (`key bigint`, `val blob`) |
| Our write target (start) | `scylladb1` → `161.22.44.52` (`root`, `~/.ssh/binlogic`) |
| Our CQL user/pass | **TODO** (discover on node) |
| Tool location | Local Mac only |

---

## 10. Existing helpers in this repo

| Path | Purpose |
|------|---------|
| `scripts/tunnels.sh` | Opens lab SRC `:19042` + lab DST `:19043` |
| `config.local.toml` | Older auditdiff sample (still had `cassandra` creds — **update** to `scylla_user` if reused) |
| `exports/benchmark-schema-src.cql` | Schema dump from lab SRC |
| `exports/sample-keys-*.txt` | Sample keys post lab migration |
| `exports/tablestats-*.txt` | Size evidence from lab |

Questions for the team after first connect to `scylladb1`: native port, authenticator, DC name, and whether full data copy or schema-only is the acceptance bar for v1.
