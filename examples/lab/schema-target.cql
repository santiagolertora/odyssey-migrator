-- Target-only schema for OUR scylladb1 cluster (DC1, 3 nodes).
-- RF=1 keeps smoke data small on disk. Do NOT run this against lab SRC.

CREATE KEYSPACE IF NOT EXISTS benchmark
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'DC1': 1
}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS benchmark.key_value (
    key bigint PRIMARY KEY,
    val blob
);
