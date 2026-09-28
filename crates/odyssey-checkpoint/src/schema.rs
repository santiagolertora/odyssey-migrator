//! SQL schema constants that define the on-disk checkpoint contract.
//!
//! These strings are part of the store's durable format, not runtime config.

/// Creates both checkpoint tables and the units-by-state index.
pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS migrations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_cluster TEXT NOT NULL,
    target_cluster TEXT NOT NULL,
    keyspace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS migration_units (
    id TEXT PRIMARY KEY,
    migration_id TEXT NOT NULL REFERENCES migrations(id),
    token_start INTEGER NOT NULL,
    token_end INTEGER NOT NULL,
    state TEXT NOT NULL,
    paging_state BLOB,
    rows_read INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    bytes_written INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    last_error TEXT,
    last_token INTEGER,
    claimed_by TEXT,
    heartbeat_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_units_migration_state
    ON migration_units(migration_id, state);
"#;

/// Best-effort upgrades for older checkpoint files (SQLite ignores duplicate columns).
pub const MIGRATE_SQL: &str = r#"
ALTER TABLE migration_units ADD COLUMN last_token INTEGER;
ALTER TABLE migration_units ADD COLUMN claimed_by TEXT;
ALTER TABLE migration_units ADD COLUMN heartbeat_at TEXT;
"#;

/// Connection pragmas applied on every open.
pub const PRAGMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
"#;
