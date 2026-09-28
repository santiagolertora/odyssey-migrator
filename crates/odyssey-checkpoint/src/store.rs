use std::path::{Path, PathBuf};
use std::sync::Mutex;

use chrono::Utc;
use odyssey_core::CheckpointConfig;
use odyssey_types::{MigrationState, MigrationUnit};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::error::CheckpointError;
use crate::schema::{MIGRATE_SQL, PRAGMA_SQL, SCHEMA_SQL};
use crate::types::{
    parse_state, MigrationRecord, MigrationSummary, NewMigration, UnitProgress, UnitRecord,
};

/// Durable SQLite store for migration metadata and per-unit progress.
///
/// Open the database with a path from [`CheckpointConfig`] — never hardcode
/// checkpoint locations in callers of the engine.
pub struct CheckpointStore {
    conn: Mutex<Connection>,
    path: PathBuf,
}

impl CheckpointStore {
    /// Open (or create) the checkpoint database at `path` and apply the schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|source| CheckpointError::CreateDir {
                    path: parent.display().to_string(),
                    source,
                })?;
            }
        }

        let conn = Connection::open(&path).map_err(|source| CheckpointError::Open {
            path: path.display().to_string(),
            source,
        })?;
        conn.execute_batch(PRAGMA_SQL)?;
        conn.execute_batch(SCHEMA_SQL)?;
        // Older DBs may already have the column; ignore duplicate-column errors.
        let _ = conn.execute_batch(MIGRATE_SQL);
        // Default busy timeout helps multi-process shared-checkpoint meshes.
        let _ = conn.busy_timeout(std::time::Duration::from_millis(5_000));

        info!(path = %path.display(), "opened checkpoint database");
        Ok(Self {
            conn: Mutex::new(conn),
            path,
        })
    }

    /// Raise SQLite busy_timeout for multi-process checkpoint sharing.
    pub fn set_busy_timeout_ms(&self, ms: u64) -> Result<(), CheckpointError> {
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        conn.busy_timeout(std::time::Duration::from_millis(ms.max(1)))?;
        Ok(())
    }

    /// Reclaim `running` units whose heartbeat (or updated_at) is older than `lease_secs`.
    pub fn reclaim_stale_running(
        &self,
        migration_id: &str,
        lease_secs: u64,
    ) -> Result<u64, CheckpointError> {
        let cutoff = (Utc::now() - chrono::Duration::seconds(lease_secs as i64)).to_rfc3339();
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let changed = conn.execute(
            r#"
            UPDATE migration_units
            SET state = ?1,
                claimed_by = NULL,
                heartbeat_at = NULL,
                updated_at = ?2
            WHERE migration_id = ?3
              AND state = ?4
              AND COALESCE(heartbeat_at, updated_at) < ?5
            "#,
            params![
                MigrationState::Pending.as_str(),
                now_rfc3339(),
                migration_id,
                MigrationState::Running.as_str(),
                cutoff,
            ],
        )?;
        if changed > 0 {
            info!(
                migration_id,
                reclaimed = changed,
                lease_secs,
                "reclaimed stale running units for mesh"
            );
        }
        Ok(changed as u64)
    }

    /// Open using the path from Odyssey's checkpoint config section.
    pub fn open_from_config(config: &CheckpointConfig) -> Result<Self, CheckpointError> {
        Self::open(&config.path)
    }

    /// Filesystem path of the underlying SQLite file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Insert a new migration row and return its id.
    pub fn create_migration(&self, meta: NewMigration) -> Result<String, CheckpointError> {
        let id = Uuid::new_v4().to_string();
        let created_at = now_rfc3339();
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        conn.execute(
            r#"
            INSERT INTO migrations (
                id, name, source_cluster, target_cluster,
                keyspace_name, table_name, schema_hash,
                created_at, completed_at, status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, 'running')
            "#,
            params![
                id,
                meta.name,
                meta.source_cluster,
                meta.target_cluster,
                meta.keyspace_name,
                meta.table_name,
                meta.schema_hash,
                created_at,
            ],
        )?;
        info!(
            migration_id = %id,
            name = %meta.name,
            keyspace = %meta.keyspace_name,
            table = %meta.table_name,
            "created migration"
        );
        Ok(id)
    }

    pub fn get_migration(&self, id: &str) -> Result<Option<MigrationRecord>, CheckpointError> {
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let mut stmt = conn.prepare(
            r#"
            SELECT id, name, source_cluster, target_cluster,
                   keyspace_name, table_name, schema_hash,
                   created_at, completed_at, status
            FROM migrations
            WHERE id = ?1
            "#,
        )?;
        let record = stmt
            .query_row(params![id], |row| {
                Ok(MigrationRecord {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    source_cluster: row.get(2)?,
                    target_cluster: row.get(3)?,
                    keyspace_name: row.get(4)?,
                    table_name: row.get(5)?,
                    schema_hash: row.get(6)?,
                    created_at: row.get(7)?,
                    completed_at: row.get(8)?,
                    status: row.get(9)?,
                })
            })
            .optional()?;
        Ok(record)
    }

    /// Newest-first list of migrations in this store (dashboard / status overview).
    pub fn list_migrations(&self) -> Result<Vec<MigrationRecord>, CheckpointError> {
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let mut stmt = conn.prepare(
            r#"
            SELECT id, name, source_cluster, target_cluster,
                   keyspace_name, table_name, schema_hash,
                   created_at, completed_at, status
            FROM migrations
            ORDER BY created_at DESC
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MigrationRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                source_cluster: row.get(2)?,
                target_cluster: row.get(3)?,
                keyspace_name: row.get(4)?,
                table_name: row.get(5)?,
                schema_hash: row.get(6)?,
                created_at: row.get(7)?,
                completed_at: row.get(8)?,
                status: row.get(9)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn list_units(&self, migration_id: &str) -> Result<Vec<UnitRecord>, CheckpointError> {
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        Self::list_units_locked(&conn, migration_id, None)
    }

    /// Persist planned units under an existing migration.
    pub fn insert_units(
        &self,
        migration_id: &str,
        units: &[MigrationUnit],
    ) -> Result<(), CheckpointError> {
        let updated_at = now_rfc3339();
        let mut conn = self.conn.lock().expect("checkpoint mutex poisoned");
        ensure_migration_exists(&conn, migration_id)?;

        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT INTO migration_units (
                    id, migration_id, token_start, token_end, state,
                    paging_state, rows_read, rows_written, bytes_written,
                    attempts, started_at, updated_at, completed_at, last_error
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5,
                    ?6, ?7, ?8, ?9,
                    0, NULL, ?10, NULL, NULL
                )
                "#,
            )?;
            for unit in units {
                stmt.execute(params![
                    unit.id.to_string(),
                    migration_id,
                    unit.range.start,
                    unit.range.end,
                    unit.state.as_str(),
                    unit.paging_state.as_deref(),
                    unit.rows_read as i64,
                    unit.rows_written as i64,
                    unit.bytes_written as i64,
                    updated_at,
                ])?;
            }
        }
        tx.commit()?;
        info!(
            migration_id = %migration_id,
            count = units.len(),
            "inserted migration units"
        );
        Ok(())
    }

    /// Atomically claim one `Pending` unit by transitioning it to `Running`.
    ///
    /// Uses an immediate transaction so concurrent workers cannot claim the
    /// same unit.
    pub fn claim_pending_unit(
        &self,
        migration_id: &str,
    ) -> Result<Option<UnitRecord>, CheckpointError> {
        self.claim_pending_unit_for(migration_id, "local")
    }

    /// Claim the next pending unit, stamping `claimed_by` for mesh observability.
    pub fn claim_pending_unit_for(
        &self,
        migration_id: &str,
        worker_id: &str,
    ) -> Result<Option<UnitRecord>, CheckpointError> {
        let now = now_rfc3339();
        let mut conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let unit_id: Option<String> = tx
            .query_row(
                r#"
                SELECT id FROM migration_units
                WHERE migration_id = ?1 AND state = ?2
                ORDER BY token_start ASC
                LIMIT 1
                "#,
                params![migration_id, MigrationState::Pending.as_str()],
                |row| row.get(0),
            )
            .optional()?;

        let Some(unit_id) = unit_id else {
            tx.commit()?;
            return Ok(None);
        };

        let changed = tx.execute(
            r#"
            UPDATE migration_units
            SET state = ?1,
                started_at = COALESCE(started_at, ?2),
                updated_at = ?2,
                attempts = attempts + 1,
                last_error = NULL,
                claimed_by = ?3,
                heartbeat_at = ?2
            WHERE id = ?4 AND state = ?5
            "#,
            params![
                MigrationState::Running.as_str(),
                now,
                worker_id,
                unit_id,
                MigrationState::Pending.as_str(),
            ],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(None);
        }

        let unit = load_unit(&tx, &unit_id)?.expect("claimed unit must exist");
        tx.commit()?;
        info!(
            migration_id = %migration_id,
            unit_id = %unit.id,
            worker_id,
            token_start = unit.token_start,
            token_end = unit.token_end,
            attempts = unit.attempts,
            "claimed pending unit -> running"
        );
        Ok(Some(unit))
    }

    /// Persist paging state and counters for a running unit.
    ///
    /// Call this after every durable page, including the last page of a unit,
    /// before [`Self::mark_completed`].
    pub fn save_progress(&self, update: UnitProgress) -> Result<(), CheckpointError> {
        update.ensure_running()?;
        let mut conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let tx = conn.transaction()?;

        let current = load_unit(&tx, &update.unit_id)?
            .ok_or_else(|| CheckpointError::UnitNotFound(update.unit_id.clone()))?;
        if current.state != MigrationState::Running {
            return Err(CheckpointError::ProgressNotRunning {
                unit_id: update.unit_id.clone(),
                found: current.state.as_str().to_string(),
            });
        }

        tx.execute(
            r#"
            UPDATE migration_units
            SET paging_state = ?1,
                rows_read = ?2,
                rows_written = ?3,
                bytes_written = ?4,
                state = ?5,
                updated_at = ?6,
                last_error = NULL,
                last_token = ?7,
                heartbeat_at = ?6
            WHERE id = ?8 AND state = ?5
            "#,
            params![
                update.paging_state.as_deref(),
                update.rows_read as i64,
                update.rows_written as i64,
                update.bytes_written as i64,
                MigrationState::Running.as_str(),
                progress_timestamp(&current),
                update.last_token,
                update.unit_id,
            ],
        )?;
        tx.commit()?;
        debug!(
            unit_id = %update.unit_id,
            rows_read = update.rows_read,
            rows_written = update.rows_written,
            bytes_written = update.bytes_written,
            last_token = ?update.last_token,
            has_paging_state = update.paging_state.is_some(),
            "saved unit progress"
        );
        Ok(())
    }

    /// Transition a unit to `Completed` in one transaction.
    ///
    /// Refuses unless the unit is currently `Running`. Callers **must** call
    /// [`Self::save_progress`] for the final page before this method so the
    /// durable counters and paging state reflect what was written.
    pub fn mark_completed(&self, unit_id: &str) -> Result<(), CheckpointError> {
        let now = now_rfc3339();
        let mut conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let current = load_unit(&tx, unit_id)?
            .ok_or_else(|| CheckpointError::UnitNotFound(unit_id.to_string()))?;
        if current.state != MigrationState::Running {
            error!(
                unit_id = %unit_id,
                found = current.state.as_str(),
                "refusing mark_completed: unit is not running"
            );
            return Err(CheckpointError::NotRunning {
                unit_id: unit_id.to_string(),
                found: current.state.as_str().to_string(),
            });
        }

        // Claim sets started_at == updated_at. save_progress always refreshes
        // updated_at, so equal timestamps mean the final page was never
        // checkpointed — refuse rather than invent a completed state.
        if current.started_at.as_deref() == Some(current.updated_at.as_str()) {
            error!(
                unit_id = %unit_id,
                "refusing mark_completed: save_progress was not called after claim"
            );
            return Err(CheckpointError::ProgressNotSaved {
                unit_id: unit_id.to_string(),
            });
        }

        let changed = tx.execute(
            r#"
            UPDATE migration_units
            SET state = ?1,
                completed_at = ?2,
                updated_at = ?2,
                last_error = NULL
            WHERE id = ?3 AND state = ?4
            "#,
            params![
                MigrationState::Completed.as_str(),
                now,
                unit_id,
                MigrationState::Running.as_str(),
            ],
        )?;
        if changed != 1 {
            let found = load_unit(&tx, unit_id)?
                .map(|u| u.state.as_str().to_string())
                .unwrap_or_else(|| "missing".into());
            return Err(CheckpointError::NotRunning {
                unit_id: unit_id.to_string(),
                found,
            });
        }

        tx.commit()?;
        info!(unit_id = %unit_id, "marked unit completed");
        Ok(())
    }

    pub fn mark_failed(&self, unit_id: &str, error: &str) -> Result<(), CheckpointError> {
        let now = now_rfc3339();
        let mut conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let tx = conn.transaction()?;

        let current = load_unit(&tx, unit_id)?
            .ok_or_else(|| CheckpointError::UnitNotFound(unit_id.to_string()))?;
        if current.state == MigrationState::Completed {
            return Err(CheckpointError::NotRunning {
                unit_id: unit_id.to_string(),
                found: current.state.as_str().to_string(),
            });
        }

        tx.execute(
            r#"
            UPDATE migration_units
            SET state = ?1,
                last_error = ?2,
                updated_at = ?3
            WHERE id = ?4
            "#,
            params![MigrationState::Failed.as_str(), error, now, unit_id],
        )?;
        tx.commit()?;
        error!(unit_id = %unit_id, error = %error, "marked unit failed");
        Ok(())
    }

    /// Units that still need work after a crash or partial run.
    pub fn resumable_units(
        &self,
        migration_id: &str,
    ) -> Result<Vec<UnitRecord>, CheckpointError> {
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        Self::list_units_locked(
            &conn,
            migration_id,
            Some(&[
                MigrationState::Pending,
                MigrationState::Running,
                MigrationState::Failed,
            ]),
        )
    }

    /// After a process crash, return in-flight `Running` units to `Pending`
    /// so they can be claimed again (at-least-once).
    pub fn reset_running_to_pending(&self, migration_id: &str) -> Result<u64, CheckpointError> {
        let now = now_rfc3339();
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let changed = conn.execute(
            r#"
            UPDATE migration_units
            SET state = ?1,
                updated_at = ?2
            WHERE migration_id = ?3 AND state = ?4
            "#,
            params![
                MigrationState::Pending.as_str(),
                now,
                migration_id,
                MigrationState::Running.as_str(),
            ],
        )?;
        let reset = changed as u64;
        info!(
            migration_id = %migration_id,
            reset,
            "reset running units to pending for resume"
        );
        Ok(reset)
    }

    /// Move failed units back to pending so `resume` can retry them after the
    /// operator fixed the underlying fault. Keeps paging_state and counters so
    /// work continues from the last durable progress (at-least-once).
    pub fn requeue_failed_to_pending(&self, migration_id: &str) -> Result<u64, CheckpointError> {
        let now = now_rfc3339();
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        let changed = conn.execute(
            r#"
            UPDATE migration_units
            SET state = ?1,
                last_error = NULL,
                updated_at = ?2
            WHERE migration_id = ?3 AND state = ?4
            "#,
            params![
                MigrationState::Pending.as_str(),
                now,
                migration_id,
                MigrationState::Failed.as_str(),
            ],
        )?;
        let requeued = changed as u64;
        info!(
            migration_id = %migration_id,
            requeued,
            "requeued failed units to pending for retry"
        );
        Ok(requeued)
    }

    pub fn migration_summary(
        &self,
        migration_id: &str,
    ) -> Result<MigrationSummary, CheckpointError> {
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        ensure_migration_exists(&conn, migration_id)?;

        let mut pending = 0u64;
        let mut running = 0u64;
        let mut completed = 0u64;
        let mut failed = 0u64;

        let mut stmt = conn.prepare(
            r#"
            SELECT state, COUNT(*)
            FROM migration_units
            WHERE migration_id = ?1
            GROUP BY state
            "#,
        )?;
        let rows = stmt.query_map(params![migration_id], |row| {
            let state: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((state, count as u64))
        })?;
        for row in rows {
            let (state, count) = row?;
            match parse_state(&state)? {
                MigrationState::Pending => pending = count,
                MigrationState::Running => running = count,
                MigrationState::Completed => completed = count,
                MigrationState::Failed => failed = count,
            }
        }

        let (total_rows_read, total_rows_written, total_bytes_written) = conn.query_row(
            r#"
            SELECT
                COALESCE(SUM(rows_read), 0),
                COALESCE(SUM(rows_written), 0),
                COALESCE(SUM(bytes_written), 0)
            FROM migration_units
            WHERE migration_id = ?1
            "#,
            params![migration_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                ))
            },
        )?;

        Ok(MigrationSummary {
            migration_id: migration_id.to_string(),
            pending,
            running,
            completed,
            failed,
            total_rows_read,
            total_rows_written,
            total_bytes_written,
        })
    }

    /// Mark the migration `completed` when every unit is `Completed`.
    ///
    /// Returns `true` if the migration was marked completed in this call (or
    /// was already completed with all units done).
    pub fn mark_migration_completed_if_done(
        &self,
        migration_id: &str,
    ) -> Result<bool, CheckpointError> {
        let summary = self.migration_summary(migration_id)?;
        if !summary.all_completed() {
            return Ok(false);
        }

        let now = now_rfc3339();
        let conn = self.conn.lock().expect("checkpoint mutex poisoned");
        conn.execute(
            r#"
            UPDATE migrations
            SET status = 'completed',
                completed_at = COALESCE(completed_at, ?1)
            WHERE id = ?2
            "#,
            params![now, migration_id],
        )?;
        info!(migration_id = %migration_id, "migration marked completed");
        Ok(true)
    }

    fn list_units_locked(
        conn: &Connection,
        migration_id: &str,
        states: Option<&[MigrationState]>,
    ) -> Result<Vec<UnitRecord>, CheckpointError> {
        let mut sql = String::from(
            r#"
            SELECT id, migration_id, token_start, token_end, state,
                   paging_state, rows_read, rows_written, bytes_written,
                   attempts, started_at, updated_at, completed_at, last_error,
                   last_token
            FROM migration_units
            WHERE migration_id = ?1
            "#,
        );
        if let Some(states) = states {
            sql.push_str(" AND state IN (");
            for (i, state) in states.iter().enumerate() {
                if i > 0 {
                    sql.push(',');
                }
                // State strings come from our enum, not user input.
                sql.push('\'');
                sql.push_str(state.as_str());
                sql.push('\'');
            }
            sql.push(')');
        }
        sql.push_str(" ORDER BY token_start ASC");

        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(params![migration_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(unit_from_row(row)?);
        }
        Ok(out)
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

/// Timestamp for a progress flush that is guaranteed to differ from `started_at`
/// so [`CheckpointStore::mark_completed`] can tell progress was saved.
fn progress_timestamp(current: &UnitRecord) -> String {
    let stamp = now_rfc3339();
    if current.started_at.as_deref() == Some(stamp.as_str()) {
        (Utc::now() + chrono::Duration::nanoseconds(1)).to_rfc3339()
    } else {
        stamp
    }
}

fn ensure_migration_exists(conn: &Connection, migration_id: &str) -> Result<(), CheckpointError> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM migrations WHERE id = ?1)",
        params![migration_id],
        |row| row.get(0),
    )?;
    if !exists {
        return Err(CheckpointError::MigrationNotFound(migration_id.to_string()));
    }
    Ok(())
}

fn load_unit(tx: &Transaction<'_>, unit_id: &str) -> Result<Option<UnitRecord>, CheckpointError> {
    let mut stmt = tx.prepare(
        r#"
        SELECT id, migration_id, token_start, token_end, state,
               paging_state, rows_read, rows_written, bytes_written,
               attempts, started_at, updated_at, completed_at, last_error,
               last_token
        FROM migration_units
        WHERE id = ?1
        "#,
    )?;
    let mut rows = stmt.query(params![unit_id])?;
    match rows.next()? {
        Some(row) => Ok(Some(unit_from_row(row)?)),
        None => Ok(None),
    }
}

fn unit_from_row(row: &rusqlite::Row<'_>) -> Result<UnitRecord, CheckpointError> {
    let state_raw: String = row.get(4)?;
    let state = parse_state(&state_raw)?;
    Ok(UnitRecord {
        id: row.get(0)?,
        migration_id: row.get(1)?,
        token_start: row.get(2)?,
        token_end: row.get(3)?,
        state,
        paging_state: row.get(5)?,
        rows_read: row.get::<_, i64>(6)? as u64,
        rows_written: row.get::<_, i64>(7)? as u64,
        bytes_written: row.get::<_, i64>(8)? as u64,
        attempts: row.get::<_, i64>(9)? as u64,
        started_at: row.get(10)?,
        updated_at: row.get(11)?,
        completed_at: row.get(12)?,
        last_error: row.get(13)?,
        last_token: row.get(14)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_types::TokenRange;
    use tempfile::TempDir;

    fn open_temp() -> (TempDir, CheckpointStore) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("migration.db");
        let store = CheckpointStore::open(&path).unwrap();
        (dir, store)
    }

    fn sample_migration(store: &CheckpointStore) -> String {
        store
            .create_migration(NewMigration {
                name: "demo".into(),
                source_cluster: "src".into(),
                target_cluster: "dst".into(),
                keyspace_name: "ks".into(),
                table_name: "events".into(),
                schema_hash: "abc".into(),
            })
            .unwrap()
    }

    fn unit(start: i64, end: i64) -> MigrationUnit {
        MigrationUnit::new("ks", "events", TokenRange::new(start, end).unwrap()).unwrap()
    }

    #[test]
    fn open_creates_schema() {
        let (_dir, store) = open_temp();
        let conn = store.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('migrations','migration_units')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn open_from_checkpoint_config_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("from-config.db");
        let config = CheckpointConfig {
            path: path.display().to_string(),
            interval_secs: 5,
        };
        let store = CheckpointStore::open_from_config(&config).unwrap();
        assert_eq!(store.path(), path.as_path());
        assert!(path.exists());
    }

    #[test]
    fn insert_and_claim_is_exclusive() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        let u1 = unit(0, 100);
        let u2 = unit(100, 200);
        store.insert_units(&mid, &[u1.clone(), u2.clone()]).unwrap();

        let first = store.claim_pending_unit(&mid).unwrap().expect("first");
        let second = store.claim_pending_unit(&mid).unwrap().expect("second");
        assert_ne!(first.id, second.id);
        assert_eq!(first.state, MigrationState::Running);
        assert_eq!(second.state, MigrationState::Running);
        assert!(store.claim_pending_unit(&mid).unwrap().is_none());

        let units = store.list_units(&mid).unwrap();
        assert_eq!(units.len(), 2);
        assert!(units.iter().all(|u| u.state == MigrationState::Running));
    }

    #[test]
    fn save_progress_persists_paging_state() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        let u = unit(0, 50);
        store.insert_units(&mid, &[u.clone()]).unwrap();
        let claimed = store.claim_pending_unit(&mid).unwrap().unwrap();

        let paging = vec![0x01, 0x02, 0xFF];
        store
            .save_progress(UnitProgress {
                unit_id: claimed.id.clone(),
                paging_state: Some(paging.clone()),
                rows_read: 10,
                rows_written: 10,
                bytes_written: 120,
                state: MigrationState::Running,
                last_token: None,
            })
            .unwrap();

        let units = store.list_units(&mid).unwrap();
        assert_eq!(units[0].paging_state.as_ref(), Some(&paging));
        assert_eq!(units[0].rows_read, 10);
        assert_eq!(units[0].rows_written, 10);
        assert_eq!(units[0].bytes_written, 120);
    }

    #[test]
    fn mark_completed_from_running() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        let u = unit(0, 10);
        store.insert_units(&mid, &[u]).unwrap();
        let claimed = store.claim_pending_unit(&mid).unwrap().unwrap();
        store
            .save_progress(UnitProgress {
                unit_id: claimed.id.clone(),
                paging_state: None,
                rows_read: 5,
                rows_written: 5,
                bytes_written: 50,
                state: MigrationState::Running,
                last_token: None,
            })
            .unwrap();
        store.mark_completed(&claimed.id).unwrap();

        let units = store.list_units(&mid).unwrap();
        assert_eq!(units[0].state, MigrationState::Completed);
        assert!(units[0].completed_at.is_some());
        assert!(store.mark_migration_completed_if_done(&mid).unwrap());
        let migration = store.get_migration(&mid).unwrap().unwrap();
        assert_eq!(migration.status, "completed");
        assert!(migration.completed_at.is_some());
    }

    #[test]
    fn mark_completed_fails_from_pending() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        let u = unit(0, 10);
        let id = u.id.to_string();
        store.insert_units(&mid, &[u]).unwrap();

        let err = store.mark_completed(&id).unwrap_err();
        assert!(matches!(err, CheckpointError::NotRunning { .. }));
        let units = store.list_units(&mid).unwrap();
        assert_eq!(units[0].state, MigrationState::Pending);
        assert!(units[0].completed_at.is_none());
    }

    #[test]
    fn resume_resets_running_then_claim_again() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        let u = unit(0, 10);
        store.insert_units(&mid, &[u]).unwrap();
        let claimed = store.claim_pending_unit(&mid).unwrap().unwrap();
        assert!(store.claim_pending_unit(&mid).unwrap().is_none());

        let reset = store.reset_running_to_pending(&mid).unwrap();
        assert_eq!(reset, 1);

        let resumable = store.resumable_units(&mid).unwrap();
        assert_eq!(resumable.len(), 1);
        assert_eq!(resumable[0].state, MigrationState::Pending);

        let again = store.claim_pending_unit(&mid).unwrap().unwrap();
        assert_eq!(again.id, claimed.id);
        assert_eq!(again.attempts, 2);
    }

    #[test]
    fn mark_completed_fails_without_save_progress() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        store.insert_units(&mid, &[unit(0, 10)]).unwrap();
        let claimed = store.claim_pending_unit(&mid).unwrap().unwrap();

        let err = store.mark_completed(&claimed.id).unwrap_err();
        assert!(matches!(err, CheckpointError::ProgressNotSaved { .. }));
        assert_eq!(
            store.list_units(&mid).unwrap()[0].state,
            MigrationState::Running
        );
    }

    #[test]
    fn summary_counts_by_state() {
        let (_dir, store) = open_temp();
        let mid = sample_migration(&store);
        let units = vec![unit(0, 10), unit(10, 20), unit(20, 30), unit(30, 40)];
        let ids: Vec<_> = units.iter().map(|u| u.id.to_string()).collect();
        store.insert_units(&mid, &units).unwrap();

        let a = store.claim_pending_unit(&mid).unwrap().unwrap();
        store
            .save_progress(UnitProgress {
                unit_id: a.id.clone(),
                paging_state: None,
                rows_read: 1,
                rows_written: 1,
                bytes_written: 8,
                state: MigrationState::Running,
                last_token: None,
            })
            .unwrap();
        store.mark_completed(&a.id).unwrap();

        let b = store.claim_pending_unit(&mid).unwrap().unwrap();
        store.mark_failed(&b.id, "boom").unwrap();

        let _c = store.claim_pending_unit(&mid).unwrap().unwrap();
        // one left pending

        let summary = store.migration_summary(&mid).unwrap();
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.running, 1);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.total_units(), 4);
        assert_eq!(summary.total_rows_read, 1);
        assert!(!summary.all_completed());
        assert_eq!(ids.len(), 4);
    }
}
