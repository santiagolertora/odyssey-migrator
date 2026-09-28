use odyssey_types::MigrationState;
use serde::{Deserialize, Serialize};

use crate::CheckpointError;

/// Input for creating a new migration row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewMigration {
    pub name: String,
    pub source_cluster: String,
    pub target_cluster: String,
    pub keyspace_name: String,
    pub table_name: String,
    pub schema_hash: String,
}

/// Durable migration metadata as stored in SQLite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub id: String,
    pub name: String,
    pub source_cluster: String,
    pub target_cluster: String,
    pub keyspace_name: String,
    pub table_name: String,
    pub schema_hash: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub status: String,
}

/// One migration unit as stored in SQLite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitRecord {
    pub id: String,
    pub migration_id: String,
    pub token_start: i64,
    pub token_end: i64,
    pub state: MigrationState,
    pub paging_state: Option<Vec<u8>>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub attempts: u64,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
    /// Highest partition token observed while copying this unit (for progress %).
    pub last_token: Option<i64>,
}

/// Progress flush for a unit that is actively being copied.
///
/// `state` must be [`MigrationState::Running`]. Call this after each durable
/// page (including the final page) before [`super::CheckpointStore::mark_completed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitProgress {
    pub unit_id: String,
    pub paging_state: Option<Vec<u8>>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub state: MigrationState,
    /// Last `token(pk)` observed on a durable page for this unit.
    pub last_token: Option<i64>,
}

impl UnitProgress {
    pub(crate) fn ensure_running(&self) -> Result<(), CheckpointError> {
        if self.state != MigrationState::Running {
            return Err(CheckpointError::InvalidProgressState(
                self.state.as_str().to_string(),
            ));
        }
        Ok(())
    }
}

/// Aggregate unit counts for a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationSummary {
    pub migration_id: String,
    pub pending: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub total_rows_read: u64,
    pub total_rows_written: u64,
    pub total_bytes_written: u64,
}

impl MigrationSummary {
    pub fn total_units(&self) -> u64 {
        self.pending + self.running + self.completed + self.failed
    }

    pub fn all_completed(&self) -> bool {
        self.total_units() > 0 && self.pending == 0 && self.running == 0 && self.failed == 0
    }
}

pub(crate) fn parse_state(raw: &str) -> Result<MigrationState, CheckpointError> {
    match raw {
        "pending" => Ok(MigrationState::Pending),
        "running" => Ok(MigrationState::Running),
        "completed" => Ok(MigrationState::Completed),
        "failed" => Ok(MigrationState::Failed),
        other => Err(CheckpointError::UnknownState(other.to_string())),
    }
}
