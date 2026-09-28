use odyssey_checkpoint::CheckpointError;
use odyssey_cql::CqlError;
use thiserror::Error;
use uuid::Uuid;

/// Errors from the copy engine. Fail-closed: any variant means the unit or
/// migration must not be treated as successfully completed.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("invalid engine configuration: {0}")]
    InvalidConfig(String),

    #[error("failed to build CQL statement: {0}")]
    Statement(String),

    #[error("source read failed: {0}")]
    Read(String),

    #[error("target write failed after retries: {0}")]
    Write(String),

    #[error("failed to prepare statement: {0}")]
    Prepare(String),

    #[error("checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),

    #[error("schema error: {0}")]
    Schema(#[from] CqlError),

    #[error("unit `{unit_id}` failed: {reason}")]
    UnitFailed { unit_id: Uuid, reason: String },

    #[error("migration `{migration_id}` aborted with {failed_units} failed unit(s)")]
    MigrationFailed {
        migration_id: String,
        failed_units: u64,
    },

    #[error("worker task panicked: {0}")]
    WorkerJoin(String),
}
