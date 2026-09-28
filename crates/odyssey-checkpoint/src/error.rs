use thiserror::Error;

/// Errors from opening or mutating the checkpoint database.
#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("failed to create checkpoint directory {path}: {source}")]
    CreateDir {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to open checkpoint database at {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: rusqlite::Error,
    },

    #[error("checkpoint database error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("unknown migration state `{0}` in checkpoint database")]
    UnknownState(String),

    #[error("migration `{0}` was not found")]
    MigrationNotFound(String),

    #[error("unit `{0}` was not found")]
    UnitNotFound(String),

    #[error(
        "refusing to mark unit `{unit_id}` completed: expected state `running`, found `{found}`"
    )]
    NotRunning { unit_id: String, found: String },

    #[error(
        "refusing to save progress for unit `{unit_id}`: expected state `running`, found `{found}`"
    )]
    ProgressNotRunning { unit_id: String, found: String },

    #[error("unit progress state must be `running`, got `{0}`")]
    InvalidProgressState(String),

    #[error(
        "refusing to mark unit `{unit_id}` completed: call save_progress for the final page first"
    )]
    ProgressNotSaved { unit_id: String },
}
