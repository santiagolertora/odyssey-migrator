use thiserror::Error;

/// Errors from CDC catch-up.
#[derive(Debug, Error)]
pub enum CdcError {
    #[error("invalid catch-up options: {0}")]
    InvalidOptions(String),

    #[error("source table `{keyspace}.{table}` does not appear to have CDC enabled \
             (missing `{keyspace}.{table}_scylla_cdc_log`). Enable CDC before live catch-up.")]
    CdcNotEnabled { keyspace: String, table: String },

    #[error("unsupported CDC operation `{op}` for live catch-up (refusing to skip — that loses data)")]
    UnsupportedOperation { op: String },

    #[error("CDC row is missing primary-key column `{column}` required to apply the change")]
    MissingPrimaryKey { column: String },

    #[error("failed to apply CDC change to target: {0}")]
    Apply(String),

    #[error("CDC reader failed: {0}")]
    Reader(String),

    #[error("{0}")]
    Other(String),
}
