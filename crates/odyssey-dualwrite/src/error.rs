use thiserror::Error;

/// Errors from planning or executing a dual-write mutation.
#[derive(Debug, Error)]
pub enum DualWriteError {
    #[error("invalid mutation: {0}")]
    Invalid(String),

    #[error("unknown table `{0}` (must match a [[tables]] mapping)")]
    UnknownTable(String),

    #[error("failed to build CQL: {0}")]
    Statement(String),

    #[error("source write failed: {0}")]
    SourceWrite(String),

    #[error("target write failed (source already applied): {0}")]
    TargetWrite(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("unauthorized")]
    Unauthorized,

    #[error("http error: {0}")]
    Http(String),
}
