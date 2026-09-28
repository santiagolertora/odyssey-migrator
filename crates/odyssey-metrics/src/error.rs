//! Metrics errors for Odyssey Migrator.

use thiserror::Error;

/// Failures while encoding or serving metrics.
#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("failed to encode metrics: {0}")]
    Encode(String),

    #[error("failed to bind metrics listener on {addr}: {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },

    #[error("metrics listener accept failed: {0}")]
    Accept(#[source] std::io::Error),

    #[error("metrics HTTP I/O error: {0}")]
    Io(#[from] std::io::Error),
}
