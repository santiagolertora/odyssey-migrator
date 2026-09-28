use thiserror::Error;

use odyssey_engine::EngineError;

/// Failures while preparing or running validation.
///
/// Any error means the caller must not invent success: treat the run as
/// incomplete, not as a match.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("invalid validation options: {0}")]
    InvalidOptions(String),

    #[error("engine error during validation: {0}")]
    Engine(#[from] EngineError),
}
