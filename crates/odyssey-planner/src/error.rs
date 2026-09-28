use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlannerError {
    #[error("desired unit count must be at least 1")]
    ZeroUnits,

    #[error("failed to build token range: {0}")]
    Range(String),
}
