use thiserror::Error;

/// Errors that originate from type construction or validation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TypesError {
    #[error("token range start ({start}) must be strictly less than end ({end})")]
    InvalidTokenRange { start: i64, end: i64 },

    #[error("keyspace must not be empty")]
    EmptyKeyspace,

    #[error("table must not be empty")]
    EmptyTable,
}
