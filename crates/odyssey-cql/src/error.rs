use thiserror::Error;

/// Errors from CQL connectivity and schema discovery.
#[derive(Debug, Error)]
pub enum CqlError {
    #[error("failed to connect to CQL cluster: {0}")]
    Connect(String),

    #[error("CQL query failed: {0}")]
    Query(String),

    #[error("table `{keyspace}.{table}` was not found")]
    TableNotFound { keyspace: String, table: String },

    #[error("table `{keyspace}.{table}` has no partition key columns")]
    MissingPartitionKey { keyspace: String, table: String },

    #[error("{0}")]
    Invalid(String),
}
