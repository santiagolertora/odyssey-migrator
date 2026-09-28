use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Supported dual-write operations (structured; not free-form CQL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOp {
    Insert,
    Update,
    DeleteRow,
    DeletePartition,
}

/// JSON mutation body for `POST /v1/mutate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mutation {
    /// Fully-qualified table as in config `[[tables]].source` (e.g. `ks.events`).
    pub table: String,
    pub op: MutationOp,
    /// Column name → JSON value (null deletes / omits depending on op).
    pub columns: serde_json::Map<String, Value>,
    /// Optional client timestamp in microseconds (USING TIMESTAMP).
    #[serde(default)]
    pub timestamp_us: Option<i64>,
    /// Optional TTL in seconds (USING TTL).
    #[serde(default)]
    pub ttl_secs: Option<i32>,
}
