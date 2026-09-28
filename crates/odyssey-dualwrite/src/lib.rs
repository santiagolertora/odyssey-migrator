//! Dual-write gateway: apply structured mutations to source then target.
//!
//! Intended for the Cassandra→Scylla cutover window when Scylla CDC on the
//! source is unavailable. Apps POST JSON mutations; Odyssey writes both clusters
//! (source first) with at-least-once semantics.

mod error;
mod http;
mod json_cql;
mod mutate;
mod writer;

pub use error::DualWriteError;
pub use http::serve_dual_write;
pub use mutate::{Mutation, MutationOp};
pub use writer::DualWriter;
