//! CQL connectivity for Cassandra and ScyllaDB.
//!
//! This crate owns sessions, cluster metadata reads, and schema introspection.
//! Row copy pipelines live in the engine crate; here we only discover enough
//! structure to plan and talk to both ends of a migration.

mod cluster;
mod consistency;
mod ensure;
mod error;
mod schema;
mod session;
mod topology;

pub use cluster::{ClusterInfo, describe_cluster};
pub use consistency::to_driver_consistency;
pub use ensure::{
    CreateSchemaOptions, EnsureSchemaAction, create_keyspace_cql, create_table_cql,
    ensure_target_table,
};
pub use error::CqlError;
pub use schema::{
    collection_kind, ColumnInfo, ColumnKind, CollectionKind, TableSchema, discover_table,
};
pub use session::{CqlSession, connect_source, connect_target};
pub use topology::{discover_vnode_tokens, vnode_parent_ranges};
