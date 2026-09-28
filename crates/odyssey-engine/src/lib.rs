//! Data copy engine for Odyssey Migrator.
//!
//! Reads token-range pages from the source, writes rows with prepared INSERTs
//! on the target, checkpoints after every durable page, and never marks a unit
//! complete without that checkpoint. Delivery is at-least-once: failed writes
//! fail the unit after retries; resume may rewrite the same primary keys.

mod batch;
mod error;
mod preserve;
mod progress;
mod reader;
mod retry;
mod runner;
mod statement;
mod throttle;
mod worker;
mod writer;

pub use batch::{RowBatch, estimate_cql_value_bytes, estimate_row_bytes};
pub use error::EngineError;
pub use preserve::{
    CollectionElementMeta, MigratedRow, PreserveOptions, bind_preserved_insert, split_preserved_row,
};
pub use progress::{PageProgress, ProgressObserver};
pub use reader::{PageRead, RangeReader};
pub use retry::RetryPolicy;
pub use runner::{MigrationReport, RunOptions, run_migration};
pub use statement::{
    build_collection_element_meta_select, build_counter_update_cql, build_insert_cql,
    build_insert_cql_preserving, build_map_element_update_cql, build_range_select_cql,
    build_range_select_cql_preserving, build_set_element_update_cql,
};
pub use throttle::AdaptiveConcurrency;
pub use worker::{UnitWorkResult, migrate_unit};
pub use writer::{RangeWriter, WriteBatchStats};
