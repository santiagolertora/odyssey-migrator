//! Durable SQLite checkpoint store for Odyssey Migrator.
//!
//! Odyssey never marks a migration unit `completed` without a durable record.
//! Callers must [`CheckpointStore::save_progress`] for the final page before
//! [`CheckpointStore::mark_completed`]; the latter refuses any unit that is
//! not already `Running`.

mod error;
mod schema;
mod store;
mod types;

pub use error::CheckpointError;
pub use store::CheckpointStore;
pub use types::{
    MigrationRecord, MigrationSummary, NewMigration, UnitProgress, UnitRecord,
};
