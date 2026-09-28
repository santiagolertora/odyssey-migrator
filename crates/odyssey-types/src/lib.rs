//! Shared domain types for Odyssey Migrator.
//!
//! These types are the contract between crates. Keep them free of I/O and
//! driver details so planners, engines, and checkpoints all speak the same
//! language.

mod error;
mod range;
mod state;
mod unit;

pub use error::TypesError;
pub use range::TokenRange;
pub use state::MigrationState;
pub use unit::MigrationUnit;
