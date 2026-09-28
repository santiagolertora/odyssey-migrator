//! Post-migration validation for Odyssey Migrator.
//!
//! Compares source and target tables over token ranges using blake3 digests.
//! Modes come from [`odyssey_core::ValidationMode`]:
//!
//! - **Sample** — digest the first [`SAMPLE_PARTITIONS_PER_RANGE`] rows per
//!   range (V0.1: row sample in token order, not distinct partitions).
//! - **Digest** — digest every row in each range.
//! - **Full** — same coverage as Digest in V0.1; logs DiffSample detail on
//!   mismatch.
//!
//! Cell encoding uses `Debug` of `Option<CqlValue>` as the V0.1 canonical form
//! (see [`hash_row`]).

mod compare;
mod digest;
mod error;
mod report;
mod validate;

pub use digest::{
    ROW_SEPARATOR, absorb_columns, absorb_row, digest_rows, hash_columns, hash_row,
    schema_fingerprint,
};
pub use error::ValidationError;
pub use report::{
    ColumnDiff, DiffKind, RangeDigest, RowDiffSample, ValidationReport,
};
pub use validate::{SAMPLE_PARTITIONS_PER_RANGE, ValidationOptions, validate_table};
