//! Scylla CDC catch-up for live Odyssey migrations.
//!
//! Bulk copy alone cannot see UPDATE/DELETE that land while ranges are being
//! read. This crate consumes the source table's CDC log (via `scylla-cdc`) and
//! applies mutations to the target with at-least-once semantics.
//!
//! Prerequisites: the source table must have CDC enabled
//! (`WITH cdc = {'enabled': true}`). Odyssey does not invent changes from CommitLog.

mod apply;
mod catchup;
mod error;

pub use apply::{ApplyPlan, plan_apply};
pub use catchup::{CatchupOptions, CatchupReport, run_catchup};
pub use error::CdcError;
