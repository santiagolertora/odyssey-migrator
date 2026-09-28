//! Token-range planning.
//!
//! Supports even Murmur3 splits and topology-aware subdivision of vnode parents.

mod error;
mod splitter;

pub use error::PlannerError;
pub use splitter::{PlanOptions, split_murmur3, split_range, split_topology};
