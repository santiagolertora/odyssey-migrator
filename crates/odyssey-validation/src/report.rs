use odyssey_core::ValidationMode;
use odyssey_types::TokenRange;

/// One differing column in a DiffSample row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDiff {
    pub name: String,
    pub source: String,
    pub target: String,
}

/// One sampled PK with column-level source vs target values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDiffSample {
    pub primary_key: String,
    pub columns: Vec<ColumnDiff>,
    /// Row present only on source / only on target / both but cells differ.
    pub kind: DiffKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffKind {
    OnlySource,
    OnlyTarget,
    ValueMismatch,
}

/// Digest comparison for one token range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDigest {
    pub range: TokenRange,
    /// Hex-encoded blake3 range digest from the source cluster.
    pub source_digest: String,
    /// Hex-encoded blake3 range digest from the target cluster.
    pub target_digest: String,
    pub source_rows: u64,
    pub target_rows: u64,
    pub matches: bool,
    /// Populated in Full mode when digests disagree (DiffSample).
    pub diffs: Vec<RowDiffSample>,
}

/// Aggregate result of [`crate::validate_table`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationReport {
    pub mode: ValidationMode,
    pub ranges: Vec<RangeDigest>,
    pub all_match: bool,
}
