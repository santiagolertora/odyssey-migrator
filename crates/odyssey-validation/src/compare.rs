//! Pure helpers that turn per-range digests into a validation report.

use odyssey_core::ValidationMode;
use odyssey_types::TokenRange;

use crate::report::{RangeDigest, ValidationReport};

/// Build one [`RangeDigest`] from source/target hex digests and row counts.
pub fn range_digest(
    range: TokenRange,
    source_digest: String,
    target_digest: String,
    source_rows: u64,
    target_rows: u64,
) -> RangeDigest {
    let matches = source_digest == target_digest && source_rows == target_rows;
    RangeDigest {
        range,
        source_digest,
        target_digest,
        source_rows,
        target_rows,
        matches,
        diffs: Vec::new(),
    }
}

/// Aggregate per-range outcomes. `all_match` is true only when every range
/// matched; an empty range list is treated as a match (nothing disagreed).
pub fn report_from_ranges(mode: ValidationMode, ranges: Vec<RangeDigest>) -> ValidationReport {
    let all_match = ranges.iter().all(|r| r.matches);
    ValidationReport {
        mode,
        ranges,
        all_match,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_types::TokenRange;

    #[test]
    fn range_matches_when_digest_and_counts_agree() {
        let range = TokenRange::new(0, 10).unwrap();
        let result = range_digest(range, "aaa".into(), "aaa".into(), 3, 3);
        assert!(result.matches);
    }

    #[test]
    fn range_mismatches_on_row_count_even_if_digest_equal() {
        // Defensive: callers should not produce this, but comparison is explicit.
        let range = TokenRange::new(0, 10).unwrap();
        let result = range_digest(range, "aaa".into(), "aaa".into(), 3, 4);
        assert!(!result.matches);
    }

    #[test]
    fn empty_range_list_all_match() {
        let report = report_from_ranges(ValidationMode::Digest, Vec::new());
        assert!(report.all_match);
        assert!(report.ranges.is_empty());
    }

    #[test]
    fn two_empty_range_digests_match() {
        let a = range_digest(
            TokenRange::new(0, 10).unwrap(),
            "e3b0".into(),
            "e3b0".into(),
            0,
            0,
        );
        let b = range_digest(
            TokenRange::new(10, 20).unwrap(),
            "e3b0".into(),
            "e3b0".into(),
            0,
            0,
        );
        let report = report_from_ranges(ValidationMode::Digest, vec![a, b]);
        assert!(report.all_match);
    }
}
