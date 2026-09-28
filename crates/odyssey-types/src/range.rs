use serde::{Deserialize, Serialize};

use crate::TypesError;

/// Half-open token interval used as the atomic unit of migration work.
///
/// Cassandra/Scylla token predicates use exclusive lower bounds and inclusive
/// upper bounds (`token(pk) > start AND token(pk) <= end`). Odyssey stores the
/// same convention so planner output can be turned into CQL without remapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

impl TokenRange {
    /// Full Murmur3 token space used by Cassandra and Scylla by default.
    pub const MURMUR3_MIN: i64 = i64::MIN;
    pub const MURMUR3_MAX: i64 = i64::MAX;

    /// Creates a range. `start` must be strictly less than `end`.
    pub fn new(start: i64, end: i64) -> Result<Self, TypesError> {
        if start >= end {
            return Err(TypesError::InvalidTokenRange { start, end });
        }
        Ok(Self { start, end })
    }

    /// The complete Murmur3 ring as a single range.
    ///
    /// Note: the real ring wraps; callers that need wrap-around coverage must
    /// split with the planner rather than treating this as a CQL predicate.
    pub fn murmur3_full() -> Self {
        Self {
            start: Self::MURMUR3_MIN,
            end: Self::MURMUR3_MAX,
        }
    }

    /// Inclusive width of the range when both ends fit in the same signed span.
    ///
    /// Returns `None` when the width would overflow `u128` arithmetic that we
    /// use for splitting (should not happen for valid Murmur3 sub-ranges).
    pub fn width(&self) -> u128 {
        (self.end as i128 - self.start as i128) as u128
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_or_inverted_ranges() {
        assert!(matches!(
            TokenRange::new(10, 10),
            Err(TypesError::InvalidTokenRange { .. })
        ));
        assert!(matches!(
            TokenRange::new(20, 10),
            Err(TypesError::InvalidTokenRange { .. })
        ));
    }

    #[test]
    fn accepts_ordered_range() {
        let range = TokenRange::new(-100, 100).expect("valid");
        assert_eq!(range.start, -100);
        assert_eq!(range.end, 100);
        assert_eq!(range.width(), 200);
    }

    #[test]
    fn murmur3_full_covers_signed_i64() {
        let full = TokenRange::murmur3_full();
        assert_eq!(full.start, i64::MIN);
        assert_eq!(full.end, i64::MAX);
    }
}
