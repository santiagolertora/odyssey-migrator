use odyssey_types::TokenRange;

use crate::PlannerError;

/// Controls how finely a parent token range is subdivided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanOptions {
    /// Target number of child ranges. Actual count may be slightly lower when
    /// the parent width cannot be divided evenly, but never exceeds this value.
    pub desired_units: usize,
}

/// Split the full Murmur3 space into `desired_units` contiguous ranges.
pub fn split_murmur3(options: PlanOptions) -> Result<Vec<TokenRange>, PlannerError> {
    split_range(TokenRange::murmur3_full(), options)
}

/// Split an arbitrary parent range into contiguous child ranges.
///
/// Children cover `[parent.start, parent.end]` without gaps or overlaps.
/// The last child absorbs any remainder from integer division.
pub fn split_range(
    parent: TokenRange,
    options: PlanOptions,
) -> Result<Vec<TokenRange>, PlannerError> {
    if options.desired_units == 0 {
        return Err(PlannerError::ZeroUnits);
    }

    let width = parent.width();
    if width == 0 {
        return Err(PlannerError::Range(
            "parent range has zero width".into(),
        ));
    }

    let units = options.desired_units as u128;
    let units = units.min(width).max(1);
    let step = width / units;

    let mut ranges = Vec::with_capacity(units as usize);
    let mut cursor = parent.start as i128;

    for index in 0..units {
        let next = if index + 1 == units {
            parent.end as i128
        } else {
            cursor + step as i128
        };

        let start = i64::try_from(cursor).map_err(|_| {
            PlannerError::Range(format!("token start {cursor} is out of i64 range"))
        })?;
        let end = i64::try_from(next).map_err(|_| {
            PlannerError::Range(format!("token end {next} is out of i64 range"))
        })?;

        let range = TokenRange::new(start, end)
            .map_err(|err| PlannerError::Range(err.to_string()))?;
        ranges.push(range);
        cursor = next;
    }

    Ok(ranges)
}

/// Subdivide vnode parent ranges until approximately `desired_units` children.
pub fn split_topology(
    parents: &[TokenRange],
    options: PlanOptions,
) -> Result<Vec<TokenRange>, PlannerError> {
    if options.desired_units == 0 {
        return Err(PlannerError::ZeroUnits);
    }
    if parents.is_empty() {
        return Err(PlannerError::Range("no vnode parent ranges".into()));
    }

    let n = parents.len();
    let base = (options.desired_units / n).max(1);
    let mut extra = options.desired_units.saturating_sub(base * n);
    let mut out = Vec::with_capacity(options.desired_units.max(n));

    for parent in parents {
        let units = base + if extra > 0 {
            extra -= 1;
            1
        } else {
            0
        };
        let children = split_range(*parent, PlanOptions { desired_units: units })?;
        out.extend(children);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_units() {
        assert_eq!(
            split_murmur3(PlanOptions { desired_units: 0 }),
            Err(PlannerError::ZeroUnits)
        );
    }

    #[test]
    fn splits_evenly_without_gaps() {
        let parent = TokenRange::new(0, 100).unwrap();
        let ranges = split_range(parent, PlanOptions { desired_units: 4 }).unwrap();
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], TokenRange::new(0, 25).unwrap());
        assert_eq!(ranges[1], TokenRange::new(25, 50).unwrap());
        assert_eq!(ranges[2], TokenRange::new(50, 75).unwrap());
        assert_eq!(ranges[3], TokenRange::new(75, 100).unwrap());
    }

    #[test]
    fn last_range_absorbs_remainder() {
        let parent = TokenRange::new(0, 10).unwrap();
        let ranges = split_range(parent, PlanOptions { desired_units: 3 }).unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[2].end, 10);
        for window in ranges.windows(2) {
            assert_eq!(window[0].end, window[1].start);
        }
    }

    #[test]
    fn caps_units_to_width() {
        let parent = TokenRange::new(0, 3).unwrap();
        let ranges = split_range(parent, PlanOptions { desired_units: 100 }).unwrap();
        assert_eq!(ranges.len(), 3);
    }

    #[test]
    fn murmur3_full_produces_requested_count() {
        let ranges = split_murmur3(PlanOptions {
            desired_units: 16,
        })
        .unwrap();
        assert_eq!(ranges.len(), 16);
        assert_eq!(ranges.first().unwrap().start, i64::MIN);
        assert_eq!(ranges.last().unwrap().end, i64::MAX);
        for window in ranges.windows(2) {
            assert_eq!(window[0].end, window[1].start);
        }
    }

    #[test]
    fn topology_subdivides_parents() {
        let parents = vec![
            TokenRange::new(0, 100).unwrap(),
            TokenRange::new(100, 200).unwrap(),
        ];
        let ranges = split_topology(&parents, PlanOptions { desired_units: 4 }).unwrap();
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, 200);
    }
}
