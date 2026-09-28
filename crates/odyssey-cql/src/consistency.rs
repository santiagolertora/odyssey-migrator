use odyssey_core::ConsistencyLevel;
use scylla::statement::Consistency;

/// Map Odyssey's config consistency to the Scylla Rust driver enum.
pub fn to_driver_consistency(level: ConsistencyLevel) -> Consistency {
    match level {
        ConsistencyLevel::One => Consistency::One,
        ConsistencyLevel::LocalOne => Consistency::LocalOne,
        ConsistencyLevel::Quorum => Consistency::Quorum,
        ConsistencyLevel::LocalQuorum => Consistency::LocalQuorum,
        ConsistencyLevel::All => Consistency::All,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_local_quorum() {
        assert_eq!(
            to_driver_consistency(ConsistencyLevel::LocalQuorum),
            Consistency::LocalQuorum
        );
    }
}
