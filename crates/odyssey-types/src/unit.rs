use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{MigrationState, TokenRange, TypesError};

/// One schedulable piece of migration work: a table plus a token range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationUnit {
    pub id: Uuid,
    pub keyspace: String,
    pub table: String,
    pub range: TokenRange,
    pub state: MigrationState,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    /// Opaque paging state from the source driver, when a unit was interrupted.
    pub paging_state: Option<Vec<u8>>,
}

impl MigrationUnit {
    pub fn new(
        keyspace: impl Into<String>,
        table: impl Into<String>,
        range: TokenRange,
    ) -> Result<Self, TypesError> {
        let keyspace = keyspace.into();
        let table = table.into();
        if keyspace.trim().is_empty() {
            return Err(TypesError::EmptyKeyspace);
        }
        if table.trim().is_empty() {
            return Err(TypesError::EmptyTable);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            keyspace,
            table,
            range,
            state: MigrationState::Pending,
            rows_read: 0,
            rows_written: 0,
            bytes_written: 0,
            paging_state: None,
        })
    }

    pub fn mark_running(&mut self) {
        self.state = MigrationState::Running;
    }

    pub fn mark_completed(&mut self) {
        self.state = MigrationState::Completed;
    }

    pub fn mark_failed(&mut self) {
        self.state = MigrationState::Failed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_pending_unit() {
        let range = TokenRange::new(0, 100).unwrap();
        let unit = MigrationUnit::new("ks", "events", range).unwrap();
        assert_eq!(unit.keyspace, "ks");
        assert_eq!(unit.table, "events");
        assert_eq!(unit.state, MigrationState::Pending);
        assert_eq!(unit.rows_read, 0);
        assert!(unit.paging_state.is_none());
    }

    #[test]
    fn rejects_blank_names() {
        let range = TokenRange::new(0, 1).unwrap();
        assert!(matches!(
            MigrationUnit::new(" ", "t", range),
            Err(TypesError::EmptyKeyspace)
        ));
        assert!(matches!(
            MigrationUnit::new("ks", "", range),
            Err(TypesError::EmptyTable)
        ));
    }
}
