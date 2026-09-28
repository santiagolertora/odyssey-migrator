use serde::{Deserialize, Serialize};

/// Lifecycle of a single migration unit (one token range).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
    Pending,
    Running,
    Completed,
    Failed,
}

impl MigrationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }

    pub fn is_resumable(self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::Failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_and_resumable_flags() {
        assert!(!MigrationState::Pending.is_terminal());
        assert!(MigrationState::Pending.is_resumable());
        assert!(MigrationState::Completed.is_terminal());
        assert!(!MigrationState::Completed.is_resumable());
        assert!(MigrationState::Failed.is_resumable());
    }
}
