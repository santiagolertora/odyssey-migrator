//! Data integrity policy for migrations.
//!
//! Odyssey's product promise is simple: never lose a row that was successfully
//! read from the source. Prefer duplicates and retries over silent drops.
//! Cassandra/Scylla INSERT-by-primary-key is idempotent for the same cell
//! values, so at-least-once delivery is the safe default for V0.1.

use serde::{Deserialize, Serialize};

/// How Odyssey treats uncertainty between read and durable checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityGuarantee {
    /// Every source row that Odyssey acknowledged as read must either be written
    /// to the target or leave the migration in a failed/resumable state. Odyssey
    /// never marks a unit `completed` while writes or the checkpoint are in
    /// doubt. Re-running a unit may rewrite the same primary keys (at-least-once).
    AtLeastOnce,
}

impl Default for DurabilityGuarantee {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

/// Hard rules the engine must obey. These are not tunables for convenience —
/// changing them changes the safety model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityPolicy {
    #[serde(default)]
    pub guarantee: DurabilityGuarantee,
    /// If true, any write error fails the unit immediately after retries are
    /// exhausted. Odyssey never skips the failed batch and continues.
    #[serde(default = "default_true")]
    pub fail_unit_on_write_error: bool,
    /// If true, Odyssey refuses to mark a unit completed unless the checkpoint
    /// write for that unit succeeded.
    #[serde(default = "default_true")]
    pub require_checkpoint_before_complete: bool,
    /// If true, CTRL-C / cancellation drains in-flight writes and checkpoints
    /// progress before exiting. Aborting mid-batch leaves the unit resumable,
    /// never "completed".
    #[serde(default = "default_true")]
    pub checkpoint_on_cancel: bool,
}

fn default_true() -> bool {
    true
}

impl Default for IntegrityPolicy {
    fn default() -> Self {
        Self {
            guarantee: DurabilityGuarantee::AtLeastOnce,
            fail_unit_on_write_error: true,
            require_checkpoint_before_complete: true,
            checkpoint_on_cancel: true,
        }
    }
}

impl IntegrityPolicy {
    /// Validates that the policy cannot be weakened into a data-loss mode.
    pub fn validate(&self) -> Result<(), String> {
        if !self.fail_unit_on_write_error {
            return Err(
                "integrity.fail_unit_on_write_error cannot be false: Odyssey refuses to skip failed writes"
                    .into(),
            );
        }
        if !self.require_checkpoint_before_complete {
            return Err(
                "integrity.require_checkpoint_before_complete cannot be false: completed units must be durable"
                    .into(),
            );
        }
        if self.guarantee != DurabilityGuarantee::AtLeastOnce {
            return Err(
                "integrity.guarantee must be at_least_once in V0.1".into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_safe() {
        let policy = IntegrityPolicy::default();
        assert!(policy.validate().is_ok());
        assert!(policy.fail_unit_on_write_error);
        assert!(policy.require_checkpoint_before_complete);
        assert!(policy.checkpoint_on_cancel);
    }

    #[test]
    fn refuses_to_skip_write_errors() {
        let policy = IntegrityPolicy {
            fail_unit_on_write_error: false,
            ..IntegrityPolicy::default()
        };
        assert!(policy.validate().unwrap_err().contains("fail_unit_on_write_error"));
    }
}
