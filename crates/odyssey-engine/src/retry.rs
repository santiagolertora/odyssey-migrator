use std::time::Duration;

use odyssey_core::EngineConfig;

use crate::EngineError;

/// Retry defaults for write failures. Kept in one place so tunables are not
/// sprinkled through the writer.
///
/// Prefer overriding via [`RetryPolicy::from_engine`] when EngineConfig gains a
/// dedicated retry section; until then these named constants are the contract.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 8;
pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 50;
pub const DEFAULT_MAX_BACKOFF_MS: u64 = 30_000;
pub const DEFAULT_BACKOFF_MULTIPLIER: f64 = 2.0;

/// Exponential backoff policy for target writes.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff: Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS),
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
            multiplier: DEFAULT_BACKOFF_MULTIPLIER,
        }
    }
}

impl RetryPolicy {
    /// Build from engine config. Today EngineConfig has no retry section, so
    /// this returns [`RetryPolicy::default`] after validating page/worker knobs
    /// are sane (already enforced by Config::validate).
    pub fn from_engine(_engine: &EngineConfig) -> Self {
        Self::default()
    }

    /// Backoff before attempt `attempt` (1-based). Attempt 1 has zero delay.
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return Duration::from_millis(0);
        }
        let exp = attempt.saturating_sub(2) as i32;
        let factor = self.multiplier.powi(exp);
        let millis = (self.initial_backoff.as_millis() as f64 * factor).round() as u64;
        let capped = millis.min(self.max_backoff.as_millis() as u64);
        Duration::from_millis(capped)
    }

    /// Run an async operation until it succeeds or attempts are exhausted.
    ///
    /// Returns `(value, retry_count)` where `retry_count` is attempts after the first
    /// (`0` when the first attempt succeeds).
    pub async fn run<F, Fut, T, E>(&self, mut op: F) -> Result<(T, u32), EngineError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        if self.max_attempts == 0 {
            return Err(EngineError::InvalidConfig(
                "retry max_attempts must be >= 1".into(),
            ));
        }

        let mut last_error = String::new();
        for attempt in 1..=self.max_attempts {
            let delay = self.backoff_for_attempt(attempt);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            match op().await {
                Ok(value) => {
                    let retries = attempt.saturating_sub(1);
                    return Ok((value, retries));
                }
                Err(err) => {
                    last_error = err.to_string();
                    tracing::warn!(
                        attempt,
                        max_attempts = self.max_attempts,
                        error = %last_error,
                        "write attempt failed; will retry if attempts remain"
                    );
                }
            }
        }

        Err(EngineError::Write(format!(
            "exhausted {} attempts: {last_error}",
            self.max_attempts
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_attempt_has_no_backoff() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.backoff_for_attempt(1), Duration::from_millis(0));
    }

    #[test]
    fn backoff_grows_exponentially_then_caps() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1_000),
            multiplier: 2.0,
        };
        assert_eq!(policy.backoff_for_attempt(2), Duration::from_millis(100));
        assert_eq!(policy.backoff_for_attempt(3), Duration::from_millis(200));
        assert_eq!(policy.backoff_for_attempt(4), Duration::from_millis(400));
        assert_eq!(policy.backoff_for_attempt(5), Duration::from_millis(800));
        assert_eq!(policy.backoff_for_attempt(6), Duration::from_millis(1_000));
        assert_eq!(policy.backoff_for_attempt(7), Duration::from_millis(1_000));
    }

    #[test]
    fn default_matches_named_constants() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(
            policy.initial_backoff,
            Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS)
        );
        assert_eq!(
            policy.max_backoff,
            Duration::from_millis(DEFAULT_MAX_BACKOFF_MS)
        );
        assert_eq!(policy.multiplier, DEFAULT_BACKOFF_MULTIPLIER);
    }
}
