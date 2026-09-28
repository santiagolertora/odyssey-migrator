use std::collections::VecDeque;
use std::time::Duration;

use odyssey_core::{AdaptiveConfig, ConcurrencyConfig, EngineConfig};

/// AIMD algorithm constants. Not environment paths — they define the controller.
/// Concurrency bounds and target latency come from [`EngineConfig`].
pub const AIMD_DECREASE_FACTOR: f64 = 0.8;
pub const AIMD_INCREASE_STEP: usize = 4;
pub const AIMD_INCREASE_LATENCY_RATIO: f64 = 0.7;
/// Sliding window used for a coarse p99 estimate.
pub const LATENCY_SAMPLE_WINDOW: usize = 32;

/// Adaptive write concurrency controller (AIMD).
#[derive(Debug, Clone)]
pub struct AdaptiveConcurrency {
    current: usize,
    minimum: usize,
    maximum: usize,
    target_p99: Duration,
    enabled: bool,
    samples: VecDeque<Duration>,
}

impl AdaptiveConcurrency {
    pub fn from_engine(engine: &EngineConfig) -> Self {
        Self::new(&engine.concurrency, &engine.adaptive)
    }

    pub fn new(concurrency: &ConcurrencyConfig, adaptive: &AdaptiveConfig) -> Self {
        let current = concurrency
            .initial
            .clamp(concurrency.minimum, concurrency.maximum);
        Self {
            current,
            minimum: concurrency.minimum,
            maximum: concurrency.maximum,
            target_p99: Duration::from_millis(adaptive.target_p99_ms),
            enabled: adaptive.enabled,
            samples: VecDeque::with_capacity(LATENCY_SAMPLE_WINDOW),
        }
    }

    pub fn current(&self) -> usize {
        self.current.max(1)
    }

    pub fn record_sample(&mut self, latency: Duration) {
        if self.samples.len() >= LATENCY_SAMPLE_WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(latency);
        if self.enabled {
            self.adjust();
        }
    }

    /// Observe one latency sample and apply AIMD.
    fn adjust(&mut self) {
        let observed = self.p99_ish().max(self.samples.back().copied().unwrap_or_default());
        let target = self.target_p99;
        if observed > target {
            let decreased = ((self.current as f64) * AIMD_DECREASE_FACTOR).floor() as usize;
            self.current = decreased.max(self.minimum);
        } else if observed < duration_mul(target, AIMD_INCREASE_LATENCY_RATIO) {
            self.current = self
                .current
                .saturating_add(AIMD_INCREASE_STEP)
                .min(self.maximum);
        }
        self.current = self.current.clamp(self.minimum, self.maximum);
    }

    /// Coarse percentile: the sample near the 99th percentile of the window.
    fn p99_ish(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted: Vec<Duration> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
        let idx = idx.saturating_sub(1).min(sorted.len() - 1);
        sorted[idx]
    }
}

fn duration_mul(d: Duration, factor: f64) -> Duration {
    Duration::from_secs_f64(d.as_secs_f64() * factor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_core::{AdaptiveConfig, ConcurrencyConfig};

    fn controller(initial: usize, min: usize, max: usize, target_ms: u64) -> AdaptiveConcurrency {
        AdaptiveConcurrency::new(
            &ConcurrencyConfig {
                initial,
                minimum: min,
                maximum: max,
            },
            &AdaptiveConfig {
                enabled: true,
                target_p99_ms: target_ms,
            },
        )
    }

    #[test]
    fn increases_when_latency_well_below_target() {
        let mut c = controller(64, 8, 512, 15);
        // Far below 0.7 * 15ms ≈ 10.5ms
        c.record_sample(Duration::from_millis(5));
        assert_eq!(c.current(), 68);
    }

    #[test]
    fn decreases_when_latency_above_target() {
        let mut c = controller(100, 8, 512, 15);
        c.record_sample(Duration::from_millis(25));
        assert_eq!(c.current(), 80); // 100 * 0.8
    }

    #[test]
    fn clamps_to_minimum_on_decrease() {
        let mut c = controller(10, 8, 512, 15);
        c.record_sample(Duration::from_millis(50));
        assert_eq!(c.current(), 8);
    }

    #[test]
    fn clamps_to_maximum_on_increase() {
        let mut c = controller(510, 8, 512, 15);
        c.record_sample(Duration::from_millis(1));
        assert_eq!(c.current(), 512);
    }

    #[test]
    fn disabled_keeps_initial_concurrency() {
        let mut c = AdaptiveConcurrency::new(
            &ConcurrencyConfig {
                initial: 64,
                minimum: 8,
                maximum: 512,
            },
            &AdaptiveConfig {
                enabled: false,
                target_p99_ms: 15,
            },
        );
        c.record_sample(Duration::from_millis(100));
        c.record_sample(Duration::from_millis(1));
        assert_eq!(c.current(), 64);
    }
}
