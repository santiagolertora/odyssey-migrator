//! Optional live progress callbacks for metrics / status UIs.

use std::time::Duration;

/// Stats for one successfully checkpointed page.
#[derive(Debug, Clone)]
pub struct PageProgress {
    pub rows: u64,
    pub bytes: u64,
    pub source_latency: Duration,
    pub target_latencies: Vec<Duration>,
    pub retries: u64,
    pub write_concurrency: usize,
}

/// Observer notified as pages and units complete.
///
/// Implement in the CLI (Prometheus) without pulling metrics into the engine.
pub trait ProgressObserver: Send + Sync {
    fn page_copied(&self, page: &PageProgress);
    fn unit_claimed(&self);
    fn unit_completed(&self);
    fn unit_failed(&self);
}

impl ProgressObserver for () {
    fn page_copied(&self, _page: &PageProgress) {}
    fn unit_claimed(&self) {}
    fn unit_completed(&self) {}
    fn unit_failed(&self) {}
}
