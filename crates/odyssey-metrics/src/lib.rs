//! In-process Prometheus-style metrics for Odyssey Migrator.
//!
//! Counters and gauges are backed by a real [`prometheus_client::registry::Registry`].
//! Callers bump values during migration and can either scrape via
//! [`OdysseyMetrics::encode_text`] or expose `/metrics` with [`serve_metrics`].

mod error;
mod serve;

use std::sync::atomic::AtomicU64;
use std::time::Duration;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

pub use error::MetricsError;
pub use serve::serve_metrics;

type U64Gauge = Gauge<u64, AtomicU64>;

/// Point-in-time view of Odyssey counter/gauge values (no histograms).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetricsSnapshot {
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub retries: u64,
    pub errors: u64,
    pub ranges_pending: u64,
    pub ranges_running: u64,
    pub ranges_completed: u64,
    pub worker_concurrency: u64,
}

/// Shared Odyssey migration metrics registered with a Prometheus client registry.
#[derive(Debug)]
pub struct OdysseyMetrics {
    registry: Registry,
    rows_read: Counter,
    rows_written: Counter,
    bytes_read: Counter,
    bytes_written: Counter,
    retries: Counter,
    errors: Counter,
    ranges_pending: U64Gauge,
    ranges_running: U64Gauge,
    ranges_completed: U64Gauge,
    worker_concurrency: U64Gauge,
    source_latency_seconds: Histogram,
    target_latency_seconds: Histogram,
}

impl OdysseyMetrics {
    /// Create a registry and register all V0.1 Odyssey metric series.
    ///
    /// Counter names are registered without the `_total` suffix; OpenMetrics
    /// encoding appends it automatically (e.g. `odyssey_rows_read` →
    /// `odyssey_rows_read_total`).
    pub fn new() -> Self {
        let mut registry = <Registry>::default();

        let rows_read = Counter::default();
        registry.register(
            "odyssey_rows_read",
            "Total rows read from the source",
            rows_read.clone(),
        );

        let rows_written = Counter::default();
        registry.register(
            "odyssey_rows_written",
            "Total rows written to the target",
            rows_written.clone(),
        );

        let bytes_read = Counter::default();
        registry.register(
            "odyssey_bytes_read",
            "Estimated bytes read from the source",
            bytes_read.clone(),
        );

        let bytes_written = Counter::default();
        registry.register(
            "odyssey_bytes_written",
            "Estimated bytes written to the target",
            bytes_written.clone(),
        );

        let retries = Counter::default();
        registry.register(
            "odyssey_retries",
            "Total write/read retries",
            retries.clone(),
        );

        let errors = Counter::default();
        registry.register(
            "odyssey_errors",
            "Total hard errors observed",
            errors.clone(),
        );

        let ranges_pending = U64Gauge::default();
        registry.register(
            "odyssey_ranges_pending",
            "Migration units still pending",
            ranges_pending.clone(),
        );

        let ranges_running = U64Gauge::default();
        registry.register(
            "odyssey_ranges_running",
            "Migration units currently running",
            ranges_running.clone(),
        );

        let ranges_completed = U64Gauge::default();
        registry.register(
            "odyssey_ranges_completed",
            "Migration units completed",
            ranges_completed.clone(),
        );

        let worker_concurrency = U64Gauge::default();
        registry.register(
            "odyssey_worker_concurrency",
            "Current in-flight write concurrency",
            worker_concurrency.clone(),
        );

        // Latency buckets from ~1ms to ~16s (suitable for CQL p50/p99).
        let source_latency_seconds = Histogram::new(exponential_buckets(0.001, 2.0, 15));
        registry.register(
            "odyssey_source_latency_seconds",
            "Source page-read latency in seconds",
            source_latency_seconds.clone(),
        );

        let target_latency_seconds = Histogram::new(exponential_buckets(0.001, 2.0, 15));
        registry.register(
            "odyssey_target_latency_seconds",
            "Target row-write latency in seconds",
            target_latency_seconds.clone(),
        );

        Self {
            registry,
            rows_read,
            rows_written,
            bytes_read,
            bytes_written,
            retries,
            errors,
            ranges_pending,
            ranges_running,
            ranges_completed,
            worker_concurrency,
            source_latency_seconds,
            target_latency_seconds,
        }
    }

    pub fn rows_read_add(&self, n: u64) {
        self.rows_read.inc_by(n);
    }

    pub fn rows_written_add(&self, n: u64) {
        self.rows_written.inc_by(n);
    }

    pub fn bytes_read_add(&self, n: u64) {
        self.bytes_read.inc_by(n);
    }

    pub fn bytes_written_add(&self, n: u64) {
        self.bytes_written.inc_by(n);
    }

    pub fn retries_add(&self, n: u64) {
        self.retries.inc_by(n);
    }

    pub fn errors_add(&self, n: u64) {
        self.errors.inc_by(n);
    }

    pub fn set_ranges_pending(&self, n: u64) {
        self.ranges_pending.set(n);
    }

    pub fn set_ranges_running(&self, n: u64) {
        self.ranges_running.set(n);
    }

    pub fn set_ranges_completed(&self, n: u64) {
        self.ranges_completed.set(n);
    }

    pub fn set_worker_concurrency(&self, n: u64) {
        self.worker_concurrency.set(n);
    }

    pub fn observe_source_latency(&self, latency: Duration) {
        self.source_latency_seconds
            .observe(latency.as_secs_f64().max(0.0));
    }

    pub fn observe_target_latency(&self, latency: Duration) {
        self.target_latency_seconds
            .observe(latency.as_secs_f64().max(0.0));
    }

    /// Point-in-time counter/gauge values for the migration dashboard JSON API.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            rows_read: self.rows_read.get(),
            rows_written: self.rows_written.get(),
            bytes_read: self.bytes_read.get(),
            bytes_written: self.bytes_written.get(),
            retries: self.retries.get(),
            errors: self.errors.get(),
            ranges_pending: self.ranges_pending.get(),
            ranges_running: self.ranges_running.get(),
            ranges_completed: self.ranges_completed.get(),
            worker_concurrency: self.worker_concurrency.get(),
        }
    }

    /// Encode all registered metrics as OpenMetrics text.
    pub fn encode_text(&self) -> Result<String, MetricsError> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)
            .map_err(|err| MetricsError::Encode(err.to_string()))?;
        Ok(buffer)
    }
}

impl Default for OdysseyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    #[test]
    fn recording_updates_openmetrics_text() {
        let m = OdysseyMetrics::new();
        m.rows_read_add(10);
        m.rows_read_add(5);
        m.rows_written_add(7);
        m.bytes_read_add(50);
        m.bytes_written_add(100);
        m.retries_add(2);
        m.errors_add(1);
        m.set_ranges_pending(3);
        m.set_ranges_running(1);
        m.set_ranges_completed(8);
        m.set_worker_concurrency(64);
        m.observe_source_latency(Duration::from_millis(5));
        m.observe_target_latency(Duration::from_millis(3));

        let text = m.encode_text().expect("encode");
        assert!(text.contains("odyssey_rows_read_total 15"));
        assert!(text.contains("odyssey_rows_written_total 7"));
        assert!(text.contains("odyssey_bytes_read_total 50"));
        assert!(text.contains("odyssey_bytes_written_total 100"));
        assert!(text.contains("odyssey_retries_total 2"));
        assert!(text.contains("odyssey_errors_total 1"));
        assert!(text.contains("odyssey_ranges_pending 3"));
        assert!(text.contains("odyssey_source_latency_seconds"));
        assert!(text.contains("odyssey_target_latency_seconds"));
        assert!(text.contains("# TYPE odyssey_source_latency_seconds histogram"));
        assert!(text.contains("# EOF"));
    }

    #[tokio::test]
    async fn serve_metrics_returns_encoded_body() {
        let metrics = Arc::new(OdysseyMetrics::new());
        metrics.rows_read_add(42);
        metrics.set_ranges_pending(9);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        drop(listener);

        let server_metrics = Arc::clone(&metrics);
        let server = tokio::spawn(async move {
            let _ = serve_metrics(addr, server_metrics).await;
        });

        let mut stream = None;
        for _ in 0..50 {
            match TcpStream::connect(addr).await {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        let mut stream = stream.expect("connect to metrics server");

        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write request");

        let mut buf = Vec::new();
        timeout(Duration::from_secs(2), stream.read_to_end(&mut buf))
            .await
            .expect("read timeout")
            .expect("read response");
        let response = String::from_utf8(buf).expect("utf8");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("odyssey_rows_read_total 42"));
        assert!(response.contains("odyssey_ranges_pending 9"));
        assert!(response.contains("application/openmetrics-text"));

        server.abort();
    }
}
