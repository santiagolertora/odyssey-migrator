use std::fs;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{CoreError, IntegrityPolicy, LogFormat, Verbosity};

/// Top-level Odyssey configuration loaded from a TOML file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub migration: MigrationMeta,
    pub source: SourceConfig,
    pub target: TargetConfig,
    pub tables: Vec<TableMapping>,
    #[serde(default)]
    pub engine: EngineConfig,
    #[serde(default)]
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub validation: ValidationConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Optional migration dashboard HTTP UI (better than Spark Master for Odyssey).
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub integrity: IntegrityPolicy,
    #[serde(default)]
    pub live: LiveConfig,
    /// Optional HTTP webhook on migration complete / error (Slack-compatible JSON).
    #[serde(default)]
    pub notify: NotifyConfig,
    #[serde(default)]
    pub planner: PlannerConfig,
    #[serde(default)]
    pub mesh: MeshConfig,
    /// HTTP dual-write gateway for app cutover (source then target).
    #[serde(default)]
    pub dual_write: DualWriteConfig,
}

/// Outbound webhook notifications for ops (complete / error).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyConfig {
    /// If empty, notifications are disabled.
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default = "default_true")]
    pub on_complete: bool,
    #[serde(default = "default_true")]
    pub on_error: bool,
    #[serde(default = "default_notify_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_notify_timeout_secs() -> u64 {
    5
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            webhook_url: String::new(),
            on_complete: true,
            on_error: true,
            timeout_secs: default_notify_timeout_secs(),
        }
    }
}

impl NotifyConfig {
    pub fn enabled(&self) -> bool {
        !self.webhook_url.trim().is_empty()
    }
}

/// How token ranges are planned for bulk migrate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannerMode {
    /// Even split of the full Murmur3 ring (default).
    Even,
    /// Discover vnode token owners and subdivide each vnode.
    Vnode,
}

impl Default for PlannerMode {
    fn default() -> Self {
        Self::Even
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerConfig {
    #[serde(default)]
    pub mode: PlannerMode,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            mode: PlannerMode::Even,
        }
    }
}

/// Cross-process claim hardening for a shared SQLite checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeshConfig {
    /// Stable worker identity written into claims (default: hostname).
    #[serde(default)]
    pub worker_id: String,
    /// Reclaim `running` units whose heartbeat is older than this many seconds.
    #[serde(default = "default_mesh_lease_secs")]
    pub lease_secs: u64,
    /// SQLite busy_timeout in milliseconds for multi-process contention.
    #[serde(default = "default_mesh_busy_timeout_ms")]
    pub busy_timeout_ms: u64,
}

fn default_mesh_lease_secs() -> u64 {
    120
}

fn default_mesh_busy_timeout_ms() -> u64 {
    5_000
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            worker_id: String::new(),
            lease_secs: default_mesh_lease_secs(),
            busy_timeout_ms: default_mesh_busy_timeout_ms(),
        }
    }
}

impl MeshConfig {
    pub fn resolved_worker_id(&self) -> String {
        if !self.worker_id.trim().is_empty() {
            return self.worker_id.trim().to_string();
        }
        std::env::var("ODYSSEY_WORKER_ID")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "odyssey-worker".into())
    }
}

/// CQL consistency level for source reads and target writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyLevel {
    One,
    LocalOne,
    Quorum,
    LocalQuorum,
    All,
}

impl Default for ConsistencyLevel {
    fn default() -> Self {
        Self::LocalQuorum
    }
}

impl ConsistencyLevel {
    /// Snake-case name for logs and config round-trips.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::LocalOne => "local_one",
            Self::Quorum => "quorum",
            Self::LocalQuorum => "local_quorum",
            Self::All => "all",
        }
    }

    /// Name form expected by Scylla/Cassandra CQL consistency keywords.
    pub fn to_scylla_name(self) -> &'static str {
        match self {
            Self::One => "ONE",
            Self::LocalOne => "LOCAL_ONE",
            Self::Quorum => "QUORUM",
            Self::LocalQuorum => "LOCAL_QUORUM",
            Self::All => "ALL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationMeta {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Source protocol. V0.1 only supports `cql`.
    #[serde(default = "default_source_kind")]
    pub kind: String,
    pub contact_points: Vec<String>,
    pub datacenter: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub consistency: ConsistencyLevel,
    /// If true, only open connections to the configured contact points
    /// (ignore cluster peer discovery). Required for SSH local-forward tunnels.
    #[serde(default)]
    pub contact_points_only: bool,
    /// Map cluster-broadcast addresses to locally reachable ones (SSH tunnels).
    /// Example: `10.108.0.86:9042` → `127.0.0.1:19042`.
    #[serde(default)]
    pub address_translations: Vec<AddressTranslation>,
}

fn default_source_kind() -> String {
    "cql".to_string()
}

/// One `from` → `to` socket mapping for the Scylla address translator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddressTranslation {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetConfig {
    pub contact_points: Vec<String>,
    pub datacenter: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub consistency: ConsistencyLevel,
    /// If true, create missing target keyspace/table from the source schema
    /// (minimal DDL). Never runs DDL on the source. Existing tables are kept.
    #[serde(default)]
    pub create_schema: bool,
    /// RF used when `create_schema` creates a keyspace (default 1).
    #[serde(default = "default_create_schema_rf")]
    pub create_schema_rf: u32,
    /// If true, only open connections to the configured contact points
    /// (ignore cluster peer discovery). Useful behind SSH tunnels.
    #[serde(default)]
    pub contact_points_only: bool,
    /// Map cluster-broadcast addresses to locally reachable ones (SSH tunnels).
    #[serde(default)]
    pub address_translations: Vec<AddressTranslation>,
}

fn default_create_schema_rf() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMapping {
    /// Fully-qualified source table: `keyspace.table`.
    pub source: String,
    /// Fully-qualified target table: `keyspace.table`.
    pub target: String,
}

impl TableMapping {
    pub fn parse_source(&self) -> Result<(String, String), CoreError> {
        split_qualified(&self.source, "tables.source")
    }

    pub fn parse_target(&self) -> Result<(String, String), CoreError> {
        split_qualified(&self.target, "tables.target")
    }
}

fn split_qualified(value: &str, field: &str) -> Result<(String, String), CoreError> {
    let mut parts = value.splitn(2, '.');
    let keyspace = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CoreError::InvalidConfig(format!("{field} must be keyspace.table")))?;
    let table = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            CoreError::InvalidConfig(format!(
                "{field} `{value}` must be keyspace.table (missing table name)"
            ))
        })?;
    Ok((keyspace.to_string(), table.to_string()))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineConfig {
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    /// Rows per CQL `UNLOGGED` batch toward the target.
    ///
    /// `1` keeps classic single-row inserts. Values `> 1` group prepared INSERTs
    /// into concurrent unlogged batches (idempotent PK rewrites on retry).
    #[serde(default = "default_write_batch_size")]
    pub write_batch_size: usize,
    #[serde(default)]
    pub concurrency: ConcurrencyConfig,
    #[serde(default)]
    pub adaptive: AdaptiveConfig,
    /// When true, bulk INSERT uses USING TIMESTAMP from max source cell writetime
    /// across preservable (non-collection) columns.
    #[serde(default)]
    pub preserve_writetimes: bool,
    /// When true, bulk INSERT uses USING TTL from max remaining source cell TTL
    /// across preservable columns (`0` when none have a TTL).
    #[serde(default)]
    pub preserve_ttls: bool,
    /// If set, only the first N planned token ranges are copied. Use for smoke
    /// tests when the full table would not fit on the target disk.
    #[serde(default)]
    pub max_units: Option<usize>,
    /// When true, migrate counter tables via `UPDATE … SET c = c + ?`.
    /// Default false: refuse counter schemas (fail closed).
    #[serde(default)]
    pub allow_counters: bool,
    /// Include frozen collections in Level-A max writetime/ttl aggregation.
    #[serde(default)]
    pub preserve_frozen_collections: bool,
    /// Preserve per-element WRITETIME/TTL for unfrozen map/set columns via
    /// follow-up SELECT + element UPDATE. Lists stay Level-A (uniform) only.
    /// Requires a cluster that accepts `WRITETIME(col[key])` / `TTL(col[elem])`.
    #[serde(default)]
    pub preserve_collection_elements: bool,
}

/// HTTP gateway that dual-writes app mutations to source then target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DualWriteConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_dual_write_listen")]
    pub listen_addr: String,
    /// Optional shared secret; when non-empty, require `Authorization: Bearer <token>`.
    #[serde(default)]
    pub auth_token: String,
    /// When true (default), fail the request if either cluster write fails.
    #[serde(default = "default_true")]
    pub require_both: bool,
}

fn default_dual_write_listen() -> String {
    "127.0.0.1:8091".into()
}

impl Default for DualWriteConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: default_dual_write_listen(),
            auth_token: String::new(),
            require_both: true,
        }
    }
}

fn default_workers() -> usize {
    32
}

fn default_page_size() -> u32 {
    5000
}

fn default_write_batch_size() -> usize {
    // Keep small: Scylla rejects oversized UNLOGGED batches (~50KB default).
    // Large rows (benchmark.key_value ~1KB) need ~10; writer also auto-splits.
    10
}

/// Practical upper bound: huge CQL batches hurt latency and coordinator memory.
pub const MAX_WRITE_BATCH_SIZE: usize = 256;

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            page_size: default_page_size(),
            write_batch_size: default_write_batch_size(),
            concurrency: ConcurrencyConfig::default(),
            adaptive: AdaptiveConfig::default(),
            preserve_writetimes: false,
            preserve_ttls: false,
            max_units: None,
            allow_counters: false,
            preserve_frozen_collections: false,
            preserve_collection_elements: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    #[serde(default = "default_concurrency_initial")]
    pub initial: usize,
    #[serde(default = "default_concurrency_minimum")]
    pub minimum: usize,
    #[serde(default = "default_concurrency_maximum")]
    pub maximum: usize,
}

fn default_concurrency_initial() -> usize {
    64
}

fn default_concurrency_minimum() -> usize {
    8
}

fn default_concurrency_maximum() -> usize {
    512
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            initial: default_concurrency_initial(),
            minimum: default_concurrency_minimum(),
            maximum: default_concurrency_maximum(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_target_p99_ms")]
    pub target_p99_ms: u64,
}

fn default_true() -> bool {
    true
}

fn default_target_p99_ms() -> u64 {
    15
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_p99_ms: default_target_p99_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointConfig {
    #[serde(default = "default_checkpoint_path")]
    pub path: String,
    #[serde(default = "default_checkpoint_interval_secs")]
    pub interval_secs: u64,
}

fn default_checkpoint_path() -> String {
    "./.odyssey/migration.db".to_string()
}

fn default_checkpoint_interval_secs() -> u64 {
    5
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            path: default_checkpoint_path(),
            interval_secs: default_checkpoint_interval_secs(),
        }
    }
}

impl CheckpointConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    Sample,
    Digest,
    Full,
}

impl Default for ValidationMode {
    fn default() -> Self {
        Self::Digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationConfig {
    #[serde(default)]
    pub mode: ValidationMode,
    /// Max rows compared per range in sample mode (token order, first N).
    #[serde(default = "default_sample_size")]
    pub sample_size: u64,
    /// When true, sample validation also compares max cell WRITETIME / TTL.
    #[serde(default)]
    pub compare_timestamps: bool,
    /// Allowed |source − target| writetime difference in microseconds.
    #[serde(default = "default_writetime_tolerance_us")]
    pub writetime_tolerance_us: u64,
    /// Allowed |source − target| remaining TTL difference in seconds.
    #[serde(default = "default_ttl_tolerance_secs")]
    pub ttl_tolerance_secs: u64,
}

fn default_sample_size() -> u64 {
    1000
}

fn default_writetime_tolerance_us() -> u64 {
    1_000_000
}

fn default_ttl_tolerance_secs() -> u64 {
    60
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            mode: ValidationMode::Digest,
            sample_size: default_sample_size(),
            compare_timestamps: false,
            writetime_tolerance_us: default_writetime_tolerance_us(),
            ttl_tolerance_secs: default_ttl_tolerance_secs(),
        }
    }
}

/// Logging sink configuration. CLI flags (`-v`, `--log-format`) override these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub verbosity: Verbosity,
    #[serde(default)]
    pub format: LogFormat,
    /// When set, also write logs to this path (in addition to stderr).
    pub file: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            verbosity: Verbosity::Info,
            format: LogFormat::Text,
            file: None,
        }
    }
}

/// In-process metrics collection and HTTP `/metrics` listen address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Bind address for the Prometheus/OpenMetrics HTTP endpoint.
    #[serde(default = "default_metrics_listen_addr")]
    pub listen_addr: String,
}

fn default_metrics_listen_addr() -> String {
    "127.0.0.1:9100".to_string()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: default_metrics_listen_addr(),
        }
    }
}

/// In-process migration dashboard (HTML + JSON), independent of Prometheus `/metrics`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Bind address for the dashboard. Default avoids Spark Master `:8080`.
    #[serde(default = "default_ui_listen_addr")]
    pub listen_addr: String,
    /// Keep the dashboard listening this many seconds after migrate/resume finishes
    /// (0 = exit immediately). Overridden by CLI `--ui-hold` (wait until Ctrl+C).
    /// Useful for fast local demos where the job finishes before you open a browser.
    #[serde(default)]
    pub hold_secs: u64,
}

fn default_ui_listen_addr() -> String {
    "127.0.0.1:9080".to_string()
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: default_ui_listen_addr(),
            hold_secs: 0,
        }
    }
}

/// Live / CDC catch-up settings (applied at runtime when `enabled`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveConfig {
    #[serde(default)]
    pub enabled: bool,
    /// CDC window size in seconds.
    #[serde(default = "default_cdc_window_secs")]
    pub window_secs: u64,
    #[serde(default = "default_cdc_safety_secs")]
    pub safety_secs: u64,
    #[serde(default = "default_cdc_sleep_secs")]
    pub sleep_secs: u64,
    /// Stop catch-up when lag is below this (ms). 0 = run until --until / end_timestamp.
    #[serde(default = "default_target_lag_ms")]
    pub target_lag_ms: u64,
    /// Optional ISO8601 / RFC3339 start watermark; if None, start near "now - overlap".
    pub start_at: Option<String>,
    /// How many seconds before bulk start to begin CDC (overlap).
    /// Used when `start_at` is None and bulk provides a start Instant.
    #[serde(default = "default_overlap_secs")]
    pub overlap_secs: u64,
}

fn default_cdc_window_secs() -> u64 {
    60
}

fn default_cdc_safety_secs() -> u64 {
    30
}

fn default_cdc_sleep_secs() -> u64 {
    10
}

fn default_target_lag_ms() -> u64 {
    500
}

fn default_overlap_secs() -> u64 {
    60
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            window_secs: default_cdc_window_secs(),
            safety_secs: default_cdc_safety_secs(),
            sleep_secs: default_cdc_sleep_secs(),
            target_lag_ms: default_target_lag_ms(),
            start_at: None,
            overlap_secs: default_overlap_secs(),
        }
    }
}

impl Config {
    /// Load and validate a config file from disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CoreError> {
        let path_ref = path.as_ref();
        let path_display = path_ref.display().to_string();
        let raw = fs::read_to_string(path_ref).map_err(|source| CoreError::ReadConfig {
            path: path_display.clone(),
            source,
        })?;
        Self::parse_str(&raw, &path_display)
    }

    /// Parse TOML from a string. `origin` is used in error messages.
    pub fn parse_str(raw: &str, origin: &str) -> Result<Self, CoreError> {
        let config: Self = toml::from_str(raw).map_err(|source| CoreError::ParseConfig {
            path: origin.to_string(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), CoreError> {
        if self.migration.name.trim().is_empty() {
            return Err(CoreError::InvalidConfig(
                "migration.name must not be empty".into(),
            ));
        }
        if self.source.kind != "cql" {
            return Err(CoreError::InvalidConfig(format!(
                "source.kind `{}` is not supported yet (only `cql`)",
                self.source.kind
            )));
        }
        if self.source.contact_points.is_empty() {
            return Err(CoreError::InvalidConfig(
                "source.contact_points must contain at least one host".into(),
            ));
        }
        if self.target.contact_points.is_empty() {
            return Err(CoreError::InvalidConfig(
                "target.contact_points must contain at least one host".into(),
            ));
        }
        if self.target.create_schema && self.target.create_schema_rf == 0 {
            return Err(CoreError::InvalidConfig(
                "target.create_schema_rf must be >= 1 when create_schema is true".into(),
            ));
        }
        if self.tables.is_empty() {
            return Err(CoreError::InvalidConfig(
                "tables must list at least one mapping".into(),
            ));
        }
        for mapping in &self.tables {
            mapping.parse_source()?;
            mapping.parse_target()?;
        }
        if self.engine.workers == 0 {
            return Err(CoreError::InvalidConfig(
                "engine.workers must be >= 1".into(),
            ));
        }
        if self.engine.page_size == 0 {
            return Err(CoreError::InvalidConfig(
                "engine.page_size must be >= 1".into(),
            ));
        }
        if self.engine.write_batch_size == 0 {
            return Err(CoreError::InvalidConfig(
                "engine.write_batch_size must be >= 1".into(),
            ));
        }
        if self.engine.write_batch_size > MAX_WRITE_BATCH_SIZE {
            return Err(CoreError::InvalidConfig(format!(
                "engine.write_batch_size must be <= {MAX_WRITE_BATCH_SIZE}"
            )));
        }
        let c = &self.engine.concurrency;
        if c.minimum == 0 || c.initial == 0 || c.maximum == 0 {
            return Err(CoreError::InvalidConfig(
                "engine.concurrency values must be >= 1".into(),
            ));
        }
        if c.minimum > c.initial || c.initial > c.maximum {
            return Err(CoreError::InvalidConfig(
                "engine.concurrency requires minimum <= initial <= maximum".into(),
            ));
        }
        if self.checkpoint.interval_secs == 0 {
            return Err(CoreError::InvalidConfig(
                "checkpoint.interval_secs must be >= 1".into(),
            ));
        }
        if self.validation.sample_size == 0 {
            return Err(CoreError::InvalidConfig(
                "validation.sample_size must be >= 1".into(),
            ));
        }
        if self.live.window_secs == 0 {
            return Err(CoreError::InvalidConfig(
                "live.window_secs must be >= 1".into(),
            ));
        }
        if self.live.safety_secs == 0 {
            return Err(CoreError::InvalidConfig(
                "live.safety_secs must be >= 1".into(),
            ));
        }
        if self.live.sleep_secs == 0 {
            return Err(CoreError::InvalidConfig(
                "live.sleep_secs must be >= 1".into(),
            ));
        }
        self.integrity
            .validate()
            .map_err(CoreError::InvalidConfig)?;
        Ok(())
    }

    /// How many migration units the planner should aim for given worker count.
    pub fn desired_work_units(&self) -> usize {
        self.engine.workers.saturating_mul(16).max(self.engine.workers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[migration]
name = "demo"

[source]
contact_points = ["10.0.0.1"]
datacenter = "dc1"

[target]
contact_points = ["10.0.0.2"]
datacenter = "dc1"

[[tables]]
source = "ecommerce.events"
target = "ecommerce.events"
"#;

    #[test]
    fn parses_sample_with_defaults() {
        let config = Config::parse_str(SAMPLE, "test.toml").unwrap();
        assert_eq!(config.migration.name, "demo");
        assert_eq!(config.source.kind, "cql");
        assert_eq!(config.engine.workers, 32);
        assert_eq!(config.engine.page_size, 5000);
        assert_eq!(config.engine.write_batch_size, 10);
        assert_eq!(config.engine.concurrency.initial, 64);
        assert!(config.engine.adaptive.enabled);
        assert_eq!(config.engine.adaptive.target_p99_ms, 15);
        assert_eq!(config.checkpoint.path, "./.odyssey/migration.db");
        assert_eq!(config.validation.mode, ValidationMode::Digest);
        assert_eq!(config.validation.sample_size, 1000);
        assert_eq!(config.logging.verbosity, Verbosity::Info);
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.listen_addr, "127.0.0.1:9100");
        assert!(config.ui.enabled);
        assert_eq!(config.ui.listen_addr, "127.0.0.1:9080");
        assert_eq!(config.ui.hold_secs, 0);
        assert_eq!(config.integrity.guarantee, crate::DurabilityGuarantee::AtLeastOnce);
        assert!(!config.live.enabled);
        assert!(!config.notify.enabled());
        assert!(config.notify.on_complete);
        assert!(config.notify.on_error);
        assert_eq!(config.live.window_secs, 60);
        assert_eq!(config.live.safety_secs, 30);
        assert_eq!(config.live.sleep_secs, 10);
        assert_eq!(config.live.target_lag_ms, 500);
        assert!(config.live.start_at.is_none());
        assert_eq!(config.live.overlap_secs, 60);
        assert_eq!(config.source.consistency, ConsistencyLevel::LocalQuorum);
        assert_eq!(config.target.consistency, ConsistencyLevel::LocalQuorum);
        assert!(!config.engine.preserve_writetimes);
        assert!(!config.engine.preserve_ttls);
        assert!(!config.validation.compare_timestamps);
        assert_eq!(config.validation.writetime_tolerance_us, 1_000_000);
        assert_eq!(config.validation.ttl_tolerance_secs, 60);
        assert_eq!(config.desired_work_units(), 512);
    }

    #[test]
    fn rejects_bad_table_names() {
        let raw = r#"
[migration]
name = "demo"
[source]
contact_points = ["a"]
[target]
contact_points = ["b"]
[[tables]]
source = "only_keyspace"
target = "ks.table"
"#;
        let err = Config::parse_str(raw, "bad.toml").unwrap_err();
        assert!(err.to_string().contains("keyspace.table"));
    }

    #[test]
    fn rejects_inverted_concurrency() {
        let raw = r#"
[migration]
name = "demo"
[source]
contact_points = ["a"]
[target]
contact_points = ["b"]
[[tables]]
source = "ks.t"
target = "ks.t"
[engine.concurrency]
minimum = 64
initial = 8
maximum = 512
"#;
        let err = Config::parse_str(raw, "bad.toml").unwrap_err();
        assert!(err.to_string().contains("minimum <= initial <= maximum"));
    }

    #[test]
    fn split_qualified_roundtrip() {
        let mapping = TableMapping {
            source: "ks.events".into(),
            target: "other.events".into(),
        };
        assert_eq!(
            mapping.parse_source().unwrap(),
            ("ks".into(), "events".into())
        );
        assert_eq!(
            mapping.parse_target().unwrap(),
            ("other".into(), "events".into())
        );
    }
}
