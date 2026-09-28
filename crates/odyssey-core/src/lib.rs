//! Core configuration and helpers shared by the CLI and engine crates.

mod config;
mod error;
mod integrity;
mod logging;

pub use config::{
    AdaptiveConfig, AddressTranslation, CheckpointConfig, ConcurrencyConfig, Config,
    ConsistencyLevel, DualWriteConfig, EngineConfig, LiveConfig, LoggingConfig, MeshConfig,
    MetricsConfig, MigrationMeta, NotifyConfig, PlannerConfig, PlannerMode, SourceConfig,
    TableMapping, TargetConfig, UiConfig, ValidationConfig, ValidationMode, MAX_WRITE_BATCH_SIZE,
};
pub use error::CoreError;
pub use integrity::{DurabilityGuarantee, IntegrityPolicy};
pub use logging::{LogFormat, Verbosity};
