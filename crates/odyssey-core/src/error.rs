use thiserror::Error;

/// Errors raised while loading or validating Odyssey configuration.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("failed to read config from {path}: {source}")]
    ReadConfig {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid TOML in {path}: {source}")]
    ParseConfig {
        path: String,
        #[source]
        source: toml::de::Error,
    },

    #[error("{0}")]
    InvalidConfig(String),
}
