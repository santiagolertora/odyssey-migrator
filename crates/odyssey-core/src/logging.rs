use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::CoreError;

/// How chatty Odyssey should be on stderr / the log sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Verbosity {
    /// Only migration-fatal events.
    Error,
    /// Recovered anomalies that still need attention (retries exhausted soon, etc.).
    Warn,
    /// State changes operators care about: connect, plan, unit start/finish, resume.
    #[default]
    Info,
    /// Per-page / per-batch progress useful while diagnosing throughput.
    Debug,
    /// Driver-level and internal traces. Extremely noisy.
    Trace,
}

impl Verbosity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }

    /// Env-filter directive covering Odyssey crates at this level.
    pub fn env_filter_directive(self) -> String {
        let level = self.as_str();
        format!(
            "odyssey_cli={level},odyssey_core={level},odyssey_cql={level},odyssey_planner={level},odyssey_engine={level},odyssey_checkpoint={level},odyssey_validation={level},odyssey_metrics={level},odyssey_types={level},odyssey_cdc={level}"
        )
    }
}

impl FromStr for Verbosity {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "warn" | "warning" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            other => Err(CoreError::InvalidConfig(format!(
                "unknown verbosity `{other}` (expected error|warn|info|debug|trace)"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-oriented single-line logs for terminals.
    #[default]
    Text,
    /// Machine-oriented JSON lines for shipping to log aggregators.
    Json,
}

impl FromStr for LogFormat {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(CoreError::InvalidConfig(format!(
                "unknown log format `{other}` (expected text|json)"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_verbosity_aliases() {
        assert_eq!("warn".parse::<Verbosity>().unwrap(), Verbosity::Warn);
        assert_eq!("warning".parse::<Verbosity>().unwrap(), Verbosity::Warn);
        assert_eq!("DEBUG".parse::<Verbosity>().unwrap(), Verbosity::Debug);
    }

    #[test]
    fn ordering_matches_noise_level() {
        assert!(Verbosity::Error < Verbosity::Warn);
        assert!(Verbosity::Info < Verbosity::Debug);
        assert!(Verbosity::Debug < Verbosity::Trace);
    }
}
