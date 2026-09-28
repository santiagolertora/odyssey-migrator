use std::fs::OpenOptions;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use odyssey_core::{Config, LogFormat, LoggingConfig, Verbosity};

use crate::cli::Cli;

/// Holds file-appender worker guards so logs flush on drop.
pub struct LoggingGuard {
    _file_guard: Option<WorkerGuard>,
}

/// Initialize tracing from CLI overrides and optional config file logging section.
///
/// Priority for verbosity: `-v`/`-q` > `RUST_LOG` > config `[logging]` > info.
pub fn init_logging(cli: &Cli) -> Result<LoggingGuard> {
    let from_config = load_logging_hint(cli);
    let verbosity = cli
        .verbosity_override()
        .or_else(|| {
            std::env::var("RUST_LOG")
                .ok()
                .filter(|v| !v.is_empty())
                .map(|_| Verbosity::Info) // EnvFilter will honor RUST_LOG fully below
                .or(from_config.as_ref().map(|c| c.verbosity))
        })
        .unwrap_or(Verbosity::Info);

    let format = cli
        .log_format
        .or(from_config.as_ref().map(|c| c.format))
        .unwrap_or(LogFormat::Text);

    let log_file = cli
        .log_file
        .clone()
        .or_else(|| from_config.as_ref().and_then(|c| c.file.as_ref().map(PathBuf::from)));

    let filter = match std::env::var("RUST_LOG") {
        Ok(directives) if !directives.is_empty() && cli.verbosity_override().is_none() => {
            EnvFilter::try_new(directives).context("parse RUST_LOG")?
        }
        _ => EnvFilter::try_new(verbosity.env_filter_directive())
            .context("build default env filter")?,
    };

    let mut file_guard = None;

    match format {
        LogFormat::Text => {
            let stderr_layer = tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_writer(std::io::stderr);

            if let Some(path) = log_file {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .with_context(|| format!("open log file {}", path.display()))?;
                let (non_blocking, guard) = tracing_appender::non_blocking(file);
                file_guard = Some(guard);
                let file_layer = tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_target(true)
                    .with_writer(non_blocking);
                tracing_subscriber::registry()
                    .with(filter)
                    .with(stderr_layer)
                    .with(file_layer)
                    .try_init()
                    .context("install tracing subscriber")?;
            } else {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(stderr_layer)
                    .try_init()
                    .context("install tracing subscriber")?;
            }
        }
        LogFormat::Json => {
            let stderr_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_writer(std::io::stderr);

            if let Some(path) = log_file {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .with_context(|| format!("open log file {}", path.display()))?;
                let (non_blocking, guard) = tracing_appender::non_blocking(file);
                file_guard = Some(guard);
                let file_layer = tracing_subscriber::fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_writer(non_blocking);
                tracing_subscriber::registry()
                    .with(filter)
                    .with(stderr_layer)
                    .with(file_layer)
                    .try_init()
                    .context("install tracing subscriber")?;
            } else {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(stderr_layer)
                    .try_init()
                    .context("install tracing subscriber")?;
            }
        }
    }

    tracing::debug!(
        verbosity = verbosity.as_str(),
        ?format,
        "logging initialized"
    );

    Ok(LoggingGuard {
        _file_guard: file_guard,
    })
}

fn load_logging_hint(cli: &Cli) -> Option<LoggingConfig> {
    let path = match &cli.command {
        crate::cli::Command::Migrate(args) => Some(args.config.as_path()),
        crate::cli::Command::Resume(args) => Some(args.config.as_path()),
        crate::cli::Command::Validate(args) => Some(args.config.as_path()),
        crate::cli::Command::Live(args) => Some(args.config.as_path()),
        crate::cli::Command::Cutover(args) => Some(args.config.as_path()),
        crate::cli::Command::DualWrite(args) => Some(args.config.as_path()),
        crate::cli::Command::NotifyTest(args) => Some(args.config.as_path()),
        crate::cli::Command::Ui(args) => Some(args.config.as_path()),
        crate::cli::Command::Status(args) => args.config.as_deref(),
    }?;

    match Config::load(path) {
        Ok(cfg) => Some(cfg.logging),
        Err(err) => {
            // Config may be invalid; migrate will fail properly later. Do not
            // abort logging setup here.
            eprintln!(
                "warning: could not preload logging from {}: {err}",
                path.display()
            );
            None
        }
    }
}

/// Helper kept for commands that need an explicit fail-fast on integrity drift.
#[allow(dead_code)]
pub fn refuse_data_loss(reason: impl Into<String>) -> Result<()> {
    let reason = reason.into();
    tracing::error!(%reason, "refusing action that could lose migration data");
    bail!("{reason}");
}
