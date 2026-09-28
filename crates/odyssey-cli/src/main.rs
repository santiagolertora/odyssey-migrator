//! Odyssey Migrator CLI.
//!
//! Logging is initialized once at process start. Library crates emit structured
//! `tracing` events; this binary owns the subscriber and process exit codes.

mod cli;
mod commands;
mod logging;
mod notify;
mod ui;

use anyhow::Context;
use clap::Parser;
use tracing::error;

use cli::{Cli, Command};
use logging::init_logging;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        // Last-resort stderr if tracing never came up, or to reinforce failure.
        error!(error = %err, "odyssey-migrator exiting with failure");
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let _log_guard = init_logging(&cli).context("initialize logging")?;

    match cli.command {
        Command::Migrate(args) => commands::migrate::run(args).await?,
        Command::Status(args) => commands::status::run(args).await?,
        Command::Resume(args) => commands::resume::run(args).await?,
        Command::Ui(args) => commands::ui::run(args).await?,
        Command::Validate(args) => commands::validate::run(args).await?,
        Command::Live(args) => commands::live::run(args).await?,
        Command::Cutover(args) => commands::cutover::run(args).await?,
        Command::DualWrite(args) => commands::dual_write::run(args).await?,
        Command::NotifyTest(args) => commands::notify_test::run(args).await?,
    }

    Ok(())
}
