use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};

use odyssey_core::{LogFormat, Verbosity};

const AFTER_HELP: &str = "\
Typical workflow:
  odyssey-migrator migrate -c ferry.toml --plan-only
  odyssey-migrator migrate -c ferry.toml
  odyssey-migrator status <migration-id> -c ferry.toml
  odyssey-migrator validate <migration-id> -c ferry.toml

Live (Scylla CDC) / cutover:
  odyssey-migrator migrate -c ferry.toml --with-live
  odyssey-migrator cutover -c ferry.toml
  odyssey-migrator dual-write -c ferry.toml   # Cassandra→Scylla writers

Docs: docs/README.md  ·  Config: examples/ferry.toml
Copyright (c) 2026 Santiago Lertora. All rights reserved.
";

#[derive(Debug, Parser)]
#[command(
    name = "odyssey-migrator",
    version,
    author = "Santiago Lertora",
    about = "Resumable CQL → ScyllaDB migrator (no Spark) — by Santiago Lertora",
    long_about = "Odyssey Migrator — by Santiago Lertora\n\
Copies Cassandra/Scylla tables into ScyllaDB without Spark.\n\n\
Durability: at-least-once. A primary key may be rewritten; a successfully read row is never skipped.\n\
Bulk path: token-range SELECT → prepared INSERT → SQLite checkpoint.\n\
Live path: Scylla CDC catch-up (UPDATE/DELETE). Cassandra sources use dual-write during cutover.\n\n\
Start with a TOML config (see examples/ferry.toml). Full docs: docs/README.md",
    after_help = AFTER_HELP,
    arg_required_else_help = true,
    propagate_version = true,
    disable_help_subcommand = false,
)]
pub struct Cli {
    /// Increase log verbosity (-v = debug, -vv = trace). Overrides [logging].verbosity.
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Quieter logs (warn+). Conflicts with -v.
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    pub quiet: bool,

    /// Log format: text or json (default from config / text).
    #[arg(long, global = true, value_name = "FORMAT", value_parser = parse_log_format)]
    pub log_format: Option<LogFormat>,

    /// Also append logs to this file path.
    #[arg(long, global = true, value_name = "PATH")]
    pub log_file: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

fn parse_log_format(raw: &str) -> Result<LogFormat, String> {
    LogFormat::from_str(raw).map_err(|err| err.to_string())
}

impl Cli {
    pub fn verbosity_override(&self) -> Option<Verbosity> {
        if self.quiet {
            return Some(Verbosity::Warn);
        }
        match self.verbose {
            0 => None,
            1 => Some(Verbosity::Debug),
            _ => Some(Verbosity::Trace),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Plan and run a bulk token-range copy (CQL → CQL).
    #[command(after_help = "\
Examples:
  odyssey-migrator migrate -c ferry.toml --plan-only
  odyssey-migrator migrate -c ferry.toml
  odyssey-migrator migrate -c ferry.toml --with-live --ui-hold
")]
    Migrate(MigrateArgs),

    /// Show checkpoint progress for a migration id.
    #[command(after_help = "\
Examples:
  odyssey-migrator status <migration-id> -c ferry.toml
  odyssey-migrator status <migration-id> --checkpoint ./.odyssey/migration.db
")]
    Status(StatusArgs),

    /// Resume an interrupted bulk migration from its SQLite checkpoint.
    #[command(after_help = "\
Examples:
  odyssey-migrator resume <migration-id> -c ferry.toml
  odyssey-migrator resume <migration-id> -c ferry.toml --ui-hold
")]
    Resume(ResumeArgs),

    /// Serve the HTML migration dashboard (Ctrl+C to exit).
    #[command(after_help = "\
Examples:
  odyssey-migrator ui -c ferry.toml
  odyssey-migrator ui -c ferry.toml <migration-id>
")]
    Ui(UiArgs),

    /// Compare source and target for the migration's token ranges.
    #[command(after_help = "\
Mode comes from [validation] in the config (digest / sample / full).

Examples:
  odyssey-migrator validate <migration-id> -c ferry.toml
")]
    Validate(ValidateArgs),

    /// Apply Scylla CDC mutations that landed during/after bulk copy.
    #[command(after_help = "\
Requires CDC enabled on source tables and [live] configured.

Examples:
  odyssey-migrator live -c ferry.toml
  odyssey-migrator live -c ferry.toml --until 2026-09-12T20:00:00Z
")]
    Live(LiveArgs),

    /// Ops cutover checklist: catch-up → quiesce confirm → final catch-up.
    #[command(after_help = "\
Does not flip application traffic. Use dual-write for app mutations during cutover.

Examples:
  odyssey-migrator cutover -c ferry.toml
  odyssey-migrator cutover -c ferry.toml --yes
")]
    Cutover(CutoverArgs),

    /// HTTP dual-write gateway (mutations → source then target).
    #[command(
        name = "dual-write",
        after_help = "\
Endpoints:
  GET  /health
  POST /v1/mutate   JSON: { table, op, columns, timestamp_us?, ttl_secs? }

op: insert | update | delete_row | delete_partition
table must match a [[tables]].source FQN (e.g. ks.events).

Examples:
  odyssey-migrator dual-write -c ferry.toml
  curl -sS -X POST http://127.0.0.1:8091/v1/mutate \\
    -H 'content-type: application/json' \\
    -d '{\"table\":\"ks.events\",\"op\":\"insert\",\"columns\":{\"pk\":\"a\"}}'
")]
    DualWrite(DualWriteArgs),

    /// POST a sample [notify] webhook (Slack-compatible JSON).
    #[command(name = "notify-test", after_help = "\
Examples:
  odyssey-migrator notify-test -c ferry.toml
  odyssey-migrator notify-test -c ferry.toml --error
")]
    NotifyTest(NotifyTestArgs),
}

#[derive(Debug, Parser)]
pub struct DualWriteArgs {
    /// Path to TOML config (source, target, [[tables]], [dual_write]).
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// Override [dual_write].listen_addr (default 127.0.0.1:8091).
    #[arg(long, value_name = "ADDR")]
    pub listen_addr: Option<String>,

    /// Start even when [dual_write] enabled = false.
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Debug, Parser)]
pub struct CutoverArgs {
    /// Path to TOML config ([live] should be enabled unless --force).
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// Skip interactive yes/no prompts.
    #[arg(long, default_value_t = false)]
    pub yes: bool,

    /// Allow cutover when [live] enabled = false.
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Debug, Parser)]
pub struct NotifyTestArgs {
    /// Path to TOML config with [notify].webhook_url set.
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// Send a migration.failed sample instead of completed.
    #[arg(long, default_value_t = false)]
    pub error: bool,
}

#[derive(Debug, Parser)]
pub struct MigrateArgs {
    /// Path to TOML migration config.
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// Discover schema and plan token ranges only; do not copy rows.
    #[arg(long, default_value_t = false)]
    pub plan_only: bool,

    /// After bulk completes, run CDC catch-up (needs CDC + [live]).
    #[arg(long, default_value_t = false)]
    pub with_live: bool,

    /// Override [ui].listen_addr (e.g. 127.0.0.1:19080 when 9080 is busy).
    #[arg(long, value_name = "ADDR")]
    pub ui_addr: Option<String>,

    /// Keep the dashboard open until Ctrl+C after the migration finishes.
    #[arg(long, default_value_t = false)]
    pub ui_hold: bool,
}

#[derive(Debug, Parser)]
pub struct StatusArgs {
    /// Migration UUID printed by migrate / stored in the checkpoint.
    pub migration_id: String,

    /// TOML config (resolves checkpoint path and logging).
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// SQLite checkpoint path override.
    #[arg(long, value_name = "PATH")]
    pub checkpoint: Option<PathBuf>,
}

#[derive(Debug, Parser)]
pub struct ResumeArgs {
    /// Migration UUID to resume.
    pub migration_id: String,

    /// Path to TOML migration config.
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// SQLite checkpoint path override.
    #[arg(long, value_name = "PATH")]
    pub checkpoint: Option<PathBuf>,

    /// Override [ui].listen_addr.
    #[arg(long, value_name = "ADDR")]
    pub ui_addr: Option<String>,

    /// Keep the dashboard open until Ctrl+C after resume finishes.
    #[arg(long, default_value_t = false)]
    pub ui_hold: bool,
}

#[derive(Debug, Parser)]
pub struct UiArgs {
    /// Path to TOML config (checkpoint + UI bind).
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// Migration id to show. If omitted, uses the most recent in the checkpoint.
    pub migration_id: Option<String>,

    /// SQLite checkpoint path override.
    #[arg(long, value_name = "PATH")]
    pub checkpoint: Option<PathBuf>,

    /// Override [ui].listen_addr.
    #[arg(long, value_name = "ADDR")]
    pub ui_addr: Option<String>,
}

#[derive(Debug, Parser)]
pub struct ValidateArgs {
    /// Migration UUID whose planned ranges will be compared.
    pub migration_id: String,

    /// Path to TOML config (source, target, [validation]).
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// SQLite checkpoint path override.
    #[arg(long, value_name = "PATH")]
    pub checkpoint: Option<PathBuf>,
}

#[derive(Debug, Parser)]
pub struct LiveArgs {
    /// TOML config (source tables must have Scylla CDC enabled).
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// End watermark (RFC3339). Default: now + --until-grace-secs.
    #[arg(long, value_name = "RFC3339")]
    pub until: Option<String>,

    /// Seconds ahead of now used as end when --until is omitted (in-flight CDC).
    #[arg(long, default_value_t = 30)]
    pub until_grace_secs: u64,
}

impl LiveArgs {
    pub fn end_duration(&self) -> anyhow::Result<Duration> {
        if let Some(raw) = &self.until {
            let dt = chrono::DateTime::parse_from_rfc3339(raw)
                .map_err(|err| anyhow::anyhow!("--until must be RFC3339: {err}"))?;
            let secs = dt.timestamp().max(0) as u64;
            return Ok(Duration::new(secs, dt.timestamp_subsec_nanos()));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Ok(now + Duration::from_secs(self.until_grace_secs))
    }
}
