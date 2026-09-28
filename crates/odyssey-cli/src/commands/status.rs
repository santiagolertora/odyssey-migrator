use anyhow::{Context, Result, bail};
use tracing::info;

use odyssey_checkpoint::CheckpointStore;
use odyssey_core::Config;

use crate::cli::StatusArgs;
use crate::commands::migrate::resolve_checkpoint_path;

pub async fn run(args: StatusArgs) -> Result<()> {
    let checkpoint_path =
        resolve_checkpoint_path(args.config.as_deref(), args.checkpoint.as_deref())?;

    let store = CheckpointStore::open(&checkpoint_path)
        .with_context(|| format!("open checkpoint {}", checkpoint_path.display()))?;

    let Some(migration) = store
        .get_migration(&args.migration_id)
        .context("load migration")?
    else {
        bail!(
            "migration `{}` not found in {}",
            args.migration_id,
            checkpoint_path.display()
        );
    };

    let summary = store
        .migration_summary(&args.migration_id)
        .context("summarize migration")?;

    info!(
        migration_id = %args.migration_id,
        status = %migration.status,
        pending = summary.pending,
        running = summary.running,
        completed = summary.completed,
        failed = summary.failed,
        "migration status"
    );

    let total = summary.total_units().max(1) as f64;
    let pct = (summary.completed as f64 / total) * 100.0;

    println!("Odyssey Migrator status");
    println!();
    println!("Migration");
    println!("  id            {}", migration.id);
    println!("  name          {}", migration.name);
    println!("  table         {}.{}", migration.keyspace_name, migration.table_name);
    println!("  status        {}", migration.status);
    println!("  created       {}", migration.created_at);
    if let Some(done) = &migration.completed_at {
        println!("  completed     {done}");
    }
    println!();
    println!("Progress");
    println!("  {pct:.1}%");
    println!(
        "  units         {} pending / {} running / {} completed / {} failed",
        summary.pending, summary.running, summary.completed, summary.failed
    );
    println!("  rows read     {}", summary.total_rows_read);
    println!("  rows written  {}", summary.total_rows_written);
    println!("  bytes written {}", summary.total_bytes_written);
    println!("  checkpoint    {}", checkpoint_path.display());

    if summary.failed > 0 {
        bail!(
            "migration `{}` has {} failed unit(s); inspect logs and run resume after fixing the cause",
            args.migration_id,
            summary.failed
        );
    }

    Ok(())
}

/// Shared helper so resume can load config + optional checkpoint override.
pub fn load_config(path: &std::path::Path) -> Result<Config> {
    Config::load(path).with_context(|| format!("load config {}", path.display()))
}
