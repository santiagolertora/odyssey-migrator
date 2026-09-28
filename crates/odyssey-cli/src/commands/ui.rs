use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tracing::info;

use odyssey_checkpoint::CheckpointStore;

use crate::cli::UiArgs;
use crate::commands::migrate::{maybe_spawn_dashboard, resolve_checkpoint_path};
use crate::commands::status::load_config;
use crate::ui::hold_dashboard_if_needed;

/// Serve the dashboard against an existing checkpoint until Ctrl+C.
pub async fn run(args: UiArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    let checkpoint_path =
        resolve_checkpoint_path(Some(args.config.as_path()), args.checkpoint.as_deref())?;
    let store = Arc::new(
        CheckpointStore::open(&checkpoint_path)
            .with_context(|| format!("open checkpoint {}", checkpoint_path.display()))?,
    );

    let migration_id = match args.migration_id {
        Some(id) => id,
        None => {
            let migrations = store.list_migrations().context("list migrations")?;
            let Some(latest) = migrations.into_iter().next() else {
                bail!(
                    "no migrations found in {}; run migrate first or pass a migration id",
                    checkpoint_path.display()
                );
            };
            latest.id
        }
    };

    let Some(migration) = store
        .get_migration(&migration_id)
        .context("load migration")?
    else {
        bail!(
            "migration `{}` not found in {}",
            migration_id,
            checkpoint_path.display()
        );
    };

    let summary = store
        .migration_summary(&migration_id)
        .context("summarize migration")?;

    let ui = maybe_spawn_dashboard(&config, Arc::clone(&store), None, args.ui_addr.as_deref())?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "UI is disabled in config; set [ui] enabled = true or pass --ui-addr ADDR"
            )
        })?;
    ui.set_migration_id(&migration_id);

    let listen_addr = args
        .ui_addr
        .as_deref()
        .unwrap_or(config.ui.listen_addr.as_str());

    info!(
        migration_id = %migration_id,
        status = %migration.status,
        completed = summary.completed,
        "serving migration dashboard"
    );

    println!("Odyssey Migrator UI");
    println!();
    println!("  migration     {migration_id}");
    println!(
        "  table         {}.{}",
        migration.keyspace_name, migration.table_name
    );
    println!("  status        {}", migration.status);
    println!(
        "  units         {} completed / {} total",
        summary.completed,
        summary.total_units()
    );
    println!("  rows written  {}", summary.total_rows_written);
    println!("  dashboard     http://{listen_addr}/");
    println!();
    println!("Press Ctrl+C to exit.");

    hold_dashboard_if_needed(Some(ui), listen_addr, 0, true).await;
    Ok(())
}
