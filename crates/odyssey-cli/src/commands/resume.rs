use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tracing::info;

use odyssey_checkpoint::CheckpointStore;
use odyssey_cql::discover_table;
use odyssey_engine::{ProgressObserver, RunOptions, run_migration};
use odyssey_types::MigrationState;

use crate::cli::ResumeArgs;
use crate::commands::migrate::{
    connect_pair, maybe_spawn_dashboard, maybe_spawn_metrics, resolve_checkpoint_path,
};
use crate::commands::status::load_config;
use crate::ui::hold_dashboard_if_needed;

pub async fn run(args: ResumeArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    config
        .integrity
        .validate()
        .map_err(|msg| anyhow::anyhow!(msg))?;

    let metrics = maybe_spawn_metrics(&config)?;

    let checkpoint_path =
        resolve_checkpoint_path(Some(args.config.as_path()), args.checkpoint.as_deref())?;
    let store = Arc::new(
        CheckpointStore::open(&checkpoint_path)
            .with_context(|| format!("open checkpoint {}", checkpoint_path.display()))?,
    );

    let ui = maybe_spawn_dashboard(
        &config,
        Arc::clone(&store),
        metrics.clone(),
        args.ui_addr.as_deref(),
    )?;

    let Some(migration) = store
        .get_migration(&args.migration_id)
        .context("load migration")?
    else {
        bail!(
            "migration `{}` not found — Odyssey will not invent ranges to resume",
            args.migration_id
        );
    };

    if let Some(ui) = &ui {
        ui.set_migration_id(&args.migration_id);
    }

    let reset = store
        .reset_running_to_pending(&args.migration_id)
        .context("reset interrupted running units")?;
    let requeued = store
        .requeue_failed_to_pending(&args.migration_id)
        .context("requeue failed units")?;

    let resumable = store
        .resumable_units(&args.migration_id)
        .context("list resumable units")?;

    info!(
        migration_id = %args.migration_id,
        reset_running = reset,
        requeued_failed = requeued,
        resumable = resumable.len(),
        "resuming migration"
    );

    if resumable.is_empty() {
        let summary = store.migration_summary(&args.migration_id)?;
        if summary.all_completed() {
            println!(
                "Migration `{}` already completed ({} units).",
                args.migration_id, summary.completed
            );
            let _ = store.mark_migration_completed_if_done(&args.migration_id);
            let listen_addr = args
                .ui_addr
                .as_deref()
                .unwrap_or(config.ui.listen_addr.as_str());
            hold_dashboard_if_needed(ui, listen_addr, config.ui.hold_secs, args.ui_hold).await;
            return Ok(());
        }
        bail!(
            "migration `{}` has nothing resumable (failed={}, pending={}, running={})",
            args.migration_id,
            summary.failed,
            summary.pending,
            summary.running
        );
    }

    // Sanity: only Pending should remain after reset/requeue among resumable.
    let pending = resumable
        .iter()
        .filter(|u| u.state == MigrationState::Pending)
        .count();

    println!("Resuming migration {}", args.migration_id);
    println!(
        "  table         {}.{}",
        migration.keyspace_name, migration.table_name
    );
    println!("  reset running {reset}");
    println!("  requeued fail {requeued}");
    println!("  pending units {pending}");
    println!();

    if let Some(m) = &metrics {
        let summary = store.migration_summary(&args.migration_id)?;
        m.set_ranges_pending(summary.pending);
        m.set_ranges_running(0);
        m.set_ranges_completed(summary.completed);
        m.set_worker_concurrency(config.engine.concurrency.initial as u64);
    }

    let (source, target) = connect_pair(&config).await?;
    let source_schema = discover_table(
        &source,
        &migration.keyspace_name,
        &migration.table_name,
    )
    .await
    .context("discover source table")?;

    let (tgt_ks, tgt_table) =
        target_mapping(&config, &migration.keyspace_name, &migration.table_name)?;
    let target_schema = discover_table(&target, &tgt_ks, &tgt_table)
        .await
        .context("discover target table")?;

    let progress = metrics.as_ref().map(|m| {
        Arc::new(crate::commands::migrate::LiveMetrics::new(
            Arc::clone(m),
            pending as u64,
        )) as Arc<dyn ProgressObserver>
    });

    let listen_addr = args
        .ui_addr
        .clone()
        .unwrap_or_else(|| config.ui.listen_addr.clone());
    let hold_secs = config.ui.hold_secs;
    let ui_hold = args.ui_hold;

    let report = run_migration(RunOptions {
        source,
        target,
        config,
        source_schema,
        target_schema,
        checkpoint: store,
        migration_id: args.migration_id.clone(),
        progress,
    })
    .await
    .context("resume migration")?;

    println!("Resume completed");
    println!("  units         {}", report.units_completed);
    println!("  rows written  {}", report.rows_written);

    hold_dashboard_if_needed(ui, &listen_addr, hold_secs, ui_hold).await;
    Ok(())
}

fn target_mapping(
    config: &odyssey_core::Config,
    src_ks: &str,
    src_table: &str,
) -> Result<(String, String)> {
    for mapping in &config.tables {
        let (ks, table) = mapping.parse_source()?;
        if ks == src_ks && table == src_table {
            return Ok(mapping.parse_target()?);
        }
    }
    Ok((src_ks.to_string(), src_table.to_string()))
}
