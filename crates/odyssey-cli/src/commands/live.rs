use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::info;

use odyssey_cdc::{CatchupOptions, run_catchup};
use odyssey_cql::discover_table;
use odyssey_core::Config;

use crate::cli::LiveArgs;
use crate::commands::migrate::connect_pair;
use crate::commands::status::load_config;

pub async fn run(args: LiveArgs) -> Result<()> {
    let mut config = load_config(&args.config)?;
    // Live command always runs catch-up even if [live].enabled is false in TOML.
    config.live.enabled = true;

    if config.live.window_secs == 0 || config.live.safety_secs == 0 || config.live.sleep_secs == 0 {
        bail!("live.window_secs / safety_secs / sleep_secs must be >= 1");
    }

    let end_at = args.end_duration()?;
    let (source, target) = connect_pair(&config).await?;

    println!("Odyssey Migrator live (CDC catch-up)");
    println!();

    for mapping in &config.tables {
        let (src_ks, src_table) = mapping.parse_source()?;
        let (tgt_ks, tgt_table) = mapping.parse_target()?;

        let source_schema = discover_table(&source, &src_ks, &src_table)
            .await
            .with_context(|| format!("discover source {src_ks}.{src_table}"))?;
        let target_schema = discover_table(&target, &tgt_ks, &tgt_table)
            .await
            .with_context(|| format!("discover target {tgt_ks}.{tgt_table}"))?;

        info!(
            source = %mapping.source,
            target = %mapping.target,
            "starting CDC catch-up for table"
        );

        println!("Table {src_ks}.{src_table} → {tgt_ks}.{tgt_table}");

        let report = run_catchup(CatchupOptions {
            source: Arc::clone(&source),
            target: Arc::clone(&target),
            source_schema,
            target_schema,
            live: config.live.clone(),
            target_consistency: config.target.consistency,
            end_at: Some(end_at),
        })
        .await
        .context("CDC catch-up")?;

        println!("  applied       {}", report.applied);
        println!("  upserts       {}", report.inserts_or_updates);
        println!("  deletes       {}", report.deletes);
        println!("  ignored       {}", report.ignored);
        println!();
    }

    println!("Live catch-up finished.");
    println!("Tip: run `odyssey-migrator validate <id> --config …` after cutover.");
    Ok(())
}

/// Shared helper for migrate --with-live.
pub async fn run_catchup_for_config(config: &Config, end_at: Duration) -> Result<()> {
    let (source, target) = connect_pair(config).await?;
    for mapping in &config.tables {
        let (src_ks, src_table) = mapping.parse_source()?;
        let (tgt_ks, tgt_table) = mapping.parse_target()?;
        let source_schema = discover_table(&source, &src_ks, &src_table).await?;
        let target_schema = discover_table(&target, &tgt_ks, &tgt_table).await?;
        let report = run_catchup(CatchupOptions {
            source: Arc::clone(&source),
            target: Arc::clone(&target),
            source_schema,
            target_schema,
            live: config.live.clone(),
            target_consistency: config.target.consistency,
            end_at: Some(end_at),
        })
        .await?;
        info!(
            table = %mapping.source,
            applied = report.applied,
            deletes = report.deletes,
            "post-bulk CDC catch-up done"
        );
    }
    Ok(())
}
