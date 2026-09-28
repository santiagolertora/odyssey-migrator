use anyhow::{Context, Result, bail};
use tracing::info;

use odyssey_checkpoint::CheckpointStore;
use odyssey_cql::discover_table;
use odyssey_core::ValidationMode;
use odyssey_types::TokenRange;
use odyssey_validation::{ValidationOptions, validate_table};

use crate::cli::ValidateArgs;
use crate::commands::migrate::{connect_pair, resolve_checkpoint_path};
use crate::commands::status::load_config;

pub async fn run(args: ValidateArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    let checkpoint_path =
        resolve_checkpoint_path(Some(args.config.as_path()), args.checkpoint.as_deref())?;

    let store = CheckpointStore::open(&checkpoint_path)
        .with_context(|| format!("open checkpoint {}", checkpoint_path.display()))?;

    let Some(migration) = store
        .get_migration(&args.migration_id)
        .context("load migration")?
    else {
        bail!(
            "migration `{}` not found — refusing to validate without durable unit ranges",
            args.migration_id
        );
    };

    let units = store
        .list_units(&args.migration_id)
        .context("list units")?;
    if units.is_empty() {
        bail!("migration `{}` has no units to validate", args.migration_id);
    }

    let ranges: Result<Vec<_>, _> = units
        .iter()
        .map(|u| {
            TokenRange::new(u.token_start, u.token_end)
                .map_err(|err| anyhow::anyhow!("unit {}: {err}", u.id))
        })
        .collect();
    let ranges = ranges?;

    let (source, target) = connect_pair(&config).await?;
    let source_schema = discover_table(
        &source,
        &migration.keyspace_name,
        &migration.table_name,
    )
    .await
    .context("discover source table")?;

    let (tgt_ks, tgt_table) = target_for(&config, &migration.keyspace_name, &migration.table_name)?;
    let target_schema = discover_table(&target, &tgt_ks, &tgt_table)
        .await
        .context("discover target table")?;

    let mode = config.validation.mode;
    info!(
        migration_id = %args.migration_id,
        ?mode,
        ranges = ranges.len(),
        "starting validation"
    );

    let report = validate_table(ValidationOptions {
        source,
        target,
        source_schema,
        target_schema,
        ranges,
        mode,
        page_size: config.engine.page_size,
        sample_partitions_per_range: config.validation.sample_size,
        compare_timestamps: config.validation.compare_timestamps,
        writetime_tolerance_us: config.validation.writetime_tolerance_us,
        ttl_tolerance_secs: config.validation.ttl_tolerance_secs,
    })
    .await
    .context("validate table")?;

    println!("Odyssey Migrator validate");
    println!();
    println!("Migration      {}", args.migration_id);
    println!("Mode           {:?}", report.mode);
    println!("Ranges         {}", report.ranges.len());
    println!();
    println!(
        "{:<24} {:<16} {:<16} {}",
        "Range", "Source rows", "Target rows", "Digest"
    );
    println!("{}", "-".repeat(72));

    for range in &report.ranges {
        let mark = if range.matches { "OK" } else { "DIFF" };
        println!(
            "{:>10}→{:<10} {:<16} {:<16} {} ({})",
            range.range.start,
            range.range.end,
            range.source_rows,
            range.target_rows,
            mark,
            short_digest(&range.source_digest, &range.target_digest, range.matches)
        );
        for sample in &range.diffs {
            println!("  PK {}  ({:?})", sample.primary_key, sample.kind);
            if sample.columns.is_empty() {
                continue;
            }
            println!("  {:<16} {:<24} {}", "column", "source", "target");
            for col in &sample.columns {
                println!(
                    "  {:<16} {:<24} {}",
                    col.name, col.source, col.target
                );
            }
        }
    }

    println!();
    if report.all_match {
        println!("Result: MATCH — source and target digests agree on every range.");
        Ok(())
    } else {
        let mismatches = report.ranges.iter().filter(|r| !r.matches).count();
        bail!(
            "validation FAILED: {mismatches} range(s) differ (mode {:?}). Odyssey will not report success.",
            mode_label(mode)
        );
    }
}

fn short_digest(source: &str, target: &str, matches: bool) -> String {
    if matches {
        source.chars().take(12).collect()
    } else {
        format!(
            "{}≠{}",
            source.chars().take(8).collect::<String>(),
            target.chars().take(8).collect::<String>()
        )
    }
}

fn mode_label(mode: ValidationMode) -> &'static str {
    match mode {
        ValidationMode::Sample => "sample",
        ValidationMode::Digest => "digest",
        ValidationMode::Full => "full",
    }
}

fn target_for(
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
