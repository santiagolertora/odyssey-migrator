use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::str::FromStr;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tracing::{info, warn};

use odyssey_checkpoint::{CheckpointStore, NewMigration};
use odyssey_cql::{
    ClusterInfo, CreateSchemaOptions, CqlSession, EnsureSchemaAction, TableSchema,
    connect_source, connect_target, describe_cluster, discover_table, discover_vnode_tokens,
    ensure_target_table, vnode_parent_ranges,
};
use odyssey_core::{Config, PlannerMode};
use odyssey_engine::{PageProgress, ProgressObserver, RunOptions, run_migration};
use odyssey_metrics::{OdysseyMetrics, serve_metrics};
use odyssey_planner::{PlanOptions, split_murmur3, split_topology};
use odyssey_types::MigrationUnit;
use odyssey_validation::schema_fingerprint;

use crate::cli::MigrateArgs;
use crate::ui::{DashboardHandle, hold_dashboard_if_needed, maybe_spawn_ui};

pub async fn run(args: MigrateArgs) -> Result<()> {
    let config = Config::load(&args.config)
        .with_context(|| format!("load config {}", args.config.display()))?;
    let migration_name = config.migration.name.clone();
    let notify_cfg = config.notify.clone();

    match run_inner(args, config).await {
        Ok(summary) => {
            if let Some(s) = summary {
                crate::notify::notify_complete(
                    &notify_cfg,
                    &migration_name,
                    &s.migration_id,
                    s.rows_written,
                    s.units_completed,
                )
                .await;
            }
            Ok(())
        }
        Err(err) => {
            crate::notify::notify_error(&notify_cfg, &migration_name, &format!("{err:#}")).await;
            Err(err)
        }
    }
}

struct MigrateSummary {
    migration_id: String,
    rows_written: u64,
    units_completed: u64,
}

async fn run_inner(args: MigrateArgs, config: Config) -> Result<Option<MigrateSummary>> {
    ensure_integrity(&config)?;

    let metrics = maybe_spawn_metrics(&config)?;

    info!(
        migration = %config.migration.name,
        tables = config.tables.len(),
        workers = config.engine.workers,
        plan_only = args.plan_only,
        "starting migrate"
    );

    let source = Arc::new(
        connect_source(&config.source)
            .await
            .context("connect source")?,
    );
    let target = Arc::new(
        connect_target(&config.target)
            .await
            .context("connect target")?,
    );

    let source_info = describe_cluster(&source)
        .await
        .context("describe source cluster")?;
    let target_info = describe_cluster(&target)
        .await
        .context("describe target cluster")?;

    if !source_info.partitioner.contains("Murmur3") {
        warn!(
            partitioner = %source_info.partitioner,
            "source partitioner is not Murmur3; token planning assumes Murmur3 ranges"
        );
    }

    print_cluster_banner(&source_info, &target_info);

    let desired_units = config.desired_work_units();
    let mut ranges = match config.planner.mode {
        PlannerMode::Even => {
            split_murmur3(PlanOptions { desired_units }).context("plan token ranges (even)")?
        }
        PlannerMode::Vnode => {
            let tokens = discover_vnode_tokens(&source)
                .await
                .context("discover vnode tokens for planner.mode=vnode")?;
            let parents = vnode_parent_ranges(&tokens).context("build vnode parent ranges")?;
            info!(
                vnodes = parents.len(),
                desired_units,
                "planning topology-aware token ranges"
            );
            split_topology(&parents, PlanOptions { desired_units })
                .context("plan token ranges (vnode)")?
        }
    };
    if let Some(max_units) = config.engine.max_units {
        if max_units == 0 {
            bail!("engine.max_units must be >= 1 when set");
        }
        if max_units < ranges.len() {
            warn!(
                planned = ranges.len(),
                max_units,
                "limiting migration to first max_units token ranges (smoke / disk safety)"
            );
            ranges.truncate(max_units);
        }
    }

    let checkpoint = if args.plan_only {
        None
    } else {
        Some(Arc::new(
            CheckpointStore::open_from_config(&config.checkpoint).context("open checkpoint")?,
        ))
    };

    let ui = match &checkpoint {
        Some(store) => {
            maybe_spawn_dashboard(&config, Arc::clone(store), metrics.clone(), args.ui_addr.as_deref())?
        }
        None => None,
    };

    let mut last_migration_id = None;
    let mut total_rows_written = 0u64;
    let mut total_units_completed = 0u64;

    for mapping in &config.tables {
        let (src_ks, src_table) = mapping.parse_source()?;
        let (tgt_ks, tgt_table) = mapping.parse_target()?;

        let source_schema = discover_table(&source, &src_ks, &src_table)
            .await
            .with_context(|| format!("discover source table {src_ks}.{src_table}"))?;

        if config.target.create_schema {
            let action = ensure_target_table(
                &target,
                &source_schema,
                &tgt_ks,
                &tgt_table,
                &CreateSchemaOptions {
                    datacenter: config.target.datacenter.clone(),
                    replication_factor: config.target.create_schema_rf,
                },
            )
            .await
            .with_context(|| format!("create_schema for target {tgt_ks}.{tgt_table}"))?;
            match action {
                EnsureSchemaAction::AlreadyExists => {
                    println!("Schema        target {tgt_ks}.{tgt_table} already exists");
                }
                EnsureSchemaAction::CreatedKeyspaceAndTable => {
                    println!(
                        "Schema        created keyspace+table {tgt_ks}.{tgt_table} on target (RF={})",
                        config.target.create_schema_rf
                    );
                }
                EnsureSchemaAction::CreatedTable => {
                    println!("Schema        created table {tgt_ks}.{tgt_table} on target");
                }
            }
        }

        let target_schema = discover_table(&target, &tgt_ks, &tgt_table)
            .await
            .with_context(|| {
                if config.target.create_schema {
                    format!("discover target table {tgt_ks}.{tgt_table} after create_schema")
                } else {
                    format!(
                        "discover target table {tgt_ks}.{tgt_table} \
                         (missing? set target.create_schema = true)"
                    )
                }
            })?;

        assert_compatible_schemas(&source_schema, &target_schema)?;

        let select_cql = source_schema.range_select_cql()?;
        let mut units = Vec::with_capacity(ranges.len());
        for range in &ranges {
            units.push(
                MigrationUnit::new(&src_ks, &src_table, *range).context("create migration unit")?,
            );
        }

        println!("Table");
        println!("  source        {src_ks}.{src_table}");
        println!("  target        {tgt_ks}.{tgt_table}");
        println!(
            "  partition key {}",
            source_schema
                .partition_key_columns()
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("  token ranges  {}", ranges.len());
        println!("  range SELECT  {select_cql}");
        println!();

        if args.plan_only {
            continue;
        }

        let store = checkpoint.as_ref().expect("checkpoint open for execute");
        let migration_id = store
            .create_migration(NewMigration {
                name: format!("{}:{}", config.migration.name, mapping.source),
                source_cluster: source_info.cluster_name.clone(),
                target_cluster: target_info.cluster_name.clone(),
                keyspace_name: src_ks.clone(),
                table_name: src_table.clone(),
                schema_hash: schema_fingerprint(&source_schema),
            })
            .context("create migration checkpoint")?;
        if let Some(ui) = &ui {
            ui.set_migration_id(&migration_id);
        }
        store
            .insert_units(&migration_id, &units)
            .context("insert migration units")?;

        let pending_units = store.migration_summary(&migration_id)?.pending;
        if let Some(m) = &metrics {
            m.set_ranges_pending(pending_units);
            m.set_ranges_running(0);
            m.set_ranges_completed(0);
            m.set_worker_concurrency(config.engine.concurrency.initial as u64);
        }

        println!("Migration");
        println!("  id            {migration_id}");
        println!("  name          {}", config.migration.name);
        println!("  workers       {}", config.engine.workers);
        println!("  work units    {}", units.len());
        println!(
            "  durability    {:?} (checkpoint before complete)",
            config.integrity.guarantee
        );
        if config.ui.enabled || args.ui_addr.is_some() {
            let addr = args
                .ui_addr
                .as_deref()
                .unwrap_or(config.ui.listen_addr.as_str());
            println!("  dashboard     http://{addr}/");
        }
        println!();

        info!(%migration_id, table = %mapping.source, "running copy workers");

        let progress = metrics.as_ref().map(|m| {
            Arc::new(LiveMetrics::new(Arc::clone(m), pending_units)) as Arc<dyn ProgressObserver>
        });

        let report = run_migration(RunOptions {
            source: Arc::clone(&source),
            target: Arc::clone(&target),
            config: config.clone(),
            source_schema,
            target_schema,
            checkpoint: Arc::clone(store),
            migration_id: migration_id.clone(),
            progress,
        })
        .await
        .context("run migration")?;

        if let Some(m) = &metrics {
            m.set_ranges_completed(report.units_completed);
            m.set_ranges_pending(0);
            m.set_ranges_running(0);
        }

        println!("Completed");
        println!("  migration     {}", report.migration_id);
        println!("  units         {}", report.units_completed);
        println!("  rows read     {}", report.rows_read);
        println!("  rows written  {}", report.rows_written);
        println!("  bytes written {}", report.bytes_written);
        println!();

        last_migration_id = Some(migration_id);
        total_rows_written = total_rows_written.saturating_add(report.rows_written);
        total_units_completed = total_units_completed.saturating_add(report.units_completed);
    }

    if args.plan_only {
        println!("Plan-only complete. No data was moved.");
        println!(
            "Re-run without --plan-only to copy rows (at-least-once; PK rewrites beat silent loss)."
        );
        return Ok(None);
    }

    if args.with_live || config.live.enabled {
        use std::time::{SystemTime, UNIX_EPOCH};
        let end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            + std::time::Duration::from_secs(config.live.safety_secs.max(30));
        println!("Running CDC catch-up (--with-live / [live].enabled)…");
        crate::commands::live::run_catchup_for_config(&config, end)
            .await
            .context("post-bulk CDC catch-up")?;
        println!("CDC catch-up finished.");
        println!();
    }

    if let Some(ref id) = last_migration_id {
        println!("Done. Check progress later with:");
        println!("  odyssey-migrator status {id} --config {}", args.config.display());
        println!("  odyssey-migrator validate {id} --config {}", args.config.display());
        println!("  odyssey-migrator ui --config {}", args.config.display());
        println!("  odyssey-migrator live --config {}", args.config.display());
    }

    let listen_addr = args
        .ui_addr
        .as_deref()
        .unwrap_or(config.ui.listen_addr.as_str());
    hold_dashboard_if_needed(ui, listen_addr, config.ui.hold_secs, args.ui_hold).await;

    Ok(last_migration_id.map(|migration_id| MigrateSummary {
        migration_id,
        rows_written: total_rows_written,
        units_completed: total_units_completed,
    }))
}

fn ensure_integrity(config: &Config) -> Result<()> {
    config
        .integrity
        .validate()
        .map_err(|msg| anyhow::anyhow!(msg))?;
    if !config.integrity.fail_unit_on_write_error
        || !config.integrity.require_checkpoint_before_complete
    {
        bail!("integrity policy refuses unsafe settings");
    }
    Ok(())
}

fn print_cluster_banner(source: &ClusterInfo, target: &ClusterInfo) {
    println!("Odyssey Migrator {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Source");
    println!("  cluster       {}", source.cluster_name);
    println!("  datacenter    {}", source.data_center);
    println!("  partitioner   {}", source.partitioner);
    println!();
    println!("Target");
    println!("  cluster       {}", target.cluster_name);
    println!("  datacenter    {}", target.data_center);
    println!("  partitioner   {}", target.partitioner);
    println!();
}

fn assert_compatible_schemas(source: &TableSchema, target: &TableSchema) -> Result<()> {
    let src = source.ordered_columns();
    let tgt = target.ordered_columns();
    if src.len() != tgt.len() {
        bail!(
            "schema mismatch: source {}.{} has {} columns, target {}.{} has {}",
            source.keyspace,
            source.table,
            src.len(),
            target.keyspace,
            target.table,
            tgt.len()
        );
    }
    for (a, b) in src.iter().zip(tgt.iter()) {
        if a.name != b.name || a.type_name != b.type_name || a.kind != b.kind {
            bail!(
                "schema mismatch on column `{}` (source type/kind {:?}/{:?} vs target {:?}/{:?})",
                a.name,
                a.type_name,
                a.kind,
                b.type_name,
                b.kind
            );
        }
    }
    Ok(())
}

pub(crate) fn maybe_spawn_metrics(config: &Config) -> Result<Option<Arc<OdysseyMetrics>>> {
    if !config.metrics.enabled {
        return Ok(None);
    }
    let addr = SocketAddr::from_str(&config.metrics.listen_addr).with_context(|| {
        format!(
            "parse metrics.listen_addr `{}`",
            config.metrics.listen_addr
        )
    })?;
    let metrics = Arc::new(OdysseyMetrics::new());
    let serve = Arc::clone(&metrics);
    tokio::spawn(async move {
        if let Err(err) = serve_metrics(addr, serve).await {
            warn!(error = %err, "metrics HTTP server stopped");
        }
    });
    info!(%addr, "metrics listening on /metrics");
    Ok(Some(metrics))
}

pub(crate) fn maybe_spawn_dashboard(
    config: &Config,
    checkpoint: Arc<CheckpointStore>,
    metrics: Option<Arc<OdysseyMetrics>>,
    ui_addr_override: Option<&str>,
) -> Result<Option<DashboardHandle>> {
    let enabled = config.ui.enabled || ui_addr_override.is_some();
    let listen_addr = ui_addr_override
        .map(str::to_string)
        .unwrap_or_else(|| config.ui.listen_addr.clone());
    let metrics_addr = if config.metrics.enabled {
        Some(config.metrics.listen_addr.clone())
    } else {
        None
    };
    maybe_spawn_ui(
        enabled,
        &listen_addr,
        checkpoint,
        config.migration.name.clone(),
        metrics,
        metrics_addr,
    )
}

pub fn resolve_checkpoint_path(
    config_path: Option<&Path>,
    checkpoint_override: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = checkpoint_override {
        return Ok(path.to_path_buf());
    }
    let Some(config_path) = config_path else {
        bail!("provide --checkpoint PATH or --config FILE so Odyssey knows where the SQLite store lives");
    };
    let config = Config::load(config_path)
        .with_context(|| format!("load config {}", config_path.display()))?;
    Ok(PathBuf::from(config.checkpoint.path))
}

pub async fn connect_pair(config: &Config) -> Result<(Arc<CqlSession>, Arc<CqlSession>)> {
    let source = Arc::new(connect_source(&config.source).await.context("connect source")?);
    let target = Arc::new(connect_target(&config.target).await.context("connect target")?);
    Ok((source, target))
}

/// Pushes page/unit events into Prometheus + periodic console progress lines.
pub(crate) struct LiveMetrics {
    inner: Arc<OdysseyMetrics>,
    pending: AtomicU64,
    running: AtomicU64,
    completed: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
    started: Instant,
    last_print: std::sync::Mutex<Instant>,
}

impl LiveMetrics {
    pub(crate) fn new(inner: Arc<OdysseyMetrics>, pending: u64) -> Self {
        Self {
            inner,
            pending: AtomicU64::new(pending),
            running: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            started: Instant::now(),
            last_print: std::sync::Mutex::new(Instant::now()),
        }
    }

    fn maybe_print_progress(&self) {
        let Ok(mut last) = self.last_print.lock() else {
            return;
        };
        if last.elapsed().as_secs() < 5 {
            return;
        }
        *last = Instant::now();
        let rows = self.rows.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let elapsed = self.started.elapsed().as_secs_f64().max(0.001);
        let rate = rows as f64 / elapsed;
        let running = self.running.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed);
        let pending = self.pending.load(Ordering::Relaxed);
        println!(
            "Progress  rows={rows}  data={:.1} MB  rate={:.0} rows/s  ranges={completed} done / {running} run / {pending} pending",
            bytes as f64 / (1024.0 * 1024.0),
            rate
        );
    }
}

impl ProgressObserver for LiveMetrics {
    fn page_copied(&self, page: &PageProgress) {
        self.inner.rows_read_add(page.rows);
        self.inner.rows_written_add(page.rows);
        self.inner.bytes_read_add(page.bytes);
        self.inner.bytes_written_add(page.bytes);
        self.inner.retries_add(page.retries);
        self.inner.observe_source_latency(page.source_latency);
        for latency in &page.target_latencies {
            self.inner.observe_target_latency(*latency);
        }
        self.inner
            .set_worker_concurrency(page.write_concurrency as u64);
        self.rows.fetch_add(page.rows, Ordering::Relaxed);
        self.bytes.fetch_add(page.bytes, Ordering::Relaxed);
        self.maybe_print_progress();
    }

    fn unit_claimed(&self) {
        let pending = self.pending.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
        let running = self.running.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner.set_ranges_pending(pending);
        self.inner.set_ranges_running(running);
    }

    fn unit_completed(&self) {
        let running = self.running.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
        let completed = self.completed.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner.set_ranges_running(running);
        self.inner.set_ranges_completed(completed);
    }

    fn unit_failed(&self) {
        let running = self.running.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
        self.inner.set_ranges_running(running);
        self.inner.errors_add(1);
    }
}
