use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use odyssey_checkpoint::CheckpointStore;
use odyssey_cql::{CqlSession, TableSchema};
use odyssey_core::Config;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::progress::ProgressObserver;
use crate::throttle::AdaptiveConcurrency;
use crate::worker::migrate_unit;
use crate::EngineError;

/// Inputs for a full migration run over claimed checkpoint units.
pub struct RunOptions {
    pub source: Arc<CqlSession>,
    pub target: Arc<CqlSession>,
    pub config: Config,
    pub source_schema: TableSchema,
    pub target_schema: TableSchema,
    pub checkpoint: Arc<CheckpointStore>,
    pub migration_id: String,
    pub progress: Option<Arc<dyn ProgressObserver>>,
}

/// Aggregate counters returned when every claimed unit completes successfully.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MigrationReport {
    pub migration_id: String,
    pub units_completed: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
}

/// Run workers that claim pending units until none remain.
///
/// Fail closed: if any unit fails after retries, new claims stop and this
/// returns [`EngineError::MigrationFailed`] (or the first unit error).
pub async fn run_migration(opts: RunOptions) -> Result<MigrationReport, EngineError> {
    opts.config
        .integrity
        .validate()
        .map_err(EngineError::InvalidConfig)?;

    let workers = opts.config.engine.workers;
    if workers == 0 {
        return Err(EngineError::InvalidConfig(
            "engine.workers must be >= 1".into(),
        ));
    }

    let throttle = Arc::new(Mutex::new(AdaptiveConcurrency::from_engine(
        &opts.config.engine,
    )));
    let stop = Arc::new(AtomicBool::new(false));
    let units_completed = Arc::new(AtomicU64::new(0));
    let rows_read = Arc::new(AtomicU64::new(0));
    let rows_written = Arc::new(AtomicU64::new(0));
    let bytes_written = Arc::new(AtomicU64::new(0));
    let failed_units = Arc::new(AtomicU64::new(0));

    let shared = Arc::new(opts);
    let mesh_worker = shared.config.mesh.resolved_worker_id();
    let _ = shared
        .checkpoint
        .set_busy_timeout_ms(shared.config.mesh.busy_timeout_ms);
    let _ = shared.checkpoint.reclaim_stale_running(
        &shared.migration_id,
        shared.config.mesh.lease_secs,
    );

    info!(
        migration_id = %shared.migration_id,
        workers,
        mesh_worker = %mesh_worker,
        page_size = shared.config.engine.page_size,
        "starting migration workers"
    );

    let mut join_set = JoinSet::new();
    for worker_id in 0..workers {
        let shared = Arc::clone(&shared);
        let throttle = Arc::clone(&throttle);
        let stop = Arc::clone(&stop);
        let units_completed = Arc::clone(&units_completed);
        let rows_read = Arc::clone(&rows_read);
        let rows_written = Arc::clone(&rows_written);
        let bytes_written = Arc::clone(&bytes_written);
        let failed_units = Arc::clone(&failed_units);

        join_set.spawn(async move {
            worker_loop(
                worker_id,
                shared,
                throttle,
                stop,
                units_completed,
                rows_read,
                rows_written,
                bytes_written,
                failed_units,
            )
            .await
        });
    }

    let mut first_error: Option<EngineError> = None;
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                stop.store(true, Ordering::SeqCst);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(join_err) => {
                stop.store(true, Ordering::SeqCst);
                let err = EngineError::WorkerJoin(join_err.to_string());
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    let failed = failed_units.load(Ordering::SeqCst);
    if let Some(err) = first_error {
        error!(
            migration_id = %shared.migration_id,
            failed_units = failed,
            error = %err,
            "migration aborted"
        );
        return Err(err);
    }
    if failed > 0 {
        return Err(EngineError::MigrationFailed {
            migration_id: shared.migration_id.clone(),
            failed_units: failed,
        });
    }

    let _ = shared
        .checkpoint
        .mark_migration_completed_if_done(&shared.migration_id);

    let report = MigrationReport {
        migration_id: shared.migration_id.clone(),
        units_completed: units_completed.load(Ordering::SeqCst),
        rows_read: rows_read.load(Ordering::SeqCst),
        rows_written: rows_written.load(Ordering::SeqCst),
        bytes_written: bytes_written.load(Ordering::SeqCst),
    };

    info!(
        migration_id = %report.migration_id,
        units_completed = report.units_completed,
        rows_read = report.rows_read,
        rows_written = report.rows_written,
        bytes_written = report.bytes_written,
        "migration finished"
    );

    Ok(report)
}

async fn worker_loop(
    worker_id: usize,
    shared: Arc<RunOptions>,
    throttle: Arc<Mutex<AdaptiveConcurrency>>,
    stop: Arc<AtomicBool>,
    units_completed: Arc<AtomicU64>,
    rows_read: Arc<AtomicU64>,
    rows_written: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
    failed_units: Arc<AtomicU64>,
) -> Result<(), EngineError> {
    loop {
        if stop.load(Ordering::SeqCst) {
            warn!(worker_id, "stopping worker after peer failure");
            break;
        }

        let claimed = shared.checkpoint.claim_pending_unit_for(
            &shared.migration_id,
            &format!(
                "{}-w{worker_id}",
                shared.config.mesh.resolved_worker_id()
            ),
        )?;
        let Some(unit) = claimed else {
            break;
        };

        if let Some(progress) = &shared.progress {
            progress.unit_claimed();
        }

        info!(
            worker_id,
            unit_id = %unit.id,
            token_start = unit.token_start,
            token_end = unit.token_end,
            "worker claimed unit"
        );

        let result = migrate_unit(
            Arc::clone(&shared.source),
            Arc::clone(&shared.target),
            &shared.config,
            &shared.source_schema,
            &shared.target_schema,
            &shared.checkpoint,
            &unit,
            &throttle,
            shared.progress.as_deref(),
        )
        .await;

        match result {
            Ok(work) => {
                units_completed.fetch_add(1, Ordering::SeqCst);
                rows_read.fetch_add(work.rows_read, Ordering::SeqCst);
                rows_written.fetch_add(work.rows_written, Ordering::SeqCst);
                bytes_written.fetch_add(work.bytes_written, Ordering::SeqCst);
                if let Some(progress) = &shared.progress {
                    progress.unit_completed();
                }
            }
            Err(err) => {
                failed_units.fetch_add(1, Ordering::SeqCst);
                if let Some(progress) = &shared.progress {
                    progress.unit_failed();
                }
                stop.store(true, Ordering::SeqCst);
                return Err(err);
            }
        }
    }
    Ok(())
}
