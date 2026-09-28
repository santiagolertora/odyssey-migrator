//! Run a CDC catch-up loop against a source table and apply to target.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use odyssey_core::{ConsistencyLevel, LiveConfig};
use odyssey_cql::{CqlSession, TableSchema, to_driver_consistency};
use scylla::client::session::Session;
use scylla::statement::unprepared::Statement;
use scylla_cdc::consumer::{CDCRow, Consumer, ConsumerFactory, OperationType};
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use tracing::{debug, error, info, warn};

use crate::apply::{ApplyPlan, CdcColumnValue, plan_apply};
use crate::CdcError;

/// Inputs for [`run_catchup`].
pub struct CatchupOptions {
    pub source: Arc<CqlSession>,
    pub target: Arc<CqlSession>,
    pub source_schema: TableSchema,
    pub target_schema: TableSchema,
    pub live: LiveConfig,
    pub target_consistency: ConsistencyLevel,
    /// When set, stop reading CDC after this instant (unix epoch duration).
    pub end_at: Option<Duration>,
}

/// Counters returned when catch-up finishes (or is stopped at end_at).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatchupReport {
    pub applied: u64,
    pub ignored: u64,
    pub inserts_or_updates: u64,
    pub deletes: u64,
}

struct Stats {
    applied: AtomicU64,
    ignored: AtomicU64,
    upserts: AtomicU64,
    deletes: AtomicU64,
}

struct ReplicatorConsumer {
    target: Arc<Session>,
    target_schema: TableSchema,
    consistency: ConsistencyLevel,
    stats: Arc<Stats>,
}

#[async_trait]
impl Consumer for ReplicatorConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        let columns = extract_columns(&data);
        let plans = plan_apply(&self.target_schema, data.operation.clone(), &columns)
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;

        for plan in plans {
            match plan {
                ApplyPlan::Ignore => {
                    self.stats.ignored.fetch_add(1, Ordering::Relaxed);
                    debug!(op = %data.operation, "ignoring CDC row");
                }
                ApplyPlan::Upsert { cql, values }
                | ApplyPlan::DeleteRow { cql, values }
                | ApplyPlan::DeletePartition { cql, values }
                | ApplyPlan::DeleteColumns { cql, values } => {
                    let mut statement = Statement::new(cql);
                    statement.set_consistency(to_driver_consistency(self.consistency));
                    self.target
                        .query_unpaged(statement, values)
                        .await
                        .map_err(|err| {
                            error!(error = %err, op = %data.operation, "failed to apply CDC change");
                            anyhow::anyhow!("apply CDC change: {err}")
                        })?;
                    self.stats.applied.fetch_add(1, Ordering::Relaxed);
                    match data.operation {
                        OperationType::RowDelete
                        | OperationType::PartitionDelete
                        | OperationType::RowRangeDelInclLeft
                        | OperationType::RowRangeDelExclLeft
                        | OperationType::RowRangeDelInclRight
                        | OperationType::RowRangeDelExclRight => {
                            self.stats.deletes.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            self.stats.upserts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

struct ReplicatorFactory {
    target: Arc<Session>,
    target_schema: TableSchema,
    consistency: ConsistencyLevel,
    stats: Arc<Stats>,
}

#[async_trait]
impl ConsumerFactory for ReplicatorFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(ReplicatorConsumer {
            target: Arc::clone(&self.target),
            target_schema: self.target_schema.clone(),
            consistency: self.consistency,
            stats: Arc::clone(&self.stats),
        })
    }
}

fn extract_columns(data: &CDCRow<'_>) -> Vec<CdcColumnValue> {
    let mut out = Vec::new();
    for name in data.get_non_cdc_column_names() {
        let deleted = data.column_deletable(name) && data.is_value_deleted(name);
        out.push(CdcColumnValue {
            name: name.to_string(),
            value: data.get_value(name).clone(),
            deleted,
        });
    }
    out
}

/// Confirm the CDC log table exists before starting the reader.
async fn ensure_cdc_log(session: &Session, keyspace: &str, table: &str) -> Result<(), CdcError> {
    let log_table = format!("{table}_scylla_cdc_log");
    let result = session
        .query_unpaged(
            "SELECT table_name FROM system_schema.tables \
             WHERE keyspace_name = ? AND table_name = ?",
            (keyspace, log_table.as_str()),
        )
        .await
        .map_err(|err| CdcError::Other(format!("probe CDC log table: {err}")))?;
    let rows = result
        .into_rows_result()
        .map_err(|err| CdcError::Other(err.to_string()))?;
    let mut iter = rows
        .rows::<(String,)>()
        .map_err(|err| CdcError::Other(err.to_string()))?;
    if iter.next().transpose().map_err(|e| CdcError::Other(e.to_string()))?.is_none() {
        return Err(CdcError::CdcNotEnabled {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        });
    }
    Ok(())
}

fn resolve_start(live: &LiveConfig) -> Result<Duration, CdcError> {
    if let Some(raw) = &live.start_at {
        let dt = chrono::DateTime::parse_from_rfc3339(raw).map_err(|err| {
            CdcError::InvalidOptions(format!("live.start_at must be RFC3339: {err}"))
        })?;
        let secs = dt.timestamp().max(0) as u64;
        let nanos = dt.timestamp_subsec_nanos();
        return Ok(Duration::new(secs, nanos));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let overlap = Duration::from_secs(live.overlap_secs);
    Ok(now.saturating_sub(overlap))
}

/// Consume CDC from `source_schema` and apply to `target_schema` until `end_at`
/// (or forever if `end_at` is None — caller should stop the process).
pub async fn run_catchup(opts: CatchupOptions) -> Result<CatchupReport, CdcError> {
    if opts.live.window_secs == 0 || opts.live.safety_secs == 0 || opts.live.sleep_secs == 0 {
        return Err(CdcError::InvalidOptions(
            "live.window_secs, safety_secs, and sleep_secs must be >= 1".into(),
        ));
    }

    ensure_cdc_log(
        opts.source.inner(),
        &opts.source_schema.keyspace,
        &opts.source_schema.table,
    )
    .await?;

    let start = resolve_start(&opts.live)?;
    let end = opts.end_at.unwrap_or(Duration::from_secs(60 * 60 * 24 * 365 * 100));

    if end <= start {
        return Err(CdcError::InvalidOptions(
            "CDC end timestamp must be after start".into(),
        ));
    }

    let stats = Arc::new(Stats {
        applied: AtomicU64::new(0),
        ignored: AtomicU64::new(0),
        upserts: AtomicU64::new(0),
        deletes: AtomicU64::new(0),
    });

    let factory = Arc::new(ReplicatorFactory {
        target: opts.target.shared(),
        target_schema: opts.target_schema.clone(),
        consistency: opts.target_consistency,
        stats: Arc::clone(&stats),
    });

    info!(
        keyspace = %opts.source_schema.keyspace,
        table = %opts.source_schema.table,
        start_secs = start.as_secs(),
        end_secs = end.as_secs(),
        window_secs = opts.live.window_secs,
        "starting CDC catch-up"
    );

    let (mut reader, handle) = CDCLogReaderBuilder::new()
        .session(opts.source.shared())
        .keyspace(&opts.source_schema.keyspace)
        .table_name(&opts.source_schema.table)
        .start_timestamp(start)
        .end_timestamp(end)
        .window_size(Duration::from_secs(opts.live.window_secs))
        .safety_interval(Duration::from_secs(opts.live.safety_secs))
        .sleep_interval(Duration::from_secs(opts.live.sleep_secs))
        .consumer_factory(factory)
        .build()
        .await
        .map_err(|err| CdcError::Reader(err.to_string()))?;

    // When end_at was None we still passed a far-future end so the library
    // returns; for continuous mode the CLI should pass a concrete until.
    let result = handle.await;
    if let Err(err) = result {
        // Soft-stop via reader if needed.
        reader.stop();
        warn!(error = %err, "CDC catch-up reader ended with error");
        return Err(CdcError::Reader(err.to_string()));
    }

    let report = CatchupReport {
        applied: stats.applied.load(Ordering::Relaxed),
        ignored: stats.ignored.load(Ordering::Relaxed),
        inserts_or_updates: stats.upserts.load(Ordering::Relaxed),
        deletes: stats.deletes.load(Ordering::Relaxed),
    };

    info!(
        applied = report.applied,
        ignored = report.ignored,
        upserts = report.inserts_or_updates,
        deletes = report.deletes,
        "CDC catch-up finished"
    );

    Ok(report)
}
