use std::sync::Arc;
use std::time::Instant;

use odyssey_checkpoint::{CheckpointStore, UnitProgress, UnitRecord};
use odyssey_cql::{CqlSession, TableSchema};
use odyssey_core::{Config, IntegrityPolicy};
use odyssey_types::{MigrationState, TokenRange};
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::progress::{PageProgress, ProgressObserver};
use crate::reader::RangeReader;
use crate::retry::RetryPolicy;
use crate::throttle::AdaptiveConcurrency;
use crate::writer::RangeWriter;
use crate::preserve::PreserveOptions;
use crate::EngineError;

/// Outcome of migrating a single claimed unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitWorkResult {
    pub unit_id: Uuid,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
}

/// Copy one migration unit end-to-end with at-least-once durability.
///
/// After a successful page write, progress is checkpointed at least every
/// [`odyssey_core::CheckpointConfig::interval_secs`] and always on the final
/// page. The unit is marked completed only after `NoMorePages` and a final
/// `save_progress`. On write failure after retries the unit is marked failed
/// and this returns an error (fail closed — never skips rows).
pub async fn migrate_unit(
    source: Arc<CqlSession>,
    target: Arc<CqlSession>,
    config: &Config,
    source_schema: &TableSchema,
    target_schema: &TableSchema,
    checkpoint: &CheckpointStore,
    unit: &UnitRecord,
    throttle: &Mutex<AdaptiveConcurrency>,
    progress: Option<&dyn ProgressObserver>,
) -> Result<UnitWorkResult, EngineError> {
    let integrity = &config.integrity;
    ensure_integrity(integrity)?;

    let unit_id = Uuid::parse_str(&unit.id).map_err(|err| {
        EngineError::InvalidConfig(format!("unit id `{}` is not a UUID: {err}", unit.id))
    })?;

    let range = TokenRange::new(unit.token_start, unit.token_end).map_err(|err| {
        EngineError::InvalidConfig(format!(
            "unit `{unit_id}` has invalid token range: {err}"
        ))
    })?;

    let retry = RetryPolicy::from_engine(&config.engine);
    let preserve = PreserveOptions::from_engine(
        config.engine.preserve_writetimes,
        config.engine.preserve_ttls,
        config.engine.preserve_frozen_collections,
        config.engine.preserve_collection_elements,
    );

    if source_schema.has_counters() {
        if !config.engine.allow_counters {
            return Err(EngineError::InvalidConfig(format!(
                "table `{}.{}` has counter columns; set engine.allow_counters = true to migrate via UPDATE",
                source_schema.keyspace, source_schema.table
            )));
        }
        if preserve.any() {
            return Err(EngineError::InvalidConfig(
                "preserve_ttls/preserve_writetimes cannot be used with counter tables".into(),
            ));
        }
    }
    let mut reader = RangeReader::prepare(
        source,
        source_schema,
        range,
        config.engine.page_size,
        unit.paging_state.clone(),
        config.source.consistency,
        preserve,
    )
    .await?;
    let writer = RangeWriter::prepare(
        target,
        target_schema,
        retry,
        integrity.clone(),
        config.target.consistency,
        preserve,
        config.engine.write_batch_size,
    )
    .await?;

    let checkpoint_interval = config.checkpoint.interval();
    // Force an immediate first flush so resume has a paging_state early.
    let mut last_checkpoint_at = Instant::now()
        .checked_sub(checkpoint_interval)
        .unwrap_or_else(Instant::now);

    let mut rows_read = unit.rows_read;
    let mut rows_written = unit.rows_written;
    let mut bytes_written = unit.bytes_written;
    let mut last_token = unit.last_token;
    let mut completed = false;

    while let Some(page) = reader.next_page().await.map_err(|err| {
        let mapped = EngineError::UnitFailed {
            unit_id,
            reason: err.to_string(),
        };
        let _ = fail_unit(checkpoint, &unit.id, &mapped);
        mapped
    })? {
        let write_stats = match writer.write_batch(&page.batch, throttle).await {
            Ok(stats) => stats,
            Err(err) => {
                let mapped = EngineError::UnitFailed {
                    unit_id,
                    reason: err.to_string(),
                };
                fail_unit(checkpoint, &unit.id, &mapped)?;
                return Err(mapped);
            }
        };

        rows_read = rows_read.saturating_add(page.batch.rows_count);
        rows_written = rows_written.saturating_add(page.batch.rows_count);
        bytes_written = bytes_written.saturating_add(page.batch.bytes_estimate);
        if let Some(t) = page.last_token {
            last_token = Some(t);
        }
        let page_rows = page.batch.rows_count;
        let page_bytes = page.batch.bytes_estimate;
        let source_latency = page.read_latency;

        let unit_progress = UnitProgress {
            unit_id: unit.id.clone(),
            paging_state: page.checkpoint_paging_state.clone(),
            rows_read,
            rows_written,
            bytes_written,
            state: MigrationState::Running,
            last_token,
        };

        let due = last_checkpoint_at.elapsed() >= checkpoint_interval;
        let should_checkpoint = page.no_more_pages || due;
        if should_checkpoint {
            if let Err(err) = checkpoint.save_progress(unit_progress) {
                error!(
                    unit_id = %unit.id,
                    error = %err,
                    "checkpoint save_progress failed; refusing to continue or complete"
                );
                let mapped = EngineError::UnitFailed {
                    unit_id,
                    reason: err.to_string(),
                };
                let _ = checkpoint.mark_failed(&unit.id, &mapped.to_string());
                return Err(mapped);
            }
            last_checkpoint_at = Instant::now();
            info!(
                unit_id = %unit.id,
                rows_read,
                rows_written,
                bytes_written,
                no_more_pages = page.no_more_pages,
                "checkpointed unit progress"
            );
        } else {
            debug!(
                unit_id = %unit.id,
                rows_read,
                rows_written,
                interval_secs = checkpoint_interval.as_secs(),
                "skipped checkpoint (interval not elapsed)"
            );
        }

        if let Some(obs) = progress {
            obs.page_copied(&PageProgress {
                rows: page_rows,
                bytes: page_bytes,
                source_latency,
                target_latencies: write_stats.latencies,
                retries: write_stats.retries,
                write_concurrency: write_stats.concurrency,
            });
        }

        if page.no_more_pages {
            if !integrity.require_checkpoint_before_complete {
                return Err(EngineError::InvalidConfig(
                    "integrity.require_checkpoint_before_complete cannot be false".into(),
                ));
            }
            // Final save_progress already succeeded above.
            if let Err(err) = checkpoint.mark_completed(&unit.id) {
                error!(
                    unit_id = %unit.id,
                    error = %err,
                    "mark_completed failed after final checkpoint"
                );
                let mapped = EngineError::UnitFailed {
                    unit_id,
                    reason: err.to_string(),
                };
                let _ = checkpoint.mark_failed(&unit.id, &mapped.to_string());
                return Err(mapped);
            }

            info!(
                unit_id = %unit.id,
                rows_read,
                rows_written,
                bytes_written,
                "unit completed"
            );
            completed = true;
            break;
        }
    }

    if !completed {
        let reason = "unit ended without a final checkpointed NoMorePages page".to_string();
        let mapped = EngineError::UnitFailed {
            unit_id,
            reason: reason.clone(),
        };
        fail_unit(checkpoint, &unit.id, &mapped)?;
        return Err(mapped);
    }

    Ok(UnitWorkResult {
        unit_id,
        rows_read,
        rows_written,
        bytes_written,
    })
}

fn ensure_integrity(policy: &IntegrityPolicy) -> Result<(), EngineError> {
    policy.validate().map_err(EngineError::InvalidConfig)?;
    Ok(())
}

fn fail_unit(
    checkpoint: &CheckpointStore,
    unit_id: &str,
    err: &EngineError,
) -> Result<(), EngineError> {
    error!(unit_id = %unit_id, error = %err, "marking unit failed");
    checkpoint.mark_failed(unit_id, &err.to_string())?;
    Ok(())
}
