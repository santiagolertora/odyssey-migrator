use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use odyssey_cql::{CollectionKind, CqlSession, TableSchema, to_driver_consistency};
use odyssey_core::{ConsistencyLevel, IntegrityPolicy};
use futures::stream::{self, StreamExt};
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlValue;
use tokio::sync::Mutex;
use tracing::{trace, warn};

use crate::batch::RowBatch;
use crate::preserve::{
    MigratedRow, PreserveOptions, bind_map_element_update, bind_preserved_insert,
    bind_set_element_update, row_primary_key_values,
};
use crate::retry::RetryPolicy;
use crate::statement::{
    build_counter_update_cql, build_insert_cql_preserving, build_map_element_update_cql,
    build_set_element_update_cql,
};
use crate::throttle::AdaptiveConcurrency;
use crate::EngineError;

/// Writes one page of rows to the target with prepared INSERTs (or counter UPDATEs).
pub struct RangeWriter {
    session: Arc<CqlSession>,
    prepared: PreparedStatement,
    schema: TableSchema,
    retry: RetryPolicy,
    integrity: IntegrityPolicy,
    preserve: PreserveOptions,
    consistency: ConsistencyLevel,
    write_batch_size: usize,
    /// When true, binds via counter UPDATE instead of INSERT.
    counters: bool,
    /// Column names in prepared bind order for counter UPDATEs.
    counter_bind_names: Vec<String>,
    /// `ordered_columns` names aligned with [`MigratedRow::values`].
    ordered_names: Vec<String>,
    /// Prepared map/set element UPDATEs keyed by column name.
    map_element_updates: HashMap<String, PreparedStatement>,
    set_element_updates: HashMap<String, PreparedStatement>,
}

impl RangeWriter {
    pub async fn prepare(
        session: Arc<CqlSession>,
        schema: &TableSchema,
        retry: RetryPolicy,
        integrity: IntegrityPolicy,
        consistency: ConsistencyLevel,
        preserve: PreserveOptions,
        write_batch_size: usize,
    ) -> Result<Self, EngineError> {
        if !integrity.fail_unit_on_write_error {
            return Err(EngineError::InvalidConfig(
                "integrity.fail_unit_on_write_error cannot be false".into(),
            ));
        }
        if write_batch_size == 0 {
            return Err(EngineError::InvalidConfig(
                "engine.write_batch_size must be >= 1".into(),
            ));
        }
        if preserve.collection_elements && !preserve.writetimes && !preserve.ttls {
            return Err(EngineError::InvalidConfig(
                "preserve_collection_elements requires preserve_writetimes and/or preserve_ttls"
                    .into(),
            ));
        }

        let ordered_names: Vec<String> = schema
            .ordered_columns()
            .into_iter()
            .map(|c| c.name.clone())
            .collect();
        let counters = schema.has_counters();
        let (cql, counter_bind_names, effective_batch) = if counters {
            let mut names: Vec<String> = schema
                .counter_columns()
                .into_iter()
                .map(|c| c.name.clone())
                .collect();
            names.extend(
                schema
                    .partition_key_columns()
                    .into_iter()
                    .chain(schema.clustering_columns())
                    .map(|c| c.name.clone()),
            );
            (build_counter_update_cql(schema)?, names, 1usize)
        } else if preserve.collection_elements {
            // Element overlays need per-row follow-up UPDATEs; keep inserts single-row.
            (
                build_insert_cql_preserving(schema, preserve)?,
                Vec::new(),
                1usize,
            )
        } else {
            (
                build_insert_cql_preserving(schema, preserve)?,
                Vec::new(),
                write_batch_size,
            )
        };

        let mut statement = Statement::new(cql);
        statement.set_consistency(to_driver_consistency(consistency));
        let prepared = session
            .inner()
            .prepare(statement)
            .await
            .map_err(|err| EngineError::Prepare(err.to_string()))?;

        let mut map_element_updates = HashMap::new();
        let mut set_element_updates = HashMap::new();
        if preserve.collection_elements && !counters {
            for col in schema.unfrozen_map_columns() {
                let cql = build_map_element_update_cql(schema, &col.name, preserve)?;
                let mut st = Statement::new(cql);
                st.set_consistency(to_driver_consistency(consistency));
                let prep = session
                    .inner()
                    .prepare(st)
                    .await
                    .map_err(|err| EngineError::Prepare(err.to_string()))?;
                map_element_updates.insert(col.name.clone(), prep);
            }
            for col in schema.unfrozen_set_columns() {
                let cql = build_set_element_update_cql(schema, &col.name, preserve)?;
                let mut st = Statement::new(cql);
                st.set_consistency(to_driver_consistency(consistency));
                let prep = session
                    .inner()
                    .prepare(st)
                    .await
                    .map_err(|err| EngineError::Prepare(err.to_string()))?;
                set_element_updates.insert(col.name.clone(), prep);
            }
        }

        Ok(Self {
            session,
            prepared,
            schema: schema.clone(),
            retry,
            integrity,
            preserve,
            consistency,
            write_batch_size: effective_batch,
            counters,
            counter_bind_names,
            ordered_names,
            map_element_updates,
            set_element_updates,
        })
    }

    /// Insert every row in the page. Never skips a failed row/chunk.
    ///
    /// When `write_batch_size == 1`, uses single-row executes. Otherwise groups
    /// rows into concurrent CQL `UNLOGGED` batches of that size. If Scylla
    /// rejects a batch as too large, Odyssey splits the chunk in half and retries
    /// (down to single-row inserts) instead of burning retry attempts.
    pub async fn write_batch(
        &self,
        batch: &RowBatch,
        throttle: &Mutex<AdaptiveConcurrency>,
    ) -> Result<WriteBatchStats, EngineError> {
        if !self.integrity.fail_unit_on_write_error {
            return Err(EngineError::InvalidConfig(
                "integrity.fail_unit_on_write_error cannot be false".into(),
            ));
        }
        if batch.is_empty() {
            return Ok(WriteBatchStats::default());
        }

        if self.write_batch_size <= 1 {
            return self.write_rows_singly(&batch.rows, throttle).await;
        }
        self.write_rows_unlogged(&batch.rows, throttle).await
    }

    async fn write_rows_singly(
        &self,
        rows: &[MigratedRow],
        throttle: &Mutex<AdaptiveConcurrency>,
    ) -> Result<WriteBatchStats, EngineError> {
        let concurrency = throttle.lock().await.current();
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        let retry = self.retry.clone();
        let preserve = self.preserve;
        let counters = self.counters;
        let counter_bind_names = self.counter_bind_names.clone();
        let ordered_names = self.ordered_names.clone();
        let schema = self.schema.clone();
        let map_updates = self.map_element_updates.clone();
        let set_updates = self.set_element_updates.clone();

        let results: Vec<Result<(Duration, u32), EngineError>> =
            stream::iter(rows.iter().cloned())
                .map(|row| {
                    let session = session.clone();
                    let prepared = prepared.clone();
                    let retry = retry.clone();
                    let counter_bind_names = counter_bind_names.clone();
                    let ordered_names = ordered_names.clone();
                    let schema = schema.clone();
                    let map_updates = map_updates.clone();
                    let set_updates = set_updates.clone();
                    async move {
                        let values = if counters {
                            counter_bind_names
                                .iter()
                                .map(|name| {
                                    let idx =
                                        ordered_names.iter().position(|n| n == name).unwrap();
                                    row.values.get(idx).cloned().unwrap_or(None)
                                })
                                .collect()
                        } else {
                            bind_preserved_insert(&row, preserve)
                        };
                        let insert_stats = retry
                            .run(|| {
                                let session = session.clone();
                                let prepared = prepared.clone();
                                let values = values.clone();
                                async move {
                                    let started = Instant::now();
                                    session
                                        .inner()
                                        .execute_unpaged(&prepared, values)
                                        .await
                                        .map_err(|err| err.to_string())?;
                                    Ok::<Duration, String>(started.elapsed())
                                }
                            })
                            .await?;

                        if !counters && preserve.collection_elements {
                            write_collection_elements(
                                &session,
                                &schema,
                                &row,
                                preserve,
                                &map_updates,
                                &set_updates,
                                &retry,
                            )
                            .await?;
                        }
                        Ok(insert_stats)
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

        collect_write_results(results, throttle, concurrency).await
    }

    async fn write_rows_unlogged(
        &self,
        rows: &[MigratedRow],
        throttle: &Mutex<AdaptiveConcurrency>,
    ) -> Result<WriteBatchStats, EngineError> {
        let concurrency = throttle.lock().await.current();
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        let retry = self.retry.clone();
        let preserve = self.preserve;
        let consistency = to_driver_consistency(self.consistency);
        let chunk_size = self.write_batch_size;

        let chunks: Vec<Vec<MigratedRow>> = rows
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let results: Vec<Result<(Duration, u32), EngineError>> = stream::iter(chunks)
            .map(|chunk| {
                let session = session.clone();
                let prepared = prepared.clone();
                let retry = retry.clone();
                async move {
                    write_chunk_adaptive(session, prepared, retry, preserve, consistency, chunk)
                        .await
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        collect_write_results(results, throttle, concurrency).await
    }
}

async fn write_collection_elements(
    session: &CqlSession,
    schema: &TableSchema,
    row: &MigratedRow,
    preserve: PreserveOptions,
    map_updates: &HashMap<String, PreparedStatement>,
    set_updates: &HashMap<String, PreparedStatement>,
    retry: &RetryPolicy,
) -> Result<(), EngineError> {
    if row.collection_elements.is_empty() {
        return Ok(());
    }
    let keys = row_primary_key_values(schema, row).map_err(EngineError::Write)?;
    for elem in &row.collection_elements {
        let (prepared, values) = match elem.kind {
            CollectionKind::Map => {
                let Some(prep) = map_updates.get(&elem.column) else {
                    warn!(column = %elem.column, "missing map element UPDATE prepare");
                    continue;
                };
                (prep, bind_map_element_update(elem, &keys, preserve))
            }
            CollectionKind::Set => {
                let Some(prep) = set_updates.get(&elem.column) else {
                    warn!(column = %elem.column, "missing set element UPDATE prepare");
                    continue;
                };
                (prep, bind_set_element_update(elem, &keys, preserve))
            }
            CollectionKind::List => continue,
        };
        retry
            .run(|| {
                let session = session;
                let prepared = prepared.clone();
                let values = values.clone();
                async move {
                    session
                        .inner()
                        .execute_unpaged(&prepared, values)
                        .await
                        .map_err(|err| err.to_string())?;
                    Ok::<(), String>(())
                }
            })
            .await?;
    }
    Ok(())
}

/// Send one UNLOGGED batch; on "Batch too large", split until single-row inserts.
async fn write_chunk_adaptive(
    session: Arc<CqlSession>,
    prepared: PreparedStatement,
    retry: RetryPolicy,
    preserve: PreserveOptions,
    consistency: scylla::statement::Consistency,
    rows: Vec<MigratedRow>,
) -> Result<(Duration, u32), EngineError> {
    if rows.is_empty() {
        return Ok((Duration::ZERO, 0));
    }
    if rows.len() == 1 {
        let values = bind_preserved_insert(&rows[0], preserve);
        return retry
            .run(|| {
                let session = session.clone();
                let prepared = prepared.clone();
                let values = values.clone();
                async move {
                    let started = Instant::now();
                    session
                        .inner()
                        .execute_unpaged(&prepared, values)
                        .await
                        .map_err(|err| err.to_string())?;
                    Ok::<Duration, String>(started.elapsed())
                }
            })
            .await;
    }

    let values: Vec<Vec<Option<CqlValue>>> = rows
        .iter()
        .map(|row| bind_preserved_insert(row, preserve))
        .collect();

    let started = Instant::now();
    let mut cql_batch = Batch::new(BatchType::Unlogged);
    cql_batch.set_consistency(consistency);
    for _ in 0..values.len() {
        cql_batch.append_statement(prepared.clone());
    }

    match session.inner().batch(&cql_batch, values).await {
        Ok(_) => Ok((started.elapsed(), 0)),
        Err(err) => {
            let msg = err.to_string();
            if is_batch_too_large(&msg) {
                let mid = rows.len() / 2;
                warn!(
                    rows = rows.len(),
                    split_into = mid,
                    "UNLOGGED batch rejected as too large; splitting"
                );
                let (left_lat, left_retries) = Box::pin(write_chunk_adaptive(
                    Arc::clone(&session),
                    prepared.clone(),
                    retry.clone(),
                    preserve,
                    consistency,
                    rows[..mid].to_vec(),
                ))
                .await?;
                let (right_lat, right_retries) = Box::pin(write_chunk_adaptive(
                    session,
                    prepared,
                    retry,
                    preserve,
                    consistency,
                    rows[mid..].to_vec(),
                ))
                .await?;
                Ok((
                    left_lat + right_lat,
                    left_retries.saturating_add(right_retries).saturating_add(1),
                ))
            } else {
                retry
                    .run(|| {
                        let session = session.clone();
                        let prepared = prepared.clone();
                        let rows = rows.clone();
                        async move {
                            let values: Vec<Vec<Option<CqlValue>>> = rows
                                .iter()
                                .map(|row| bind_preserved_insert(row, preserve))
                                .collect();
                            let mut cql_batch = Batch::new(BatchType::Unlogged);
                            cql_batch.set_consistency(consistency);
                            for _ in 0..values.len() {
                                cql_batch.append_statement(prepared.clone());
                            }
                            let started = Instant::now();
                            session
                                .inner()
                                .batch(&cql_batch, values)
                                .await
                                .map_err(|err| err.to_string())?;
                            Ok::<Duration, String>(started.elapsed())
                        }
                    })
                    .await
            }
        }
    }
}

fn is_batch_too_large(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("batch too large")
}

async fn collect_write_results(
    results: Vec<Result<(Duration, u32), EngineError>>,
    throttle: &Mutex<AdaptiveConcurrency>,
    concurrency: usize,
) -> Result<WriteBatchStats, EngineError> {
    let mut first_error: Option<EngineError> = None;
    let mut latencies = Vec::with_capacity(results.len());
    let mut retries = 0u64;
    for result in results {
        match result {
            Ok((latency, row_retries)) => {
                throttle.lock().await.record_sample(latency);
                latencies.push(latency);
                retries = retries.saturating_add(u64::from(row_retries));
                trace!(?latency, concurrency, "write chunk ok");
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(WriteBatchStats {
        latencies,
        retries,
        concurrency,
    })
}

/// Outcome of writing one page.
#[derive(Debug, Clone, Default)]
pub struct WriteBatchStats {
    pub latencies: Vec<Duration>,
    pub retries: u64,
    pub concurrency: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_cover_all_rows() {
        let rows: Vec<u32> = (0..120).collect();
        let size = 50;
        let chunks: Vec<Vec<u32>> = rows.chunks(size).map(|c| c.to_vec()).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 50);
        assert_eq!(chunks[1].len(), 50);
        assert_eq!(chunks[2].len(), 20);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 120);
    }

    #[test]
    fn detects_batch_too_large_message() {
        assert!(is_batch_too_large(
            "Database returned an error: The query is syntactically correct but invalid, Error message: Batch too large"
        ));
        assert!(!is_batch_too_large("Unavailable"));
    }
}
