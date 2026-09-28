use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use odyssey_cql::{CqlSession, TableSchema, to_driver_consistency};
use odyssey_core::ConsistencyLevel;
use odyssey_types::TokenRange;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::value::{CqlValue, Row};
use tracing::{debug, warn};

use crate::batch::RowBatch;
use crate::preserve::{
    MigratedRow, PreserveOptions, parse_element_meta_cells, row_primary_key_values,
    split_preserved_row,
};
use crate::statement::{
    build_collection_element_meta_select, build_range_select_cql_preserving,
};
use crate::EngineError;

/// Reads one token-range page at a time from the source cluster.
pub struct RangeReader {
    session: Arc<CqlSession>,
    prepared: PreparedStatement,
    schema: TableSchema,
    preserve: PreserveOptions,
    range: TokenRange,
    paging_state: PagingState,
    finished: bool,
    /// Prepared `SELECT writetime(col[?]), ttl(col[?]) …` keyed by column name.
    element_meta: HashMap<String, PreparedStatement>,
}

impl RangeReader {
    pub async fn prepare(
        session: Arc<CqlSession>,
        schema: &TableSchema,
        range: TokenRange,
        page_size: u32,
        resume_paging: Option<Vec<u8>>,
        consistency: ConsistencyLevel,
        preserve: PreserveOptions,
    ) -> Result<Self, EngineError> {
        if page_size == 0 {
            return Err(EngineError::InvalidConfig(
                "engine.page_size must be >= 1".into(),
            ));
        }
        if preserve.collection_elements && !preserve.writetimes && !preserve.ttls {
            return Err(EngineError::InvalidConfig(
                "preserve_collection_elements requires preserve_writetimes and/or preserve_ttls"
                    .into(),
            ));
        }

        let cql = build_range_select_cql_preserving(schema, preserve)?;
        let mut statement = Statement::new(cql).with_page_size(page_size as i32);
        statement.set_consistency(to_driver_consistency(consistency));
        let prepared = session
            .inner()
            .prepare(statement)
            .await
            .map_err(|err| EngineError::Prepare(err.to_string()))?;

        let mut element_meta = HashMap::new();
        if preserve.collection_elements {
            let mut cols: Vec<String> = schema
                .unfrozen_map_columns()
                .into_iter()
                .chain(schema.unfrozen_set_columns())
                .map(|c| c.name.clone())
                .collect();
            cols.sort();
            cols.dedup();
            for col in cols {
                let meta_cql = build_collection_element_meta_select(schema, &col)?;
                let mut st = Statement::new(meta_cql);
                st.set_consistency(to_driver_consistency(consistency));
                let prep = session
                    .inner()
                    .prepare(st)
                    .await
                    .map_err(|err| {
                        EngineError::Prepare(format!(
                            "element WRITETIME/TTL for `{col}` failed (cluster may lack per-element meta): {err}"
                        ))
                    })?;
                element_meta.insert(col, prep);
            }
        }

        let paging_state = match resume_paging {
            Some(bytes) => PagingState::new_from_raw_bytes(bytes),
            None => PagingState::start(),
        };

        Ok(Self {
            session,
            prepared,
            schema: schema.clone(),
            preserve,
            range,
            paging_state,
            finished: false,
            element_meta,
        })
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Fetch the next page. Returns `None` when the range is exhausted.
    pub async fn next_page(&mut self) -> Result<Option<PageRead>, EngineError> {
        if self.finished {
            return Ok(None);
        }

        let started = Instant::now();
        let (result, paging_response) = self
            .session
            .inner()
            .execute_single_page(
                &self.prepared,
                (self.range.start, self.range.end),
                self.paging_state.clone(),
            )
            .await
            .map_err(|err| EngineError::Read(err.to_string()))?;
        let read_latency = started.elapsed();

        let rows_result = result
            .into_rows_result()
            .map_err(|err| EngineError::Read(err.to_string()))?;

        let mut migrated = Vec::with_capacity(rows_result.rows_num());
        let mut last_token: Option<i64> = None;
        for row in rows_result
            .rows::<Row>()
            .map_err(|err| EngineError::Read(err.to_string()))?
        {
            let mut row = row.map_err(|err| EngineError::Read(err.to_string()))?;
            if let Some(token_cell) = row.columns.pop() {
                match token_cell {
                    Some(CqlValue::BigInt(t)) => last_token = Some(t),
                    Some(CqlValue::Int(t)) => last_token = Some(i64::from(t)),
                    other => {
                        return Err(EngineError::Read(format!(
                            "expected trailing token(pk) BigInt on SELECT row, got {other:?}"
                        )));
                    }
                }
            } else {
                return Err(EngineError::Read(
                    "SELECT row missing trailing token(pk) column".into(),
                ));
            }
            let mut row = split_preserved_row(&self.schema, row, self.preserve)
                .map_err(EngineError::Read)?;
            self.enrich_collection_elements(&mut row).await?;
            migrated.push(row);
        }

        let batch = RowBatch::from_migrated(migrated);
        let checkpoint_paging = match &paging_response {
            PagingStateResponse::HasMorePages { state } => {
                state.as_bytes_slice().map(|arc| arc.as_ref().to_vec())
            }
            PagingStateResponse::NoMorePages => None,
        };

        match paging_response.into_paging_control_flow() {
            ControlFlow::Continue(next) => {
                self.paging_state = next;
            }
            ControlFlow::Break(()) => {
                self.finished = true;
            }
        }

        debug!(
            rows = batch.rows_count,
            bytes = batch.bytes_estimate,
            last_token = ?last_token,
            finished = self.finished,
            preserve_wt = self.preserve.writetimes,
            preserve_ttl = self.preserve.ttls,
            preserve_collection_elements = self.preserve.collection_elements,
            "read source page"
        );

        Ok(Some(PageRead {
            batch,
            checkpoint_paging_state: checkpoint_paging,
            no_more_pages: self.finished,
            read_latency,
            last_token,
        }))
    }

    async fn enrich_collection_elements(
        &self,
        row: &mut MigratedRow,
    ) -> Result<(), EngineError> {
        if !self.preserve.collection_elements || row.collection_elements.is_empty() {
            return Ok(());
        }
        let keys = row_primary_key_values(&self.schema, row).map_err(EngineError::Read)?;
        for elem in &mut row.collection_elements {
            let Some(prepared) = self.element_meta.get(&elem.column) else {
                warn!(
                    column = %elem.column,
                    "no element-meta prepared statement; leaving Level-A collection meta"
                );
                continue;
            };
            let mut binds = vec![
                Some(elem.key_or_elem.clone()),
                Some(elem.key_or_elem.clone()),
            ];
            binds.extend(keys.iter().cloned());
            let result = self
                .session
                .inner()
                .execute_unpaged(prepared, binds)
                .await
                .map_err(|err| EngineError::Read(err.to_string()))?;
            let rows = result
                .into_rows_result()
                .map_err(|err| EngineError::Read(err.to_string()))?;
            let mut iter = rows
                .rows::<Row>()
                .map_err(|err| EngineError::Read(err.to_string()))?;
            if let Some(Ok(meta_row)) = iter.next() {
                let (wt, ttl) = parse_element_meta_cells(&meta_row.columns);
                elem.writetime_us = wt.or(row.writetime_us);
                elem.ttl_secs = ttl.or(row.ttl_secs).or(Some(0));
            } else {
                elem.writetime_us = row.writetime_us;
                elem.ttl_secs = row.ttl_secs.or(Some(0));
            }
        }
        Ok(())
    }
}

/// Outcome of one source page fetch.
#[derive(Debug)]
pub struct PageRead {
    pub batch: RowBatch,
    /// Paging state to persist after the page is written. `None` when finished.
    pub checkpoint_paging_state: Option<Vec<u8>>,
    pub no_more_pages: bool,
    pub read_latency: Duration,
    /// Highest `token(pk)` in this page (for checkpoint progress %).
    pub last_token: Option<i64>,
}
