//! Async table validation against source and target clusters.

use std::collections::HashSet;
use std::sync::Arc;

use odyssey_core::ValidationMode;
use odyssey_cql::{CqlSession, TableSchema};
use odyssey_engine::{PreserveOptions, RangeReader};
use odyssey_types::TokenRange;
use tracing::{debug, info, warn};

use crate::compare::{range_digest, report_from_ranges};
use crate::digest::absorb_row;
use crate::error::ValidationError;
use crate::report::{ColumnDiff, DiffKind, RangeDigest, RowDiffSample, ValidationReport};

/// Default Sample-mode row cap per token range (V0.1: first N rows in token order).
pub const SAMPLE_PARTITIONS_PER_RANGE: u64 = 1000;

/// Max differing row digests logged when Full mode finds a mismatch (DiffSample).
const DIFF_SAMPLE_LIMIT: usize = 20;

/// Inputs for [`validate_table`].
pub struct ValidationOptions {
    pub source: Arc<CqlSession>,
    pub target: Arc<CqlSession>,
    pub source_schema: TableSchema,
    pub target_schema: TableSchema,
    pub ranges: Vec<TokenRange>,
    pub mode: ValidationMode,
    pub page_size: u32,
    pub sample_partitions_per_range: u64,
    /// Also read WRITETIME/TTL and enforce tolerances (sample: pairwise; digest: exact in hash).
    pub compare_timestamps: bool,
    pub writetime_tolerance_us: u64,
    pub ttl_tolerance_secs: u64,
}

/// Compare source vs target for every configured range.
pub async fn validate_table(opts: ValidationOptions) -> Result<ValidationReport, ValidationError> {
    if opts.page_size == 0 {
        return Err(ValidationError::InvalidOptions(
            "page_size must be >= 1".into(),
        ));
    }
    if opts.mode == ValidationMode::Sample && opts.sample_partitions_per_range == 0 {
        return Err(ValidationError::InvalidOptions(
            "sample_partitions_per_range must be >= 1 in Sample mode".into(),
        ));
    }

    let row_limit = match opts.mode {
        ValidationMode::Sample => Some(opts.sample_partitions_per_range),
        ValidationMode::Digest | ValidationMode::Full => None,
    };

    let preserve = if opts.compare_timestamps {
        PreserveOptions {
            writetimes: true,
            ttls: true,
            frozen_collections: false,
            collection_elements: false,
        }
    } else {
        PreserveOptions::default()
    };

    if opts.compare_timestamps && opts.mode == ValidationMode::Digest {
        info!(
            "compare_timestamps with digest mode embeds exact max WRITETIME/TTL in the hash \
             (tolerances apply in sample mode pairwise checks)"
        );
    }

    info!(
        ranges = opts.ranges.len(),
        mode = ?opts.mode,
        compare_timestamps = opts.compare_timestamps,
        sample_partitions_per_range = opts.sample_partitions_per_range,
        "starting table validation"
    );

    let mut results = Vec::with_capacity(opts.ranges.len());
    for range in opts.ranges {
        let result = validate_one_range(
            opts.source.clone(),
            opts.target.clone(),
            &opts.source_schema,
            &opts.target_schema,
            range,
            opts.page_size,
            row_limit,
            opts.mode,
            preserve,
            opts.compare_timestamps,
            opts.writetime_tolerance_us,
            opts.ttl_tolerance_secs,
        )
        .await?;
        results.push(result);
    }

    let report = report_from_ranges(opts.mode, results);
    if report.all_match {
        info!(
            ranges_checked = report.ranges.len(),
            mode = ?report.mode,
            "validation matched on all ranges"
        );
    } else {
        warn!(
            ranges_checked = report.ranges.len(),
            mode = ?report.mode,
            mismatched = report.ranges.iter().filter(|r| !r.matches).count(),
            "validation found mismatches"
        );
    }
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn validate_one_range(
    source: Arc<CqlSession>,
    target: Arc<CqlSession>,
    source_schema: &TableSchema,
    target_schema: &TableSchema,
    range: TokenRange,
    page_size: u32,
    row_limit: Option<u64>,
    mode: ValidationMode,
    preserve: PreserveOptions,
    compare_timestamps: bool,
    writetime_tolerance_us: u64,
    ttl_tolerance_secs: u64,
) -> Result<RangeDigest, ValidationError> {
    let ((source_side, source_meta), (target_side, target_meta)) = futures::try_join!(
        digest_side(
            source.clone(),
            source_schema,
            range,
            page_size,
            row_limit,
            preserve,
            compare_timestamps && mode != ValidationMode::Sample,
        ),
        digest_side(
            target.clone(),
            target_schema,
            range,
            page_size,
            row_limit,
            preserve,
            compare_timestamps && mode != ValidationMode::Sample,
        ),
    )?;

    let mut result = range_digest(
        range,
        source_side.0,
        target_side.0,
        source_side.1,
        target_side.1,
    );

    if compare_timestamps && mode == ValidationMode::Sample {
        if !metas_within_tolerance(
            &source_meta,
            &target_meta,
            writetime_tolerance_us,
            ttl_tolerance_secs,
        ) {
            result.matches = false;
            warn!(
                range_start = range.start,
                range_end = range.end,
                "sample validation failed WRITETIME/TTL tolerance"
            );
        }
    }

    debug!(
        range_start = range.start,
        range_end = range.end,
        source_rows = result.source_rows,
        target_rows = result.target_rows,
        matches = result.matches,
        "range validation finished"
    );

    if mode == ValidationMode::Full && !result.matches {
        match collect_column_diffs(
            source,
            target,
            source_schema,
            target_schema,
            range,
            page_size,
        )
        .await
        {
            Ok(diffs) => {
                for sample in &diffs {
                    warn!(
                        range_start = range.start,
                        range_end = range.end,
                        primary_key = %sample.primary_key,
                        kind = ?sample.kind,
                        columns = ?sample.columns,
                        "full validation DiffSample"
                    );
                }
                result.diffs = diffs;
            }
            Err(err) => {
                warn!(
                    error = %err,
                    range_start = range.start,
                    range_end = range.end,
                    "full-mode DiffSample failed after digest mismatch"
                );
            }
        }
    }

    Ok(result)
}

type SideDigest = ((String, u64), Vec<(Option<i64>, Option<i32>)>);

async fn digest_side(
    session: Arc<CqlSession>,
    schema: &TableSchema,
    range: TokenRange,
    page_size: u32,
    row_limit: Option<u64>,
    preserve: PreserveOptions,
    embed_meta_in_hash: bool,
) -> Result<SideDigest, ValidationError> {
    let mut reader = RangeReader::prepare(
        session,
        schema,
        range,
        page_size,
        None,
        odyssey_core::ConsistencyLevel::LocalQuorum,
        preserve,
    )
    .await?;
    let mut hasher = blake3::Hasher::new();
    let mut rows = 0u64;
    let mut metas = Vec::new();

    while let Some(page) = reader.next_page().await? {
        for row in &page.batch.rows {
            if let Some(limit) = row_limit {
                if rows >= limit {
                    break;
                }
            }
            absorb_row(&mut hasher, &row.values);
            if embed_meta_in_hash {
                hasher.update(&row.writetime_us.unwrap_or(0).to_le_bytes());
                hasher.update(&i64::from(row.ttl_secs.unwrap_or(0)).to_le_bytes());
            }
            metas.push((row.writetime_us, row.ttl_secs));
            rows = rows.saturating_add(1);
        }
        if row_limit.is_some_and(|limit| rows >= limit) {
            break;
        }
    }

    Ok(((hasher.finalize().to_hex().to_string(), rows), metas))
}

fn metas_within_tolerance(
    source: &[(Option<i64>, Option<i32>)],
    target: &[(Option<i64>, Option<i32>)],
    writetime_tolerance_us: u64,
    ttl_tolerance_secs: u64,
) -> bool {
    if source.len() != target.len() {
        return false;
    }
    for (s, t) in source.iter().zip(target.iter()) {
        let sw = s.0.unwrap_or(0);
        let tw = t.0.unwrap_or(0);
        let wt_delta = sw.abs_diff(tw);
        if wt_delta > writetime_tolerance_us {
            return false;
        }
        let st = i64::from(s.1.unwrap_or(0));
        let tt = i64::from(t.1.unwrap_or(0));
        let ttl_delta = st.abs_diff(tt);
        if ttl_delta > ttl_tolerance_secs {
            return false;
        }
    }
    true
}

async fn collect_column_diffs(
    source: Arc<CqlSession>,
    target: Arc<CqlSession>,
    source_schema: &TableSchema,
    target_schema: &TableSchema,
    range: TokenRange,
    page_size: u32,
) -> Result<Vec<RowDiffSample>, ValidationError> {
    let source_rows = collect_pk_rows(source, source_schema, range, page_size).await?;
    let target_rows = collect_pk_rows(target, target_schema, range, page_size).await?;

    let target_map: std::collections::HashMap<String, Vec<String>> =
        target_rows.into_iter().collect();
    let mut seen_target = HashSet::new();
    let mut diffs = Vec::new();
    let col_names: Vec<String> = source_schema
        .ordered_columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    for (pk, source_vals) in source_rows {
        match target_map.get(&pk) {
            None => {
                if diffs.len() < DIFF_SAMPLE_LIMIT {
                    diffs.push(RowDiffSample {
                        primary_key: pk,
                        columns: Vec::new(),
                        kind: DiffKind::OnlySource,
                    });
                }
            }
            Some(target_vals) => {
                seen_target.insert(pk.clone());
                if source_vals == *target_vals {
                    continue;
                }
                if diffs.len() >= DIFF_SAMPLE_LIMIT {
                    continue;
                }
                let mut columns = Vec::new();
                for (i, name) in col_names.iter().enumerate() {
                    let s = source_vals.get(i).cloned().unwrap_or_default();
                    let t = target_vals.get(i).cloned().unwrap_or_default();
                    if s != t {
                        columns.push(ColumnDiff {
                            name: name.clone(),
                            source: s,
                            target: t,
                        });
                    }
                }
                diffs.push(RowDiffSample {
                    primary_key: pk,
                    columns,
                    kind: DiffKind::ValueMismatch,
                });
            }
        }
    }

    for (pk, _) in target_map {
        if seen_target.contains(&pk) {
            continue;
        }
        if diffs.len() >= DIFF_SAMPLE_LIMIT {
            break;
        }
        diffs.push(RowDiffSample {
            primary_key: pk,
            columns: Vec::new(),
            kind: DiffKind::OnlyTarget,
        });
    }

    Ok(diffs)
}

async fn collect_pk_rows(
    session: Arc<CqlSession>,
    schema: &TableSchema,
    range: TokenRange,
    page_size: u32,
) -> Result<Vec<(String, Vec<String>)>, ValidationError> {
    let mut reader = RangeReader::prepare(
        session,
        schema,
        range,
        page_size,
        None,
        odyssey_core::ConsistencyLevel::LocalQuorum,
        PreserveOptions::default(),
    )
    .await?;
    let pk_len = schema.partition_key_columns().len()
        + schema.clustering_columns().len();
    let mut out = Vec::new();
    while let Some(page) = reader.next_page().await? {
        for row in &page.batch.rows {
            let rendered: Vec<String> = row
                .values
                .iter()
                .map(|v| format!("{v:?}"))
                .collect();
            let pk = rendered.iter().take(pk_len).cloned().collect::<Vec<_>>().join("|");
            out.push((pk, rendered));
        }
    }
    Ok(out)
}

