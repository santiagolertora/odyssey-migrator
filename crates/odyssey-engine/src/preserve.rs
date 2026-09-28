//! Level-A cell metadata preservation (max WRITETIME / TTL per row) plus
//! optional Level-B per-element meta for unfrozen map/set columns.

use odyssey_cql::{CollectionKind, TableSchema, collection_kind};
use scylla::value::{CqlValue, Row};

/// Which metadata Odyssey should read from the source and bind on INSERT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PreserveOptions {
    pub writetimes: bool,
    pub ttls: bool,
    pub frozen_collections: bool,
    /// Per-element WRITETIME/TTL for unfrozen maps/sets (lists stay Level-A).
    pub collection_elements: bool,
}

impl PreserveOptions {
    pub fn from_engine(
        preserve_writetimes: bool,
        preserve_ttls: bool,
        preserve_frozen_collections: bool,
        preserve_collection_elements: bool,
    ) -> Self {
        Self {
            writetimes: preserve_writetimes,
            ttls: preserve_ttls,
            frozen_collections: preserve_frozen_collections,
            collection_elements: preserve_collection_elements,
        }
    }

    pub fn any(self) -> bool {
        self.writetimes || self.ttls || self.collection_elements
    }

    /// Level-A SELECT/INSERT USING flags (not collection element overlays).
    pub fn level_a(self) -> bool {
        self.writetimes || self.ttls
    }
}

/// One unfrozen map/set element with optional per-cell metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionElementMeta {
    pub column: String,
    pub kind: CollectionKind,
    /// Map key or set element.
    pub key_or_elem: CqlValue,
    /// Map value (None for sets).
    pub value: Option<CqlValue>,
    pub writetime_us: Option<i64>,
    pub ttl_secs: Option<i32>,
}

/// One source row ready for the target writer.
#[derive(Debug, Clone, PartialEq)]
pub struct MigratedRow {
    /// Values in [`TableSchema::ordered_columns`] order only (no metadata cols).
    pub values: Vec<Option<CqlValue>>,
    /// Max `writetime(c)` across preservable columns (microseconds), if requested.
    pub writetime_us: Option<i64>,
    /// Max remaining `ttl(c)` across preservable columns (seconds), if requested.
    pub ttl_secs: Option<i32>,
    /// Per-element overlays when `preserve_collection_elements` is on.
    pub collection_elements: Vec<CollectionElementMeta>,
}

/// Split a dynamic SELECT row into data values + aggregated metadata.
///
/// Expected column layout from [`TableSchema::range_select_cql_preserving`]:
/// `ordered_columns…`, then optional `writetime` per preservable col, then
/// optional `ttl` per preservable col.
pub fn split_preserved_row(
    schema: &TableSchema,
    row: Row,
    opts: PreserveOptions,
) -> Result<MigratedRow, String> {
    let data_len = schema.ordered_columns().len();
    let meta_len = schema
        .preservable_value_columns(opts.frozen_collections)
        .len();
    let expected = data_len
        + if opts.writetimes { meta_len } else { 0 }
        + if opts.ttls { meta_len } else { 0 };

    if row.columns.len() != expected {
        return Err(format!(
            "row has {} columns, expected {expected} (data={data_len}, meta_cols={meta_len}, preserve={opts:?})",
            row.columns.len()
        ));
    }

    let mut cols = row.columns;
    let mut rest = cols.split_off(data_len);
    let values = cols;

    let mut writetime_us = None;
    if opts.writetimes {
        let wt_cols: Vec<_> = rest.drain(..meta_len).collect();
        writetime_us = max_i64_cells(&wt_cols);
    }

    let mut ttl_secs = None;
    if opts.ttls {
        let ttl_cols: Vec<_> = rest.drain(..meta_len).collect();
        ttl_secs = max_i32_cells(&ttl_cols).or(Some(0));
    }

    if !rest.is_empty() {
        return Err(format!(
            "internal preserve split left {} leftover columns",
            rest.len()
        ));
    }

    let collection_elements = if opts.collection_elements {
        extract_collection_elements(schema, &values)
    } else {
        Vec::new()
    };

    Ok(MigratedRow {
        values,
        writetime_us,
        ttl_secs,
        collection_elements,
    })
}

/// Build skeleton element metas from map/set cell values (meta filled later).
pub fn extract_collection_elements(
    schema: &TableSchema,
    values: &[Option<CqlValue>],
) -> Vec<CollectionElementMeta> {
    let ordered = schema.ordered_columns();
    let mut out = Vec::new();
    for (idx, col) in ordered.iter().enumerate() {
        let Some(kind) = collection_kind(&col.type_name) else {
            continue;
        };
        if !matches!(kind, CollectionKind::Map | CollectionKind::Set) {
            continue;
        }
        let Some(Some(cell)) = values.get(idx) else {
            continue;
        };
        match (kind, cell) {
            (CollectionKind::Map, CqlValue::Map(entries)) => {
                for (k, v) in entries {
                    out.push(CollectionElementMeta {
                        column: col.name.clone(),
                        kind: CollectionKind::Map,
                        key_or_elem: k.clone(),
                        value: Some(v.clone()),
                        writetime_us: None,
                        ttl_secs: None,
                    });
                }
            }
            (CollectionKind::Set, CqlValue::Set(elems)) => {
                for e in elems {
                    out.push(CollectionElementMeta {
                        column: col.name.clone(),
                        kind: CollectionKind::Set,
                        key_or_elem: e.clone(),
                        value: None,
                        writetime_us: None,
                        ttl_secs: None,
                    });
                }
            }
            _ => {}
        }
    }
    out
}

fn max_i64_cells(cells: &[Option<CqlValue>]) -> Option<i64> {
    let mut max = None;
    for cell in cells {
        if let Some(CqlValue::BigInt(v)) = cell {
            max = Some(max.map_or(*v, |m: i64| m.max(*v)));
        }
    }
    max
}

fn max_i32_cells(cells: &[Option<CqlValue>]) -> Option<i32> {
    let mut max = None;
    for cell in cells {
        // TTL() returns int in Cassandra/Scylla.
        match cell {
            Some(CqlValue::Int(v)) => {
                max = Some(max.map_or(*v, |m: i32| m.max(*v)));
            }
            Some(CqlValue::BigInt(v)) => {
                let as_i32 = i32::try_from(*v).unwrap_or(i32::MAX);
                max = Some(max.map_or(as_i32, |m: i32| m.max(as_i32)));
            }
            _ => {}
        }
    }
    max
}

/// Parse `writetime` / `ttl` cells from an element meta SELECT (two columns).
pub fn parse_element_meta_cells(
    cells: &[Option<CqlValue>],
) -> (Option<i64>, Option<i32>) {
    let wt = cells.first().and_then(|c| match c {
        Some(CqlValue::BigInt(v)) => Some(*v),
        _ => None,
    });
    let ttl = cells.get(1).and_then(|c| match c {
        Some(CqlValue::Int(v)) => Some(*v),
        Some(CqlValue::BigInt(v)) => i32::try_from(*v).ok(),
        _ => None,
    });
    (wt, ttl)
}

/// Build bind values for a preserving INSERT (data + optional TIMESTAMP + TTL).
pub fn bind_preserved_insert(row: &MigratedRow, opts: PreserveOptions) -> Vec<Option<CqlValue>> {
    let mut out = row.values.clone();
    if opts.writetimes {
        let ts = row.writetime_us.unwrap_or(0);
        out.push(Some(CqlValue::BigInt(ts)));
    }
    if opts.ttls {
        let ttl = row.ttl_secs.unwrap_or(0);
        out.push(Some(CqlValue::Int(ttl)));
    }
    out
}

/// Bind values for a map element UPDATE with optional USING TIMESTAMP/TTL.
pub fn bind_map_element_update(
    elem: &CollectionElementMeta,
    key_values: &[Option<CqlValue>],
    opts: PreserveOptions,
) -> Vec<Option<CqlValue>> {
    let mut out = Vec::new();
    if opts.writetimes {
        out.push(Some(CqlValue::BigInt(elem.writetime_us.unwrap_or(0))));
    }
    if opts.ttls {
        out.push(Some(CqlValue::Int(elem.ttl_secs.unwrap_or(0))));
    }
    out.push(Some(elem.key_or_elem.clone()));
    out.push(elem.value.clone());
    out.extend(key_values.iter().cloned());
    out
}

/// Bind values for a set element UPDATE (`SET s = s + ?`).
pub fn bind_set_element_update(
    elem: &CollectionElementMeta,
    key_values: &[Option<CqlValue>],
    opts: PreserveOptions,
) -> Vec<Option<CqlValue>> {
    let mut out = Vec::new();
    if opts.writetimes {
        out.push(Some(CqlValue::BigInt(elem.writetime_us.unwrap_or(0))));
    }
    if opts.ttls {
        out.push(Some(CqlValue::Int(elem.ttl_secs.unwrap_or(0))));
    }
    out.push(Some(CqlValue::Set(vec![elem.key_or_elem.clone()])));
    out.extend(key_values.iter().cloned());
    out
}

/// Primary-key bind values from a migrated row (partition + clustering order).
pub fn row_primary_key_values(
    schema: &TableSchema,
    row: &MigratedRow,
) -> Result<Vec<Option<CqlValue>>, String> {
    let ordered = schema.ordered_columns();
    let mut out = Vec::new();
    for key in schema
        .partition_key_columns()
        .into_iter()
        .chain(schema.clustering_columns())
    {
        let idx = ordered
            .iter()
            .position(|c| c.name == key.name)
            .ok_or_else(|| format!("missing key column `{}` in ordered columns", key.name))?;
        out.push(row.values.get(idx).cloned().unwrap_or(None));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_cql::{ColumnInfo, ColumnKind, TableSchema};

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            columns: vec![
                ColumnInfo {
                    name: "pk".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 0,
                },
                ColumnInfo {
                    name: "v".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
                ColumnInfo {
                    name: "tags".into(),
                    type_name: "set<text>".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        }
    }

    #[test]
    fn preservable_skips_collections() {
        let s = schema();
        let names: Vec<_> = s
            .preservable_value_columns(false)
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["v"]);
    }

    #[test]
    fn split_max_writetime_and_ttl() {
        let s = schema();
        let opts = PreserveOptions {
            writetimes: true,
            ttls: true,
            frozen_collections: false,
            collection_elements: false,
        };
        // ordered: pk, tags, v
        let row = Row {
            columns: vec![
                Some(CqlValue::Text("a".into())),
                Some(CqlValue::Set(vec![CqlValue::Text("x".into())])),
                Some(CqlValue::Text("hello".into())),
                // writetime only for preservable `v`
                Some(CqlValue::BigInt(1_700_000_000_000_000)),
                // ttl for `v`
                Some(CqlValue::Int(3600)),
            ],
        };
        let migrated = split_preserved_row(&s, row, opts).unwrap();
        assert_eq!(migrated.values.len(), 3);
        assert_eq!(migrated.writetime_us, Some(1_700_000_000_000_000));
        assert_eq!(migrated.ttl_secs, Some(3600));
        assert!(migrated.collection_elements.is_empty());
        let binds = bind_preserved_insert(&migrated, opts);
        assert_eq!(binds.len(), 5);
    }

    #[test]
    fn extract_set_and_map_elements() {
        let s = TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            columns: vec![
                ColumnInfo {
                    name: "pk".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 0,
                },
                ColumnInfo {
                    name: "attrs".into(),
                    type_name: "map<text, int>".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
                ColumnInfo {
                    name: "tags".into(),
                    type_name: "set<text>".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        };
        let values = vec![
            Some(CqlValue::Text("a".into())),
            Some(CqlValue::Map(vec![(
                CqlValue::Text("k".into()),
                CqlValue::Int(1),
            )])),
            Some(CqlValue::Set(vec![CqlValue::Text("x".into())])),
        ];
        let elems = extract_collection_elements(&s, &values);
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].column, "attrs");
        assert_eq!(elems[0].kind, CollectionKind::Map);
        assert_eq!(elems[1].column, "tags");
        assert_eq!(elems[1].kind, CollectionKind::Set);
    }
}
