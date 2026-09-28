//! Pure planning of how a CDC operation becomes target CQL.
//!
//! Kept free of I/O so unit tests can lock the safety rules: we never silently
//! drop deletes, and unsupported ops fail closed.

use odyssey_cql::TableSchema;
use scylla::value::CqlValue;
use scylla_cdc::consumer::OperationType;

use crate::CdcError;

/// What the catch-up worker should execute on the target.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyPlan {
    /// Upsert columns that are present (INSERT of PK + provided cells).
    Upsert {
        cql: String,
        values: Vec<Option<CqlValue>>,
    },
    /// Delete one row (partition key + clustering).
    DeleteRow {
        cql: String,
        values: Vec<Option<CqlValue>>,
    },
    /// Delete an entire partition.
    DeletePartition {
        cql: String,
        values: Vec<Option<CqlValue>>,
    },
    /// Delete specific non-key columns on a row (`DELETE col FROM … WHERE pk/ck`).
    DeleteColumns {
        cql: String,
        values: Vec<Option<CqlValue>>,
    },
    /// No target mutation (PreImage is informational only).
    Ignore,
}

/// Column values taken from a CDC row for planning (name → value).
#[derive(Debug, Clone)]
pub struct CdcColumnValue {
    pub name: String,
    pub value: Option<CqlValue>,
    pub deleted: bool,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn pk_ck_names(schema: &TableSchema) -> (Vec<String>, Vec<String>) {
    let pk = schema
        .partition_key_columns()
        .into_iter()
        .map(|c| c.name.clone())
        .collect();
    let ck = schema
        .clustering_columns()
        .into_iter()
        .map(|c| c.name.clone())
        .collect();
    (pk, ck)
}

fn require_keys(
    names: &[String],
    columns: &[CdcColumnValue],
) -> Result<Vec<Option<CqlValue>>, CdcError> {
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let found = columns.iter().find(|c| c.name == *name);
        match found {
            Some(col) if col.value.is_some() => out.push(col.value.clone()),
            _ => {
                return Err(CdcError::MissingPrimaryKey {
                    column: name.clone(),
                });
            }
        }
    }
    Ok(out)
}

/// Build one or more apply plans from operation type + CDC column payload.
///
/// Upserts may emit a follow-up [`ApplyPlan::DeleteColumns`] when cells are
/// marked deleted. Range deletes become a single DELETE with CK bounds.
pub fn plan_apply(
    schema: &TableSchema,
    operation: OperationType,
    columns: &[CdcColumnValue],
) -> Result<Vec<ApplyPlan>, CdcError> {
    let (pk, ck) = pk_ck_names(schema);
    let target_ks = quote_ident(&schema.keyspace);
    let target_table = quote_ident(&schema.table);

    match operation {
        OperationType::PreImage => Ok(vec![ApplyPlan::Ignore]),

        OperationType::RowInsert | OperationType::RowUpdate | OperationType::PostImage => {
            let mut names: Vec<String> = Vec::new();
            let mut values: Vec<Option<CqlValue>> = Vec::new();
            let mut deleted_cols: Vec<String> = Vec::new();

            for name in pk.iter().chain(ck.iter()) {
                let v = require_keys(std::slice::from_ref(name), columns)?;
                names.push(name.clone());
                values.push(v[0].clone());
            }

            for col in columns {
                let is_key = pk.contains(&col.name) || ck.contains(&col.name);
                if is_key {
                    continue;
                }
                if !schema.columns.iter().any(|c| c.name == col.name) {
                    continue;
                }
                if col.deleted {
                    deleted_cols.push(col.name.clone());
                    continue;
                }
                if col.value.is_some() {
                    names.push(col.name.clone());
                    values.push(col.value.clone());
                }
            }

            let mut plans = Vec::new();
            // Upsert whenever there are non-key cells to write, or when there are
            // no column deletes (key-only insert / full PostImage).
            let has_data_cells = names.len() > pk.len() + ck.len();
            if has_data_cells || deleted_cols.is_empty() {
                let cols_sql = names
                    .iter()
                    .map(|n| quote_ident(n))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = std::iter::repeat_n("?", names.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let cql = format!(
                    "INSERT INTO {target_ks}.{target_table} ({cols_sql}) VALUES ({placeholders})"
                );
                plans.push(ApplyPlan::Upsert {
                    cql,
                    values: values.clone(),
                });
            }

            if !deleted_cols.is_empty() {
                let mut key_names = pk.clone();
                key_names.extend(ck.clone());
                let key_values = require_keys(&key_names, columns)?;
                let cols_sql = deleted_cols
                    .iter()
                    .map(|n| quote_ident(n))
                    .collect::<Vec<_>>()
                    .join(", ");
                let preds = key_names
                    .iter()
                    .map(|n| format!("{} = ?", quote_ident(n)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let cql =
                    format!("DELETE {cols_sql} FROM {target_ks}.{target_table} WHERE {preds}");
                plans.push(ApplyPlan::DeleteColumns {
                    cql,
                    values: key_values,
                });
            }

            if plans.is_empty() {
                plans.push(ApplyPlan::Ignore);
            }
            Ok(plans)
        }

        OperationType::RowDelete => {
            let mut key_names = pk.clone();
            key_names.extend(ck.clone());
            let values = require_keys(&key_names, columns)?;
            let preds = key_names
                .iter()
                .map(|n| format!("{} = ?", quote_ident(n)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let cql = format!("DELETE FROM {target_ks}.{target_table} WHERE {preds}");
            Ok(vec![ApplyPlan::DeleteRow { cql, values }])
        }

        OperationType::PartitionDelete => {
            let values = require_keys(&pk, columns)?;
            let preds = pk
                .iter()
                .map(|n| format!("{} = ?", quote_ident(n)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let cql = format!("DELETE FROM {target_ks}.{target_table} WHERE {preds}");
            Ok(vec![ApplyPlan::DeletePartition { cql, values }])
        }

        OperationType::RowRangeDelInclLeft
        | OperationType::RowRangeDelExclLeft
        | OperationType::RowRangeDelInclRight
        | OperationType::RowRangeDelExclRight => {
            plan_range_delete(
                &target_ks,
                &target_table,
                &pk,
                &ck,
                columns,
                operation,
            )
        }
    }
}

fn plan_range_delete(
    target_ks: &str,
    target_table: &str,
    pk: &[String],
    ck: &[String],
    columns: &[CdcColumnValue],
    operation: OperationType,
) -> Result<Vec<ApplyPlan>, CdcError> {
    if ck.is_empty() {
        return Err(CdcError::UnsupportedOperation {
            op: format!("{operation} (table has no clustering key)"),
        });
    }

    // Scylla CDC encodes composite CK range deletes with:
    // - equality prefix = all non-null CK values except the last
    // - range column = last non-null CK (inequality from op)
    // - trailing CKs after the range column are null
    // - all CK null = open-ended marker for the other half of the batch → Ignore
    let ck_values: Vec<(String, Option<CqlValue>)> = ck
        .iter()
        .map(|name| {
            let value = columns
                .iter()
                .find(|c| c.name == *name)
                .and_then(|c| c.value.clone());
            (name.clone(), value)
        })
        .collect();

    let present: Vec<usize> = ck_values
        .iter()
        .enumerate()
        .filter_map(|(i, (_, v))| v.as_ref().map(|_| i))
        .collect();
    if present.is_empty() {
        return Ok(vec![ApplyPlan::Ignore]);
    }

    let range_idx = *present.last().expect("present non-empty");
    let prefix = &present[..present.len() - 1];
    let range_name = &ck_values[range_idx].0;
    let range_value = ck_values[range_idx]
        .1
        .clone()
        .expect("present index has value");

    let ineq = match operation {
        OperationType::RowRangeDelInclLeft => ">=",
        OperationType::RowRangeDelExclLeft => ">",
        OperationType::RowRangeDelInclRight => "<=",
        OperationType::RowRangeDelExclRight => "<",
        _ => unreachable!(),
    };

    let pk_values = require_keys(pk, columns)?;
    let mut preds: Vec<String> = pk
        .iter()
        .map(|n| format!("{} = ?", quote_ident(n)))
        .collect();
    let mut values = pk_values;
    for &idx in prefix {
        let (name, value) = &ck_values[idx];
        preds.push(format!("{} = ?", quote_ident(name)));
        values.push(value.clone());
    }
    preds.push(format!("{} {} ?", quote_ident(range_name), ineq));
    values.push(Some(range_value));

    let pred_sql = preds.join(" AND ");
    let cql = format!("DELETE FROM {target_ks}.{target_table} WHERE {pred_sql}");
    Ok(vec![ApplyPlan::DeleteRow { cql, values }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_cql::{ColumnInfo, ColumnKind};

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
                    name: "ck".into(),
                    type_name: "int".into(),
                    kind: ColumnKind::Clustering,
                    position: 0,
                },
                ColumnInfo {
                    name: "v".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        }
    }

    #[test]
    fn insert_plans_upsert() {
        let cols = vec![
            CdcColumnValue {
                name: "pk".into(),
                value: Some(CqlValue::Text("a".into())),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck".into(),
                value: Some(CqlValue::Int(1)),
                deleted: false,
            },
            CdcColumnValue {
                name: "v".into(),
                value: Some(CqlValue::Text("x".into())),
                deleted: false,
            },
        ];
        let plans = plan_apply(&schema(), OperationType::RowInsert, &cols).unwrap();
        assert_eq!(plans.len(), 1);
        match &plans[0] {
            ApplyPlan::Upsert { cql, values } => {
                assert!(cql.contains("INSERT INTO"));
                assert_eq!(values.len(), 3);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn cell_delete_emits_delete_columns() {
        let cols = vec![
            CdcColumnValue {
                name: "pk".into(),
                value: Some(CqlValue::Text("a".into())),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck".into(),
                value: Some(CqlValue::Int(1)),
                deleted: false,
            },
            CdcColumnValue {
                name: "v".into(),
                value: None,
                deleted: true,
            },
        ];
        let plans = plan_apply(&schema(), OperationType::RowUpdate, &cols).unwrap();
        assert!(
            plans
                .iter()
                .any(|p| matches!(p, ApplyPlan::DeleteColumns { .. })),
            "plans={plans:?}"
        );
        let del = plans
            .iter()
            .find_map(|p| match p {
                ApplyPlan::DeleteColumns { cql, .. } => Some(cql.as_str()),
                _ => None,
            })
            .unwrap();
        assert!(del.contains("DELETE \"v\" FROM"));
    }

    #[test]
    fn row_delete_plans_delete() {
        let cols = vec![
            CdcColumnValue {
                name: "pk".into(),
                value: Some(CqlValue::Text("a".into())),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck".into(),
                value: Some(CqlValue::Int(1)),
                deleted: false,
            },
        ];
        let plans = plan_apply(&schema(), OperationType::RowDelete, &cols).unwrap();
        match &plans[0] {
            ApplyPlan::DeleteRow { cql, .. } => {
                assert!(cql.starts_with("DELETE FROM"));
                assert!(cql.contains("\"pk\" = ?"));
                assert!(cql.contains("\"ck\" = ?"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn range_delete_plans_bounded_delete() {
        let cols = vec![
            CdcColumnValue {
                name: "pk".into(),
                value: Some(CqlValue::Text("a".into())),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck".into(),
                value: Some(CqlValue::Int(10)),
                deleted: false,
            },
        ];
        let plans = plan_apply(&schema(), OperationType::RowRangeDelInclLeft, &cols).unwrap();
        match &plans[0] {
            ApplyPlan::DeleteRow { cql, values } => {
                assert!(cql.contains("\"ck\" >= ?"));
                assert_eq!(values.len(), 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    fn composite_schema() -> TableSchema {
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
                    name: "ck1".into(),
                    type_name: "int".into(),
                    kind: ColumnKind::Clustering,
                    position: 0,
                },
                ColumnInfo {
                    name: "ck2".into(),
                    type_name: "int".into(),
                    kind: ColumnKind::Clustering,
                    position: 1,
                },
                ColumnInfo {
                    name: "ck3".into(),
                    type_name: "int".into(),
                    kind: ColumnKind::Clustering,
                    position: 2,
                },
            ],
        }
    }

    #[test]
    fn composite_range_delete_uses_prefix_eq_and_last_ineq() {
        let cols = vec![
            CdcColumnValue {
                name: "pk".into(),
                value: Some(CqlValue::Text("a".into())),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck1".into(),
                value: Some(CqlValue::Int(0)),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck2".into(),
                value: Some(CqlValue::Int(5)),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck3".into(),
                value: None,
                deleted: false,
            },
        ];
        let plans =
            plan_apply(&composite_schema(), OperationType::RowRangeDelExclLeft, &cols).unwrap();
        match &plans[0] {
            ApplyPlan::DeleteRow { cql, values } => {
                assert!(cql.contains("\"ck1\" = ?"));
                assert!(cql.contains("\"ck2\" > ?"));
                assert!(!cql.contains("\"ck3\""));
                assert_eq!(values.len(), 3);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn open_ended_range_marker_is_ignored() {
        let cols = vec![
            CdcColumnValue {
                name: "pk".into(),
                value: Some(CqlValue::Text("a".into())),
                deleted: false,
            },
            CdcColumnValue {
                name: "ck1".into(),
                value: None,
                deleted: false,
            },
            CdcColumnValue {
                name: "ck2".into(),
                value: None,
                deleted: false,
            },
            CdcColumnValue {
                name: "ck3".into(),
                value: None,
                deleted: false,
            },
        ];
        let plans =
            plan_apply(&composite_schema(), OperationType::RowRangeDelInclLeft, &cols).unwrap();
        assert_eq!(plans, vec![ApplyPlan::Ignore]);
    }

    #[test]
    fn preimage_ignored() {
        assert_eq!(
            plan_apply(&schema(), OperationType::PreImage, &[]).unwrap(),
            vec![ApplyPlan::Ignore]
        );
    }

    #[test]
    fn missing_pk_fails() {
        let err = plan_apply(
            &schema(),
            OperationType::RowDelete,
            &[CdcColumnValue {
                name: "ck".into(),
                value: Some(CqlValue::Int(1)),
                deleted: false,
            }],
        )
        .unwrap_err();
        assert!(matches!(err, CdcError::MissingPrimaryKey { .. }));
    }
}
