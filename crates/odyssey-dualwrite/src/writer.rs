use std::collections::HashMap;
use std::sync::Arc;

use odyssey_cql::{CqlSession, TableSchema, to_driver_consistency};
use odyssey_core::{ConsistencyLevel, DualWriteConfig, TableMapping};
use scylla::statement::unprepared::Statement;
use scylla::value::CqlValue;
use tracing::{info, warn};

use crate::json_cql::json_to_cql;
use crate::mutate::{Mutation, MutationOp};
use crate::DualWriteError;

struct PlannedMutation {
    cql: String,
    values: Vec<Option<CqlValue>>,
}

/// Writes the same mutation to source then target.
pub struct DualWriter {
    source: Arc<CqlSession>,
    target: Arc<CqlSession>,
    source_consistency: ConsistencyLevel,
    target_consistency: ConsistencyLevel,
    require_both: bool,
    /// source FQN (`ks.table`) → schemas
    tables: HashMap<String, TablePair>,
}

struct TablePair {
    source_schema: TableSchema,
    target_schema: TableSchema,
}

impl DualWriter {
    pub async fn new(
        source: Arc<CqlSession>,
        target: Arc<CqlSession>,
        mappings: &[TableMapping],
        source_consistency: ConsistencyLevel,
        target_consistency: ConsistencyLevel,
        dual: &DualWriteConfig,
    ) -> Result<Self, DualWriteError> {
        let mut tables = HashMap::new();
        for mapping in mappings {
            let (sks, stable) = mapping
                .parse_source()
                .map_err(|err| DualWriteError::Invalid(err.to_string()))?;
            let (tks, ttable) = mapping
                .parse_target()
                .map_err(|err| DualWriteError::Invalid(err.to_string()))?;
            let source_schema = odyssey_cql::discover_table(source.as_ref(), &sks, &stable)
                .await
                .map_err(|err| DualWriteError::Schema(err.to_string()))?;
            let target_schema = odyssey_cql::discover_table(target.as_ref(), &tks, &ttable)
                .await
                .map_err(|err| DualWriteError::Schema(err.to_string()))?;
            let key = format!("{sks}.{stable}");
            tables.insert(
                key,
                TablePair {
                    source_schema,
                    target_schema,
                },
            );
        }
        Ok(Self {
            source,
            target,
            source_consistency,
            target_consistency,
            require_both: dual.require_both,
            tables,
        })
    }

    pub async fn apply(&self, mutation: &Mutation) -> Result<(), DualWriteError> {
        let pair = self
            .tables
            .get(mutation.table.trim())
            .ok_or_else(|| DualWriteError::UnknownTable(mutation.table.clone()))?;

        let planned_source = plan_mutation(&pair.source_schema, mutation)?;
        let planned_target = plan_mutation(&pair.target_schema, mutation)?;

        execute_one(
            &self.source,
            self.source_consistency,
            &planned_source,
            true,
        )
        .await?;

        match execute_one(
            &self.target,
            self.target_consistency,
            &planned_target,
            false,
        )
        .await
        {
            Ok(()) => {
                info!(table = %mutation.table, op = ?mutation.op, "dual-write ok");
                Ok(())
            }
            Err(err) if !self.require_both => {
                warn!(error = %err, "target write failed; require_both=false");
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

fn plan_mutation(
    schema: &TableSchema,
    mutation: &Mutation,
) -> Result<PlannedMutation, DualWriteError> {
    let pk: Vec<_> = schema.partition_key_columns();
    let ck: Vec<_> = schema.clustering_columns();
    let ks = quote_ident(&schema.keyspace);
    let table = quote_ident(&schema.table);

    let using = using_clause(mutation.timestamp_us, mutation.ttl_secs);

    match mutation.op {
        MutationOp::Insert | MutationOp::Update => {
            // INSERT upserts; UPDATE requires SET of non-key columns.
            if mutation.op == MutationOp::Update {
                let mut set_names = Vec::new();
                let mut set_values = Vec::new();
                for col in schema.ordered_columns() {
                    if matches!(
                        col.kind,
                        odyssey_cql::ColumnKind::PartitionKey | odyssey_cql::ColumnKind::Clustering
                    ) {
                        continue;
                    }
                    if let Some(raw) = mutation.columns.get(&col.name) {
                        set_names.push(quote_ident(&col.name));
                        set_values.push(json_to_cql(raw, &col.type_name)?);
                    }
                }
                if set_names.is_empty() {
                    return Err(DualWriteError::Invalid(
                        "update requires at least one non-key column".into(),
                    ));
                }
                let mut key_names = Vec::new();
                let mut key_values = Vec::new();
                for col in pk.iter().chain(ck.iter()) {
                    let raw = mutation.columns.get(&col.name).ok_or_else(|| {
                        DualWriteError::Invalid(format!("missing key column `{}`", col.name))
                    })?;
                    key_names.push(quote_ident(&col.name));
                    key_values.push(json_to_cql(raw, &col.type_name)?);
                }
                let sets = set_names
                    .iter()
                    .map(|n| format!("{n} = ?"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let preds = key_names
                    .iter()
                    .map(|n| format!("{n} = ?"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                // Bind order: USING TIMESTAMP/TTL, then SET values, then WHERE keys.
                let mut bind = Vec::new();
                if let Some(ts) = mutation.timestamp_us {
                    bind.push(Some(CqlValue::BigInt(ts)));
                }
                if let Some(ttl) = mutation.ttl_secs {
                    bind.push(Some(CqlValue::Int(ttl)));
                }
                bind.extend(set_values);
                bind.extend(key_values);
                let cql = format!("UPDATE {ks}.{table}{using} SET {sets} WHERE {preds}");
                return Ok(PlannedMutation {
                    cql,
                    values: bind,
                });
            }

            // Insert: all provided columns (must include PK).
            let mut names = Vec::new();
            let mut values = Vec::new();
            for col in pk.iter().chain(ck.iter()) {
                let raw = mutation.columns.get(&col.name).ok_or_else(|| {
                    DualWriteError::Invalid(format!("missing key column `{}`", col.name))
                })?;
                names.push(quote_ident(&col.name));
                values.push(json_to_cql(raw, &col.type_name)?);
            }
            for col in schema.ordered_columns() {
                if matches!(
                    col.kind,
                    odyssey_cql::ColumnKind::PartitionKey | odyssey_cql::ColumnKind::Clustering
                ) {
                    continue;
                }
                if let Some(raw) = mutation.columns.get(&col.name) {
                    names.push(quote_ident(&col.name));
                    values.push(json_to_cql(raw, &col.type_name)?);
                }
            }
            let cols_sql = names.join(", ");
            let placeholders = std::iter::repeat_n("?", names.len())
                .collect::<Vec<_>>()
                .join(", ");
            if let Some(ts) = mutation.timestamp_us {
                values.push(Some(CqlValue::BigInt(ts)));
            }
            if let Some(ttl) = mutation.ttl_secs {
                values.push(Some(CqlValue::Int(ttl)));
            }
            let cql =
                format!("INSERT INTO {ks}.{table} ({cols_sql}) VALUES ({placeholders}){using}");
            Ok(PlannedMutation { cql, values })
        }
        MutationOp::DeleteRow => {
            let mut key_names = Vec::new();
            let mut values = Vec::new();
            for col in pk.iter().chain(ck.iter()) {
                let raw = mutation.columns.get(&col.name).ok_or_else(|| {
                    DualWriteError::Invalid(format!("missing key column `{}`", col.name))
                })?;
                key_names.push(quote_ident(&col.name));
                values.push(json_to_cql(raw, &col.type_name)?);
            }
            let preds = key_names
                .iter()
                .map(|n| format!("{n} = ?"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let cql = format!("DELETE FROM {ks}.{table} WHERE {preds}");
            Ok(PlannedMutation { cql, values })
        }
        MutationOp::DeletePartition => {
            let mut key_names = Vec::new();
            let mut values = Vec::new();
            for col in &pk {
                let raw = mutation.columns.get(&col.name).ok_or_else(|| {
                    DualWriteError::Invalid(format!("missing partition key `{}`", col.name))
                })?;
                key_names.push(quote_ident(&col.name));
                values.push(json_to_cql(raw, &col.type_name)?);
            }
            let preds = key_names
                .iter()
                .map(|n| format!("{n} = ?"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let cql = format!("DELETE FROM {ks}.{table} WHERE {preds}");
            Ok(PlannedMutation { cql, values })
        }
    }
}

fn using_clause(timestamp_us: Option<i64>, ttl_secs: Option<i32>) -> String {
    match (timestamp_us.is_some(), ttl_secs.is_some()) {
        (true, true) => " USING TIMESTAMP ? AND TTL ?".into(),
        (true, false) => " USING TIMESTAMP ?".into(),
        (false, true) => " USING TTL ?".into(),
        (false, false) => String::new(),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn execute_one(
    session: &CqlSession,
    consistency: ConsistencyLevel,
    planned: &PlannedMutation,
    is_source: bool,
) -> Result<(), DualWriteError> {
    let mut statement = Statement::new(planned.cql.clone());
    statement.set_consistency(to_driver_consistency(consistency));
    session
        .inner()
        .query_unpaged(statement, planned.values.clone())
        .await
        .map_err(|err| {
            if is_source {
                DualWriteError::SourceWrite(err.to_string())
            } else {
                DualWriteError::TargetWrite(err.to_string())
            }
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_cql::{ColumnInfo, ColumnKind, TableSchema};
    use serde_json::json;

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
            ],
        }
    }

    #[test]
    fn plans_insert() {
        let mut columns = serde_json::Map::new();
        columns.insert("pk".into(), json!("a"));
        columns.insert("v".into(), json!("x"));
        let m = Mutation {
            table: "ks.t".into(),
            op: MutationOp::Insert,
            columns,
            timestamp_us: None,
            ttl_secs: None,
        };
        let planned = plan_mutation(&schema(), &m).unwrap();
        assert!(planned.cql.starts_with("INSERT INTO"));
        assert_eq!(planned.values.len(), 2);
    }

    #[test]
    fn plans_delete_row() {
        let mut columns = serde_json::Map::new();
        columns.insert("pk".into(), json!("a"));
        let m = Mutation {
            table: "ks.t".into(),
            op: MutationOp::DeleteRow,
            columns,
            timestamp_us: None,
            ttl_secs: None,
        };
        let planned = plan_mutation(&schema(), &m).unwrap();
        assert!(planned.cql.starts_with("DELETE FROM"));
    }
}
