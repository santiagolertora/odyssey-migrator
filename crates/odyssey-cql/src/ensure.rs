//! Opt-in target DDL: create keyspace/table from a discovered source schema.
//!
//! Never runs against the source session. Existing objects are left untouched.

use scylla::DeserializeRow;
use tracing::info;

use crate::schema::{ColumnKind, TableSchema, quote_ident};
use crate::{CqlError, CqlSession};

/// Options for [`ensure_target_table`].
#[derive(Debug, Clone)]
pub struct CreateSchemaOptions {
    /// Target datacenter for NetworkTopologyStrategy. If `None`, uses SimpleStrategy.
    pub datacenter: Option<String>,
    /// Replication factor (default callers should pass ≥ 1).
    pub replication_factor: u32,
}

/// What [`ensure_target_table`] did on the target cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureSchemaAction {
    AlreadyExists,
    CreatedKeyspaceAndTable,
    CreatedTable,
}

/// Ensure `keyspace.table` exists on the **target** using `source` column layout.
///
/// If the table already exists, returns [`EnsureSchemaAction::AlreadyExists`]
/// without ALTER. Source is never modified.
pub async fn ensure_target_table(
    target: &CqlSession,
    source: &TableSchema,
    keyspace: &str,
    table: &str,
    opts: &CreateSchemaOptions,
) -> Result<EnsureSchemaAction, CqlError> {
    if opts.replication_factor == 0 {
        return Err(CqlError::Invalid(
            "create_schema replication_factor must be >= 1".into(),
        ));
    }

    if table_exists(target, keyspace, table).await? {
        info!(keyspace, table, "target table already exists; skipping DDL");
        return Ok(EnsureSchemaAction::AlreadyExists);
    }

    let mut created_ks = false;
    if !keyspace_exists(target, keyspace).await? {
        let cql = create_keyspace_cql(keyspace, opts.datacenter.as_deref(), opts.replication_factor);
        info!(%cql, "creating target keyspace");
        target
            .inner()
            .query_unpaged(cql, &[])
            .await
            .map_err(|err| CqlError::Query(err.to_string()))?;
        created_ks = true;
    }

    let cql = create_table_cql(source, keyspace, table)?;
    info!(%cql, "creating target table");
    target
        .inner()
        .query_unpaged(cql, &[])
        .await
        .map_err(|err| CqlError::Query(err.to_string()))?;

    Ok(if created_ks {
        EnsureSchemaAction::CreatedKeyspaceAndTable
    } else {
        EnsureSchemaAction::CreatedTable
    })
}

/// Build a minimal `CREATE KEYSPACE` for the target.
pub fn create_keyspace_cql(keyspace: &str, datacenter: Option<&str>, rf: u32) -> String {
    let ks = quote_ident(keyspace);
    match datacenter {
        Some(dc) if !dc.is_empty() => format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': {rf}}} AND durable_writes = true"
        ),
        _ => format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {rf}}} AND durable_writes = true"
        ),
    }
}

/// Build a minimal `CREATE TABLE` matching source PK/columns (no WITH options).
pub fn create_table_cql(
    source: &TableSchema,
    keyspace: &str,
    table: &str,
) -> Result<String, CqlError> {
    let pk = source.partition_key_columns();
    if pk.is_empty() {
        return Err(CqlError::MissingPartitionKey {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        });
    }

    let mut col_defs = Vec::new();
    for col in source.ordered_columns() {
        let static_suffix = if col.kind == ColumnKind::Static {
            " static"
        } else {
            ""
        };
        col_defs.push(format!(
            "{} {}{}",
            quote_ident(&col.name),
            col.type_name,
            static_suffix
        ));
    }

    let pk_list = pk
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let ck = source.clustering_columns();
    let primary = if ck.is_empty() {
        if pk.len() == 1 {
            format!("PRIMARY KEY ({pk_list})")
        } else {
            format!("PRIMARY KEY (({pk_list}))")
        }
    } else {
        let ck_list = ck
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PRIMARY KEY (({pk_list}), {ck_list})")
    };

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {}.{} ({}, {})",
        quote_ident(keyspace),
        quote_ident(table),
        col_defs.join(", "),
        primary
    ))
}

async fn keyspace_exists(session: &CqlSession, keyspace: &str) -> Result<bool, CqlError> {
    #[derive(Debug, DeserializeRow)]
    struct Row {
        count: i64,
    }

    let result = session
        .inner()
        .query_unpaged(
            "SELECT count(*) AS count FROM system_schema.keyspaces WHERE keyspace_name = ?",
            (keyspace,),
        )
        .await
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let rows = result
        .into_rows_result()
        .map_err(|err| CqlError::Query(err.to_string()))?;
    let mut iter = rows
        .rows::<Row>()
        .map_err(|err| CqlError::Query(err.to_string()))?;
    let count = iter
        .next()
        .transpose()
        .map_err(|err| CqlError::Query(err.to_string()))?
        .map(|r| r.count)
        .unwrap_or(0);
    Ok(count > 0)
}

async fn table_exists(session: &CqlSession, keyspace: &str, table: &str) -> Result<bool, CqlError> {
    #[derive(Debug, DeserializeRow)]
    struct Row {
        count: i64,
    }

    let result = session
        .inner()
        .query_unpaged(
            "SELECT count(*) AS count FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            (keyspace, table),
        )
        .await
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let rows = result
        .into_rows_result()
        .map_err(|err| CqlError::Query(err.to_string()))?;
    let mut iter = rows
        .rows::<Row>()
        .map_err(|err| CqlError::Query(err.to_string()))?;
    let count = iter
        .next()
        .transpose()
        .map_err(|err| CqlError::Query(err.to_string()))?
        .map(|r| r.count)
        .unwrap_or(0);
    Ok(count > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnInfo, ColumnKind, TableSchema};

    fn kv_schema() -> TableSchema {
        TableSchema {
            keyspace: "benchmark".into(),
            table: "key_value".into(),
            columns: vec![
                ColumnInfo {
                    name: "key".into(),
                    type_name: "bigint".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 0,
                },
                ColumnInfo {
                    name: "val".into(),
                    type_name: "blob".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        }
    }

    #[test]
    fn keyspace_nts() {
        let cql = create_keyspace_cql("benchmark", Some("DC1"), 1);
        assert!(cql.contains("NetworkTopologyStrategy"));
        assert!(cql.contains("'DC1': 1"));
    }

    #[test]
    fn table_simple_pk() {
        let cql = create_table_cql(&kv_schema(), "benchmark", "key_value").unwrap();
        assert!(cql.contains("CREATE TABLE IF NOT EXISTS \"benchmark\".\"key_value\""));
        assert!(cql.contains("\"key\" bigint"));
        assert!(cql.contains("\"val\" blob"));
        assert!(cql.contains("PRIMARY KEY (\"key\")"));
    }
}
