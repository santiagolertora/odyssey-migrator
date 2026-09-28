use scylla::DeserializeRow;

use crate::{CqlError, CqlSession};

/// A column belonging to a table, with its role in the primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub kind: ColumnKind,
    pub position: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    PartitionKey,
    Clustering,
    Regular,
    Static,
}

impl ColumnKind {
    fn parse(raw: &str) -> Result<Self, CqlError> {
        match raw {
            "partition_key" => Ok(Self::PartitionKey),
            "clustering" => Ok(Self::Clustering),
            "regular" => Ok(Self::Regular),
            "static" => Ok(Self::Static),
            other => Err(CqlError::Invalid(format!("unknown column kind `{other}`"))),
        }
    }
}

/// Schema facts needed to build token-range SELECT/INSERT statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
}

impl TableSchema {
    pub fn partition_key_columns(&self) -> Vec<&ColumnInfo> {
        let mut keys: Vec<&ColumnInfo> = self
            .columns
            .iter()
            .filter(|c| c.kind == ColumnKind::PartitionKey)
            .collect();
        keys.sort_by_key(|c| c.position);
        keys
    }

    pub fn clustering_columns(&self) -> Vec<&ColumnInfo> {
        let mut keys: Vec<&ColumnInfo> = self
            .columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Clustering)
            .collect();
        keys.sort_by_key(|c| c.position);
        keys
    }

    /// Columns in a stable order for SELECT/INSERT value binding.
    ///
    /// Partition keys (by position), clustering (by position), then static and
    /// regular columns sorted by name so two schema discoveries agree.
    pub fn ordered_columns(&self) -> Vec<&ColumnInfo> {
        let mut cols: Vec<&ColumnInfo> = self.columns.iter().collect();
        cols.sort_by(|a, b| {
            kind_sort_key(a.kind)
                .cmp(&kind_sort_key(b.kind))
                .then_with(|| a.position.cmp(&b.position))
                .then_with(|| a.name.cmp(&b.name))
        });
        cols
    }

    /// Comma-separated quoted column list in [`Self::ordered_columns`] order.
    pub fn select_columns_cql(&self) -> Result<String, CqlError> {
        let cols = self.ordered_columns();
        if cols.is_empty() {
            return Err(CqlError::Invalid(format!(
                "table `{}.{}` has no columns",
                self.keyspace, self.table
            )));
        }
        Ok(cols
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", "))
    }

    /// CQL fragment for `token(pk1, pk2, ...)`.
    pub fn token_argument_list(&self) -> Result<String, CqlError> {
        let keys = self.partition_key_columns();
        if keys.is_empty() {
            return Err(CqlError::MissingPartitionKey {
                keyspace: self.keyspace.clone(),
                table: self.table.clone(),
            });
        }
        Ok(keys
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", "))
    }

    /// SELECT that pages one token range for this table.
    ///
    /// Uses an explicit column list so reader and writer share the same binding
    /// order (never `SELECT *`).
    pub fn range_select_cql(&self) -> Result<String, CqlError> {
        let token_args = self.token_argument_list()?;
        let columns = self.select_columns_cql()?;
        Ok(format!(
            "SELECT {}, token({}) FROM {}.{} WHERE token({}) > ? AND token({}) <= ?",
            columns,
            token_args,
            quote_ident(&self.keyspace),
            quote_ident(&self.table),
            token_args,
            token_args
        ))
    }

    /// Prepared INSERT for every column in [`Self::ordered_columns`] order.
    pub fn insert_cql(&self) -> Result<String, CqlError> {
        self.insert_cql_preserving(false, false)
    }

    /// INSERT with optional `USING TIMESTAMP` / `USING TTL` bind placeholders.
    ///
    /// Bind order: data columns, then timestamp (µs) if `preserve_writetimes`,
    /// then TTL (seconds) if `preserve_ttls`.
    pub fn insert_cql_preserving(
        &self,
        preserve_writetimes: bool,
        preserve_ttls: bool,
    ) -> Result<String, CqlError> {
        let cols = self.ordered_columns();
        if cols.is_empty() {
            return Err(CqlError::Invalid(format!(
                "table `{}.{}` has no columns",
                self.keyspace, self.table
            )));
        }
        let names = cols
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat_n("?", cols.len())
            .collect::<Vec<_>>()
            .join(", ");
        let mut cql = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            quote_ident(&self.keyspace),
            quote_ident(&self.table),
            names,
            placeholders
        );
        match (preserve_writetimes, preserve_ttls) {
            (true, true) => cql.push_str(" USING TIMESTAMP ? AND TTL ?"),
            (true, false) => cql.push_str(" USING TIMESTAMP ?"),
            (false, true) => cql.push_str(" USING TTL ?"),
            (false, false) => {}
        }
        Ok(cql)
    }

    /// Non-key columns that support `WRITETIME` / `TTL` in SELECT (level-A preserve).
    ///
    /// Skips counters and (by default) collections. Frozen collections are included
    /// when `include_frozen_collections` is true.
    pub fn preservable_value_columns(&self, include_frozen_collections: bool) -> Vec<&ColumnInfo> {
        self.ordered_columns()
            .into_iter()
            .filter(|c| {
                matches!(c.kind, ColumnKind::Regular | ColumnKind::Static)
                    && is_preservable_cql_type(&c.type_name, include_frozen_collections)
            })
            .collect()
    }

    /// Unfrozen `map<…>` regular/static columns (multi-cell; element WRITETIME/TTL).
    pub fn unfrozen_map_columns(&self) -> Vec<&ColumnInfo> {
        self.ordered_columns()
            .into_iter()
            .filter(|c| {
                matches!(c.kind, ColumnKind::Regular | ColumnKind::Static)
                    && collection_kind(&c.type_name) == Some(CollectionKind::Map)
            })
            .collect()
    }

    /// Unfrozen `set<…>` regular/static columns.
    pub fn unfrozen_set_columns(&self) -> Vec<&ColumnInfo> {
        self.ordered_columns()
            .into_iter()
            .filter(|c| {
                matches!(c.kind, ColumnKind::Regular | ColumnKind::Static)
                    && collection_kind(&c.type_name) == Some(CollectionKind::Set)
            })
            .collect()
    }

    /// Unfrozen `list<…>` regular/static columns (no per-element CQL meta).
    pub fn unfrozen_list_columns(&self) -> Vec<&ColumnInfo> {
        self.ordered_columns()
            .into_iter()
            .filter(|c| {
                matches!(c.kind, ColumnKind::Regular | ColumnKind::Static)
                    && collection_kind(&c.type_name) == Some(CollectionKind::List)
            })
            .collect()
    }

    /// True when the table contains at least one counter column.
    pub fn has_counters(&self) -> bool {
        self.columns.iter().any(|c| {
            let t = c.type_name.trim().to_ascii_lowercase();
            t == "counter" || t.starts_with("counter")
        })
    }

    /// Counter columns (for UPDATE `col = col + ?` binders).
    pub fn counter_columns(&self) -> Vec<&ColumnInfo> {
        self.ordered_columns()
            .into_iter()
            .filter(|c| {
                let t = c.type_name.trim().to_ascii_lowercase();
                t == "counter" || t.starts_with("counter")
            })
            .collect()
    }

    /// Token-range SELECT; when preserve flags are set, appends `writetime(c)` /
    /// `ttl(c)` for each preservable value column after the data columns.
    pub fn range_select_cql_preserving(
        &self,
        preserve_writetimes: bool,
        preserve_ttls: bool,
    ) -> Result<String, CqlError> {
        self.range_select_cql_preserving_opts(preserve_writetimes, preserve_ttls, false)
    }

    pub fn range_select_cql_preserving_opts(
        &self,
        preserve_writetimes: bool,
        preserve_ttls: bool,
        include_frozen_collections: bool,
    ) -> Result<String, CqlError> {
        let token_args = self.token_argument_list()?;
        let mut select_list = self.select_columns_cql()?;
        let meta_cols = self.preservable_value_columns(include_frozen_collections);
        if preserve_writetimes {
            for col in &meta_cols {
                select_list.push_str(", writetime(");
                select_list.push_str(&quote_ident(&col.name));
                select_list.push(')');
            }
        }
        if preserve_ttls {
            for col in &meta_cols {
                select_list.push_str(", ttl(");
                select_list.push_str(&quote_ident(&col.name));
                select_list.push(')');
            }
        }
        Ok(format!(
            "SELECT {}, token({}) FROM {}.{} WHERE token({}) > ? AND token({}) <= ?",
            select_list,
            token_args,
            quote_ident(&self.keyspace),
            quote_ident(&self.table),
            token_args,
            token_args
        ))
    }
}

fn is_preservable_cql_type(type_name: &str, include_frozen_collections: bool) -> bool {
    let t = type_name.trim().to_ascii_lowercase();
    if t == "counter" || t.starts_with("counter") {
        return false;
    }
    if t.starts_with("frozen<") {
        return include_frozen_collections;
    }
    let head = t.split('<').next().unwrap_or(&t);
    !matches!(head, "list" | "set" | "map")
}

/// Unfrozen collection head (`map` / `set` / `list`); `None` for scalars or frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionKind {
    Map,
    Set,
    List,
}

pub fn collection_kind(type_name: &str) -> Option<CollectionKind> {
    let t = type_name.trim().to_ascii_lowercase();
    if t.starts_with("frozen<") {
        return None;
    }
    match t.split('<').next().unwrap_or(&t) {
        "map" => Some(CollectionKind::Map),
        "set" => Some(CollectionKind::Set),
        "list" => Some(CollectionKind::List),
        _ => None,
    }
}

fn kind_sort_key(kind: ColumnKind) -> u8 {
    match kind {
        ColumnKind::PartitionKey => 0,
        ColumnKind::Clustering => 1,
        ColumnKind::Static => 2,
        ColumnKind::Regular => 3,
    }
}

pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Debug, DeserializeRow)]
struct SchemaColumnRow {
    column_name: String,
    #[scylla(rename = "type")]
    type_name: String,
    kind: String,
    position: i32,
}

/// Load column metadata for one table from `system_schema.columns`.
pub async fn discover_table(
    session: &CqlSession,
    keyspace: &str,
    table: &str,
) -> Result<TableSchema, CqlError> {
    tracing::debug!(keyspace, table, "discovering table schema");

    let result = session
        .inner()
        .query_unpaged(
            "SELECT column_name, type, kind, position \
             FROM system_schema.columns \
             WHERE keyspace_name = ? AND table_name = ?",
            (keyspace, table),
        )
        .await
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let rows = result
        .into_rows_result()
        .map_err(|err| CqlError::Query(err.to_string()))?;

    let mut columns = Vec::new();
    for row in rows
        .rows::<SchemaColumnRow>()
        .map_err(|err| CqlError::Query(err.to_string()))?
    {
        let row = row.map_err(|err| CqlError::Query(err.to_string()))?;
        columns.push(ColumnInfo {
            name: row.column_name,
            type_name: row.type_name,
            kind: ColumnKind::parse(&row.kind)?,
            position: row.position,
        });
    }

    if columns.is_empty() {
        return Err(CqlError::TableNotFound {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        });
    }

    let schema = TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        columns,
    };

    if schema.partition_key_columns().is_empty() {
        return Err(CqlError::MissingPartitionKey {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        });
    }

    tracing::info!(
        keyspace,
        table,
        columns = schema.columns.len(),
        partition_keys = schema.partition_key_columns().len(),
        "table schema discovered"
    );

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> TableSchema {
        TableSchema {
            keyspace: "ecommerce".into(),
            table: "events".into(),
            columns: vec![
                ColumnInfo {
                    name: "tenant_id".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 0,
                },
                ColumnInfo {
                    name: "device_id".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 1,
                },
                ColumnInfo {
                    name: "ts".into(),
                    type_name: "timestamp".into(),
                    kind: ColumnKind::Clustering,
                    position: 0,
                },
                ColumnInfo {
                    name: "payload".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        }
    }

    #[test]
    fn builds_token_range_select() {
        let schema = sample_schema();
        let cql = schema.range_select_cql().unwrap();
        assert_eq!(
            cql,
            "SELECT \"tenant_id\", \"device_id\", \"ts\", \"payload\", token(tenant_id, device_id) FROM \"ecommerce\".\"events\" WHERE token(tenant_id, device_id) > ? AND token(tenant_id, device_id) <= ?"
        );
    }

    #[test]
    fn builds_insert_with_placeholders() {
        let schema = sample_schema();
        let cql = schema.insert_cql().unwrap();
        assert_eq!(
            cql,
            "INSERT INTO \"ecommerce\".\"events\" (\"tenant_id\", \"device_id\", \"ts\", \"payload\") VALUES (?, ?, ?, ?)"
        );
    }

    #[test]
    fn preserve_select_and_insert() {
        let schema = sample_schema();
        let select = schema
            .range_select_cql_preserving(true, true)
            .unwrap();
        assert!(select.contains("writetime(\"payload\")"));
        assert!(select.contains("ttl(\"payload\")"));
        assert!(select.contains("token(tenant_id, device_id)"));
        // ts is clustering key — not in preservable value columns
        assert!(!select.contains("writetime(\"ts\")"));

        let insert = schema.insert_cql_preserving(true, true).unwrap();
        assert!(insert.ends_with("USING TIMESTAMP ? AND TTL ?"));
    }

    #[test]
    fn orders_partition_keys_by_position() {
        let schema = sample_schema();
        let keys = schema.partition_key_columns();
        assert_eq!(keys[0].name, "tenant_id");
        assert_eq!(keys[1].name, "device_id");
    }

    #[test]
    fn ordered_columns_are_stable() {
        let schema = sample_schema();
        let names: Vec<_> = schema
            .ordered_columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["tenant_id", "device_id", "ts", "payload"]);
    }
}
