use odyssey_cql::TableSchema;

use crate::preserve::PreserveOptions;
use crate::EngineError;

/// Build the paged token-range SELECT for a source table.
pub fn build_range_select_cql(schema: &TableSchema) -> Result<String, EngineError> {
    build_range_select_cql_preserving(schema, PreserveOptions::default())
}

pub fn build_range_select_cql_preserving(
    schema: &TableSchema,
    opts: PreserveOptions,
) -> Result<String, EngineError> {
    schema
        .range_select_cql_preserving_opts(
            opts.writetimes,
            opts.ttls,
            opts.frozen_collections,
        )
        .map_err(|err| EngineError::Statement(err.to_string()))
}

/// Build the prepared INSERT for a target table.
pub fn build_insert_cql(schema: &TableSchema) -> Result<String, EngineError> {
    build_insert_cql_preserving(schema, PreserveOptions::default())
}

pub fn build_insert_cql_preserving(
    schema: &TableSchema,
    opts: PreserveOptions,
) -> Result<String, EngineError> {
    schema
        .insert_cql_preserving(opts.writetimes, opts.ttls)
        .map_err(|err| EngineError::Statement(err.to_string()))
}

/// `UPDATE … SET counter = counter + ? WHERE pk = ? …` for counter tables.
pub fn build_counter_update_cql(schema: &TableSchema) -> Result<String, EngineError> {
    let counters = schema.counter_columns();
    if counters.is_empty() {
        return Err(EngineError::Statement(
            "build_counter_update_cql called on non-counter table".into(),
        ));
    }
    let key_names: Vec<String> = schema
        .partition_key_columns()
        .into_iter()
        .chain(schema.clustering_columns())
        .map(|c| c.name.clone())
        .collect();
    if key_names.is_empty() {
        return Err(EngineError::Statement(
            "counter table missing partition key".into(),
        ));
    }
    let sets = counters
        .iter()
        .map(|c| {
            let q = format!("\"{}\"", c.name.replace('"', "\"\""));
            format!("{q} = {q} + ?")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let preds = key_names
        .iter()
        .map(|n| format!("\"{}\" = ?", n.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "UPDATE \"{}\".\"{}\" SET {sets} WHERE {preds}",
        schema.keyspace.replace('"', "\"\""),
        schema.table.replace('"', "\"\"")
    ))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn using_clause(opts: PreserveOptions) -> String {
    match (opts.writetimes, opts.ttls) {
        (true, true) => " USING TIMESTAMP ? AND TTL ?".into(),
        (true, false) => " USING TIMESTAMP ?".into(),
        (false, true) => " USING TTL ?".into(),
        (false, false) => String::new(),
    }
}

fn key_predicates(schema: &TableSchema) -> Result<String, EngineError> {
    let keys: Vec<_> = schema
        .partition_key_columns()
        .into_iter()
        .chain(schema.clustering_columns())
        .collect();
    if keys.is_empty() {
        return Err(EngineError::Statement(
            "table missing partition key for collection element update".into(),
        ));
    }
    Ok(keys
        .iter()
        .map(|c| format!("{} = ?", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(" AND "))
}

/// `SELECT writetime(col[?]), ttl(col[?]) FROM … WHERE pk/ck = ?`
pub fn build_collection_element_meta_select(
    schema: &TableSchema,
    column: &str,
) -> Result<String, EngineError> {
    let preds = key_predicates(schema)?;
    let col = quote_ident(column);
    Ok(format!(
        "SELECT writetime({col}[?]), ttl({col}[?]) FROM {}.{} WHERE {preds}",
        quote_ident(&schema.keyspace),
        quote_ident(&schema.table),
    ))
}

/// `UPDATE … USING … SET col[?] = ? WHERE pk/ck`
pub fn build_map_element_update_cql(
    schema: &TableSchema,
    column: &str,
    opts: PreserveOptions,
) -> Result<String, EngineError> {
    let preds = key_predicates(schema)?;
    let col = quote_ident(column);
    Ok(format!(
        "UPDATE {}.{}{} SET {col}[?] = ? WHERE {preds}",
        quote_ident(&schema.keyspace),
        quote_ident(&schema.table),
        using_clause(opts),
    ))
}

/// `UPDATE … USING … SET col = col + ? WHERE pk/ck`
pub fn build_set_element_update_cql(
    schema: &TableSchema,
    column: &str,
    opts: PreserveOptions,
) -> Result<String, EngineError> {
    let preds = key_predicates(schema)?;
    let col = quote_ident(column);
    Ok(format!(
        "UPDATE {}.{}{} SET {col} = {col} + ? WHERE {preds}",
        quote_ident(&schema.keyspace),
        quote_ident(&schema.table),
        using_clause(opts),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_cql::{ColumnInfo, ColumnKind, TableSchema};

    fn sample() -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            columns: vec![
                ColumnInfo {
                    name: "pk".into(),
                    type_name: "int".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 0,
                },
                ColumnInfo {
                    name: "ck".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::Clustering,
                    position: 0,
                },
                ColumnInfo {
                    name: "val".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        }
    }

    #[test]
    fn select_lists_columns_and_token_predicate() {
        let cql = build_range_select_cql(&sample()).unwrap();
        assert!(cql.starts_with("SELECT \"pk\", \"ck\", \"val\", token(pk) FROM \"ks\".\"t\""));
        assert!(cql.contains("token(pk) > ? AND token(pk) <= ?"));
        assert!(!cql.contains("SELECT *"));
    }

    #[test]
    fn insert_matches_column_order() {
        let cql = build_insert_cql(&sample()).unwrap();
        assert_eq!(
            cql,
            "INSERT INTO \"ks\".\"t\" (\"pk\", \"ck\", \"val\") VALUES (?, ?, ?)"
        );
    }

    #[test]
    fn preserve_select_appends_writetime_and_ttl() {
        let opts = PreserveOptions {
            writetimes: true,
            ttls: true,
            frozen_collections: false,
            collection_elements: false,
        };
        let cql = build_range_select_cql_preserving(&sample(), opts).unwrap();
        assert!(cql.contains("writetime(\"val\")"));
        assert!(cql.contains("ttl(\"val\")"));
        assert!(cql.contains(", token(pk) FROM"));
    }

    #[test]
    fn preserve_insert_adds_using_clause() {
        let opts = PreserveOptions {
            writetimes: true,
            ttls: true,
            frozen_collections: false,
            collection_elements: false,
        };
        let cql = build_insert_cql_preserving(&sample(), opts).unwrap();
        assert!(cql.ends_with("USING TIMESTAMP ? AND TTL ?"));
    }

    #[test]
    fn counter_update_cql() {
        let schema = TableSchema {
            keyspace: "ks".into(),
            table: "hits".into(),
            columns: vec![
                ColumnInfo {
                    name: "pk".into(),
                    type_name: "text".into(),
                    kind: ColumnKind::PartitionKey,
                    position: 0,
                },
                ColumnInfo {
                    name: "c".into(),
                    type_name: "counter".into(),
                    kind: ColumnKind::Regular,
                    position: -1,
                },
            ],
        };
        let cql = build_counter_update_cql(&schema).unwrap();
        assert!(cql.contains("UPDATE"));
        assert!(cql.contains("\"c\" = \"c\" + ?"));
        assert!(cql.contains("WHERE \"pk\" = ?"));
    }

    #[test]
    fn map_element_update_cql() {
        let opts = PreserveOptions {
            writetimes: true,
            ttls: true,
            frozen_collections: false,
            collection_elements: true,
        };
        let cql = build_map_element_update_cql(&sample(), "attrs", opts).unwrap();
        assert!(cql.contains("SET \"attrs\"[?] = ?"));
        assert!(cql.contains("USING TIMESTAMP ? AND TTL ?"));
        assert!(cql.contains("WHERE \"pk\" = ? AND \"ck\" = ?"));
    }

    #[test]
    fn element_meta_select_cql() {
        let cql = build_collection_element_meta_select(&sample(), "attrs").unwrap();
        assert!(cql.starts_with("SELECT writetime(\"attrs\"[?]), ttl(\"attrs\"[?])"));
    }
}
