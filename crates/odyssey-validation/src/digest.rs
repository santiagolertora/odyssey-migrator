//! Row and schema digests for V0.1 validation.
//!
//! # Canonical form (V0.1)
//!
//! Each cell is encoded as the UTF-8 bytes of `format!("{:?}", cell)` where
//! `cell` is `&Option<CqlValue>`. That Debug representation is the intentional
//! V0.1 canonical form: simple, stable across our supported driver version, and
//! good enough until we introduce a tagged binary encoding.
//!
//! Cells are framed with a little-endian `u64` length prefix. After every row,
//! a fixed [`ROW_SEPARATOR`] is absorbed so adjacent rows cannot collide into
//! the same stream.

use odyssey_cql::{ColumnKind, TableSchema};
use scylla::value::{CqlValue, Row};

/// Byte sequence written between rows in a range digest stream.
pub const ROW_SEPARATOR: &[u8] = b"\n";

/// Hash one dynamic [`Row`] to a 32-byte blake3 digest.
pub fn hash_row(row: &Row) -> [u8; 32] {
    hash_columns(&row.columns)
}

/// Hash an ordered column vector the same way [`hash_row`] does.
pub fn hash_columns(columns: &[Option<CqlValue>]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    absorb_columns(&mut hasher, columns);
    *hasher.finalize().as_bytes()
}

/// Absorb one row's cells into a running hasher (no row separator).
pub fn absorb_columns(hasher: &mut blake3::Hasher, columns: &[Option<CqlValue>]) {
    for cell in columns {
        // V0.1: Debug of Option<CqlValue> is the canonical cell encoding.
        let encoded = format!("{cell:?}");
        hasher.update(&(encoded.len() as u64).to_le_bytes());
        hasher.update(encoded.as_bytes());
    }
}

/// Absorb one complete row (cells + [`ROW_SEPARATOR`]) into a range hasher.
pub fn absorb_row(hasher: &mut blake3::Hasher, columns: &[Option<CqlValue>]) {
    absorb_columns(hasher, columns);
    hasher.update(ROW_SEPARATOR);
}

/// Digest a slice of rows in order. Returns (blake3 hex, row count).
///
/// Used by unit tests and by DiffSample helpers that already hold rows in memory.
pub fn digest_rows(rows: &[Vec<Option<CqlValue>>]) -> (String, u64) {
    let mut hasher = blake3::Hasher::new();
    for columns in rows {
        absorb_row(&mut hasher, columns);
    }
    let hex = hasher.finalize().to_hex().to_string();
    (hex, rows.len() as u64)
}

/// Blake3 hex fingerprint of ordered column name + type + kind + position.
///
/// Used by the CLI when filling `NewMigration.schema_hash`.
pub fn schema_fingerprint(schema: &TableSchema) -> String {
    let mut hasher = blake3::Hasher::new();
    for col in schema.ordered_columns() {
        absorb_str(&mut hasher, &col.name);
        absorb_str(&mut hasher, &col.type_name);
        absorb_str(&mut hasher, kind_label(col.kind));
        hasher.update(&col.position.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn absorb_str(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn kind_label(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::PartitionKey => "partition_key",
        ColumnKind::Clustering => "clustering",
        ColumnKind::Regular => "regular",
        ColumnKind::Static => "static",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_cql::{ColumnInfo, ColumnKind, TableSchema};
    use scylla::value::{CqlValue, Row};

    fn row(cols: Vec<Option<CqlValue>>) -> Row {
        Row { columns: cols }
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            columns: vec![
                ColumnInfo {
                    name: "pk".into(),
                    type_name: "uuid".into(),
                    kind: ColumnKind::PartitionKey,
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
    fn hash_row_is_deterministic_for_same_cql_values() {
        let a = row(vec![
            Some(CqlValue::Text("tenant".into())),
            Some(CqlValue::Int(7)),
            None,
        ]);
        let b = row(vec![
            Some(CqlValue::Text("tenant".into())),
            Some(CqlValue::Int(7)),
            None,
        ]);
        assert_eq!(hash_row(&a), hash_row(&b));
    }

    #[test]
    fn hash_row_changes_when_values_differ() {
        let a = row(vec![Some(CqlValue::Text("a".into()))]);
        let b = row(vec![Some(CqlValue::Text("b".into()))]);
        assert_ne!(hash_row(&a), hash_row(&b));
    }

    #[test]
    fn null_and_empty_text_differ() {
        let null_row = row(vec![None]);
        let empty_text = row(vec![Some(CqlValue::Text(String::new()))]);
        assert_ne!(hash_row(&null_row), hash_row(&empty_text));
    }

    #[test]
    fn schema_fingerprint_is_stable() {
        let first = schema_fingerprint(&sample_schema());
        let second = schema_fingerprint(&sample_schema());
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn schema_fingerprint_changes_with_column_type() {
        let mut altered = sample_schema();
        altered.columns[1].type_name = "blob".into();
        assert_ne!(
            schema_fingerprint(&sample_schema()),
            schema_fingerprint(&altered)
        );
    }

    #[test]
    fn two_empty_ranges_match() {
        let (left, left_rows) = digest_rows(&[]);
        let (right, right_rows) = digest_rows(&[]);
        assert_eq!(left_rows, 0);
        assert_eq!(right_rows, 0);
        assert_eq!(left, right);
    }

    #[test]
    fn range_digest_is_order_sensitive() {
        let forward = digest_rows(&[
            vec![Some(CqlValue::Int(1))],
            vec![Some(CqlValue::Int(2))],
        ]);
        let reverse = digest_rows(&[
            vec![Some(CqlValue::Int(2))],
            vec![Some(CqlValue::Int(1))],
        ]);
        assert_ne!(forward.0, reverse.0);
    }
}
