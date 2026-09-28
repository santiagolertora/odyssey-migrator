use scylla::value::{CqlValue, Row};

use crate::preserve::MigratedRow;

/// One page of rows read from the source for a token range.
#[derive(Debug, Clone)]
pub struct RowBatch {
    pub rows: Vec<MigratedRow>,
    pub rows_count: u64,
    pub bytes_estimate: u64,
}

impl RowBatch {
    pub fn from_migrated(rows: Vec<MigratedRow>) -> Self {
        let mut bytes_estimate = 0u64;
        for row in &rows {
            bytes_estimate = bytes_estimate.saturating_add(estimate_row_bytes(&row.values));
        }
        let rows_count = rows.len() as u64;
        Self {
            rows,
            rows_count,
            bytes_estimate,
        }
    }

    /// Convenience when preserve metadata is disabled (plain dynamic rows).
    pub fn from_dynamic_rows(rows: Vec<Row>) -> Self {
        let migrated = rows
            .into_iter()
            .map(|row| MigratedRow {
                values: row.columns,
                writetime_us: None,
                ttl_secs: None,
                collection_elements: Vec::new(),
            })
            .collect();
        Self::from_migrated(migrated)
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Rough serialized size used for progress accounting (not wire-accurate).
pub fn estimate_row_bytes(columns: &[Option<CqlValue>]) -> u64 {
    columns
        .iter()
        .map(|col| match col {
            None => 1,
            Some(value) => estimate_cql_value_bytes(value),
        })
        .sum()
}

pub fn estimate_cql_value_bytes(value: &CqlValue) -> u64 {
    match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => s.len() as u64,
        CqlValue::Blob(b) => b.len() as u64,
        CqlValue::Boolean(_) | CqlValue::TinyInt(_) => 1,
        CqlValue::SmallInt(_) => 2,
        CqlValue::Int(_) | CqlValue::Float(_) | CqlValue::Date(_) => 4,
        CqlValue::BigInt(_)
        | CqlValue::Double(_)
        | CqlValue::Timestamp(_)
        | CqlValue::Time(_)
        | CqlValue::Counter(_) => 8,
        CqlValue::Uuid(_) | CqlValue::Timeuuid(_) => 16,
        CqlValue::Inet(ip) => match ip {
            std::net::IpAddr::V4(_) => 4,
            std::net::IpAddr::V6(_) => 16,
        },
        CqlValue::Duration(_) => 12,
        CqlValue::Empty => 0,
        CqlValue::Decimal(d) => {
            let (bytes, _) = d.as_signed_be_bytes_slice_and_exponent();
            bytes.len() as u64 + 4
        }
        CqlValue::Varint(v) => v.as_signed_bytes_be_slice().len() as u64,
        CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => items
            .iter()
            .map(estimate_cql_value_bytes)
            .fold(4u64, u64::saturating_add),
        CqlValue::Map(entries) => entries
            .iter()
            .map(|(k, v)| {
                estimate_cql_value_bytes(k).saturating_add(estimate_cql_value_bytes(v))
            })
            .fold(4u64, u64::saturating_add),
        CqlValue::Tuple(items) => items
            .iter()
            .map(|item| match item {
                None => 1,
                Some(v) => estimate_cql_value_bytes(v),
            })
            .fold(4u64, u64::saturating_add),
        CqlValue::UserDefinedType { fields, .. } => fields
            .iter()
            .map(|(_, v)| match v {
                None => 1,
                Some(val) => estimate_cql_value_bytes(val),
            })
            .fold(4u64, u64::saturating_add),
        _ => 16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::CqlValue;

    #[test]
    fn batch_accounts_rows_and_bytes() {
        let rows = vec![
            MigratedRow {
                values: vec![
                    Some(CqlValue::Text("abc".into())),
                    Some(CqlValue::Int(1)),
                ],
                writetime_us: None,
                ttl_secs: None,
                collection_elements: Vec::new(),
            },
            MigratedRow {
                values: vec![None, Some(CqlValue::Blob(vec![0, 1, 2, 3]))],
                writetime_us: None,
                ttl_secs: None,
                collection_elements: Vec::new(),
            },
        ];
        let batch = RowBatch::from_migrated(rows);
        assert_eq!(batch.rows_count, 2);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.bytes_estimate, 12);
    }

    #[test]
    fn empty_batch() {
        let batch = RowBatch::from_migrated(Vec::new());
        assert!(batch.is_empty());
        assert_eq!(batch.bytes_estimate, 0);
    }
}
