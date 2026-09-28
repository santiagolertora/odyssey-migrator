use serde_json::Value;
use scylla::value::CqlValue;
use uuid::Uuid;

use crate::DualWriteError;

/// Convert a JSON value into a [`CqlValue`] using the CQL type hint from schema.
pub fn json_to_cql(value: &Value, type_name: &str) -> Result<Option<CqlValue>, DualWriteError> {
    if value.is_null() {
        return Ok(None);
    }
    let t = type_name.trim().to_ascii_lowercase();
    let head = t.split('<').next().unwrap_or(&t).trim();

    match head {
        "text" | "ascii" | "varchar" => {
            let s = value
                .as_str()
                .ok_or_else(|| DualWriteError::Invalid(format!("expected string for `{type_name}`")))?;
            Ok(Some(CqlValue::Text(s.to_string())))
        }
        "boolean" | "bool" => {
            let b = value.as_bool().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected bool for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::Boolean(b)))
        }
        "int" => {
            let n = value.as_i64().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected int for `{type_name}`"))
            })?;
            let v = i32::try_from(n).map_err(|_| {
                DualWriteError::Invalid(format!("int out of range for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::Int(v)))
        }
        "bigint" | "counter" => {
            let n = value.as_i64().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected bigint for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::BigInt(n)))
        }
        "smallint" => {
            let n = value.as_i64().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected smallint for `{type_name}`"))
            })?;
            let v = i16::try_from(n).map_err(|_| {
                DualWriteError::Invalid(format!("smallint out of range for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::SmallInt(v)))
        }
        "tinyint" => {
            let n = value.as_i64().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected tinyint for `{type_name}`"))
            })?;
            let v = i8::try_from(n).map_err(|_| {
                DualWriteError::Invalid(format!("tinyint out of range for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::TinyInt(v)))
        }
        "float" => {
            let n = value.as_f64().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected float for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::Float(n as f32)))
        }
        "double" => {
            let n = value.as_f64().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected double for `{type_name}`"))
            })?;
            Ok(Some(CqlValue::Double(n)))
        }
        "uuid" | "timeuuid" => {
            let s = value.as_str().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected uuid string for `{type_name}`"))
            })?;
            let id = Uuid::parse_str(s).map_err(|err| {
                DualWriteError::Invalid(format!("invalid uuid `{s}`: {err}"))
            })?;
            Ok(Some(CqlValue::Uuid(id)))
        }
        "blob" => {
            let s = value.as_str().ok_or_else(|| {
                DualWriteError::Invalid("expected base64 string for blob".into())
            })?;
            if let Ok(bytes) = decode_hex(s) {
                Ok(Some(CqlValue::Blob(bytes)))
            } else {
                Ok(Some(CqlValue::Blob(s.as_bytes().to_vec())))
            }
        }
        "list" => {
            let arr = value.as_array().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected array for `{type_name}`"))
            })?;
            let inner = inner_type_one(&t)?;
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                if let Some(v) = json_to_cql(item, inner)? {
                    out.push(v);
                }
            }
            Ok(Some(CqlValue::List(out)))
        }
        "set" => {
            let arr = value.as_array().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected array for `{type_name}`"))
            })?;
            let inner = inner_type_one(&t)?;
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                if let Some(v) = json_to_cql(item, inner)? {
                    out.push(v);
                }
            }
            Ok(Some(CqlValue::Set(out)))
        }
        "map" => {
            let obj = value.as_object().ok_or_else(|| {
                DualWriteError::Invalid(format!("expected object for `{type_name}`"))
            })?;
            let (kty, vty) = inner_type_two(&t)?;
            let mut out = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                let key = json_to_cql(&Value::String(k.clone()), kty)?
                    .ok_or_else(|| DualWriteError::Invalid("map key cannot be null".into()))?;
                let val = json_to_cql(v, vty)?
                    .ok_or_else(|| DualWriteError::Invalid("map value cannot be null".into()))?;
                out.push((key, val));
            }
            Ok(Some(CqlValue::Map(out)))
        }
        other => Err(DualWriteError::Invalid(format!(
            "unsupported CQL type `{other}` in dual-write JSON"
        ))),
    }
}

fn inner_type_one(type_name: &str) -> Result<&str, DualWriteError> {
    let start = type_name
        .find('<')
        .ok_or_else(|| DualWriteError::Invalid(format!("bad collection type `{type_name}`")))?;
    let end = type_name
        .rfind('>')
        .ok_or_else(|| DualWriteError::Invalid(format!("bad collection type `{type_name}`")))?;
    Ok(type_name[start + 1..end].trim())
}

fn inner_type_two(type_name: &str) -> Result<(&str, &str), DualWriteError> {
    let inner = inner_type_one(type_name)?;
    let (a, b) = inner
        .split_once(',')
        .ok_or_else(|| DualWriteError::Invalid(format!("bad map type `{type_name}`")))?;
    Ok((a.trim(), b.trim()))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_and_int() {
        assert_eq!(
            json_to_cql(&Value::String("hi".into()), "text").unwrap(),
            Some(CqlValue::Text("hi".into()))
        );
        assert_eq!(
            json_to_cql(&serde_json::json!(42), "int").unwrap(),
            Some(CqlValue::Int(42))
        );
    }

    #[test]
    fn map_object() {
        let v = serde_json::json!({"a": 1});
        let cql = json_to_cql(&v, "map<text, int>").unwrap().unwrap();
        match cql {
            CqlValue::Map(entries) => {
                assert_eq!(entries.len(), 1);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
