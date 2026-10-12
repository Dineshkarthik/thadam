//! Mapping OPC UA `Variant`/`DataValue` values into JSON.
//!
//! The mapping is deliberately small and total: anything outside it becomes
//! `null` plus a named entry in the message's `opcua.unsupported` attribute,
//! so nothing is silently dropped:
//!
//! | OPC UA type                                  | JSON                                  |
//! |----------------------------------------------|---------------------------------------|
//! | Empty Variant / null String or ByteString    | `null`                                |
//! | Boolean                                      | boolean                               |
//! | SByte, Byte, Int16..Int64, UInt16..UInt64    | number (i64/u64 exact)                |
//! | Float, Double                                | number (f64; NaN/inf become `null`    |
//! |                                              | as JSON cannot carry them)            |
//! | String                                       | string                                |
//! | DateTime                                     | RFC 3339 string with UTC offset `Z`   |
//! | ByteString                                   | base64 string (OPC UA JSON, Part 6)   |
//! | Array of Boolean or any mapped numeric type  | array of the mapped scalars; a        |
//! |                                              | multi-dimensional array is flattened  |
//! |                                              | in wire order                         |
//! | arrays of Float/Double containing a NaN or   | the whole array is unsupported        |
//! | infinite element                             |                                       |
//!
//! Guid, StatusCode, XmlElement, QualifiedName, LocalizedText, NodeId,
//! ExpandedNodeId, ExtensionObject, nested Variant, DataValue, DiagnosticInfo,
//! and arrays of String/DateTime/ByteString are unsupported.

use base64::Engine as _;
use chrono::SecondsFormat;
use opcua::types::{DataValue, DateTime, Variant};
use serde_json::Value;

use crate::error::status_name;

/// How one data change fared in the mapping.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MappedValue {
    /// The JSON value: `null` when the status is not Good or the variant is
    /// unsupported.
    pub json: Value,
    /// The status code's name (`BadSensorFailure`) when the status is not
    /// Good (Bad or Uncertain severities both count).
    pub bad_status: Option<String>,
    /// The variant type's name when the value is outside the supported
    /// mapping.
    pub unsupported: Option<&'static str>,
}

/// Maps one data change's value, honoring its status first.
pub(crate) fn map_data_value(dv: &DataValue) -> MappedValue {
    let bad_status = dv.status.filter(|s| !s.is_good()).map(|s| status_name(&s));

    let (json, unsupported) = match dv.value.as_ref() {
        Some(variant) if bad_status.is_none() => match variant_json(variant) {
            Ok(json) => (json, None),
            Err(name) => (Value::Null, Some(name)),
        },
        // A non-Good status or a missing value maps to null on its own.
        _ => (Value::Null, None),
    };

    MappedValue {
        json,
        bad_status,
        unsupported,
    }
}

/// The newest timestamp carried by `values`, in microseconds since the Unix
/// epoch: the most recent source timestamp when any value carries one, else
/// the most recent server timestamp, else `None` (the caller falls back to
/// the receive time).
pub(crate) fn newest_timestamp_us(values: &[DataValue]) -> Option<i64> {
    fn newest(values: &[DataValue], pick: impl Fn(&DataValue) -> Option<DateTime>) -> Option<i64> {
        values
            .iter()
            .filter_map(|v| pick(v).map(|dt| dt.as_chrono().timestamp_micros()))
            .max()
    }
    newest(values, |v| v.source_timestamp).or_else(|| newest(values, |v| v.server_timestamp))
}

/// Maps a variant to JSON, or reports its OPC UA type name as unsupported.
fn variant_json(variant: &Variant) -> Result<Value, &'static str> {
    let json = match variant {
        Variant::Empty => Value::Null,
        Variant::Boolean(b) => Value::from(*b),
        Variant::SByte(v) => Value::from(*v),
        Variant::Byte(v) => Value::from(*v),
        Variant::Int16(v) => Value::from(*v),
        Variant::UInt16(v) => Value::from(*v),
        Variant::Int32(v) => Value::from(*v),
        Variant::UInt32(v) => Value::from(*v),
        Variant::Int64(v) => Value::from(*v),
        Variant::UInt64(v) => Value::from(*v),
        Variant::Float(v) => finite_f64(f64::from(*v)).ok_or("Float")?,
        Variant::Double(v) => finite_f64(*v).ok_or("Double")?,
        Variant::String(s) => match s.value() {
            Some(text) => Value::from(text.clone()),
            None => Value::Null,
        },
        Variant::DateTime(dt) => {
            Value::from(dt.as_chrono().to_rfc3339_opts(SecondsFormat::AutoSi, true))
        }
        Variant::ByteString(bs) => match bs.value.as_ref() {
            Some(bytes) => Value::from(base64::engine::general_purpose::STANDARD.encode(bytes)),
            None => Value::Null,
        },
        Variant::Array(array) => array_json(array)?,
        other => return Err(type_name(other)),
    };
    Ok(json)
}

/// Arrays of booleans and mapped numeric types become JSON arrays.
fn array_json(array: &opcua::types::Array) -> Result<Value, &'static str> {
    use opcua::types::VariantScalarTypeId as Ty;
    let ok = matches!(
        array.value_type,
        Ty::Boolean
            | Ty::SByte
            | Ty::Byte
            | Ty::Int16
            | Ty::UInt16
            | Ty::Int32
            | Ty::UInt32
            | Ty::Int64
            | Ty::UInt64
    ) || matches!(array.value_type, Ty::Float | Ty::Double)
        && array.values.iter().all(|v| match v {
            Variant::Float(f) => f.is_finite(),
            Variant::Double(f) => f.is_finite(),
            _ => false,
        });
    if !ok {
        return Err("Array");
    }
    let mut out = Vec::with_capacity(array.values.len());
    for value in &array.values {
        out.push(variant_json(value)?);
    }
    Ok(Value::Array(out))
}

/// `Some` finite number as JSON, `None` for NaN and infinities.
fn finite_f64(v: f64) -> Option<Value> {
    serde_json::Number::from_f64(v).map(Value::from)
}

/// A variant type's name, for the unsupported-value report.
fn type_name(variant: &Variant) -> &'static str {
    match variant {
        Variant::Guid(_) => "Guid",
        Variant::StatusCode(_) => "StatusCode",
        Variant::XmlElement(_) => "XmlElement",
        Variant::QualifiedName(_) => "QualifiedName",
        Variant::LocalizedText(_) => "LocalizedText",
        Variant::NodeId(_) => "NodeId",
        Variant::ExpandedNodeId(_) => "ExpandedNodeId",
        Variant::ExtensionObject(_) => "ExtensionObject",
        Variant::Variant(_) => "Variant",
        Variant::DataValue(_) => "DataValue",
        Variant::DiagnosticInfo(_) => "DiagnosticInfo",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::types::StatusCode;
    use opcua::types::{Array, ByteString, UAString, VariantScalarTypeId};

    fn dv(variant: Variant, status: StatusCode) -> DataValue {
        DataValue {
            value: Some(variant),
            status: Some(status),
            ..DataValue::default()
        }
    }

    fn date_time(micros: i64) -> DateTime {
        chrono::DateTime::from_timestamp_micros(micros)
            .expect("in range")
            .into()
    }

    #[test]
    fn scalars_map_to_json() {
        let cases: Vec<(Variant, Value)> = vec![
            (Variant::Boolean(true), Value::from(true)),
            (Variant::SByte(-3), Value::from(-3)),
            (Variant::Byte(250u8), Value::from(250u64)),
            (Variant::Int32(i32::MIN), Value::from(i64::from(i32::MIN))),
            (Variant::UInt64(u64::MAX), Value::from(u64::MAX)),
            (Variant::Float(0.5), Value::from(0.5)),
            (Variant::Double(-1.25), Value::from(-1.25)),
            (
                Variant::String(UAString::from("pump".to_owned())),
                Value::from("pump"),
            ),
            (
                Variant::ByteString(ByteString::from(vec![0xDEu8, 0xAD])),
                Value::from("3q0="),
            ),
            (Variant::Empty, Value::Null),
        ];
        for (variant, expected) in cases {
            let mapped = map_data_value(&dv(variant, StatusCode::Good));
            assert_eq!(mapped.json, expected);
            assert_eq!(mapped.bad_status, None);
            assert_eq!(mapped.unsupported, None);
        }
    }

    #[test]
    fn datetimes_map_to_rfc3339_and_micros() {
        let dt = date_time(1_752_435_304_123_456);
        let mapped = map_data_value(&dv(Variant::DateTime(Box::new(dt)), StatusCode::Good));
        assert_eq!(mapped.json, Value::from("2025-07-13T19:35:04.123456Z"));
    }

    #[test]
    fn numeric_arrays_map_and_float_arrays_with_nans_do_not() {
        let ints = Array {
            value_type: VariantScalarTypeId::Int16,
            values: vec![Variant::Int16(1), Variant::Int16(-2)],
            dimensions: None,
        };
        let mapped = map_data_value(&dv(Variant::Array(Box::new(ints)), StatusCode::Good));
        assert_eq!(mapped.json, serde_json::json!([1, -2]));

        let nans = Array {
            value_type: VariantScalarTypeId::Double,
            values: vec![Variant::Double(1.0), Variant::Double(f64::NAN)],
            dimensions: None,
        };
        let mapped = map_data_value(&dv(Variant::Array(Box::new(nans)), StatusCode::Good));
        assert_eq!(mapped.json, Value::Null);
        assert_eq!(mapped.unsupported, Some("Array"));

        let strings = Array {
            value_type: VariantScalarTypeId::String,
            values: vec![Variant::String(UAString::from("a".to_owned()))],
            dimensions: None,
        };
        let mapped = map_data_value(&dv(Variant::Array(Box::new(strings)), StatusCode::Good));
        assert_eq!(mapped.unsupported, Some("Array"));
    }

    #[test]
    fn non_finite_scalars_are_unsupported() {
        let mapped = map_data_value(&dv(Variant::Double(f64::INFINITY), StatusCode::Good));
        assert_eq!(mapped.json, Value::Null);
        assert_eq!(mapped.unsupported, Some("Double"));
    }

    #[test]
    fn a_bad_status_wins_over_the_value() {
        let mut value = dv(Variant::Int32(7), StatusCode::BadSensorFailure);
        value.source_timestamp = Some(DateTime::now());
        let mapped = map_data_value(&value);
        assert_eq!(mapped.json, Value::Null);
        assert_eq!(mapped.bad_status.as_deref(), Some("BadSensorFailure"));
        assert_eq!(mapped.unsupported, None);

        let uncertain = map_data_value(&dv(
            Variant::Int32(7),
            StatusCode::UncertainSensorNotAccurate,
        ));
        assert_eq!(
            uncertain.bad_status.as_deref(),
            Some("UncertainSensorNotAccurate")
        );
        assert_eq!(uncertain.json, Value::Null);
    }

    #[test]
    fn unsupported_variants_report_their_type() {
        let mapped = map_data_value(&dv(
            Variant::QualifiedName(Box::default()),
            StatusCode::Good,
        ));
        assert_eq!(mapped.json, Value::Null);
        assert_eq!(mapped.unsupported, Some("QualifiedName"));
    }

    #[test]
    fn missing_status_means_good_and_missing_value_is_null() {
        let mapped = map_data_value(&DataValue::default());
        assert_eq!(mapped.json, Value::Null);
        assert_eq!(mapped.bad_status, None);
    }

    #[test]
    fn newest_timestamp_prefers_source_over_server() {
        let source = date_time(100);
        let server = date_time(999);
        let values = [
            DataValue {
                value: None,
                status: None,
                source_timestamp: None,
                server_timestamp: Some(server),
                ..DataValue::default()
            },
            DataValue {
                value: None,
                status: None,
                source_timestamp: Some(source),
                server_timestamp: Some(server),
                ..DataValue::default()
            },
        ];
        assert_eq!(newest_timestamp_us(&values), Some(100));
        assert_eq!(newest_timestamp_us(&values[..1]), Some(999));
        assert_eq!(newest_timestamp_us(&values[..0]), None);
    }
}
