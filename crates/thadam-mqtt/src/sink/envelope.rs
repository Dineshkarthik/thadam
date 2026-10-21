//! The `payload: json` document for one event.
//!
//! The document merges three regions into one object: the `_`-prefixed
//! lineage fields, the identity levels, and the record fields. Later regions
//! never overwrite an earlier key, so `_event_id` is always the event id even
//! when a record field happens to share the name. Times are RFC 3339 with
//! nanosecond precision (integer nanoseconds would lose precision through a
//! JSON number beyond 2^53); non-finite floats become `null` (JSON has no
//! NaN); `bytes` values are lowercase hex.

use thadam_core::{ErrorDomain, Event, Payload, ThadamError, Timestamp, Value, codes};

/// Why one event could not be encoded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EnvelopeError {
    /// `payload: json` needs a decoded record; this event has another payload kind.
    #[error("event payload is not a record ({what})")]
    NotARecord {
        /// What the payload actually is.
        what: String,
    },
    /// The record could not be serialized as JSON.
    #[error("record cannot be encoded as JSON: {reason}")]
    Encode {
        /// The serializer's message.
        reason: String,
    },
}

impl EnvelopeError {
    /// The permanent per-event reject this failure becomes.
    pub fn into_reject_error(self) -> ThadamError {
        ThadamError::permanent(ErrorDomain::Sink, codes::SINK_REJECTED, self.to_string())
    }
}

/// Formats `ts` as RFC 3339 with nanosecond precision, UTC (`Z`).
///
/// No datetime crate: days-civil decomposition (Howard Hinnant's
/// `civil_from_days`).
pub(crate) fn rfc3339_nanos(ts: Timestamp) -> String {
    let nanos = ts.as_nanos();
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let day_secs = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (day_secs / 3600, (day_secs % 3600) / 60, day_secs % 60);

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{sub:09}Z")
}

/// A `Value` as JSON. Non-finite floats become `null`; `Bytes` hex-encodes;
/// timestamps render with [`rfc3339_nanos`].
pub(crate) fn value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => (*b).into(),
        Value::I8(v) => i64::from(*v).into(),
        Value::I16(v) => i64::from(*v).into(),
        Value::I32(v) => i64::from(*v).into(),
        Value::I64(v) => (*v).into(),
        Value::U8(v) => u64::from(*v).into(),
        Value::U16(v) => u64::from(*v).into(),
        Value::U32(v) => u64::from(*v).into(),
        Value::U64(v) => (*v).into(),
        Value::F32(v) => f64_json(f64::from(*v)),
        Value::F64(v) => f64_json(*v),
        Value::Str(s) => serde_json::Value::String(s.as_str().to_owned()),
        Value::Bytes(b) => serde_json::Value::String(hex(b)),
        Value::Ts(t) => serde_json::Value::String(rfc3339_nanos(*t)),
        Value::F32s(a) => {
            serde_json::Value::Array(a.iter().map(|v| f64_json(f64::from(*v))).collect())
        }
        Value::F64s(a) => serde_json::Value::Array(a.iter().map(|v| f64_json(*v)).collect()),
        Value::I32s(a) => {
            serde_json::Value::Array(a.iter().map(|v| i64::from(*v).into()).collect())
        }
        Value::I64s(a) => serde_json::Value::Array(a.iter().map(|v| (*v).into()).collect()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_json).collect()),
        Value::Record(record) => {
            let mut map = serde_json::Map::with_capacity(record.len());
            for (name, v) in record.iter() {
                map.insert(name.to_owned(), value_json(v));
            }
            serde_json::Value::Object(map)
        }
        // A variant added after this sink's build (the set is non-exhaustive)
        // serializes as null rather than failing the whole event.
        _ => serde_json::Value::Null,
    }
}

fn f64_json(v: f64) -> serde_json::Value {
    serde_json::Number::from_f64(v).map_or(serde_json::Value::Null, Into::into)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The `payload: json` document for one event, serialized once into the
/// exact bytes published.
pub fn event_json(event: &Event) -> Result<Vec<u8>, EnvelopeError> {
    let Some(record) = event.payload().as_record() else {
        let what = match event.payload() {
            Payload::Raw(raw) => format!("undecoded bytes ({})", raw.content_type),
            Payload::ObjectRef(obj) => format!("object reference ({})", obj.uri),
            _ => "unsupported payload kind".to_owned(),
        };
        return Err(EnvelopeError::NotARecord { what });
    };

    let times = event.times();
    let mut map = serde_json::Map::with_capacity(4 + event.asset().len() + record.len());
    map.insert(
        "_event_id".to_owned(),
        serde_json::Value::String(event.id().to_string()),
    );
    map.insert(
        "_ingest_time".to_owned(),
        serde_json::Value::String(rfc3339_nanos(times.ingest())),
    );
    map.insert(
        "_event_time".to_owned(),
        times.event_time().map_or(serde_json::Value::Null, |t| {
            serde_json::Value::String(rfc3339_nanos(t))
        }),
    );
    map.insert(
        "_quality_status".to_owned(),
        serde_json::Value::String(event.quality().status.as_str().to_owned()),
    );
    for (level, value) in event.asset().iter() {
        map.entry(level.to_owned())
            .or_insert_with(|| serde_json::Value::String(value.to_owned()));
    }
    for (name, value) in record.iter() {
        map.entry(name.to_owned())
            .or_insert_with(|| value_json(value));
    }
    serde_json::to_vec(&serde_json::Value::Object(map)).map_err(|e| EnvelopeError::Encode {
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use thadam_core::{
        AssetPath, EventId, FieldDef, FieldType, Record, Shape, ShapeRef, TimeDomain, Times, WalPos,
    };

    fn record_event(id: u128) -> Event {
        let shape = ShapeRef::new(
            Shape::new(vec![
                FieldDef::new("v", FieldType::I64),
                FieldDef::new("_event_id", FieldType::Str),
            ])
            .expect("shape"),
        );
        let record =
            Record::new(shape, vec![Value::I64(42), Value::str("record-owned")]).expect("record");
        Event::builder(
            EventId::from_u128(id),
            WalPos::new(1),
            Times::new(Timestamp::from_nanos(1_000), TimeDomain::Utc),
        )
        .asset(AssetPath::new([("robot", "r7")]).expect("path"))
        .payload(record)
        .build()
        .expect("event")
    }

    fn parse(event: &Event) -> serde_json::Value {
        serde_json::from_slice(&event_json(event).expect("json")).expect("valid json")
    }

    #[test]
    fn rfc3339_formats_epoch_leap_and_negative_times() {
        assert_eq!(
            rfc3339_nanos(Timestamp::from_nanos(0)),
            "1970-01-01T00:00:00.000000000Z"
        );
        assert_eq!(
            rfc3339_nanos(Timestamp::from_nanos(1_709_210_096_787_898_989)),
            "2024-02-29T12:34:56.787898989Z"
        );
        assert_eq!(
            rfc3339_nanos(Timestamp::from_nanos(-1)),
            "1969-12-31T23:59:59.999999999Z"
        );
    }

    #[test]
    fn envelope_has_exactly_the_documented_fields_and_types() {
        let doc = parse(&record_event(0x0123_4567_89ab_cdef_0011_2233_4455_6677));
        let mut keys: Vec<&str> = doc
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "_event_id",
                "_event_time",
                "_ingest_time",
                "_quality_status",
                "robot",
                "v"
            ]
        );
        assert!(doc["_event_id"].is_string());
        assert_eq!(doc["_event_id"].as_str().map(str::len), Some(32));
        assert_eq!(doc["_event_id"], "0123456789abcdef0011223344556677");
        assert_eq!(doc["_ingest_time"], "1970-01-01T00:00:00.000001000Z");
        assert!(doc["_event_time"].is_null(), "no source or correction");
        assert_eq!(doc["_quality_status"], "good");
        assert_eq!(doc["robot"], "r7");
        assert!(doc["v"].is_i64());
        assert_eq!(doc["v"], 42);
    }

    #[test]
    fn lineage_wins_over_a_record_field_of_the_same_name() {
        let doc = parse(&record_event(1));
        assert_eq!(doc["_event_id"], EventId::from_u128(1).to_string());
    }

    #[test]
    fn event_time_follows_the_source_time_rule() {
        let mut times = Times::new(Timestamp::from_nanos(1_000), TimeDomain::Utc);
        times
            .set_source(Timestamp::from_nanos(500))
            .expect("source");
        let shape = ShapeRef::new(Shape::new(vec![]).expect("shape"));
        let event = Event::builder(EventId::from_u128(1), WalPos::new(1), times)
            .payload(Record::new(shape, vec![]).expect("record"))
            .build()
            .expect("event");
        let doc = parse(&event);
        assert_eq!(doc["_event_time"], "1970-01-01T00:00:00.000000500Z");
    }

    #[test]
    fn non_record_payloads_are_rejected() {
        let times = Times::new(Timestamp::from_nanos(1_000), TimeDomain::Utc);
        let event = Event::builder(EventId::from_u128(1), WalPos::new(1), times)
            .payload(Payload::raw(
                "application/octet-stream",
                bytes::Bytes::new(),
            ))
            .build()
            .expect("event");
        let err = event_json(&event).expect_err("raw payload");
        assert!(matches!(err, EnvelopeError::NotARecord { .. }));
        assert_eq!(err.into_reject_error().code, codes::SINK_REJECTED);
    }

    #[test]
    fn value_json_maps_the_extended_value_set() {
        let shape =
            ShapeRef::new(Shape::new(vec![FieldDef::new("f", FieldType::F64)]).expect("shape"));
        let nested = Value::record(Record::new(shape, vec![Value::F64(1.5)]).expect("record"));
        let cases: Vec<(Value, serde_json::Value)> = vec![
            (Value::Null, serde_json::Value::Null),
            (Value::Bool(true), true.into()),
            (Value::I64(-7), (-7).into()),
            (Value::U64(u64::MAX), u64::MAX.into()),
            (Value::F64(f64::NAN), serde_json::Value::Null),
            (Value::F64(2.5), 2.5.into()),
            (Value::str("s"), "s".into()),
            (
                Value::bytes(bytes::Bytes::from_static(b"\xde\xad")),
                "dead".into(),
            ),
            (
                Value::Ts(Timestamp::from_nanos(60)),
                "1970-01-01T00:00:00.000000060Z".into(),
            ),
            (
                Value::F64s(Arc::from([1.0, f64::NAN])),
                serde_json::json!([1.0, null]),
            ),
            (
                Value::List(vec![Value::I32(1), Value::Null]),
                serde_json::json!([1, null]),
            ),
            (nested, serde_json::json!({"f": 1.5})),
        ];
        for (value, want) in cases {
            assert_eq!(value_json(&value), want, "{value:?}");
        }
    }
}
