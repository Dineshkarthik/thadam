//! The per-event subject template and the `payload: json` document.
//!
//! A subject template such as `thadam.{dataset}.{robot}` is a dot-joined
//! token list where each `{name}` placeholder is a whole token. `{dataset}`
//! renders the dataset id; any other name renders the asset identity level
//! of that name. Rendering happens per event, so an event that lacks the
//! level a template needs cannot be routed and is rejected individually.
//!
//! The JSON document merges three regions into one object: the `_`-prefixed
//! lineage fields, the identity levels, and the record fields. Later regions
//! never overwrite an earlier key, so `_event_id` is always the event id even
//! when a record field happens to share the name. Times are RFC 3339 with
//! nanosecond precision (integer nanoseconds would lose precision through a
//! JSON number beyond 2^53); non-finite floats become `null` (JSON has no
//! NaN); `bytes` values are lowercase hex.

use thadam_core::codes;
use thadam_core::{AssetPath, ErrorDomain, Event, Payload, ThadamError, Timestamp, Value};

/// Why a template string could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TemplateParseError(pub String);

/// Why one event could not be rendered or encoded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EnvelopeError {
    /// The event's identity has no value for a level the template names.
    #[error("identity has no level `{level}` named by the subject template")]
    MissingLevel {
        /// The missing level.
        level: String,
    },
    /// A level value cannot appear in a NATS subject token.
    #[error("identity level `{level}` has a value that is unusable in a subject: {reason}")]
    UnusableValue {
        /// The level.
        level: String,
        /// What is wrong with the value.
        reason: String,
    },
    /// `payload: json` needs a decoded record; this event has another
    /// payload kind.
    #[error("event payload is not a record ({what})")]
    NotARecord {
        /// What the payload actually is.
        what: String,
    },
}

impl EnvelopeError {
    /// The permanent per-event reject this failure becomes.
    pub fn into_reject_error(self) -> ThadamError {
        ThadamError::permanent(ErrorDomain::Sink, codes::SINK_REJECTED, self.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Literal(String),
    Var(String),
}

/// A parsed `subject` template.
///
/// Placeholders are whole dot-separated tokens: `thadam.{dataset}.{robot}`.
/// A partially interpolated token such as `pre-{robot}` is refused at parse
/// time because it has no wildcard equivalent for stream subject binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectTemplate {
    parts: Vec<Part>,
}

/// Subject tokens must not contain these; `{dataset}` renders from
/// configuration, level values from data — both are checked.
fn token_problem(token: &str) -> Option<&'static str> {
    if token.is_empty() {
        return Some("subject tokens must not be empty");
    }
    if token
        .bytes()
        .any(|b| matches!(b, b'.' | b'*' | b'>' | b'{' | b'}') || b.is_ascii_whitespace())
    {
        return Some("subject tokens must not contain `. * > { }` or whitespace");
    }
    None
}

impl SubjectTemplate {
    /// Parses and validates `text`.
    pub fn parse(text: &str) -> Result<Self, TemplateParseError> {
        let err = |m: &str| TemplateParseError(format!("{m} (in `{text}`)"));
        if text.is_empty() {
            return Err(err("the subject template must not be empty"));
        }

        // Split into literals and {vars}; braces cannot nest or dangle.
        let mut parts: Vec<Part> = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find('{') {
            let (lit, after) = rest.split_at(open);
            if !lit.is_empty() {
                parts.push(Part::Literal(lit.to_owned()));
            }
            let Some(close) = after.find('}') else {
                return Err(err("unclosed `{`"));
            };
            let name = &after[1..close];
            if name.is_empty() {
                return Err(err("empty `{}` placeholder"));
            }
            if let Some(why) = token_problem(name) {
                return Err(err(&format!("placeholder name `{name}`: {why}")));
            }
            parts.push(Part::Var(name.to_owned()));
            rest = &after[close + 1..];
        }
        if !rest.is_empty() {
            if rest.contains('}') {
                return Err(err("stray `}` outside a placeholder"));
            }
            parts.push(Part::Literal(rest.to_owned()));
        }

        // Whole-template shape: no empty tokens, no forbidden characters in
        // literals, placeholders already checked.
        let mut rendered = String::new();
        for part in &parts {
            match part {
                Part::Literal(l) => rendered.push_str(l),
                Part::Var(_) => rendered.push('x'),
            }
        }
        // Structural problems (empty tokens) outrank lexical ones, and both
        // outrank the whole-token placeholder rule.
        for token in rendered.split('.') {
            if token.is_empty() {
                return Err(err("subject tokens must not be empty"));
            }
        }
        for token in rendered.split('.') {
            if let Some(why) = token_problem(token) {
                return Err(err(why));
            }
        }

        // Every placeholder must span a whole token (start of template or
        // after a dot, end of template or before a dot).
        for (i, part) in parts.iter().enumerate() {
            if !matches!(part, Part::Var(_)) {
                continue;
            }
            let left_ok = i == 0
                || matches!(parts.get(i.wrapping_sub(1)), Some(Part::Literal(l)) if l.ends_with('.'));
            let right_ok = i + 1 == parts.len()
                || matches!(parts.get(i + 1), Some(Part::Literal(l)) if l.starts_with('.'));
            if !left_ok || !right_ok {
                return Err(err(
                    "a placeholder must be a whole subject token between dots",
                ));
            }
        }
        Ok(SubjectTemplate { parts })
    }

    /// The placeholder names the template uses, in occurrence order.
    pub fn variables(&self) -> Vec<&str> {
        self.parts
            .iter()
            .filter_map(|p| match p {
                Part::Var(name) => Some(name.as_str()),
                Part::Literal(_) => None,
            })
            .collect()
    }

    /// The subject pattern with every placeholder replaced by `*`, for the
    /// `subjects` of a stream this sink creates.
    pub fn wildcard(&self) -> String {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(l) => out.push_str(l),
                Part::Var(_) => out.push('*'),
            }
        }
        out
    }

    /// Renders the subject for one event. `{dataset}` takes the dataset id;
    /// other placeholders read the event's identity levels.
    pub fn render(&self, dataset: &str, asset: &AssetPath) -> Result<String, EnvelopeError> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(l) => out.push_str(l),
                Part::Var(name) => {
                    let value = if name == "dataset" {
                        Some(dataset)
                    } else {
                        asset.get(name)
                    };
                    let Some(value) = value else {
                        return Err(EnvelopeError::MissingLevel {
                            level: name.clone(),
                        });
                    };
                    if let Some(why) = token_problem(value) {
                        return Err(EnvelopeError::UnusableValue {
                            level: name.clone(),
                            reason: why.to_owned(),
                        });
                    }
                    out.push_str(value);
                }
            }
        }
        Ok(out)
    }
}

/// Formats `ts` as RFC 3339 with nanosecond precision, UTC (`Z`).
///
/// No datetime crate: days-civil decomposition (Howard Hinnant's
/// `civil_from_days`, the inverse of the one the object-store docker tests
/// use for AWS dates).
pub fn rfc3339_nanos(ts: Timestamp) -> String {
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
pub fn value_json(value: &Value) -> serde_json::Value {
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

/// The `payload: json` document for one event, serialized.
///
/// Merges lineage (`_event_id`, `_ingest_time`, `_event_time`,
/// `_quality_status`), identity levels, and record fields; a later region
/// never overwrites an earlier key. The value is serialized once into the
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
    serde_json::to_vec(&serde_json::Value::Object(map)).map_err(|e| EnvelopeError::NotARecord {
        what: format!("record cannot be encoded as JSON: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::{
        AssetPath, Event, EventId, FieldDef, FieldType, Record, Shape, ShapeRef, TimeDomain, Times,
        WalPos,
    };

    fn template(text: &str) -> SubjectTemplate {
        SubjectTemplate::parse(text).expect("parse")
    }

    #[test]
    fn parses_literals_and_placeholders() {
        let t = template("thadam.{dataset}.{robot}");
        assert_eq!(t.variables(), ["dataset", "robot"]);
        assert_eq!(t.wildcard(), "thadam.*.*");
        assert_eq!(template("a.b.c").variables().len(), 0);
        assert_eq!(template("a.b.c").wildcard(), "a.b.c");
    }

    #[test]
    fn parse_rejects_bad_templates() {
        for (text, want) in [
            ("thadam.{dataset", "unclosed"),
            ("thadam.}", "stray"),
            ("thadam.{}", "empty"),
            ("pre-{robot}", "whole subject token"),
            ("{robot}-x", "whole subject token"),
            ("thadam.*.", "must not be empty"),
            ("thadam..b", "must not be empty"),
            (".thadam.b", "must not be empty"),
            ("thadam.>{dataset}", "must not contain"),
            ("r i v e t.x", "must not contain"),
            ("", "must not be empty"),
        ] {
            let err = SubjectTemplate::parse(text).expect_err(text);
            assert!(err.0.contains(want), "`{text}`: missing `{want}` in {err}");
        }
    }

    #[test]
    fn renders_dataset_and_identity_levels() {
        let t = template("thadam.{dataset}.{robot}");
        let asset = AssetPath::new([("robot", "r7")]).expect("path");
        assert_eq!(t.render("imu", &asset).expect("render"), "thadam.imu.r7");

        let err = t.render("imu", &AssetPath::empty()).expect_err("no robot");
        assert_eq!(
            err,
            EnvelopeError::MissingLevel {
                level: "robot".into()
            }
        );

        let dotted = AssetPath::new([("robot", "r.7")]).expect("path");
        let err = t.render("imu", &dotted).expect_err("dotted value");
        assert!(matches!(err, EnvelopeError::UnusableValue { .. }));

        let err = err.into_reject_error();
        assert_eq!(err.code, codes::SINK_REJECTED);
        assert!(!err.is_transient());
    }

    #[test]
    fn event_id_placeholder_is_not_special_cased() {
        let t = template("r.{dataset}");
        let empty = AssetPath::empty();
        assert_eq!(t.render("d", &empty).expect("render"), "r.d");
    }

    #[test]
    fn rfc3339_formats_epoch_leap_and_negative_times() {
        assert_eq!(
            rfc3339_nanos(Timestamp::from_nanos(0)),
            "1970-01-01T00:00:00.000000000Z"
        );
        // 2024-02-29T12:34:56.787898989Z
        let nanos = 1_709_210_096_787_898_989i64;
        assert_eq!(
            rfc3339_nanos(Timestamp::from_nanos(nanos)),
            "2024-02-29T12:34:56.787898989Z"
        );
        // One nanosecond before the epoch lands in 1969.
        assert_eq!(
            rfc3339_nanos(Timestamp::from_nanos(-1)),
            "1969-12-31T23:59:59.999999999Z"
        );
    }

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

    #[test]
    fn envelope_merges_regions_with_lineage_winning_keys() {
        let event = record_event(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        let bytes = event_json(&event).expect("json");
        let doc: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
        assert_eq!(doc["_event_id"], "0123456789abcdef0011223344556677");
        assert_eq!(doc["_ingest_time"], "1970-01-01T00:00:00.000001000Z");
        assert!(doc["_event_time"].is_null(), "no source or correction");
        assert_eq!(doc["_quality_status"], "good");
        assert_eq!(doc["robot"], "r7");
        assert_eq!(doc["v"], 42, "record field");
        let name_collision = doc["_event_id"].as_str().expect("lineage stays");
        assert_eq!(name_collision, "0123456789abcdef0011223344556677");
    }

    #[test]
    fn envelope_event_time_follows_the_domain_rule() {
        let mut times = Times::new(Timestamp::from_nanos(1_000), TimeDomain::Utc);
        times
            .set_source(Timestamp::from_nanos(500))
            .expect("source");
        let shape = ShapeRef::new(Shape::new(vec![]).expect("shape"));
        let event = Event::builder(EventId::from_u128(1), WalPos::new(1), times)
            .payload(Record::new(shape, vec![]).expect("record"))
            .build()
            .expect("event");
        let doc: serde_json::Value =
            serde_json::from_slice(&event_json(&event).expect("json")).expect("valid json");
        assert_eq!(doc["_event_time"], "1970-01-01T00:00:00.000000500Z");
    }

    #[test]
    fn envelope_rejects_non_record_payloads() {
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
    }

    #[test]
    fn value_json_maps_the_extended_value_set() {
        use std::sync::Arc;
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
