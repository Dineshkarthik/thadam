//! Source-time extraction: units, domains, and skew flagging.

use serde_json::Value as Json;
use thadam_core::{
    Attributes, ErrorDomain, QualityFlags, Record, ThadamError, TimeDomain, Times, Timestamp,
    Value, codes,
};

use crate::paths::FieldPath;
use crate::spec::TimeSpec;
use crate::timeparse::{EpochUnit, TimeParseError, UtcOffset, parse_rfc3339};

/// Reads one JSON value, or one attribute's text, as nanoseconds since the epoch.
#[derive(Clone, Debug)]
pub(crate) enum TimeParser {
    Epoch(EpochUnit),
    Rfc3339(Option<UtcOffset>),
}

#[derive(Debug)]
pub(crate) enum TimeIssue {
    Type(&'static str),
    Range,
    Text(TimeParseError),
    Number,
}

impl TimeIssue {
    pub(crate) fn into_error(self, what: &str) -> ThadamError {
        let detail = match self {
            TimeIssue::Type(found) => format!("expected a time value, found {found}"),
            TimeIssue::Range => "time is outside the representable range".to_owned(),
            TimeIssue::Text(e) => e.to_string(),
            TimeIssue::Number => "expected a whole number of the time unit".to_owned(),
        };
        ThadamError::permanent(
            ErrorDomain::Decode,
            codes::DECODE_INVALID_TIME,
            format!("{what}: {detail}"),
        )
    }
}

fn json_kind(j: &Json) -> &'static str {
    match j {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

/// [`json_kind`] for a decoded record value: the word the JSON bridge's
/// spelling would have produced (`Ts` and every number are "number"; the
/// array variants are "array").
fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::I8(_)
        | Value::I16(_)
        | Value::I32(_)
        | Value::I64(_)
        | Value::U8(_)
        | Value::U16(_)
        | Value::U32(_)
        | Value::U64(_)
        | Value::F32(_)
        | Value::F64(_)
        | Value::Ts(_) => "number",
        Value::Str(_) => "string",
        Value::Bytes(_)
        | Value::F32s(_)
        | Value::F64s(_)
        | Value::I32s(_)
        | Value::I64s(_)
        | Value::List(_) => "array",
        Value::Record(_) => "object",
        _ => "unknown",
    }
}

impl TimeParser {
    pub(crate) fn compile(unit: &str, default_offset: Option<&str>) -> Result<Self, String> {
        if unit == "rfc3339" {
            let offset = default_offset
                .map(|o| UtcOffset::parse(o).map_err(|e| format!("default_offset: {e}")))
                .transpose()?;
            return Ok(TimeParser::Rfc3339(offset));
        }
        if default_offset.is_some() {
            return Err("`default_offset` only applies to unit `rfc3339`".to_owned());
        }
        EpochUnit::parse(unit)
            .map(TimeParser::Epoch)
            .ok_or_else(|| format!("unknown time unit `{unit}`; use s, ms, us, ns, or rfc3339"))
    }

    pub(crate) fn parse(&self, j: &Json) -> Result<i64, TimeIssue> {
        match (self, j) {
            (TimeParser::Epoch(unit), Json::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    unit.int_to_nanos(i).ok_or(TimeIssue::Range)
                } else if n.is_u64() {
                    Err(TimeIssue::Range)
                } else {
                    n.as_f64()
                        .and_then(|f| unit.float_to_nanos(f))
                        .ok_or(TimeIssue::Range)
                }
            }
            (TimeParser::Rfc3339(offset), Json::String(s)) => {
                parse_rfc3339(s, *offset).map_err(TimeIssue::Text)
            }
            (_, other) => Err(TimeIssue::Type(json_kind(other))),
        }
    }

    /// Reads a decoded record value as nanoseconds since the epoch — the
    /// direct-read counterpart of [`TimeParser::parse`]. The ROS 2 decoder's
    /// `builtin_interfaces/Time` stamp is a `Ts` already: with `unit: ns`
    /// it is taken as-is; with another epoch unit its nanosecond count is
    /// read in that unit, which is what the JSON bridge spelled.
    pub(crate) fn parse_value(&self, v: &Value) -> Result<i64, TimeIssue> {
        match (self, v) {
            (TimeParser::Epoch(EpochUnit::Nanos), Value::Ts(ts)) => Ok(ts.as_nanos()),
            (TimeParser::Epoch(unit), Value::Ts(ts)) => {
                unit.int_to_nanos(ts.as_nanos()).ok_or(TimeIssue::Range)
            }
            (TimeParser::Epoch(unit), Value::F32(x)) => {
                unit.float_to_nanos(f64::from(*x)).ok_or(TimeIssue::Range)
            }
            (TimeParser::Epoch(unit), Value::F64(x)) => {
                unit.float_to_nanos(*x).ok_or(TimeIssue::Range)
            }
            (TimeParser::Epoch(unit), other) => {
                if let Some(i) = other.as_i64() {
                    unit.int_to_nanos(i).ok_or(TimeIssue::Range)
                } else if other.as_u64().is_some() {
                    Err(TimeIssue::Range)
                } else {
                    Err(TimeIssue::Type(value_kind(other)))
                }
            }
            (TimeParser::Rfc3339(offset), Value::Str(s)) => {
                parse_rfc3339(s.as_str(), *offset).map_err(TimeIssue::Text)
            }
            (_, other) => Err(TimeIssue::Type(value_kind(other))),
        }
    }

    /// Reads an attribute's text. Epoch units take a whole decimal number.
    pub(crate) fn parse_text(&self, text: &str) -> Result<i64, TimeIssue> {
        match self {
            TimeParser::Epoch(unit) => {
                let i = text.parse::<i64>().map_err(|_| TimeIssue::Number)?;
                unit.int_to_nanos(i).ok_or(TimeIssue::Range)
            }
            TimeParser::Rfc3339(offset) => parse_rfc3339(text, *offset).map_err(TimeIssue::Text),
        }
    }
}

#[derive(Clone, Debug)]
enum TimeSource {
    Field(FieldPath),
    Attr(String),
}

/// A dataset's time extraction.
#[derive(Clone, Debug)]
pub(crate) struct TimePlan {
    source: TimeSource,
    parser: TimeParser,
    domain: TimeDomain,
    max_skew_ns: Option<i128>,
}

pub(crate) fn parse_domain(s: &str) -> Option<TimeDomain> {
    TimeDomain::ALL.into_iter().find(|d| d.as_str() == s)
}

impl TimePlan {
    pub(crate) fn compile(spec: &TimeSpec) -> Result<TimePlan, Vec<(&'static str, String)>> {
        let mut errs = Vec::new();
        let source = match (&spec.field, &spec.attr) {
            (Some(field), None) => FieldPath::parse(field)
                .map(TimeSource::Field)
                .map_err(|e| errs.push(("field", e.to_string())))
                .ok(),
            (None, Some(attr)) if !attr.is_empty() => Some(TimeSource::Attr(attr.clone())),
            (None, Some(_)) => {
                errs.push(("attr", "`attr` must name a message attribute".to_owned()));
                None
            }
            _ => {
                errs.push((
                    "field",
                    "`time` needs exactly one of `field` or `attr`".to_owned(),
                ));
                None
            }
        };
        let parser = TimeParser::compile(&spec.unit, spec.default_offset.as_deref())
            .map_err(|e| errs.push(("unit", e)))
            .ok();
        let domain = match spec.domain.as_deref() {
            None => Some(TimeDomain::Unknown),
            Some(d) => parse_domain(d).or_else(|| {
                errs.push((
                    "domain",
                    format!("unknown domain `{d}`; use utc, tai, monotonic, sim, or unknown"),
                ));
                None
            }),
        };
        match (source, parser, domain) {
            (Some(source), Some(parser), Some(domain)) if errs.is_empty() => Ok(TimePlan {
                source,
                parser,
                domain,
                max_skew_ns: spec.max_skew.map(|d| d.0.as_nanos() as i128),
            }),
            _ => Err(errs),
        }
    }

    pub(crate) fn domain(&self) -> TimeDomain {
        self.domain
    }

    /// Whether the payload side of this plan names one flattened record
    /// field. An attribute source always qualifies: it never touches the
    /// payload.
    pub(crate) fn uses_flat_pointer(&self) -> bool {
        match &self.source {
            TimeSource::Field(path) => path.sole_token().is_some(),
            TimeSource::Attr(_) => true,
        }
    }

    /// An attribute's text as nanoseconds, or `None` when absent.
    fn parse_attr(&self, key: &str, attrs: &Attributes) -> Result<Option<i64>, ThadamError> {
        attrs
            .get(key)
            .map(|text| {
                self.parser
                    .parse_text(text)
                    .map_err(|issue| issue.into_error(&format!("time attribute `{key}`")))
            })
            .transpose()
    }

    /// The source time, or `None` if the message carries none. A payload
    /// field is read from `scope`; an attribute is read from `attrs`. Absent
    /// or null is `None`; a present value that does not parse is an error.
    pub(crate) fn extract(
        &self,
        scope: &Json,
        root: &Json,
        attrs: &Attributes,
    ) -> Result<Option<Timestamp>, ThadamError> {
        let parsed = match &self.source {
            TimeSource::Field(path) => match path.resolve(scope, root) {
                None | Some(Json::Null) => return Ok(None),
                Some(j) => self
                    .parser
                    .parse(j)
                    .map_err(|issue| issue.into_error("time field")),
            },
            TimeSource::Attr(key) => {
                return self
                    .parse_attr(key, attrs)
                    .map(|opt| opt.map(Timestamp::from_nanos));
            }
        };
        parsed.map(|ns| Some(Timestamp::from_nanos(ns)))
    }

    /// The direct-read counterpart of [`TimePlan::extract`]: the payload
    /// field resolves to one named field of the decoded record, without the
    /// JSON bridge. `scope` works as in
    /// [`FieldPlan::extract_record`](crate::decode::FieldPlan::extract_record).
    pub(crate) fn extract_record(
        &self,
        scope: Option<&Record>,
        root: &Record,
        attrs: &Attributes,
    ) -> Result<Option<Timestamp>, ThadamError> {
        match &self.source {
            TimeSource::Field(path) => {
                let record = if path.is_parent() { Some(root) } else { scope };
                let Some(token) = path.sole_token() else {
                    return Err(ThadamError::fatal(
                        ErrorDomain::Config,
                        codes::CONFIG_MISSING,
                        "a direct-read time plan is one pointer token (the compile gate missed it)",
                    ));
                };
                match record.and_then(|r| r.get(token)) {
                    None | Some(Value::Null) => Ok(None),
                    Some(v) => self
                        .parser
                        .parse_value(v)
                        .map(|ns| Some(Timestamp::from_nanos(ns)))
                        .map_err(|issue| issue.into_error("time field")),
                }
            }
            TimeSource::Attr(key) => self
                .parse_attr(key, attrs)
                .map(|opt| opt.map(Timestamp::from_nanos)),
        }
    }

    /// The times and quality flags for an event received at `ingest`.
    pub(crate) fn times(
        &self,
        ingest: Timestamp,
        source: Option<Timestamp>,
    ) -> (Times, QualityFlags) {
        let mut times = Times::new(ingest, self.domain);
        let mut flags = QualityFlags::empty();
        match source {
            Some(ts) => {
                if times.set_source(ts).is_ok()
                    && self.domain == TimeDomain::Utc
                    && let Some(max) = self.max_skew_ns
                {
                    let skew = i128::from(ts.as_nanos()) - i128::from(ingest.as_nanos());
                    if skew.abs() > max {
                        flags |= QualityFlags::CLOCK_SKEW;
                    }
                }
            }
            None => flags |= QualityFlags::MISSING_TIME,
        }
        (times, flags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(unit: &str, domain: Option<&str>, skew: Option<u64>) -> TimeSpec {
        let mut v = json!({ "field": "/t", "unit": unit });
        if let Some(d) = domain {
            v["domain"] = json!(d);
        }
        if let Some(s) = skew {
            v["max_skew"] = json!(format!("{s}s"));
        }
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn microseconds_become_nanoseconds() {
        let plan = TimePlan::compile(&spec("us", Some("utc"), None)).unwrap();
        let root = json!({ "t": 1_791_369_600_123_456i64 });
        let ts = plan
            .extract(&root, &root, &Attributes::new())
            .unwrap()
            .unwrap();
        assert_eq!(ts.as_nanos(), 1_791_369_600_123_456_000);
    }

    #[test]
    fn absent_and_null_are_missing_not_errors() {
        let plan = TimePlan::compile(&spec("ms", Some("utc"), None)).unwrap();
        let none = Attributes::new();
        assert_eq!(plan.extract(&json!({}), &json!({}), &none).unwrap(), None);
        assert_eq!(
            plan.extract(&json!({ "t": null }), &json!({}), &none)
                .unwrap(),
            None
        );
        let (_, flags) = plan.times(Timestamp::from_nanos(5), None);
        assert_eq!(flags, QualityFlags::MISSING_TIME);
    }

    #[test]
    fn wrong_types_and_overflow_are_decode_time_errors() {
        let plan = TimePlan::compile(&spec("s", Some("utc"), None)).unwrap();
        for bad in [
            json!("12"),
            json!(true),
            json!([1]),
            json!(i64::MAX),
            json!(u64::MAX),
            json!(1e30),
        ] {
            let err = plan
                .extract(&json!({ "t": bad }), &json!({}), &Attributes::new())
                .unwrap_err();
            assert_eq!(err.code, codes::DECODE_INVALID_TIME);
        }
    }

    #[test]
    fn skew_is_flagged_only_for_utc_and_beyond_the_bound() {
        let utc = TimePlan::compile(&spec("s", Some("utc"), Some(5))).unwrap();
        let ingest = Timestamp::from_secs(1_000).unwrap();
        let near = Timestamp::from_secs(1_004);
        let far = Timestamp::from_secs(1_006);
        let late = Timestamp::from_secs(994);
        assert_eq!(utc.times(ingest, near).1, QualityFlags::empty());
        assert_eq!(utc.times(ingest, far).1, QualityFlags::CLOCK_SKEW);
        assert_eq!(utc.times(ingest, late).1, QualityFlags::CLOCK_SKEW);

        let mono = TimePlan::compile(&spec("s", Some("monotonic"), Some(5))).unwrap();
        let (times, flags) = mono.times(ingest, far);
        assert!(flags.is_empty());
        assert_eq!(times.event_time(), None, "non-UTC never becomes event time");
        assert_eq!(times.source(), far);
    }

    #[test]
    fn domain_defaults_to_unknown() {
        let plan = TimePlan::compile(&spec("us", None, None)).unwrap();
        assert_eq!(plan.domain(), TimeDomain::Unknown);
        let (times, _) = plan.times(Timestamp::from_nanos(0), Timestamp::from_secs(3));
        assert_eq!(times.event_time(), None);
    }

    #[test]
    fn rfc3339_needs_an_offset_unless_configured() {
        let s: TimeSpec = serde_json::from_value(json!({
            "field": "/t", "unit": "rfc3339", "domain": "utc"
        }))
        .unwrap();
        let plan = TimePlan::compile(&s).unwrap();
        let ok = json!({ "t": "1970-01-01T00:00:01Z" });
        assert_eq!(
            plan.extract(&ok, &ok, &Attributes::new())
                .unwrap()
                .unwrap()
                .as_nanos(),
            1_000_000_000
        );
        let naive = json!({ "t": "1970-01-01T00:00:01" });
        assert!(plan.extract(&naive, &naive, &Attributes::new()).is_err());

        let s: TimeSpec = serde_json::from_value(json!({
            "field": "/t", "unit": "rfc3339", "domain": "utc", "default_offset": "+01:00"
        }))
        .unwrap();
        let plan = TimePlan::compile(&s).unwrap();
        assert_eq!(
            plan.extract(&naive, &naive, &Attributes::new())
                .unwrap()
                .unwrap()
                .as_nanos(),
            -3_599_000_000_000
        );
    }

    #[test]
    fn bad_specs_report_each_problem() {
        let s: TimeSpec = serde_json::from_value(
            json!({ "field": "ts", "unit": "fortnights", "domain": "mars" }),
        )
        .unwrap();
        let errs = TimePlan::compile(&s).unwrap_err();
        assert_eq!(
            errs.iter().map(|e| e.0).collect::<Vec<_>>(),
            ["field", "unit", "domain"]
        );
    }

    #[test]
    fn parent_paths_read_the_message_root() {
        let s: TimeSpec =
            serde_json::from_value(json!({ "field": "^/base", "unit": "s", "domain": "utc" }))
                .unwrap();
        let plan = TimePlan::compile(&s).unwrap();
        let root = json!({ "base": 9, "samples": [{ "x": 1 }] });
        let sample = &root["samples"][0];
        assert_eq!(
            plan.extract(sample, &root, &Attributes::new())
                .unwrap()
                .unwrap()
                .as_nanos(),
            9_000_000_000
        );
    }

    fn with_attr(attr: &str, unit: &str, skew: Option<u64>) -> TimePlan {
        let mut v = json!({ "attr": attr, "unit": unit, "domain": "utc" });
        if let Some(s) = skew {
            v["max_skew"] = json!(format!("{s}s"));
        }
        TimePlan::compile(&serde_json::from_value(v).unwrap()).unwrap()
    }

    #[test]
    fn ros_stamp_nanoseconds_read_without_conversion() {
        let stamp = 1_758_000_000_123_456_789i64;
        let s: TimeSpec = serde_json::from_value(json!({
            "field": "/header.stamp", "unit": "ns", "domain": "utc"
        }))
        .unwrap();
        let plan_flat = TimePlan::compile(&s).unwrap();
        let flat = json!({ "header.stamp": stamp, "header.frame_id": "base" });
        assert_eq!(
            plan_flat
                .extract(&flat, &flat, &Attributes::new())
                .unwrap()
                .unwrap()
                .as_nanos(),
            stamp
        );
        let nested = json!({ "header": { "stamp": stamp } });
        let s: TimeSpec = serde_json::from_value(json!({
            "field": "/header/stamp", "unit": "ns", "domain": "utc"
        }))
        .unwrap();
        let plan_nested = TimePlan::compile(&s).unwrap();
        assert_eq!(
            plan_nested
                .extract(&nested, &nested, &Attributes::new())
                .unwrap()
                .unwrap()
                .as_nanos(),
            stamp
        );
    }

    #[test]
    fn attribute_time_reads_decimal_text_in_its_unit() {
        let plan = with_attr("mcap.publish_time", "ns", Some(5));
        let mut attrs = Attributes::new();
        attrs.insert("mcap.publish_time", "1791369600123456789");
        let root = json!({ "payload": "ignored" });
        let ts = plan.extract(&root, &root, &attrs).unwrap().unwrap();
        assert_eq!(ts.as_nanos(), 1_791_369_600_123_456_789);

        let us = with_attr("mcap.log_time", "us", None);
        let mut attrs = Attributes::new();
        attrs.insert("mcap.log_time", "1791369600123456");
        assert_eq!(
            us.extract(&root, &root, &attrs)
                .unwrap()
                .unwrap()
                .as_nanos(),
            1_791_369_600_123_456_000
        );
    }

    #[test]
    fn attribute_time_flags_skew_like_payload_time() {
        let plan = with_attr("mcap.publish_time", "s", Some(5));
        let ingest = Timestamp::from_secs(1_000).unwrap();
        let mut near = Attributes::new();
        near.insert("mcap.publish_time", "1004");
        let mut far = Attributes::new();
        far.insert("mcap.publish_time", "1006");
        let root = json!({});
        let src = plan.extract(&root, &root, &near).unwrap();
        assert_eq!(plan.times(ingest, src).1, QualityFlags::empty());
        let src = plan.extract(&root, &root, &far).unwrap();
        assert_eq!(plan.times(ingest, src).1, QualityFlags::CLOCK_SKEW);
    }

    #[test]
    fn absent_attribute_is_missing_and_unparseable_is_a_reject() {
        let plan = with_attr("mcap.publish_time", "ns", None);
        let root = json!({});
        assert_eq!(
            plan.extract(&root, &root, &Attributes::new()).unwrap(),
            None
        );
        let (_, flags) = plan.times(Timestamp::from_nanos(5), None);
        assert_eq!(flags, QualityFlags::MISSING_TIME);

        for bad in ["", "12.5", "abc", "99999999999999999999"] {
            let mut attrs = Attributes::new();
            attrs.insert("mcap.publish_time", bad);
            let err = plan.extract(&root, &root, &attrs).unwrap_err();
            assert_eq!(err.code, codes::DECODE_INVALID_TIME, "{bad:?}");
            assert!(err.to_string().contains("mcap.publish_time"));
        }
    }

    #[test]
    fn attribute_rfc3339_needs_an_offset_like_payload_text() {
        let s: TimeSpec = serde_json::from_value(json!({
            "attr": "mcap.log_time", "unit": "rfc3339", "domain": "utc"
        }))
        .unwrap();
        let plan = TimePlan::compile(&s).unwrap();
        let mut attrs = Attributes::new();
        attrs.insert("mcap.log_time", "1970-01-01T00:00:01");
        assert!(plan.extract(&json!({}), &json!({}), &attrs).is_err());
        let mut attrs = Attributes::new();
        attrs.insert("mcap.log_time", "1970-01-01T00:00:01Z");
        assert_eq!(
            plan.extract(&json!({}), &json!({}), &attrs)
                .unwrap()
                .unwrap()
                .as_nanos(),
            1_000_000_000
        );
    }

    /// The result with nanos or the issue kind only, so the two parsers
    /// compare equal.
    fn outcome(r: Result<i64, TimeIssue>) -> String {
        format!("{r:?}")
    }

    #[test]
    fn parse_value_matches_the_json_spelling() {
        use thadam_core::Value;

        let plan = TimeParser::compile("ns", None).unwrap();
        let nano_rows = [
            Value::Ts(Timestamp::from_nanos(1_758_000_000_123_456_789)),
            Value::I64(42),
            Value::U64(u64::MAX),
            Value::F64(1.5),
            Value::I32(-3),
        ];
        // What the record bridge made of each value.
        let spellings = [
            json!(1_758_000_000_123_456_789i64),
            json!(42),
            json!(u64::MAX),
            json!(1.5),
            json!(-3),
        ];
        for (value, json) in nano_rows.iter().zip(spellings.iter()) {
            assert_eq!(
                outcome(plan.parse_value(value)),
                outcome(plan.parse(json)),
                "{value:?}"
            );
        }
        // The documented spelling: a nanosecond `Ts` read as-is, passthrough.
        assert_eq!(
            plan.parse_value(&Value::Ts(Timestamp::from_nanos(7)))
                .unwrap(),
            7
        );

        // Another epoch unit reads the Ts' nanosecond number, as the bridge
        // spelled it.
        let secs = TimeParser::compile("s", None).unwrap();
        let stamp = Value::Ts(Timestamp::from_nanos(2_000_000_000));
        assert_eq!(
            outcome(secs.parse_value(&stamp)),
            outcome(secs.parse(&json!(2_000_000_000i64)))
        );

        let rfc = TimeParser::compile("rfc3339", None).unwrap();
        let text = Value::str("1970-01-01T00:00:01Z");
        assert_eq!(
            outcome(rfc.parse_value(&text)),
            outcome(rfc.parse(&json!("1970-01-01T00:00:01Z")))
        );
        // A wrong-shaped value names the same kind on both paths.
        assert_eq!(
            outcome(rfc.parse_value(&Value::Bool(true))),
            outcome(rfc.parse(&json!(true)))
        );
        assert_eq!(
            outcome(secs.parse_value(&Value::Bool(true))),
            outcome(secs.parse(&json!(true)))
        );
    }

    #[test]
    fn exactly_one_of_field_or_attr() {
        let both: TimeSpec = serde_json::from_value(json!({
            "field": "/t", "attr": "mcap.log_time", "unit": "s"
        }))
        .unwrap();
        let errs = TimePlan::compile(&both).unwrap_err();
        assert_eq!(errs[0].0, "field");
        assert!(errs[0].1.contains("exactly one"));

        let neither: TimeSpec = serde_json::from_value(json!({ "unit": "s" })).unwrap();
        let errs = TimePlan::compile(&neither).unwrap_err();
        assert_eq!(errs[0].0, "field");
        assert!(errs[0].1.contains("exactly one"));

        let empty: TimeSpec = serde_json::from_value(json!({ "attr": "", "unit": "s" })).unwrap();
        let errs = TimePlan::compile(&empty).unwrap_err();
        assert_eq!(errs[0].0, "attr");
    }
}
