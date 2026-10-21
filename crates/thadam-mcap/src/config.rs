//! Typed, validated configuration for the MCAP source.

use std::fmt;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::de::{self, Deserializer};
use thadam_core::{ErrorDomain, ThadamError, Timestamp, codes};

/// A log-time bound, written as an RFC 3339 string (`2026-10-01T12:00:00Z`)
/// or as integer nanoseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeBound(pub Timestamp);

impl TimeBound {
    fn parse(text: &str) -> Result<Timestamp, String> {
        let system_time = humantime::parse_rfc3339(text)
            .map_err(|_| format!("`{text}` is not an RFC 3339 timestamp"))?;
        let nanos = match system_time.duration_since(UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
            Err(before) => {
                i64::try_from(before.duration().as_nanos()).map_or(i64::MIN, |n| n.saturating_neg())
            }
        };
        Ok(Timestamp::from_nanos(nanos))
    }
}

impl<'de> Deserialize<'de> for TimeBound {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = TimeBound;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an RFC 3339 timestamp or integer nanoseconds since the epoch")
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<TimeBound, E> {
                Ok(TimeBound(Timestamp::from_nanos(
                    i64::try_from(v).unwrap_or(i64::MAX),
                )))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<TimeBound, E> {
                Ok(TimeBound(Timestamp::from_nanos(v)))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<TimeBound, E> {
                TimeBound::parse(v).map(TimeBound).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// How fast messages replay, relative to their recorded log times.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Pace {
    /// As fast as the messages can be read. The default.
    #[default]
    Max,
    /// Replay with the original inter-message timing (a gap of one recorded
    /// second sleeps one second).
    Realtime,
    /// Replay at a multiple of real time: `2.0` halves every gap, `0.5`
    /// doubles it.
    Factor(f64),
}

impl<'de> Deserialize<'de> for Pace {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = Pace;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`max`, `realtime`, or a positive numeric factor")
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Pace, E> {
                if v.is_finite() && v > 0.0 {
                    Ok(Pace::Factor(v))
                } else {
                    Err(E::custom("a pace factor must be a positive number"))
                }
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Pace, E> {
                self.visit_f64(v as f64)
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Pace, E> {
                self.visit_f64(v as f64)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Pace, E> {
                match v {
                    "max" => Ok(Pace::Max),
                    "realtime" => Ok(Pace::Realtime),
                    other => other
                        .parse::<f64>()
                        .ok()
                        .filter(|f| f.is_finite() && *f > 0.0)
                        .map(Pace::Factor)
                        .ok_or_else(|| {
                            E::custom(format!(
                                "`{other}` is not `max`, `realtime`, or a positive factor"
                            ))
                        }),
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// What `decode: ros2` does with `ros2msg` schemas. Either way the payload
/// bytes stay raw; decoding into records is the transform chain's job
/// (see [`crate::decode_ros2`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodeMode {
    /// Pass schema metadata through as attributes without touching it.
    #[default]
    Raw,
    /// Pre-flight every `ros2msg` schema through the `thadam-ros2-msg`
    /// compiler when its channel is first seen (fail fast on a schema that
    /// does not compile) and attach the compiled shape's flattened field
    /// names as `mcap.ros2.fields`.
    Ros2,
}

/// The topic allow-list: exact names, or prefixes when the entry ends with
/// `/`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct TopicFilter(pub Vec<String>);

impl TopicFilter {
    /// Whether `topic` passes. An empty filter passes everything.
    pub fn matches(&self, topic: &str) -> bool {
        self.0.is_empty()
            || self
                .0
                .iter()
                .any(|f| topic == f || (f.ends_with('/') && topic.starts_with(f.as_str())))
    }
}

/// The whole `config:` block of an MCAP source. Every key is optional
/// except `paths`, whose absence is `config.missing`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McapConfig {
    /// MCAP files, or directories scanned one level deep for `*.mcap`.
    /// Entries are replayed as one stream, merged in log-time order; files
    /// are visited in sorted path order.
    pub paths: Vec<PathBuf>,
    /// Optional topic allow-list (exact or, ending in `/`, prefix).
    #[serde(default)]
    pub topics: TopicFilter,
    /// Inclusive lower log-time bound.
    #[serde(default)]
    pub start: Option<TimeBound>,
    /// Inclusive upper log-time bound.
    #[serde(default)]
    pub end: Option<TimeBound>,
    /// Replay pace.
    #[serde(default)]
    pub pace: Pace,
    /// `raw` or `ros2`.
    #[serde(default)]
    pub decode: DecodeMode,
}

impl McapConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<McapConfig, Vec<ThadamError>> {
        let config: McapConfig = serde_json::from_value(value.clone()).map_err(|e| {
            let text = e.to_string();
            let code = if text.starts_with("missing field") {
                codes::CONFIG_MISSING
            } else {
                codes::CONFIG_INVALID
            };
            vec![ThadamError::permanent(ErrorDomain::Config, code, text)]
        })?;
        let errors = config.problems();
        if errors.is_empty() {
            Ok(config)
        } else {
            Err(errors)
        }
    }

    /// Every validation problem, empty when the configuration is sound.
    pub fn problems(&self) -> Vec<ThadamError> {
        let mut found: Vec<(&'static str, String)> = Vec::new();

        if self.paths.is_empty() {
            found.push((
                "paths",
                "at least one MCAP file or directory is required".into(),
            ));
        }

        for (i, topic) in self.topics.0.iter().enumerate() {
            if topic.is_empty() || topic.chars().any(char::is_whitespace) {
                found.push((
                    "topics",
                    format!("topics[{i}] must be non-empty and contain no whitespace"),
                ));
            }
        }

        if let (Some(start), Some(end)) = (&self.start, &self.end)
            && start.0 > end.0
        {
            found.push(("start", "start must not be after end".into()));
        }

        found
            .into_iter()
            .map(|(field, message)| {
                ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
                    .with_context("field", field)
            })
            .collect()
    }
}

/// The JSON Schema for the `config:` block of an MCAP source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "MCAP file source",
        "type": "object",
        "additionalProperties": false,
        "required": ["paths"],
        "properties": {
            "paths": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "minLength": 1},
                "description": "MCAP files, or directories scanned one level deep for *.mcap. Files are visited in sorted path order and merged in log-time order."
            },
            "topics": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "description": "Optional topic allow-list: exact names, or prefixes when the entry ends with `/`."
            },
            "start": {
                "$ref": "#/$defs/log_time",
                "description": "Inclusive lower bound on the MCAP log time."
            },
            "end": {
                "$ref": "#/$defs/log_time",
                "description": "Inclusive upper bound on the MCAP log time."
            },
            "pace": {
                "oneOf": [
                    {"type": "string", "enum": ["max", "realtime"]},
                    {"type": "number", "exclusiveMinimum": 0},
                    {"type": "string", "pattern": "^[0-9]+(\\.[0-9]+)?$"}
                ],
                "default": "max",
                "description": "Replay pace: as fast as possible, with the original inter-message timing, or at a multiple of real time."
            },
            "decode": {
                "type": "string",
                "enum": ["raw", "ros2"],
                "default": "raw",
                "description": "`ros2` pre-flights ros2msg schemas through the thadam-ros2-msg compiler and attaches compiled-shape metadata; payloads stay raw either way."
            }
        },
        "$defs": {
            "log_time": {
                "oneOf": [
                    {"type": "integer"},
                    {"type": "string", "format": "date-time"}
                ]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fields(value: serde_json::Value) -> Vec<String> {
        match McapConfig::from_value(&value) {
            Ok(_) => Vec::new(),
            Err(errors) => errors
                .into_iter()
                .map(|e| {
                    e.context
                        .iter()
                        .find(|(k, _)| *k == "field")
                        .map(|(_, v)| v.to_string())
                        .unwrap_or_default()
                })
                .collect(),
        }
    }

    #[test]
    fn paths_are_required() {
        let errors = McapConfig::from_value(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
        assert!(errors[0].message.contains("paths"));
        assert_eq!(fields(json!({"paths": []})), vec!["paths".to_string()]);
        assert!(McapConfig::from_value(&json!({"paths": ["/tmp/a.mcap", "/data"]})).is_ok());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let errors = McapConfig::from_value(&json!({"paths": ["a.mcap"], "nope": 1})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_INVALID);
        assert!(errors[0].message.contains("unknown field"));
    }

    #[test]
    fn pace_spellings_and_bounds() {
        let cfg = McapConfig::from_value(&json!({"paths": ["a.mcap"]})).expect("valid");
        assert_eq!(cfg.pace, Pace::Max);

        for (value, want) in [
            (json!("max"), Pace::Max),
            (json!("realtime"), Pace::Realtime),
            (json!(2), Pace::Factor(2.0)),
            (json!(2.5), Pace::Factor(2.5)),
            (json!("0.25"), Pace::Factor(0.25)),
        ] {
            let cfg = McapConfig::from_value(&json!({"paths": ["a.mcap"], "pace": value}))
                .expect("valid pace");
            assert_eq!(cfg.pace, want);
        }
        for bad in [
            json!("fast"),
            json!(0),
            json!(-1.0),
            json!("0"),
            json!(-0.5),
        ] {
            assert!(
                McapConfig::from_value(&json!({"paths": ["a.mcap"], "pace": bad})).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn time_bounds_accept_rfc3339_and_nanos_and_must_be_ordered() {
        let cfg = McapConfig::from_value(&json!({
            "paths": ["a.mcap"],
            "start": "2026-10-01T00:00:00Z",
            "end": 1_790_812_800_000_000_001i64,
        }))
        .expect("valid");
        assert_eq!(
            cfg.start.expect("start").0,
            Timestamp::from_nanos(1_790_812_800_000_000_000)
        );
        assert_eq!(
            cfg.end.expect("end").0,
            Timestamp::from_nanos(1_790_812_800_000_000_001)
        );

        assert_eq!(
            fields(json!({
                "paths": ["a.mcap"],
                "start": "2026-10-02T00:00:00Z",
                "end": "2026-10-01T00:00:00Z",
            })),
            vec!["start".to_string()]
        );
        assert!(
            McapConfig::from_value(&json!({"paths": ["a.mcap"], "start": "10 seconds ago"}))
                .is_err()
        );
        assert!(McapConfig::from_value(&json!({"paths": ["a.mcap"], "start": 1.5})).is_err());
    }

    #[test]
    fn topic_filter_exact_and_slash_prefix() {
        let filter = TopicFilter(vec![
            "/joint_states".into(),
            "/tf/".into(),
            "/lidar/points".into(),
        ]);
        assert!(filter.matches("/joint_states"));
        assert!(filter.matches("/tf/static"));
        assert!(!filter.matches("/tf"));
        assert!(!filter.matches("/joint_states_extra"));
        assert!(!filter.matches("/lidar"));
        assert!(!filter.matches("/lidar/points2"));
        assert!(TopicFilter::default().matches("/anything"));

        assert_eq!(
            fields(json!({"paths": ["a.mcap"], "topics": ["  "]})),
            vec!["topics".to_string()]
        );
        assert!(
            McapConfig::from_value(&json!({"paths": ["a.mcap"], "topics": ["/a", "/b/"]})).is_ok()
        );
    }

    #[test]
    fn schema_declares_the_closed_object_shape() {
        let schema = config_schema();
        assert_eq!(schema["title"], "MCAP file source");
        assert_eq!(schema["additionalProperties"], false);
        let mut keys: Vec<&str> = schema["properties"]
            .as_object()
            .expect("properties")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["decode", "end", "pace", "paths", "start", "topics"]
        );
        assert_eq!(schema["required"], json!(["paths"]));
    }
}
