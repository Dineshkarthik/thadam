//! Typed, validated configuration for the live ROS 2 source.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use thadam_core::{ErrorDomain, ThadamError, codes};
use thadam_ros2_msg::msg::TypeName;
use serde::Deserialize;
use serde::de::{self, Deserializer};

/// A duration written as whole seconds (`10`) or with a unit: `500ms`, `10s`,
/// `5m`, `1h`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct DurationSpec(pub Duration);

impl DurationSpec {
    /// Parses `text`, where a bare number means seconds.
    pub fn parse(text: &str) -> Result<Duration, String> {
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len());
        let (digits, unit) = text.split_at(split);
        let n: u64 = digits.parse().map_err(|_| {
            format!("`{text}` is not a duration: use digits with an optional ms, s, m, or h suffix")
        })?;
        let seconds_per_unit = match unit {
            "ms" => return Ok(Duration::from_millis(n)),
            "" | "s" => 1,
            "m" => 60,
            "h" => 3600,
            _ => {
                return Err(format!("`{text}` has an unknown unit: use ms, s, m, or h"));
            }
        };
        n.checked_mul(seconds_per_unit)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("`{text}` is too large"))
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = DurationSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration such as `500ms`, `10s`, `5m`, or whole seconds")
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<DurationSpec, E> {
                Ok(DurationSpec(Duration::from_secs(v)))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<DurationSpec, E> {
                u64::try_from(v)
                    .map(|secs| DurationSpec(Duration::from_secs(secs)))
                    .map_err(|_| E::custom("a duration cannot be negative"))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<DurationSpec, E> {
                DurationSpec::parse(v).map(DurationSpec).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// One topic to subscribe, with the message type its payloads carry.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicSpec {
    /// The ROS topic with a leading slash, for example `/imu`.
    pub name: String,
    /// The ROS type the publisher sends, `pkg/msg/Type` (the `pkg/Type`
    /// spelling is also accepted), for example `sensor_msgs/msg/Imu`. Its
    /// `.msg` definition must be readable under one of the `msg_dirs`.
    #[serde(rename = "type")]
    pub ros_type: String,
    /// Run the transient_local late-join query for this topic (needed for
    /// `/tf_static`: without it a joiner misses what was published before
    /// it subscribed).
    #[serde(default)]
    pub transient_local: bool,
}

/// Reconnect behavior after a lost Zenoh session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectConfig {
    /// First retry delay; doubles (jittered) with consecutive failures.
    #[serde(default = "default_min_backoff")]
    pub min_backoff: DurationSpec,
    /// The most a retry delay grows to.
    #[serde(default = "default_max_backoff")]
    pub max_backoff: DurationSpec,
}

fn default_min_backoff() -> DurationSpec {
    DurationSpec(Duration::from_secs(1))
}

fn default_max_backoff() -> DurationSpec {
    DurationSpec(Duration::from_secs(30))
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            min_backoff: default_min_backoff(),
            max_backoff: default_max_backoff(),
        }
    }
}

/// The whole `config:` block of a live ROS 2 source.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ros2LiveConfig {
    /// The Zenoh locator of the `rmw_zenohd` router to connect to, for
    /// example `tcp/127.0.0.1:7447`. The session is a Zenoh **client** with
    /// multicast scouting off (spike S3 §4: peer mode receives nothing in
    /// this topology, and nothing must leak onto the LAN).
    pub endpoint: String,
    /// The ROS domain id; must match the publishers' `ROS_DOMAIN_ID`.
    #[serde(default)]
    pub domain: u32,
    /// Directories holding `.msg` definitions in the ROS layout
    /// (`<dir>/<pkg>/msg/<Type>.msg`, for example `/opt/ros/jazzy/share`).
    /// Required and non-empty: there is no recording to read schemas from on
    /// a live stream, so the schemas the chain's `decode: ros2` needs are
    /// assembled from here. Later entries are fallback search paths.
    pub msg_dirs: Vec<PathBuf>,
    /// The topics to subscribe, an explicit list. Liveliness-based discovery
    /// is deliberately not used: subscriptions must be declared.
    pub topics: Vec<TopicSpec>,
    /// Reconnect behavior.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

impl Ros2LiveConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<Ros2LiveConfig, Vec<ThadamError>> {
        let config: Ros2LiveConfig = serde_json::from_value(value.clone()).map_err(|e| {
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

        if self.endpoint.trim().is_empty() {
            found.push(("endpoint", "endpoint must not be empty".into()));
        }
        if self.msg_dirs.is_empty() {
            found.push((
                "msg_dirs",
                "msg_dirs is required: a live stream has no recording to read schemas from".into(),
            ));
        }
        for dir in &self.msg_dirs {
            if dir.as_os_str().is_empty() {
                found.push(("msg_dirs", "msg_dirs entries must not be empty".into()));
                break;
            }
        }
        if self.topics.is_empty() {
            found.push(("topics", "topics is required and must not be empty".into()));
        }
        for (i, topic) in self.topics.iter().enumerate() {
            if let Some(reason) = topic_problem(topic) {
                found.push(("topics", format!("topics[{i}]: {reason}")));
                continue;
            }
            if self
                .topics
                .iter()
                .take(i)
                .any(|other| other.name == topic.name)
            {
                found.push((
                    "topics",
                    format!("topics[{i}]: duplicate topic {}", topic.name),
                ));
            }
        }

        if self.reconnect.min_backoff.0.is_zero() {
            found.push((
                "reconnect",
                "reconnect.min_backoff must be greater than zero".into(),
            ));
        }
        if self.reconnect.min_backoff.0 > self.reconnect.max_backoff.0 {
            found.push((
                "reconnect",
                "reconnect.max_backoff must be at least min_backoff".into(),
            ));
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

/// Why a topic entry is unusable.
fn topic_problem(topic: &TopicSpec) -> Option<String> {
    if !topic.name.starts_with('/') || topic.name.len() < 2 {
        return Some(format!(
            "topic name `{}` must start with `/` and name a topic",
            topic.name
        ));
    }
    if topic
        .name
        .chars()
        .any(|c| c.is_whitespace() || matches!(c, '*' | '#' | '?'))
    {
        return Some(format!(
            "topic name `{}` must not contain whitespace or wildcards",
            topic.name
        ));
    }
    if let Err(e) = TypeName::parse(&topic.ros_type) {
        return Some(format!(
            "type `{}` is not `pkg/msg/Type`: {e}",
            topic.ros_type
        ));
    }
    None
}

/// The JSON Schema for the `config:` block of a live ROS 2 source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Live ROS 2 source",
        "type": "object",
        "additionalProperties": false,
        "required": ["endpoint", "msg_dirs", "topics"],
        "properties": {
            "endpoint": {
                "type": "string",
                "minLength": 1,
                "description": "Zenoh locator of the rmw_zenohd router, for example tcp/127.0.0.1:7447. The session is a Zenoh client with multicast scouting off."
            },
            "domain": {
                "type": "integer",
                "minimum": 0,
                "default": 0,
                "description": "ROS domain id; must match the publishers' ROS_DOMAIN_ID."
            },
            "msg_dirs": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "minLength": 1},
                "description": "Directories holding .msg definitions in the ROS layout (<dir>/<pkg>/msg/<Type>.msg), for example /opt/ros/jazzy/share. Required: a live stream has no recording to read schemas from."
            },
            "topics": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "type"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "pattern": "^/\\S+$",
                            "description": "The ROS topic with a leading slash, for example /imu."
                        },
                        "type": {
                            "type": "string",
                            "pattern": "^[A-Za-z][A-Za-z0-9_]*/(msg/)?[A-Za-z][A-Za-z0-9_]*$",
                            "description": "The ROS type, pkg/msg/Type, for example sensor_msgs/msg/Imu."
                        },
                        "transient_local": {
                            "type": "boolean",
                            "default": false,
                            "description": "Run the transient_local late-join query (needed for /tf_static)."
                        }
                    }
                }
            },
            "reconnect": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "min_backoff": {"$ref": "#/$defs/duration", "default": "1s"},
                    "max_backoff": {"$ref": "#/$defs/duration", "default": "30s"}
                }
            }
        },
        "$defs": {
            "duration": {
                "oneOf": [
                    {"type": "integer", "minimum": 0},
                    {"type": "string", "pattern": "^[0-9]+(ms|s|m|h)?$"}
                ]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::codes;
    use serde_json::json;

    fn problems_of(value: serde_json::Value) -> Vec<(&'static str, String)> {
        match Ros2LiveConfig::from_value(&value) {
            Ok(_) => Vec::new(),
            Err(errors) => errors
                .into_iter()
                .map(|e| {
                    let field = e
                        .context
                        .iter()
                        .find(|(k, _)| *k == "field")
                        .map(|(_, v)| v.to_string())
                        .unwrap_or_default();
                    (e.code, field)
                })
                .collect(),
        }
    }

    fn base() -> serde_json::Value {
        json!({
            "endpoint": "tcp/127.0.0.1:7447",
            "msg_dirs": ["/opt/ros/jazzy/share"],
            "topics": [{"name": "/imu", "type": "sensor_msgs/msg/Imu"}],
        })
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = Ros2LiveConfig::from_value(&base()).expect("valid");
        assert_eq!(cfg.domain, 0);
        assert_eq!(cfg.reconnect.min_backoff.0, Duration::from_secs(1));
        assert_eq!(cfg.reconnect.max_backoff.0, Duration::from_secs(30));
        assert!(!cfg.topics[0].transient_local);
    }

    #[test]
    fn endpoint_msg_dirs_and_topics_are_required() {
        // serde derive reports one missing field at a time; every one of
        // the three is required.
        for (value, field) in [
            (json!({}), "endpoint"),
            (json!({"endpoint": "tcp/127.0.0.1:7447"}), "msg_dirs"),
            (
                json!({"endpoint": "tcp/127.0.0.1:7447", "msg_dirs": ["/x"]}),
                "topics",
            ),
        ] {
            let found = problems_of(value.clone());
            assert_eq!(found.len(), 1, "{value}");
            assert_eq!(found[0].0, codes::CONFIG_MISSING, "{value}");
            assert!(found[0].1.is_empty(), "field context on serde errors");
            let _ = field;
        }
        for (bad, field) in [
            (json!({"endpoint": "  "}), "endpoint"),
            (json!({"msg_dirs": []}), "msg_dirs"),
            (json!({"msg_dirs": [""]}), "msg_dirs"),
            (json!({"topics": []}), "topics"),
        ] {
            let mut value = base();
            for (k, v) in bad.as_object().expect("object") {
                value[k] = v.clone();
            }
            let found = problems_of(value);
            assert_eq!(found, vec![(codes::CONFIG_INVALID, field.to_string())]);
        }
    }

    #[test]
    fn topic_names_and_types_are_checked() {
        for (bad_name, bad_type) in [
            ("imu", "sensor_msgs/msg/Imu"),
            ("/", "sensor_msgs/msg/Imu"),
            ("/im u", "sensor_msgs/msg/Imu"),
            ("/imu/**", "sensor_msgs/msg/Imu"),
            ("/imu", "Imu"),
            ("/imu", "sensor_msgs/msg/"),
            ("/imu", "sensor_msgs/msg/Imu/extra"),
        ] {
            let mut value = base();
            value["topics"] = json!([{"name": bad_name, "type": bad_type}]);
            let found = problems_of(value.clone());
            assert_eq!(
                found,
                vec![(codes::CONFIG_INVALID, "topics".to_string())],
                "{value}"
            );
        }
        // `pkg/Type` is an accepted spelling; transient_local parses.
        let mut value = base();
        value["topics"] = json!([
            {"name": "/imu", "type": "sensor_msgs/Imu"},
            {"name": "/tf_static", "type": "tf2_msgs/msg/TFMessage", "transient_local": true},
        ]);
        let cfg = Ros2LiveConfig::from_value(&value).expect("valid");
        assert!(cfg.topics[1].transient_local);
    }

    #[test]
    fn duplicate_topics_are_rejected() {
        let mut value = base();
        value["topics"] = json!([
            {"name": "/imu", "type": "sensor_msgs/msg/Imu"},
            {"name": "/imu", "type": "sensor_msgs/msg/Imu"},
        ]);
        let found = problems_of(value);
        assert_eq!(found, vec![(codes::CONFIG_INVALID, "topics".to_string())]);
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        for value in [
            json!({"endpoint": "tcp/127.0.0.1:7447", "msg_dirs": ["/x"], "topics": [{"name": "/imu", "type": "sensor_msgs/msg/Imu"}], "nope": 1}),
            json!({"endpoint": "tcp/127.0.0.1:7447", "msg_dirs": ["/x"], "topics": [{"name": "/imu", "type": "sensor_msgs/msg/Imu", "nope": 2}]}),
            json!({"endpoint": "tcp/127.0.0.1:7447", "msg_dirs": ["/x"], "topics": [{"name": "/imu", "type": "sensor_msgs/msg/Imu"}], "reconnect": {"nope": 3}}),
        ] {
            let found = problems_of(value.clone());
            assert_eq!(found.len(), 1, "{value}");
            assert_eq!(found[0].0, codes::CONFIG_INVALID, "{value}");
        }
    }

    #[test]
    fn reconnect_limits_validate() {
        let mut value = base();
        value["reconnect"] = json!({"min_backoff": "0ms"});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["reconnect"] = json!({"min_backoff": "10s", "max_backoff": "1s"});
        assert_eq!(problems_of(value).len(), 1);
    }

    #[test]
    fn duration_specs_parse() {
        assert_eq!(DurationSpec::parse("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(DurationSpec::parse("10"), Ok(Duration::from_secs(10)));
        assert_eq!(DurationSpec::parse("5m"), Ok(Duration::from_secs(300)));
        assert!(DurationSpec::parse("-1").is_err());
        assert!(DurationSpec::parse("10ns").is_err());
    }

    #[test]
    fn schema_declares_the_closed_shape_and_required_keys() {
        let schema = config_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["required"],
            json!(["endpoint", "msg_dirs", "topics"])
        );
        assert_eq!(
            schema["properties"]["topics"]["items"]["additionalProperties"],
            false
        );
        let mut keys: Vec<&str> = schema["properties"]
            .as_object()
            .expect("properties")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["domain", "endpoint", "msg_dirs", "reconnect", "topics"]
        );
    }
}
