//! Typed, validated configuration for the MQTT publish sink.
//!
//! Mirrors the source configuration: the same URL, TLS, and credential rules,
//! the same closed schema, and every problem reported at once.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thadam_core::{ErrorDomain, ThadamError, codes};

use super::topic::{TopicTemplate, TopicTemplateError};
use crate::config::{DurationSpec, TlsConfig, parse_broker_url};

/// Worst-case size of a PUBLISH packet's header besides the payload: two
/// topic-length bytes, a topic of up to 65,535 bytes, and the packet id.
const PACKET_OVERHEAD: usize = 2 + 65_535 + 2;

/// Largest MQTT remaining-length value (268,435,455) the codec can express.
const MAX_REMAINING_LENGTH: u64 = 268_435_455;

const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1 << 20;

/// The payload encoding. Only `json` exists today.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    /// The `_`-prefixed lineage fields, identity levels, and record fields as
    /// one JSON object.
    #[default]
    Json,
}

/// The configuration of one MQTT publish sink.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttPublishConfig {
    /// `mqtt://host[:1883]` or `mqtts://host[:8883]`.
    pub url: String,
    /// The stable client identifier of this publisher.
    pub client_id: String,
    /// The topic template, such as `thadam/{dataset}/{robot}`.
    pub topic: String,
    /// 0 (at most once) or 1 (at least once). Defaults to 1.
    #[serde(default = "default_qos")]
    pub qos: u8,
    /// Whether the broker keeps each published message as the topic's last value.
    #[serde(default)]
    pub retain: bool,
    /// The payload encoding.
    #[serde(default)]
    pub payload: PayloadFormat,
    /// TLS settings; only valid with an `mqtts://` URL.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// The username.
    #[serde(default)]
    pub username: Option<String>,
    /// A file holding the password, so the secret stays out of the config.
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    /// The number of publishes awaiting acknowledgement before the sink stops
    /// taking new events.
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: u32,
    /// How long one publish may wait for its acknowledgement.
    #[serde(default = "default_request_timeout")]
    pub request_timeout: DurationSpec,
    /// The largest payload published. Events above it are rejected.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: u64,
    /// The longest idle period before the client sends a PINGREQ.
    #[serde(default = "default_keep_alive")]
    pub keep_alive: DurationSpec,
    /// How long the broker may take to accept the connection.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: DurationSpec,
    /// The dataset selector. The runtime reads it to restrict the build
    /// context; it is parsed here so a malformed selector fails with a good
    /// message and `deny_unknown_fields` accepts the key.
    #[serde(default)]
    pub datasets: Option<Vec<serde_json::Value>>,
}

fn default_qos() -> u8 {
    1
}
fn default_max_in_flight() -> u32 {
    100
}
fn default_request_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(10))
}
fn default_max_message_bytes() -> u64 {
    DEFAULT_MAX_MESSAGE_BYTES
}
fn default_keep_alive() -> DurationSpec {
    DurationSpec(Duration::from_secs(30))
}
fn default_connect_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(10))
}

impl MqttPublishConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<MqttPublishConfig, Vec<ThadamError>> {
        let config: MqttPublishConfig = serde_json::from_value(value.clone()).map_err(|e| {
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
        let mut out = Vec::new();
        let mut bad = |field: &'static str, message: String| {
            out.push(
                ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
                    .with_context("field", field),
            );
        };

        let broker = match parse_broker_url(&self.url) {
            Ok(b) => Some(b),
            Err(e) => {
                bad("url", e);
                None
            }
        };

        if self.client_id.is_empty() {
            bad("client_id", "a non-empty client_id is required".into());
        } else if self.client_id.len() > 256 || self.client_id.chars().any(char::is_control) {
            bad(
                "client_id",
                "client_id must be at most 256 bytes with no control characters".into(),
            );
        }

        if let Err(e) = TopicTemplate::parse(&self.topic) {
            bad("topic", e.0);
        }

        if self.qos > 1 {
            bad(
                "qos",
                "qos must be 0 (at most once) or 1 (at least once)".into(),
            );
        }

        match (&self.tls, broker.as_ref().map(|b| b.tls)) {
            (Some(_), Some(false)) => {
                bad("tls", "a `tls` block needs an `mqtts://` URL".into());
            }
            (Some(tls), _) if tls.cert.is_some() != tls.key.is_some() => {
                bad("tls", "tls.cert and tls.key must be given together".into());
            }
            _ => {}
        }

        if self.password_file.is_some() && self.username.is_none() {
            bad("password_file", "password_file needs a username".into());
        }

        if !(1..=u32::from(u16::MAX)).contains(&self.max_in_flight) {
            bad(
                "max_in_flight",
                "max_in_flight must be from 1 to 65535".into(),
            );
        }

        let timeout = self.request_timeout.0;
        if timeout.subsec_nanos() != 0 || timeout.as_secs() < 1 {
            bad(
                "request_timeout",
                "request_timeout must be whole seconds, at least 1".into(),
            );
        }

        let max_payload = MAX_REMAINING_LENGTH - PACKET_OVERHEAD as u64;
        if self.max_message_bytes == 0 || self.max_message_bytes > max_payload {
            bad(
                "max_message_bytes",
                format!("max_message_bytes must be from 1 to {max_payload}"),
            );
        }

        let keep_alive = self.keep_alive.0;
        if keep_alive.subsec_nanos() != 0
            || keep_alive.as_secs() < 1
            || keep_alive.as_secs() > u64::from(u16::MAX)
        {
            bad(
                "keep_alive",
                "keep_alive must be whole seconds, from 1 to 65535".into(),
            );
        }

        let connect = self.connect_timeout.0;
        if connect.subsec_nanos() != 0 || connect.as_secs() < 1 {
            bad(
                "connect_timeout",
                "connect_timeout must be whole seconds, at least 1".into(),
            );
        }

        if let Some(datasets) = &self.datasets {
            if datasets.iter().any(|item| !item.is_string()) {
                bad(
                    "datasets",
                    "every `datasets` entry must be a dataset id string".into(),
                );
            }
            if datasets.len() != 1 {
                bad(
                    "datasets",
                    "a `kind: mqtt_publish` sink publishes exactly one dataset; add one sink entry per dataset".into(),
                );
            }
        }
        out
    }

    /// The compiled topic template. Valid once [`Self::problems`] is empty.
    pub fn topic_template(&self) -> Result<TopicTemplate, TopicTemplateError> {
        TopicTemplate::parse(&self.topic)
    }
}

/// The JSON Schema for the `config:` block of an MQTT publish sink.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "MQTT sink",
        "type": "object",
        "additionalProperties": false,
        "required": ["url", "client_id", "topic"],
        "properties": {
            "url": {
                "type": "string",
                "pattern": "^mqtts?://[^/@?#]+$",
                "description": "mqtt://host[:1883] or mqtts://host[:8883]"
            },
            "client_id": {
                "type": "string", "minLength": 1, "maxLength": 256,
                "description": "Stable identifier of this publisher."
            },
            "topic": {
                "type": "string", "minLength": 1,
                "description": "Topic template such as thadam/{dataset}/{robot}. Placeholders are {dataset} and identity levels; no + or # wildcards."
            },
            "qos": { "type": "integer", "minimum": 0, "maximum": 1, "default": 1 },
            "retain": { "type": "boolean", "default": false },
            "payload": { "enum": ["json"], "default": "json" },
            "tls": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "ca": { "type": "string" },
                    "cert": { "type": "string" },
                    "key": { "type": "string" }
                }
            },
            "username": { "type": "string" },
            "password_file": { "type": "string" },
            "max_in_flight": { "type": "integer", "minimum": 1, "maximum": 65535, "default": 100 },
            "request_timeout": { "$ref": "#/$defs/duration", "default": "10s" },
            "max_message_bytes": { "type": "integer", "minimum": 1, "default": DEFAULT_MAX_MESSAGE_BYTES },
            "keep_alive": { "$ref": "#/$defs/duration", "default": "30s" },
            "connect_timeout": { "$ref": "#/$defs/duration", "default": "10s" },
            "datasets": {"type": "array", "items": {"type": "string"}, "maxItems": 1,
                "description": "Selector consumed by the runtime; a mqtt_publish sink publishes exactly one dataset."}
        },
        "$defs": {
            "duration": {
                "description": "Whole seconds, or a string such as 30s, 500ms, 5m, 1h.",
                "oneOf": [
                    { "type": "integer", "minimum": 0 },
                    { "type": "string", "pattern": "^[0-9]+(ms|s|m|h)?$" }
                ]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> serde_json::Value {
        json!({
            "url": "mqtt://broker:1883",
            "client_id": "thadam-sink-01",
            "topic": "thadam/{dataset}/{robot}"
        })
    }

    fn with(mut base: serde_json::Value, key: &str, value: serde_json::Value) -> serde_json::Value {
        base[key] = value;
        base
    }

    fn fields(errors: &[ThadamError]) -> Vec<&str> {
        errors
            .iter()
            .filter_map(|e| e.context.get("field"))
            .collect()
    }

    #[test]
    fn minimal_config_gets_the_documented_defaults() {
        let c = MqttPublishConfig::from_value(&minimal()).unwrap();
        assert_eq!(c.qos, 1);
        assert!(!c.retain);
        assert_eq!(c.payload, PayloadFormat::Json);
        assert_eq!(c.max_in_flight, 100);
        assert_eq!(c.request_timeout.0, Duration::from_secs(10));
        assert_eq!(c.max_message_bytes, 1 << 20);
        assert_eq!(c.keep_alive.0, Duration::from_secs(30));
        assert_eq!(c.connect_timeout.0, Duration::from_secs(10));
        assert!(c.tls.is_none() && c.username.is_none() && c.password_file.is_none());
    }

    #[test]
    fn keep_alive_and_connect_timeout_are_whole_seconds() {
        for (key, bad) in [
            ("keep_alive", "500ms"),
            ("keep_alive", "0s"),
            ("connect_timeout", "1500ms"),
            ("connect_timeout", "0"),
        ] {
            let v = with(minimal(), key, json!(bad));
            assert_eq!(
                fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
                [key]
            );
        }
        let v = with(minimal(), "keep_alive", json!("45s"));
        assert_eq!(
            MqttPublishConfig::from_value(&v).unwrap().keep_alive.0,
            Duration::from_secs(45)
        );
    }

    #[test]
    fn required_fields_report_config_missing() {
        let e =
            MqttPublishConfig::from_value(&json!({ "url": "mqtt://b", "topic": "t" })).unwrap_err();
        assert_eq!(e[0].code, codes::CONFIG_MISSING);
        assert!(e[0].message.contains("client_id"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let e = MqttPublishConfig::from_value(&with(minimal(), "topics", json!([]))).unwrap_err();
        assert_eq!(e[0].code, codes::CONFIG_INVALID);
        assert!(e[0].message.contains("topics"));
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let mut v = minimal();
        v["url"] = json!("http://broker");
        v["client_id"] = json!("");
        v["topic"] = json!("thadam/+/x");
        v["max_in_flight"] = json!(0);
        let e = MqttPublishConfig::from_value(&v).unwrap_err();
        let f = fields(&e);
        for expected in ["url", "client_id", "topic", "max_in_flight"] {
            assert!(f.contains(&expected), "{expected} missing from {f:?}");
        }
    }

    #[test]
    fn topic_must_be_a_valid_template() {
        for bad in ["", "a//b", "thadam/#", "thadam/{robot", "$SYS/x"] {
            let v = with(minimal(), "topic", json!(bad));
            assert_eq!(
                fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
                ["topic"],
                "{bad}"
            );
        }
        let v = with(minimal(), "topic", json!("thadam/{dataset}/{robot}"));
        assert!(
            MqttPublishConfig::from_value(&v)
                .unwrap()
                .topic_template()
                .is_ok()
        );
    }

    #[test]
    fn qos_is_zero_or_one() {
        for ok in [0, 1] {
            let v = with(minimal(), "qos", json!(ok));
            assert!(MqttPublishConfig::from_value(&v).is_ok(), "{ok}");
        }
        let v = with(minimal(), "qos", json!(2));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["qos"]
        );
    }

    #[test]
    fn payload_only_accepts_json() {
        assert!(MqttPublishConfig::from_value(&with(minimal(), "payload", json!("json"))).is_ok());
        assert!(MqttPublishConfig::from_value(&with(minimal(), "payload", json!("avro"))).is_err());
    }

    #[test]
    fn max_in_flight_and_timeout_bounds() {
        let v = with(minimal(), "max_in_flight", json!(65_536));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["max_in_flight"]
        );
        let v = with(minimal(), "request_timeout", json!("500ms"));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["request_timeout"]
        );
        let c = MqttPublishConfig::from_value(&with(minimal(), "request_timeout", json!("2m")))
            .unwrap();
        assert_eq!(c.request_timeout.0, Duration::from_secs(120));
    }

    #[test]
    fn max_message_bytes_bounds() {
        for bad in [json!(0), json!(300_000_000u64)] {
            let v = with(minimal(), "max_message_bytes", bad);
            assert_eq!(
                fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
                ["max_message_bytes"]
            );
        }
    }

    #[test]
    fn broker_url_rules_match_the_source() {
        let v = with(minimal(), "url", json!("tcp://broker"));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["url"]
        );
        let v = with(minimal(), "url", json!("mqtt://user:pw@broker"));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["url"]
        );
    }

    #[test]
    fn tls_block_rules_match_the_source() {
        let v = with(minimal(), "tls", json!({ "ca": "/ca.pem" }));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["tls"]
        );

        let mut v = with(
            minimal(),
            "tls",
            json!({ "ca": "/ca.pem", "cert": "/c.pem" }),
        );
        v["url"] = json!("mqtts://broker");
        let e = MqttPublishConfig::from_value(&v).unwrap_err();
        assert!(e[0].message.contains("together"));

        v["tls"] = json!({ "ca": "/ca.pem", "cert": "/c.pem", "key": "/k.pem" });
        assert!(MqttPublishConfig::from_value(&v).is_ok());

        let mut v = minimal();
        v["url"] = json!("mqtts://broker");
        assert!(
            MqttPublishConfig::from_value(&v).is_ok(),
            "native roots need no tls block"
        );
    }

    #[test]
    fn password_file_needs_a_username() {
        let v = with(minimal(), "password_file", json!("/run/secrets/pw"));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["password_file"]
        );
        let v = with(v, "username", json!("thadam"));
        assert!(MqttPublishConfig::from_value(&v).is_ok());
    }

    #[test]
    fn datasets_selects_exactly_one() {
        let v = with(minimal(), "datasets", json!(["imu", "pose"]));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["datasets"]
        );
        let v = with(minimal(), "datasets", json!([7]));
        assert_eq!(
            fields(&MqttPublishConfig::from_value(&v).unwrap_err()),
            ["datasets"]
        );
        let v = with(minimal(), "datasets", json!(["imu"]));
        assert!(MqttPublishConfig::from_value(&v).is_ok());
    }

    #[test]
    fn schema_lists_exactly_the_config_fields_and_is_closed() {
        let full = MqttPublishConfig::from_value(&minimal()).unwrap();
        let serialized = serde_json::to_value(&full).unwrap();
        let mut struct_fields: Vec<String> =
            serialized.as_object().unwrap().keys().cloned().collect();
        let schema = config_schema();
        let mut schema_fields: Vec<String> = schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        struct_fields.sort();
        schema_fields.sort();
        assert_eq!(struct_fields, schema_fields);
        assert_eq!(schema["additionalProperties"], json!(false));
        assert_eq!(schema["required"], json!(["url", "client_id", "topic"]));
        assert_eq!(
            schema["properties"]["tls"]["additionalProperties"],
            json!(false)
        );
    }

    #[test]
    fn config_round_trips_through_json() {
        let c = MqttPublishConfig::from_value(&minimal()).unwrap();
        let again = MqttPublishConfig::from_value(&serde_json::to_value(&c).unwrap()).unwrap();
        assert_eq!(c, again);
    }
}
