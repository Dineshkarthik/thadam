//! Typed, validated configuration for the MQTT source.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use thadam_core::{AckMode, ErrorDomain, ThadamError, codes};
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// Worst-case size of everything in an incoming PUBLISH except the payload:
/// two topic-length bytes, a topic of up to 65,535 bytes, and the packet id.
const PACKET_OVERHEAD: usize = 2 + 65_535 + 2;

/// Largest MQTT remaining-length value (268,435,455) the codec can express.
const MAX_REMAINING_LENGTH: u64 = 268_435_455;

const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1 << 20;
const DEFAULT_QOS0_QUEUE_BYTES: u64 = 8 << 20;

/// The MQTT protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Protocol {
    /// MQTT 3.1.1. The supported default.
    #[default]
    #[serde(rename = "v311")]
    V311,
    /// MQTT 5. Experimental: needs the `v5` cargo feature.
    #[serde(rename = "v5")]
    V5,
}

impl Protocol {
    /// The configuration spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Protocol::V311 => "v311",
            Protocol::V5 => "v5",
        }
    }
}

/// When messages are acknowledged to the broker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckModeConfig {
    /// After the WAL commit: at-least-once. The default.
    #[default]
    OnCommit,
    /// When the runtime pulls the message: at-most-once across a crash.
    OnReceive,
}

impl From<AckModeConfig> for AckMode {
    fn from(mode: AckModeConfig) -> Self {
        match mode {
            AckModeConfig::OnCommit => AckMode::OnCommit,
            AckModeConfig::OnReceive => AckMode::OnReceive,
        }
    }
}

/// A duration written as whole seconds (`30`) or with a unit (`500ms`, `30s`,
/// `5m`, `1h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DurationSpec(pub Duration);

impl DurationSpec {
    fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len());
        let (digits, unit) = text.split_at(split);
        let n: u64 = digits
            .parse()
            .map_err(|_| format!("`{text}` is not a duration like `30s` or `500ms`"))?;
        let overflow = || format!("duration `{text}` is too large");
        let duration = match unit.trim() {
            "" | "s" => Duration::from_secs(n),
            "ms" => Duration::from_millis(n),
            "m" => Duration::from_secs(n.checked_mul(60).ok_or_else(overflow)?),
            "h" => Duration::from_secs(n.checked_mul(3600).ok_or_else(overflow)?),
            other => return Err(format!("unknown duration unit `{other}` in `{text}`")),
        };
        Ok(DurationSpec(duration))
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = DurationSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("whole seconds or a duration string such as `30s`")
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
                DurationSpec::parse(v).map_err(E::custom)
            }
        }
        d.deserialize_any(Visitor)
    }
}

impl Serialize for DurationSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.0.subsec_millis() == 0 {
            s.serialize_str(&format!("{}s", self.0.as_secs()))
        } else {
            s.serialize_str(&format!("{}ms", self.0.as_millis()))
        }
    }
}

/// One subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfig {
    /// The topic filter, with `+` and `#` wildcards.
    pub filter: String,
    /// The requested QoS (0, 1, or 2). Defaults to 1.
    #[serde(default = "default_qos")]
    pub qos: u8,
}

fn default_qos() -> u8 {
    1
}

/// TLS settings. Absent `ca` means the platform's trust roots.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM file with the CA certificate(s) that sign the broker certificate.
    #[serde(default)]
    pub ca: Option<PathBuf>,
    /// PEM file with the client certificate chain (mutual TLS).
    #[serde(default)]
    pub cert: Option<PathBuf>,
    /// PEM file with the client private key (mutual TLS).
    #[serde(default)]
    pub key: Option<PathBuf>,
}

/// The configuration of one MQTT source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    /// `mqtt://host[:1883]` or `mqtts://host[:8883]`.
    pub url: String,
    /// The stable client identifier that names the persistent session.
    pub client_id: String,
    /// What to subscribe to.
    pub topics: Vec<TopicConfig>,
    /// Start a clean session on every connect. Incompatible with `on_commit`,
    /// because a clean session has nothing to redeliver after a crash.
    #[serde(default)]
    pub clean_start: bool,
    /// MQTT 5 session expiry interval.
    #[serde(default)]
    pub session_expiry: Option<DurationSpec>,
    /// The keepalive interval. At least 1 s (v311) or 5 s (v5); 30 to 60 s is
    /// recommended.
    #[serde(default = "default_keep_alive")]
    pub keep_alive: DurationSpec,
    /// Per-attempt connect timeout. It also bounds each network flush.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: DurationSpec,
    /// How long `open` waits for the first connection and subscription.
    #[serde(default = "default_open_timeout")]
    pub open_timeout: DurationSpec,
    /// MQTT 5 Receive Maximum: the window the client asks the broker for. It
    /// can only lower the broker's own window.
    #[serde(default)]
    pub receive_maximum: Option<u16>,
    /// TLS settings; only valid with an `mqtts://` URL.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// The username.
    #[serde(default)]
    pub username: Option<String>,
    /// A file holding the password, so the secret stays out of the config.
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    /// When messages are acknowledged.
    #[serde(default)]
    pub ack_mode: AckModeConfig,
    /// The inflight window you expect the broker to use (Mosquitto
    /// `max_inflight_messages`, EMQX `mqtt.max_inflight`). In MQTT 3.1.1 the
    /// broker, not the client, sets it; this value only feeds the memory
    /// estimate and the unacknowledged-message guard.
    #[serde(default = "default_expected_broker_window")]
    pub expected_broker_window: u32,
    /// The guard disconnects when unacknowledged messages exceed
    /// `expected_broker_window` times this.
    #[serde(default = "default_guard_multiple")]
    pub unacked_guard_multiple: u32,
    /// The byte budget of queued QoS 0 messages; beyond it the newest is
    /// dropped and counted.
    #[serde(default = "default_qos0_queue_bytes")]
    pub qos0_queue_bytes: u64,
    /// The largest payload accepted. The incoming packet limit is derived from it.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: u64,
    /// The protocol version.
    #[serde(default)]
    pub protocol: Protocol,
    /// Capacity of the client's request channel.
    #[serde(default = "default_request_channel_capacity")]
    pub request_channel_capacity: usize,
}

fn default_keep_alive() -> DurationSpec {
    DurationSpec(Duration::from_secs(30))
}
fn default_connect_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(10))
}
fn default_open_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(30))
}
fn default_expected_broker_window() -> u32 {
    100
}
fn default_guard_multiple() -> u32 {
    4
}
fn default_qos0_queue_bytes() -> u64 {
    DEFAULT_QOS0_QUEUE_BYTES
}
fn default_max_message_bytes() -> u64 {
    DEFAULT_MAX_MESSAGE_BYTES
}
fn default_request_channel_capacity() -> usize {
    1024
}

/// A parsed broker URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BrokerUrl {
    pub tls: bool,
    pub host: String,
    pub port: u16,
}

pub(crate) fn parse_broker_url(url: &str) -> Result<BrokerUrl, String> {
    let (tls, rest) = if let Some(rest) = url.strip_prefix("mqtts://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("mqtt://") {
        (false, rest)
    } else {
        return Err("the scheme must be `mqtt://` or `mqtts://`".into());
    };
    if rest.contains('@') {
        return Err(
            "credentials do not belong in the URL; use `username` and `password_file`".into(),
        );
    }
    if rest.contains(['/', '?', '#']) {
        return Err("the URL must be `scheme://host[:port]` with no path or query".into());
    }
    let default_port = if tls { 8883 } else { 1883 };
    let (host, port) = if let Some(bracketed) = rest.strip_prefix('[') {
        let (host, after) = bracketed
            .split_once(']')
            .ok_or("an IPv6 address needs a closing `]`")?;
        match after {
            "" => (host, default_port),
            _ => (
                host,
                parse_port(
                    after
                        .strip_prefix(':')
                        .ok_or("expected `:port` after `]`")?,
                )?,
            ),
        }
    } else {
        match rest.split_once(':') {
            Some((host, port)) => (host, parse_port(port)?),
            None => (rest, default_port),
        }
    };
    if host.is_empty() {
        return Err("the URL has no host".into());
    }
    if host.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("the host contains invalid characters".into());
    }
    Ok(BrokerUrl {
        tls,
        host: host.to_owned(),
        port,
    })
}

fn parse_port(text: &str) -> Result<u16, String> {
    match text.parse::<u16>() {
        Ok(0) | Err(_) => Err(format!("`{text}` is not a valid port")),
        Ok(port) => Ok(port),
    }
}

/// Checks a subscription filter against the MQTT topic filter rules.
pub(crate) fn validate_topic_filter(filter: &str) -> Result<(), String> {
    if filter.is_empty() {
        return Err("the filter is empty".into());
    }
    if filter.len() > 65_535 {
        return Err("the filter is longer than 65535 bytes".into());
    }
    if filter.contains('\0') {
        return Err("the filter contains a NUL character".into());
    }
    if filter == "$share" || filter.starts_with("$share/") {
        return Err(
            "shared subscriptions are not supported: they lose cross-connection ordering".into(),
        );
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || i + 1 != levels.len()) {
            return Err("`#` must be a whole level and the last one".into());
        }
        if level.contains('+') && *level != "+" {
            return Err("`+` must be a whole level".into());
        }
    }
    Ok(())
}

impl MqttConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<MqttConfig, Vec<ThadamError>> {
        let config: MqttConfig = serde_json::from_value(value.clone()).map_err(|e| {
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
            bad(
                "client_id",
                "a stable, non-empty client_id is required: it names the persistent session".into(),
            );
        } else if self.client_id.len() > 256 || self.client_id.chars().any(char::is_control) {
            bad(
                "client_id",
                "client_id must be at most 256 bytes with no control characters".into(),
            );
        }

        if self.topics.is_empty() {
            bad("topics", "at least one topic is required".into());
        }
        for (i, topic) in self.topics.iter().enumerate() {
            if let Err(e) = validate_topic_filter(&topic.filter) {
                bad("topics", format!("topics[{i}] `{}`: {e}", topic.filter));
            }
            if topic.qos > 2 {
                bad("topics", format!("topics[{i}]: qos must be 0, 1, or 2"));
            }
            if self.topics[..i].iter().any(|t| t.filter == topic.filter) {
                bad(
                    "topics",
                    format!("topics[{i}]: duplicate filter `{}`", topic.filter),
                );
            }
        }

        let min_keep_alive = match self.protocol {
            Protocol::V311 => 1,
            Protocol::V5 => 5,
        };
        let keep_alive = self.keep_alive.0;
        if keep_alive.subsec_nanos() != 0
            || keep_alive.as_secs() < min_keep_alive
            || keep_alive.as_secs() > u64::from(u16::MAX)
        {
            bad(
                "keep_alive",
                format!(
                    "keep_alive must be whole seconds from {min_keep_alive} to {} for {}; \
                     the keepalive is what keeps the connection up while acknowledgements are withheld",
                    u16::MAX,
                    self.protocol.as_str()
                ),
            );
        }
        for (field, spec) in [
            ("connect_timeout", self.connect_timeout),
            ("open_timeout", self.open_timeout),
        ] {
            if spec.0.subsec_nanos() != 0 || spec.0.as_secs() < 1 {
                bad(field, format!("{field} must be whole seconds, at least 1"));
            }
        }

        if self.clean_start && self.ack_mode == AckModeConfig::OnCommit {
            bad(
                "clean_start",
                "clean_start: true cannot give at-least-once with ack_mode `on_commit`: \
                 a clean session redelivers nothing after a crash; use `on_receive` or a persistent session"
                    .into(),
            );
        }

        match self.protocol {
            Protocol::V311 => {
                if self.session_expiry.is_some() {
                    bad(
                        "session_expiry",
                        "session_expiry needs `protocol: v5`".into(),
                    );
                }
                if self.receive_maximum.is_some() {
                    bad(
                        "receive_maximum",
                        "receive_maximum needs `protocol: v5`; in MQTT 3.1.1 the broker sets the window"
                            .into(),
                    );
                }
            }
            Protocol::V5 => {
                if !cfg!(feature = "v5") {
                    bad(
                        "protocol",
                        "`v5` is experimental and this build lacks the `v5` cargo feature".into(),
                    );
                }
                if self.receive_maximum == Some(0) {
                    bad(
                        "receive_maximum",
                        "receive_maximum must be at least 1".into(),
                    );
                }
                if let Some(expiry) = self.session_expiry
                    && expiry.0.as_secs() > u64::from(u32::MAX)
                {
                    bad(
                        "session_expiry",
                        "session_expiry exceeds 4294967295 seconds".into(),
                    );
                }
            }
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

        if !(1..=u32::from(u16::MAX)).contains(&self.expected_broker_window) {
            bad(
                "expected_broker_window",
                "expected_broker_window must be from 1 to 65535".into(),
            );
        }
        if self.unacked_guard_multiple < 2 {
            bad(
                "unacked_guard_multiple",
                "unacked_guard_multiple must be at least 2".into(),
            );
        }
        if self.max_message_bytes == 0
            || self.max_message_bytes > MAX_REMAINING_LENGTH - PACKET_OVERHEAD as u64
        {
            bad(
                "max_message_bytes",
                format!(
                    "max_message_bytes must be from 1 to {}",
                    MAX_REMAINING_LENGTH - PACKET_OVERHEAD as u64
                ),
            );
        }
        if self.qos0_queue_bytes < self.max_message_bytes {
            bad(
                "qos0_queue_bytes",
                "qos0_queue_bytes must hold at least one maximum-size message".into(),
            );
        }
        if self.request_channel_capacity < 16 {
            bad(
                "request_channel_capacity",
                "request_channel_capacity must be at least 16".into(),
            );
        }
        out
    }

    pub(crate) fn broker(&self) -> Result<BrokerUrl, String> {
        parse_broker_url(&self.url)
    }

    /// The largest packet the client accepts from the broker: the payload
    /// limit plus the worst-case header. Align the broker's own limit (Mosquitto
    /// `message_size_limit`, EMQX `mqtt.max_packet_size`) with `max_message_bytes`.
    pub fn max_incoming_packet_size(&self) -> usize {
        usize::try_from(self.max_message_bytes)
            .unwrap_or(usize::MAX - PACKET_OVERHEAD)
            .saturating_add(PACKET_OVERHEAD)
    }

    /// The unacknowledged-message count above which the guard disconnects.
    pub fn unacked_guard_limit(&self) -> usize {
        (self.expected_broker_window as usize).saturating_mul(self.unacked_guard_multiple as usize)
    }

    /// An upper bound on source-side memory: the broker window times the
    /// largest message, plus the QoS 0 queue budget.
    pub fn memory_estimate_bytes(&self) -> u64 {
        u64::from(self.expected_broker_window)
            .saturating_mul(self.max_message_bytes)
            .saturating_add(self.qos0_queue_bytes)
    }

    /// The throughput ceiling of one connection under `on_commit`
    /// (ADR-0006): `window / (commit_latency + RTT)` messages per second.
    pub fn max_messages_per_second(&self, commit_latency: Duration, rtt: Duration) -> f64 {
        let seconds = (commit_latency + rtt).as_secs_f64();
        if seconds <= 0.0 {
            return f64::INFINITY;
        }
        f64::from(self.expected_broker_window) / seconds
    }
}

/// The JSON Schema for the `config:` block of an MQTT source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "MQTT source",
        "type": "object",
        "additionalProperties": false,
        "required": ["url", "client_id", "topics"],
        "properties": {
            "url": {
                "type": "string",
                "pattern": "^mqtts?://[^/@?#]+$",
                "description": "mqtt://host[:1883] or mqtts://host[:8883]"
            },
            "client_id": {
                "type": "string", "minLength": 1, "maxLength": 256,
                "description": "Stable identifier that names the persistent session."
            },
            "topics": {
                "type": "array", "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["filter"],
                    "properties": {
                        "filter": { "type": "string", "minLength": 1 },
                        "qos": { "type": "integer", "minimum": 0, "maximum": 2, "default": 1 }
                    }
                }
            },
            "clean_start": { "type": "boolean", "default": false },
            "session_expiry": { "$ref": "#/$defs/duration", "description": "MQTT 5 only." },
            "keep_alive": { "$ref": "#/$defs/duration", "default": "30s" },
            "connect_timeout": { "$ref": "#/$defs/duration", "default": "10s" },
            "open_timeout": { "$ref": "#/$defs/duration", "default": "30s" },
            "receive_maximum": { "type": "integer", "minimum": 1, "maximum": 65535, "description": "MQTT 5 only." },
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
            "ack_mode": { "enum": ["on_commit", "on_receive"], "default": "on_commit" },
            "expected_broker_window": { "type": "integer", "minimum": 1, "maximum": 65535, "default": 100 },
            "unacked_guard_multiple": { "type": "integer", "minimum": 2, "default": 4 },
            "qos0_queue_bytes": { "type": "integer", "minimum": 1, "default": DEFAULT_QOS0_QUEUE_BYTES },
            "max_message_bytes": { "type": "integer", "minimum": 1, "default": DEFAULT_MAX_MESSAGE_BYTES },
            "protocol": { "enum": ["v311", "v5"], "default": "v311", "description": "v5 is experimental." },
            "request_channel_capacity": { "type": "integer", "minimum": 16, "default": 1024 }
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
            "client_id": "thadam-edge-01",
            "topics": [{ "filter": "robots/+/telemetry" }]
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
        let c = MqttConfig::from_value(&minimal()).unwrap();
        assert_eq!(c.topics[0].qos, 1);
        assert!(!c.clean_start);
        assert_eq!(c.ack_mode, AckModeConfig::OnCommit);
        assert_eq!(c.protocol, Protocol::V311);
        assert_eq!(c.keep_alive.0, Duration::from_secs(30));
        assert_eq!(c.expected_broker_window, 100);
        assert_eq!(c.max_message_bytes, 1 << 20);
        assert_eq!(c.qos0_queue_bytes, 8 << 20);
    }

    #[test]
    fn required_fields_report_config_missing() {
        let e = MqttConfig::from_value(&json!({ "url": "mqtt://b" })).unwrap_err();
        assert_eq!(e[0].code, codes::CONFIG_MISSING);
        assert!(e[0].message.contains("client_id"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let e = MqttConfig::from_value(&with(minimal(), "inflight", json!(5))).unwrap_err();
        assert_eq!(e[0].code, codes::CONFIG_INVALID);
        assert!(e[0].message.contains("inflight"));
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let mut v = minimal();
        v["url"] = json!("http://broker");
        v["client_id"] = json!("");
        v["topics"] = json!([]);
        v["expected_broker_window"] = json!(0);
        let e = MqttConfig::from_value(&v).unwrap_err();
        let f = fields(&e);
        for expected in ["url", "client_id", "topics", "expected_broker_window"] {
            assert!(f.contains(&expected), "{expected} missing from {f:?}");
        }
    }

    #[test]
    fn broker_urls_parse_with_default_ports_and_ipv6() {
        let url = |s| parse_broker_url(s).unwrap();
        assert_eq!(
            url("mqtt://localhost"),
            BrokerUrl {
                tls: false,
                host: "localhost".into(),
                port: 1883
            }
        );
        assert_eq!(
            url("mqtts://broker.example:8884"),
            BrokerUrl {
                tls: true,
                host: "broker.example".into(),
                port: 8884
            }
        );
        assert_eq!(url("mqtts://h").port, 8883);
        assert_eq!(
            url("mqtt://[::1]:1884"),
            BrokerUrl {
                tls: false,
                host: "::1".into(),
                port: 1884
            }
        );
        assert_eq!(url("mqtt://[::1]").port, 1883);
        for bad in [
            "tcp://h",
            "mqtt://",
            "mqtt://h:0",
            "mqtt://h:99999",
            "mqtt://u:p@h",
            "mqtt://h/path",
            "mqtt://[::1",
            "mqtt://h x",
        ] {
            assert!(parse_broker_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn topic_filters_follow_the_mqtt_rules() {
        for ok in [
            "a", "a/b", "a/+/b", "#", "a/#", "+", "+/+", "$SYS/#", "a//b",
        ] {
            assert!(validate_topic_filter(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "a/#/b",
            "a#",
            "a+/b",
            "a/b#",
            "#/a",
            "$share/g/a",
            "a\0b",
        ] {
            assert!(validate_topic_filter(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn duplicate_and_bad_topics_are_rejected() {
        let v = with(
            minimal(),
            "topics",
            json!([{ "filter": "a/#" }, { "filter": "a/#" }, { "filter": "b", "qos": 3 }]),
        );
        let e = MqttConfig::from_value(&v).unwrap_err();
        assert_eq!(e.len(), 2);
        assert!(e.iter().any(|e| e.message.contains("duplicate")));
        assert!(e.iter().any(|e| e.message.contains("qos")));
    }

    #[test]
    fn durations_accept_numbers_and_units() {
        let c = |v| DurationSpec::parse(v).unwrap().0;
        assert_eq!(c("30"), Duration::from_secs(30));
        assert_eq!(c("30s"), Duration::from_secs(30));
        assert_eq!(c("500ms"), Duration::from_millis(500));
        assert_eq!(c("5m"), Duration::from_secs(300));
        assert_eq!(c("1h"), Duration::from_secs(3600));
        assert!(DurationSpec::parse("fast").is_err());
        assert!(DurationSpec::parse("5y").is_err());
        assert!(DurationSpec::parse("").is_err());

        let cfg = MqttConfig::from_value(&with(minimal(), "keep_alive", json!(45))).unwrap();
        assert_eq!(cfg.keep_alive.0, Duration::from_secs(45));
        let cfg = MqttConfig::from_value(&with(minimal(), "keep_alive", json!("2m"))).unwrap();
        assert_eq!(cfg.keep_alive.0, Duration::from_secs(120));
        assert!(MqttConfig::from_value(&with(minimal(), "keep_alive", json!(-1))).is_err());
    }

    #[test]
    fn keepalive_must_be_whole_seconds_within_protocol_limits() {
        for bad in [json!(0), json!("500ms"), json!(70_000)] {
            let e = MqttConfig::from_value(&with(minimal(), "keep_alive", bad)).unwrap_err();
            assert_eq!(fields(&e), ["keep_alive"]);
        }
        assert!(MqttConfig::from_value(&with(minimal(), "keep_alive", json!(1))).is_ok());
    }

    #[test]
    fn clean_start_conflicts_with_on_commit() {
        let v = with(minimal(), "clean_start", json!(true));
        let e = MqttConfig::from_value(&v).unwrap_err();
        assert_eq!(fields(&e), ["clean_start"]);
        let v = with(v, "ack_mode", json!("on_receive"));
        assert!(MqttConfig::from_value(&v).is_ok());
    }

    #[test]
    fn v5_only_fields_are_rejected_on_v311() {
        let v = with(
            with(minimal(), "session_expiry", json!("1h")),
            "receive_maximum",
            json!(10),
        );
        let e = MqttConfig::from_value(&v).unwrap_err();
        assert_eq!(fields(&e), ["session_expiry", "receive_maximum"]);
    }

    #[test]
    fn v5_needs_the_cargo_feature() {
        let v = with(minimal(), "protocol", json!("v5"));
        let result = MqttConfig::from_value(&v);
        assert_eq!(result.is_ok(), cfg!(feature = "v5"));
        if let Err(e) = result {
            assert_eq!(fields(&e), ["protocol"]);
        }
    }

    #[test]
    fn tls_block_rules() {
        let v = with(minimal(), "tls", json!({ "ca": "/ca.pem" }));
        let e = MqttConfig::from_value(&v).unwrap_err();
        assert_eq!(fields(&e), ["tls"]);

        let mut v = with(
            minimal(),
            "tls",
            json!({ "ca": "/ca.pem", "cert": "/c.pem" }),
        );
        v["url"] = json!("mqtts://broker");
        let e = MqttConfig::from_value(&v).unwrap_err();
        assert!(e[0].message.contains("together"));

        v["tls"] = json!({ "ca": "/ca.pem", "cert": "/c.pem", "key": "/k.pem" });
        assert!(MqttConfig::from_value(&v).is_ok());

        let mut v = minimal();
        v["url"] = json!("mqtts://broker");
        assert!(
            MqttConfig::from_value(&v).is_ok(),
            "native roots need no tls block"
        );
    }

    #[test]
    fn password_file_needs_a_username() {
        let v = with(minimal(), "password_file", json!("/run/secrets/pw"));
        assert_eq!(
            fields(&MqttConfig::from_value(&v).unwrap_err()),
            ["password_file"]
        );
        let v = with(v, "username", json!("thadam"));
        assert!(MqttConfig::from_value(&v).is_ok());
    }

    #[test]
    fn size_limits_are_consistent() {
        let v = with(
            with(minimal(), "max_message_bytes", json!(4096)),
            "qos0_queue_bytes",
            json!(1024),
        );
        assert_eq!(
            fields(&MqttConfig::from_value(&v).unwrap_err()),
            ["qos0_queue_bytes"]
        );
        let v = with(minimal(), "max_message_bytes", json!(0));
        assert!(!fields(&MqttConfig::from_value(&v).unwrap_err()).is_empty());
        let v = with(minimal(), "max_message_bytes", json!(300_000_000u64));
        assert!(!fields(&MqttConfig::from_value(&v).unwrap_err()).is_empty());
    }

    #[test]
    fn incoming_packet_limit_is_derived_from_max_message_bytes() {
        let c = MqttConfig::from_value(&with(minimal(), "max_message_bytes", json!(2048))).unwrap();
        assert_eq!(c.max_incoming_packet_size(), 2048 + PACKET_OVERHEAD);
        let default = MqttConfig::from_value(&minimal()).unwrap();
        assert!(
            default.max_incoming_packet_size() > 10 * 1024,
            "rumqttc's 10 KiB default is not used"
        );
        assert!(default.max_incoming_packet_size() > 1 << 20);
    }

    #[test]
    fn estimates_follow_the_adr_formulas() {
        let c =
            MqttConfig::from_value(&with(minimal(), "expected_broker_window", json!(20))).unwrap();
        assert_eq!(c.memory_estimate_bytes(), 20 * (1 << 20) + (8 << 20));
        assert_eq!(c.unacked_guard_limit(), 80);
        let rate = c.max_messages_per_second(Duration::from_millis(100), Duration::ZERO);
        assert!((rate - 200.0).abs() < 1e-9, "{rate}");
        let rate = c.max_messages_per_second(Duration::from_millis(10), Duration::from_micros(160));
        assert!((rate - 1968.5).abs() < 1.0, "{rate}");
        assert!(
            c.max_messages_per_second(Duration::ZERO, Duration::ZERO)
                .is_infinite()
        );
    }

    #[test]
    fn schema_lists_exactly_the_config_fields() {
        let full = MqttConfig::from_value(&minimal()).unwrap();
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
        assert_eq!(schema["required"], json!(["url", "client_id", "topics"]));
    }

    #[test]
    fn config_round_trips_through_json() {
        let c = MqttConfig::from_value(&minimal()).unwrap();
        let again = MqttConfig::from_value(&serde_json::to_value(&c).unwrap()).unwrap();
        assert_eq!(c, again);
    }
}
