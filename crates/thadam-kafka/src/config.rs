//! Typed configuration for the `kind: kafka` batch sink, parsed from
//! `serde_json::Value` (D22), with a JSON Schema for `thadam validate`.
//!
//! Secrets arrive already resolved by `thadam-config`'s `${env:}` / `${file:}`
//! interpolation; they are wrapped in [`Secret`] so `Debug` never prints
//! them (ADR-0020).

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use thadam_core::{ErrorDomain, ThadamError, codes};

use crate::template::{KeyTemplate, TopicTemplate};

/// The default produce/leader timeout.
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The largest accepted timeout (guards typos like `30d`).
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(3600);
/// The default number of produce requests awaiting their response.
pub(crate) const DEFAULT_MAX_IN_FLIGHT: usize = 256;
/// The largest accepted in-flight window.
const MAX_IN_FLIGHT: usize = 10_000;
/// The default per-message size limit: the stock broker `message.max.bytes`.
pub(crate) const DEFAULT_MAX_MESSAGE_BYTES: usize = 1_048_576;
/// Guard-band bounds for `max_message_bytes` (typo protection only).
const MIN_MAX_MESSAGE_BYTES: usize = 1024;
/// The largest accepted `max_message_bytes`.
const MAX_MAX_MESSAGE_BYTES: usize = 268_435_456;

/// A parsed, validated Kafka sink configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct KafkaConfig {
    /// The bootstrap broker list, `host:port` entries.
    pub bootstrap: Vec<String>,
    /// The per-event topic template.
    pub topic: TopicTemplate,
    /// The per-event key template (`None` keys each record by its event id).
    pub key: Option<KeyTemplate>,
    /// TLS trust and client identity, if configured. Setting it enables TLS.
    pub tls: Option<TlsConfig>,
    /// SASL authentication, if configured.
    pub sasl: Option<Sasl>,
    /// The produce compression codec.
    pub compression: Compression,
    /// The per-request timeout (connect, metadata, produce).
    pub request_timeout: Duration,
    /// The number of produce requests awaiting their response.
    pub max_in_flight: usize,
    /// The largest serialized message size accepted per event; bigger events
    /// are rejected individually to quarantine.
    pub max_message_bytes: usize,
}

/// TLS trust and client identity (the thadam-nats semantics over PEM files).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// A PEM file of extra root certificates to trust.
    pub ca_file: Option<PathBuf>,
    /// A PEM client certificate chain.
    pub client_cert: Option<PathBuf>,
    /// The PEM private key for `client_cert`.
    pub client_key: Option<PathBuf>,
}

/// SASL authentication (rsasl-backed; OAUTHBEARER is not exposed).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Sasl {
    /// The mechanism.
    pub mechanism: SaslMechanism,
    /// The user name.
    pub user: String,
    /// The password, kept secret.
    pub password: Secret,
}

/// The SASL mechanism.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaslMechanism {
    /// PLAIN (RFC 4616).
    Plain,
    /// SCRAM-SHA-256 (RFC 7677).
    ScramSha256,
    /// SCRAM-SHA-512.
    ScramSha512,
}

/// The produce compression codec. Only codecs without a native dependency
/// are offered (ADR-0020 build policy): gzip is miniz_oxide, snappy is snap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    #[default]
    None,
    /// gzip (flate2's pure-Rust backend).
    Gzip,
    /// Snappy (snap).
    Snappy,
}

/// A secret that never prints its content.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    pub(crate) fn reveal(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    bootstrap: Vec<String>,
    topic: String,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    acks: Option<String>,
    #[serde(default)]
    tls: Option<RawTls>,
    #[serde(default)]
    sasl: Option<RawSasl>,
    #[serde(default)]
    compression: Option<String>,
    #[serde(default = "default_request_timeout", deserialize_with = "de_duration")]
    request_timeout: Duration,
    #[serde(default = "default_max_in_flight")]
    max_in_flight: usize,
    #[serde(default = "default_max_message_bytes")]
    max_message_bytes: usize,
    /// The runtime consumes this key (it restricts the datasets in the build
    /// context); it is parsed here so a malformed selector fails with a good
    /// message and so `deny_unknown_fields` accepts it.
    #[serde(default)]
    datasets: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTls {
    #[serde(default)]
    ca_file: Option<PathBuf>,
    #[serde(default)]
    client_cert: Option<PathBuf>,
    #[serde(default)]
    client_key: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSasl {
    mechanism: String,
    user: String,
    password: String,
}

fn default_request_timeout() -> Duration {
    DEFAULT_REQUEST_TIMEOUT
}

fn default_max_in_flight() -> usize {
    DEFAULT_MAX_IN_FLIGHT
}

fn default_max_message_bytes() -> usize {
    DEFAULT_MAX_MESSAGE_BYTES
}

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// A bootstrap entry is `host:port` — the host may be a name, an IP, or a
/// bracketed IPv6 literal; the port 1..=65535.
fn bootstrap_problem(entry: &str) -> Option<String> {
    let Some((host, port)) = entry.rsplit_once(':') else {
        return Some("must be `host:port`".to_owned());
    };
    if host.trim().is_empty() {
        return Some("the host part is empty".to_owned());
    }
    match port.parse::<u16>() {
        Ok(0) | Err(_) => Some(format!("`{port}` is not a port between 1 and 65535")),
        Ok(_) => None,
    }
}

fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("`{text}` is not a duration like `10s` or `500ms`"))?;
    let overflow = || format!("duration `{text}` is too large");
    Ok(match unit.trim() {
        "" | "s" => Duration::from_secs(n),
        "ms" => Duration::from_millis(n),
        "m" => Duration::from_secs(n.checked_mul(60).ok_or_else(overflow)?),
        "h" => Duration::from_secs(n.checked_mul(3600).ok_or_else(overflow)?),
        other => return Err(format!("unknown duration unit `{other}` in `{text}`")),
    })
}

fn de_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = Duration;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("whole seconds or a duration string such as `10s`")
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Duration, E> {
            Ok(Duration::from_secs(v))
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Duration, E> {
            parse_duration(v).map_err(E::custom)
        }
    }
    d.deserialize_any(V)
}

fn sasl_from(raw: RawSasl, errors: &mut Vec<ThadamError>) -> Option<Sasl> {
    let mechanism = match raw.mechanism.as_str() {
        "plain" => Some(SaslMechanism::Plain),
        "scram-sha-256" => Some(SaslMechanism::ScramSha256),
        "scram-sha-512" => Some(SaslMechanism::ScramSha512),
        other => {
            errors.push(config_error(
                "sasl.mechanism",
                format!("`{other}` is not one of `plain`, `scram-sha-256`, `scram-sha-512`"),
            ));
            None
        }
    };
    if raw.user.is_empty() {
        errors.push(config_error("sasl.user", "must not be empty"));
    }
    if raw.password.is_empty() {
        errors.push(config_error("sasl.password", "must not be empty"));
    }
    mechanism.map(|mechanism| Sasl {
        mechanism,
        user: raw.user,
        password: Secret::new(raw.password),
    })
}

/// Parses and validates the `config:` block of a `kind: kafka` sink. Every
/// problem found is reported, in configuration order.
pub fn parse_config(value: &serde_json::Value) -> Result<KafkaConfig, Vec<ThadamError>> {
    let raw: RawConfig = match serde_json::from_value(value.clone()) {
        Ok(raw) => raw,
        Err(e) => return Err(vec![config_error("config", e.to_string())]),
    };
    let mut errors = Vec::new();

    if raw.bootstrap.is_empty() {
        errors.push(config_error(
            "bootstrap",
            "must list at least one `host:port` bootstrap broker",
        ));
    }
    for (i, entry) in raw.bootstrap.iter().enumerate() {
        if let Some(problem) = bootstrap_problem(entry) {
            errors.push(config_error(
                "bootstrap",
                format!("entry {i} (`{entry}`): {problem}"),
            ));
        }
    }
    let topic = match TopicTemplate::parse(&raw.topic) {
        Ok(topic) => Some(topic),
        Err(message) => {
            errors.push(config_error("topic", message.to_string()));
            None
        }
    };
    let key = match &raw.key {
        Some(text) => match KeyTemplate::parse(text) {
            Ok(key) => Some(key),
            Err(message) => {
                errors.push(config_error("key", message.to_string()));
                None
            }
        },
        None => None,
    };
    if raw.acks.as_deref().is_some_and(|a| a != "all") {
        errors.push(config_error("acks", "the only supported `acks` is `all`"));
    }
    if raw.request_timeout < Duration::from_millis(1) {
        errors.push(config_error("request_timeout", "must be at least 1ms"));
    }
    if raw.request_timeout > MAX_REQUEST_TIMEOUT {
        errors.push(config_error(
            "request_timeout",
            "must be at most 1h (3600s)",
        ));
    }
    if raw.max_in_flight == 0 || raw.max_in_flight > MAX_IN_FLIGHT {
        errors.push(config_error(
            "max_in_flight",
            format!("must be between 1 and {MAX_IN_FLIGHT}"),
        ));
    }
    if !(MIN_MAX_MESSAGE_BYTES..=MAX_MAX_MESSAGE_BYTES).contains(&raw.max_message_bytes) {
        errors.push(config_error(
            "max_message_bytes",
            format!("must be between {MIN_MAX_MESSAGE_BYTES} and {MAX_MAX_MESSAGE_BYTES}"),
        ));
    }

    let compression = match raw.compression.as_deref() {
        None | Some("none") => Some(Compression::None),
        Some("gzip") => Some(Compression::Gzip),
        Some("snappy") => Some(Compression::Snappy),
        Some(other) => {
            errors.push(config_error(
                "compression",
                format!(
                    "`{other}` is not supported here: only `none`, `gzip`, and `snappy` add no native dependency (ADR-0020)"
                ),
            ));
            None
        }
    };

    let tls = raw.tls.map(|tls| {
        if tls.client_cert.is_some() != tls.client_key.is_some() {
            errors.push(config_error(
                "tls",
                "`client_cert` and `client_key` must be set together",
            ));
        }
        TlsConfig {
            ca_file: tls.ca_file,
            client_cert: tls.client_cert,
            client_key: tls.client_key,
        }
    });

    let sasl = raw.sasl.and_then(|sasl| sasl_from(sasl, &mut errors));
    if let Some(sasl) = &sasl
        && sasl.mechanism == SaslMechanism::Plain
        && tls.is_none()
    {
        errors.push(config_error(
            "sasl",
            "`mechanism: plain` sends the password in clear text; set `tls`",
        ));
    }

    if let Some(datasets) = &raw.datasets {
        if datasets.iter().any(|item| !item.is_string()) {
            errors.push(config_error(
                "datasets",
                "every `datasets` entry must be a dataset id string",
            ));
        }
        if datasets.len() != 1 {
            errors.push(config_error(
                "datasets",
                "a `kind: kafka` sink publishes exactly one dataset; add one sink entry per dataset",
            ));
        }
    }

    let (Some(topic), Some(compression)) = (topic, compression) else {
        return Err(errors);
    };
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(KafkaConfig {
        bootstrap: raw.bootstrap,
        topic,
        key,
        tls,
        sasl,
        compression,
        request_timeout: raw.request_timeout,
        max_in_flight: raw.max_in_flight,
        max_message_bytes: raw.max_message_bytes,
    })
}

/// The JSON Schema for a `kind: kafka` `config:` block.
pub fn config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Apache Kafka batch sink",
        "description": "Publishes one dataset's events to Kafka, one record per event with acks=all, the event id in the x-thadam-event-id header for consumer-side dedupe. Topics must already exist (the sink never creates them); at_least_once delivery.",
        "type": "object",
        "additionalProperties": false,
        "required": ["bootstrap", "topic"],
        "properties": {
            "bootstrap": {"type": "array", "minItems": 1, "items": {"type": "string"},
                "description": "Bootstrap brokers as `host:port` entries."},
            "topic": {"type": "string", "minLength": 1,
                "description": "Topic template. `{dataset}` is the dataset id; `{<level>}` is an identity level of the dataset (for example `telemetry.{dataset}.{robot}`). Placeholders span a whole dot-separated token; rendered names must match [a-zA-Z0-9._-] (249 bytes max)."},
            "key": {"type": "string", "minLength": 1,
                "description": "Key template; `{dataset}`, `{event_id}`, and identity levels, interpolated anywhere. Default: the event id (hex). Partitioning follows the record key."},
            "acks": {"type": "string", "enum": ["all"], "default": "all",
                "description": "Fixed `all` (the client produces with acks=-1)."},
            "tls": {
                "type": "object",
                "additionalProperties": false,
                "description": "TLS trust and client identity. Setting this block enables TLS.",
                "properties": {
                    "ca_file": {"type": "string"},
                    "client_cert": {"type": "string"},
                    "client_key": {"type": "string"}
                }
            },
            "sasl": {
                "type": "object",
                "additionalProperties": false,
                "required": ["mechanism", "user", "password"],
                "properties": {
                    "mechanism": {"type": "string", "enum": ["plain", "scram-sha-256", "scram-sha-512"]},
                    "user": {"type": "string"},
                    "password": {"type": "string", "description": "Supply through ${env:} or ${file:}; never printed."}
                }
            },
            "compression": {"type": "string", "enum": ["none", "gzip", "snappy"], "default": "none",
                "description": "Produce compression; only codecs without a native dependency are offered."},
            "request_timeout": {"type": ["integer", "string"], "default": "10s",
                "description": "Connect, metadata, and produce timeout, e.g. `500ms`, `10s`."},
            "max_in_flight": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 256,
                "description": "Produce requests awaiting their response."},
            "max_message_bytes": {"type": "integer", "minimum": 1024, "maximum": 268435456, "default": 1048576,
                "description": "Largest accepted serialized message size per event (key + value + headers); oversized events are rejected individually to quarantine."},
            "datasets": {"type": "array", "items": {"type": "string"}, "maxItems": 1,
                "description": "Selector consumed by the runtime; a kafka sink publishes exactly one dataset."}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_json() -> serde_json::Value {
        serde_json::json!({
            "bootstrap": ["kafka-1.internal:9092", "kafka-2.internal:9092"],
            "topic": "telemetry.{dataset}.{robot}",
            "key": "{robot}",
            "sasl": {"mechanism": "scram-sha-512", "user": "thadam", "password": "s3cret"},
            "request_timeout": "5s",
            "max_in_flight": 64,
            "max_message_bytes": 2097152,
            "compression": "gzip",
            "datasets": ["imu"]
        })
    }

    #[test]
    fn parses_a_full_config() {
        let cfg = parse_config(&demo_json()).expect("valid");
        assert_eq!(cfg.bootstrap.len(), 2);
        assert_eq!(cfg.topic.variables(), ["dataset", "robot"]);
        assert_eq!(cfg.key.expect("key").variables(), ["robot"]);
        assert!(matches!(
            cfg.sasl.as_ref().map(|s| &s.mechanism),
            Some(SaslMechanism::ScramSha512)
        ));
        assert_eq!(
            cfg.sasl.as_ref().map(|s| s.password.reveal()),
            Some("s3cret")
        );
        assert_eq!(cfg.request_timeout, Duration::from_secs(5));
        assert_eq!(cfg.max_in_flight, 64);
        assert_eq!(cfg.max_message_bytes, 2_097_152);
        assert_eq!(cfg.compression, Compression::Gzip);
    }

    #[test]
    fn defaults_fill_in() {
        let cfg = parse_config(&serde_json::json!({
            "bootstrap": ["h:9092"],
            "topic": "telemetry.{dataset}"
        }))
        .expect("valid");
        assert!(cfg.key.is_none());
        assert!(cfg.sasl.is_none());
        assert!(cfg.tls.is_none());
        assert_eq!(cfg.compression, Compression::None);
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(cfg.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert_eq!(cfg.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
    }

    #[test]
    fn bootstrap_entries_are_checked() {
        for (entries, want) in [
            (vec![], "at least one"),
            (vec!["no-colon"], "entry 0"),
            (vec![":9092"], "host part is empty"),
            (vec!["h:0"], "not a port"),
            (vec!["h:70000"], "not a port"),
            (vec!["h:abc"], "not a port"),
        ] {
            let v = serde_json::json!({"bootstrap": entries, "topic": "t.{dataset}"});
            let errs = parse_config(&v).expect_err(want);
            let text = errs
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join(" | ");
            assert!(text.contains(want), "missing `{want}` in: {text}");
        }
        let v = serde_json::json!({
            "bootstrap": ["h:9092", "[::1]:9093", "name.internal.:1"],
            "topic": "t.{dataset}"
        });
        assert!(parse_config(&v).is_ok());
    }

    #[test]
    fn collects_multiple_errors() {
        let v = serde_json::json!({
            "bootstrap": [],
            "topic": "t.{dataset",
            "key": "{",
            "acks": "1",
            "compression": "lz4",
            "request_timeout": "0s",
            "max_in_flight": 0,
            "max_message_bytes": 3,
            "sasl": {"mechanism": "gssapi", "user": "", "password": ""},
            "tls": {"client_cert": "/tmp/c.pem"},
            "datasets": ["a", "b"]
        });
        let errs = parse_config(&v).expect_err("invalid");
        let text = errs
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join(" | ");
        for want in [
            "at least one",
            "unclosed",
            "only supported `acks`",
            "native dependency",
            "at least 1ms",
            "between 1 and",
            "must be between 1024",
            "not one of",
            "must not be empty",
            "client_key",
            "exactly one dataset",
        ] {
            assert!(text.contains(want), "missing `{want}` in: {text}");
        }
    }

    #[test]
    fn timeout_forms_and_bounds() {
        let mut v = demo_json();
        v["request_timeout"] = serde_json::json!(45);
        assert_eq!(
            parse_config(&v).expect("seconds").request_timeout,
            Duration::from_secs(45)
        );
        v["request_timeout"] = serde_json::json!("500ms");
        assert_eq!(
            parse_config(&v).expect("millis").request_timeout,
            Duration::from_millis(500)
        );
        v["request_timeout"] = serde_json::json!("2h");
        assert!(parse_config(&v).is_err(), "above the cap");
        v["request_timeout"] = serde_json::json!("30fortnights");
        assert!(parse_config(&v).is_err(), "bad unit");
    }

    #[test]
    fn unknown_keys_are_rejected_and_schema_is_anchored() {
        let mut v = demo_json();
        v["bogus"] = serde_json::json!(1);
        let errs = parse_config(&v).expect_err("unknown key");
        assert!(errs.iter().any(|e| e.message.contains("bogus")));

        let schema = config_schema();
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
        assert_eq!(
            schema["required"],
            serde_json::json!(["bootstrap", "topic"])
        );
        assert!(schema["properties"]["sasl"].is_object());

        // A schema-only check: the sasl block requires all three fields.
        assert_eq!(
            schema["properties"]["sasl"]["required"],
            serde_json::json!(["mechanism", "user", "password"])
        );
    }

    #[test]
    fn plain_sasl_requires_tls() {
        let mut v = demo_json();
        v["sasl"] = serde_json::json!({"mechanism": "plain", "user": "thadam", "password": "p"});
        let errs = parse_config(&v).expect_err("plain without tls");
        assert!(errs.iter().any(|e| e.message.contains("clear text")));

        v["tls"] = serde_json::json!({});
        assert!(parse_config(&v).is_ok(), "plain over tls");
    }

    #[test]
    fn secrets_are_redacted() {
        let cfg = parse_config(&demo_json()).expect("valid");
        let shown = format!("{:?}", cfg.sasl);
        assert!(!shown.contains("s3cret"), "redacted: {shown}");
    }

    #[test]
    fn datasets_must_select_exactly_one() {
        let mut v = demo_json();
        v["datasets"] = serde_json::json!(["a", "b"]);
        assert!(parse_config(&v).is_err());
        v["datasets"] = serde_json::Value::Null;
        assert!(parse_config(&v).is_ok());
    }

    #[test]
    fn tls_client_identity_needs_both_halves() {
        let mut v = demo_json();
        v["tls"] = serde_json::json!({"client_cert": "/tmp/c.pem"});
        let errs = parse_config(&v).expect_err("cert without key");
        assert!(errs.iter().any(|e| e.message.contains("client_key")));
    }
}
