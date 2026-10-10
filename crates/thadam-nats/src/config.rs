//! Typed configuration for the `kind: nats` batch sink, parsed from
//! `serde_json::Value` (D22), with a JSON Schema for `thadam validate`.
//!
//! Secrets arrive already resolved by `thadam-config`'s `${env:}` / `${file:}`
//! interpolation; they are wrapped in [`Secret`] so `Debug` never prints them
//! (ADR-0020).

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;

use crate::envelope::SubjectTemplate;

/// The default connect and request timeout.
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The largest accepted timeout (guards typos like `30d`).
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(3600);
/// The duplicate window of a stream this sink creates (`create_stream: true`).
pub(crate) const DEFAULT_DUPLICATE_WINDOW: Duration = Duration::from_secs(120);
/// The default number of publishes awaiting their acknowledgement.
pub(crate) const DEFAULT_MAX_IN_FLIGHT: usize = 256;
/// The largest accepted in-flight window.
const MAX_IN_FLIGHT: usize = 10_000;

/// A parsed, validated NATS sink configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct NatsConfig {
    /// The server address, `nats://host:4222` or `tls://host:4222`.
    pub url: String,
    /// The per-event subject template.
    pub subject: SubjectTemplate,
    /// The JetStream stream that must bind the subjects.
    pub stream: String,
    /// Create the stream at `open` when it is missing.
    pub create_stream: bool,
    /// The duplicate window of a created stream.
    pub duplicate_window: Duration,
    /// How the sink authenticates.
    pub auth: Auth,
    /// TLS trust and client identity, if configured.
    pub tls: Option<TlsConfig>,
    /// The connect and per-request timeout.
    pub request_timeout: Duration,
    /// The number of publishes awaiting their acknowledgement.
    pub max_in_flight: usize,
}

impl NatsConfig {
    /// Whether the connection must upgrade to TLS.
    pub fn require_tls(&self) -> bool {
        self.url.starts_with("tls://") || self.tls.is_some()
    }
}

/// The credential the sink presents. At most one method is configured.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum Auth {
    /// No credentials.
    #[default]
    None,
    /// A shared token.
    Token(Secret),
    /// A user and password.
    UserPassword {
        /// The user name.
        user: String,
        /// The password, kept secret.
        password: Secret,
    },
    /// A NATS credentials file (user JWT and NKey seed).
    CredsFile(PathBuf),
}

/// TLS trust and client identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// A PEM file of extra root certificates to trust.
    pub ca_file: Option<PathBuf>,
    /// A PEM client certificate chain.
    pub client_cert: Option<PathBuf>,
    /// The PEM private key for `client_cert`.
    pub client_key: Option<PathBuf>,
}

/// A secret that never prints its content.
#[derive(Clone)]
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
    url: String,
    subject: String,
    stream: String,
    #[serde(default)]
    create_stream: bool,
    #[serde(default = "default_duplicate_window", deserialize_with = "de_duration")]
    duplicate_window: Duration,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    creds_file: Option<PathBuf>,
    #[serde(default)]
    tls: Option<RawTls>,
    #[serde(default)]
    payload: Option<String>,
    #[serde(default = "default_request_timeout", deserialize_with = "de_duration")]
    request_timeout: Duration,
    #[serde(default = "default_max_in_flight")]
    max_in_flight: usize,
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

fn default_duplicate_window() -> Duration {
    DEFAULT_DUPLICATE_WINDOW
}

fn default_request_timeout() -> Duration {
    DEFAULT_REQUEST_TIMEOUT
}

fn default_max_in_flight() -> usize {
    DEFAULT_MAX_IN_FLIGHT
}

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// A JetStream stream name: non-empty, with no whitespace and none of the
/// characters NATS reserves in subjects.
fn stream_name_ok(name: &str) -> bool {
    !name.is_empty()
        && !name
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '.' | '*' | '>' | '/' | '\\'))
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

fn auth_from(raw: &RawConfig, errors: &mut Vec<ThadamError>) -> Auth {
    let methods = [
        raw.token.is_some(),
        raw.user.is_some() || raw.password.is_some(),
        raw.creds_file.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if methods > 1 {
        errors.push(config_error(
            "token",
            "choose one credential: `token`, `user` with `password`, or `creds_file`",
        ));
        return Auth::None;
    }
    if let Some(token) = &raw.token {
        if token.is_empty() {
            errors.push(config_error("token", "must not be empty"));
        }
        return Auth::Token(Secret::new(token.clone()));
    }
    match (&raw.user, &raw.password) {
        (Some(user), Some(password)) => {
            return Auth::UserPassword {
                user: user.clone(),
                password: Secret::new(password.clone()),
            };
        }
        (None, None) => {}
        _ => errors.push(config_error(
            "password",
            "`user` and `password` must be set together",
        )),
    }
    match &raw.creds_file {
        Some(path) => Auth::CredsFile(path.clone()),
        None => Auth::None,
    }
}

/// Parses and validates the `config:` block of a `kind: nats` sink. Every
/// problem found is reported, in configuration order.
pub fn parse_config(value: &serde_json::Value) -> Result<NatsConfig, Vec<ThadamError>> {
    let raw: RawConfig = match serde_json::from_value(value.clone()) {
        Ok(raw) => raw,
        Err(e) => return Err(vec![config_error("config", e.to_string())]),
    };
    let mut errors = Vec::new();

    if !(raw.url.starts_with("nats://") || raw.url.starts_with("tls://")) {
        errors.push(config_error(
            "url",
            "`url` must be a nats:// or tls:// server address",
        ));
    }
    let subject = match SubjectTemplate::parse(&raw.subject) {
        Ok(subject) => Some(subject),
        Err(message) => {
            errors.push(config_error("subject", message.to_string()));
            None
        }
    };
    if !stream_name_ok(&raw.stream) {
        errors.push(config_error(
            "stream",
            format!("`{}` is not a usable JetStream stream name", raw.stream),
        ));
    }
    if raw.payload.as_deref().is_some_and(|p| p != "json") {
        errors.push(config_error(
            "payload",
            "the only supported `payload` is `json`",
        ));
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

    let auth = auth_from(&raw, &mut errors);

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
                "a `kind: nats` sink publishes exactly one dataset; add one sink entry per dataset",
            ));
        }
    }

    let Some(subject) = subject else {
        return Err(errors);
    };
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(NatsConfig {
        url: raw.url,
        subject,
        stream: raw.stream,
        create_stream: raw.create_stream,
        duplicate_window: raw.duplicate_window,
        auth,
        tls,
        request_timeout: raw.request_timeout,
        max_in_flight: raw.max_in_flight,
    })
}

/// The JSON Schema for a `kind: nats` `config:` block.
pub fn config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "NATS JetStream batch sink",
        "description": "Publishes one dataset's events to a JetStream stream, one acknowledged message per event, deduplicated by Nats-Msg-Id (the event id). The stream must already exist unless create_stream is true.",
        "type": "object",
        "additionalProperties": false,
        "required": ["url", "subject", "stream"],
        "properties": {
            "url": {"type": "string", "pattern": "^(nats|tls)://",
                "description": "nats:// or tls:// server address."},
            "subject": {"type": "string", "minLength": 1,
                "description": "Publish subject template. `{dataset}` is the dataset id; `{<level>}` is an identity level of the dataset (for example `thadam.{dataset}.{robot}`). Variable values must not contain `.`, `*`, `>` or whitespace."},
            "stream": {"type": "string", "minLength": 1,
                "description": "JetStream stream that binds the subject. Must exist unless create_stream is true."},
            "create_stream": {"type": "boolean", "default": false,
                "description": "Create the stream at open when it is missing, with the subject wildcarded from the template."},
            "duplicate_window": {"type": ["integer", "string"], "default": "2m",
                "description": "Duplicate window of a created stream, e.g. `2m`. A stream that already exists keeps its own window."},
            "token": {"type": "string", "description": "Shared token. Supply through ${env:} or ${file:}; never printed."},
            "user": {"type": "string", "description": "User name; requires password."},
            "password": {"type": "string", "description": "Password; supply through ${env:} or ${file:}."},
            "creds_file": {"type": "string", "description": "NATS credentials file path."},
            "tls": {
                "type": "object",
                "additionalProperties": false,
                "description": "TLS trust and client identity. Setting this block requires TLS.",
                "properties": {
                    "ca_file": {"type": "string"},
                    "client_cert": {"type": "string"},
                    "client_key": {"type": "string"}
                }
            },
            "payload": {"type": "string", "enum": ["json"], "default": "json"},
            "request_timeout": {"type": ["integer", "string"], "default": "10s",
                "description": "Connect and per-request timeout, e.g. `500ms`, `10s`."},
            "max_in_flight": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 256,
                "description": "Publishes awaiting their acknowledgement."},
            "datasets": {"type": "array", "items": {"type": "string"}, "maxItems": 1,
                "description": "Selector consumed by the runtime; a nats sink publishes exactly one dataset."}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_json() -> serde_json::Value {
        serde_json::json!({
            "url": "nats://nats.internal:4222",
            "subject": "thadam.{dataset}.{robot}",
            "stream": "THADAM",
            "token": "s3cret",
            "request_timeout": "5s",
            "max_in_flight": 64,
            "datasets": ["imu"]
        })
    }

    #[test]
    fn parses_a_full_config() {
        let cfg = parse_config(&demo_json()).expect("valid");
        assert_eq!(cfg.url, "nats://nats.internal:4222");
        assert_eq!(cfg.stream, "THADAM");
        assert!(!cfg.create_stream);
        assert_eq!(cfg.duplicate_window, DEFAULT_DUPLICATE_WINDOW);
        assert_eq!(cfg.request_timeout, Duration::from_secs(5));
        assert_eq!(cfg.max_in_flight, 64);
        assert!(!cfg.require_tls());
        assert!(matches!(&cfg.auth, Auth::Token(t) if t.reveal() == "s3cret"));
    }

    #[test]
    fn defaults_fill_in() {
        let cfg = parse_config(&serde_json::json!({
            "url": "nats://h:4222",
            "subject": "thadam.{dataset}",
            "stream": "S"
        }))
        .expect("valid");
        assert!(matches!(cfg.auth, Auth::None));
        assert!(cfg.tls.is_none());
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(cfg.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
    }

    #[test]
    fn credentials_are_mutually_exclusive() {
        let mut v = demo_json();
        v["user"] = serde_json::json!("alice");
        let errs = parse_config(&v).expect_err("token and user");
        assert!(
            errs.iter()
                .any(|e| e.message.contains("choose one credential"))
        );

        let mut v = demo_json();
        v.as_object_mut().expect("object").remove("token");
        v["user"] = serde_json::json!("alice");
        let errs = parse_config(&v).expect_err("user without password");
        assert!(errs.iter().any(|e| e.message.contains("set together")));

        v["password"] = serde_json::json!("pw");
        let cfg = parse_config(&v).expect("user and password");
        assert!(matches!(cfg.auth, Auth::UserPassword { .. }));
    }

    #[test]
    fn tls_client_identity_needs_both_halves() {
        let mut v = demo_json();
        v["tls"] = serde_json::json!({"client_cert": "/tmp/c.pem"});
        let errs = parse_config(&v).expect_err("cert without key");
        assert!(errs.iter().any(|e| e.message.contains("client_key")));

        v["tls"] = serde_json::json!({"ca_file": "/tmp/ca.pem"});
        let cfg = parse_config(&v).expect("CA only");
        assert!(cfg.require_tls());
    }

    #[test]
    fn collects_multiple_errors() {
        let v = serde_json::json!({
            "url": "http://h:4222",
            "subject": "thadam.{dataset",
            "stream": "bad.name",
            "payload": "avro",
            "request_timeout": "0s",
            "max_in_flight": 0,
            "token": "x",
            "password": "y"
        });
        let errs = parse_config(&v).expect_err("invalid");
        let text = errs
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join(" | ");
        for want in [
            "nats:// or tls://",
            "unclosed",
            "stream name",
            "only supported `payload`",
            "at least 1ms",
            "between 1 and",
            "choose one credential",
        ] {
            assert!(text.contains(want), "missing `{want}` in: {text}");
        }
    }

    #[test]
    fn datasets_must_select_exactly_one() {
        let mut v = demo_json();
        v["datasets"] = serde_json::json!(["a", "b"]);
        let errs = parse_config(&v).expect_err("two datasets");
        assert!(
            errs.iter()
                .any(|e| e.message.contains("exactly one dataset"))
        );
        v["datasets"] = serde_json::Value::Null;
        assert!(parse_config(&v).is_ok());
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
            serde_json::json!(["url", "subject", "stream"])
        );
        assert!(schema["properties"]["token"].is_object());
    }

    #[test]
    fn secrets_are_redacted() {
        let cfg = parse_config(&demo_json()).expect("valid");
        let shown = format!("{:?}", cfg.auth);
        assert!(!shown.contains("s3cret"), "redacted: {shown}");
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
}
