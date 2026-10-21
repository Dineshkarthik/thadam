//! Typed, validated configuration for the HTTP source.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::time::Duration;

use axum::http::{HeaderName, HeaderValue};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use thadam_core::{ErrorDomain, ThadamError, codes};

/// The largest message accepted by default: 1 MiB.
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1 << 20;
/// The most messages accepted but not yet committed, by default.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;
/// The smallest queue that still lets a burst of clients queue behind a commit.
pub const MIN_QUEUE_CAPACITY: usize = 16;

const DEFAULT_BIND: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
const DEFAULT_PATH: &str = "/ingest";

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

/// Which way messages come in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpMode {
    /// Serve `POST` requests; each body is one message. The default.
    #[default]
    Listen,
    /// Periodically fetch URLs; each response body is one message.
    Poll,
}

/// Server certificate and key, both PEM files.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert: PathBuf,
    /// PEM private key.
    pub key: PathBuf,
}

/// Settings for `mode: listen`.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenConfig {
    /// The address to bind. Loopback by default.
    pub bind: SocketAddr,
    /// The path that accepts `POST`.
    pub path: String,
    /// When set, requests must send `Authorization: Bearer <token>`.
    pub bearer_token: Option<String>,
    /// When set, the server speaks TLS.
    pub tls: Option<TlsConfig>,
    /// Request headers copied into `http.header.<name>` attributes.
    pub extract_headers: Vec<String>,
}

impl ListenConfig {
    /// Bearer token travels on a non-loopback address without TLS.
    pub(crate) fn sends_credentials_unsecured(&self) -> bool {
        !self.bind.ip().is_loopback() && self.bearer_token.is_some() && self.tls.is_none()
    }
}

impl Default for ListenConfig {
    fn default() -> Self {
        ListenConfig {
            bind: DEFAULT_BIND,
            path: DEFAULT_PATH.into(),
            bearer_token: None,
            tls: None,
            extract_headers: Vec::new(),
        }
    }
}

impl fmt::Debug for ListenConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenConfig")
            .field("bind", &self.bind)
            .field("path", &self.path)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("tls", &self.tls)
            .field("extract_headers", &self.extract_headers)
            .finish()
    }
}

/// The HTTP method a poll uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PollMethod {
    /// `GET`. The default.
    #[default]
    Get,
    /// `POST` with `body`.
    Post,
}

/// Settings for `mode: poll`.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PollConfig {
    /// Absolute `http://` or `https://` URLs, each polled on its own schedule.
    pub urls: Vec<String>,
    /// The request method.
    pub method: PollMethod,
    /// The request body for `method: post`.
    pub body: Option<String>,
    /// Headers sent with every request. Values may be secrets.
    pub headers: BTreeMap<String, String>,
    /// Time between successful polls of one URL.
    pub interval: DurationSpec,
    /// Per-request deadline, covering connect, headers, and body.
    pub timeout: DurationSpec,
    /// The longest wait after consecutive failures.
    pub backoff_max: DurationSpec,
}

impl Default for PollConfig {
    fn default() -> Self {
        PollConfig {
            urls: Vec::new(),
            method: PollMethod::Get,
            body: None,
            headers: BTreeMap::new(),
            interval: DurationSpec(Duration::from_secs(10)),
            timeout: DurationSpec(Duration::from_secs(30)),
            backoff_max: DurationSpec(Duration::from_secs(300)),
        }
    }
}

impl fmt::Debug for PollConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollConfig")
            .field("urls", &self.urls)
            .field("method", &self.method)
            .field("body", &self.body.as_ref().map(|_| "<redacted>"))
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("interval", &self.interval.0)
            .field("timeout", &self.timeout.0)
            .field("backoff_max", &self.backoff_max.0)
            .finish()
    }
}

/// The whole `config:` block of an HTTP source.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// `listen` or `poll`.
    pub mode: HttpMode,
    /// Settings for `mode: listen`. Defaults apply when absent.
    pub listen: Option<ListenConfig>,
    /// Settings for `mode: poll`. Required in that mode.
    pub poll: Option<PollConfig>,
    /// The largest accepted request or response body, in bytes.
    pub max_message_bytes: u64,
    /// Most messages accepted but not yet committed. Above this, listen
    /// answers `503`.
    pub queue_capacity: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            mode: HttpMode::Listen,
            listen: None,
            poll: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

impl HttpConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<HttpConfig, Vec<ThadamError>> {
        let config: HttpConfig = serde_json::from_value(value.clone()).map_err(|e| {
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
            if let Some(listen) = &config.listen
                && config.sends_credentials_unsecured()
            {
                tracing::warn!(
                    bind = %listen.bind,
                    "HTTP bearer_token is configured on a non-loopback address without tls; the token is not encrypted"
                );
            }
            Ok(config)
        } else {
            Err(errors)
        }
    }

    /// The listen settings in effect: the configured block or the defaults.
    pub fn listen_settings(&self) -> ListenConfig {
        self.listen.clone().unwrap_or_default()
    }

    /// Whether `mode: listen` accepts a `bearer_token` on a non-loopback
    /// address without `tls`.
    pub(crate) fn sends_credentials_unsecured(&self) -> bool {
        self.mode == HttpMode::Listen
            && self
                .listen
                .as_ref()
                .is_some_and(ListenConfig::sends_credentials_unsecured)
    }

    /// Every validation problem, empty when the configuration is sound.
    pub fn problems(&self) -> Vec<ThadamError> {
        let mut found: Vec<(&'static str, String)> = Vec::new();

        if self.max_message_bytes == 0 {
            found.push((
                "max_message_bytes",
                "max_message_bytes must be at least 1".into(),
            ));
        }
        if self.queue_capacity < MIN_QUEUE_CAPACITY {
            found.push((
                "queue_capacity",
                format!("queue_capacity must be at least {MIN_QUEUE_CAPACITY}"),
            ));
        }

        match self.mode {
            HttpMode::Listen => {
                if self.poll.is_some() {
                    found.push(("poll", "a poll block needs `mode: poll`".into()));
                }
                if let Some(listen) = &self.listen {
                    found.extend(listen_problems(listen));
                }
            }
            HttpMode::Poll => {
                if self.listen.is_some() {
                    found.push(("listen", "a listen block needs `mode: listen`".into()));
                }
                match &self.poll {
                    None => found.push((
                        "poll",
                        "`mode: poll` needs a poll block with at least one url".into(),
                    )),
                    Some(poll) => found.extend(poll_problems(poll)),
                }
            }
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

fn listen_problems(listen: &ListenConfig) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();

    let path = &listen.path;
    if !path.starts_with('/')
        || path.contains(|c: char| c.is_whitespace() || matches!(c, '?' | '#' | '{' | '}' | '*'))
    {
        found.push((
            "path",
            "path must start with `/` and contain no whitespace, `?`, `#`, `{`, `}`, or `*`".into(),
        ));
    }

    if let Some(token) = &listen.bearer_token
        && (token.is_empty() || token.chars().any(char::is_control))
    {
        found.push((
            "bearer_token",
            "bearer_token must be non-empty with no control characters".into(),
        ));
    }

    if !listen.bind.ip().is_loopback() && listen.bearer_token.is_none() {
        found.push((
            "bind",
            format!(
                "binding to {} exposes the endpoint beyond loopback: set `bearer_token`",
                listen.bind.ip()
            ),
        ));
    }

    let mut seen = Vec::new();
    for name in &listen.extract_headers {
        if HeaderName::from_bytes(name.as_bytes()).is_err() {
            found.push((
                "extract_headers",
                format!("`{name}` is not a valid header name"),
            ));
            continue;
        }
        let lower = name.to_ascii_lowercase();
        if seen.contains(&lower) {
            found.push((
                "extract_headers",
                format!("header `{lower}` is listed more than once"),
            ));
        }
        seen.push(lower);
    }

    found
}

fn poll_problems(poll: &PollConfig) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();

    if poll.urls.is_empty() {
        found.push(("urls", "at least one url is required".into()));
    }
    for (i, url) in poll.urls.iter().enumerate() {
        if !is_http_url(url) {
            found.push((
                "urls",
                format!("urls[{i}] must be an absolute http:// or https:// URL"),
            ));
        }
    }

    if poll.method == PollMethod::Get && poll.body.is_some() {
        found.push(("body", "a body needs `method: post`".into()));
    }

    for (name, value) in &poll.headers {
        if HeaderName::from_bytes(name.as_bytes()).is_err() {
            found.push(("headers", format!("header name `{name}` is not valid")));
        } else if HeaderValue::from_str(value).is_err() {
            found.push((
                "headers",
                format!("the value of header `{name}` is not a valid header value"),
            ));
        }
    }

    for (field, spec) in [("interval", poll.interval), ("timeout", poll.timeout)] {
        if spec.0.is_zero() {
            found.push((field, format!("{field} must be greater than zero")));
        }
    }
    if poll.backoff_max.0 < poll.interval.0 {
        found.push((
            "backoff_max",
            "backoff_max must be at least interval".into(),
        ));
    }

    found
}

/// `http://` or `https://`, then a non-empty authority, with no whitespace,
/// control characters, or fragment. The URL itself is never echoed in errors:
/// it may carry credentials or tokens in its query.
fn is_http_url(url: &str) -> bool {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"));
    match rest {
        Some(rest) => {
            let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
            authority_end > 0
                && !url.contains('#')
                && !url.chars().any(|c| c.is_whitespace() || c.is_control())
        }
        None => false,
    }
}

/// The JSON Schema for the `config:` block of an HTTP source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "HTTP source",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "mode": {
                "type": "string",
                "enum": ["listen", "poll"],
                "default": "listen"
            },
            "listen": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "bind": {
                        "type": "string",
                        "description": "Socket address, for example 127.0.0.1:8080. Non-loopback needs bearer_token.",
                        "default": "127.0.0.1:8080"
                    },
                    "path": {
                        "type": "string",
                        "pattern": "^/[^?#{}*\\s]*$",
                        "default": "/ingest"
                    },
                    "bearer_token": {"type": "string", "minLength": 1},
                    "tls": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["cert", "key"],
                        "properties": {
                            "cert": {"type": "string"},
                            "key": {"type": "string"}
                        }
                    },
                    "extract_headers": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                }
            },
            "poll": {
                "type": "object",
                "additionalProperties": false,
                "required": ["urls"],
                "properties": {
                    "urls": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "pattern": "^https?://[^/?#\\s]"}
                    },
                    "method": {"type": "string", "enum": ["get", "post"], "default": "get"},
                    "body": {"type": "string"},
                    "headers": {
                        "type": "object",
                        "additionalProperties": {"type": "string"}
                    },
                    "interval": {"$ref": "#/$defs/duration", "default": "10s"},
                    "timeout": {"$ref": "#/$defs/duration", "default": "30s"},
                    "backoff_max": {"$ref": "#/$defs/duration", "default": "5m"}
                }
            },
            "max_message_bytes": {"type": "integer", "minimum": 1, "default": 1048576},
            "queue_capacity": {"type": "integer", "minimum": 16, "default": 1024}
        },
        "if": {"properties": {"mode": {"const": "poll"}}, "required": ["mode"]},
        "then": {"required": ["poll"], "not": {"required": ["listen"]}},
        "else": {"not": {"required": ["poll"]}},
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
    use serde_json::json;

    fn codes_of(value: serde_json::Value) -> Vec<(&'static str, String)> {
        match HttpConfig::from_value(&value) {
            Ok(_) => Vec::new(),
            Err(errors) => errors.into_iter().map(|e| (e.code, e.message)).collect(),
        }
    }

    fn fields(value: serde_json::Value) -> Vec<String> {
        match HttpConfig::from_value(&value) {
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
    fn empty_block_is_a_valid_loopback_listener() {
        let cfg = HttpConfig::from_value(&json!({})).expect("valid");
        assert_eq!(cfg.mode, HttpMode::Listen);
        let listen = cfg.listen_settings();
        assert_eq!(listen.bind, DEFAULT_BIND);
        assert_eq!(listen.path, "/ingest");
        assert_eq!(cfg.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
        assert_eq!(cfg.queue_capacity, DEFAULT_QUEUE_CAPACITY);
    }

    #[test]
    fn minimal_poll_block_gets_documented_defaults() {
        let cfg = HttpConfig::from_value(&json!({
            "mode": "poll",
            "poll": {"urls": ["http://127.0.0.1:9/x"]}
        }))
        .expect("valid");
        let poll = cfg.poll.expect("poll block");
        assert_eq!(poll.method, PollMethod::Get);
        assert_eq!(poll.interval.0, Duration::from_secs(10));
        assert_eq!(poll.timeout.0, Duration::from_secs(30));
        assert_eq!(poll.backoff_max.0, Duration::from_secs(300));
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        for value in [
            json!({"nope": 1}),
            json!({"listen": {"bind": "127.0.0.1:1", "nope": true}}),
            json!({"mode": "poll", "poll": {"urls": ["http://a"], "nope": 1}}),
            json!({"listen": {"tls": {"cert": "c", "key": "k", "nope": 1}}}),
        ] {
            let found = codes_of(value.clone());
            assert_eq!(found.len(), 1, "{value}");
            assert_eq!(found[0].0, codes::CONFIG_INVALID, "{value}");
            assert!(found[0].1.contains("unknown field"), "{}", found[0].1);
        }
    }

    #[test]
    fn non_loopback_bind_requires_token_and_flags_plaintext_token() {
        assert_eq!(
            fields(json!({"listen": {"bind": "0.0.0.0:8080"}})),
            vec!["bind".to_string()]
        );
        assert_eq!(
            fields(json!({"listen": {"bind": "[::]:8080"}})),
            vec!["bind".to_string()]
        );
        assert_eq!(
            fields(json!({
                "listen": {
                    "bind": "[::]:8443",
                    "tls": {"cert": "/c.pem", "key": "/k.pem"}
                }
            })),
            vec!["bind".to_string()],
            "TLS without client auth does not authenticate callers"
        );
        let plaintext = HttpConfig::from_value(&json!({
            "listen": {"bind": "0.0.0.0:8080", "bearer_token": "s3cret"}
        }))
        .expect("valid");
        assert!(plaintext.sends_credentials_unsecured());

        let secured = HttpConfig::from_value(&json!({
            "listen": {
                "bind": "[::]:8443",
                "bearer_token": "s3cret",
                "tls": {"cert": "/c.pem", "key": "/k.pem"}
            }
        }))
        .expect("valid");
        assert!(!secured.sends_credentials_unsecured());

        let loopback = HttpConfig::from_value(&json!({
            "listen": {"bind": "127.0.0.1:8080", "bearer_token": "s3cret"}
        }))
        .expect("valid");
        assert!(!loopback.sends_credentials_unsecured());
        assert!(HttpConfig::from_value(&json!({"listen": {"bind": "[::1]:8080"}})).is_ok());
    }

    #[test]
    fn poll_mode_requires_a_poll_block_with_urls() {
        assert_eq!(fields(json!({"mode": "poll"})), vec!["poll".to_string()]);
        assert_eq!(
            fields(json!({"mode": "poll", "poll": {}})),
            vec!["urls".to_string()]
        );
        assert_eq!(
            fields(json!({"mode": "listen", "poll": {"urls": ["http://a"]}})),
            vec!["poll".to_string()]
        );
    }

    #[test]
    fn poll_urls_must_be_absolute_http_urls_and_are_not_echoed() {
        let found = codes_of(json!({
            "mode": "poll",
            "poll": {"urls": ["ftp://host/x", "http://", "https://h/x#frag", "http://h/?token=abc def"]}
        }));
        assert_eq!(found.len(), 4, "{found:?}");
        assert!(found.iter().all(|(_, m)| !m.contains("token=abc")));
        assert!(found.iter().all(|(_, m)| !m.contains("ftp://")));
    }

    #[test]
    fn body_needs_post_and_backoff_must_cover_interval() {
        assert_eq!(
            fields(json!({"mode": "poll", "poll": {"urls": ["http://a"], "body": "x"}})),
            vec!["body".to_string()]
        );
        assert!(
            HttpConfig::from_value(&json!({
                "mode": "poll",
                "poll": {"urls": ["http://a"], "method": "post", "body": "x"}
            }))
            .is_ok()
        );
        assert_eq!(
            fields(json!({
                "mode": "poll",
                "poll": {"urls": ["http://a"], "interval": "10s", "backoff_max": "1s"}
            })),
            vec!["backoff_max".to_string()]
        );
    }

    #[test]
    fn zero_durations_and_bad_headers_are_rejected() {
        let found = fields(json!({
            "mode": "poll",
            "poll": {
                "urls": ["http://a"],
                "interval": 0,
                "timeout": "0ms",
                "headers": {"bad name": "x", "X-Ok": "bad\nvalue"}
            }
        }));
        let mut found = found;
        found.sort();
        assert_eq!(found, vec!["headers", "headers", "interval", "timeout"]);
    }

    #[test]
    fn path_must_be_an_axum_safe_absolute_path() {
        assert_eq!(
            fields(json!({"listen": {"path": "ingest"}})),
            vec!["path".to_string()]
        );
        assert_eq!(
            fields(json!({"listen": {"path": "/a/{id}"}})),
            vec!["path".to_string()]
        );
        assert!(HttpConfig::from_value(&json!({"listen": {"path": "/"}})).is_ok());
    }

    #[test]
    fn size_and_capacity_bounds() {
        assert_eq!(
            fields(json!({"max_message_bytes": 0})),
            vec!["max_message_bytes".to_string()]
        );
        assert_eq!(
            fields(json!({"queue_capacity": 15})),
            vec!["queue_capacity".to_string()]
        );
    }

    #[test]
    fn invalid_bind_and_wrong_types_are_config_errors() {
        let found = codes_of(json!({"listen": {"bind": "127.0.0.1:99999"}}));
        assert_eq!(found[0].0, codes::CONFIG_INVALID);
        let found = codes_of(json!({"listen": {"tls": {"cert": "c"}}}));
        assert_eq!(found[0].0, codes::CONFIG_MISSING);
    }

    #[test]
    fn duration_spellings() {
        assert_eq!(DurationSpec::parse("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(DurationSpec::parse("10"), Ok(Duration::from_secs(10)));
        assert_eq!(DurationSpec::parse("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(DurationSpec::parse("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(DurationSpec::parse("1h"), Ok(Duration::from_secs(3600)));
        assert!(DurationSpec::parse("").is_err());
        assert!(DurationSpec::parse("5d").is_err());
        assert!(DurationSpec::parse("-1s").is_err());
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let cfg = HttpConfig::from_value(&json!({
            "listen": {"bind": "0.0.0.0:8080", "bearer_token": "hunter2"}
        }))
        .expect("valid");
        assert!(!format!("{cfg:?}").contains("hunter2"));
        let cfg = HttpConfig::from_value(&json!({
            "mode": "poll",
            "poll": {"urls": ["http://a"], "method": "post", "body": "secret-body",
                     "headers": {"Authorization": "Bearer hunter2"}}
        }))
        .expect("valid");
        let text = format!("{cfg:?}");
        assert!(!text.contains("hunter2"));
        assert!(!text.contains("secret-body"));
    }

    #[test]
    fn schema_declares_the_closed_object_shape() {
        let schema = config_schema();
        assert_eq!(schema["title"], "HTTP source");
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
            vec![
                "listen",
                "max_message_bytes",
                "mode",
                "poll",
                "queue_capacity"
            ]
        );
        for block in ["listen", "poll"] {
            assert_eq!(schema["properties"][block]["additionalProperties"], false);
        }
    }

    #[test]
    fn a_fully_specified_config_is_valid() {
        let value = json!({
            "mode": "listen",
            "listen": {
                "bind": "127.0.0.1:0",
                "path": "/ingest",
                "bearer_token": "t",
                "tls": {"cert": "c.pem", "key": "k.pem"},
                "extract_headers": ["X-Device-Id"]
            },
            "max_message_bytes": 4096,
            "queue_capacity": 64
        });
        assert!(HttpConfig::from_value(&value).is_ok());
        let value = json!({
            "mode": "poll",
            "poll": {
                "urls": ["https://example.test/a?x=1"],
                "method": "post",
                "body": "{}",
                "headers": {"Accept": "application/json"},
                "interval": "1s",
                "timeout": 5,
                "backoff_max": "2m"
            },
            "max_message_bytes": 4096,
            "queue_capacity": 64
        });
        assert!(HttpConfig::from_value(&value).is_ok());
    }
}
