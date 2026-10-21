//! Typed configuration for the `kind: clickhouse` batch sink (ADR-0008),
//! parsed from `serde_json::Value` (D22), with a JSON Schema for
//! `thadam validate`.
//!
//! The password arrives already resolved by `thadam-config`'s `${env:}` /
//! `${file:}` interpolation; here it is wrapped in [`Secret`] so `Debug`
//! never prints it (ADR-0020).

use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use serde::Deserialize;
use thadam_core::{ErrorDomain, ThadamError, codes};

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// The default ClickHouse user.
pub(crate) const DEFAULT_USER: &str = "default";
/// The default ClickHouse database.
pub(crate) const DEFAULT_DATABASE: &str = "default";
/// The default per-statement timeout.
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// The largest accepted per-statement timeout (guards typos like `30d`).
pub(crate) const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(3600);

/// A parsed, validated ClickHouse sink configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ChConfig {
    /// The HTTP(S) endpoint, such as `https://ch.internal:8443`.
    pub url: String,
    /// The database holding the target table.
    pub database: String,
    /// The target table (a bare identifier; the database comes from
    /// [`ChConfig::database`]).
    pub table: String,
    /// The user the statements run as.
    pub user: String,
    /// The password, kept secret.
    pub password: Option<Secret>,
    /// The HTTP compression of insert payloads.
    pub compression: Compression,
    /// The per-statement timeout (probes and each insert).
    pub request_timeout: Duration,
    /// How `insert_deduplication_token` is sent and how the delivery class
    /// is decided.
    pub dedup: Dedup,
    /// The explicit dataset-column to table-column mapping. `None` means the
    /// default projection: every dataset field identity-mapped, envelope and
    /// level columns only where the table carries them.
    pub columns: Option<BTreeMap<String, String>>,
}

impl ChConfig {
    /// Whether the endpoint is plain `http://` (credentials travel in the
    /// clear; the sink warns when it carries a password).
    pub(crate) fn is_plain_http(&self) -> bool {
        self.url.starts_with("http://")
    }
}

/// Whether `url` is plain `http://` to a loopback host (`localhost`,
/// `127.0.0.0/8`, `::1`), where a password never leaves the machine.
pub(crate) fn is_loopback_http(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host_port)| host_port);
    let host = match host_port.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
        None => host_port
            .rsplit_once(':')
            .map_or(host_port, |(host, _)| host),
    };
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
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

/// HTTP payload compression for inserts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Compression {
    /// LZ4 (the ClickHouse default; has a small CPU cost, large size win).
    #[default]
    Lz4,
    /// No compression.
    None,
}

/// The dedup mode (ADR-0008).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Dedup {
    /// Probe for dedup support; send `insert_deduplication_token` when the
    /// table can deduplicate, report the delivery class the probe found.
    #[default]
    Auto,
    /// Always send the token; `open` refuses to start when the probe finds
    /// no dedup support, since the requested guarantee would be hollow.
    Token,
    /// Never send the token; the sink reports `AtLeastOnce` even where the
    /// table could deduplicate. A table that keeps a window still drops a
    /// byte-identical block by content hash (ClickHouse offers no insert
    /// setting to prevent that on non-replicated tables), but Thadam does not
    /// rely on it.
    Off,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    url: String,
    #[serde(default = "default_database")]
    database: String,
    table: String,
    #[serde(default = "default_user")]
    user: String,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    compression: RawCompression,
    #[serde(default = "default_request_timeout", deserialize_with = "de_duration")]
    request_timeout: Duration,
    #[serde(default)]
    dedup: RawDedup,
    #[serde(default)]
    columns: Option<BTreeMap<String, String>>,
    /// The runtime consumes this key (it restricts the chain and the
    /// datasets in the build context); it is parsed here so a malformed
    /// selector fails with a good message and so `deny_unknown_fields`
    /// accepts it.
    #[serde(default)]
    datasets: Option<Vec<serde_json::Value>>,
}

fn default_database() -> String {
    DEFAULT_DATABASE.to_owned()
}

fn default_user() -> String {
    DEFAULT_USER.to_owned()
}

fn default_request_timeout() -> Duration {
    DEFAULT_REQUEST_TIMEOUT
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawCompression {
    #[default]
    Lz4,
    None,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawDedup {
    #[default]
    Auto,
    Token,
    Off,
}

/// A ClickHouse identifier (database, table, column): verified by rejecting
/// the quotes the identifier gets wrapped in. ClickHouse allows almost any
/// name under backticks; keeping out backticks and backslashes makes the
/// escaping trivially safe.
fn ident_ok(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name
            .bytes()
            .any(|b| matches!(b, b'`' | b'\\' | b'\'' | b'"'))
}

fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(split);
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("`{text}` is not a duration like `30s` or `500ms`"))?;
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
            f.write_str("whole seconds or a duration string such as `30s`")
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

/// Parses and validates the `config:` block of a `kind: clickhouse` sink.
/// Every problem found is reported, in configuration order.
pub fn parse_config(value: &serde_json::Value) -> Result<ChConfig, Vec<ThadamError>> {
    let raw: RawConfig = match serde_json::from_value(value.clone()) {
        Ok(raw) => raw,
        Err(e) => return Err(vec![config_error("config", e.to_string())]),
    };
    let mut errors = Vec::new();

    if !raw.url.starts_with("https://") && !raw.url.starts_with("http://") {
        errors.push(config_error(
            "url",
            "`url` must be an http:// or https:// ClickHouse endpoint",
        ));
    }
    if !ident_ok(&raw.database) {
        errors.push(config_error(
            "database",
            format!(
                "`{0}` is not a usable ClickHouse database name",
                raw.database
            ),
        ));
    }
    if !ident_ok(&raw.table) {
        errors.push(config_error(
            "table",
            format!("`{0}` is not a usable ClickHouse table name", raw.table),
        ));
    }
    if raw.user.is_empty() {
        errors.push(config_error("user", "must not be empty"));
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

    if let Some(columns) = &raw.columns {
        for (dataset_col, table_col) in columns {
            if dataset_col.is_empty() {
                errors.push(config_error(
                    "columns",
                    "a mapping key (dataset column) must not be empty",
                ));
            }
            if !ident_ok(table_col) {
                errors.push(config_error(
                    "columns",
                    format!("`{table_col}` is not a usable ClickHouse column name"),
                ));
            }
        }
    }

    // The runtime validates dataset ids against the pipeline; here only the
    // selector shape and the single-dataset limit are checked.
    if let Some(datasets) = &raw.datasets {
        for item in datasets {
            if !item.is_string() {
                errors.push(config_error(
                    "datasets",
                    "every `datasets` entry must be a dataset id string",
                ));
            }
        }
        if datasets.len() != 1 {
            errors.push(config_error(
                "datasets",
                "a `kind: clickhouse` sink consumes exactly one dataset (one table per sink); add one sink entry per dataset",
            ));
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(ChConfig {
        url: raw.url.trim_end_matches('/').to_owned(),
        database: raw.database,
        table: raw.table,
        user: raw.user,
        password: raw.password.map(Secret::new),
        compression: match raw.compression {
            RawCompression::Lz4 => Compression::Lz4,
            RawCompression::None => Compression::None,
        },
        request_timeout: raw.request_timeout,
        dedup: match raw.dedup {
            RawDedup::Auto => Dedup::Auto,
            RawDedup::Token => Dedup::Token,
            RawDedup::Off => Dedup::Off,
        },
        columns: raw.columns,
    })
}

/// The JSON Schema for a `kind: clickhouse` `config:` block.
pub fn config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ClickHouse batch sink",
        "description": "Inserts one dataset's events into an existing ClickHouse table as Native blocks (ADR-0008). The sink never runs DDL: the table is owned by the operator and must already exist with compatible columns.",
        "type": "object",
        "additionalProperties": false,
        "required": ["url", "table"],
        "properties": {
            "url": {"type": "string",
                "description": "http:// or https:// ClickHouse endpoint (the HTTP interface, 8123/8443 by default)."},
            "database": {"type": "string", "default": "default",
                "description": "Database holding the target table."},
            "table": {"type": "string", "minLength": 1,
                "description": "Target table; must exist. One table per sink."},
            "user": {"type": "string", "default": "default"},
            "password": {"type": "string",
                "description": "Supply through ${env:} or ${file:}; never printed."},
            "compression": {"type": "string", "enum": ["lz4", "none"], "default": "lz4",
                "description": "HTTP compression. Probe responses are compressed with it; the vendor client currently sends Native insert bodies uncompressed whatever this is set to (see the insert-path notes in the sink docs)."},
            "request_timeout": {"type": ["integer", "string"], "default": "30s",
                "description": "Per-statement timeout for probes and inserts, e.g. `500ms`, `30s`."},
            "dedup": {"type": "string", "enum": ["auto", "token", "off"], "default": "auto",
                "description": "auto: send insert_deduplication_token when the probe finds dedup support and report the probed delivery class. token: require support (open fails otherwise). off: never send; always at-least-once (a table with a window can still drop byte-identical retries by content hash)."},
            "columns": {"type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Explicit dataset-column to table-column mapping. When present it is the complete projection: every dataset field must appear. When absent, dataset fields map by name and envelope/level columns insert only where the table carries them."},
            "datasets": {"type": "array", "items": {"type": "string"}, "maxItems": 1,
                "description": "Selector consumed by the runtime; a clickhouse sink consumes exactly one dataset."}
        }
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn demo_json() -> serde_json::Value {
        serde_json::json!({
            "url": "https://clickhouse.internal:8443",
            "database": "telemetry",
            "table": "imu",
            "user": "writer",
            "password": "s3cret",
            "compression": "lz4",
            "request_timeout": "15s",
            "dedup": "auto",
            "datasets": ["imu"]
        })
    }

    #[test]
    fn parses_a_full_config() {
        let cfg = parse_config(&demo_json()).expect("valid");
        assert_eq!(cfg.url, "https://clickhouse.internal:8443");
        assert!(!cfg.is_plain_http());
        assert_eq!(cfg.database, "telemetry");
        assert_eq!(cfg.table, "imu");
        assert_eq!(cfg.user, "writer");
        assert_eq!(cfg.password.as_ref().map(Secret::reveal), Some("s3cret"));
        assert_eq!(cfg.compression, Compression::Lz4);
        assert_eq!(cfg.request_timeout, Duration::from_secs(15));
        assert_eq!(cfg.dedup, Dedup::Auto);
        assert!(cfg.columns.is_none());
    }

    #[test]
    fn defaults_fill_in() {
        let cfg = parse_config(&serde_json::json!({
            "url": "https://h:8443",
            "table": "t"
        }))
        .expect("valid");
        assert_eq!(cfg.database, "default");
        assert_eq!(cfg.user, "default");
        assert!(cfg.password.is_none());
        assert_eq!(cfg.compression, Compression::Lz4);
        assert_eq!(cfg.request_timeout, Duration::from_secs(30));
        assert_eq!(cfg.dedup, Dedup::Auto);
    }

    #[test]
    fn plain_http_is_accepted_and_flagged() {
        let mut v = demo_json();
        v["url"] = serde_json::json!("http://localhost:8123/");
        let cfg = parse_config(&v).expect("plain http is allowed");
        assert!(cfg.is_plain_http());
        assert_eq!(cfg.url, "http://localhost:8123", "trailing slash trimmed");
    }

    #[test]
    fn loopback_http_is_recognised_by_host_only() {
        for url in [
            "http://localhost:8123",
            "http://LOCALHOST/",
            "http://127.0.0.1:8123",
            "http://127.8.9.1",
            "http://[::1]:8123",
            "http://user:pw@localhost:8123/?x=1",
        ] {
            assert!(is_loopback_http(url), "{url}");
        }
        for url in [
            "http://ch.internal:8123",
            "http://10.0.0.5:8123",
            "http://0.0.0.0:8123",
            "http://localhost.evil.test:8123",
            "http://127.0.0.1.nip.io",
            "http://localhost@evil.test:8123",
            "http://evil.test@localhost.evil:8123",
            "https://localhost:8443",
            "localhost:8123",
        ] {
            assert!(!is_loopback_http(url), "{url}");
        }
    }

    #[test]
    fn compression_and_dedup_modes_parse() {
        let mut v = demo_json();
        v["compression"] = serde_json::json!("none");
        v["dedup"] = serde_json::json!("token");
        let cfg = parse_config(&v).expect("valid");
        assert_eq!(cfg.compression, Compression::None);
        assert_eq!(cfg.dedup, Dedup::Token);
        v["dedup"] = serde_json::json!("off");
        assert_eq!(parse_config(&v).expect("valid").dedup, Dedup::Off);

        v["compression"] = serde_json::json!("zstd");
        let errs = parse_config(&v).expect_err("zstd is not offered");
        assert!(errs[0].message.contains("zstd"), "{errs:?}");
        v["compression"] = serde_json::json!("lz4");
        v["dedup"] = serde_json::json!("sometimes");
        assert!(parse_config(&v).is_err());
    }

    #[test]
    fn columns_mapping_is_kept_in_order() {
        let mut v = demo_json();
        v["columns"] = serde_json::json!({ "temp": "temperature", "robot": "robot_id" });
        let cfg = parse_config(&v).expect("valid");
        let map = cfg.columns.expect("mapping");
        assert_eq!(map.get("temp").map(String::as_str), Some("temperature"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn a_non_string_password_is_rejected() {
        let mut v = demo_json();
        v["password"] = serde_json::json!(12345);
        assert!(parse_config(&v).is_err());
    }

    #[test]
    fn collects_multiple_errors() {
        let v = serde_json::json!({
            "url": "tcp://h:9000",
            "database": "bad`db",
            "table": "",
            "request_timeout": "0s",
            "columns": { "": "ok", "x": "bad`col" }
        });
        let errs = parse_config(&v).expect_err("invalid");
        let text = errs
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join(" | ");
        for want in [
            "http:// or https://",
            "database name",
            "table name",
            "at least 1ms",
            "dataset column",
            "column name",
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
                .any(|e| e.message.contains("exactly one dataset")),
            "{errs:?}"
        );
        v["datasets"] = serde_json::json!([]);
        assert!(parse_config(&v).is_err(), "empty selector");
        v["datasets"] = serde_json::json!(["only"]);
        assert!(parse_config(&v).is_ok());
        // The key may be absent; the runtime then passes every pipeline
        // dataset and `build` enforces the limit against ctx.datasets.
        v["datasets"] = serde_json::Value::Null;
        assert!(parse_config(&v).is_ok());
    }

    #[test]
    fn password_is_redacted_and_unknown_keys_rejected() {
        let cfg = parse_config(&demo_json()).expect("valid");
        let shown = format!("{:?}", cfg.password);
        assert!(!shown.contains("s3cret"), "redacted: {shown}");

        let mut v = demo_json();
        v["bogus"] = serde_json::json!(1);
        let errs = parse_config(&v).expect_err("unknown key");
        assert!(errs.iter().any(|e| e.message.contains("bogus")));

        let schema = config_schema();
        assert_eq!(schema["required"], serde_json::json!(["url", "table"]));
        assert!(schema["properties"]["password"].is_object());
    }

    #[test]
    fn timeout_bounds_and_forms() {
        let mut v = demo_json();
        v["request_timeout"] = serde_json::json!(45);
        assert_eq!(
            parse_config(&v).expect("seconds as int").request_timeout,
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
