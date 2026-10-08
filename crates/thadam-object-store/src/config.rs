//! Typed configuration for the `kind: object_store` shipper service, parsed
//! from `serde_json::Value` (D22), with a JSON Schema for `thadam validate`.
//!
//! Credentials arrive already resolved by `thadam-config`'s `${env:}` /
//! `${file:}` interpolation; here they are wrapped in [`Credentials`] so
//! `Debug` never prints them (ADR-0020).

use std::fmt;
use std::time::Duration;

use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// Default uploads in flight (ARCHITECTURE.md section 8).
pub(crate) const DEFAULT_UPLOAD_CONCURRENCY: u64 = 2;
/// Files at or below this use one conditional put.
pub(crate) const DEFAULT_MULTIPART_THRESHOLD: u64 = 32 << 20;
/// Multipart part size; at most `upload_concurrency x part_size` is in memory.
pub(crate) const DEFAULT_PART_SIZE: u64 = 8 << 20;
/// S3 rejects smaller parts (except the last one).
const MIN_PART_SIZE: u64 = 5 << 20;
/// S3 rejects larger parts.
const MAX_PART_SIZE: u64 = 5 << 30;

/// A parsed, validated shipper configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ShipConfig {
    /// The segment-sink id whose catalog this shipper consumes.
    pub from: String,
    /// The pipeline name, written into every object key (ADR-0013).
    pub pipeline: String,
    /// The `s3://bucket/prefix` URL, as parsed. `bucket` and `prefix` come
    /// from it; the string is passed to the store builder unchanged.
    pub url: String,
    /// The key prefix from `url` (no leading or trailing `/`; may be empty).
    pub prefix: String,
    /// Custom endpoint for S3-compatible stores (`http://minio:9000`).
    pub endpoint: Option<String>,
    /// The signing region.
    pub region: String,
    /// Allow plain-HTTP endpoints.
    pub allow_http: bool,
    /// The static credentials (already interpolated by `thadam-config`).
    pub credentials: Credentials,
    /// Uploads in flight.
    pub upload_concurrency: usize,
    /// Files at or below this many bytes use a single conditional put.
    pub multipart_threshold: u64,
    /// Multipart part size in bytes.
    pub part_size: u64,
    /// How create-if-absent is achieved.
    pub conditional_put: ConditionalPut,
    /// The unbounded per-file retry schedule.
    pub retry: RetryConfig,
    /// When the local copy of an uploaded segment may be deleted.
    pub retention: RetentionConfig,
}

/// Static access credentials; `Debug` is redacted.
#[derive(Clone)]
pub struct Credentials {
    access_key_id: String,
    secret_access_key: String,
}

impl Credentials {
    /// Wraps a key pair.
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Credentials {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }

    pub(crate) fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub(crate) fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials(<redacted>)")
    }
}

/// How create-if-absent uploads are made.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConditionalPut {
    /// Probe at startup: conditional create where enforced, overwrite
    /// otherwise (safe only because finalized files are immutable and keys
    /// deterministic). The probe result is logged and reported.
    #[default]
    Auto,
    /// Require create-if-absent; the shipper refuses to start when the probe
    /// finds the store ignoring `If-None-Match`.
    Create,
    /// Always overwrite; no integrity verification on re-upload.
    Overwrite,
}

/// The outer per-file retry schedule: unbounded attempts, capped jittered
/// backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetryConfig {
    /// The first delay between attempts.
    pub initial: Duration,
    /// The largest delay between attempts.
    pub max: Duration,
    /// Draw each delay uniformly from `[delay/2, delay]`.
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            jitter: true,
        }
    }
}

/// When the local copy of an uploaded segment may be deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetentionConfig {
    /// The mode.
    pub mode: RetentionMode,
    /// For [`RetentionMode::KeepFor`]: how long after upload the file stays.
    /// Accepted and ignored for the other modes.
    pub keep_for: Option<Duration>,
}

/// The local-retention mode (ARCHITECTURE.md section 8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetentionMode {
    /// Delete once uploaded (and the WAL cursor has passed the file's end,
    /// which eligibility already implies).
    #[default]
    DeleteAfterUpload,
    /// Keep uploaded files for `keep_for` from their upload time.
    KeepFor,
    /// Keep uploaded files until the shared segment budget is under
    /// pressure; then the oldest uploads are evicted first.
    KeepUntilPressure,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawShipConfig {
    from: String,
    pipeline: String,
    url: String,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default = "default_region")]
    region: String,
    #[serde(default)]
    allow_http: bool,
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    #[serde(default = "default_upload_concurrency")]
    upload_concurrency: u64,
    #[serde(default = "default_threshold", deserialize_with = "de_size")]
    multipart_threshold: u64,
    #[serde(default = "default_part_size", deserialize_with = "de_size")]
    part_size: u64,
    #[serde(default)]
    conditional_put: RawConditionalPut,
    #[serde(default)]
    retry: RawRetry,
    #[serde(default)]
    local_retention: RawRetention,
}

fn default_region() -> String {
    "us-east-1".to_owned()
}

fn default_upload_concurrency() -> u64 {
    DEFAULT_UPLOAD_CONCURRENCY
}

fn default_threshold() -> u64 {
    DEFAULT_MULTIPART_THRESHOLD
}

fn default_part_size() -> u64 {
    DEFAULT_PART_SIZE
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawConditionalPut {
    #[default]
    Auto,
    Create,
    Overwrite,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRetry {
    #[serde(default = "retry_initial", deserialize_with = "de_duration")]
    initial: Duration,
    #[serde(default = "retry_max", deserialize_with = "de_duration")]
    max: Duration,
    #[serde(default = "default_jitter")]
    jitter: bool,
}

impl Default for RawRetry {
    fn default() -> Self {
        let d = RetryConfig::default();
        RawRetry {
            initial: d.initial,
            max: d.max,
            jitter: d.jitter,
        }
    }
}

fn retry_initial() -> Duration {
    RetryConfig::default().initial
}

fn retry_max() -> Duration {
    RetryConfig::default().max
}

fn default_jitter() -> bool {
    true
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRetention {
    #[serde(default)]
    mode: RawRetentionMode,
    #[serde(default, deserialize_with = "de_opt_duration")]
    keep_for: Option<Duration>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawRetentionMode {
    #[default]
    DeleteAfterUpload,
    KeepFor,
    KeepUntilPressure,
}

fn id_shape_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 100
        && !id.starts_with('.')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn parse_size(text: &str) -> Result<u64, String> {
    text.trim()
        .parse::<bytesize::ByteSize>()
        .map(|b| b.as_u64())
        .map_err(|_| format!("`{text}` is not a size like `32MiB` or `1GiB`"))
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
        "d" => Duration::from_secs(n.checked_mul(86_400).ok_or_else(overflow)?),
        other => return Err(format!("unknown duration unit `{other}` in `{text}`")),
    })
}

fn de_size<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = u64;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a byte count or a size string such as `32MiB`")
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<u64, E> {
            parse_size(v).map_err(E::custom)
        }
    }
    d.deserialize_any(V)
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

fn de_opt_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    struct OptV;
    impl<'de> serde::de::Visitor<'de> for OptV {
        type Value = Option<Duration>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("null, whole seconds, or a duration string such as `30s`")
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Option<Duration>, E> {
            Ok(None)
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Option<Duration>, E> {
            Ok(None)
        }
        fn visit_some<D2: serde::Deserializer<'de>>(
            self,
            d2: D2,
        ) -> Result<Option<Duration>, D2::Error> {
            de_duration(d2).map(Some)
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Option<Duration>, E> {
            Ok(Some(Duration::from_secs(v)))
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Option<Duration>, E> {
            let secs = u64::try_from(v).map_err(|_| E::custom("a duration cannot be negative"))?;
            Ok(Some(Duration::from_secs(secs)))
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Option<Duration>, E> {
            parse_duration(v).map(Some).map_err(E::custom)
        }
    }
    d.deserialize_any(OptV)
}

/// Parses and validates the `config:` block of a `kind: object_store`
/// sink/service entry. Every problem found is reported, in configuration
/// order.
pub fn parse_ship_config(value: &serde_json::Value) -> Result<ShipConfig, Vec<ThadamError>> {
    let raw: RawShipConfig = match serde_json::from_value(value.clone()) {
        Ok(raw) => raw,
        Err(e) => return Err(vec![config_error("config", e.to_string())]),
    };
    let mut errors = Vec::new();

    if !id_shape_ok(&raw.from) {
        errors.push(config_error(
            "from",
            "`from` must name a segment sink id: 1..100 chars of [A-Za-z0-9_.-], not starting with '.'",
        ));
    }
    if !id_shape_ok(&raw.pipeline) {
        errors.push(config_error(
            "pipeline",
            "`pipeline` must be 1..100 chars of [A-Za-z0-9_.-], not starting with '.'",
        ));
    }

    let (_, prefix) = match parse_s3_url(&raw.url) {
        Ok(parts) => parts,
        Err(message) => {
            errors.push(config_error("url", message));
            (String::new(), String::new())
        }
    };

    if let Some(endpoint) = &raw.endpoint {
        let secure = endpoint.starts_with("https://");
        let plain = endpoint.starts_with("http://");
        if !secure && !plain {
            errors.push(config_error(
                "endpoint",
                "`endpoint` must be an http:// or https:// URL",
            ));
        }
        if plain && !raw.allow_http {
            errors.push(config_error(
                "allow_http",
                "`endpoint` uses plain http; set `allow_http: true` (local demos only)",
            ));
        }
    }

    let credentials = match (&raw.access_key_id, &raw.secret_access_key) {
        (Some(a), Some(s)) => Some(Credentials::new(a.clone(), s.clone())),
        (None, None) => None,
        (Some(_), None) => {
            errors.push(config_error(
                "secret_access_key",
                "access_key_id requires secret_access_key",
            ));
            None
        }
        (None, Some(_)) => {
            errors.push(config_error(
                "access_key_id",
                "secret_access_key requires access_key_id",
            ));
            None
        }
    };
    if credentials.is_none()
        && !errors.iter().any(|e| {
            matches!(
                e.context.get("path"),
                Some("access_key_id" | "secret_access_key")
            )
        })
    {
        errors.push(config_error(
            "access_key_id",
            "static credentials are required in this build; supply them through ${env:} or ${file:}",
        ));
    }

    if raw.upload_concurrency == 0 {
        errors.push(config_error("upload_concurrency", "must be at least 1"));
    }
    if !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&raw.part_size) {
        errors.push(config_error(
            "part_size",
            format!(
                "must be between 5MiB and 5GiB (S3 part limits), got {}",
                raw.part_size
            ),
        ));
    }
    if raw.part_size > raw.multipart_threshold {
        errors.push(config_error(
            "multipart_threshold",
            "must be at least part_size",
        ));
    }

    if raw.retry.initial.is_zero() {
        errors.push(config_error("retry.initial", "must be at least 1ms"));
    }
    if raw.retry.max < raw.retry.initial {
        errors.push(config_error("retry.max", "must be at least retry.initial"));
    }

    if matches!(raw.local_retention.mode, RawRetentionMode::KeepFor) {
        match raw.local_retention.keep_for {
            None => errors.push(config_error(
                "local_retention.keep_for",
                "`mode: keep_for` needs a `keep_for` duration",
            )),
            Some(d) if d.is_zero() => errors.push(config_error(
                "local_retention.keep_for",
                "`keep_for` must be at least 1s",
            )),
            _ => {}
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    let Some(credentials) = credentials else {
        // Each missing-credential pair pushed an error above, so this is
        // unreachable on the success path; keep the function total.
        return Err(errors);
    };
    Ok(ShipConfig {
        from: raw.from,
        pipeline: raw.pipeline,
        url: raw.url,
        prefix,
        endpoint: raw.endpoint,
        region: raw.region,
        allow_http: raw.allow_http,
        credentials,
        upload_concurrency: raw.upload_concurrency as usize,
        multipart_threshold: raw.multipart_threshold,
        part_size: raw.part_size,
        conditional_put: match raw.conditional_put {
            RawConditionalPut::Auto => ConditionalPut::Auto,
            RawConditionalPut::Create => ConditionalPut::Create,
            RawConditionalPut::Overwrite => ConditionalPut::Overwrite,
        },
        retry: RetryConfig {
            initial: raw.retry.initial,
            max: raw.retry.max,
            jitter: raw.retry.jitter,
        },
        retention: RetentionConfig {
            mode: match raw.local_retention.mode {
                RawRetentionMode::DeleteAfterUpload => RetentionMode::DeleteAfterUpload,
                RawRetentionMode::KeepFor => RetentionMode::KeepFor,
                RawRetentionMode::KeepUntilPressure => RetentionMode::KeepUntilPressure,
            },
            keep_for: raw.local_retention.keep_for,
        },
    })
}

/// `s3://bucket/prefix` -> `(bucket, prefix)`; the prefix has no leading or
/// trailing slash (empty when absent).
fn parse_s3_url(url: &str) -> Result<(String, String), String> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| format!("`url` must be an `s3://bucket/prefix` URL, got `{url}`"))?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b.to_owned(), p.to_owned()),
        None => (rest.to_owned(), String::new()),
    };
    let prefix = prefix.trim_matches('/');
    let bucket_ok = (3..=63).contains(&bucket.len())
        && bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-'))
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
        && !bucket.contains("..");
    if !bucket_ok {
        return Err(format!(
            "bucket `{bucket}` must be 3..63 lowercase letters, digits, dots, or dashes, not starting or ending with a dash"
        ));
    }
    if !prefix.is_empty()
        && prefix
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == "..")
    {
        return Err("the url prefix must consist of non-empty path components".to_owned());
    }
    Ok((bucket, prefix.to_owned()))
}

/// The JSON Schema for a `kind: object_store` `config:` block.
pub fn ship_config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Object-store shipper",
        "description": "Ships finalized, cursor-committed segments from the named segment sink to S3-compatible storage (commit-then-ship, ADR-0011 D18). Configured as a sinks: entry; runs as a service.",
        "type": "object",
        "additionalProperties": false,
        "required": ["from", "pipeline", "url", "access_key_id", "secret_access_key"],
        "properties": {
            "from": {"type": "string", "minLength": 1,
                "description": "The segment-sink id whose catalog this shipper ships."},
            "pipeline": {"type": "string", "minLength": 1,
                "description": "Pipeline name, written into every object key: {prefix}/thadam/v1/{pipeline}/..."},
            "url": {"type": "string", "pattern": "^s3://",
                "description": "s3://bucket/prefix; keys land under {prefix}/thadam/v1/{pipeline}/."},
            "endpoint": {"type": "string",
                "description": "Custom endpoint for S3-compatible stores, e.g. http://minio:9000."},
            "region": {"type": "string", "default": "us-east-1"},
            "allow_http": {"type": "boolean", "default": false,
                "description": "Permit a plain-HTTP endpoint (local demos only)."},
            "access_key_id": {"type": "string",
                "description": "Supply through ${env:} or ${file:}; never printed."},
            "secret_access_key": {"type": "string",
                "description": "Supply through ${env:} or ${file:}; never printed."},
            "upload_concurrency": {"type": "integer", "minimum": 1, "default": 2},
            "multipart_threshold": {"type": ["integer", "string"], "default": "32MiB",
                "description": "Files at or below this size use one conditional put; larger files multipart."},
            "part_size": {"type": ["integer", "string"], "default": "8MiB",
                "description": "Multipart part size, 5MiB..5GiB. At most upload_concurrency x part_size is in memory."},
            "conditional_put": {"type": "string", "enum": ["auto", "create", "overwrite"], "default": "auto",
                "description": "auto probes at startup and falls back to overwrite (safe: immutable files, deterministic keys). create requires If-None-Match support."},
            "retry": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "initial": {"type": ["integer", "string"], "default": "1s", "description": "First retry delay."},
                    "max": {"type": ["integer", "string"], "default": "60s", "description": "Longest retry delay."},
                    "jitter": {"type": "boolean", "default": true}
                }
            },
            "local_retention": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "mode": {"type": "string", "enum": ["delete_after_upload", "keep_for", "keep_until_pressure"], "default": "delete_after_upload"},
                    "keep_for": {"type": ["integer", "string"], "description": "How long uploaded files stay; required with mode: keep_for."}
                }
            }
        }
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    fn demo_json() -> serde_json::Value {
        serde_json::json!({
            "from": "local",
            "pipeline": "edge-capture",
            "url": "s3://thadam-demo/fleet",
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
            "allow_http": true,
            "access_key_id": "AKIA",
            "secret_access_key": "SECRET",
            "upload_concurrency": 2,
            "multipart_threshold": "32MiB",
            "part_size": "8MiB",
            "conditional_put": "auto",
            "retry": { "initial": "1s", "max": "60s", "jitter": true },
            "local_retention": { "mode": "delete_after_upload", "keep_for": "30m" }
        })
    }

    #[test]
    fn parses_the_demo_shape_with_defaults() {
        let cfg = parse_ship_config(&demo_json()).expect("valid");
        assert_eq!(cfg.from, "local");
        assert_eq!(cfg.pipeline, "edge-capture");
        assert_eq!(cfg.prefix, "fleet");
        assert_eq!(cfg.multipart_threshold, 32 << 20);
        assert_eq!(cfg.part_size, 8 << 20);
        assert_eq!(cfg.upload_concurrency, 2);
        assert_eq!(cfg.retry.initial, Duration::from_secs(1));
        assert_eq!(cfg.retry.max, Duration::from_secs(60));
        assert!(cfg.retry.jitter);
        assert_eq!(cfg.retention.mode, RetentionMode::DeleteAfterUpload);
        assert_eq!(cfg.retention.keep_for, Some(Duration::from_secs(1800)));
        assert_eq!(cfg.conditional_put, ConditionalPut::Auto);
        assert_eq!(cfg.region, "us-east-1");
    }

    #[test]
    fn collects_multiple_errors() {
        let cfg = serde_json::json!({
            "from": "not a sink id!",
            "pipeline": "",
            "url": "http://bucket/prefix",
            "endpoint": "http://minio:9000",
            "multipart_threshold": 4 << 10,
            "part_size": 2 << 20,
            "upload_concurrency": 0,
            "retry": { "initial": "0s", "max": "60s" },
            "local_retention": { "mode": "keep_for" }
        });
        let errs = parse_ship_config(&cfg).expect_err("invalid");
        let text = errs
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join(" | ");
        for want in [
            "from",
            "pipeline",
            "s3://",
            "allow_http",
            "at least 1",
            "5MiB and 5GiB",
            "part_size",
            "at least 1ms",
            "keep_for",
        ] {
            assert!(text.contains(want), "missing `{want}` in: {text}");
        }
        assert!(
            text.contains("credentials are required"),
            "missing creds error in: {text}"
        );
    }

    #[test]
    fn credential_pairing_and_redaction() {
        let mut v = demo_json();
        v["secret_access_key"] = serde_json::Value::Null;
        let errs = parse_ship_config(&v).expect_err("missing secret");
        assert!(
            errs.iter()
                .any(|e| e.message.contains("access_key_id requires"))
        );

        let cfg = parse_ship_config(&demo_json()).expect("valid");
        let shown = format!("{:?}", cfg.credentials);
        assert!(!shown.contains("SECRET"), "redacted: {shown}");
        assert_str_pairing_kept(&cfg);
    }

    fn assert_str_pairing_kept(cfg: &ShipConfig) {
        assert_eq!(cfg.credentials.access_key_id(), "AKIA");
    }

    #[test]
    fn bucket_and_prefix_validation() {
        let mut v = demo_json();
        v["url"] = serde_json::json!("s3://Bad_Bucket/x");
        assert!(parse_ship_config(&v).is_err());
        v["url"] = serde_json::json!("s3://bucket/a//b");
        assert!(parse_ship_config(&v).is_err());
        v["url"] = serde_json::json!("s3://bucket/../x");
        assert!(parse_ship_config(&v).is_err());
        v["url"] = serde_json::json!("s3://bucket/deep/prefix/");
        let cfg = parse_ship_config(&v).expect("ok");
        assert_eq!(cfg.prefix, "deep/prefix");
        v["url"] = serde_json::json!("s3://bucket");
        let cfg = parse_ship_config(&v).expect("ok");
        assert_eq!(cfg.prefix, "");
    }

    #[test]
    fn unknown_keys_are_rejected_and_schema_is_anchored() {
        let mut v = demo_json();
        v["bogus"] = serde_json::json!(1);
        let errs = parse_ship_config(&v).expect_err("unknown key");
        assert!(errs.iter().any(|e| e.message.contains("bogus")));

        let schema = ship_config_schema();
        assert_eq!(
            schema["required"],
            serde_json::json!([
                "from",
                "pipeline",
                "url",
                "access_key_id",
                "secret_access_key"
            ])
        );
    }
}
