//! Typed configuration for the `kind: iceberg` registrar service, parsed
//! from `serde_json::Value` (D22), with a JSON Schema for `thadam validate`.
//!
//! Mirrors the shipper's conventions (`thadam-object-store`): credentials
//! arrive already resolved by `thadam-config`'s `${env:}` / `${file:}`
//! interpolation and are wrapped in [`Credentials`] so `Debug` never prints
//! them (ADR-0020).

use std::fmt;
use std::path::Path;
use std::time::Duration;

use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// A parsed, validated registrar configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct IcebergConfig {
    /// The segment-sink id whose catalog this registrar consumes.
    pub from: String,
    /// The pipeline name, written into the table location; must match the
    /// shipper's `pipeline`.
    pub pipeline: String,
    /// The Iceberg REST catalog URI (`http://` or `https://`).
    pub catalog_uri: String,
    /// PEM file of the CA that signed the catalog's certificate; replaces
    /// the platform trust roots for the catalog connection (and nothing
    /// else: the S3 store keeps its own trust).
    pub catalog_ca_file: Option<String>,
    /// How the registrar authenticates to the catalog, on catalog requests
    /// and on the direct schema commits alike.
    pub catalog_auth: CatalogAuth,
    /// PEM file of the client certificate for mutual TLS to the catalog;
    /// requires `catalog_client_key`.
    pub catalog_client_cert: Option<String>,
    /// PEM file of the private key for `catalog_client_cert`.
    pub catalog_client_key: Option<String>,
    /// The `s3://bucket/prefix` URL, as parsed (same semantics as the
    /// shipper's `url`).
    pub url: String,
    /// The bucket from `url`.
    pub bucket: String,
    /// The key prefix from `url` (no leading or trailing `/`; may be empty).
    pub prefix: String,
    /// The Iceberg namespace levels (`a.b` becomes `["a", "b"]`).
    pub namespace: Vec<String>,
    /// Create the namespace and table when absent. Off by default: tables
    /// must exist and match.
    pub create_table: bool,
    /// S3 access for table metadata, manifests, and footer reads.
    pub store: StoreConfig,
    /// The unbounded retry schedule for catalog and store outages.
    pub retry: RetryConfig,
}

/// S3-compatible access for Iceberg file IO.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct StoreConfig {
    /// Custom endpoint for S3-compatible stores (`http://minio:9000`).
    pub endpoint: Option<String>,
    /// The signing region.
    pub region: String,
    /// Allow plain-HTTP endpoints.
    pub allow_http: bool,
    /// The static credentials (already interpolated by `thadam-config`).
    pub credentials: Credentials,
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

/// How the registrar authenticates to the REST catalog.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum CatalogAuth {
    /// No `Authorization` header.
    #[default]
    None,
    /// A static bearer token, sent on every catalog request.
    Token(Secret),
    /// OAuth2 client credentials, exchanged at the token endpoint.
    Credential(CatalogCredential),
}

/// OAuth2 client credentials (`grant_type=client_credentials`).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CatalogCredential {
    /// The OAuth2 client id (no `:`, which the catalog client splits on).
    pub client_id: String,
    /// The OAuth2 client secret.
    pub client_secret: Secret,
    /// The requested scope; the catalog's default (`catalog`) when absent.
    pub scope: Option<String>,
    /// The token endpoint: `oauth2_server_uri`, else
    /// `{catalog_uri}/v1/oauth/tokens`.
    pub token_url: String,
}

/// A secret string; `Debug` is redacted.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// The retry schedule: unbounded attempts, capped jittered backoff (mirrors
/// `thadam-object-store`'s `RetryConfig`).
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIcebergConfig {
    from: String,
    pipeline: String,
    catalog_uri: String,
    #[serde(default)]
    catalog_ca_file: Option<String>,
    #[serde(default)]
    catalog_token: Option<String>,
    #[serde(default)]
    catalog_credential: Option<RawCredential>,
    #[serde(default)]
    catalog_client_cert: Option<String>,
    #[serde(default)]
    catalog_client_key: Option<String>,
    url: String,
    namespace: String,
    #[serde(default)]
    create_table: bool,
    #[serde(default)]
    store: RawStore,
    #[serde(default)]
    retry: RawRetry,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCredential {
    client_id: String,
    client_secret: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    oauth2_server_uri: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStore {
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
}

fn default_region() -> String {
    "us-east-1".to_owned()
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

fn id_shape_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 100
        && !id.starts_with('.')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// `(bucket, prefix)` of an `s3://bucket/prefix` URL (mirrors
/// `thadam-object-store`'s parser).
pub(crate) fn parse_s3_url(url: &str) -> Result<(String, String), String> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| "url must start with `s3://`".to_owned())?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        return Err("url has no bucket".to_owned());
    }
    Ok((bucket.to_owned(), prefix.trim_matches('/').to_owned()))
}

fn catalog_auth_of(
    token: Option<String>,
    credential: Option<RawCredential>,
    catalog_root: &str,
    errors: &mut Vec<ThadamError>,
) -> CatalogAuth {
    match (token, credential) {
        (None, None) => CatalogAuth::None,
        (Some(_), Some(_)) => {
            errors.push(config_error(
                "catalog_credential",
                "`catalog_token` and `catalog_credential` are mutually exclusive; set one",
            ));
            CatalogAuth::None
        }
        (Some(token), None) if token.is_empty() => {
            errors.push(config_error(
                "catalog_token",
                "`catalog_token` must not be empty (use ${file:} or ${env:})",
            ));
            CatalogAuth::None
        }
        (Some(token), None) => CatalogAuth::Token(Secret::new(token)),
        (None, Some(raw)) => credential_of(raw, catalog_root, errors)
            .map_or(CatalogAuth::None, CatalogAuth::Credential),
    }
}

fn credential_of(
    raw: RawCredential,
    catalog_root: &str,
    errors: &mut Vec<ThadamError>,
) -> Option<CatalogCredential> {
    let before = errors.len();
    if raw.client_id.is_empty() || raw.client_id.contains(':') {
        errors.push(config_error(
            "catalog_credential.client_id",
            "`client_id` must be non-empty and must not contain ':'",
        ));
    }
    if raw.client_secret.is_empty() {
        errors.push(config_error(
            "catalog_credential.client_secret",
            "`client_secret` must not be empty (use ${file:} or ${env:})",
        ));
    }
    if raw.scope.as_deref().is_some_and(str::is_empty) {
        errors.push(config_error(
            "catalog_credential.scope",
            "`scope` must not be empty",
        ));
    }
    if let Some(uri) = &raw.oauth2_server_uri
        && !(uri.starts_with("http://") || uri.starts_with("https://"))
    {
        errors.push(config_error(
            "catalog_credential.oauth2_server_uri",
            "`oauth2_server_uri` must be an `http://` or `https://` URI",
        ));
    }
    if errors.len() > before {
        return None;
    }
    let token_url = raw
        .oauth2_server_uri
        .unwrap_or_else(|| format!("{catalog_root}/v1/oauth/tokens"));
    Some(CatalogCredential {
        client_id: raw.client_id,
        client_secret: Secret::new(raw.client_secret),
        scope: raw.scope,
        token_url,
    })
}

fn client_identity_errors(
    cert: Option<&str>,
    key: Option<&str>,
    catalog_uri: &str,
    errors: &mut Vec<ThadamError>,
) {
    match (cert, key) {
        (None, None) => {}
        (Some(cert), Some(key)) => {
            for (path, file) in [("catalog_client_cert", cert), ("catalog_client_key", key)] {
                if !Path::new(file).is_file() {
                    errors.push(config_error(
                        path,
                        format!("`{path}` `{file}` is not an existing PEM file"),
                    ));
                }
            }
            if catalog_uri.starts_with("http://") {
                errors.push(config_error(
                    "catalog_client_cert",
                    "`catalog_client_cert` needs an `https://` `catalog_uri`",
                ));
            }
        }
        (Some(_), None) => errors.push(config_error(
            "catalog_client_key",
            "`catalog_client_cert` and `catalog_client_key` must be set together",
        )),
        (None, Some(_)) => errors.push(config_error(
            "catalog_client_cert",
            "`catalog_client_cert` and `catalog_client_key` must be set together",
        )),
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

/// Parses and validates the `config:` block of a `kind: iceberg` sink
/// entry. Every problem found is reported, in configuration order.
pub fn parse_iceberg_config(value: &serde_json::Value) -> Result<IcebergConfig, Vec<ThadamError>> {
    let raw: RawIcebergConfig = match serde_json::from_value(value.clone()) {
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
    if !(raw.catalog_uri.starts_with("http://") || raw.catalog_uri.starts_with("https://")) {
        errors.push(config_error(
            "catalog_uri",
            "`catalog_uri` must be an `http://` or `https://` URI of an Iceberg REST catalog",
        ));
    }
    if raw.catalog_ca_file.as_deref().is_some_and(str::is_empty) {
        errors.push(config_error(
            "catalog_ca_file",
            "`catalog_ca_file` must be a PEM file path",
        ));
    } else if raw.catalog_ca_file.is_some() && raw.catalog_uri.starts_with("http://") {
        errors.push(config_error(
            "catalog_ca_file",
            "`catalog_ca_file` needs an `https://` `catalog_uri`",
        ));
    }
    let catalog_root = raw.catalog_uri.trim_end_matches('/');
    let catalog_auth = catalog_auth_of(
        raw.catalog_token,
        raw.catalog_credential,
        catalog_root,
        &mut errors,
    );
    client_identity_errors(
        raw.catalog_client_cert.as_deref(),
        raw.catalog_client_key.as_deref(),
        &raw.catalog_uri,
        &mut errors,
    );
    let (bucket, prefix) = match parse_s3_url(&raw.url) {
        Ok(parts) => parts,
        Err(message) => {
            errors.push(config_error("url", message));
            (String::new(), String::new())
        }
    };

    let mut namespace: Vec<String> = Vec::new();
    for level in raw.namespace.split('.') {
        if level.is_empty()
            || !level
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            errors.push(config_error(
                "namespace",
                format!("namespace level `{level}` must be 1+ chars of [A-Za-z0-9_-]; levels join with '.'"),
            ));
        } else {
            namespace.push(level.to_owned());
        }
    }

    if let Some(endpoint) = &raw.store.endpoint {
        let http = endpoint.starts_with("http://");
        if !(http || endpoint.starts_with("https://")) {
            errors.push(config_error(
                "store.endpoint",
                "`store.endpoint` must start with `http://` or `https://`",
            ));
        } else if http && !raw.store.allow_http {
            errors.push(config_error(
                "store.allow_http",
                "`store.endpoint` is plain HTTP, so `store.allow_http: true` is required",
            ));
        }
    }
    let credentials = match (
        raw.store.access_key_id.filter(|s| !s.is_empty()),
        raw.store.secret_access_key.filter(|s| !s.is_empty()),
    ) {
        (Some(id), Some(secret)) => Some(Credentials::new(id, secret)),
        (None, None) => {
            errors.push(config_error(
                "store.access_key_id",
                "`store.access_key_id` and `store.secret_access_key` are required (use ${env:} or ${file:})",
            ));
            None
        }
        _ => {
            errors.push(config_error(
                "store",
                "`store.access_key_id` and `store.secret_access_key` must be set together",
            ));
            None
        }
    };

    if raw.retry.initial.is_zero() {
        errors.push(config_error(
            "retry.initial",
            "`retry.initial` must be greater than zero",
        ));
    }
    if !raw.retry.initial.is_zero() && raw.retry.max < raw.retry.initial {
        errors.push(config_error(
            "retry.max",
            "`retry.max` must be at least `retry.initial`",
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(IcebergConfig {
        from: raw.from,
        pipeline: raw.pipeline,
        catalog_uri: catalog_root.to_owned(),
        catalog_ca_file: raw.catalog_ca_file,
        catalog_auth,
        catalog_client_cert: raw.catalog_client_cert,
        catalog_client_key: raw.catalog_client_key,
        url: raw.url,
        bucket,
        prefix,
        namespace,
        create_table: raw.create_table,
        store: StoreConfig {
            endpoint: raw.store.endpoint,
            region: raw.store.region,
            allow_http: raw.store.allow_http,
            credentials: credentials
                .unwrap_or_else(|| Credentials::new(String::new(), String::new())),
        },
        retry: RetryConfig {
            initial: raw.retry.initial,
            max: raw.retry.max,
            jitter: raw.retry.jitter,
        },
    })
}

/// The JSON Schema for the `config:` block of `kind: iceberg`.
pub fn iceberg_config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Iceberg registrar",
        "description": "Registers every uploaded segment of the named segment sink with an Apache Iceberg table (fast_append over the shipped Parquet files; no data rewrite). Configured as a sinks: entry; runs as a service. Status: experimental.",
        "type": "object",
        "additionalProperties": false,
        "required": ["from", "pipeline", "catalog_uri", "url", "namespace"],
        "properties": {
            "from": {"type": "string", "minLength": 1,
                "description": "The segment-sink id whose catalog this registrar reads (the same sink the object_store shipper ships)."},
            "pipeline": {"type": "string", "minLength": 1,
                "description": "Pipeline name; must match the shipper's `pipeline`. Written into the table location."},
            "catalog_uri": {"type": "string", "pattern": "^https?://",
                "description": "The Iceberg REST catalog URI."},
            "catalog_ca_file": {"type": "string", "minLength": 1,
                "description": "PEM file of the CA that signed the REST catalog's certificate; replaces the platform trust roots for the catalog connection. Needs an https:// catalog_uri."},
            "catalog_token": {"type": "string", "minLength": 1,
                "description": "Static bearer token sent on every catalog request. Supply through ${file:} or ${env:}; never printed. Exclusive with catalog_credential."},
            "catalog_credential": {
                "type": "object",
                "additionalProperties": false,
                "required": ["client_id", "client_secret"],
                "properties": {
                    "client_id": {"type": "string", "minLength": 1, "pattern": "^[^:]+$"},
                    "client_secret": {"type": "string", "minLength": 1,
                        "description": "Supply through ${env:} or ${file:}; never printed."},
                    "scope": {"type": "string", "minLength": 1,
                        "description": "Requested OAuth2 scope; the catalog's default (catalog) when absent."},
                    "oauth2_server_uri": {"type": "string", "pattern": "^https?://",
                        "description": "Token endpoint; defaults to {catalog_uri}/v1/oauth/tokens."}
                },
                "description": "OAuth2 client credentials (grant_type client_credentials), exchanged at the token endpoint. Exclusive with catalog_token."
            },
            "catalog_client_cert": {"type": "string", "minLength": 1,
                "description": "PEM client certificate for mutual TLS to the REST catalog. Needs catalog_client_key and an https:// catalog_uri."},
            "catalog_client_key": {"type": "string", "minLength": 1,
                "description": "PEM private key for catalog_client_cert."},
            "url": {"type": "string", "pattern": "^s3://",
                "description": "s3://bucket/prefix; the bucket the shipper writes data files into. Tables are created at {url}/thadam/v1/{pipeline}/{dataset}."},
            "namespace": {"type": "string", "minLength": 1,
                "description": "Iceberg namespace for the per-dataset tables; dotted for multiple levels (a.b)."},
            "create_table": {"type": "boolean", "default": false,
                "description": "Create the namespace and per-dataset tables when absent. Off by default: tables must exist and match the segment footers."},
            "store": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "endpoint": {"type": "string",
                        "description": "Custom endpoint for S3-compatible stores, e.g. http://seaweedfs:9000."},
                    "region": {"type": "string", "default": "us-east-1"},
                    "allow_http": {"type": "boolean", "default": false,
                        "description": "Permit a plain-HTTP endpoint (local demos only)."},
                    "access_key_id": {"type": "string",
                        "description": "Supply through ${env:} or ${file:}; never printed."},
                    "secret_access_key": {"type": "string",
                        "description": "Supply through ${env:} or ${file:}; never printed."}
                }
            },
            "retry": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "initial": {"type": ["integer", "string"], "default": "1s"},
                    "max": {"type": ["integer", "string"], "default": "60s"},
                    "jitter": {"type": "boolean", "default": true}
                },
                "description": "Unbounded retry schedule for catalog/store outages."
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> serde_json::Value {
        serde_json::json!({
            "from": "local",
            "pipeline": "edge-capture",
            "catalog_uri": "http://127.0.0.1:8181",
            "url": "s3://warehouse/lake",
            "namespace": "robotics",
            "store": {
                "endpoint": "http://127.0.0.1:9000",
                "allow_http": true,
                "access_key_id": "k",
                "secret_access_key": "s",
            },
        })
    }

    #[test]
    fn parses_a_full_config() {
        let cfg = parse_iceberg_config(&base()).expect("valid");
        assert_eq!(cfg.bucket, "warehouse");
        assert_eq!(cfg.prefix, "lake");
        assert_eq!(cfg.namespace, ["robotics"]);
        assert!(!cfg.create_table);
        assert_eq!(cfg.catalog_uri, "http://127.0.0.1:8181");
        assert_eq!(cfg.retry, RetryConfig::default());
    }

    #[test]
    fn namespace_splits_levels() {
        let mut v = base();
        v["namespace"] = serde_json::json!("fleet.core");
        let cfg = parse_iceberg_config(&v).expect("valid");
        assert_eq!(cfg.namespace, ["fleet", "core"]);
    }

    #[test]
    fn rejects_http_endpoint_without_allow_http() {
        let mut v = base();
        v["store"]["allow_http"] = serde_json::json!(false);
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert!(errors.iter().any(|e| {
            e.context
                .get("path")
                .is_some_and(|p| p == "store.allow_http")
        }));
    }

    #[test]
    fn reports_every_problem_together() {
        let mut v = base();
        v["catalog_uri"] = serde_json::json!("iceberg://rest");
        v["url"] = serde_json::json!("gs://bucket");
        v["namespace"] = serde_json::json!("bad space");
        v["store"] = serde_json::json!({});
        let errors = parse_iceberg_config(&v).unwrap_err();
        let paths: Vec<&str> = errors
            .iter()
            .filter_map(|e| e.context.get("path"))
            .collect();
        assert!(paths.contains(&"catalog_uri"), "{paths:?}");
        assert!(paths.contains(&"url"), "{paths:?}");
        assert!(paths.contains(&"namespace"), "{paths:?}");
        assert!(paths.contains(&"store.access_key_id"), "{paths:?}");
    }

    #[test]
    fn catalog_ca_file_needs_an_https_catalog() {
        let mut v = base();
        v["catalog_ca_file"] = serde_json::json!("/etc/thadam/ca.pem");
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert!(errors.iter().any(|e| {
            e.context
                .get("path")
                .is_some_and(|p| p == "catalog_ca_file")
        }));
        v["catalog_uri"] = serde_json::json!("https://catalog.example:8181/");
        let cfg = parse_iceberg_config(&v).expect("valid");
        assert_eq!(cfg.catalog_ca_file.as_deref(), Some("/etc/thadam/ca.pem"));
        assert_eq!(cfg.catalog_uri, "https://catalog.example:8181");
    }

    fn error_paths(errors: &[ThadamError]) -> Vec<&str> {
        errors
            .iter()
            .filter_map(|e| e.context.get("path"))
            .collect()
    }

    #[test]
    fn catalog_token_and_credential_are_exclusive() {
        let mut v = base();
        v["catalog_token"] = serde_json::json!("tok");
        v["catalog_credential"] = serde_json::json!({"client_id": "thadam", "client_secret": "s"});
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert_eq!(error_paths(&errors), ["catalog_credential"], "{errors:?}");
    }

    #[test]
    fn catalog_secrets_are_never_debug_printed() {
        let mut v = base();
        v["catalog_token"] = serde_json::json!("tok-very-secret");
        let cfg = parse_iceberg_config(&v).expect("valid");
        assert!(matches!(cfg.catalog_auth, CatalogAuth::Token(_)));
        assert!(!format!("{cfg:?}").contains("tok-very-secret"));

        v.as_object_mut().expect("object").remove("catalog_token");
        v["catalog_credential"] =
            serde_json::json!({"client_id": "thadam-catalog", "client_secret": "s3cret-value"});
        let cfg = parse_iceberg_config(&v).expect("valid");
        assert!(matches!(cfg.catalog_auth, CatalogAuth::Credential(_)));
        assert!(!format!("{cfg:?}").contains("s3cret-value"));
    }

    #[test]
    fn catalog_credential_defaults_the_token_endpoint_to_the_catalog() {
        let mut v = base();
        v["catalog_credential"] =
            serde_json::json!({"client_id": "thadam-catalog", "client_secret": "s"});
        let cfg = parse_iceberg_config(&v).expect("valid");
        let CatalogAuth::Credential(cred) = &cfg.catalog_auth else {
            panic!("credential auth: {:?}", cfg.catalog_auth);
        };
        assert_eq!(cred.token_url, "http://127.0.0.1:8181/v1/oauth/tokens");
        assert_eq!(cred.client_id, "thadam-catalog");
        assert_eq!(cred.scope, None);

        v["catalog_credential"]["oauth2_server_uri"] = serde_json::json!("https://auth.example/t");
        v["catalog_credential"]["scope"] = serde_json::json!("catalog:write");
        let cfg = parse_iceberg_config(&v).expect("valid");
        let CatalogAuth::Credential(cred) = &cfg.catalog_auth else {
            panic!("credential auth: {:?}", cfg.catalog_auth);
        };
        assert_eq!(cred.token_url, "https://auth.example/t");
        assert_eq!(cred.scope.as_deref(), Some("catalog:write"));
    }

    #[test]
    fn catalog_auth_rejects_bad_client_ids_and_empty_secrets() {
        let mut v = base();
        v["catalog_credential"] = serde_json::json!({
            "client_id": "a:b",
            "client_secret": "",
            "scope": "",
            "oauth2_server_uri": "ftp://auth.example/t",
        });
        let errors = parse_iceberg_config(&v).unwrap_err();
        let paths = error_paths(&errors);
        assert!(paths.contains(&"catalog_credential.client_id"), "{paths:?}");
        assert!(
            paths.contains(&"catalog_credential.client_secret"),
            "{paths:?}"
        );
        assert!(paths.contains(&"catalog_credential.scope"), "{paths:?}");
        assert!(
            paths.contains(&"catalog_credential.oauth2_server_uri"),
            "{paths:?}"
        );

        let mut v = base();
        v["catalog_token"] = serde_json::json!("");
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert_eq!(error_paths(&errors), ["catalog_token"], "{errors:?}");
    }

    #[test]
    fn client_cert_and_key_are_set_together_and_must_exist() {
        let dir = tempfile::tempdir().expect("dir");
        let cert = dir.path().join("client.pem");
        let key = dir.path().join("client.key");
        std::fs::write(&cert, "cert").expect("write");
        let mut v = base();
        v["catalog_uri"] = serde_json::json!("https://catalog.example:8181");

        v["catalog_client_cert"] = serde_json::json!(cert.to_str().expect("utf-8"));
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert_eq!(error_paths(&errors), ["catalog_client_key"], "{errors:?}");

        v.as_object_mut()
            .expect("object")
            .remove("catalog_client_cert");
        v["catalog_client_key"] = serde_json::json!(key.to_str().expect("utf-8"));
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert_eq!(error_paths(&errors), ["catalog_client_cert"], "{errors:?}");

        v["catalog_client_cert"] = serde_json::json!(cert.to_str().expect("utf-8"));
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert_eq!(
            error_paths(&errors),
            ["catalog_client_key"],
            "missing key file: {errors:?}"
        );

        std::fs::write(&key, "key").expect("write");
        let cfg = parse_iceberg_config(&v).expect("valid");
        assert_eq!(cfg.catalog_client_key.as_deref(), key.to_str());

        v["catalog_uri"] = serde_json::json!("http://127.0.0.1:8181");
        let errors = parse_iceberg_config(&v).unwrap_err();
        assert_eq!(error_paths(&errors), ["catalog_client_cert"], "{errors:?}");
    }

    #[test]
    fn rejects_unknown_keys() {
        let mut v = base();
        v["schema"] = serde_json::json!({});
        assert!(parse_iceberg_config(&v).is_err());
    }

    #[test]
    fn url_parses_bucket_and_prefix() {
        assert_eq!(
            parse_s3_url("s3://b/a/b/").expect("url"),
            ("b".to_owned(), "a/b".to_owned())
        );
        assert_eq!(
            parse_s3_url("s3://b").expect("url"),
            ("b".to_owned(), String::new())
        );
        assert!(parse_s3_url("s3:///x").is_err());
        assert!(parse_s3_url("http://b").is_err());
    }
}
