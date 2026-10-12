//! Typed, validated configuration for the OPC UA source.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use opcua::types::NodeId;
use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;
use serde::de::{self, Deserializer};

use crate::pki;

/// The reserved JSON key that carries the timestamp in every payload.
pub const TIMESTAMP_KEY: &str = "ts_us";

/// A duration written as whole seconds (`10`) or with a unit: `500ms`,
/// `10s`, `5m`, `1h`.
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

/// The OPC UA security policy for the secure channel. There is no default:
/// picking `none` must be an explicit decision, like `on_full` in the WAL
/// config.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityPolicySpec {
    /// No channel security. Only for trusted networks.
    None,
    /// Basic256Sha256 (AES-128 CBC, RSA-OAEP, SHA-256). The `aes*` policies
    /// are preferred.
    #[serde(rename = "basic256sha256")]
    Basic256Sha256,
    /// AES-128 with SHA-256 and RSA-OAEP. One of the two preferred policies.
    #[serde(rename = "aes128_sha256_rsaoaep")]
    Aes128Sha256RsaOaep,
    /// AES-256 with SHA-256 and RSA-PSS. The most preferred policy.
    #[serde(rename = "aes256_sha256_rsapss")]
    Aes256Sha256RsaPss,
}

impl SecurityPolicySpec {
    /// Whether the policy encrypts the channel.
    pub fn is_secured(self) -> bool {
        !matches!(self, SecurityPolicySpec::None)
    }

    /// The OPC UA security policy URI.
    pub fn uri(self) -> &'static str {
        match self {
            SecurityPolicySpec::None => "http://opcfoundation.org/UA/SecurityPolicy#None",
            SecurityPolicySpec::Basic256Sha256 => {
                "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256"
            }
            SecurityPolicySpec::Aes128Sha256RsaOaep => {
                "http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep"
            }
            SecurityPolicySpec::Aes256Sha256RsaPss => {
                "http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss"
            }
        }
    }
}

/// The OPC UA message security mode. No default, for the same reason as
/// `security_policy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityModeSpec {
    /// Messages are neither signed nor encrypted.
    None,
    /// Messages are signed but not encrypted.
    Sign,
    /// Messages are signed and encrypted.
    SignAndEncrypt,
}

/// How the client authenticates to the server.
#[derive(Clone, PartialEq, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthConfig {
    /// Anonymous identity token (the default).
    #[default]
    Anonymous,
    /// Username and password. The password is a string, so it may carry
    /// `${env:}`/`${file:}` placeholders resolved by the config loader.
    Username {
        /// The username.
        username: String,
        /// The password. Never logged: `Debug` prints `<redacted>`.
        password: String,
    },
    /// X.509 user token: the server matches the certificate, and the client
    /// proves possession of `key`. Requires a secured `security_policy`.
    /// Relative paths resolve against `cert.pki_dir`.
    Certificate {
        /// The user certificate (DER or PEM).
        cert: PathBuf,
        /// The user certificate's private key (PEM). A file path only:
        /// key material is never accepted inline.
        key: PathBuf,
    },
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthConfig::Anonymous => f.write_str("Anonymous"),
            AuthConfig::Username { username, .. } => f
                .debug_struct("Username")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            AuthConfig::Certificate { cert, key } => f
                .debug_struct("Certificate")
                .field("cert", cert)
                .field("key", key)
                .finish(),
        }
    }
}

/// The client application's certificate material, needed when
/// `security_policy` is not `none`: the secure channel handshake signs and
/// encrypts with it regardless of how the session authenticates.
///
/// Server certificates are rejected unless they are listed in
/// `trust_server_certs` (pinned, recommended) or `accept_unknown_server_cert`
/// is set (INSECURE). A rejected certificate is written to
/// `<pki_dir>/rejected` for review.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertConfig {
    /// The client's own certificate (DER or PEM), relative to `pki_dir` or
    /// absolute.
    pub cert: PathBuf,
    /// The client's private key (PEM, PKCS#8 or RSA), relative to `pki_dir`
    /// or absolute.
    pub key: PathBuf,
    /// The PKI directory. Trusted server certificates live under
    /// `<pki_dir>/trusted`, refused ones under `<pki_dir>/rejected`.
    pub pki_dir: PathBuf,
    /// Server certificate files to trust, DER or PEM, relative to `pki_dir`
    /// or absolute. Each is copied into `<pki_dir>/trusted` before connecting.
    #[serde(default)]
    pub trust_server_certs: Vec<PathBuf>,
    /// Trust any server certificate the first time it is seen, storing it in
    /// `<pki_dir>/trusted`. INSECURE: any server that presents a certificate
    /// is accepted, including an impostor. Hostname and validity-period
    /// checks still run. Default `false`.
    #[serde(default)]
    pub accept_unknown_server_cert: bool,
}

impl CertConfig {
    /// A path from the configuration, anchored at `pki_dir` when relative.
    pub fn resolve(&self, path: &Path) -> PathBuf {
        self.pki_dir.join(path)
    }
}

/// One named node to monitor.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// The OPC UA node id, for example `ns=2;s=Temp`, `ns=2;i=1003`, `i=85`,
    /// `ns=2;g=<guid>`, or `ns=2;b=<base64>`.
    #[serde(deserialize_with = "node_id_from_str")]
    pub node_id: NodeId,
    /// The JSON key this node's value is stored under.
    pub name: String,
}

fn node_id_from_str<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NodeId, D::Error> {
    let text = String::deserialize(deserializer)?;
    NodeId::from_str(&text).map_err(|_| {
        de::Error::custom(format!(
            "`{text}` is not a valid OPC UA node id (examples: `ns=2;s=Temp`, `ns=2;i=1003`, `i=85`)"
        ))
    })
}

/// Subscription timing for `mode: subscribe`.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SubscribeConfig {
    /// How often the server publishes batches of data changes.
    pub publishing_interval: DurationSpec,
    /// How often the server samples each node. `0` means "as fast as the
    /// server supports".
    pub sampling_interval: DurationSpec,
    /// Per-node server-side queue per monitored item.
    pub queue_size: u32,
}

impl Default for SubscribeConfig {
    fn default() -> Self {
        SubscribeConfig {
            publishing_interval: DurationSpec(Duration::from_secs(1)),
            sampling_interval: DurationSpec(Duration::from_millis(250)),
            queue_size: 10,
        }
    }
}

/// Reconnect behavior after the connection drops.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReconnectConfig {
    /// The first retry delay; doubles (jittered) with failed handshakes,
    /// up to `max_backoff`.
    pub min_backoff: DurationSpec,
    /// The longest retry delay.
    pub max_backoff: DurationSpec,
    /// The most connection handshakes per minute: a hard rate limit, evenly
    /// spaced with no bursts. This bounds the RSA timing samples an observer
    /// can collect (RUSTSEC-2023-0071; see the crate docs).
    pub max_handshakes_per_minute: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            min_backoff: DurationSpec(Duration::from_secs(1)),
            max_backoff: DurationSpec(Duration::from_secs(30)),
            max_handshakes_per_minute: 12,
        }
    }
}

/// How values reach the pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModeSpec {
    /// Monitored items on one subscription; one message per publish response.
    #[default]
    Subscribe,
    /// One Read service call over all nodes every `interval`; one message
    /// per poll.
    Poll,
}

/// The whole `config:` block of an OPC UA source.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpcUaConfig {
    /// The server's endpoint URL, `opc.tcp://host:port`.
    pub endpoint: String,
    /// The channel security policy. Required, no default.
    pub security_policy: SecurityPolicySpec,
    /// The message security mode. Required, no default.
    pub security_mode: SecurityModeSpec,
    /// How the client authenticates. Anonymous by default.
    #[serde(default)]
    pub auth: AuthConfig,
    /// The client's certificate material. Required when `security_policy`
    /// is not `none`.
    pub cert: Option<CertConfig>,
    /// `subscribe` (default) or `poll`.
    #[serde(default)]
    pub mode: ModeSpec,
    /// `poll`: how often to read every node. Required with `mode: poll`,
    /// rejected otherwise.
    pub interval: Option<DurationSpec>,
    /// `subscribe`: subscription timing. Rejected with `mode: poll`.
    pub subscribe: Option<SubscribeConfig>,
    /// Reconnect behavior.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
    /// The nodes to monitor, in order.
    pub nodes: Vec<NodeSpec>,
}

impl OpcUaConfig {
    /// The subscription timing in effect: the configured block, or the
    /// documented defaults.
    pub fn subscribe_settings(&self) -> SubscribeConfig {
        self.subscribe.unwrap_or_default()
    }

    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<OpcUaConfig, Vec<ThadamError>> {
        let config: OpcUaConfig = serde_json::from_value(value.clone()).map_err(|e| {
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

        match url::Url::parse(&self.endpoint) {
            Ok(url) => {
                if url.scheme() != "opc.tcp" {
                    found.push((
                        "endpoint",
                        format!(
                            "endpoint scheme `{}` is not supported: use opc.tcp://host:port",
                            url.scheme()
                        ),
                    ));
                } else if url.host_str().is_none_or(str::is_empty) || url.port().is_none() {
                    found.push((
                        "endpoint",
                        "endpoint must include a host and an explicit port, for example opc.tcp://192.0.2.10:4840".into(),
                    ));
                }
            }
            Err(e) => {
                found.push((
                    "endpoint",
                    format!("endpoint is not a URL ({e}); use opc.tcp://host:port"),
                ));
            }
        }

        let secured = self.security_policy.is_secured();
        if secured != (self.security_mode != SecurityModeSpec::None) {
            found.push(("security_mode", match self.security_policy {
                SecurityPolicySpec::None => "with security_policy: none, security_mode must be none (there is no channel crypto to sign with)".into(),
                _ => "with a secured security_policy, security_mode must be sign or sign_and_encrypt (never none)".into(),
            }));
        }
        match (secured, &self.cert) {
            (true, None) => found.push((
                "cert",
                "a secured security_policy needs cert: the client's certificate, key, and pki_dir"
                    .into(),
            )),
            (false, Some(_)) => found.push((
                "cert",
                "cert is only meaningful with a secured security_policy".into(),
            )),
            _ => {}
        }

        match &self.auth {
            AuthConfig::Anonymous => {}
            AuthConfig::Username { username, password } => {
                if username.is_empty() {
                    found.push(("auth", "auth.username must not be empty".into()));
                }
                if password.is_empty() {
                    found.push(("auth", "auth.password must not be empty".into()));
                }
            }
            AuthConfig::Certificate { .. } => {
                if !secured {
                    found.push((
                        "auth",
                        "auth kind certificate needs a secured security_policy: the user token is signed on the secure channel".into(),
                    ));
                }
            }
        }
        found.extend(credential_problems(self));

        match self.mode {
            ModeSpec::Subscribe => {
                if self.interval.is_some() {
                    found.push(("interval", "interval applies only to mode: poll".into()));
                }
            }
            ModeSpec::Poll => {
                match self.interval {
                    None => found.push((
                        "interval",
                        "mode: poll needs interval, for example 1s".into(),
                    )),
                    Some(interval) if interval.0.is_zero() => {
                        found.push(("interval", "interval must be greater than zero".into()))
                    }
                    Some(_) => {}
                }
                if self.subscribe.is_some() {
                    found.push((
                        "subscribe",
                        "subscribe settings apply only to mode: subscribe".into(),
                    ));
                }
            }
        }

        let subscribe = self.subscribe_settings();
        if subscribe.publishing_interval.0.is_zero() {
            found.push((
                "subscribe",
                "subscribe.publishing_interval must be greater than zero".into(),
            ));
        }
        if subscribe.queue_size == 0 {
            found.push((
                "subscribe",
                "subscribe.queue_size must be at least 1".into(),
            ));
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
        if self.reconnect.max_handshakes_per_minute == 0 {
            found.push((
                "reconnect",
                "reconnect.max_handshakes_per_minute must be at least 1".into(),
            ));
        }

        found.extend(nodes_problems(&self.nodes));

        found
            .into_iter()
            .map(|(field, message)| {
                ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
                    .with_context("field", field)
            })
            .collect()
    }
}

/// Certificate and key files, checked at config time so `thadam validate`
/// reports them before anything connects: each file exists and parses, each
/// key matches its certificate, and the trust options agree with each other.
fn credential_problems(config: &OpcUaConfig) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();
    if let Some(cert) = &config.cert {
        if let Err(e) = pki::check_key_pair(&cert.resolve(&cert.cert), &cert.resolve(&cert.key)) {
            found.push(("cert", e));
        }
        if cert.accept_unknown_server_cert && !cert.trust_server_certs.is_empty() {
            found.push((
                "cert",
                "set either trust_server_certs or accept_unknown_server_cert, not both".into(),
            ));
        }
        for path in &cert.trust_server_certs {
            if let Err(e) = pki::read_certificate(&cert.resolve(path)) {
                found.push(("cert", format!("trust_server_certs: {e}")));
            }
        }
        if cert.pki_dir.exists() && !cert.pki_dir.is_dir() {
            found.push((
                "cert",
                format!("pki_dir {} is not a directory", cert.pki_dir.display()),
            ));
        }
    }
    if let (AuthConfig::Certificate { cert, key }, Some(pki_cfg)) = (&config.auth, &config.cert)
        && let Err(e) = pki::check_key_pair(&pki_cfg.resolve(cert), &pki_cfg.resolve(key))
    {
        found.push(("auth", e));
    }
    found
}

fn nodes_problems(nodes: &[NodeSpec]) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();
    if nodes.is_empty() {
        found.push(("nodes", "at least one node is required".into()));
    }

    let mut names: Vec<&str> = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        if names.contains(&node.name.as_str()) {
            found.push((
                "nodes",
                format!("nodes[{i}]: name `{}` is used more than once", node.name),
            ));
        }
        names.push(&node.name);

        if node.name.is_empty() {
            found.push(("nodes", format!("nodes[{i}]: name must not be empty")));
        }
        if node.name == TIMESTAMP_KEY {
            found.push((
                "nodes",
                format!("nodes[{i}]: `{TIMESTAMP_KEY}` is reserved for the publish time"),
            ));
        }
    }
    found
}

/// The JSON Schema for the `config:` block of an OPC UA source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "OPC UA source",
        "type": "object",
        "additionalProperties": false,
        "required": ["endpoint", "security_policy", "security_mode", "nodes"],
        "properties": {
            "endpoint": {
                "type": "string",
                "description": "The server's endpoint URL, with an explicit port.",
                "pattern": "^opc\\.tcp://[^/]+$"
            },
            "security_policy": {
                "type": "string",
                "enum": ["none", "basic256sha256", "aes128_sha256_rsaoaep", "aes256_sha256_rsapss"]
            },
            "security_mode": {
                "type": "string",
                "enum": ["none", "sign", "sign_and_encrypt"]
            },
            "auth": {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind"],
                "properties": {
                    "kind": {"type": "string", "enum": ["anonymous", "username", "certificate"]},
                    "username": {
                        "type": "string",
                        "description": "With kind: username. The password may be an ${env:} or ${file:} secret."
                    },
                    "password": {"type": "string"},
                    "cert": {
                        "type": "string",
                        "description": "With kind: certificate. The user certificate (DER or PEM); relative to cert.pki_dir."
                    },
                    "key": {
                        "type": "string",
                        "description": "With kind: certificate. The user private key file (PEM); relative to cert.pki_dir. Inline keys are not accepted."
                    }
                }
            },
            "cert": {
                "type": "object",
                "additionalProperties": false,
                "required": ["cert", "key", "pki_dir"],
                "description": "Client application certificate; required with a secured security_policy.",
                "properties": {
                    "cert": {"type": "string", "description": "Client certificate (DER or PEM)"},
                    "key": {"type": "string", "description": "Client private key (PEM)"},
                    "pki_dir": {"type": "string", "description": "PKI store directory (trusted/, rejected/)"},
                    "trust_server_certs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                        "description": "Server certificate files to trust (DER or PEM); relative to pki_dir."
                    },
                    "accept_unknown_server_cert": {
                        "type": "boolean",
                        "default": false,
                        "description": "Trust any server certificate on first sight. INSECURE: accepts impostors; development only."
                    }
                }
            },
            "mode": {"type": "string", "enum": ["subscribe", "poll"], "default": "subscribe"},
            "interval": {
                "$ref": "#/$defs/duration",
                "description": "mode: poll only. How often every node is read; must be > 0."
            },
            "subscribe": {
                "type": "object",
                "additionalProperties": false,
                "description": "mode: subscribe only.",
                "properties": {
                    "publishing_interval": {"$ref": "#/$defs/duration", "default": "1s"},
                    "sampling_interval": {"$ref": "#/$defs/duration", "default": "250ms"},
                    "queue_size": {"type": "integer", "minimum": 1, "default": 10}
                }
            },
            "reconnect": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "min_backoff": {"$ref": "#/$defs/duration", "default": "1s"},
                    "max_backoff": {"$ref": "#/$defs/duration", "default": "30s"},
                    "max_handshakes_per_minute": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 12,
                        "description": "Hard rate limit on connection handshakes (RUSTSEC-2023-0071 mitigation)."
                    }
                }
            },
            "nodes": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["node_id", "name"],
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "OPC UA node id, for example ns=2;s=Temp, ns=2;i=1003, or i=85."
                        },
                        "name": {
                            "type": "string",
                            "minLength": 1,
                            "not": {"const": TIMESTAMP_KEY}
                        }
                    }
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
    use serde_json::json;

    fn problems_of(value: serde_json::Value) -> Vec<(&'static str, String)> {
        match OpcUaConfig::from_value(&value) {
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
            "endpoint": "opc.tcp://192.0.2.10:4840",
            "security_policy": "none",
            "security_mode": "none",
            "nodes": [{"node_id": "ns=2;s=Temp", "name": "temp"}]
        })
    }

    /// A self-signed certificate and key on disk, generated once per test
    /// process. Config validation reads these files, so tests that reach
    /// that point need real material.
    fn material() -> &'static (PathBuf, PathBuf) {
        static MATERIAL: std::sync::OnceLock<(PathBuf, PathBuf)> = std::sync::OnceLock::new();
        MATERIAL.get_or_init(|| {
            let dir = tempfile::tempdir().expect("temp dir").keep();
            let cert = dir.join("cert.der");
            let key = dir.join("key.pem");
            let description = opcua::types::ApplicationDescription {
                application_uri: opcua::types::UAString::from("urn:thadam:opcua:config-test"),
                ..Default::default()
            };
            opcua::crypto::CertificateStore::create_certificate_and_key(
                &description.into(),
                true,
                &cert,
                &key,
            )
            .expect("generate test certificate");
            (cert, key)
        })
    }

    /// A secured `cert` block pointing at the test material, with `pki_dir`
    /// as the given directory.
    fn secured_cert(pki_dir: &str) -> serde_json::Value {
        let (cert, key) = material();
        json!({"cert": cert, "key": key, "pki_dir": pki_dir})
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = OpcUaConfig::from_value(&base()).expect("valid");
        assert_eq!(cfg.auth, AuthConfig::Anonymous);
        assert_eq!(cfg.mode, ModeSpec::Subscribe);
        assert_eq!(cfg.interval, None);
        assert_eq!(cfg.subscribe, None);
        let subscribe = cfg.subscribe_settings();
        assert_eq!(subscribe.publishing_interval.0, Duration::from_secs(1));
        assert_eq!(subscribe.sampling_interval.0, Duration::from_millis(250));
        assert_eq!(subscribe.queue_size, 10);
        assert_eq!(cfg.reconnect.min_backoff.0, Duration::from_secs(1));
        assert_eq!(cfg.reconnect.max_backoff.0, Duration::from_secs(30));
        assert_eq!(cfg.reconnect.max_handshakes_per_minute, 12);
        assert_eq!(cfg.cert, None);
        assert_eq!(
            cfg.nodes[0].node_id,
            NodeId::from_str("ns=2;s=Temp").expect("parse")
        );
    }

    #[test]
    fn endpoint_security_policy_security_mode_and_nodes_are_required() {
        for key in ["endpoint", "security_policy", "security_mode", "nodes"] {
            let mut value = base();
            value.as_object_mut().unwrap().remove(key);
            let found = problems_of(value);
            assert_eq!(
                found.first().map(|(code, _)| *code),
                Some(codes::CONFIG_MISSING),
                "{key}"
            );
        }
    }

    #[test]
    fn endpoint_must_be_an_opc_tcp_url_with_host_and_port() {
        let found = problems_of(json!({
            "endpoint": "http://192.0.2.10:4840",
            "security_policy": "none",
            "security_mode": "none",
            "nodes": [{"node_id": "i=85", "name": "temp"}]
        }));
        assert!(found.iter().any(|(_, f)| f == "endpoint"));

        for bad in ["opc.tcp://192.0.2.10", "not a url", ""] {
            let mut value = base();
            value["endpoint"] = json!(bad);
            let found = problems_of(value);
            assert!(found.iter().any(|(_, f)| f == "endpoint"), "{bad}");
        }
    }

    #[test]
    fn security_mode_must_match_the_policy() {
        let mut value = base();
        value["security_mode"] = json!("sign");
        let found = problems_of(value);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1, "security_mode");

        let mut value = base();
        value["security_policy"] = json!("basic256sha256");
        value["security_mode"] = json!("none");
        let found = problems_of(value.clone());
        let mut fields: Vec<&str> = found.iter().map(|(_, f)| f.as_str()).collect();
        fields.sort_unstable();
        assert_eq!(fields, vec!["cert", "security_mode"]);

        let mut value = base();
        value["security_policy"] = json!("aes256_sha256_rsapss");
        value["security_mode"] = json!("sign_and_encrypt");
        value["cert"] = secured_cert("/tmp/pki");
        assert!(OpcUaConfig::from_value(&value).is_ok());
    }

    #[test]
    fn cert_is_rejected_with_security_none() {
        let mut value = base();
        value["cert"] = secured_cert("/tmp/pki");
        let found = problems_of(value);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1, "cert");
    }

    #[test]
    fn auth_variants_validate() {
        let mut value = base();
        value["auth"] = json!({"kind": "anonymous"});
        assert!(OpcUaConfig::from_value(&value).is_ok());

        for auth in [
            json!({"kind": "username", "username": "", "password": "x"}),
            json!({"kind": "username", "username": "op", "password": ""}),
        ] {
            let mut value = base();
            value["auth"] = auth;
            let found = problems_of(value);
            assert_eq!(found.len(), 1);
            assert_eq!(found[0].1, "auth");
        }

        let mut value = base();
        value["auth"] = json!({"kind": "username", "username": "op", "password": "hunter2"});
        let cfg = OpcUaConfig::from_value(&value).expect("valid");
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
        assert!(dbg.contains("<redacted>"), "{dbg}");
    }

    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        let mut value = base();
        value["nope"] = json!(1);
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["nodes"] = json!([{"node_id": "i=85", "name": "t", "nope": 1}]);
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["subscribe"] = json!({"nope": 1});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["reconnect"] = json!({"nope": 1});
        assert_eq!(problems_of(value).len(), 1);
    }

    #[test]
    fn bad_node_ids_are_config_errors() {
        let mut value = base();
        value["nodes"] = json!([{"node_id": "Temp", "name": "t"}]);
        let found = problems_of(value);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, codes::CONFIG_INVALID);
    }

    #[test]
    fn node_names_must_be_unique_nonempty_and_not_reserved() {
        let mut value = base();
        value["nodes"] = json!([
            {"node_id": "i=85", "name": "t"},
            {"node_id": "i=86", "name": "t"}
        ]);
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["nodes"] = json!([{"node_id": "i=85", "name": "ts_us"}]);
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["nodes"] = json!([{"node_id": "i=85", "name": ""}]);
        assert_eq!(problems_of(value).len(), 1);
    }

    #[test]
    fn empty_nodes_are_rejected() {
        let mut value = base();
        value["nodes"] = json!([]);
        let found = problems_of(value);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1, "nodes");
    }

    #[test]
    fn subscribe_and_reconnect_limits_validate() {
        let mut value = base();
        value["subscribe"] = json!({"publishing_interval": 0});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["subscribe"] = json!({"queue_size": 0});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["reconnect"] = json!({"min_backoff": "0ms"});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["reconnect"] = json!({"min_backoff": "10s", "max_backoff": "1s"});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["reconnect"] = json!({"max_handshakes_per_minute": 0});
        assert_eq!(problems_of(value).len(), 1);

        let mut value = base();
        value["subscribe"] = json!({"sampling_interval": "0ms"});
        assert!(OpcUaConfig::from_value(&value).is_ok());
    }

    #[test]
    fn duration_spellings() {
        assert_eq!(DurationSpec::parse("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(DurationSpec::parse("10"), Ok(Duration::from_secs(10)));
        assert_eq!(DurationSpec::parse("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(DurationSpec::parse("1h"), Ok(Duration::from_secs(3600)));
        assert!(DurationSpec::parse("").is_err());
        assert!(DurationSpec::parse("5d").is_err());
        assert!(DurationSpec::parse("-1s").is_err());
    }

    #[test]
    fn schema_declares_the_closed_shape() {
        let schema = config_schema();
        assert_eq!(schema["title"], "OPC UA source");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["nodes"]["items"]["additionalProperties"],
            false
        );
        assert_eq!(
            schema["required"],
            json!(["endpoint", "security_policy", "security_mode", "nodes"])
        );
        assert!(schema["properties"]["auth"]["properties"]["cert"].is_object());
        assert_eq!(
            schema["properties"]["mode"]["enum"],
            json!(["subscribe", "poll"])
        );
        assert!(schema["properties"]["interval"].is_object());
    }

    fn secured(mut value: serde_json::Value) -> serde_json::Value {
        value["security_policy"] = json!("aes256_sha256_rsapss");
        value["security_mode"] = json!("sign_and_encrypt");
        value["cert"] = secured_cert("/tmp/pki");
        value
    }

    #[test]
    fn certificate_auth_parses_and_validates_its_files() {
        let (cert, key) = material();
        let mut value = secured(base());
        value["auth"] = json!({"kind": "certificate", "cert": cert, "key": key});
        let cfg = OpcUaConfig::from_value(&value).expect("valid");
        assert!(matches!(cfg.auth, AuthConfig::Certificate { .. }));
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("Certificate"), "{dbg}");
    }

    #[test]
    fn certificate_auth_requires_a_secured_policy() {
        let (cert, key) = material();
        let mut value = base();
        value["auth"] = json!({"kind": "certificate", "cert": cert, "key": key});
        let found = problems_of(value);
        assert!(found.iter().any(|(_, f)| f == "auth"), "{found:?}");
    }

    #[test]
    fn certificate_auth_rejects_a_key_that_does_not_match() {
        let (cert, _) = material();
        let dir = tempfile::tempdir().expect("temp dir");
        let other_cert = dir.path().join("other.der");
        let other_key = dir.path().join("other.pem");
        let description = opcua::types::ApplicationDescription {
            application_uri: opcua::types::UAString::from("urn:thadam:opcua:other".to_owned()),
            ..Default::default()
        };
        opcua::crypto::CertificateStore::create_certificate_and_key(
            &description.into(),
            true,
            &other_cert,
            &other_key,
        )
        .expect("generate second certificate");

        let mut value = secured(base());
        value["auth"] = json!({"kind": "certificate", "cert": cert, "key": other_key});
        let found = problems_of(value);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].1, "auth");
    }

    #[test]
    fn certificate_auth_names_missing_files_without_contents() {
        let mut value = secured(base());
        value["auth"] = json!({"kind": "certificate", "cert": "missing.der", "key": "missing.pem"});
        let errors = OpcUaConfig::from_value(&value).expect_err("missing files");
        assert!(errors[0].message.contains("missing.der"), "{errors:?}");
    }

    #[test]
    fn cert_paths_resolve_against_pki_dir() {
        let (cert, key) = material();
        let dir = cert.parent().expect("dir").to_path_buf();
        let mut value = base();
        value["security_policy"] = json!("aes256_sha256_rsapss");
        value["security_mode"] = json!("sign_and_encrypt");
        let name = |p: &PathBuf| {
            p.file_name()
                .and_then(|n| n.to_str())
                .expect("name")
                .to_owned()
        };
        value["cert"] = json!({
            "cert": name(cert),
            "key": name(key),
            "pki_dir": dir,
        });
        assert!(OpcUaConfig::from_value(&value).is_ok());
    }

    #[test]
    fn trust_options_are_checked_and_exclusive() {
        let mut value = secured(base());
        value["cert"]["trust_server_certs"] = json!(["/nonexistent/server.der"]);
        let found = problems_of(value);
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].1, "cert");

        let (cert, _) = material();
        let mut value = secured(base());
        value["cert"]["trust_server_certs"] = json!([cert]);
        assert!(OpcUaConfig::from_value(&value).is_ok());

        let mut value = secured(base());
        value["cert"]["trust_server_certs"] = json!([cert]);
        value["cert"]["accept_unknown_server_cert"] = json!(true);
        assert_eq!(problems_of(value).len(), 1);

        let mut value = secured(base());
        value["cert"]["accept_unknown_server_cert"] = json!(true);
        assert!(OpcUaConfig::from_value(&value).is_ok());

        let mut value = secured(base());
        value["cert"]["trust_server"] = json!(true);
        assert_eq!(problems_of(value).len(), 1);
    }

    #[test]
    fn poll_mode_needs_a_positive_interval_and_no_subscribe_block() {
        let mut value = base();
        value["mode"] = json!("poll");
        let found = problems_of(value);
        assert!(found.iter().any(|(_, f)| f == "interval"), "{found:?}");

        let mut value = base();
        value["mode"] = json!("poll");
        value["interval"] = json!("0ms");
        let found = problems_of(value);
        assert!(found.iter().any(|(_, f)| f == "interval"), "{found:?}");

        let mut value = base();
        value["mode"] = json!("poll");
        value["interval"] = json!("500ms");
        value["subscribe"] = json!({"queue_size": 4});
        let found = problems_of(value);
        assert!(found.iter().any(|(_, f)| f == "subscribe"), "{found:?}");

        let mut value = base();
        value["mode"] = json!("poll");
        value["interval"] = json!("500ms");
        let cfg = OpcUaConfig::from_value(&value).expect("valid");
        assert_eq!(cfg.mode, ModeSpec::Poll);
        assert_eq!(cfg.interval.map(|i| i.0), Some(Duration::from_millis(500)));
    }

    #[test]
    fn interval_is_rejected_in_subscribe_mode() {
        let mut value = base();
        value["interval"] = json!("1s");
        let found = problems_of(value);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1, "interval");
    }
}
