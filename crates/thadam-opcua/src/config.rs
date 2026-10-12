//! Typed, validated configuration for the OPC UA source.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use opcua::types::NodeId;
use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;
use serde::de::{self, Deserializer};

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

/// How the client authenticates to the server. Certificate (`x509`) token
/// authentication is step 2.
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
        }
    }
}

/// The client application's certificate material, needed when
/// `security_policy` is not `none`: the secure channel handshake signs and
/// encrypts with it regardless of how the session authenticates.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertConfig {
    /// The client's own certificate (DER or PEM), relative to `pki_dir` or
    /// absolute.
    pub cert: PathBuf,
    /// The client's private key (PEM, PKCS#8 or RSA), relative to `pki_dir`
    /// or absolute.
    pub key: PathBuf,
    /// The PKI directory; server certificates the client accepts live under
    /// `<pki_dir>/trusted`, first-rejected ones under `<pki_dir>/rejected`.
    pub pki_dir: PathBuf,
    /// Trust any server certificate without verifying it. INSECURE, for
    /// development only: this disables both the trust-store check and the
    /// hostname/validity verification. Default `false`.
    #[serde(default)]
    pub trust_server: bool,
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

/// How values reach the pipeline. Only `subscribe` exists; a polling mode
/// may follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModeSpec {
    /// Monitored items on one subscription. The only mode.
    #[default]
    Subscribe,
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
    /// `subscribe`: monitored items.
    #[serde(default)]
    pub mode: ModeSpec,
    /// Subscription timing.
    #[serde(default)]
    pub subscribe: SubscribeConfig,
    /// Reconnect behavior.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
    /// The nodes to monitor, in order.
    pub nodes: Vec<NodeSpec>,
}

impl OpcUaConfig {
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
        }

        if self.subscribe.publishing_interval.0.is_zero() {
            found.push((
                "subscribe",
                "subscribe.publishing_interval must be greater than zero".into(),
            ));
        }
        if self.subscribe.queue_size == 0 {
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
                    "kind": {"type": "string", "enum": ["anonymous", "username"]},
                    "username": {
                        "type": "string",
                        "description": "With kind: username. The password may be an ${env:} or ${file:} secret."
                    },
                    "password": {"type": "string"}
                }
            },
            "cert": {
                "type": "object",
                "additionalProperties": false,
                "required": ["cert", "key", "pki_dir"],
                "description": "Client certificate material; required with a secured security_policy.",
                "properties": {
                    "cert": {"type": "string", "description": "Client certificate (DER or PEM)"},
                    "key": {"type": "string", "description": "Client private key (PEM)"},
                    "pki_dir": {"type": "string", "description": "PKI store directory (trusted/, rejected/)"},
                    "trust_server": {
                        "type": "boolean",
                        "default": false,
                        "description": "Trust any server certificate without verifying it. INSECURE, development only."
                    }
                }
            },
            "mode": {"type": "string", "enum": ["subscribe"], "default": "subscribe"},
            "subscribe": {
                "type": "object",
                "additionalProperties": false,
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

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = OpcUaConfig::from_value(&base()).expect("valid");
        assert_eq!(cfg.auth, AuthConfig::Anonymous);
        assert_eq!(cfg.mode, ModeSpec::Subscribe);
        assert_eq!(cfg.subscribe.publishing_interval.0, Duration::from_secs(1));
        assert_eq!(
            cfg.subscribe.sampling_interval.0,
            Duration::from_millis(250)
        );
        assert_eq!(cfg.subscribe.queue_size, 10);
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
        value["cert"] = json!({"cert": "c.der", "key": "k.pem", "pki_dir": "/tmp/pki"});
        assert!(OpcUaConfig::from_value(&value).is_ok());
    }

    #[test]
    fn cert_is_rejected_with_security_none() {
        let mut value = base();
        value["cert"] = json!({"cert": "c.der", "key": "k.pem", "pki_dir": "/tmp/pki"});
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
    }
}
