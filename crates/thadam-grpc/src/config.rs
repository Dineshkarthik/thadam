//! Typed, validated configuration for the gRPC source.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use serde::Deserialize;
use thadam_core::{ErrorDomain, ThadamError, codes};

/// The largest message accepted by default: 1 MiB.
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1 << 20;
/// The most messages accepted but not yet committed, by default.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;
/// The smallest queue that still lets a burst of clients queue behind a commit.
pub const MIN_QUEUE_CAPACITY: usize = 16;

const DEFAULT_BIND: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 50051));

/// Server certificate and key, both PEM files.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert: PathBuf,
    /// PEM private key.
    pub key: PathBuf,
}

/// The whole `config:` block of a gRPC source.
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrpcConfig {
    /// The address to bind. Loopback by default.
    pub bind: SocketAddr,
    /// When set, calls must send `authorization: Bearer <token>` metadata.
    pub bearer_token: Option<String>,
    /// When set, the server speaks TLS.
    pub tls: Option<TlsConfig>,
    /// The largest accepted message payload, in bytes.
    pub max_message_bytes: u64,
    /// Most messages accepted but not yet committed. Above this, calls answer
    /// `RESOURCE_EXHAUSTED`.
    pub queue_capacity: usize,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        GrpcConfig {
            bind: DEFAULT_BIND,
            bearer_token: None,
            tls: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

impl fmt::Debug for GrpcConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcConfig")
            .field("bind", &self.bind)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field("tls", &self.tls)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("queue_capacity", &self.queue_capacity)
            .finish()
    }
}

impl GrpcConfig {
    /// Bearer token travels on a non-loopback address without TLS.
    pub(crate) fn sends_credentials_unsecured(&self) -> bool {
        !self.bind.ip().is_loopback() && self.bearer_token.is_some() && self.tls.is_none()
    }

    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<GrpcConfig, Vec<ThadamError>> {
        let config: GrpcConfig = serde_json::from_value(value.clone()).map_err(|e| {
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
            if config.sends_credentials_unsecured() {
                tracing::warn!(
                    bind = %config.bind,
                    "gRPC bearer_token is configured on a non-loopback address without tls; the token is not encrypted"
                );
            }
            Ok(config)
        } else {
            Err(errors)
        }
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
        if let Some(token) = &self.bearer_token
            && (token.is_empty() || token.chars().any(char::is_control))
        {
            found.push((
                "bearer_token",
                "bearer_token must be non-empty with no control characters".into(),
            ));
        }
        if !self.bind.ip().is_loopback() && self.bearer_token.is_none() {
            found.push((
                "bind",
                format!(
                    "binding to {} exposes the endpoint beyond loopback: set `bearer_token`",
                    self.bind.ip()
                ),
            ));
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

/// The JSON Schema for the `config:` block of a gRPC source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "gRPC source",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "bind": {
                "type": "string",
                "description": "Socket address, for example 127.0.0.1:50051. Non-loopback needs bearer_token.",
                "default": "127.0.0.1:50051"
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
            "max_message_bytes": {"type": "integer", "minimum": 1, "default": 1048576},
            "queue_capacity": {"type": "integer", "minimum": 16, "default": 1024}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn codes_of(value: serde_json::Value) -> Vec<(&'static str, String)> {
        match GrpcConfig::from_value(&value) {
            Ok(_) => Vec::new(),
            Err(errors) => errors.into_iter().map(|e| (e.code, e.message)).collect(),
        }
    }

    fn fields(value: serde_json::Value) -> Vec<String> {
        match GrpcConfig::from_value(&value) {
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
    fn empty_block_is_a_valid_loopback_server() {
        let cfg = GrpcConfig::from_value(&json!({})).expect("valid");
        assert_eq!(cfg.bind, DEFAULT_BIND);
        assert_eq!(cfg.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
        assert_eq!(cfg.queue_capacity, DEFAULT_QUEUE_CAPACITY);
        assert!(cfg.bearer_token.is_none() && cfg.tls.is_none());
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        for value in [
            json!({"nope": 1}),
            json!({"bind": "127.0.0.1:1", "nope": true}),
            json!({"tls": {"cert": "c", "key": "k", "nope": 1}}),
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
            fields(json!({"bind": "0.0.0.0:50051"})),
            vec!["bind".to_string()]
        );
        assert_eq!(
            fields(json!({"bind": "[::]:50051"})),
            vec!["bind".to_string()]
        );
        assert_eq!(
            fields(json!({
                "bind": "[::]:50443", "tls": {"cert": "/c.pem", "key": "/k.pem"}
            })),
            vec!["bind".to_string()],
            "TLS without client auth does not authenticate callers"
        );
        let plaintext = GrpcConfig::from_value(&json!({
            "bind": "0.0.0.0:50051", "bearer_token": "s3cret"
        }))
        .expect("valid");
        assert!(plaintext.sends_credentials_unsecured());

        let secured = GrpcConfig::from_value(&json!({
            "bind": "[::]:50443",
            "bearer_token": "s3cret",
            "tls": {"cert": "/c.pem", "key": "/k.pem"}
        }))
        .expect("valid");
        assert!(!secured.sends_credentials_unsecured());

        let loopback = GrpcConfig::from_value(&json!({
            "bind": "127.0.0.1:50051", "bearer_token": "s3cret"
        }))
        .expect("valid");
        assert!(!loopback.sends_credentials_unsecured());
        assert!(GrpcConfig::from_value(&json!({"bind": "[::1]:50051"})).is_ok());
    }

    #[test]
    fn size_capacity_and_token_bounds() {
        assert_eq!(
            fields(json!({"max_message_bytes": 0})),
            vec!["max_message_bytes".to_string()]
        );
        assert_eq!(
            fields(json!({"queue_capacity": 15})),
            vec!["queue_capacity".to_string()]
        );
        assert_eq!(
            fields(json!({"bearer_token": ""})),
            vec!["bearer_token".to_string()]
        );
        assert_eq!(
            fields(json!({"bearer_token": "a\nb"})),
            vec!["bearer_token".to_string()]
        );
    }

    #[test]
    fn invalid_bind_and_missing_tls_fields_are_classified() {
        let found = codes_of(json!({"bind": "127.0.0.1:99999"}));
        assert_eq!(found[0].0, codes::CONFIG_INVALID);
        let found = codes_of(json!({"tls": {"cert": "c"}}));
        assert_eq!(found[0].0, codes::CONFIG_MISSING);
    }

    #[test]
    fn debug_output_redacts_the_token() {
        let cfg = GrpcConfig::from_value(&json!({
            "bind": "0.0.0.0:50051", "bearer_token": "hunter2"
        }))
        .expect("valid");
        assert!(!format!("{cfg:?}").contains("hunter2"));
    }

    #[test]
    fn schema_is_closed_and_lists_every_key() {
        let schema = config_schema();
        assert_eq!(schema["title"], "gRPC source");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["tls"]["additionalProperties"], false);
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
                "bearer_token",
                "bind",
                "max_message_bytes",
                "queue_capacity",
                "tls"
            ]
        );
    }
}
