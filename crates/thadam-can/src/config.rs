//! Typed, validated configuration for the SocketCAN source.

use std::fmt;
use std::path::PathBuf;

use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;
use serde::de::{self, Deserializer};

/// Longest Linux network interface name (`IFNAMSIZ - 1`).
const MAX_INTERFACE_LEN: usize = 15;
/// Highest classic (11-bit) CAN identifier.
pub(crate) const MAX_STANDARD_ID: u32 = 0x7FF;
/// Highest extended (29-bit) CAN identifier.
pub(crate) const MAX_EXTENDED_ID: u32 = 0x1FFF_FFFF;

/// How each received frame becomes a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Emitter {
    /// The payload is the frame's data bytes. The default.
    #[default]
    Raw,
    /// The payload is a JSON object of decoded signal values and `ts_us`.
    /// Requires `dbc`.
    Decoded,
}

/// What `emit: decoded` does with a frame whose id is not in the DBC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unknown {
    /// Emit the frame like `emit: raw` would. The default.
    #[default]
    Raw,
    /// Drop the frame without counting it as a backpressure drop.
    Drop,
}

/// One SocketCAN receive filter: a frame is accepted when
/// `(frame_id & mask) == (id & mask)`, with `extended` matching the frame's
/// EFF bit as well. `id` and `mask` are decimal (`256`) or hex (`"0x12A"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdFilter {
    /// The CAN identifier to match.
    #[serde(deserialize_with = "deserialize_u32")]
    pub id: u32,
    /// The mask applied before comparing.
    #[serde(deserialize_with = "deserialize_u32")]
    pub mask: u32,
    /// Match extended (29-bit) frames; `false` matches classic (11-bit).
    #[serde(default)]
    pub extended: bool,
}

/// An integer written as a JSON number or as a `"0x"`-prefixed hex string.
fn deserialize_u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    struct Visitor;

    impl de::Visitor<'_> for Visitor {
        type Value = u32;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a non-negative integer or a hex string like \"0x12A\"")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u32, E> {
            u32::try_from(v).map_err(|_| E::custom(format!("{v} does not fit a 32-bit integer")))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<u32, E> {
            v.strip_prefix("0x")
                .or_else(|| v.strip_prefix("0X"))
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                .ok_or_else(|| E::custom(format!("`{v}` is not a hex string like \"0x12A\"")))
        }
    }

    deserializer.deserialize_any(Visitor)
}

/// The kernel CAN_RAW_RECV filter word pair for `filter`: a frame is accepted
/// when `frame_id & mask == id & mask`, where the EFF flag bit participates
/// through the mask so `extended` selects the addressing mode.
#[cfg(target_os = "linux")]
pub(crate) fn filter_words(filter: IdFilter) -> (u32, u32) {
    const CAN_EFF_FLAG: u32 = 0x8000_0000;
    let mut id = filter.id & !CAN_EFF_FLAG;
    let mask = filter.mask | CAN_EFF_FLAG;
    if filter.extended {
        id |= CAN_EFF_FLAG;
    }
    (id, mask)
}

/// The whole `config:` block of a SocketCAN source.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanConfig {
    /// The SocketCAN interface to receive from, for example `vcan0` or
    /// `can0`. The interface must exist and be up before the pipeline starts.
    pub interface: String,
    /// Open the interface for CAN FD (usually `ip link set ... fd on`).
    /// When `false` only classic CAN 2.0 frames are received.
    #[serde(default)]
    pub fd: bool,
    /// Kernel receive filters. Empty accepts every frame.
    #[serde(default)]
    pub filters: Vec<IdFilter>,
    /// Path to a DBC file with the message and signal definitions. Required
    /// with `emit: decoded`; rejected otherwise.
    pub dbc: Option<PathBuf>,
    /// How frames become messages.
    #[serde(default)]
    pub emit: Emitter,
    /// With `emit: decoded`, what to do with ids missing from the DBC.
    /// Ignored with `emit: raw`.
    #[serde(default)]
    pub unknown: Unknown,
}

impl CanConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<CanConfig, Vec<ThadamError>> {
        let config: CanConfig = serde_json::from_value(value.clone()).map_err(|e| {
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

        if let Some(reason) = interface_problem(&self.interface) {
            found.push(("interface", reason));
        }
        match (self.emit, &self.dbc) {
            (Emitter::Decoded, None) => {
                found.push(("dbc", "emit: decoded requires a dbc file path".into()))
            }
            (Emitter::Raw, Some(_)) => {
                found.push(("dbc", "dbc is only used with emit: decoded".into()))
            }
            _ => {}
        }
        for (i, filter) in self.filters.iter().enumerate() {
            let max = if filter.extended {
                MAX_EXTENDED_ID
            } else {
                MAX_STANDARD_ID
            };
            let width = if filter.extended { "29-bit" } else { "11-bit" };
            for (field, value) in [("id", filter.id), ("mask", filter.mask)] {
                if value > max {
                    found.push((
                        "filters",
                        format!(
                            "filters[{i}]: {field} 0x{value:X} does not fit a {width} CAN identifier"
                        ),
                    ));
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

/// Why an interface name is unusable, matching the kernel's rules.
fn interface_problem(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("interface must not be empty".into());
    }
    if name.len() > MAX_INTERFACE_LEN {
        return Some(format!(
            "interface is longer than {MAX_INTERFACE_LEN} characters"
        ));
    }
    if name == "." || name == ".." {
        return Some(format!("`{name}` is not a valid interface name"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
    {
        return Some("interface names may contain only ASCII letters, digits, and - _ . :".into());
    }
    None
}

/// The JSON Schema for the `config:` block of a SocketCAN source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "SocketCAN source",
        "type": "object",
        "additionalProperties": false,
        "required": ["interface"],
        "properties": {
            "interface": {
                "type": "string",
                "minLength": 1,
                "maxLength": 15,
                "pattern": "^[A-Za-z0-9._:-]+$",
                "description": "SocketCAN interface to receive from, for example vcan0 or can0. Linux only."
            },
            "fd": {
                "type": "boolean",
                "default": false,
                "description": "Receive CAN FD frames (the interface must be configured with fd on)."
            },
            "filters": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["id", "mask"],
                    "properties": {
                        "id": {"$ref": "#/$defs/canint"},
                        "mask": {"$ref": "#/$defs/canint"},
                        "extended": {"type": "boolean", "default": false}
                    }
                }
            },
            "dbc": {
                "type": "string",
                "minLength": 1,
                "description": "Path to a DBC file. Required with emit: decoded."
            },
            "emit": {
                "type": "string",
                "enum": ["raw", "decoded"],
                "default": "raw"
            },
            "unknown": {
                "type": "string",
                "enum": ["raw", "drop"],
                "default": "raw",
                "description": "With emit: decoded, how to handle ids missing from the DBC."
            }
        },
        "$defs": {
            "canint": {
                "oneOf": [
                    {"type": "integer", "minimum": 0, "maximum": 536870911},
                    {"type": "string", "pattern": "^0[xX][0-9a-fA-F]{1,8}$"}
                ]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::codes;
    use serde_json::json;

    fn problems_of(value: serde_json::Value) -> Vec<(&'static str, String)> {
        match CanConfig::from_value(&value) {
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
        json!({"interface": "vcan0"})
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = CanConfig::from_value(&base()).expect("valid");
        assert!(!cfg.fd);
        assert!(cfg.filters.is_empty());
        assert_eq!(cfg.dbc, None);
        assert_eq!(cfg.emit, Emitter::Raw);
        assert_eq!(cfg.unknown, Unknown::Raw);
    }

    #[test]
    fn interface_is_required() {
        let found = problems_of(json!({}));
        assert_eq!(found[0].0, codes::CONFIG_MISSING);
    }

    #[test]
    fn interface_names_are_checked_against_kernel_rules() {
        for (bad, reason) in [
            ("", "empty"),
            ("this-interface-name-is-long", "too long"),
            ("can 0", "bad char"),
            ("can/0", "bad char"),
            (".", "dots"),
            ("..", "dots"),
        ] {
            let found = problems_of(json!({"interface": bad}));
            assert_eq!(
                found,
                vec![(codes::CONFIG_INVALID, "interface".to_string())],
                "{bad}: {reason}"
            );
        }
        for good in ["vcan0", "can0", "vxcan-1.0", "vcan:1"] {
            assert!(
                CanConfig::from_value(&json!({"interface": good})).is_ok(),
                "{good}"
            );
        }
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        for value in [
            json!({"interface": "vcan0", "nope": 1}),
            json!({"interface": "vcan0", "filters": [{"id": 1, "mask": 1, "nope": 2}]}),
        ] {
            let found = problems_of(value.clone());
            assert_eq!(found.len(), 1, "{value}");
            assert_eq!(found[0].0, codes::CONFIG_INVALID, "{value}");
        }
    }

    #[test]
    fn decoded_requires_dbc_and_raw_rejects_it() {
        let found = problems_of(json!({"interface": "vcan0", "emit": "decoded"}));
        assert_eq!(found, vec![(codes::CONFIG_INVALID, "dbc".to_string())]);
        let found = problems_of(json!({"interface": "vcan0", "dbc": "x.dbc"}));
        assert_eq!(found, vec![(codes::CONFIG_INVALID, "dbc".to_string())]);
        assert!(
            CanConfig::from_value(
                &json!({"interface": "vcan0", "emit": "decoded", "dbc": "x.dbc"})
            )
            .is_ok()
        );
    }

    #[test]
    fn filters_accept_numbers_hex_strings_and_defaults() {
        let cfg = CanConfig::from_value(&json!({
            "interface": "vcan0",
            "filters": [{"id": "0x12A", "mask": "0x7FF"}, {"id": 291, "mask": 536870911, "extended": true}]
        }))
        .expect("valid");
        assert_eq!(
            cfg.filters,
            vec![
                IdFilter {
                    id: 0x12A,
                    mask: 0x7FF,
                    extended: false
                },
                IdFilter {
                    id: 0x123,
                    mask: 0x1FFF_FFFF,
                    extended: true
                },
            ]
        );

        for bad in [
            json!({"id": "12A", "mask": 1}),
            json!({"id": -1, "mask": 1}),
        ] {
            let found = problems_of(json!({"interface": "vcan0", "filters": [bad]}));
            assert_eq!(found.len(), 1, "{bad}");
            assert_eq!(found[0].0, codes::CONFIG_INVALID, "{bad}");
        }
    }

    #[test]
    fn filter_id_and_mask_must_fit_the_addressing_mode() {
        let found = problems_of(json!({
            "interface": "vcan0",
            "filters": [{"id": "0x800", "mask": "0x7FF"}]
        }));
        assert_eq!(found, vec![(codes::CONFIG_INVALID, "filters".to_string())]);
        let found = problems_of(json!({
            "interface": "vcan0",
            "filters": [{"id": "0x123", "mask": "0x1FFFFFFF", "extended": true},
                        {"id": 1, "mask": "0x20000000", "extended": true}]
        }));
        assert_eq!(found, vec![(codes::CONFIG_INVALID, "filters".to_string())]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn filter_words_carry_the_extended_bit_in_id_and_mask() {
        let f = |id, mask, extended| filter_words(IdFilter { id, mask, extended });
        assert_eq!(f(0x123, 0x7FF, false), (0x0000_0123, 0x8000_07FF));
        assert_eq!(f(0x123, 0x1FFF_FFFF, true), (0x8000_0123, 0x9FFF_FFFF));
        assert_eq!(f(0x8000_0000, 0x7FF, false), (0, 0x8000_07FF));
    }

    #[test]
    fn schema_declares_the_closed_shape() {
        let schema = config_schema();
        assert_eq!(schema["title"], "SocketCAN source");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["filters"]["items"]["additionalProperties"],
            false
        );
        let mut keys: Vec<&str> = schema["properties"]
            .as_object()
            .expect("properties")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["dbc", "emit", "fd", "filters", "interface", "unknown"]
        );
    }
}
