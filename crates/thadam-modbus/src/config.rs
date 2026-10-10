//! Typed, validated configuration for the Modbus source.

use std::fmt;
use std::time::Duration;

use thadam_core::{ErrorDomain, ThadamError, codes};
use serde::Deserialize;
use serde::de::{self, Deserializer};

/// The most holding or input registers one read may ask for (Modbus limit).
pub const MAX_REGISTERS_PER_READ: u16 = 125;
/// The most coils or discrete inputs one read may ask for (Modbus limit).
pub const MAX_BITS_PER_READ: u16 = 2000;
/// The reserved JSON key that carries the poll time in every payload.
pub const TIMESTAMP_KEY: &str = "ts_us";

const DEFAULT_UNIT_ID: u8 = 1;

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

/// Which table a read addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadKind {
    /// Holding registers (function 03). 16-bit words.
    Holding,
    /// Input registers (function 04). 16-bit words.
    Input,
    /// Coils (function 01). Single bits.
    Coil,
    /// Discrete inputs (function 02). Single bits.
    Discrete,
}

impl ReadKind {
    /// Whether the read returns single bits rather than 16-bit words.
    pub fn is_bits(self) -> bool {
        matches!(self, ReadKind::Coil | ReadKind::Discrete)
    }
}

/// How a read's registers become one value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    /// Unsigned 16-bit integer, one register.
    U16,
    /// Signed 16-bit integer, one register.
    I16,
    /// Unsigned 32-bit integer, two registers.
    U32,
    /// Signed 32-bit integer, two registers.
    I32,
    /// IEEE 754 single precision, two registers.
    F32,
    /// Single bit. Only for `coil` and `discrete` reads.
    Bool,
    /// The registers as unsigned 16-bit integers, unchanged. Each register is
    /// one element of the array.
    Raw,
}

/// Which register of a 32-bit value comes first on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WordOrder {
    /// The first register holds the high 16 bits. The default.
    #[default]
    Big,
    /// The first register holds the low 16 bits (word swap).
    Little,
}

/// One named read. Every poll performs all configured reads in order.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSpec {
    /// The JSON key this read's value is stored under.
    pub name: String,
    /// The table to read.
    pub kind: ReadKind,
    /// The first address (0-based) in the table.
    pub start: u16,
    /// How many registers or bits to read.
    pub count: u16,
    /// How the registers are interpreted.
    pub data_type: DataType,
    /// Word order for 32-bit values. Ignored for other types.
    #[serde(default)]
    pub word_order: WordOrder,
    /// Multiplier applied to numeric values. Numeric types only.
    pub scale: Option<f64>,
    /// Added after scaling. Numeric types only.
    pub offset: Option<f64>,
}

impl ReadSpec {
    /// Whether `scale` or `offset` is set, which makes numeric values floats.
    pub fn is_scaled(&self) -> bool {
        self.scale.is_some() || self.offset.is_some()
    }

    /// How many JSON elements the read produces: one per value.
    pub fn value_count(&self) -> usize {
        let width = match self.data_type {
            DataType::U32 | DataType::I32 | DataType::F32 => 2,
            _ => 1,
        };
        usize::from(self.count) / width
    }
}

/// The whole `config:` block of a Modbus source.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModbusConfig {
    /// `host:port` or `ip:port` of the Modbus TCP server.
    pub address: String,
    /// The Modbus unit identifier sent in the MBAP header.
    #[serde(default = "default_unit_id")]
    pub unit_id: u8,
    /// Time between the starts of successful polls.
    #[serde(default = "default_interval")]
    pub interval: DurationSpec,
    /// Deadline for connecting and for each read.
    #[serde(default = "default_timeout")]
    pub timeout: DurationSpec,
    /// The longest wait after consecutive failures.
    #[serde(default = "default_backoff_max")]
    pub backoff_max: DurationSpec,
    /// The reads performed, in order, on every poll.
    pub reads: Vec<ReadSpec>,
}

fn default_unit_id() -> u8 {
    DEFAULT_UNIT_ID
}

fn default_interval() -> DurationSpec {
    DurationSpec(Duration::from_secs(10))
}

fn default_timeout() -> DurationSpec {
    DurationSpec(Duration::from_secs(5))
}

fn default_backoff_max() -> DurationSpec {
    DurationSpec(Duration::from_secs(300))
}

impl ModbusConfig {
    /// Parses and validates `value`, returning every problem found.
    pub fn from_value(value: &serde_json::Value) -> Result<ModbusConfig, Vec<ThadamError>> {
        let config: ModbusConfig = serde_json::from_value(value.clone()).map_err(|e| {
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

        if split_address(&self.address).is_none() {
            found.push((
                "address",
                "address must be host:port, for example 192.0.2.10:502".into(),
            ));
        }
        for (field, spec) in [("interval", self.interval), ("timeout", self.timeout)] {
            if spec.0.is_zero() {
                found.push((field, format!("{field} must be greater than zero")));
            }
        }
        if self.backoff_max.0 < self.interval.0 {
            found.push((
                "backoff_max",
                "backoff_max must be at least interval".into(),
            ));
        }
        found.extend(reads_problems(&self.reads));

        found
            .into_iter()
            .map(|(field, message)| {
                ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
                    .with_context("field", field)
            })
            .collect()
    }
}

/// Splits `host:port`, accepting bracketed IPv6 hosts. The port must be
/// non-zero.
pub(crate) fn split_address(address: &str) -> Option<(&str, u16)> {
    let (host, port) = address.rsplit_once(':')?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let port: u16 = port.parse().ok()?;
    let host_ok = !host.is_empty() && !host.chars().any(|c| c.is_whitespace() || c == '/');
    (host_ok && port != 0).then_some((host, port))
}

fn reads_problems(reads: &[ReadSpec]) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();
    if reads.is_empty() {
        found.push(("reads", "at least one read is required".into()));
    }

    let mut names: Vec<&str> = Vec::new();
    for (i, read) in reads.iter().enumerate() {
        if names.contains(&read.name.as_str()) {
            found.push((
                "reads",
                format!("reads[{i}]: name `{}` is used more than once", read.name),
            ));
        }
        names.push(&read.name);

        for message in read_problems(read) {
            found.push(("reads", format!("reads[{i}] `{}`: {message}", read.name)));
        }
    }
    found
}

fn read_problems(read: &ReadSpec) -> Vec<String> {
    let mut found = Vec::new();

    if read.name.is_empty() {
        found.push("name must not be empty".into());
    }
    if read.name == TIMESTAMP_KEY {
        found.push(format!("`{TIMESTAMP_KEY}` is reserved for the poll time"));
    }

    let (max, unit) = if read.kind.is_bits() {
        (MAX_BITS_PER_READ, "coils or discrete inputs")
    } else {
        (MAX_REGISTERS_PER_READ, "registers")
    };
    if read.count == 0 || read.count > max {
        found.push(format!("count must be between 1 and {max} {unit}"));
    }
    if u32::from(read.start) + u32::from(read.count) > 0x1_0000 {
        found.push("start + count runs past the end of the address space (65536)".into());
    }

    match (read.kind.is_bits(), read.data_type) {
        (true, DataType::Bool)
        | (
            false,
            DataType::U16
            | DataType::I16
            | DataType::U32
            | DataType::I32
            | DataType::F32
            | DataType::Raw,
        ) => {}
        (true, _) => {
            found.push("coil and discrete reads return bits: use `data_type: bool`".into())
        }
        (false, DataType::Bool) => found.push(
            "holding and input reads return registers: use u16, i16, u32, i32, f32, or raw".into(),
        ),
    }

    if matches!(
        read.data_type,
        DataType::U32 | DataType::I32 | DataType::F32
    ) && !read.count.is_multiple_of(2)
    {
        found.push("32-bit types need an even count of registers".into());
    }

    if matches!(read.data_type, DataType::Bool | DataType::Raw) && read.is_scaled() {
        found.push("scale and offset apply to numeric types only, not bool or raw".into());
    }

    found
}

/// The JSON Schema for the `config:` block of a Modbus source.
pub fn config_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Modbus source",
        "type": "object",
        "additionalProperties": false,
        "required": ["address", "reads"],
        "properties": {
            "address": {
                "type": "string",
                "description": "host:port of the Modbus TCP server, for example 192.0.2.10:502.",
                "pattern": "^.+:[0-9]+$"
            },
            "unit_id": {
                "type": "integer",
                "minimum": 0,
                "maximum": 255,
                "default": 1
            },
            "interval": {"$ref": "#/$defs/duration", "default": "10s"},
            "timeout": {"$ref": "#/$defs/duration", "default": "5s"},
            "backoff_max": {"$ref": "#/$defs/duration", "default": "5m"},
            "reads": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "kind", "start", "count", "data_type"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "minLength": 1,
                            "not": {"const": TIMESTAMP_KEY}
                        },
                        "kind": {
                            "type": "string",
                            "enum": ["holding", "input", "coil", "discrete"]
                        },
                        "start": {"type": "integer", "minimum": 0, "maximum": 65535},
                        "count": {"type": "integer", "minimum": 1, "maximum": 2000},
                        "data_type": {
                            "type": "string",
                            "enum": ["u16", "i16", "u32", "i32", "f32", "bool", "raw"]
                        },
                        "word_order": {
                            "type": "string",
                            "enum": ["big", "little"],
                            "default": "big"
                        },
                        "scale": {"type": "number"},
                        "offset": {"type": "number"}
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
    use thadam_core::codes;
    use serde_json::json;

    fn problems_of(value: serde_json::Value) -> Vec<(&'static str, String)> {
        match ModbusConfig::from_value(&value) {
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

    fn base(reads: serde_json::Value) -> serde_json::Value {
        json!({"address": "127.0.0.1:502", "reads": reads})
    }

    fn one_read(read: serde_json::Value) -> serde_json::Value {
        base(json!([read]))
    }

    fn valid_read() -> serde_json::Value {
        json!({"name": "t", "kind": "holding", "start": 0, "count": 2, "data_type": "f32"})
    }

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let cfg = ModbusConfig::from_value(&base(json!([valid_read()]))).expect("valid");
        assert_eq!(cfg.unit_id, 1);
        assert_eq!(cfg.interval.0, Duration::from_secs(10));
        assert_eq!(cfg.timeout.0, Duration::from_secs(5));
        assert_eq!(cfg.backoff_max.0, Duration::from_secs(300));
        assert_eq!(cfg.reads[0].word_order, WordOrder::Big);
        assert_eq!(cfg.reads[0].scale, None);
    }

    #[test]
    fn address_and_reads_are_required() {
        let found = problems_of(json!({"reads": [valid_read()]}));
        assert_eq!(found[0].0, codes::CONFIG_MISSING);
        let found = problems_of(json!({"address": "127.0.0.1:502"}));
        assert_eq!(found[0].0, codes::CONFIG_MISSING);
    }

    #[test]
    fn address_must_be_host_and_nonzero_port() {
        for bad in [
            "127.0.0.1",
            "127.0.0.1:0",
            ":502",
            "a b:502",
            "host:70000",
            "",
        ] {
            let found = problems_of(json!({"address": bad, "reads": [valid_read()]}));
            assert_eq!(
                found,
                vec![(codes::CONFIG_INVALID, "address".to_string())],
                "{bad}"
            );
        }
        for good in ["127.0.0.1:502", "plc.local:502", "[::1]:502"] {
            assert!(
                ModbusConfig::from_value(&json!({"address": good, "reads": [valid_read()]}))
                    .is_ok(),
                "{good}"
            );
        }
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        for value in [
            json!({"address": "h:1", "reads": [valid_read()], "nope": 1}),
            one_read(
                json!({"name": "t", "kind": "holding", "start": 0, "count": 1,
                            "data_type": "u16", "nope": 1}),
            ),
        ] {
            let found = problems_of(value.clone());
            assert_eq!(found.len(), 1, "{value}");
            assert_eq!(found[0].0, codes::CONFIG_INVALID, "{value}");
        }
    }

    #[test]
    fn unknown_kind_and_data_type_are_config_errors() {
        let found = problems_of(one_read(json!({"name": "t", "kind": "hold", "start": 0,
                                                "count": 1, "data_type": "u16"})));
        assert_eq!(found[0].0, codes::CONFIG_INVALID);
        let found = problems_of(one_read(json!({"name": "t", "kind": "holding", "start": 0,
                                                "count": 1, "data_type": "f64"})));
        assert_eq!(found[0].0, codes::CONFIG_INVALID);
    }

    #[test]
    fn start_must_fit_a_register_address() {
        let found = problems_of(one_read(
            json!({"name": "t", "kind": "holding", "start": 70000,
                                                "count": 1, "data_type": "u16"}),
        ));
        assert_eq!(found[0].0, codes::CONFIG_INVALID);
    }

    #[test]
    fn register_and_bit_counts_follow_the_modbus_limits() {
        let read = |kind: &str, count: u16, data_type: &str| {
            one_read(
                json!({"name": "t", "kind": kind, "start": 0, "count": count,
                            "data_type": data_type}),
            )
        };
        assert!(ModbusConfig::from_value(&read("holding", 125, "raw")).is_ok());
        assert_eq!(problems_of(read("holding", 126, "raw"))[0].1, "reads");
        assert_eq!(problems_of(read("input", 0, "raw"))[0].1, "reads");
        assert!(ModbusConfig::from_value(&read("coil", 2000, "bool")).is_ok());
        assert_eq!(problems_of(read("discrete", 2001, "bool"))[0].1, "reads");
    }

    #[test]
    fn address_space_overflow_is_rejected() {
        let found = problems_of(one_read(
            json!({"name": "t", "kind": "holding", "start": 65535,
                                                "count": 2, "data_type": "raw"}),
        ));
        assert_eq!(found.len(), 1);
        assert!(
            ModbusConfig::from_value(&one_read(json!({"name": "t", "kind": "holding",
                "start": 65534, "count": 2, "data_type": "raw"})))
            .is_ok()
        );
    }

    #[test]
    fn data_type_must_match_the_table_kind() {
        let bits = one_read(json!({"name": "t", "kind": "coil", "start": 0, "count": 1,
                                   "data_type": "u16"}));
        assert_eq!(problems_of(bits).len(), 1);
        let words = one_read(json!({"name": "t", "kind": "input", "start": 0, "count": 1,
                                    "data_type": "bool"}));
        assert_eq!(problems_of(words).len(), 1);
    }

    #[test]
    fn thirty_two_bit_types_need_an_even_count() {
        let found = problems_of(one_read(json!({"name": "t", "kind": "holding", "start": 0,
                                                "count": 3, "data_type": "u32"})));
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn scale_and_offset_are_numeric_only() {
        let found = problems_of(one_read(json!({"name": "b", "kind": "coil", "start": 0,
                                                "count": 1, "data_type": "bool", "scale": 2})));
        assert_eq!(found.len(), 1);
        let found = problems_of(one_read(json!({"name": "r", "kind": "holding", "start": 0,
                                                "count": 1, "data_type": "raw", "offset": 1})));
        assert_eq!(found.len(), 1);
        assert!(
            ModbusConfig::from_value(&one_read(json!({"name": "n", "kind": "holding",
                "start": 0, "count": 1, "data_type": "i16", "scale": 0.1, "offset": -40})))
            .is_ok()
        );
    }

    #[test]
    fn reads_need_unique_names_and_none_may_be_reserved() {
        let read = |name: &str| {
            json!({"name": name, "kind": "holding", "start": 0, "count": 1,
                                       "data_type": "u16"})
        };
        let found = problems_of(base(json!([read("a"), read("a")])));
        assert_eq!(found.len(), 1);
        let found = problems_of(one_read(read("ts_us")));
        assert_eq!(found.len(), 1);
        let found = problems_of(one_read(read("")));
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn empty_reads_are_rejected() {
        assert_eq!(problems_of(base(json!([])))[0].1, "reads");
    }

    #[test]
    fn durations_must_be_positive_and_backoff_must_cover_interval() {
        let found = problems_of(json!({
            "address": "h:1", "reads": [valid_read()],
            "interval": 0, "timeout": "0ms"
        }));
        let mut fields: Vec<&str> = found.iter().map(|(_, f)| f.as_str()).collect();
        fields.sort_unstable();
        assert_eq!(fields, vec!["interval", "timeout"]);

        let found = problems_of(json!({
            "address": "h:1", "reads": [valid_read()],
            "interval": "10s", "backoff_max": "1s"
        }));
        assert_eq!(
            found,
            vec![(codes::CONFIG_INVALID, "backoff_max".to_string())]
        );
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
        assert_eq!(schema["title"], "Modbus source");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["reads"]["items"]["additionalProperties"],
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
            vec![
                "address",
                "backoff_max",
                "interval",
                "reads",
                "timeout",
                "unit_id"
            ]
        );
    }
}
