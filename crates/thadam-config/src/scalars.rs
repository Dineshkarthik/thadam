//! Scalar configuration types: durations, sizes, counts, field types, time domains.
//!
//! Each parses a human-friendly string (`50ms`, `2GiB`, `500k`, `list<f32>`)
//! and serializes to one canonical form, so the config hash does not depend on
//! how a value was spelled.

use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use thadam_core::{FieldType, TimeDomain};

/// The deepest `list<list<...>>` nesting accepted.
const MAX_LIST_DEPTH: usize = 3;

pub(crate) fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s.trim()).map_err(|e| format!("invalid duration `{s}`: {e}"))
}

pub(crate) fn parse_size(s: &str) -> Result<u64, String> {
    s.trim()
        .parse::<bytesize::ByteSize>()
        .map(|b| b.as_u64())
        .map_err(|_| {
            format!("invalid size `{s}`: write a number with a unit such as `128MiB` or `2GiB` (KiB, MiB, GiB, TiB; or KB, MB, GB, TB)")
        })
}

pub(crate) fn parse_count(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let (digits, multiplier) = match t.strip_suffix(['k', 'K']) {
        Some(d) => (d, 1_000),
        None => match t.strip_suffix(['m', 'M']) {
            Some(d) => (d, 1_000_000),
            None => (t, 1),
        },
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .ok_or_else(|| {
            format!("invalid count `{s}`: expected an integer, optionally with a `k` or `m` suffix")
        })
}

macro_rules! scalar {
    (
        $(#[$meta:meta])* $name:ident($inner:ty), $expecting:literal,
        str: $parse_str:expr, uint: $from_uint:expr,
        serialize: $ser:expr, schema: $schema:expr
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct $name(pub $inner);

        impl $name {
            /// The parsed value.
            pub fn get(self) -> $inner {
                self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl Visitor<'_> for V {
                    type Value = $name;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str($expecting)
                    }

                    fn visit_str<E: de::Error>(self, v: &str) -> Result<$name, E> {
                        let parse: fn(&str) -> Result<$inner, String> = $parse_str;
                        parse(v).map($name).map_err(E::custom)
                    }

                    fn visit_u64<E: de::Error>(self, v: u64) -> Result<$name, E> {
                        let conv: fn(u64) -> Result<$inner, String> = $from_uint;
                        conv(v).map($name).map_err(E::custom)
                    }

                    fn visit_i64<E: de::Error>(self, v: i64) -> Result<$name, E> {
                        match u64::try_from(v) {
                            Ok(v) => self.visit_u64(v),
                            Err(_) => Err(E::custom("a negative value is not allowed")),
                        }
                    }
                }
                d.deserialize_any(V)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let ser: fn(&$inner) -> serde_json::Value = $ser;
                ser(&self.0).serialize(s)
            }
        }

        impl JsonSchema for $name {
            fn schema_name() -> Cow<'static, str> {
                Cow::Borrowed(stringify!($name))
            }

            fn inline_schema() -> bool {
                true
            }

            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                $schema
            }
        }
    };
}

scalar!(
    /// A duration written with a unit, such as `50ms`, `5s`, `10m`, `1h`, or `1d`.
    /// A bare number is rejected because its unit would be ambiguous.
    DurationSpec(Duration),
    "a duration with a unit, such as `50ms`, `5s` or `1h`",
    str: parse_duration,
    uint: |_| Err("a duration needs a unit, such as `50ms`, `5s` or `1h`".to_owned()),
    serialize: |d| serde_json::Value::String(humantime::format_duration(*d).to_string()),
    schema: json_schema!({
        "type": "string",
        "description": "A duration with a unit, such as `50ms`, `5s`, `10m`, `1h` or `1d`.",
        "examples": ["50ms", "5s", "1h"]
    })
);

scalar!(
    /// A size in bytes, written as `128MiB`, `2GiB`, or a plain integer of bytes.
    SizeSpec(u64),
    "a size such as `128MiB`, `2GiB` or a number of bytes",
    str: parse_size,
    uint: Ok,
    serialize: |n| serde_json::Value::from(*n),
    schema: json_schema!({
        "type": ["string", "integer"],
        "minimum": 0,
        "description": "A size such as `128MiB` or `2GiB`, or a plain number of bytes.",
        "examples": ["16MiB", "2GiB", 1048576]
    })
);

scalar!(
    /// A count, written as an integer or with a `k` (thousand) or `m` (million) suffix.
    CountSpec(u64),
    "a count such as `4`, `500k` or `2m`",
    str: parse_count,
    uint: Ok,
    serialize: |n| serde_json::Value::from(*n),
    schema: json_schema!({
        "type": ["string", "integer"],
        "minimum": 0,
        "description": "A count: an integer, or text with a `k` (thousand) or `m` (million) suffix.",
        "examples": [4, "500k"]
    })
);

/// A declared field type: `bool`, `i8` to `i64`, `u8` to `u64`, `f32`, `f64`,
/// `str`, `bytes`, `ts`, or `list<T>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldTypeSpec(pub FieldType);

impl FieldTypeSpec {
    /// The declared type, as defined in `thadam-core`.
    pub fn field_type(&self) -> &FieldType {
        &self.0
    }

    /// The canonical spelling, such as `list<f32>`.
    pub fn canonical(&self) -> String {
        type_name(&self.0)
    }
}

fn type_name(ty: &FieldType) -> String {
    match ty {
        FieldType::Bool => "bool".into(),
        FieldType::I8 => "i8".into(),
        FieldType::I16 => "i16".into(),
        FieldType::I32 => "i32".into(),
        FieldType::I64 => "i64".into(),
        FieldType::U8 => "u8".into(),
        FieldType::U16 => "u16".into(),
        FieldType::U32 => "u32".into(),
        FieldType::U64 => "u64".into(),
        FieldType::F32 => "f32".into(),
        FieldType::F64 => "f64".into(),
        FieldType::Str => "str".into(),
        FieldType::Bytes => "bytes".into(),
        FieldType::Ts => "ts".into(),
        FieldType::List(inner) => format!("list<{}>", type_name(inner)),
        _ => "unknown".into(),
    }
}

pub(crate) fn parse_field_type(text: &str) -> Result<FieldType, String> {
    fn go(t: &str, depth: usize) -> Result<FieldType, String> {
        let t = t.trim();
        if let Some(rest) = t.strip_prefix("list") {
            let inner = rest
                .trim_start()
                .strip_prefix('<')
                .and_then(|r| r.strip_suffix('>'))
                .ok_or_else(|| {
                    format!("invalid list type `{t}`: write `list<T>`, for example `list<f32>`")
                })?;
            if depth >= MAX_LIST_DEPTH {
                return Err(format!("list types nest at most {MAX_LIST_DEPTH} deep"));
            }
            return Ok(FieldType::List(Box::new(go(inner, depth + 1)?)));
        }
        Ok(match t {
            "bool" => FieldType::Bool,
            "i8" => FieldType::I8,
            "i16" => FieldType::I16,
            "i32" => FieldType::I32,
            "i64" => FieldType::I64,
            "u8" => FieldType::U8,
            "u16" => FieldType::U16,
            "u32" => FieldType::U32,
            "u64" => FieldType::U64,
            "f32" => FieldType::F32,
            "f64" => FieldType::F64,
            "str" | "string" => FieldType::Str,
            "bytes" => FieldType::Bytes,
            "ts" | "timestamp" => FieldType::Ts,
            "record" => {
                return Err("nested record types are not supported in config version 1".into());
            }
            other => {
                return Err(format!(
                    "unknown field type `{other}`, expected one of bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, str, bytes, ts, list<T>"
                ));
            }
        })
    }
    go(text, 0)
}

impl<'de> Deserialize<'de> for FieldTypeSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_field_type(&s)
            .map(FieldTypeSpec)
            .map_err(de::Error::custom)
    }
}

impl Serialize for FieldTypeSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.canonical())
    }
}

impl JsonSchema for FieldTypeSpec {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("FieldTypeSpec")
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": "^(bool|i8|i16|i32|i64|u8|u16|u32|u64|f32|f64|str|string|bytes|ts|timestamp|list<.+>)$",
            "description": "A declared field type: a scalar name or `list<T>`.",
            "examples": ["f32", "u64", "list<f64>"]
        })
    }
}

/// A time domain from `thadam-core`: `utc`, `tai`, `monotonic`, `sim`, or `unknown`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimeDomainSpec(pub TimeDomain);

impl TimeDomainSpec {
    /// The domain, as defined in `thadam-core`.
    pub fn domain(self) -> TimeDomain {
        self.0
    }
}

impl<'de> Deserialize<'de> for TimeDomainSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        TimeDomain::ALL
            .into_iter()
            .find(|dom| dom.as_str() == s)
            .map(TimeDomainSpec)
            .ok_or_else(|| {
                let names: Vec<_> = TimeDomain::ALL.iter().map(|d| format!("`{d}`")).collect();
                de::Error::custom(format!(
                    "unknown variant `{s}`, expected one of {}",
                    names.join(", ")
                ))
            })
    }
}

impl Serialize for TimeDomainSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.0.as_str())
    }
}

impl JsonSchema for TimeDomainSpec {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("TimeDomainSpec")
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        let names: Vec<&str> = TimeDomain::ALL.iter().map(|d| d.as_str()).collect();
        json_schema!({ "type": "string", "enum": names })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn de<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> Result<T, String> {
        serde_json::from_value(v).map_err(|e| e.to_string())
    }

    #[test]
    fn durations_sizes_counts_parse() {
        assert_eq!(
            de::<DurationSpec>("1h".into()).unwrap().get(),
            Duration::from_secs(3600)
        );
        assert_eq!(de::<SizeSpec>("2GiB".into()).unwrap().get(), 2 << 30);
        assert_eq!(de::<SizeSpec>(1024.into()).unwrap().get(), 1024);
        assert_eq!(de::<CountSpec>("500k".into()).unwrap().get(), 500_000);
        assert_eq!(de::<CountSpec>(7.into()).unwrap().get(), 7);
    }

    #[test]
    fn bare_numbers_are_not_durations() {
        let err = de::<DurationSpec>(5.into()).unwrap_err();
        assert!(err.contains("needs a unit"), "{err}");
        assert!(de::<DurationSpec>("1hr30".into()).is_err());
        assert!(de::<SizeSpec>(serde_json::json!(-1)).is_err());
        assert!(de::<CountSpec>("5oo".into()).is_err());
    }

    #[test]
    fn equal_values_serialize_identically() {
        let a = serde_json::to_value(de::<DurationSpec>("60m".into()).unwrap()).unwrap();
        let b = serde_json::to_value(de::<DurationSpec>("1h".into()).unwrap()).unwrap();
        assert_eq!(a, b);
        let c = serde_json::to_value(de::<SizeSpec>("1MiB".into()).unwrap()).unwrap();
        let d = serde_json::to_value(de::<SizeSpec>("1048576".into()).unwrap()).unwrap();
        assert_eq!(c, d);
    }

    #[test]
    fn field_types() {
        assert_eq!(parse_field_type("f32"), Ok(FieldType::F32));
        assert_eq!(
            parse_field_type("list<list<u8>>"),
            Ok(FieldType::List(Box::new(FieldType::List(Box::new(
                FieldType::U8
            )))))
        );
        assert_eq!(parse_field_type(" string "), Ok(FieldType::Str));
        assert!(parse_field_type("list<f32").is_err());
        assert!(parse_field_type("float").is_err());
        assert!(parse_field_type("record").is_err());
        assert_eq!(
            FieldTypeSpec(parse_field_type("list < f32 >").unwrap()).canonical(),
            "list<f32>"
        );
    }

    #[test]
    fn time_domains() {
        assert_eq!(
            de::<TimeDomainSpec>("utc".into()).unwrap().domain(),
            TimeDomain::Utc
        );
        assert!(de::<TimeDomainSpec>("local".into()).is_err());
    }
}
