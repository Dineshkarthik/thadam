//! JSON decoding to declared types: bounded parsing, JSON Pointer extraction,
//! and coercion of JSON values to the declared field types.

use std::sync::Arc;

use thadam_core::{ErrorDomain, FieldType, Record, ThadamError, Timestamp, Value, codes};
use serde_json::Value as Json;

use crate::limits::ChainLimits;
use crate::paths::FieldPath;
use crate::timex::TimeParser;
use crate::units::UnitConversion;

/// The deepest nesting in `payload`, stopping early once it exceeds `limit`.
///
/// This is a byte scan that tracks strings, so it costs far less than the
/// parse and keeps a hostile payload from reaching the parser's own stack.
fn exceeds_depth(payload: &[u8], limit: usize) -> bool {
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for &b in payload {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > limit {
                    return true;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    false
}

/// Parses a payload as one JSON document within `limits`.
///
/// Errors never contain payload text.
pub(crate) fn parse_json(payload: &[u8], limits: &ChainLimits) -> Result<Json, ThadamError> {
    let decode_err =
        |code, message: &str| ThadamError::permanent(ErrorDomain::Decode, code, message.to_owned());
    if payload.len() > limits.max_payload_bytes {
        return Err(
            decode_err(codes::DECODE_TOO_LARGE, "payload exceeds the size limit")
                .with_context("limit", limits.max_payload_bytes.to_string()),
        );
    }
    if exceeds_depth(payload, limits.max_json_depth) {
        return Err(decode_err(
            codes::DECODE_DEPTH_EXCEEDED,
            "payload nests deeper than the limit",
        )
        .with_context("limit", limits.max_json_depth.to_string()));
    }
    serde_json::from_slice(payload).map_err(|e| {
        use serde_json::error::Category;
        if e.is_syntax() && std::str::from_utf8(payload).is_err() {
            return decode_err(codes::DECODE_INVALID_UTF8, "payload is not valid UTF-8");
        }
        let message = match e.classify() {
            Category::Eof => "payload is truncated",
            Category::Syntax => "payload is not valid JSON",
            Category::Data => "payload has unexpected data",
            Category::Io => "payload could not be read",
        };
        if e.to_string().starts_with("recursion limit") {
            return decode_err(
                codes::DECODE_DEPTH_EXCEEDED,
                "payload nests deeper than the parser allows",
            );
        }
        decode_err(codes::DECODE_INVALID_PAYLOAD, message)
            .with_context("line", e.line().to_string())
            .with_context("column", e.column().to_string())
    })
}

pub(crate) fn json_kind(j: &Json) -> &'static str {
    match j {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(n) if n.is_f64() => "float",
        Json::Number(_) => "integer",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

/// The spelling a decoded [`Value`] would have had after the ROS 2 record
/// bridge (`crate::ros2::record_to_json`), so direct record reads report
/// type mismatches in the same words. A `Ts` bridged to its nanoseconds
/// integer; `Bytes`, typed arrays, and `List` bridged to JSON arrays.
pub(crate) fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::I8(_)
        | Value::I16(_)
        | Value::I32(_)
        | Value::I64(_)
        | Value::U8(_)
        | Value::U16(_)
        | Value::U32(_)
        | Value::U64(_) => "integer",
        Value::F32(_) | Value::F64(_) => "float",
        Value::Str(_) => "string",
        Value::Ts(_) => "integer",
        Value::Bytes(_)
        | Value::F32s(_)
        | Value::F64s(_)
        | Value::I32s(_)
        | Value::I64s(_)
        | Value::List(_) => "array",
        Value::Record(_) => "object",
        // `Value` is non-exhaustive; a new variant reports a word rather
        // than failing to compile here.
        _ => "unknown",
    }
}

#[derive(Debug)]
pub(crate) enum CoerceError {
    Mismatch {
        expected: &'static str,
        found: &'static str,
    },
    OutOfRange(&'static str),
    NullElement,
}

impl CoerceError {
    pub(crate) fn into_error(self, field: &str) -> ThadamError {
        let message = match self {
            CoerceError::Mismatch { expected, found } => {
                format!("field `{field}` expects {expected}, found {found}")
            }
            CoerceError::OutOfRange(ty) => format!("field `{field}` is out of range for {ty}"),
            CoerceError::NullElement => format!("field `{field}` has a null element"),
        };
        ThadamError::permanent(ErrorDomain::Schema, codes::SCHEMA_TYPE_MISMATCH, message)
            .with_context("field", field)
    }
}

/// How one JSON scalar becomes a declared scalar.
#[derive(Clone, Debug)]
pub(crate) enum Scalar {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Str,
    Ts(TimeParser),
}

fn signed(j: &Json, ty: &'static str, min: i64, max: i64) -> Result<i64, CoerceError> {
    match j {
        Json::Number(n) => match n.as_i64() {
            Some(v) if (min..=max).contains(&v) => Ok(v),
            Some(_) => Err(CoerceError::OutOfRange(ty)),
            None if n.is_u64() => Err(CoerceError::OutOfRange(ty)),
            None => Err(CoerceError::Mismatch {
                expected: "integer",
                found: "float",
            }),
        },
        other => Err(CoerceError::Mismatch {
            expected: "integer",
            found: json_kind(other),
        }),
    }
}

fn unsigned(j: &Json, ty: &'static str, max: u64) -> Result<u64, CoerceError> {
    match j {
        Json::Number(n) => match n.as_u64() {
            Some(v) if v <= max => Ok(v),
            Some(_) => Err(CoerceError::OutOfRange(ty)),
            None if n.is_i64() => Err(CoerceError::OutOfRange(ty)),
            None => Err(CoerceError::Mismatch {
                expected: "integer",
                found: "float",
            }),
        },
        other => Err(CoerceError::Mismatch {
            expected: "integer",
            found: json_kind(other),
        }),
    }
}

fn float(j: &Json, conv: Option<&UnitConversion>) -> Result<f64, CoerceError> {
    match j {
        Json::Number(n) => {
            let x = n.as_f64().ok_or(CoerceError::OutOfRange("f64"))?;
            let x = conv.map_or(x, |c| c.apply_f64(x));
            if x.is_finite() {
                Ok(x)
            } else {
                Err(CoerceError::OutOfRange("a finite number"))
            }
        }
        other => Err(CoerceError::Mismatch {
            expected: "number",
            found: json_kind(other),
        }),
    }
}

fn f32_of(j: &Json, conv: Option<&UnitConversion>) -> Result<f32, CoerceError> {
    let x = float(j, conv)? as f32;
    if x.is_finite() {
        Ok(x)
    } else {
        Err(CoerceError::OutOfRange("f32"))
    }
}

impl Scalar {
    pub(crate) fn for_type(ty: &FieldType, ts: Option<TimeParser>) -> Option<Scalar> {
        Some(match ty {
            FieldType::Bool => Scalar::Bool,
            FieldType::I8 => Scalar::I8,
            FieldType::I16 => Scalar::I16,
            FieldType::I32 => Scalar::I32,
            FieldType::I64 => Scalar::I64,
            FieldType::U8 => Scalar::U8,
            FieldType::U16 => Scalar::U16,
            FieldType::U32 => Scalar::U32,
            FieldType::U64 => Scalar::U64,
            FieldType::F32 => Scalar::F32,
            FieldType::F64 => Scalar::F64,
            FieldType::Str => Scalar::Str,
            FieldType::Ts => Scalar::Ts(ts?),
            _ => return None,
        })
    }

    /// Converts `j`. Integer fields accept only JSON integers; float fields
    /// accept any number, converted in `f64` before rounding to `f32`.
    pub(crate) fn extract(
        &self,
        j: &Json,
        conv: Option<&UnitConversion>,
    ) -> Result<Value, CoerceError> {
        Ok(match self {
            Scalar::Bool => match j {
                Json::Bool(b) => Value::Bool(*b),
                other => {
                    return Err(CoerceError::Mismatch {
                        expected: "boolean",
                        found: json_kind(other),
                    });
                }
            },
            Scalar::I8 => Value::I8(signed(j, "i8", i64::from(i8::MIN), i64::from(i8::MAX))? as i8),
            Scalar::I16 => {
                Value::I16(signed(j, "i16", i64::from(i16::MIN), i64::from(i16::MAX))? as i16)
            }
            Scalar::I32 => {
                Value::I32(signed(j, "i32", i64::from(i32::MIN), i64::from(i32::MAX))? as i32)
            }
            Scalar::I64 => Value::I64(signed(j, "i64", i64::MIN, i64::MAX)?),
            Scalar::U8 => Value::U8(unsigned(j, "u8", u64::from(u8::MAX))? as u8),
            Scalar::U16 => Value::U16(unsigned(j, "u16", u64::from(u16::MAX))? as u16),
            Scalar::U32 => Value::U32(unsigned(j, "u32", u64::from(u32::MAX))? as u32),
            Scalar::U64 => Value::U64(unsigned(j, "u64", u64::MAX)?),
            Scalar::F32 => Value::F32(f32_of(j, conv)?),
            Scalar::F64 => Value::F64(float(j, conv)?),
            Scalar::Str => match j {
                Json::String(s) => Value::str(s.as_str()),
                other => {
                    return Err(CoerceError::Mismatch {
                        expected: "string",
                        found: json_kind(other),
                    });
                }
            },
            Scalar::Ts(parser) => parser
                .parse(j)
                .map(|ns| Value::Ts(Timestamp::from_nanos(ns)))
                .map_err(|issue| match issue {
                    crate::timex::TimeIssue::Type(found) => CoerceError::Mismatch {
                        expected: "a time",
                        found,
                    },
                    _ => CoerceError::OutOfRange("a timestamp"),
                })?,
        })
    }
}

fn signed_value(v: &Value, ty: &'static str, min: i64, max: i64) -> Result<i64, CoerceError> {
    if let Some(x) = v.as_i64() {
        return if (min..=max).contains(&x) {
            Ok(x)
        } else {
            Err(CoerceError::OutOfRange(ty))
        };
    }
    if v.as_u64().is_some() {
        return Err(CoerceError::OutOfRange(ty));
    }
    Err(CoerceError::Mismatch {
        expected: "integer",
        found: value_kind(v),
    })
}

fn unsigned_value(v: &Value, ty: &'static str, max: u64) -> Result<u64, CoerceError> {
    if let Some(x) = v.as_u64() {
        return if x <= max {
            Ok(x)
        } else {
            Err(CoerceError::OutOfRange(ty))
        };
    }
    if v.as_i64().is_some() {
        return Err(CoerceError::OutOfRange(ty));
    }
    Err(CoerceError::Mismatch {
        expected: "integer",
        found: value_kind(v),
    })
}

/// Applies the unit conversion like the JSON path: a finite input must stay
/// finite (the JSON path's `OutOfRange`), but a non-finite input passes
/// through — the ROS 2 record bridge spelled those as `null`, and the
/// direct read delivers them to the validation steps instead.
fn finite_unless_raw(raw: f64, out: f64) -> Result<f64, CoerceError> {
    if raw.is_finite() && !out.is_finite() {
        return Err(CoerceError::OutOfRange("a finite number"));
    }
    Ok(out)
}

fn float_value(v: &Value, conv: Option<&UnitConversion>) -> Result<f64, CoerceError> {
    let raw = match v {
        Value::F32(x) => Some(f64::from(*x)),
        Value::F64(x) => Some(*x),
        other => other
            .as_i64()
            .map(|i| i as f64)
            .or_else(|| other.as_u64().map(|u| u as f64)),
    };
    let Some(raw) = raw else {
        return Err(CoerceError::Mismatch {
            expected: "number",
            found: value_kind(v),
        });
    };
    finite_unless_raw(raw, conv.map_or(raw, |c| c.apply_f64(raw)))
}

fn raw_non_finite(v: &Value) -> bool {
    matches!(v, Value::F32(x) if !x.is_finite()) || matches!(v, Value::F64(x) if !x.is_finite())
}

fn f32_of_value(v: &Value, conv: Option<&UnitConversion>) -> Result<f32, CoerceError> {
    let out = float_value(v, conv)? as f32;
    if out.is_finite() || raw_non_finite(v) {
        Ok(out)
    } else {
        Err(CoerceError::OutOfRange("f32"))
    }
}

impl Scalar {
    /// The direct-read counterpart of [`Scalar::extract`]: coerces a decoded
    /// record's [`Value`]. Acceptance and error kinds match the JSON path;
    /// the one intentional difference is that non-finite floats pass
    /// through (the ROS 2 bridge spelled them as `null`).
    pub(crate) fn extract_value(
        &self,
        v: &Value,
        conv: Option<&UnitConversion>,
    ) -> Result<Value, CoerceError> {
        Ok(match self {
            Scalar::Bool => match v {
                Value::Bool(b) => Value::Bool(*b),
                other => {
                    return Err(CoerceError::Mismatch {
                        expected: "boolean",
                        found: value_kind(other),
                    });
                }
            },
            Scalar::I8 => {
                Value::I8(signed_value(v, "i8", i64::from(i8::MIN), i64::from(i8::MAX))? as i8)
            }
            Scalar::I16 => {
                Value::I16(signed_value(v, "i16", i64::from(i16::MIN), i64::from(i16::MAX))? as i16)
            }
            Scalar::I32 => {
                Value::I32(signed_value(v, "i32", i64::from(i32::MIN), i64::from(i32::MAX))? as i32)
            }
            Scalar::I64 => Value::I64(signed_value(v, "i64", i64::MIN, i64::MAX)?),
            Scalar::U8 => Value::U8(unsigned_value(v, "u8", u64::from(u8::MAX))? as u8),
            Scalar::U16 => Value::U16(unsigned_value(v, "u16", u64::from(u16::MAX))? as u16),
            Scalar::U32 => Value::U32(unsigned_value(v, "u32", u64::from(u32::MAX))? as u32),
            Scalar::U64 => Value::U64(unsigned_value(v, "u64", u64::MAX)?),
            Scalar::F32 => Value::F32(f32_of_value(v, conv)?),
            Scalar::F64 => Value::F64(float_value(v, conv)?),
            Scalar::Str => match v {
                Value::Str(s) => Value::Str(s.clone()),
                other => {
                    return Err(CoerceError::Mismatch {
                        expected: "string",
                        found: value_kind(other),
                    });
                }
            },
            Scalar::Ts(parser) => parser
                .parse_value(v)
                .map(|ns| Value::Ts(Timestamp::from_nanos(ns)))
                .map_err(|issue| match issue {
                    crate::timex::TimeIssue::Type(found) => CoerceError::Mismatch {
                        expected: "a time",
                        found,
                    },
                    _ => CoerceError::OutOfRange("a timestamp"),
                })?,
        })
    }
}

fn collect_value<T>(
    items: &[Value],
    mut f: impl FnMut(&Value) -> Result<T, CoerceError>,
) -> Result<Arc<[T]>, CoerceError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        if item.is_null() {
            return Err(CoerceError::NullElement);
        }
        out.push(f(item)?);
    }
    Ok(Arc::from(out))
}

/// A scalar or a list of scalars. Lists of `f32`, `f64`, `i32`, and `i64`
/// decode straight to typed arrays.
#[derive(Clone, Debug)]
pub(crate) enum Coerce {
    Scalar(Scalar),
    List(Scalar),
}

impl Coerce {
    pub(crate) fn for_type(ty: &FieldType, ts: Option<TimeParser>) -> Option<Coerce> {
        match ty {
            FieldType::List(inner) => Scalar::for_type(inner, ts).map(Coerce::List),
            other => Scalar::for_type(other, ts).map(Coerce::Scalar),
        }
    }

    pub(crate) fn extract(
        &self,
        j: &Json,
        conv: Option<&UnitConversion>,
    ) -> Result<Value, CoerceError> {
        let elem = match self {
            Coerce::Scalar(s) => return s.extract(j, conv),
            Coerce::List(elem) => elem,
        };
        let Json::Array(items) = j else {
            return Err(CoerceError::Mismatch {
                expected: "array",
                found: json_kind(j),
            });
        };
        fn collect<T>(
            items: &[Json],
            mut f: impl FnMut(&Json) -> Result<T, CoerceError>,
        ) -> Result<Arc<[T]>, CoerceError> {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                if item.is_null() {
                    return Err(CoerceError::NullElement);
                }
                out.push(f(item)?);
            }
            Ok(Arc::from(out))
        }
        Ok(match elem {
            Scalar::F32 => Value::F32s(collect(items, |j| f32_of(j, conv))?),
            Scalar::F64 => Value::F64s(collect(items, |j| float(j, conv))?),
            Scalar::I32 => Value::I32s(collect(items, |j| {
                signed(j, "i32", i64::from(i32::MIN), i64::from(i32::MAX)).map(|v| v as i32)
            })?),
            Scalar::I64 => Value::I64s(collect(items, |j| signed(j, "i64", i64::MIN, i64::MAX))?),
            other => Value::List(
                items
                    .iter()
                    .map(|item| {
                        if item.is_null() {
                            Ok(Value::Null)
                        } else {
                            other.extract(item, None)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        })
    }
}

/// One declared field's extraction plan.
#[derive(Clone, Debug)]
pub(crate) struct FieldPlan {
    pub(crate) name: Arc<str>,
    pub(crate) path: FieldPath,
    pub(crate) coerce: Coerce,
    pub(crate) conv: Option<UnitConversion>,
    pub(crate) required: bool,
}

impl Coerce {
    /// The direct-read counterpart of [`Coerce::extract`]: coerces a decoded
    /// record's [`Value`]. A typed array whose target matches exactly
    /// shares its storage; anything else converts element-wise, exactly as
    /// the JSON path would have read the bridge (`Bytes` spelled an int
    /// array, typed arrays spelled number arrays).
    pub(crate) fn extract_value(
        &self,
        v: &Value,
        conv: Option<&UnitConversion>,
    ) -> Result<Value, CoerceError> {
        fn not_array(v: &Value) -> CoerceError {
            CoerceError::Mismatch {
                expected: "array",
                found: value_kind(v),
            }
        }
        /// Float typed arrays against an integer target mismatch the way a
        /// JSON array of floats does (the first element's kind).
        fn float_array(v: &Value) -> Option<CoerceError> {
            matches!(v, Value::F32s(_) | Value::F64s(_)).then(|| CoerceError::Mismatch {
                expected: "integer",
                found: "float",
            })
        }
        let elem = match self {
            Coerce::Scalar(s) => return s.extract_value(v, conv),
            Coerce::List(elem) => elem,
        };
        Ok(match elem {
            Scalar::F32 => {
                let one = |x: f64| -> Result<f32, CoerceError> {
                    let out = finite_unless_raw(x, conv.map_or(x, |c| c.apply_f64(x)))? as f32;
                    if out.is_finite() || !x.is_finite() {
                        Ok(out)
                    } else {
                        Err(CoerceError::OutOfRange("f32"))
                    }
                };
                match v {
                    Value::F32s(_) if conv.is_none() => v.clone(),
                    Value::F32s(a) => Value::F32s(
                        a.iter()
                            .map(|x| one(f64::from(*x)))
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::F64s(a) => {
                        Value::F32s(a.iter().map(|x| one(*x)).collect::<Result<_, _>>()?)
                    }
                    Value::I32s(a) => Value::F32s(
                        a.iter()
                            .map(|x| one(f64::from(*x)))
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::I64s(a) => {
                        Value::F32s(a.iter().map(|x| one(*x as f64)).collect::<Result<_, _>>()?)
                    }
                    Value::Bytes(b) => Value::F32s(
                        b.iter()
                            .map(|x| one(f64::from(*x)))
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::List(items) => {
                        Value::F32s(collect_value(items, |item| f32_of_value(item, conv))?)
                    }
                    other => return Err(not_array(other)),
                }
            }
            Scalar::F64 => {
                let one = |x: f64| finite_unless_raw(x, conv.map_or(x, |c| c.apply_f64(x)));
                match v {
                    Value::F64s(_) if conv.is_none() => v.clone(),
                    Value::F64s(a) => {
                        Value::F64s(a.iter().map(|x| one(*x)).collect::<Result<_, _>>()?)
                    }
                    Value::F32s(a) => Value::F64s(
                        a.iter()
                            .map(|x| one(f64::from(*x)))
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::I32s(a) => Value::F64s(
                        a.iter()
                            .map(|x| one(f64::from(*x)))
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::I64s(a) => {
                        Value::F64s(a.iter().map(|x| one(*x as f64)).collect::<Result<_, _>>()?)
                    }
                    Value::Bytes(b) => Value::F64s(
                        b.iter()
                            .map(|x| one(f64::from(*x)))
                            .collect::<Result<_, _>>()?,
                    ),
                    Value::List(items) => {
                        Value::F64s(collect_value(items, |item| float_value(item, conv))?)
                    }
                    other => return Err(not_array(other)),
                }
            }
            Scalar::I32 => match v {
                Value::I32s(_) => v.clone(),
                Value::I64s(a) => Value::I32s(
                    a.iter()
                        .map(|x| i32::try_from(*x).map_err(|_| CoerceError::OutOfRange("i32")))
                        .collect::<Result<_, _>>()?,
                ),
                Value::Bytes(b) => Value::I32s(b.iter().map(|x| i32::from(*x)).collect()),
                Value::List(items) => Value::I32s(collect_value(items, |item| {
                    signed_value(item, "i32", i64::from(i32::MIN), i64::from(i32::MAX))
                        .map(|x| x as i32)
                })?),
                other => return Err(float_array(other).unwrap_or_else(|| not_array(other))),
            },
            Scalar::I64 => match v {
                Value::I64s(_) => v.clone(),
                Value::I32s(a) => Value::I64s(a.iter().map(|x| i64::from(*x)).collect()),
                Value::Bytes(b) => Value::I64s(b.iter().map(|x| i64::from(*x)).collect()),
                Value::List(items) => Value::I64s(collect_value(items, |item| {
                    signed_value(item, "i64", i64::MIN, i64::MAX)
                })?),
                other => return Err(float_array(other).unwrap_or_else(|| not_array(other))),
            },
            other_scalar => {
                let items: Vec<Value> = match v {
                    Value::List(items) => items
                        .iter()
                        .map(|item| {
                            if item.is_null() {
                                Ok(Value::Null)
                            } else {
                                other_scalar.extract_value(item, None)
                            }
                        })
                        .collect::<Result<_, _>>()?,
                    Value::Bytes(b) => b
                        .iter()
                        .map(|x| other_scalar.extract_value(&Value::U8(*x), None))
                        .collect::<Result<_, _>>()?,
                    Value::I32s(a) => a
                        .iter()
                        .map(|x| other_scalar.extract_value(&Value::I32(*x), None))
                        .collect::<Result<_, _>>()?,
                    Value::I64s(a) => a
                        .iter()
                        .map(|x| other_scalar.extract_value(&Value::I64(*x), None))
                        .collect::<Result<_, _>>()?,
                    Value::F32s(a) => a
                        .iter()
                        .map(|x| other_scalar.extract_value(&Value::F32(*x), None))
                        .collect::<Result<_, _>>()?,
                    Value::F64s(a) => a
                        .iter()
                        .map(|x| other_scalar.extract_value(&Value::F64(*x), None))
                        .collect::<Result<_, _>>()?,
                    other => return Err(not_array(other)),
                };
                Value::List(items)
            }
        })
    }
}

impl FieldPlan {
    pub(crate) fn extract(&self, scope: &Json, root: &Json) -> Result<Value, ThadamError> {
        match self.path.resolve(scope, root) {
            None | Some(Json::Null) => {
                if self.required {
                    Err(ThadamError::permanent(
                        ErrorDomain::Schema,
                        codes::SCHEMA_MISSING_FIELD,
                        format!("required field `{}` is missing", self.name),
                    )
                    .with_context("field", &*self.name))
                } else {
                    Ok(Value::Null)
                }
            }
            Some(j) => self
                .coerce
                .extract(j, self.conv.as_ref())
                .map_err(|e| e.into_error(&self.name)),
        }
    }

    /// Direct read from a decoded record, for `decode: ros2` datasets whose
    /// pointer is a single token (the flattened dotted spelling): resolves
    /// `path` to one field lookup and coerces without the JSON bridge.
    /// `scope` is `None` for exploded samples that are not nested records,
    /// which resolves like a JSON scalar scope: nothing is found.
    pub(crate) fn extract_record(
        &self,
        scope: Option<&Record>,
        root: &Record,
    ) -> Result<Value, ThadamError> {
        let record = if self.path.is_parent() {
            Some(root)
        } else {
            scope
        };
        let Some(token) = self.path.sole_token() else {
            return Err(ThadamError::fatal(
                ErrorDomain::Config,
                codes::CONFIG_MISSING,
                "a direct-read field plan is one pointer token (the compile gate missed it)",
            ));
        };
        match record.and_then(|r| r.get(token)) {
            None | Some(Value::Null) => {
                if self.required {
                    Err(ThadamError::permanent(
                        ErrorDomain::Schema,
                        codes::SCHEMA_MISSING_FIELD,
                        format!("required field `{}` is missing", self.name),
                    )
                    .with_context("field", &*self.name))
                } else {
                    Ok(Value::Null)
                }
            }
            Some(v) => self
                .coerce
                .extract_value(v, self.conv.as_ref())
                .map_err(|e| e.into_error(&self.name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn limits() -> ChainLimits {
        ChainLimits::default()
    }

    fn scalar(ty: &str) -> Coerce {
        Coerce::for_type(&crate::spec::parse_field_type(ty).unwrap(), None).unwrap()
    }

    #[test]
    fn depth_is_bounded_before_parsing() {
        let mut l = limits();
        l.max_json_depth = 4;
        assert!(parse_json(b"[[[[1]]]]", &l).is_ok());
        let err = parse_json(b"[[[[[1]]]]]", &l).unwrap_err();
        assert_eq!(err.code, codes::DECODE_DEPTH_EXCEEDED);
        let deep = "[".repeat(100_000);
        let err = parse_json(deep.as_bytes(), &l).unwrap_err();
        assert_eq!(err.code, codes::DECODE_DEPTH_EXCEEDED);
    }

    #[test]
    fn brackets_inside_strings_do_not_count() {
        let mut l = limits();
        l.max_json_depth = 1;
        let ok = br#"{"a":"[[[[[[[","b":"\"[[[["}"#;
        assert!(parse_json(ok, &l).is_ok());
    }

    #[test]
    fn size_and_syntax_and_utf8_have_distinct_codes() {
        let mut l = limits();
        l.max_payload_bytes = 8;
        assert_eq!(
            parse_json(b"[1,2,3,4,5,6]", &l).unwrap_err().code,
            codes::DECODE_TOO_LARGE
        );
        let l = limits();
        assert_eq!(
            parse_json(b"{", &l).unwrap_err().code,
            codes::DECODE_INVALID_PAYLOAD
        );
        assert_eq!(
            parse_json(b"nope", &l).unwrap_err().code,
            codes::DECODE_INVALID_PAYLOAD
        );
        assert_eq!(
            parse_json(b"", &l).unwrap_err().code,
            codes::DECODE_INVALID_PAYLOAD
        );
        assert_eq!(
            parse_json(b"{\"a\":\"\xff\xfe\"}", &l).unwrap_err().code,
            codes::DECODE_INVALID_UTF8
        );
    }

    #[test]
    fn errors_never_contain_payload_text() {
        let err = parse_json(b"{\"secret\": tru}", &limits()).unwrap_err();
        assert!(!format!("{err:?}").contains("secret"));
    }

    #[test]
    fn integers_are_strict_floats_are_lenient() {
        assert_eq!(
            scalar("i64").extract(&json!(5), None).unwrap(),
            Value::I64(5)
        );
        assert!(matches!(
            scalar("i64").extract(&json!(5.0), None),
            Err(CoerceError::Mismatch { .. })
        ));
        assert!(matches!(
            scalar("u8").extract(&json!(256), None),
            Err(CoerceError::OutOfRange("u8"))
        ));
        assert!(matches!(
            scalar("u8").extract(&json!(-1), None),
            Err(CoerceError::OutOfRange("u8"))
        ));
        assert_eq!(
            scalar("u64").extract(&json!(u64::MAX), None).unwrap(),
            Value::U64(u64::MAX)
        );
        assert!(matches!(
            scalar("i64").extract(&json!(u64::MAX), None),
            Err(CoerceError::OutOfRange("i64"))
        ));
        assert_eq!(
            scalar("f64").extract(&json!(3), None).unwrap(),
            Value::F64(3.0)
        );
        assert_eq!(
            scalar("f32").extract(&json!(81.5), None).unwrap(),
            Value::F32(81.5)
        );
        assert!(matches!(
            scalar("f32").extract(&json!(1e300), None),
            Err(CoerceError::OutOfRange("f32"))
        ));
        assert!(scalar("str").extract(&json!(1), None).is_err());
        assert!(scalar("bool").extract(&json!("true"), None).is_err());
    }

    #[test]
    fn lists_decode_to_typed_arrays() {
        let v = scalar("list<f32>")
            .extract(&json!([1, 2.5, -3]), None)
            .unwrap();
        assert_eq!(v.as_f32s(), Some(&[1.0f32, 2.5, -3.0][..]));
        let v = scalar("list<i64>").extract(&json!([1, 2]), None).unwrap();
        assert_eq!(v.as_i64s(), Some(&[1i64, 2][..]));
        assert!(matches!(
            scalar("list<f32>").extract(&json!([1, null]), None),
            Err(CoerceError::NullElement)
        ));
        let v = scalar("list<str>")
            .extract(&json!(["a", null]), None)
            .unwrap();
        assert_eq!(v.as_list().map(<[Value]>::len), Some(2));
        assert!(scalar("list<f32>").extract(&json!({}), None).is_err());
    }

    #[test]
    fn unit_conversion_runs_before_rounding_to_f32() {
        let conv = UnitConversion::from_codes("dCel", "Cel").unwrap();
        let v = scalar("f32").extract(&json!(612), Some(&conv)).unwrap();
        assert_eq!(v, Value::F32(61.2));
        let v = scalar("list<f32>")
            .extract(&json!([412, 398, 455]), Some(&conv))
            .unwrap();
        assert_eq!(v.as_f32s(), Some(&[41.2f32, 39.8, 45.5][..]));
    }

    #[test]
    fn missing_required_field_is_a_schema_error() {
        let plan = FieldPlan {
            name: Arc::from("seq"),
            path: FieldPath::parse("/seq").unwrap(),
            coerce: scalar("u64"),
            conv: None,
            required: true,
        };
        let root = json!({});
        let err = plan.extract(&root, &root).unwrap_err();
        assert_eq!(err.code, codes::SCHEMA_MISSING_FIELD);
        let root = json!({ "seq": null });
        assert!(plan.extract(&root, &root).is_err());
        let optional = FieldPlan {
            required: false,
            ..plan
        };
        assert_eq!(optional.extract(&root, &root).unwrap(), Value::Null);
    }

    /// A one-field record whose only field carries `value`, and its JSON
    /// bridge spelling — the inputs the two coercions see for the same
    /// decoded message.
    fn bridged(value: &Value) -> (Value, Json) {
        use thadam_core::{FieldDef, Shape, ShapeRef};

        let ty = match value {
            Value::Bool(_) => FieldType::Bool,
            Value::I8(_) => FieldType::I8,
            Value::I16(_) => FieldType::I16,
            Value::I32(_) => FieldType::I32,
            Value::I64(_) => FieldType::I64,
            Value::U8(_) => FieldType::U8,
            Value::U16(_) => FieldType::U16,
            Value::U32(_) => FieldType::U32,
            Value::U64(_) => FieldType::U64,
            Value::F32(_) => FieldType::F32,
            Value::F64(_) => FieldType::F64,
            Value::Str(_) => FieldType::Str,
            Value::Bytes(_) => FieldType::Bytes,
            Value::Ts(_) => FieldType::Ts,
            Value::F32s(_) => FieldType::List(Box::new(FieldType::F32)),
            Value::F64s(_) => FieldType::List(Box::new(FieldType::F64)),
            Value::I32s(_) => FieldType::List(Box::new(FieldType::I32)),
            Value::I64s(_) => FieldType::List(Box::new(FieldType::I64)),
            Value::List(_) | Value::Record(_) | Value::Null => FieldType::Str,
            _ => unimplemented!("test table only"),
        };
        let shape = ShapeRef::new(Shape::new(vec![FieldDef::new("f", ty)]).unwrap());
        let record = Record::new(shape, vec![value.clone()]).unwrap();
        let root = crate::ros2::record_to_json(&record);
        let json = root.pointer("/f").expect("the bridge writes /f").clone();
        (value.clone(), json)
    }

    /// The result with values/typed-arrays and error kinds only, so the two
    /// paths compare equal even where element order inside an error differs.
    fn outcome(r: Result<Value, CoerceError>) -> String {
        format!("{r:?}")
    }

    #[test]
    fn extract_value_matches_the_json_bridge_for_every_variant() {
        let rows: &[(&str, Value)] = &[
            ("bool", Value::Bool(true)),
            ("i8", Value::I32(-5)),
            ("i16", Value::U32(300)),
            ("i32", Value::I64(i64::from(i32::MAX))),
            ("i64", Value::U32(7)),
            ("u8", Value::U64(255)),
            ("u16", Value::I8(-1)), // out of range, both
            ("u32", Value::U64(u64::from(u32::MAX))),
            ("u64", Value::U64(u64::MAX)),
            ("i64", Value::U64(u64::MAX)), // out of range, both
            ("u8", Value::F64(3.0)),       // float to int mismatch, both
            ("f64", Value::I64(5)),
            ("f32", Value::U64(8)),
            ("f32", Value::F64(1e300)), // out of range for f32, both
            ("str", Value::str("text")),
            ("str", Value::I32(1)),       // mismatch, both
            ("bool", Value::str("true")), // mismatch, both
            ("list<f64>", Value::from(vec![0.1, -2.5])),
            ("list<f32>", Value::from(vec![0.1, -2.5])),
            ("list<f64>", Value::from(vec![0.25f32, -1.5])),
            ("list<i64>", Value::from(vec![1i32, -2])),
            ("list<i32>", Value::from(vec![1i64, -2])),
            ("list<i32>", Value::from(vec![i64::from(i32::MAX) + 1])),
            ("list<i32>", Value::from(vec![1.0f64])), // float array mismatch
            ("list<f64>", Value::from(vec![1i32, 2])),
            (
                "list<u8>",
                Value::bytes(bytes::Bytes::from_static(&[1, 2, 250])),
            ),
            ("list<i32>", Value::bytes(bytes::Bytes::from_static(&[9]))),
            (
                "list<str>",
                Value::List(vec![Value::from("a"), Value::Null]),
            ),
            ("list<u16>", Value::List(vec![Value::U32(3)])),
            (
                "list<str>",
                Value::List(vec![Value::from("x"), Value::I32(3)]),
            ),
        ];
        for (decl, value) in rows {
            let (value, json) = bridged(value);
            let coerce = scalar(decl);
            let via_json = coerce.extract(&json, None);
            let direct = coerce.extract_value(&value, None);
            assert_eq!(outcome(direct), outcome(via_json), "{decl} from {value:?}");
        }
    }

    #[test]
    fn extract_value_timestamps_match_the_bridge() {
        use crate::timeparse::EpochUnit;
        use crate::timex::TimeParser;

        let stamp = Value::Ts(Timestamp::from_nanos(1_758_000_000_123_456_789));
        let (_, json) = bridged(&stamp);
        let ns = Coerce::Scalar(Scalar::Ts(TimeParser::Epoch(EpochUnit::Nanos)));
        assert_eq!(
            outcome(ns.extract_value(&stamp, None)),
            outcome(ns.extract(&json, None))
        );
        // Another epoch unit reads the same nanosecond number on both paths.
        let secs = Coerce::Scalar(Scalar::Ts(TimeParser::Epoch(EpochUnit::Seconds)));
        assert_eq!(
            outcome(secs.extract_value(&stamp, None)),
            outcome(secs.extract(&json, None))
        );

        let text = Value::str("1970-01-01T00:00:01Z");
        let (_, json) = bridged(&text);
        let rfc = Coerce::Scalar(Scalar::Ts(TimeParser::Rfc3339(None)));
        assert_eq!(
            outcome(rfc.extract_value(&text, None)),
            outcome(rfc.extract(&json, None))
        );
        // A wrong-shaped value errors with the same kind words.
        let bad = Value::U32(3);
        let (_, json) = bridged(&bad);
        assert_eq!(
            outcome(rfc.extract_value(&bad, None)),
            outcome(rfc.extract(&json, None))
        );
    }

    #[test]
    fn the_bridge_losses_stay_whole_values_on_the_direct_read() {
        // Non-finite floats: the bridge spelled them as `null`; the direct
        // read keeps them so the validation steps can flag them.
        let f_nan = scalar("f64");
        assert_eq!(
            f_nan.extract_value(&Value::F64(f64::NAN), None).unwrap(),
            Value::F64(f64::NAN)
        );
        let f_inf = scalar("f32");
        assert_eq!(
            f_inf
                .extract_value(&Value::F32(f32::NEG_INFINITY), None)
                .unwrap(),
            Value::F32(f32::NEG_INFINITY)
        );
        let list = scalar("list<f64>");
        let array = Value::from(vec![0.1, f64::NAN, f64::INFINITY]);
        let direct = list.extract_value(&array, None).unwrap();
        let bits: Vec<u64> = direct
            .as_f64s()
            .unwrap()
            .iter()
            .map(|x| x.to_bits())
            .collect();
        assert_eq!(
            bits,
            [
                0.1f64.to_bits(),
                f64::NAN.to_bits(),
                f64::INFINITY.to_bits()
            ]
        );
        // ...where the bridge's JSON spelling is a null element, an error.
        let (_, json) = bridged(&array);
        assert!(matches!(
            list.extract(&json, None),
            Err(CoerceError::NullElement)
        ));

        // A finite value the unit conversion overflows still errors, like
        // the JSON path.
        let conv = UnitConversion::from_codes("V", "mV").unwrap();
        let overflow = scalar("f64");
        assert!(matches!(
            overflow.extract_value(&Value::F64(1e306), Some(&conv)),
            Err(CoerceError::OutOfRange("a finite number"))
        ));
    }
}
