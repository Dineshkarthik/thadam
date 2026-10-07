//! JSON decoding to declared types: bounded parsing, JSON Pointer extraction,
//! and coercion of JSON values to the declared field types.

use std::sync::Arc;

use thadam_core::{ErrorDomain, FieldType, ThadamError, Timestamp, Value, codes};
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
}
