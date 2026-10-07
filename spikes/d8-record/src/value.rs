//! Value type sets under test (decision D9).
//!
//! `min::Value`: i64 / f64 / str / list only.
//! `ext::Value`: narrow ints, f32, typed primitive arrays (`Arc<[T]>`).
//!
//! Both use `Str(Utf8Bytes)` with `Utf8Bytes(Bytes)`, exactly as in the plan
//! (implementation plan 5.1), so any difference between the two sets comes
//! from the numeric / array variants only.

use arrow_array::builder::Float64Builder;
use bytes::Bytes;
use serde_json::Value as Json;
use std::sync::Arc;

/// Declared column type (datasets use declared schemas, D12/D13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ty {
    I64,
    U32,
    U8,
    F32,
    F64,
    Str,
    ListF64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Utf8Bytes(Bytes);

impl Utf8Bytes {
    pub fn from_str_copy(s: &str) -> Self {
        Utf8Bytes(Bytes::copy_from_slice(s.as_bytes()))
    }
    pub fn as_str(&self) -> &str {
        // SAFETY: only constructed from &str, so always valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

pub trait Val: Clone + Send + Sync + 'static {
    const NAME: &'static str;
    fn from_json(j: &Json, ty: Ty) -> Self;
    fn as_i64(&self) -> Option<i64>;
    fn as_f64(&self) -> Option<f64>;
    fn as_str(&self) -> Option<&str>;
    /// In-place `x * scale + offset` on floats (scalar or array elements).
    fn convert(&mut self, scale: f64, offset: f64);
    /// True if a float scalar, or any element of a float array, exceeds `thr`.
    fn gt_any(&self, thr: f64) -> bool;
    /// Append array elements to an Arrow builder; false if not a float array.
    fn push_f64_list(&self, out: &mut Float64Builder) -> bool;
}

pub mod min {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    pub enum Value {
        Null,
        Bool(bool),
        I64(i64),
        F64(f64),
        Str(Utf8Bytes),
        List(Vec<Value>),
    }

    impl Val for Value {
        const NAME: &'static str = "min";

        fn from_json(j: &Json, ty: Ty) -> Self {
            match (j, ty) {
                (Json::Number(n), Ty::I64 | Ty::U32 | Ty::U8) => Value::I64(n.as_i64().unwrap_or(0)),
                (Json::Number(n), Ty::F32 | Ty::F64) => Value::F64(n.as_f64().unwrap_or(f64::NAN)),
                (Json::String(s), Ty::Str) => Value::Str(Utf8Bytes::from_str_copy(s)),
                (Json::Array(a), Ty::ListF64) => Value::List(
                    a.iter()
                        .map(|x| Value::F64(x.as_f64().unwrap_or(f64::NAN)))
                        .collect(),
                ),
                (Json::Bool(b), _) => Value::Bool(*b),
                _ => Value::Null,
            }
        }

        fn as_i64(&self) -> Option<i64> {
            match self {
                Value::I64(x) => Some(*x),
                _ => None,
            }
        }
        fn as_f64(&self) -> Option<f64> {
            match self {
                Value::F64(x) => Some(*x),
                Value::I64(x) => Some(*x as f64),
                _ => None,
            }
        }
        fn as_str(&self) -> Option<&str> {
            match self {
                Value::Str(s) => Some(s.as_str()),
                _ => None,
            }
        }
        fn convert(&mut self, scale: f64, offset: f64) {
            match self {
                Value::F64(x) => *x = *x * scale + offset,
                Value::List(v) => v.iter_mut().for_each(|e| e.convert(scale, offset)),
                _ => {}
            }
        }
        fn gt_any(&self, thr: f64) -> bool {
            match self {
                Value::F64(x) => *x > thr,
                Value::List(v) => v.iter().any(|e| e.gt_any(thr)),
                _ => false,
            }
        }
        fn push_f64_list(&self, out: &mut Float64Builder) -> bool {
            match self {
                Value::List(v) => {
                    for e in v {
                        match e {
                            Value::F64(x) => out.append_value(*x),
                            _ => out.append_null(),
                        }
                    }
                    true
                }
                _ => false,
            }
        }
    }
}

pub mod ext {
    use super::*;

    // Variants that no workload here constructs (I8, I16, U16, U64, Bytes, Ts,
    // F32s, I32s, I64s, nested Record) are omitted; none is wider than the
    // existing 32-byte payloads, so they would not change size_of::<Value>().
    #[derive(Clone, Debug, PartialEq)]
    pub enum Value {
        Null,
        Bool(bool),
        U8(u8),
        U32(u32),
        I64(i64),
        F32(f32),
        F64(f64),
        Str(Utf8Bytes),
        F64s(Arc<[f64]>),
        List(Vec<Value>),
    }

    impl Val for Value {
        const NAME: &'static str = "ext";

        fn from_json(j: &Json, ty: Ty) -> Self {
            match (j, ty) {
                (Json::Number(n), Ty::I64) => Value::I64(n.as_i64().unwrap_or(0)),
                (Json::Number(n), Ty::U32) => Value::U32(n.as_u64().unwrap_or(0) as u32),
                (Json::Number(n), Ty::U8) => Value::U8(n.as_u64().unwrap_or(0) as u8),
                (Json::Number(n), Ty::F32) => Value::F32(n.as_f64().unwrap_or(f64::NAN) as f32),
                (Json::Number(n), Ty::F64) => Value::F64(n.as_f64().unwrap_or(f64::NAN)),
                (Json::String(s), Ty::Str) => Value::Str(Utf8Bytes::from_str_copy(s)),
                (Json::Array(a), Ty::ListF64) => Value::F64s(
                    a.iter()
                        .map(|x| x.as_f64().unwrap_or(f64::NAN))
                        .collect::<Arc<[f64]>>(),
                ),
                (Json::Bool(b), _) => Value::Bool(*b),
                _ => Value::Null,
            }
        }

        fn as_i64(&self) -> Option<i64> {
            match self {
                Value::I64(x) => Some(*x),
                Value::U32(x) => Some(*x as i64),
                Value::U8(x) => Some(*x as i64),
                _ => None,
            }
        }
        fn as_f64(&self) -> Option<f64> {
            match self {
                Value::F64(x) => Some(*x),
                Value::F32(x) => Some(*x as f64),
                Value::I64(x) => Some(*x as f64),
                Value::U32(x) => Some(*x as f64),
                Value::U8(x) => Some(*x as f64),
                _ => None,
            }
        }
        fn as_str(&self) -> Option<&str> {
            match self {
                Value::Str(s) => Some(s.as_str()),
                _ => None,
            }
        }
        fn convert(&mut self, scale: f64, offset: f64) {
            match self {
                Value::F64(x) => *x = *x * scale + offset,
                Value::F32(x) => *x = (*x as f64 * scale + offset) as f32,
                Value::F64s(a) => match Arc::get_mut(a) {
                    Some(s) => s.iter_mut().for_each(|x| *x = *x * scale + offset),
                    None => {
                        *a = a.iter().map(|x| *x * scale + offset).collect::<Arc<[f64]>>();
                    }
                },
                Value::List(v) => v.iter_mut().for_each(|e| e.convert(scale, offset)),
                _ => {}
            }
        }
        fn gt_any(&self, thr: f64) -> bool {
            match self {
                Value::F64(x) => *x > thr,
                Value::F32(x) => (*x as f64) > thr,
                Value::F64s(a) => a.iter().any(|x| *x > thr),
                Value::List(v) => v.iter().any(|e| e.gt_any(thr)),
                _ => false,
            }
        }
        fn push_f64_list(&self, out: &mut Float64Builder) -> bool {
            match self {
                Value::F64s(a) => {
                    out.append_slice(a);
                    true
                }
                _ => false,
            }
        }
    }
}
