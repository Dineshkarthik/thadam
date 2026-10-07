//! The extended value set (D9): narrow scalars, typed primitive arrays, lists,
//! and nested records.

use std::hash::{Hash, Hasher};
use std::mem::discriminant;
use std::sync::Arc;

use bytes::Bytes;

use crate::record::Record;
use crate::text::Utf8Bytes;
use crate::time::Timestamp;

/// A dynamically typed field value.
///
/// `size_of::<Value>()` is 24 bytes: payloads wider than 16 bytes are boxed
/// (`Bytes`, nested `Record`), and `Str` is a thin-pointer string (see the
/// crate docs for the layout evaluation).
///
/// # Equality
///
/// `PartialEq`, `Eq`, and `Hash` compare floats by bit pattern, so `NaN` equals
/// itself and `0.0` differs from `-0.0`. That makes equality reflexive, which
/// replay-equivalence and deduplication tests need; use the numeric accessors
/// for IEEE comparisons. Values of different variants are never equal, even
/// when numerically equal (`I32(1) != I64(1)`).
///
/// # Typed arrays
///
/// `F32s`, `F64s`, `I32s`, and `I64s` share their storage on clone. In-place
/// edits need unique ownership: the `*_mut` accessors copy a shared array first
/// (copy on write), and the `*_unique_mut` accessors refuse instead. The normal
/// pipeline moves events, so arrays stay unique and edits stay in place.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Value {
    /// An absent value.
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed 8-bit integer.
    I8(i8),
    /// A signed 16-bit integer.
    I16(i16),
    /// A signed 32-bit integer.
    I32(i32),
    /// A signed 64-bit integer.
    I64(i64),
    /// An unsigned 8-bit integer.
    U8(u8),
    /// An unsigned 16-bit integer.
    U16(u16),
    /// An unsigned 32-bit integer.
    U32(u32),
    /// An unsigned 64-bit integer.
    U64(u64),
    /// A 32-bit float.
    F32(f32),
    /// A 64-bit float.
    F64(f64),
    /// A validated UTF-8 string.
    Str(Utf8Bytes),
    /// Binary data. Boxed to keep `Value` small; build with [`Value::bytes`].
    Bytes(Box<Bytes>),
    /// A timestamp.
    Ts(Timestamp),
    /// A shared array of `f32`.
    F32s(Arc<[f32]>),
    /// A shared array of `f64`.
    F64s(Arc<[f64]>),
    /// A shared array of `i32`.
    I32s(Arc<[i32]>),
    /// A shared array of `i64`.
    I64s(Arc<[i64]>),
    /// A heterogeneous or nested list.
    List(Vec<Value>),
    /// A nested record. Boxed to keep `Value` small; build with [`Value::record`].
    Record(Box<Record>),
}

/// The type of a [`Value`] without its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ValueType {
    /// [`Value::Null`].
    Null,
    /// [`Value::Bool`].
    Bool,
    /// [`Value::I8`].
    I8,
    /// [`Value::I16`].
    I16,
    /// [`Value::I32`].
    I32,
    /// [`Value::I64`].
    I64,
    /// [`Value::U8`].
    U8,
    /// [`Value::U16`].
    U16,
    /// [`Value::U32`].
    U32,
    /// [`Value::U64`].
    U64,
    /// [`Value::F32`].
    F32,
    /// [`Value::F64`].
    F64,
    /// [`Value::Str`].
    Str,
    /// [`Value::Bytes`].
    Bytes,
    /// [`Value::Ts`].
    Ts,
    /// [`Value::F32s`].
    F32s,
    /// [`Value::F64s`].
    F64s,
    /// [`Value::I32s`].
    I32s,
    /// [`Value::I64s`].
    I64s,
    /// [`Value::List`].
    List,
    /// [`Value::Record`].
    Record,
}

impl ValueType {
    /// The integer's signedness and width in bits, for integer types.
    fn int_info(self) -> Option<(bool, u8)> {
        Some(match self {
            ValueType::I8 => (true, 8),
            ValueType::I16 => (true, 16),
            ValueType::I32 => (true, 32),
            ValueType::I64 => (true, 64),
            ValueType::U8 => (false, 8),
            ValueType::U16 => (false, 16),
            ValueType::U32 => (false, 32),
            ValueType::U64 => (false, 64),
            _ => return None,
        })
    }

    /// Whether every value of `self` converts losslessly to `target`.
    ///
    /// The rules are widening only: signed to a wider signed integer, unsigned
    /// to a wider unsigned or wider signed integer, `F32` to `F64`, `F32s` to
    /// `F64s`, `I32s` to `I64s`, and `Null` to anything. A type widens to
    /// itself. Nothing narrows, and integers never become floats.
    pub fn widens_to(self, target: ValueType) -> bool {
        if self == target || self == ValueType::Null {
            return true;
        }
        match (self.int_info(), target.int_info()) {
            (Some((from_signed, from_bits)), Some((to_signed, to_bits))) => {
                to_bits > from_bits && (from_signed == to_signed || (!from_signed && to_signed))
            }
            _ => matches!(
                (self, target),
                (ValueType::F32, ValueType::F64)
                    | (ValueType::F32s, ValueType::F64s)
                    | (ValueType::I32s, ValueType::I64s)
            ),
        }
    }

    /// The stable lowercase name.
    pub const fn as_str(self) -> &'static str {
        match self {
            ValueType::Null => "null",
            ValueType::Bool => "bool",
            ValueType::I8 => "i8",
            ValueType::I16 => "i16",
            ValueType::I32 => "i32",
            ValueType::I64 => "i64",
            ValueType::U8 => "u8",
            ValueType::U16 => "u16",
            ValueType::U32 => "u32",
            ValueType::U64 => "u64",
            ValueType::F32 => "f32",
            ValueType::F64 => "f64",
            ValueType::Str => "str",
            ValueType::Bytes => "bytes",
            ValueType::Ts => "ts",
            ValueType::F32s => "f32s",
            ValueType::F64s => "f64s",
            ValueType::I32s => "i32s",
            ValueType::I64s => "i64s",
            ValueType::List => "list",
            ValueType::Record => "record",
        }
    }
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

macro_rules! typed_array {
    ($variant:ident, $ty:ty, $as_fn:ident, $mut_fn:ident, $unique_fn:ident) => {
        /// The array, if this is that variant.
        pub fn $as_fn(&self) -> Option<&[$ty]> {
            match self {
                Value::$variant(a) => Some(a),
                _ => None,
            }
        }

        /// The array for in-place edits, copying it first if it is shared.
        pub fn $mut_fn(&mut self) -> Option<&mut [$ty]> {
            match self {
                Value::$variant(a) => Some(Arc::make_mut(a)),
                _ => None,
            }
        }

        /// The array for in-place edits, or `None` if this is not that variant
        /// or the array is shared.
        pub fn $unique_fn(&mut self) -> Option<&mut [$ty]> {
            match self {
                Value::$variant(a) => Arc::get_mut(a),
                _ => None,
            }
        }
    };
}

impl Value {
    /// A binary value.
    pub fn bytes(bytes: Bytes) -> Value {
        Value::Bytes(Box::new(bytes))
    }

    /// A nested record value.
    pub fn record(record: Record) -> Value {
        Value::Record(Box::new(record))
    }

    /// A string value.
    pub fn str(s: impl Into<Utf8Bytes>) -> Value {
        Value::Str(s.into())
    }

    /// The variant's type.
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::I8(_) => ValueType::I8,
            Value::I16(_) => ValueType::I16,
            Value::I32(_) => ValueType::I32,
            Value::I64(_) => ValueType::I64,
            Value::U8(_) => ValueType::U8,
            Value::U16(_) => ValueType::U16,
            Value::U32(_) => ValueType::U32,
            Value::U64(_) => ValueType::U64,
            Value::F32(_) => ValueType::F32,
            Value::F64(_) => ValueType::F64,
            Value::Str(_) => ValueType::Str,
            Value::Bytes(_) => ValueType::Bytes,
            Value::Ts(_) => ValueType::Ts,
            Value::F32s(_) => ValueType::F32s,
            Value::F64s(_) => ValueType::F64s,
            Value::I32s(_) => ValueType::I32s,
            Value::I64s(_) => ValueType::I64s,
            Value::List(_) => ValueType::List,
            Value::Record(_) => ValueType::Record,
        }
    }

    /// Whether this is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The boolean, if this is one.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Any integer variant as `i64`, if it fits.
    pub fn as_i64(&self) -> Option<i64> {
        self.int_parts().and_then(|(v, _, _)| i64::try_from(v).ok())
    }

    /// Any integer variant as `u64`, if it fits.
    pub fn as_u64(&self) -> Option<u64> {
        self.int_parts().and_then(|(v, _, _)| u64::try_from(v).ok())
    }

    /// A float variant as `f64` (`F32` widens exactly).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F32(v) => Some(f64::from(*v)),
            Value::F64(v) => Some(*v),
            _ => None,
        }
    }

    /// The string, if this is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// The binary data, if this is a `Bytes` value.
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// The timestamp, if this is one.
    pub fn as_ts(&self) -> Option<Timestamp> {
        match self {
            Value::Ts(t) => Some(*t),
            _ => None,
        }
    }

    /// The list items, if this is a list.
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(items) => Some(items),
            _ => None,
        }
    }

    /// The nested record, if this is one.
    pub fn as_record(&self) -> Option<&Record> {
        match self {
            Value::Record(r) => Some(r),
            _ => None,
        }
    }

    typed_array!(F32s, f32, as_f32s, f32s_mut, f32s_unique_mut);
    typed_array!(F64s, f64, as_f64s, f64s_mut, f64s_unique_mut);
    typed_array!(I32s, i32, as_i32s, i32s_mut, i32s_unique_mut);
    typed_array!(I64s, i64, as_i64s, i64s_mut, i64s_unique_mut);

    /// The integer's value, signedness, and width in bits.
    fn int_parts(&self) -> Option<(i128, bool, u8)> {
        Some(match self {
            Value::I8(v) => (i128::from(*v), true, 8),
            Value::I16(v) => (i128::from(*v), true, 16),
            Value::I32(v) => (i128::from(*v), true, 32),
            Value::I64(v) => (i128::from(*v), true, 64),
            Value::U8(v) => (i128::from(*v), false, 8),
            Value::U16(v) => (i128::from(*v), false, 16),
            Value::U32(v) => (i128::from(*v), false, 32),
            Value::U64(v) => (i128::from(*v), false, 64),
            _ => return None,
        })
    }

    fn int_of_type(ty: ValueType, v: i128) -> Option<Value> {
        Some(match ty {
            ValueType::I8 => Value::I8(i8::try_from(v).ok()?),
            ValueType::I16 => Value::I16(i16::try_from(v).ok()?),
            ValueType::I32 => Value::I32(i32::try_from(v).ok()?),
            ValueType::I64 => Value::I64(i64::try_from(v).ok()?),
            ValueType::U8 => Value::U8(u8::try_from(v).ok()?),
            ValueType::U16 => Value::U16(u16::try_from(v).ok()?),
            ValueType::U32 => Value::U32(u32::try_from(v).ok()?),
            ValueType::U64 => Value::U64(u64::try_from(v).ok()?),
            _ => return None,
        })
    }

    /// Converts to `target` if that widens the type, preserving the value
    /// exactly. Returns `None` for anything that would narrow or change kind,
    /// so a widening cast can never lose information. `Null` stays `Null`.
    pub fn widen(&self, target: ValueType) -> Option<Value> {
        if self.is_null() {
            return Some(Value::Null);
        }
        if self.value_type() == target {
            return Some(self.clone());
        }
        if !self.value_type().widens_to(target) {
            return None;
        }
        match (self, target) {
            (Value::F32(v), ValueType::F64) => Some(Value::F64(f64::from(*v))),
            (Value::F32s(a), ValueType::F64s) => {
                Some(Value::F64s(a.iter().map(|x| f64::from(*x)).collect()))
            }
            (Value::I32s(a), ValueType::I64s) => {
                Some(Value::I64s(a.iter().map(|x| i64::from(*x)).collect()))
            }
            _ => {
                let (v, _, _) = self.int_parts()?;
                Self::int_of_type(target, v)
            }
        }
    }
}

macro_rules! impl_from_scalar {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(impl From<$ty> for Value {
            fn from(v: $ty) -> Self {
                Value::$variant(v)
            }
        })*
    };
}

impl_from_scalar!(
    bool => Bool, i8 => I8, i16 => I16, i32 => I32, i64 => I64,
    u8 => U8, u16 => U16, u32 => U32, u64 => U64, f32 => F32, f64 => F64,
    Timestamp => Ts, Utf8Bytes => Str,
);

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.into())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Str(s.into())
    }
}

impl From<Vec<f64>> for Value {
    fn from(v: Vec<f64>) -> Self {
        Value::F64s(v.into())
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::F32s(v.into())
    }
}

impl From<Vec<i32>> for Value {
    fn from(v: Vec<i32>) -> Self {
        Value::I32s(v.into())
    }
}

impl From<Vec<i64>> for Value {
    fn from(v: Vec<i64>) -> Self {
        Value::I64s(v.into())
    }
}

fn f32_bits_eq(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

fn f64_bits_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (I8(a), I8(b)) => a == b,
            (I16(a), I16(b)) => a == b,
            (I32(a), I32(b)) => a == b,
            (I64(a), I64(b)) => a == b,
            (U8(a), U8(b)) => a == b,
            (U16(a), U16(b)) => a == b,
            (U32(a), U32(b)) => a == b,
            (U64(a), U64(b)) => a == b,
            (F32(a), F32(b)) => a.to_bits() == b.to_bits(),
            (F64(a), F64(b)) => a.to_bits() == b.to_bits(),
            (Str(a), Str(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (Ts(a), Ts(b)) => a == b,
            (F32s(a), F32s(b)) => f32_bits_eq(a, b),
            (F64s(a), F64s(b)) => f64_bits_eq(a, b),
            (I32s(a), I32s(b)) => a == b,
            (I64s(a), I64s(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Record(a), Record(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        discriminant(self).hash(state);
        match self {
            Null => {}
            Bool(v) => v.hash(state),
            I8(v) => v.hash(state),
            I16(v) => v.hash(state),
            I32(v) => v.hash(state),
            I64(v) => v.hash(state),
            U8(v) => v.hash(state),
            U16(v) => v.hash(state),
            U32(v) => v.hash(state),
            U64(v) => v.hash(state),
            F32(v) => v.to_bits().hash(state),
            F64(v) => v.to_bits().hash(state),
            Str(v) => v.hash(state),
            Bytes(v) => v.hash(state),
            Ts(v) => v.hash(state),
            F32s(a) => {
                a.len().hash(state);
                a.iter().for_each(|x| x.to_bits().hash(state));
            }
            F64s(a) => {
                a.len().hash(state);
                a.iter().for_each(|x| x.to_bits().hash(state));
            }
            I32s(a) => a.hash(state),
            I64s(a) => a.hash(state),
            List(items) => items.hash(state),
            Record(r) => r.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_is_24_bytes() {
        assert_eq!(size_of::<Value>(), 24);
        assert_eq!(size_of::<Option<Value>>(), 24);
    }

    #[test]
    fn float_equality_is_bitwise() {
        assert_eq!(Value::F64(f64::NAN), Value::F64(f64::NAN));
        assert_ne!(Value::F64(0.0), Value::F64(-0.0));
        assert_eq!(
            Value::F32s(vec![f32::NAN].into()),
            Value::F32s(vec![f32::NAN].into())
        );
        assert_ne!(Value::I32(1), Value::I64(1));
    }

    #[test]
    fn integer_accessors_check_range() {
        assert_eq!(Value::U64(u64::MAX).as_i64(), None);
        assert_eq!(Value::U64(u64::MAX).as_u64(), Some(u64::MAX));
        assert_eq!(Value::I8(-1).as_u64(), None);
        assert_eq!(Value::U16(7).as_i64(), Some(7));
        assert_eq!(Value::F64(1.0).as_i64(), None);
        assert_eq!(Value::F32(0.5).as_f64(), Some(0.5));
    }

    #[test]
    fn widening_rules() {
        assert_eq!(Value::I8(-5).widen(ValueType::I64), Some(Value::I64(-5)));
        assert_eq!(Value::U8(200).widen(ValueType::I16), Some(Value::I16(200)));
        assert_eq!(Value::U32(7).widen(ValueType::U64), Some(Value::U64(7)));
        assert_eq!(Value::F32(1.5).widen(ValueType::F64), Some(Value::F64(1.5)));
        assert_eq!(Value::Null.widen(ValueType::F64), Some(Value::Null));
        assert_eq!(
            Value::I32s(vec![1, -2].into()).widen(ValueType::I64s),
            Some(Value::I64s(vec![1, -2].into()))
        );
        assert_eq!(
            Value::F32s(vec![0.25].into()).widen(ValueType::F64s),
            Some(Value::F64s(vec![0.25].into()))
        );
    }

    #[test]
    fn narrowing_and_kind_changes_are_refused() {
        assert_eq!(Value::I64(1).widen(ValueType::I32), None);
        assert_eq!(Value::U8(1).widen(ValueType::I8), None);
        assert_eq!(Value::I8(1).widen(ValueType::U64), None);
        assert_eq!(Value::U64(1).widen(ValueType::I64), None);
        assert_eq!(Value::I32(1).widen(ValueType::F64), None);
        assert_eq!(Value::F64(1.0).widen(ValueType::F32), None);
        assert_eq!(Value::str("1").widen(ValueType::I64), None);
        assert_eq!(Value::F64s(vec![1.0].into()).widen(ValueType::F32s), None);
    }

    #[test]
    fn shared_array_is_copied_on_write_and_unique_is_edited_in_place() {
        let mut a = Value::from(vec![1.0f64, 2.0]);
        let before = a.as_f64s().map(<[f64]>::as_ptr);
        assert!(a.f64s_unique_mut().is_some());
        a.f64s_mut().unwrap()[0] = 9.0;
        assert_eq!(
            a.as_f64s().map(<[f64]>::as_ptr),
            before,
            "unique arrays edit in place"
        );

        let b = a.clone();
        assert!(
            a.f64s_unique_mut().is_none(),
            "shared arrays refuse unique access"
        );
        a.f64s_mut().unwrap()[1] = 7.0;
        assert_eq!(a.as_f64s(), Some(&[9.0, 7.0][..]));
        assert_eq!(
            b.as_f64s(),
            Some(&[9.0, 2.0][..]),
            "the clone is unaffected"
        );
        assert_ne!(
            a.as_f64s().map(<[f64]>::as_ptr),
            b.as_f64s().map(<[f64]>::as_ptr)
        );
    }

    #[test]
    fn accessors_return_none_for_other_variants() {
        let v = Value::I32s(vec![1].into());
        assert!(v.as_f64s().is_none() && v.as_str().is_none() && v.as_list().is_none());
        assert_eq!(v.as_i32s(), Some(&[1][..]));
    }
}
