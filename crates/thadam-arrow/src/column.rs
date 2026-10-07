//! Builders for declared columns.
//!
//! Every push is two-phase so a rejected event leaves no partial row behind:
//! [`Col::check`] validates a value against the column (type, widening, range,
//! nullability) and accumulates its variable-length size, then
//! [`Col::append`] writes it and cannot fail.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, StringBuilder};
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, ListArray, PrimitiveArray, StructArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_buffer::{NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{FieldRef, Fields};
use thadam_core::{ShapeRef, Sym, Value, ValueType, codes};

use crate::schema::{Child, Node, ScalarNode, Storage};

pub(crate) const UTC: &str = "UTC";

/// Elements per row assumed for list columns until a batch has been seen.
const FIRST_BATCH_LIST_LEN: usize = 8;
/// Cap on the string capacity reserved from the previous batch's average.
const MAX_RESERVED_DATA_BYTES: usize = 16 << 20;

/// A growable primitive column: values plus a lazily materialized validity
/// bitmap.
pub(crate) struct Prim<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
    nulls: NullBufferBuilder,
}

impl<T: ArrowPrimitiveType> Prim<T> {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Prim {
            values: Vec::with_capacity(cap),
            nulls: NullBufferBuilder::new(cap),
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, v: T::Native) {
        self.values.push(v);
        self.nulls.append_non_null();
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.values.push(T::Native::default());
        self.nulls.append_null();
    }

    fn extend_from_slice(&mut self, vs: &[T::Native]) {
        self.values.extend_from_slice(vs);
        self.nulls.append_n_non_nulls(vs.len());
    }

    fn extend<I: ExactSizeIterator<Item = T::Native>>(&mut self, vs: I) {
        let n = vs.len();
        self.values.extend(vs);
        self.nulls.append_n_non_nulls(n);
    }

    /// Takes the buffers, leaving fresh ones sized for `next_cap` values.
    pub(crate) fn finish_parts(
        &mut self,
        next_cap: usize,
    ) -> (ScalarBuffer<T::Native>, Option<NullBuffer>) {
        let values = std::mem::replace(&mut self.values, Vec::with_capacity(next_cap));
        let mut nulls = std::mem::replace(&mut self.nulls, NullBufferBuilder::new(next_cap));
        (ScalarBuffer::from(values), nulls.finish())
    }

    pub(crate) fn finish(&mut self, next_cap: usize) -> PrimitiveArray<T> {
        let (values, nulls) = self.finish_parts(next_cap);
        PrimitiveArray::new(values, nulls)
    }
}

enum Buf {
    Bool(BooleanBuilder),
    I8(Prim<Int8Type>),
    I16(Prim<Int16Type>),
    I32(Prim<Int32Type>),
    I64(Prim<Int64Type>),
    U8(Prim<UInt8Type>),
    U16(Prim<UInt16Type>),
    U32(Prim<UInt32Type>),
    U64(Prim<UInt64Type>),
    F32(Prim<Float32Type>),
    F64(Prim<Float64Type>),
    TsMicros(Prim<TimestampMicrosecondType>),
    TsNanos(Prim<TimestampNanosecondType>),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

impl Buf {
    fn new(storage: Storage, cap: usize) -> Buf {
        match storage {
            Storage::Bool => Buf::Bool(BooleanBuilder::with_capacity(cap)),
            Storage::I8 => Buf::I8(Prim::with_capacity(cap)),
            Storage::I16 => Buf::I16(Prim::with_capacity(cap)),
            Storage::I32 => Buf::I32(Prim::with_capacity(cap)),
            Storage::I64 => Buf::I64(Prim::with_capacity(cap)),
            Storage::U8 => Buf::U8(Prim::with_capacity(cap)),
            Storage::U16 => Buf::U16(Prim::with_capacity(cap)),
            Storage::U32 => Buf::U32(Prim::with_capacity(cap)),
            Storage::U64 => Buf::U64(Prim::with_capacity(cap)),
            Storage::F32 => Buf::F32(Prim::with_capacity(cap)),
            Storage::F64 => Buf::F64(Prim::with_capacity(cap)),
            Storage::TsMicros => Buf::TsMicros(Prim::with_capacity(cap)),
            Storage::TsNanos => Buf::TsNanos(Prim::with_capacity(cap)),
            Storage::Utf8 => Buf::Utf8(StringBuilder::with_capacity(cap, cap * 16)),
            Storage::Binary => Buf::Binary(BinaryBuilder::with_capacity(cap, cap * 16)),
        }
    }

    fn push_null(&mut self) {
        match self {
            Buf::Bool(b) => b.append_null(),
            Buf::I8(p) => p.push_null(),
            Buf::I16(p) => p.push_null(),
            Buf::I32(p) => p.push_null(),
            Buf::I64(p) => p.push_null(),
            Buf::U8(p) => p.push_null(),
            Buf::U16(p) => p.push_null(),
            Buf::U32(p) => p.push_null(),
            Buf::U64(p) => p.push_null(),
            Buf::F32(p) => p.push_null(),
            Buf::F64(p) => p.push_null(),
            Buf::TsMicros(p) => p.push_null(),
            Buf::TsNanos(p) => p.push_null(),
            Buf::Utf8(b) => b.append_null(),
            Buf::Binary(b) => b.append_null(),
        }
    }

    /// Bytes per value this column stores in fixed-width buffers.
    fn fixed_bytes(&self) -> usize {
        match self {
            Buf::Bool(_) | Buf::I8(_) | Buf::U8(_) => 1,
            Buf::I16(_) | Buf::U16(_) => 2,
            Buf::I32(_) | Buf::U32(_) | Buf::F32(_) => 4,
            Buf::I64(_) | Buf::U64(_) | Buf::F64(_) | Buf::TsMicros(_) | Buf::TsNanos(_) => 8,
            Buf::Utf8(_) | Buf::Binary(_) => 4,
        }
    }

    fn finish(&mut self, rows: usize, next_cap: usize) -> ArrayRef {
        match self {
            Buf::Bool(b) => {
                let array = b.finish();
                *b = BooleanBuilder::with_capacity(next_cap);
                Arc::new(array)
            }
            Buf::I8(p) => Arc::new(p.finish(next_cap)),
            Buf::I16(p) => Arc::new(p.finish(next_cap)),
            Buf::I32(p) => Arc::new(p.finish(next_cap)),
            Buf::I64(p) => Arc::new(p.finish(next_cap)),
            Buf::U8(p) => Arc::new(p.finish(next_cap)),
            Buf::U16(p) => Arc::new(p.finish(next_cap)),
            Buf::U32(p) => Arc::new(p.finish(next_cap)),
            Buf::U64(p) => Arc::new(p.finish(next_cap)),
            Buf::F32(p) => Arc::new(p.finish(next_cap)),
            Buf::F64(p) => Arc::new(p.finish(next_cap)),
            Buf::TsMicros(p) => {
                let (values, nulls) = p.finish_parts(next_cap);
                Arc::new(TimestampMicrosecondArray::new(values, nulls).with_timezone(UTC))
            }
            Buf::TsNanos(p) => {
                let (values, nulls) = p.finish_parts(next_cap);
                Arc::new(TimestampNanosecondArray::new(values, nulls).with_timezone(UTC))
            }
            Buf::Utf8(b) => {
                let data_cap = reserve_data(b.values_slice().len(), rows, next_cap);
                let array = b.finish();
                *b = StringBuilder::with_capacity(next_cap, data_cap);
                Arc::new(array)
            }
            Buf::Binary(b) => {
                let data_cap = reserve_data(b.values_slice().len(), rows, next_cap);
                let array = b.finish();
                *b = BinaryBuilder::with_capacity(next_cap, data_cap);
                Arc::new(array)
            }
        }
    }
}

fn reserve_data(last_bytes: usize, last_rows: usize, next_rows: usize) -> usize {
    if last_rows == 0 {
        return next_rows * 16;
    }
    (last_bytes / last_rows)
        .saturating_mul(next_rows)
        .min(MAX_RESERVED_DATA_BYTES)
}

/// Any integer variant as an `i128`; `0` for everything else.
#[inline]
fn int_of(v: &Value) -> i128 {
    match v {
        Value::I8(x) => i128::from(*x),
        Value::I16(x) => i128::from(*x),
        Value::I32(x) => i128::from(*x),
        Value::I64(x) => i128::from(*x),
        Value::U8(x) => i128::from(*x),
        Value::U16(x) => i128::from(*x),
        Value::U32(x) => i128::from(*x),
        Value::U64(x) => i128::from(*x),
        _ => 0,
    }
}

/// Why a value was refused. Carries the path of record fields it was found
/// under, innermost first.
pub(crate) struct Fail {
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) path: Vec<Sym>,
}

impl Fail {
    fn new(code: &'static str, message: String) -> Self {
        Fail {
            code,
            message,
            path: Vec::new(),
        }
    }

    fn mismatch(expected: &Col, actual: ValueType) -> Self {
        Fail::new(
            codes::SCHEMA_TYPE_MISMATCH,
            format!("expected {}, got {actual}", expected.type_name()),
        )
    }

    fn missing() -> Self {
        Fail::new(
            codes::SCHEMA_MISSING_FIELD,
            "a required field is null or absent".to_owned(),
        )
    }

    fn narrowing(expected: &Col, actual: ValueType) -> Self {
        Fail::new(
            codes::SCHEMA_NARROWING,
            format!(
                "{actual} value is out of range for the {} column",
                expected.type_name()
            ),
        )
    }
}

struct Scalar {
    decl: ValueType,
    range: Option<(i128, i128)>,
    buf: Buf,
}

struct ListData {
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    child: Box<Col>,
    field: FieldRef,
    typed: Option<ValueType>,
}

struct StructData {
    shape: ShapeRef,
    children: Vec<Col>,
    nulls: NullBufferBuilder,
    fields: Fields,
}

enum Kind {
    Scalar(Scalar),
    List(ListData),
    Struct(StructData),
}

/// A builder for one declared column, recursive for lists and records.
pub(crate) struct Col {
    nullable: bool,
    len: usize,
    kind: Kind,
}

static NULL: Value = Value::Null;

impl Col {
    pub(crate) fn new(nullable: bool, node: &Node, cap: usize) -> Col {
        let kind = match node {
            Node::Scalar(ScalarNode { decl, storage }) => Kind::Scalar(Scalar {
                decl: *decl,
                range: storage.int_range(),
                buf: Buf::new(*storage, cap),
            }),
            Node::List { child, typed } => {
                let mut offsets = Vec::with_capacity(cap + 1);
                offsets.push(0);
                Kind::List(ListData {
                    offsets,
                    nulls: NullBufferBuilder::new(cap),
                    child: Box::new(Col::new(
                        child.field.is_nullable(),
                        &child.node,
                        cap * FIRST_BATCH_LIST_LEN,
                    )),
                    field: Arc::clone(&child.field),
                    typed: *typed,
                })
            }
            Node::Struct { shape, children } => Kind::Struct(StructData {
                shape: shape.clone(),
                children: children.iter().map(|c| child_col(c, cap)).collect(),
                nulls: NullBufferBuilder::new(cap),
                fields: children.iter().map(|c| Arc::clone(&c.field)).collect(),
            }),
        };
        Col {
            nullable,
            len: 0,
            kind,
        }
    }

    /// Bytes per value in fixed-width buffers, for size estimates.
    pub(crate) fn fixed_bytes(&self) -> usize {
        match &self.kind {
            Kind::Scalar(s) => s.buf.fixed_bytes(),
            Kind::List(_) => 4,
            Kind::Struct(s) => s.children.iter().map(Col::fixed_bytes).sum(),
        }
    }

    fn type_name(&self) -> String {
        match &self.kind {
            Kind::Scalar(s) => s.decl.as_str().to_owned(),
            Kind::List(l) => format!("list<{}>", l.child.type_name()),
            Kind::Struct(_) => "record".to_owned(),
        }
    }

    /// Validates `v` for this column and adds the variable-length bytes it
    /// would store to `bytes`. Nothing is written.
    pub(crate) fn check(&self, v: &Value, bytes: &mut u64) -> Result<(), Fail> {
        if v.is_null() {
            return if self.nullable {
                Ok(())
            } else {
                Err(Fail::missing())
            };
        }
        self.check_value(v, bytes)
    }

    fn check_value(&self, v: &Value, bytes: &mut u64) -> Result<(), Fail> {
        match &self.kind {
            Kind::Scalar(s) => {
                let vt = v.value_type();
                if !vt.widens_to(s.decl) {
                    return Err(Fail::mismatch(self, vt));
                }
                if let Some((lo, hi)) = s.range {
                    let x = int_of(v);
                    if x < lo || x > hi {
                        return Err(Fail::narrowing(self, vt));
                    }
                }
                match v {
                    Value::Str(s) => *bytes += s.len() as u64,
                    Value::Bytes(b) => *bytes += b.len() as u64,
                    _ => {}
                }
                Ok(())
            }
            Kind::List(l) => {
                let elem = l.child.fixed_bytes().max(1) as u64;
                let vt = v.value_type();
                let typed_len = match v {
                    Value::List(items) => {
                        for item in items {
                            if !item.is_null() {
                                l.child.check_value(item, bytes)?;
                            }
                        }
                        *bytes += items.len() as u64 * elem;
                        return Ok(());
                    }
                    Value::F32s(a) => a.len(),
                    Value::F64s(a) => a.len(),
                    Value::I32s(a) => a.len(),
                    Value::I64s(a) => a.len(),
                    _ => return Err(Fail::mismatch(self, vt)),
                };
                match l.typed {
                    Some(target) if vt.widens_to(target) => {
                        *bytes += typed_len as u64 * elem;
                        Ok(())
                    }
                    _ => Err(Fail::mismatch(self, vt)),
                }
            }
            Kind::Struct(s) => {
                let Value::Record(rec) = v else {
                    return Err(Fail::mismatch(self, v.value_type()));
                };
                if rec.shape() != &s.shape {
                    return Err(Fail::mismatch(self, ValueType::Record));
                }
                for (i, child) in s.children.iter().enumerate() {
                    let cv = rec.values().get(i).unwrap_or(&NULL);
                    child.check(cv, bytes).map_err(|mut f| {
                        f.path.push(Sym::from(s.fields[i].name().as_str()));
                        f
                    })?;
                }
                Ok(())
            }
        }
    }

    /// Writes `v`, which [`Col::check`] accepted for this column.
    pub(crate) fn append(&mut self, v: &Value) {
        if v.is_null() {
            self.append_null();
            return;
        }
        self.len += 1;
        match &mut self.kind {
            Kind::Scalar(s) => append_scalar(&mut s.buf, v),
            Kind::List(l) => {
                match v {
                    Value::List(items) => items.iter().for_each(|item| l.child.append(item)),
                    Value::F32s(a) => l.child.append_f32s(a),
                    Value::F64s(a) => l.child.append_f64s(a),
                    Value::I32s(a) => l.child.append_i32s(a),
                    Value::I64s(a) => l.child.append_i64s(a),
                    _ => debug_assert!(false, "append after a failed check"),
                }
                l.offsets.push(l.child.len as i32);
                l.nulls.append_non_null();
            }
            Kind::Struct(s) => {
                if let Value::Record(rec) = v {
                    for (child, cv) in s.children.iter_mut().zip(rec.values()) {
                        child.append(cv);
                    }
                    s.nulls.append_non_null();
                } else {
                    debug_assert!(false, "append after a failed check");
                    s.children.iter_mut().for_each(Col::append_null);
                    s.nulls.append_null();
                }
            }
        }
    }

    fn append_null(&mut self) {
        self.len += 1;
        match &mut self.kind {
            Kind::Scalar(s) => s.buf.push_null(),
            Kind::List(l) => {
                l.offsets.push(l.child.len as i32);
                l.nulls.append_null();
            }
            Kind::Struct(s) => {
                s.children.iter_mut().for_each(Col::append_null);
                s.nulls.append_null();
            }
        }
    }

    fn append_f32s(&mut self, a: &[f32]) {
        self.len += a.len();
        if let Kind::Scalar(s) = &mut self.kind {
            match &mut s.buf {
                Buf::F32(p) => p.extend_from_slice(a),
                Buf::F64(p) => p.extend(a.iter().map(|x| f64::from(*x))),
                _ => debug_assert!(false, "typed array into a mismatched column"),
            }
        }
    }

    fn append_f64s(&mut self, a: &[f64]) {
        self.len += a.len();
        if let Kind::Scalar(s) = &mut self.kind {
            match &mut s.buf {
                Buf::F64(p) => p.extend_from_slice(a),
                _ => debug_assert!(false, "typed array into a mismatched column"),
            }
        }
    }

    fn append_i32s(&mut self, a: &[i32]) {
        self.len += a.len();
        if let Kind::Scalar(s) = &mut self.kind {
            match &mut s.buf {
                Buf::I32(p) => p.extend_from_slice(a),
                Buf::I64(p) => p.extend(a.iter().map(|x| i64::from(*x))),
                _ => debug_assert!(false, "typed array into a mismatched column"),
            }
        }
    }

    fn append_i64s(&mut self, a: &[i64]) {
        self.len += a.len();
        if let Kind::Scalar(s) = &mut self.kind {
            match &mut s.buf {
                Buf::I64(p) => p.extend_from_slice(a),
                _ => debug_assert!(false, "typed array into a mismatched column"),
            }
        }
    }

    /// Takes the finished array and resets the builder, reserving room for
    /// `next_cap` rows (and the same average element count, for lists).
    pub(crate) fn finish(&mut self, next_cap: usize) -> ArrayRef {
        let rows = std::mem::take(&mut self.len);
        match &mut self.kind {
            Kind::Scalar(s) => s.buf.finish(rows, next_cap),
            Kind::List(l) => {
                let child_len = l.child.len;
                let child_cap = child_len
                    .saturating_mul(next_cap)
                    .checked_div(rows)
                    .unwrap_or(next_cap * FIRST_BATCH_LIST_LEN);
                let values = l.child.finish(child_cap);
                let mut next_offsets = Vec::with_capacity(next_cap + 1);
                next_offsets.push(0);
                let offsets = std::mem::replace(&mut l.offsets, next_offsets);
                let mut nulls = std::mem::replace(&mut l.nulls, NullBufferBuilder::new(next_cap));
                Arc::new(ListArray::new(
                    Arc::clone(&l.field),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    values,
                    nulls.finish(),
                ))
            }
            Kind::Struct(s) => {
                let arrays: Vec<ArrayRef> =
                    s.children.iter_mut().map(|c| c.finish(next_cap)).collect();
                let mut nulls = std::mem::replace(&mut s.nulls, NullBufferBuilder::new(next_cap));
                Arc::new(StructArray::new(s.fields.clone(), arrays, nulls.finish()))
            }
        }
    }
}

fn child_col(child: &Child, cap: usize) -> Col {
    Col::new(child.field.is_nullable(), &child.node, cap)
}

/// Writes a value `Col::check` accepted. A value that does not fit the buffer
/// is a bug in `check`; it becomes a null so the column stays aligned.
fn append_scalar(buf: &mut Buf, v: &Value) {
    match (buf, v) {
        (Buf::Bool(b), Value::Bool(x)) => b.append_value(*x),
        (Buf::I8(p), v) => p.push(int_of(v) as i8),
        (Buf::I16(p), v) => p.push(int_of(v) as i16),
        (Buf::I32(p), v) => p.push(int_of(v) as i32),
        (Buf::I64(p), v) => p.push(int_of(v) as i64),
        (Buf::U8(p), v) => p.push(int_of(v) as u8),
        (Buf::U16(p), v) => p.push(int_of(v) as u16),
        (Buf::U32(p), v) => p.push(int_of(v) as u32),
        (Buf::U64(p), v) => p.push(int_of(v) as u64),
        (Buf::F32(p), Value::F32(x)) => p.push(*x),
        (Buf::F64(p), Value::F64(x)) => p.push(*x),
        (Buf::F64(p), Value::F32(x)) => p.push(f64::from(*x)),
        (Buf::TsMicros(p), Value::Ts(t)) => p.push(t.as_micros()),
        (Buf::TsNanos(p), Value::Ts(t)) => p.push(t.as_nanos()),
        (Buf::Utf8(b), Value::Str(s)) => b.append_value(s.as_str()),
        (Buf::Binary(b), Value::Bytes(x)) => b.append_value(x.as_ref()),
        (buf, _) => {
            debug_assert!(false, "append after a failed check");
            buf.push_null();
        }
    }
}
