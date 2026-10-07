//! Declared field types, shapes, and the per-chain shape registry (D8, O1).
//!
//! A [`Shape`] holds field names, types, units, and nullability once; every
//! [`Record`](crate::Record) of that shape stores only positional values.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::codes;
use crate::error::{Classify, ErrorDomain, Retry};
use crate::text::Sym;
use crate::value::Value;

/// Most fields one shape may have. Shapes can derive from untrusted payloads.
pub const MAX_SHAPE_FIELDS: usize = 4096;

/// Shapes at or below this many fields are searched linearly by name; larger
/// ones get a hash index.
const LINEAR_SCAN_MAX: usize = 8;

/// The declared type of a field. Declared-field types live in `thadam-core` so
/// `thadam-arrow` and `thadam-transform` need not depend on `thadam-config`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FieldType {
    /// A boolean.
    Bool,
    /// A signed 8-bit integer.
    I8,
    /// A signed 16-bit integer.
    I16,
    /// A signed 32-bit integer.
    I32,
    /// A signed 64-bit integer.
    I64,
    /// An unsigned 8-bit integer.
    U8,
    /// An unsigned 16-bit integer.
    U16,
    /// An unsigned 32-bit integer.
    U32,
    /// An unsigned 64-bit integer.
    U64,
    /// A 32-bit float.
    F32,
    /// A 64-bit float.
    F64,
    /// A UTF-8 string.
    Str,
    /// Binary data.
    Bytes,
    /// A timestamp.
    Ts,
    /// A list of the element type. A list of `F32`, `F64`, `I32`, or `I64` may
    /// be held as a typed array or as a [`Value::List`].
    List(Box<FieldType>),
    /// A nested record of the given shape.
    Record(ShapeRef),
}

impl FieldType {
    fn tag(&self) -> u8 {
        match self {
            FieldType::Bool => 1,
            FieldType::I8 => 2,
            FieldType::I16 => 3,
            FieldType::I32 => 4,
            FieldType::I64 => 5,
            FieldType::U8 => 6,
            FieldType::U16 => 7,
            FieldType::U32 => 8,
            FieldType::U64 => 9,
            FieldType::F32 => 10,
            FieldType::F64 => 11,
            FieldType::Str => 12,
            FieldType::Bytes => 13,
            FieldType::Ts => 14,
            FieldType::List(_) => 15,
            FieldType::Record(_) => 16,
        }
    }

    fn encode(&self, h: &mut Fnv1a) {
        h.byte(self.tag());
        match self {
            FieldType::List(inner) => inner.encode(h),
            FieldType::Record(shape) => h.u64(shape.fingerprint().0),
            _ => {}
        }
    }

    /// Whether `value` is a valid value of this type. `Null` is not accepted
    /// here; nullability is a property of the field (see [`FieldDef::nullable`]).
    pub fn accepts(&self, value: &Value) -> bool {
        match (self, value) {
            (FieldType::Bool, Value::Bool(_))
            | (FieldType::I8, Value::I8(_))
            | (FieldType::I16, Value::I16(_))
            | (FieldType::I32, Value::I32(_))
            | (FieldType::I64, Value::I64(_))
            | (FieldType::U8, Value::U8(_))
            | (FieldType::U16, Value::U16(_))
            | (FieldType::U32, Value::U32(_))
            | (FieldType::U64, Value::U64(_))
            | (FieldType::F32, Value::F32(_))
            | (FieldType::F64, Value::F64(_))
            | (FieldType::Str, Value::Str(_))
            | (FieldType::Bytes, Value::Bytes(_))
            | (FieldType::Ts, Value::Ts(_)) => true,
            (FieldType::List(inner), Value::List(items)) => {
                items.iter().all(|v| v.is_null() || inner.accepts(v))
            }
            (FieldType::List(inner), Value::F32s(_)) => **inner == FieldType::F32,
            (FieldType::List(inner), Value::F64s(_)) => **inner == FieldType::F64,
            (FieldType::List(inner), Value::I32s(_)) => **inner == FieldType::I32,
            (FieldType::List(inner), Value::I64s(_)) => **inner == FieldType::I64,
            (FieldType::Record(shape), Value::Record(r)) => r.shape() == shape,
            _ => false,
        }
    }
}

/// A declared field: name, type, unit, nullability, and an optional stable ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldDef {
    /// The field name, unique within a shape.
    pub name: Sym,
    /// The declared type.
    pub ty: FieldType,
    /// A UCUM unit such as `Cel`, if declared.
    pub unit: Option<Sym>,
    /// Whether the field may be `Null`. Declared fields are nullable unless
    /// marked required.
    pub nullable: bool,
    /// A stable column ID for Parquet, if assigned. Unique within a shape.
    pub field_id: Option<u32>,
}

impl FieldDef {
    /// A nullable field with no unit or ID.
    pub fn new(name: impl Into<Sym>, ty: FieldType) -> Self {
        FieldDef {
            name: name.into(),
            ty,
            unit: None,
            nullable: true,
            field_id: None,
        }
    }

    /// Sets the unit.
    #[must_use]
    pub fn with_unit(mut self, unit: impl Into<Sym>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Marks the field non-nullable.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Sets the stable column ID.
    #[must_use]
    pub fn with_field_id(mut self, id: u32) -> Self {
        self.field_id = Some(id);
        self
    }

    fn encode(&self, h: &mut Fnv1a) {
        h.u64(self.name.len() as u64);
        h.bytes(self.name.as_bytes());
        self.ty.encode(h);
        match &self.unit {
            Some(unit) => {
                h.byte(1);
                h.u64(unit.len() as u64);
                h.bytes(unit.as_bytes());
            }
            None => h.byte(0),
        }
        h.byte(u8::from(self.nullable));
        match self.field_id {
            Some(id) => {
                h.byte(1);
                h.u64(u64::from(id));
            }
            None => h.byte(0),
        }
    }
}

/// A position within a shape's fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FieldIdx(u32);

impl FieldIdx {
    /// Wraps a field position.
    pub const fn new(index: u32) -> Self {
        FieldIdx(index)
    }

    /// The position as an index.
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// A 64-bit fingerprint of a shape's fields.
///
/// FNV-1a over a canonical, versioned encoding of every field, so it is stable
/// across processes. It identifies shapes in files and metrics but is not
/// collision resistant: shape identity is always decided by structural
/// equality, never by fingerprint alone.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ShapeFingerprint(u64);

impl ShapeFingerprint {
    /// The raw 64-bit value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ShapeFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

struct Fnv1a(u64);

impl Fnv1a {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    fn new() -> Self {
        Fnv1a(Self::OFFSET)
    }

    fn byte(&mut self, b: u8) {
        self.0 = (self.0 ^ u64::from(b)).wrapping_mul(Self::PRIME);
    }

    fn bytes(&mut self, bytes: &[u8]) {
        bytes.iter().for_each(|b| self.byte(*b));
    }

    fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }
}

/// An identity for a shape that is never reused within a process.
///
/// Steps cache `ShapeKey -> plan`. Unlike a pointer address, a key is never
/// recycled after its shape is freed, so a stale cache entry cannot match a
/// different shape.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct ShapeKey(u64);

static NEXT_SHAPE_KEY: AtomicU64 = AtomicU64::new(1);

/// Why a [`Shape`] could not be built.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ShapeError {
    /// A field name was empty.
    #[error("field {index} has an empty name")]
    EmptyName {
        /// Position of the field.
        index: usize,
    },
    /// Two fields shared a name.
    #[error("duplicate field name `{0}`")]
    DuplicateName(String),
    /// Two fields shared a column ID.
    #[error("duplicate field id {0}")]
    DuplicateFieldId(u32),
    /// More than [`MAX_SHAPE_FIELDS`] fields.
    #[error("shape has {0} fields, the maximum is {MAX_SHAPE_FIELDS}")]
    TooManyFields(usize),
}

impl Classify for ShapeError {
    fn domain(&self) -> ErrorDomain {
        ErrorDomain::Schema
    }
    fn code(&self) -> &'static str {
        codes::SCHEMA_INVALID_SHAPE
    }
    fn retry(&self) -> Retry {
        Retry::Permanent
    }
}

/// An immutable, ordered set of declared fields.
///
/// Shapes are shared behind [`ShapeRef`] and never cloned.
pub struct Shape {
    key: ShapeKey,
    fields: Box<[FieldDef]>,
    index: Option<HashMap<Sym, FieldIdx>>,
    fingerprint: ShapeFingerprint,
}

impl Shape {
    /// Builds a shape, checking names, IDs, and the field cap.
    pub fn new(fields: Vec<FieldDef>) -> Result<Shape, ShapeError> {
        if fields.len() > MAX_SHAPE_FIELDS {
            return Err(ShapeError::TooManyFields(fields.len()));
        }
        let mut names: HashSet<&str> = HashSet::with_capacity(fields.len());
        let mut ids: HashSet<u32> = HashSet::new();
        for (index, f) in fields.iter().enumerate() {
            if f.name.is_empty() {
                return Err(ShapeError::EmptyName { index });
            }
            if !names.insert(&f.name) {
                return Err(ShapeError::DuplicateName(f.name.to_string()));
            }
            if let Some(id) = f.field_id
                && !ids.insert(id)
            {
                return Err(ShapeError::DuplicateFieldId(id));
            }
        }
        let mut h = Fnv1a::new();
        h.byte(1);
        h.u64(fields.len() as u64);
        fields.iter().for_each(|f| f.encode(&mut h));
        let index: Option<HashMap<Sym, FieldIdx>> = (fields.len() > LINEAR_SCAN_MAX).then(|| {
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| (Sym::clone(&f.name), FieldIdx(i as u32)))
                .collect()
        });
        Ok(Shape {
            key: ShapeKey(NEXT_SHAPE_KEY.fetch_add(1, Ordering::Relaxed)),
            fields: fields.into_boxed_slice(),
            index,
            fingerprint: ShapeFingerprint(h.0),
        })
    }

    /// The process-unique identity of this shape instance.
    pub fn key(&self) -> ShapeKey {
        self.key
    }

    /// The stable fingerprint of the field definitions.
    pub fn fingerprint(&self) -> ShapeFingerprint {
        self.fingerprint
    }

    /// The fields in order.
    pub fn fields(&self) -> &[FieldDef] {
        &self.fields
    }

    /// The field at `idx`.
    pub fn field(&self, idx: FieldIdx) -> Option<&FieldDef> {
        self.fields.get(idx.index())
    }

    /// Number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Whether the shape has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The position of the named field. This is the slow path: steps resolve
    /// names once per shape and cache the index.
    pub fn index_of(&self, name: &str) -> Option<FieldIdx> {
        match &self.index {
            Some(index) => index.get(name).copied(),
            None => self
                .fields
                .iter()
                .position(|f| &*f.name == name)
                .map(|i| FieldIdx(i as u32)),
        }
    }
}

impl PartialEq for Shape {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            || (self.fingerprint == other.fingerprint && self.fields == other.fields)
    }
}

impl Eq for Shape {}

impl Hash for Shape {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fingerprint.hash(state);
    }
}

impl fmt::Debug for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shape")
            .field("fingerprint", &self.fingerprint)
            .field("fields", &self.fields)
            .finish()
    }
}

/// A shared handle to an immutable [`Shape`].
///
/// This is deliberately not a bare `Arc<Shape>`: it offers [`ShapeRef::key`]
/// for identity and structural `==` for equality, and never exposes the
/// pointer, so identity can move from pointer to registry id (D8, representation
/// C) without touching callers. Cloning bumps one reference count.
#[derive(Clone)]
pub struct ShapeRef(Arc<Shape>);

impl ShapeRef {
    /// Shares `shape`.
    pub fn new(shape: Shape) -> Self {
        ShapeRef(Arc::new(shape))
    }

    /// The shape's cache key.
    pub fn key(&self) -> ShapeKey {
        self.0.key
    }

    fn holders(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

impl Deref for ShapeRef {
    type Target = Shape;
    fn deref(&self) -> &Shape {
        &self.0
    }
}

impl PartialEq for ShapeRef {
    fn eq(&self, other: &Self) -> bool {
        *self.0 == *other.0
    }
}

impl Eq for ShapeRef {}

impl Hash for ShapeRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Debug for ShapeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The registry refused a new shape because it holds `max_shapes` already.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("shape registry is full ({max_shapes} shapes)")]
pub struct ShapeLimitExceeded {
    /// The configured cap.
    pub max_shapes: usize,
}

impl Classify for ShapeLimitExceeded {
    fn domain(&self) -> ErrorDomain {
        ErrorDomain::Schema
    }
    fn code(&self) -> &'static str {
        codes::SCHEMA_SHAPE_LIMIT
    }
    fn retry(&self) -> Retry {
        Retry::Permanent
    }
}

/// A per-chain table of distinct shapes with a size cap.
///
/// Shape keys can come from untrusted payloads, so growth is bounded: past
/// `max_shapes`, [`ShapeRegistry::intern`] refuses and counts the reject.
/// [`ShapeRegistry::evict_unreferenced`] reclaims shapes no record uses.
/// Registries are owned by one chain and never shared across threads.
#[derive(Debug)]
pub struct ShapeRegistry {
    max_shapes: usize,
    len: usize,
    by_fingerprint: HashMap<ShapeFingerprint, Vec<ShapeRef>>,
    rejected: u64,
}

impl ShapeRegistry {
    /// An empty registry holding at most `max_shapes` shapes.
    pub fn new(max_shapes: usize) -> Self {
        ShapeRegistry {
            max_shapes,
            len: 0,
            by_fingerprint: HashMap::new(),
            rejected: 0,
        }
    }

    /// The registered shape structurally equal to `shape`, adding `shape` if
    /// there is none and there is room.
    pub fn intern(&mut self, shape: Shape) -> Result<ShapeRef, ShapeLimitExceeded> {
        let bucket = self.by_fingerprint.entry(shape.fingerprint()).or_default();
        if let Some(existing) = bucket.iter().find(|s| ***s == shape) {
            return Ok(existing.clone());
        }
        if self.len >= self.max_shapes {
            self.rejected += 1;
            if bucket.is_empty() {
                self.by_fingerprint.remove(&shape.fingerprint());
            }
            return Err(ShapeLimitExceeded {
                max_shapes: self.max_shapes,
            });
        }
        let handle = ShapeRef::new(shape);
        bucket.push(handle.clone());
        self.len += 1;
        Ok(handle)
    }

    /// Drops every shape that no record or other handle references, returning
    /// how many were removed.
    pub fn evict_unreferenced(&mut self) -> usize {
        let before = self.len;
        self.by_fingerprint.retain(|_, bucket| {
            bucket.retain(|s| s.holders() > 1);
            !bucket.is_empty()
        });
        self.len = self.by_fingerprint.values().map(Vec::len).sum();
        before - self.len
    }

    /// Number of registered shapes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no shapes are registered.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The configured cap.
    pub fn max_shapes(&self) -> usize {
        self.max_shapes
    }

    /// How many shapes were refused at the cap.
    pub fn rejected(&self) -> u64 {
        self.rejected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(name: &str, ty: FieldType) -> FieldDef {
        FieldDef::new(name, ty)
    }

    fn shape(fields: Vec<FieldDef>) -> Shape {
        Shape::new(fields).unwrap()
    }

    #[test]
    fn rejects_bad_shapes() {
        assert_eq!(
            Shape::new(vec![f("", FieldType::I32)]).err(),
            Some(ShapeError::EmptyName { index: 0 })
        );
        assert_eq!(
            Shape::new(vec![f("a", FieldType::I32), f("a", FieldType::F64)]).err(),
            Some(ShapeError::DuplicateName("a".into()))
        );
        assert_eq!(
            Shape::new(vec![
                f("a", FieldType::I32).with_field_id(1),
                f("b", FieldType::I32).with_field_id(1)
            ])
            .err(),
            Some(ShapeError::DuplicateFieldId(1))
        );
        let many = (0..=MAX_SHAPE_FIELDS)
            .map(|i| f(&format!("f{i}"), FieldType::Bool))
            .collect();
        assert_eq!(
            Shape::new(many).err(),
            Some(ShapeError::TooManyFields(MAX_SHAPE_FIELDS + 1))
        );
    }

    #[test]
    fn name_lookup_works_for_small_and_wide_shapes() {
        for n in [3usize, 8, 9, 200] {
            let s = shape(
                (0..n)
                    .map(|i| f(&format!("f{i}"), FieldType::F64))
                    .collect(),
            );
            assert_eq!(s.len(), n);
            for i in 0..n {
                assert_eq!(s.index_of(&format!("f{i}")), Some(FieldIdx::new(i as u32)));
            }
            assert_eq!(s.index_of("nope"), None);
        }
    }

    #[test]
    fn equal_shapes_have_equal_fingerprints_and_distinct_keys() {
        let make = || {
            shape(vec![
                f("t", FieldType::F32).with_unit("Cel"),
                f("id", FieldType::U64),
            ])
        };
        let (a, b) = (make(), make());
        assert_eq!(a, b);
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert_ne!(a.key(), b.key());
    }

    #[test]
    fn fingerprint_covers_every_field_property() {
        let base = shape(vec![f("t", FieldType::F32)]).fingerprint();
        let variants = [
            vec![f("u", FieldType::F32)],
            vec![f("t", FieldType::F64)],
            vec![f("t", FieldType::F32).with_unit("Cel")],
            vec![f("t", FieldType::F32).required()],
            vec![f("t", FieldType::F32).with_field_id(1)],
            vec![f("t", FieldType::List(Box::new(FieldType::F32)))],
            vec![f("t", FieldType::F32), f("x", FieldType::F32)],
        ];
        for v in variants {
            assert_ne!(shape(v).fingerprint(), base);
        }
    }

    #[test]
    fn fingerprint_is_stable() {
        let s = shape(vec![
            f("t", FieldType::F32).with_unit("Cel"),
            f("n", FieldType::Str).required(),
        ]);
        insta::assert_snapshot!(s.fingerprint().to_string());
    }

    #[test]
    fn field_type_accepts_matching_values_only() {
        use crate::value::Value;
        assert!(FieldType::F32.accepts(&Value::F32(1.0)));
        assert!(!FieldType::F32.accepts(&Value::F64(1.0)));
        assert!(!FieldType::I64.accepts(&Value::Null));
        let list = FieldType::List(Box::new(FieldType::F64));
        assert!(list.accepts(&Value::from(vec![1.0f64])));
        assert!(!list.accepts(&Value::from(vec![1.0f32])));
        assert!(list.accepts(&Value::List(vec![Value::F64(1.0), Value::Null])));
        assert!(!list.accepts(&Value::List(vec![Value::F32(1.0)])));
    }

    #[test]
    fn registry_dedups_structurally_equal_shapes() {
        let mut reg = ShapeRegistry::new(10);
        let a = reg.intern(shape(vec![f("a", FieldType::I32)])).unwrap();
        let b = reg.intern(shape(vec![f("a", FieldType::I32)])).unwrap();
        assert_eq!(a.key(), b.key());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registry_counts_rejects_at_the_cap() {
        let mut reg = ShapeRegistry::new(2);
        let _a = reg.intern(shape(vec![f("a", FieldType::I32)])).unwrap();
        let _b = reg.intern(shape(vec![f("b", FieldType::I32)])).unwrap();
        let err = reg.intern(shape(vec![f("c", FieldType::I32)])).unwrap_err();
        assert_eq!(err.max_shapes, 2);
        assert_eq!(reg.rejected(), 1);
        assert!(
            reg.intern(shape(vec![f("a", FieldType::I32)])).is_ok(),
            "known shapes still resolve"
        );
        assert_eq!(reg.rejected(), 1);
    }

    #[test]
    fn registry_holds_10_000_distinct_shapes_then_rejects_and_evicts() {
        const CAP: usize = 10_000;
        let mut reg = ShapeRegistry::new(CAP);
        let mut live = Vec::new();
        for i in 0..CAP {
            let s = reg
                .intern(shape(vec![f(&format!("field_{i}"), FieldType::F64)]))
                .unwrap();
            if i % 2 == 0 {
                live.push(s);
            }
        }
        assert_eq!(reg.len(), CAP);
        for i in CAP..CAP + 50 {
            assert!(
                reg.intern(shape(vec![f(&format!("field_{i}"), FieldType::F64)]))
                    .is_err()
            );
        }
        assert_eq!(reg.rejected(), 50);

        assert_eq!(reg.evict_unreferenced(), CAP / 2);
        assert_eq!(reg.len(), CAP / 2);
        assert!(reg.intern(shape(vec![f("fresh", FieldType::F64)])).is_ok());
        assert_eq!(reg.len(), CAP / 2 + 1);
        drop(live);
        assert_eq!(
            reg.evict_unreferenced(),
            CAP / 2 + 1,
            "the live half plus the fresh shape"
        );
        assert!(reg.is_empty());
    }

    #[test]
    fn shape_error_classifies_as_permanent_schema_error() {
        let e = ShapeError::TooManyFields(9999).to_thadam_error();
        assert_eq!(e.code, "schema.invalid_shape");
        assert_eq!(e.retry, Retry::Permanent);
    }
}
