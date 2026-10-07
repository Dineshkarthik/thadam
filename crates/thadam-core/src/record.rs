//! The opaque record: a shared shape plus positional values (D8, option B).

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::codes;
use crate::error::{Classify, ErrorDomain, Retry};
use crate::shape::{FieldDef, FieldIdx, ShapeKey, ShapeRef};
use crate::value::Value;

/// Why a record or a value assignment was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecordError {
    /// The value count does not match the shape.
    #[error("record has {actual} values but its shape has {expected} fields")]
    LengthMismatch {
        /// Fields in the shape.
        expected: usize,
        /// Values supplied.
        actual: usize,
    },
    /// A value does not match its field's declared type.
    #[error("field `{field}` expects {expected}, got {actual}")]
    TypeMismatch {
        /// The field name.
        field: String,
        /// The declared type.
        expected: String,
        /// The offending value's type.
        actual: &'static str,
    },
    /// `Null` was assigned to a non-nullable field.
    #[error("field `{field}` is required but the value is null")]
    NullInRequiredField {
        /// The field name.
        field: String,
    },
    /// The index is outside the shape.
    #[error("field index {index} is out of range for a shape of {len} fields")]
    IndexOutOfRange {
        /// The index used.
        index: usize,
        /// Fields in the shape.
        len: usize,
    },
}

impl Classify for RecordError {
    fn domain(&self) -> ErrorDomain {
        ErrorDomain::Schema
    }
    fn code(&self) -> &'static str {
        match self {
            RecordError::NullInRequiredField { .. } => codes::SCHEMA_MISSING_FIELD,
            _ => codes::SCHEMA_TYPE_MISMATCH,
        }
    }
    fn retry(&self) -> Retry {
        Retry::Permanent
    }
}

fn check_field(def: &FieldDef, value: &Value) -> Result<(), RecordError> {
    if value.is_null() {
        return if def.nullable {
            Ok(())
        } else {
            Err(RecordError::NullInRequiredField {
                field: def.name.to_string(),
            })
        };
    }
    if def.ty.accepts(value) {
        Ok(())
    } else {
        Err(RecordError::TypeMismatch {
            field: def.name.to_string(),
            expected: format!("{:?}", def.ty),
            actual: value.value_type().as_str(),
        })
    }
}

/// A set of field values sharing one [`Shape`](crate::Shape).
///
/// The representation is private. Hot paths resolve field indexes once per
/// [`ShapeKey`] and use [`Record::get_at`]; [`Record::get`] and
/// [`Record::iter`] look names up and are the slow path.
///
/// # Clone
///
/// `Clone` is a reference-count bump on the shape plus a clone of the value
/// slice (typed arrays and strings share their storage). It is not meant for
/// cross-thread fan-out: events are moved, not cloned, on the hot path.
#[derive(Clone)]
pub struct Record {
    shape: ShapeRef,
    values: Box<[Value]>,
}

impl Record {
    /// A record of `shape` with the given values, one per field in order.
    ///
    /// Only the count is checked here; the decoder produces values of the
    /// declared types, and [`Record::validate`] checks them in full.
    pub fn new(shape: ShapeRef, values: Vec<Value>) -> Result<Record, RecordError> {
        if values.len() != shape.len() {
            return Err(RecordError::LengthMismatch {
                expected: shape.len(),
                actual: values.len(),
            });
        }
        Ok(Record {
            shape,
            values: values.into_boxed_slice(),
        })
    }

    /// A record of `shape` with every field `Null`.
    pub fn nulls(shape: ShapeRef) -> Record {
        let values = vec![Value::Null; shape.len()].into_boxed_slice();
        Record { shape, values }
    }

    /// Checks every value against its field's type and nullability.
    pub fn validate(&self) -> Result<(), RecordError> {
        self.shape
            .fields()
            .iter()
            .zip(self.values.iter())
            .try_for_each(|(def, value)| check_field(def, value))
    }

    /// Number of fields.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the record has no fields.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The record's shape.
    pub fn shape(&self) -> &ShapeRef {
        &self.shape
    }

    /// The identity steps use to cache per-shape plans.
    pub fn shape_key(&self) -> ShapeKey {
        self.shape.key()
    }

    /// The value at `i`. This is the hot-path accessor.
    ///
    /// # Panics
    ///
    /// If `i` is out of range, which means the index came from another shape.
    pub fn get_at(&self, i: FieldIdx) -> &Value {
        &self.values[i.index()]
    }

    /// The value at `i`, or `None` if out of range.
    pub fn try_get_at(&self, i: FieldIdx) -> Option<&Value> {
        self.values.get(i.index())
    }

    /// Replaces the value at `i`. Debug builds assert the value fits the
    /// field; use [`Record::try_set_at`] for a checked assignment.
    ///
    /// # Panics
    ///
    /// If `i` is out of range, which means the index came from another shape.
    pub fn set_at(&mut self, i: FieldIdx, v: Value) {
        debug_assert!(
            self.shape
                .field(i)
                .is_none_or(|def| check_field(def, &v).is_ok()),
            "value does not fit field {i:?}"
        );
        self.values[i.index()] = v;
    }

    /// Replaces the value at `i` after checking it against the field,
    /// returning the previous value.
    pub fn try_set_at(&mut self, i: FieldIdx, v: Value) -> Result<Value, RecordError> {
        let def = self.shape.field(i).ok_or(RecordError::IndexOutOfRange {
            index: i.index(),
            len: self.values.len(),
        })?;
        check_field(def, &v)?;
        Ok(std::mem::replace(&mut self.values[i.index()], v))
    }

    /// The value of the named field. Slow path.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.shape.index_of(name).and_then(|i| self.try_get_at(i))
    }

    /// Iterates (name, value) pairs in field order. Slow path.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.shape
            .fields()
            .iter()
            .map(|f| &*f.name)
            .zip(self.values.iter())
    }

    /// The values in field order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes the record, returning its values in field order.
    pub fn into_values(self) -> Vec<Value> {
        self.values.into_vec()
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.shape == other.shape && self.values == other.values
    }
}

impl Eq for Record {}

impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.shape.hash(state);
        self.values.hash(state);
    }
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shape::{FieldType, Shape};
    use std::sync::Arc;

    fn shape() -> ShapeRef {
        ShapeRef::new(
            Shape::new(vec![
                FieldDef::new("id", FieldType::U64).required(),
                FieldDef::new("temp", FieldType::F32),
                FieldDef::new("joints", FieldType::List(Box::new(FieldType::F64))),
            ])
            .unwrap(),
        )
    }

    fn record() -> Record {
        Record::new(
            shape(),
            vec![
                Value::U64(7),
                Value::F32(21.5),
                Value::from(vec![0.1f64, 0.2]),
            ],
        )
        .unwrap()
    }

    #[test]
    fn positional_and_named_access_agree() {
        let r = record();
        let temp = r.shape().index_of("temp").unwrap();
        assert_eq!(r.get_at(temp), &Value::F32(21.5));
        assert_eq!(r.get("temp"), Some(&Value::F32(21.5)));
        assert_eq!(r.get("nope"), None);
        assert_eq!(r.len(), 3);
        assert_eq!(
            r.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            ["id", "temp", "joints"]
        );
        assert_eq!(r.shape_key(), r.shape().key());
    }

    #[test]
    fn length_mismatch_is_refused() {
        assert_eq!(
            Record::new(shape(), vec![Value::U64(1)]).err(),
            Some(RecordError::LengthMismatch {
                expected: 3,
                actual: 1
            })
        );
    }

    #[test]
    fn validate_checks_types_and_nullability() {
        assert_eq!(record().validate(), Ok(()));
        let bad_type =
            Record::new(shape(), vec![Value::U64(1), Value::F64(1.0), Value::Null]).unwrap();
        assert!(
            matches!(bad_type.validate(), Err(RecordError::TypeMismatch { field, .. }) if field == "temp")
        );
        let null_required = Record::nulls(shape());
        assert!(matches!(
            null_required.validate(),
            Err(RecordError::NullInRequiredField { field }) if field == "id"
        ));
    }

    #[test]
    fn try_set_at_is_checked_and_returns_the_old_value() {
        let mut r = record();
        let temp = r.shape().index_of("temp").unwrap();
        assert_eq!(r.try_set_at(temp, Value::F32(1.0)), Ok(Value::F32(21.5)));
        assert!(r.try_set_at(temp, Value::I32(1)).is_err());
        assert_eq!(r.get_at(temp), &Value::F32(1.0));
        assert!(matches!(
            r.try_set_at(FieldIdx::new(9), Value::Null),
            Err(RecordError::IndexOutOfRange { index: 9, len: 3 })
        ));
    }

    #[test]
    fn clone_shares_shape_and_array_storage_but_not_the_value_slots() {
        let r = record();
        let mut c = r.clone();
        assert_eq!(c.shape_key(), r.shape_key());
        let joints = r.shape().index_of("joints").unwrap();
        let (Value::F64s(a), Value::F64s(b)) = (r.get_at(joints), c.get_at(joints)) else {
            panic!("joints are a typed array");
        };
        assert!(Arc::ptr_eq(a, b));
        c.set_at(r.shape().index_of("id").unwrap(), Value::U64(8));
        assert_eq!(r.get("id"), Some(&Value::U64(7)));
    }

    #[test]
    fn equality_is_structural_across_separately_built_shapes() {
        let a = record();
        let b = record();
        assert_ne!(a.shape_key(), b.shape_key());
        assert_eq!(a, b);
        let mut c = record();
        c.set_at(c.shape().index_of("temp").unwrap(), Value::F32(0.0));
        assert_ne!(a, c);
    }

    #[test]
    fn nested_records_work_as_values() {
        let inner_shape =
            ShapeRef::new(Shape::new(vec![FieldDef::new("x", FieldType::F64)]).unwrap());
        let outer_shape = ShapeRef::new(
            Shape::new(vec![FieldDef::new(
                "pos",
                FieldType::Record(inner_shape.clone()),
            )])
            .unwrap(),
        );
        let inner = Record::new(inner_shape, vec![Value::F64(1.5)]).unwrap();
        let outer = Record::new(outer_shape, vec![Value::record(inner.clone())]).unwrap();
        assert_eq!(outer.validate(), Ok(()));
        assert_eq!(outer.get("pos").and_then(Value::as_record), Some(&inner));
    }

    #[test]
    fn debug_prints_field_names() {
        let text = format!("{:?}", record());
        assert!(text.contains("\"temp\": F32(21.5)"), "{text}");
    }

    #[test]
    fn record_errors_classify_as_schema_errors() {
        let e = RecordError::NullInRequiredField { field: "id".into() }.to_thadam_error();
        assert_eq!(
            (e.domain, e.code),
            (ErrorDomain::Schema, "schema.missing_field")
        );
    }
}
