//! Normalization of Arrow columns into plain per-cell values for the Native
//! block writer.
//!
//! Decoding from Arrow storage happens here once per write, over the small
//! set of shapes `thadam-arrow` produces: integer and float primitives,
//! microsecond and nanosecond timestamps, UTF-8 and binary, dictionaries of
//! UTF-8, and one level of lists. Anything else means the open-time plan
//! disagrees with the batch.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow_array::{Array, DictionaryArray, GenericListArray};
use arrow_schema::{DataType, TimeUnit};

/// Why a column could not be normalized.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum NormError {
    /// The Arrow type is outside the set the plan accepts.
    #[error("unsupported Arrow array type {0}")]
    Unsupported(String),
}

/// A flat run of cells with nulls explicit. Narrow integers are widened and
/// timestamps stay raw ticks; the block writer narrows them again, checked.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Leaves {
    Bool(Vec<Option<bool>>),
    I64(Vec<Option<i64>>),
    U64(Vec<Option<u64>>),
    F64(Vec<Option<f64>>),
    /// Strings and binaries as raw bytes: a ClickHouse `String` is a byte
    /// string, so no UTF-8 assumption is made.
    Bytes(Vec<Option<Box<[u8]>>>),
}

impl Leaves {
    pub(crate) fn len(&self) -> usize {
        match self {
            Leaves::Bool(v) => v.len(),
            Leaves::I64(v) => v.len(),
            Leaves::U64(v) => v.len(),
            Leaves::F64(v) => v.len(),
            Leaves::Bytes(v) => v.len(),
        }
    }
}

/// The row bounds of a list column: per row `Some((start, end))` into the
/// child leaves, or `None` for a null list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Lists {
    pub(crate) rows: Vec<Option<(usize, usize)>>,
}

/// One normalized Arrow column. For a list column `leaves` holds the child
/// elements of every row and `lists` the row bounds; for a scalar column
/// `leaves` has one cell per row and `lists` is `None`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Norm {
    pub(crate) leaves: Leaves,
    pub(crate) lists: Option<Lists>,
}

impl Norm {
    /// Rows in the column.
    pub(crate) fn rows(&self) -> usize {
        match &self.lists {
            Some(l) => l.rows.len(),
            None => self.leaves.len(),
        }
    }

    /// Decodes an Arrow array of a type `thadam-arrow` produces.
    pub(crate) fn from_arrow(array: &dyn Array) -> Result<Self, NormError> {
        if let DataType::List(_) = array.data_type() {
            let list = array
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .ok_or_else(|| NormError::Unsupported(array.data_type().to_string()))?;
            let offsets = list.value_offsets();
            let bound = |v: i32| usize::try_from(v).unwrap_or(0);
            let rows = (0..list.len())
                .map(|i| (!list.is_null(i)).then(|| (bound(offsets[i]), bound(offsets[i + 1]))))
                .collect();
            return Ok(Norm {
                leaves: leaves(list.values().as_ref())?,
                lists: Some(Lists { rows }),
            });
        }
        Ok(Norm {
            leaves: leaves(array)?,
            lists: None,
        })
    }
}

fn widen<N: Copy, W: From<N>>(cells: impl Iterator<Item = Option<N>>) -> Vec<Option<W>> {
    cells.map(|c| c.map(W::from)).collect()
}

fn dictionary_bytes<K: ArrowDictionaryKeyType>(
    dict: &DictionaryArray<K>,
) -> Result<Vec<Option<Box<[u8]>>>, NormError> {
    let values = dict.values().as_string_opt::<i32>().ok_or_else(|| {
        NormError::Unsupported(format!("dictionary of {}", dict.values().data_type()))
    })?;
    Ok((0..dict.len())
        .map(|i| {
            let key = dict.key(i)?;
            (!values.is_null(key)).then(|| Box::<[u8]>::from(values.value(key).as_bytes()))
        })
        .collect())
}

fn leaves(array: &dyn Array) -> Result<Leaves, NormError> {
    Ok(match array.data_type() {
        DataType::Boolean => Leaves::Bool(array.as_boolean().iter().collect()),
        DataType::Int8 => Leaves::I64(widen(array.as_primitive::<Int8Type>().iter())),
        DataType::Int16 => Leaves::I64(widen(array.as_primitive::<Int16Type>().iter())),
        DataType::Int32 => Leaves::I64(widen(array.as_primitive::<Int32Type>().iter())),
        DataType::Int64 => Leaves::I64(array.as_primitive::<Int64Type>().iter().collect()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Leaves::I64(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Leaves::I64(
            array
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .collect(),
        ),
        DataType::UInt8 => Leaves::U64(widen(array.as_primitive::<UInt8Type>().iter())),
        DataType::UInt16 => Leaves::U64(widen(array.as_primitive::<UInt16Type>().iter())),
        DataType::UInt32 => Leaves::U64(widen(array.as_primitive::<UInt32Type>().iter())),
        DataType::UInt64 => Leaves::U64(array.as_primitive::<UInt64Type>().iter().collect()),
        DataType::Float32 => Leaves::F64(widen(array.as_primitive::<Float32Type>().iter())),
        DataType::Float64 => Leaves::F64(array.as_primitive::<Float64Type>().iter().collect()),
        DataType::Utf8 => Leaves::Bytes(
            array
                .as_string::<i32>()
                .iter()
                .map(|c| c.map(|s| Box::<[u8]>::from(s.as_bytes())))
                .collect(),
        ),
        DataType::Binary => Leaves::Bytes(
            array
                .as_binary::<i32>()
                .iter()
                .map(|c| c.map(Box::<[u8]>::from))
                .collect(),
        ),
        DataType::FixedSizeBinary(_) => Leaves::Bytes(
            array
                .as_fixed_size_binary()
                .iter()
                .map(|c| c.map(Box::<[u8]>::from))
                .collect(),
        ),
        DataType::Dictionary(key, _) if **key == DataType::Int8 => {
            Leaves::Bytes(dictionary_bytes(array.as_dictionary::<Int8Type>())?)
        }
        DataType::Dictionary(key, _) if **key == DataType::Int32 => {
            Leaves::Bytes(dictionary_bytes(array.as_dictionary::<Int32Type>())?)
        }
        other => return Err(NormError::Unsupported(other.to_string())),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{
        ListBuilder, StringBuilder, StringDictionaryBuilder, TimestampMicrosecondBuilder,
    };
    use arrow_array::{
        ArrayRef, BooleanArray, FixedSizeBinaryArray, Float32Array, Int8Array, UInt16Array,
    };

    use super::*;

    #[test]
    fn primitives_widen_and_keep_nulls() {
        let norm = Norm::from_arrow(&Int8Array::from(vec![Some(-3), None, Some(7)])).unwrap();
        assert_eq!(norm.leaves, Leaves::I64(vec![Some(-3), None, Some(7)]));
        assert_eq!(norm.rows(), 3);

        let norm = Norm::from_arrow(&UInt16Array::from(vec![Some(65_535), None])).unwrap();
        assert_eq!(norm.leaves, Leaves::U64(vec![Some(65_535), None]));

        let norm = Norm::from_arrow(&Float32Array::from(vec![Some(1.5_f32)])).unwrap();
        assert_eq!(norm.leaves, Leaves::F64(vec![Some(1.5)]));

        let norm = Norm::from_arrow(&BooleanArray::from(vec![Some(true), None])).unwrap();
        assert_eq!(norm.leaves, Leaves::Bool(vec![Some(true), None]));
    }

    #[test]
    fn timestamps_stay_raw_ticks() {
        let mut b = TimestampMicrosecondBuilder::new().with_timezone("UTC");
        b.append_value(1_700_000_000_123_456);
        b.append_null();
        let norm = Norm::from_arrow(&b.finish()).unwrap();
        assert_eq!(
            norm.leaves,
            Leaves::I64(vec![Some(1_700_000_000_123_456), None])
        );
    }

    #[test]
    fn strings_dictionaries_and_fixed_binary_become_bytes() {
        let mut s = StringBuilder::new();
        s.append_value("héllo");
        s.append_null();
        let norm = Norm::from_arrow(&s.finish()).unwrap();
        assert_eq!(
            norm.leaves,
            Leaves::Bytes(vec![Some(Box::from("héllo".as_bytes())), None])
        );

        let mut d = StringDictionaryBuilder::<Int8Type>::new();
        d.append("good").unwrap();
        d.append("bad").unwrap();
        d.append("good").unwrap();
        d.append_null();
        let norm = Norm::from_arrow(&d.finish()).unwrap();
        assert_eq!(
            norm.leaves,
            Leaves::Bytes(vec![
                Some(Box::from(&b"good"[..])),
                Some(Box::from(&b"bad"[..])),
                Some(Box::from(&b"good"[..])),
                None,
            ])
        );

        let fixed = FixedSizeBinaryArray::try_from_iter(
            [[1u8, 2, 3, 4], [5, 6, 7, 8]]
                .into_iter()
                .map(|b| b.to_vec()),
        )
        .unwrap();
        let norm = Norm::from_arrow(&fixed).unwrap();
        assert_eq!(
            norm.leaves,
            Leaves::Bytes(vec![
                Some(Box::from(&[1u8, 2, 3, 4][..])),
                Some(Box::from(&[5u8, 6, 7, 8][..])),
            ])
        );
    }

    #[test]
    fn lists_carry_row_bounds_and_null_rows() {
        let mut b = ListBuilder::new(arrow_array::builder::Float32Builder::new());
        b.values().append_value(1.0);
        b.values().append_value(2.0);
        b.append(true);
        b.append(false);
        b.values().append_value(3.0);
        b.append(true);
        let list: ArrayRef = Arc::new(b.finish());
        let norm = Norm::from_arrow(list.as_ref()).unwrap();
        assert_eq!(
            norm.leaves,
            Leaves::F64(vec![Some(1.0), Some(2.0), Some(3.0)])
        );
        assert_eq!(
            norm.lists,
            Some(Lists {
                rows: vec![Some((0, 2)), None, Some((2, 3))]
            })
        );
        assert_eq!(norm.rows(), 3);
    }

    #[test]
    fn unsupported_types_are_named() {
        let err = Norm::from_arrow(&arrow_array::Date32Array::from(vec![1])).unwrap_err();
        assert!(err.to_string().contains("Date32"), "{err}");
    }
}
