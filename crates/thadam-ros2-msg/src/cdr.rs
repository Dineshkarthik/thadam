//! Dynamic XCDR1 CDR decoding.
//!
//! The 4-byte encapsulation header gives the byte order; everything after it
//! is the CDR payload. Alignment is relative to the start of that payload:
//! primitive values pad the stream to a multiple of their size (1, 2, 4, or
//! 8). Strings are a `uint32` length (including the trailing NUL) followed
//! by the bytes; `wstring` is a code-unit count plus UTF-16. Sequences are a
//! `uint32` length followed by the elements; fixed-size arrays have no
//! length.
//!
//! All reads are bounds-checked and error messages never carry payload text.

use thadam_core::{ErrorDomain, ThadamError, Utf8Bytes, Value, codes};

use crate::msg::{ArrayBound, Primitive};
use crate::registry::{ListElem, Node, TypedArrayKind};

const CDR_BE: u16 = 0x0000;
const CDR_LE: u16 = 0x0001;

/// Buffers the encapsulation header (4 bytes) ahead of the payload cursor.
const ENCAPSULATION_LEN: usize = 4;

/// Sequences of zero-minimum-size element types (empty messages) cannot be
/// cross-checked against the payload length, so their declared size is
/// capped; a real ROS 2 payload sequence of empty messages far below this.
const MAX_ZERO_SIZE_SEQUENCE: usize = 16_384;

pub(crate) fn decode_message(
    compiled: &crate::registry::CompiledMessage,
    payload: &[u8],
) -> Result<thadam_core::Record, ThadamError> {
    let mut cursor = Cursor::new(payload)?;
    let mut values = Vec::with_capacity(compiled.shape().len());
    decode_nodes(&mut cursor, compiled.plan(), &mut values)?;
    let record = match thadam_core::Record::new(compiled.shape().clone(), values) {
        Ok(record) => record,
        Err(err) => {
            // Unreachable: the plan appends exactly one value per field.
            return Err(err.to_thadam_error());
        }
    };
    debug_assert!(
        record.validate().is_ok(),
        "the decode plan builds values of the declared types"
    );
    Ok(record)
}

use thadam_core::Classify;

fn decode_nodes(
    cursor: &mut Cursor<'_>,
    nodes: &[Node],
    values: &mut Vec<Value>,
) -> Result<(), ThadamError> {
    for node in nodes {
        match node {
            Node::Prim { prim } => values.push(read_prim(cursor, *prim)?),
            Node::Str { wide, bound } => {
                values.push(Value::Str(read_string(cursor, *wide, *bound)?));
            }
            Node::Bytes { bound } => values.push(read_byte_array(cursor, *bound)?),
            Node::TypedArray { elem, kind, bound } => {
                values.push(read_typed_array(cursor, *elem, *kind, *bound)?);
            }
            Node::ValueList { elem, bound, .. } => {
                values.push(read_value_list(cursor, elem, *bound)?);
            }
            Node::RecordList {
                shape,
                plan,
                bound,
                item_min_bytes,
            } => {
                let count = read_len(cursor, *bound)?;
                guard_declared_len(cursor, count, *item_min_bytes)?;
                let mut items = Vec::with_capacity(count.min(MAX_ZERO_SIZE_SEQUENCE));
                for _ in 0..count {
                    let mut item_values = Vec::with_capacity(shape.len());
                    decode_nodes(cursor, plan, &mut item_values)?;
                    let item = match thadam_core::Record::new(shape.clone(), item_values) {
                        Ok(item) => item,
                        Err(err) => return Err(err.to_thadam_error()),
                    };
                    items.push(Value::record(item));
                }
                values.push(Value::List(items));
            }
            Node::Time => {
                let sec = cursor.read_i32()?;
                let nanosec = cursor.read_u32()?;
                values.push(Value::I32(sec));
                values.push(Value::U32(nanosec));
                let nanos = i64::from(sec)
                    .saturating_mul(1_000_000_000)
                    .saturating_add(i64::from(nanosec));
                values.push(Value::from(thadam_core::Timestamp::from_nanos(nanos)));
            }
        }
    }
    Ok(())
}

fn read_prim(cursor: &mut Cursor<'_>, prim: Primitive) -> Result<Value, ThadamError> {
    Ok(match prim {
        Primitive::Bool => Value::Bool(cursor.read_u8()? != 0),
        Primitive::Byte => Value::I8(cursor.read_u8()? as i8),
        Primitive::Char | Primitive::U8 => Value::U8(cursor.read_u8()?),
        Primitive::I8 => Value::I8(cursor.read_u8()? as i8),
        Primitive::I16 => Value::I16(cursor.read_i16()?),
        Primitive::U16 => Value::U16(cursor.read_u16()?),
        Primitive::I32 => Value::I32(cursor.read_i32()?),
        Primitive::U32 => Value::U32(cursor.read_u32()?),
        Primitive::I64 => Value::I64(cursor.read_i64()?),
        Primitive::U64 => Value::U64(cursor.read_u64()?),
        Primitive::F32 => Value::F32(cursor.read_f32()?),
        Primitive::F64 => Value::F64(cursor.read_f64()?),
        Primitive::String | Primitive::WString => {
            return Err(malformed("a text type is not a fixed-size primitive"));
        }
    })
}

/// Reads a CDR `string` (NUL-terminated UTF-8) or `wstring` (UTF-16 code
/// units, one trailing NUL tolerated), enforcing the declared `<=N` bound.
fn read_string(
    cursor: &mut Cursor<'_>,
    wide: bool,
    bound: Option<u32>,
) -> Result<Utf8Bytes, ThadamError> {
    if wide {
        read_wstring(cursor, bound)
    } else {
        let len = cursor.read_u32()? as usize;
        if len == 0 {
            return Err(malformed("string length 0 is missing the NUL terminator")
                .with_context("offset", cursor.offset().to_string()));
        }
        let bytes = cursor.take(len)?;
        if bytes[bytes.len() - 1] != 0 {
            return Err(malformed("string is not NUL-terminated")
                .with_context("offset", cursor.offset().to_string()));
        }
        let text = &bytes[..bytes.len() - 1];
        if let Some(bound) = bound
            && text.len() > bound as usize
        {
            return Err(malformed("string exceeds its declared bound")
                .with_context("bound", bound.to_string())
                .with_context("length", text.len().to_string()));
        }
        Utf8Bytes::from_utf8_slice(text).map_err(|_| {
            ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_INVALID_UTF8,
                "string is not valid UTF-8",
            )
            .with_context("offset", cursor.offset().to_string())
        })
    }
}

fn read_wstring(cursor: &mut Cursor<'_>, bound: Option<u32>) -> Result<Utf8Bytes, ThadamError> {
    let count = cursor.read_u32()? as usize;
    let mut units = Vec::with_capacity(count.min(MAX_ZERO_SIZE_SEQUENCE));
    for _ in 0..count {
        units.push(cursor.read_u16()?);
    }
    // One trailing NUL code unit is tolerated for encoders that include it.
    if units.last() == Some(&0) {
        units.pop();
    }
    if let Some(bound) = bound
        && units.len() > bound as usize
    {
        return Err(malformed("wstring exceeds its declared bound")
            .with_context("bound", bound.to_string())
            .with_context("length", units.len().to_string()));
    }
    let text: String = char::decode_utf16(units.iter().copied())
        .collect::<Result<String, _>>()
        .map_err(|_| {
            ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_INVALID_UTF8,
                "wstring is not valid UTF-16",
            )
            .with_context("offset", cursor.offset().to_string())
        })?;
    Ok(Utf8Bytes::from(text))
}

fn read_byte_array(cursor: &mut Cursor<'_>, bound: ArrayBound) -> Result<Value, ThadamError> {
    let count = read_len(cursor, bound)?;
    let bytes = cursor.take(count)?;
    Ok(Value::bytes(bytes::Bytes::copy_from_slice(bytes)))
}

fn read_typed_array(
    cursor: &mut Cursor<'_>,
    elem: Primitive,
    kind: TypedArrayKind,
    bound: ArrayBound,
) -> Result<Value, ThadamError> {
    let count = read_len(cursor, bound)?;
    let elem_size = elem
        .wire_size()
        .ok_or_else(|| malformed("a text type is not a typed-array element"))?;
    guard_declared_len(cursor, count, elem_size)?;
    Ok(match kind {
        TypedArrayKind::F32 => {
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(cursor.read_f32()?);
            }
            Value::from(items)
        }
        TypedArrayKind::F64 => {
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(cursor.read_f64()?);
            }
            Value::from(items)
        }
        TypedArrayKind::I32 => {
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let value = match elem {
                    Primitive::I8 | Primitive::Byte => i32::from(cursor.read_u8()? as i8),
                    Primitive::I16 => i32::from(cursor.read_i16()?),
                    Primitive::U16 => i32::from(cursor.read_u16()?),
                    Primitive::I32 => cursor.read_i32()?,
                    _ => return Err(malformed("element type does not widen to i32")),
                };
                items.push(value);
            }
            Value::from(items)
        }
        TypedArrayKind::I64 => {
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let value = match elem {
                    Primitive::I64 => cursor.read_i64()?,
                    Primitive::U32 => i64::from(cursor.read_u32()?),
                    _ => return Err(malformed("element type does not widen to i64")),
                };
                items.push(value);
            }
            Value::from(items)
        }
    })
}

fn read_value_list(
    cursor: &mut Cursor<'_>,
    elem: &ListElem,
    bound: ArrayBound,
) -> Result<Value, ThadamError> {
    let elem_min = match elem {
        ListElem::Bool => 1,
        ListElem::U64 => 8,
        ListElem::Text { .. } => 4,
    };
    let count = read_len(cursor, bound)?;
    guard_declared_len(cursor, count, elem_min)?;
    let mut items = Vec::with_capacity(count.min(MAX_ZERO_SIZE_SEQUENCE));
    match elem {
        ListElem::Bool => {
            for _ in 0..count {
                items.push(Value::Bool(cursor.read_u8()? != 0));
            }
        }
        ListElem::U64 => {
            for _ in 0..count {
                items.push(Value::U64(cursor.read_u64()?));
            }
        }
        ListElem::Text {
            wide,
            bound: str_bound,
        } => {
            for _ in 0..count {
                items.push(Value::Str(read_string(cursor, *wide, *str_bound)?));
            }
        }
    }
    Ok(Value::List(items))
}

/// The element count of an array: read from the wire for sequences, or the
/// declared constant for fixed-size arrays.
fn read_len(cursor: &mut Cursor<'_>, bound: ArrayBound) -> Result<usize, ThadamError> {
    match bound {
        ArrayBound::Fixed(n) => Ok(n as usize),
        ArrayBound::Bounded(bound) => {
            let len = cursor.read_u32()?;
            if len > bound {
                return Err(malformed("sequence length exceeds its declared bound")
                    .with_context("bound", bound.to_string())
                    .with_context("length", len.to_string()));
            }
            Ok(len as usize)
        }
        ArrayBound::Unbounded => Ok(cursor.read_u32()? as usize),
    }
}

/// Guards against a corrupted declared length before allocating or looping:
/// with a non-zero minimum element size the whole array must fit in the
/// remaining payload; zero-size element types get the fixed cap.
fn guard_declared_len(
    cursor: &Cursor<'_>,
    count: usize,
    elem_min_bytes: usize,
) -> Result<(), ThadamError> {
    match cursor.remaining().checked_div(elem_min_bytes) {
        Some(max) if count > max => {
            return Err(malformed("declared length exceeds the remaining payload")
                .with_context("length", count.to_string())
                .with_context("offset", cursor.offset().to_string()));
        }
        Some(_) => {}
        // A zero-minimum-size element type cannot be cross-checked against
        // the payload length, so its declared size is capped instead.
        None if count > MAX_ZERO_SIZE_SEQUENCE => {
            return Err(ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_TOO_LARGE,
                "sequence of empty messages exceeds the element cap",
            )
            .with_context("limit", MAX_ZERO_SIZE_SEQUENCE.to_string()));
        }
        None => {}
    }
    Ok(())
}

fn malformed(message: &str) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Decode, codes::DECODE_INVALID_PAYLOAD, message)
}

/// A payload cursor with explicit alignment. `pos` counts from the start of
/// `buf`; alignment is relative to the start of the payload after the
/// encapsulation header.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
    little: bool,
}

impl<'a> Cursor<'a> {
    fn new(payload: &'a [u8]) -> Result<Self, ThadamError> {
        if payload.len() < ENCAPSULATION_LEN {
            return Err(malformed("the encapsulation header is truncated")
                .with_context("length", payload.len().to_string()));
        }
        let scheme = u16::from_be_bytes([payload[0], payload[1]]);
        let little = match scheme {
            CDR_LE => true,
            CDR_BE => false,
            _ => {
                return Err(ThadamError::permanent(
                    ErrorDomain::Decode,
                    codes::DECODE_UNSUPPORTED_ENCODING,
                    "unsupported CDR encapsulation scheme (XCDR1 0x0000/0x0001 only)",
                )
                .with_context("scheme", format!("0x{scheme:04x}")));
            }
        };
        Ok(Cursor {
            buf: payload,
            pos: ENCAPSULATION_LEN,
            little,
        })
    }

    fn offset(&self) -> usize {
        self.pos.saturating_sub(ENCAPSULATION_LEN)
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    /// Pads `pos` up to a multiple of `align`, counting from the start of the
    /// payload after the encapsulation header.
    fn align(&mut self, align: usize) -> Result<(), ThadamError> {
        let rel = self.pos - ENCAPSULATION_LEN;
        let rem = rel % align;
        if rem == 0 {
            return Ok(());
        }
        let pad = align - rem;
        let end = self
            .pos
            .checked_add(pad)
            .ok_or_else(|| malformed("payload position overflow"))?;
        if end > self.buf.len() {
            return Err(malformed("payload is truncated at alignment padding")
                .with_context("offset", self.offset().to_string()));
        }
        self.pos = end;
        Ok(())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ThadamError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| malformed("payload position overflow"))?;
        if end > self.buf.len() {
            return Err(
                malformed("payload is truncated").with_context("offset", self.offset().to_string())
            );
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read<const N: usize>(&mut self) -> Result<[u8; N], ThadamError> {
        self.align(N)?;
        let bytes = self.take(N)?;
        bytes
            .try_into()
            .map_err(|_| malformed("short fixed-size read"))
    }

    fn read_u8(&mut self) -> Result<u8, ThadamError> {
        Ok(self.read::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ThadamError> {
        let b = self.read::<2>()?;
        Ok(if self.little {
            u16::from_le_bytes(b)
        } else {
            u16::from_be_bytes(b)
        })
    }

    fn read_i16(&mut self) -> Result<i16, ThadamError> {
        Ok(self.read_u16()? as i16)
    }

    fn read_u32(&mut self) -> Result<u32, ThadamError> {
        let b = self.read::<4>()?;
        Ok(if self.little {
            u32::from_le_bytes(b)
        } else {
            u32::from_be_bytes(b)
        })
    }

    fn read_i32(&mut self) -> Result<i32, ThadamError> {
        Ok(self.read_u32()? as i32)
    }

    fn read_u64(&mut self) -> Result<u64, ThadamError> {
        let b = self.read::<8>()?;
        Ok(if self.little {
            u64::from_le_bytes(b)
        } else {
            u64::from_be_bytes(b)
        })
    }

    fn read_i64(&mut self) -> Result<i64, ThadamError> {
        Ok(self.read_u64()? as i64)
    }

    fn read_f32(&mut self) -> Result<f32, ThadamError> {
        Ok(f32::from_bits(self.read_u32()?))
    }

    fn read_f64(&mut self) -> Result<f64, ThadamError> {
        Ok(f64::from_bits(self.read_u64()?))
    }
}
