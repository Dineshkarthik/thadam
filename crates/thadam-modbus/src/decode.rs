//! Turns the registers or bits of one read into a JSON value.
//!
//! A read that yields one value produces a scalar; several values produce an
//! array. `raw` reads always produce an array of unsigned 16-bit integers.

use serde_json::Value;

use crate::config::{DataType, ReadSpec, WordOrder};
use crate::error::ModbusError;

/// The raw answer to one read, before interpretation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Table {
    /// Holding or input registers, in address order.
    Words(Vec<u16>),
    /// Coils or discrete inputs, in address order.
    Bits(Vec<bool>),
}

/// Interprets `table` according to `read`.
pub(crate) fn decode(read: &ReadSpec, table: &Table) -> Result<Value, ModbusError> {
    match (read.data_type, table) {
        (DataType::Bool, Table::Bits(bits)) => {
            check_len(read, bits.len())?;
            Ok(one_or_many(
                bits.iter().map(|&bit| Value::Bool(bit)).collect(),
            ))
        }
        (DataType::Raw, Table::Words(words)) => {
            check_len(read, words.len())?;
            Ok(Value::Array(
                words.iter().map(|&w| Value::from(w)).collect(),
            ))
        }
        (data_type, Table::Words(words)) if data_type != DataType::Bool => {
            check_len(read, words.len())?;
            let values = words
                .chunks_exact(width(data_type))
                .map(|chunk| number(read, data_type, chunk))
                .collect();
            Ok(one_or_many(values))
        }
        _ => Err(ModbusError::Protocol(format!(
            "`{}`: the answer does not match its data type",
            read.name
        ))),
    }
}

fn check_len(read: &ReadSpec, got: usize) -> Result<(), ModbusError> {
    if got == usize::from(read.count) {
        Ok(())
    } else {
        Err(ModbusError::Protocol(format!(
            "`{}`: asked for {} values, the server returned {got}",
            read.name, read.count
        )))
    }
}

fn width(data_type: DataType) -> usize {
    match data_type {
        DataType::U32 | DataType::I32 | DataType::F32 => 2,
        _ => 1,
    }
}

fn one_or_many(mut values: Vec<Value>) -> Value {
    if values.len() == 1 {
        values.swap_remove(0)
    } else {
        Value::Array(values)
    }
}

/// Joins one or two registers into 32 bits. A 16-bit value is the register
/// itself, so word order does not apply to it.
fn assemble(order: WordOrder, words: &[u16]) -> u32 {
    match *words {
        [first, second] => {
            let (high, low) = match order {
                WordOrder::Big => (first, second),
                WordOrder::Little => (second, first),
            };
            (u32::from(high) << 16) | u32::from(low)
        }
        [only] => u32::from(only),
        _ => 0,
    }
}

fn number(read: &ReadSpec, data_type: DataType, words: &[u16]) -> Value {
    let bits = assemble(read.word_order, words);
    if read.is_scaled() {
        let raw = match data_type {
            DataType::F32 => shortest(f32::from_bits(bits)),
            DataType::I16 => f64::from(bits as u16 as i16),
            DataType::I32 => f64::from(bits as i32),
            _ => f64::from(bits),
        };
        return float(raw * read.scale.unwrap_or(1.0) + read.offset.unwrap_or(0.0));
    }
    match data_type {
        DataType::F32 => float(shortest(f32::from_bits(bits))),
        DataType::I16 => Value::from(bits as u16 as i16),
        DataType::I32 => Value::from(bits as i32),
        _ => Value::from(bits),
    }
}

/// The `f32` as the nearest `f64` to its shortest decimal form, so `3.14`
/// stays `3.14` in JSON rather than `3.140000104904175`.
fn shortest(value: f32) -> f64 {
    value.to_string().parse().unwrap_or(f64::from(value))
}

/// NaN and infinities have no JSON form and become `null`.
fn float(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config::ReadKind;

    fn read(kind: ReadKind, data_type: DataType, count: u16) -> ReadSpec {
        ReadSpec {
            name: "v".into(),
            kind,
            start: 0,
            count,
            data_type,
            word_order: WordOrder::Big,
            scale: None,
            offset: None,
        }
    }

    fn words(read: &ReadSpec, words: Vec<u16>) -> Result<Value, ModbusError> {
        decode(read, &Table::Words(words))
    }

    #[test]
    fn sixteen_bit_integers_keep_their_sign_rules() {
        let u = read(ReadKind::Holding, DataType::U16, 1);
        let i = read(ReadKind::Holding, DataType::I16, 1);
        assert_eq!(words(&u, vec![0xFFFF]).unwrap(), json!(65535));
        assert_eq!(words(&i, vec![0xFFFF]).unwrap(), json!(-1));
    }

    #[test]
    fn thirty_two_bit_integers_follow_word_order() {
        let mut u = read(ReadKind::Holding, DataType::U32, 2);
        assert_eq!(words(&u, vec![0x0001, 0x0002]).unwrap(), json!(65538));
        u.word_order = WordOrder::Little;
        assert_eq!(words(&u, vec![0x0001, 0x0002]).unwrap(), json!(131073));
    }

    #[test]
    fn signed_thirty_two_bit_values_are_negative_when_the_high_bit_is_set() {
        let i = read(ReadKind::Holding, DataType::I32, 2);
        assert_eq!(words(&i, vec![0xFFFF, 0xFFFE]).unwrap(), json!(-2));
    }

    #[test]
    fn f32_assembles_both_word_orders() {
        let mut f = read(ReadKind::Holding, DataType::F32, 2);
        assert_eq!(words(&f, vec![0x4148, 0x0000]).unwrap(), json!(12.5));
        f.word_order = WordOrder::Little;
        assert_eq!(words(&f, vec![0x0000, 0x4148]).unwrap(), json!(12.5));
    }

    #[test]
    fn f32_uses_its_shortest_decimal_form() {
        let f = read(ReadKind::Input, DataType::F32, 2);
        assert_eq!(words(&f, vec![0x4116, 0x6666]).unwrap(), json!(9.4));
    }

    #[test]
    fn nan_has_no_json_form_and_becomes_null() {
        let f = read(ReadKind::Input, DataType::F32, 2);
        assert_eq!(words(&f, vec![0x7FC0, 0x0000]).unwrap(), Value::Null);
    }

    #[test]
    fn scale_and_offset_turn_integers_into_floats() {
        let mut r = read(ReadKind::Holding, DataType::U16, 1);
        r.scale = Some(0.5);
        r.offset = Some(-1.0);
        assert_eq!(words(&r, vec![10]).unwrap(), json!(4.0));
    }

    #[test]
    fn scale_applies_to_signed_and_float_values() {
        let mut r = read(ReadKind::Holding, DataType::I16, 1);
        r.scale = Some(0.1);
        r.offset = Some(2.0);
        let value = words(&r, vec![(-20i16) as u16]).unwrap();
        assert!((value.as_f64().unwrap() - 0.0).abs() < 1e-9, "{value}");

        let mut f = read(ReadKind::Holding, DataType::F32, 2);
        f.scale = Some(2.0);
        assert_eq!(words(&f, vec![0x4148, 0x0000]).unwrap(), json!(25.0));
    }

    #[test]
    fn raw_registers_stay_an_array_even_for_one_register() {
        let r = read(ReadKind::Holding, DataType::Raw, 1);
        assert_eq!(words(&r, vec![7]).unwrap(), json!([7]));
    }

    #[test]
    fn several_values_become_an_array() {
        let r = read(ReadKind::Holding, DataType::U16, 3);
        assert_eq!(words(&r, vec![1, 2, 3]).unwrap(), json!([1, 2, 3]));
    }

    #[test]
    fn coils_decode_to_booleans_scalar_for_one_bit() {
        let one = read(ReadKind::Coil, DataType::Bool, 1);
        assert_eq!(decode(&one, &Table::Bits(vec![true])).unwrap(), json!(true));
        let three = read(ReadKind::Coil, DataType::Bool, 3);
        assert_eq!(
            decode(&three, &Table::Bits(vec![true, false, false])).unwrap(),
            json!([true, false, false])
        );
    }

    #[test]
    fn wrong_length_or_table_is_a_protocol_error() {
        let r = read(ReadKind::Holding, DataType::U16, 2);
        assert!(matches!(words(&r, vec![1]), Err(ModbusError::Protocol(_))));
        assert!(matches!(
            decode(&r, &Table::Bits(vec![true, false])),
            Err(ModbusError::Protocol(_))
        ));
    }
}
