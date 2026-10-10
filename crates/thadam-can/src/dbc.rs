//! DBC-based signal decoding for `emit: decoded`.
//!
//! [`Decoder`] is compiled once from the DBC file text and then turns each
//! received frame into its decoded signals. Extraction follows the DBC bit
//! conventions: Intel (little-endian) start bits name the signal's least
//! significant bit, Motorola (big-endian) start bits name its most
//! significant bit and consecutive signal bits walk toward the byte's LSB,
//! continuing at the next byte's MSB (the "sawtooth" where DBC bit numbers
//! map as byte `n/8`, physical bit `n%8`).

use std::collections::HashMap;

use can_dbc::{ByteOrder, MessageId, MultiplexIndicator, SignalExtendedValueType, ValueType};
use serde_json::{Map, Value};

use crate::frame::CanFrameData;

/// Map key for a message: the id with the extended flag folded in.
type MessageKey = u64;

fn message_key(id: u32, extended: bool) -> MessageKey {
    (u64::from(extended) << 32) | u64::from(id)
}

/// How a signal participates in message multiplexing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Multiplex {
    /// A normal signal, always decoded.
    Plain,
    /// The multiplexer switch: its raw value selects the multiplexed signals.
    Switch,
    /// Decoded only when the switch's raw value equals the selector.
    Multiplexed(u64),
}

/// One signal's extraction and scaling recipe.
#[derive(Clone, Debug)]
struct SignalDecode {
    name: String,
    start_bit: u64,
    size: u64,
    big_endian: bool,
    signed: bool,
    float: bool,
    factor: f64,
    offset: f64,
    multiplex: Multiplex,
}

/// One DBC message's decode recipe.
#[derive(Clone, Debug)]
struct MessageDecode {
    name: String,
    switch: Option<usize>,
    signals: Vec<SignalDecode>,
}

/// A compiled DBC: message decode recipes by (id, extended).
#[derive(Clone, Debug)]
pub(crate) struct Decoder {
    messages: HashMap<MessageKey, MessageDecode>,
}

impl Decoder {
    /// Compiles the DBC in `text`, naming the offending message or signal
    /// when the definition is unsound.
    pub(crate) fn from_text(text: &str) -> Result<Decoder, String> {
        let dbc = can_dbc::Dbc::try_from(text).map_err(|e| e.to_string())?;
        let mut messages = HashMap::new();
        for message in &dbc.messages {
            let (id, extended) = match message.id {
                MessageId::Standard(id) => (u32::from(id), false),
                MessageId::Extended(id) => (id, true),
            };
            let mut switch = None;
            let mut signals = Vec::with_capacity(message.signals.len());
            for (index, signal) in message.signals.iter().enumerate() {
                let multiplex = match signal.multiplexer_indicator {
                    MultiplexIndicator::Plain => Multiplex::Plain,
                    MultiplexIndicator::Multiplexor => {
                        if switch.replace(index).is_some() {
                            return Err(format!(
                                "message {} has more than one multiplexer switch",
                                message.name
                            ));
                        }
                        Multiplex::Switch
                    }
                    MultiplexIndicator::MultiplexedSignal(selector)
                    | MultiplexIndicator::MultiplexorAndMultiplexedSignal(selector) => {
                        Multiplex::Multiplexed(selector)
                    }
                };
                if signal.size == 0 || signal.size > 64 {
                    return Err(format!(
                        "signal {} in message {} must be 1..64 bits wide",
                        signal.name, message.name
                    ));
                }
                let float = match dbc.extended_value_type_for_signal(message.id, &signal.name) {
                    Some(SignalExtendedValueType::IEEEfloat32Bit) => Some(32),
                    Some(SignalExtendedValueType::IEEEdouble64bit) => Some(64),
                    _ => None,
                };
                if float.is_some_and(|width| width != signal.size) {
                    return Err(format!(
                        "IEEE float signal {} in message {} must be {} bits wide",
                        signal.name,
                        message.name,
                        float.unwrap_or_default()
                    ));
                }
                signals.push(SignalDecode {
                    name: signal.name.clone(),
                    start_bit: signal.start_bit,
                    size: signal.size,
                    big_endian: matches!(signal.byte_order, ByteOrder::BigEndian),
                    signed: matches!(signal.value_type, ValueType::Signed),
                    float: float.is_some(),
                    factor: signal.factor,
                    offset: signal.offset,
                    multiplex,
                });
            }
            messages.insert(
                message_key(id, extended),
                MessageDecode {
                    name: message.name.clone(),
                    switch,
                    signals,
                },
            );
        }
        Ok(Decoder { messages })
    }

    /// The message name for a frame's id, when the DBC knows it.
    pub(crate) fn message_name(&self, frame: &CanFrameData) -> Option<&str> {
        self.messages
            .get(&message_key(frame.id, frame.extended))
            .map(|m| m.name.as_str())
    }

    /// Decodes a frame's signals into a JSON object, or `None` when the id
    /// is unknown. Signals whose bits extend past the received data are
    /// skipped, so a short classic frame never decodes garbage.
    pub(crate) fn decode(&self, frame: &CanFrameData) -> Option<Map<String, Value>> {
        let message = self.messages.get(&message_key(frame.id, frame.extended))?;

        let switch_value = message.switch.map(|i| {
            let s = &message.signals[i];
            extract_signal(&frame.data, s).0
        });

        let mut object = Map::new();
        for signal in &message.signals {
            let selected = match signal.multiplex {
                Multiplex::Plain | Multiplex::Switch => true,
                Multiplex::Multiplexed(selector) => switch_value == Some(selector),
            };
            if !selected || !fits(&frame.data, signal) {
                continue;
            }
            let (raw, signed) = extract_signal(&frame.data, signal);
            object.insert(signal.name.clone(), signal_value(raw, signed, signal));
        }
        Some(object)
    }
}

/// Whether every bit of the signal lands inside `data`.
fn fits(data: &[u8], signal: &SignalDecode) -> bool {
    if signal.big_endian {
        let mut bit = signal.start_bit;
        for _ in 0..signal.size {
            if usize::try_from(bit / 8).is_ok_and(|b| b >= data.len()) {
                return false;
            }
            bit = if bit.is_multiple_of(8) {
                bit + 15
            } else {
                bit - 1
            };
        }
        true
    } else {
        signal
            .start_bit
            .checked_add(signal.size)
            .is_some_and(|end| end <= (data.len() as u64) * 8)
    }
}

/// The bits of the signal as `(unsigned, signed)` raw values.
fn extract_signal(data: &[u8], signal: &SignalDecode) -> (u64, i64) {
    let raw = if signal.big_endian {
        extract_big_endian(data, signal.start_bit, signal.size)
    } else {
        extract_little_endian(data, signal.start_bit, signal.size)
    };
    (raw, sign_extend(raw, signal.size))
}

/// Intel layout: the start bit is the signal's least significant bit, in
/// standard little-endian numbering. Works for any frame length, so CAN FD
/// signals past byte 8 decode too.
fn extract_little_endian(data: &[u8], start: u64, size: u64) -> u64 {
    debug_assert!((1..=64).contains(&size));
    let mut value = 0u64;
    for i in 0..size {
        let bit = start + i;
        let byte = usize::try_from(bit / 8).unwrap_or(usize::MAX);
        let one = data
            .get(byte)
            .map_or(0u64, |b| u64::from(b >> (bit % 8)) & 1);
        value |= one << i;
    }
    value
}

/// Motorola layout: the start bit is the signal's most significant bit;
/// continued bits descend within the byte and, at a byte's LSB, wrap to the
/// next byte's MSB.
fn extract_big_endian(data: &[u8], start: u64, size: u64) -> u64 {
    debug_assert!((1..=64).contains(&size));
    let mut value = 0u64;
    let mut bit = start;
    for _ in 0..size {
        let byte = usize::try_from(bit / 8).unwrap_or(usize::MAX);
        let one = data
            .get(byte)
            .map_or(0u64, |b| u64::from(b >> (bit % 8)) & 1);
        value = (value << 1) | one;
        bit = if bit.is_multiple_of(8) {
            bit + 15
        } else {
            bit - 1
        };
    }
    value
}

/// Sign-extends a `size`-bit raw value to i64.
fn sign_extend(raw: u64, size: u64) -> i64 {
    if size == 0 || size >= 64 {
        raw as i64
    } else {
        let shift = 64 - size;
        ((raw << shift) as i64) >> shift
    }
}

/// The signal's JSON value: an integer for unscaled integer signals, a float
/// once `factor` or `offset` apply or for IEEE float signals (`null` when the
/// result is not finite).
fn signal_value(raw: u64, signed: i64, signal: &SignalDecode) -> Value {
    let unscaled = signal.factor == 1.0 && signal.offset == 0.0;
    if signal.float {
        let number = if signal.size == 32 {
            f64::from(f32::from_bits(u32::try_from(raw).unwrap_or_default()))
        } else {
            f64::from_bits(raw)
        };
        return Value::from(if unscaled {
            number
        } else {
            number.mul_add(signal.factor, signal.offset)
        });
    }
    if unscaled {
        if signal.signed {
            Value::from(signed)
        } else {
            Value::from(raw)
        }
    } else {
        let number = if signal.signed {
            signed as f64
        } else {
            raw as f64
        };
        Value::from(number.mul_add(signal.factor, signal.offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A compact fixture covering Intel and Motorola layouts, signedness,
    /// scaling, and multiplexing. Motorola start bits named here (39, 23, 8)
    /// sit at each byte's MSB or LSB position in the DBC sawtooth.
    const FIXTURE: &str = r#"
VERSION ""


NS_ :

BS_:

BU_: TEST_ECU


BO_ 291 EngineData: 8 TEST_ECU
 SG_ Rpm : 0|16@1+ (0.25,0) [0|16383.75] "rpm" TEST_ECU
 SG_ CoolantTemp : 16|8@1- (1,-40) [-40|215] "degC" TEST_ECU
 SG_ Throttle : 24|8@1+ (1,0) [0|100] "percent" TEST_ECU
 SG_ Crc : 39|16@0+ (1,0) [0|65535] "" TEST_ECU

BO_ 292 Wide: 8 TEST_ECU
 SG_ TwelveBit : 23|12@0+ (1,0) [0|4095] "" TEST_ECU
 SG_ SignedBe : 39|16@0- (0.5,10) [-16000|16000] "" TEST_ECU

BO_ 293 Muxed: 8 TEST_ECU
 SG_ Mode M : 0|4@1+ (1,0) [0|15] "" TEST_ECU
 SG_ HeatLevel m1 : 8|16@1+ (0.1,0) [0|100] "degC" TEST_ECU
 SG_ FanRpm m2 : 23|16@0+ (1,0) [0|65000] "rpm" TEST_ECU
 SG_ AlwaysThere : 56|8@1+ (1,0) [0|255] "" TEST_ECU

BO_ 2147488308 ExtendedMsg: 8 TEST_ECU
 SG_ ExtSignal : 0|8@1+ (1,0) [0|255] "" TEST_ECU

BO_ 294 FdMessage: 16 TEST_ECU
 SG_ Speed : 0|32@1+ (1,0) [0|0] "" TEST_ECU
 SG_ Torque : 32|64@1+ (0.5,0) [0|0] "" TEST_ECU
 SG_ Tail : 96|16@1+ (1,0) [0|65535] "" TEST_ECU

BO_ 295 Odd: 2 TEST_ECU
 SG_ Nibbles : 3|6@0+ (1,0) [0|63] "" TEST_ECU

SIG_VALTYPE_ 294 Speed : 1;
SIG_VALTYPE_ 294 Torque : 2;
"#;

    fn decoder() -> Decoder {
        Decoder::from_text(FIXTURE).expect("fixture parses")
    }

    #[test]
    fn little_endian_unsigned_scaled() {
        // 3000 rpm -> raw 12000 (0x2EE0) little-endian in bytes 0..1.
        let value = decoder().decode(&CanFrameData::classic(291, &[0xE0, 0x2E, 0, 0, 0, 0, 0, 0]));
        assert_eq!(value.expect("known id")["Rpm"], json!(3000.0));
    }

    #[test]
    fn little_endian_signed_with_offset() {
        let value = decoder().decode(&CanFrameData::classic(291, &[0, 0, 100, 0, 0, 0, 0, 0]));
        assert_eq!(value.expect("known id")["CoolantTemp"], json!(60.0));
        // Raw 0x9C reads as -100; with the -40 offset the value is -140.
        let value = decoder().decode(&CanFrameData::classic(291, &[0, 0, 0x9C, 0, 0, 0, 0, 0]));
        assert_eq!(value.expect("known id")["CoolantTemp"], json!(-140.0));
    }

    #[test]
    fn big_endian_two_byte_signal() {
        // Crc at start bit 39 size 16: bytes 4..5 big-endian.
        let mut data = [0u8; 8];
        data[4] = 0xAB;
        data[5] = 0xCD;
        let value = decoder().decode(&CanFrameData::classic(291, &data));
        assert_eq!(value.expect("known id")["Crc"], json!(0xABCD));
    }

    #[test]
    fn big_endian_twelve_bit_alignment() {
        // TwelveBit at start bit 23 size 12: byte 2 followed by the top
        // nibble of byte 3.
        let value = decoder().decode(&CanFrameData::classic(292, &[0, 0, 0xAB, 0xC0, 0, 0, 0, 0]));
        assert_eq!(value.expect("known id")["TwelveBit"], json!(0xABC));
    }

    #[test]
    fn big_endian_signed_with_scale_and_offset() {
        // SignedBe at start bit 39 size 16: bytes 4..5 big-endian, raw
        // 0xFFFE is -2, so -2 * 0.5 + 10 = 9.
        let value = decoder().decode(&CanFrameData::classic(292, &[0, 0, 0, 0, 0xFF, 0xFE, 0, 0]));
        assert_eq!(value.expect("known id")["SignedBe"], json!(9.0));
        let value = decoder().decode(&CanFrameData::classic(292, &[0, 0, 0, 0, 0x00, 0x64, 0, 0]));
        assert_eq!(value.expect("known id")["SignedBe"], json!(60.0));
    }

    #[test]
    fn big_endian_start_bit_inside_a_byte_wraps_to_the_next_msb() {
        // Nibbles starts at bit 3 (byte 0): its bits are byte 0 bits 3..0,
        // then byte 1 bits 7..6.
        let value = decoder().decode(&CanFrameData::classic(295, &[0b0000_1010, 0b1100_0000]));
        assert_eq!(value.expect("known id")["Nibbles"], json!(0b10_1011));
    }

    #[test]
    fn fd_frames_decode_signals_beyond_the_eighth_byte() {
        let mut data = vec![0u8; 16];
        data[..4].copy_from_slice(&1.5f32.to_le_bytes());
        data[4..12].copy_from_slice(&84.0f64.to_le_bytes());
        data[12] = 0x34;
        data[13] = 0x12;
        let mut frame = CanFrameData::classic(294, &data);
        frame.fd = true;
        let value = decoder().decode(&frame).expect("known id");
        assert_eq!(value["Speed"], json!(1.5));
        assert_eq!(value["Torque"], json!(42.0));
        assert_eq!(value["Tail"], json!(0x1234));
    }

    #[test]
    fn non_finite_floats_become_null() {
        let mut data = vec![0u8; 16];
        data[..4].copy_from_slice(&f32::NAN.to_le_bytes());
        let value = decoder()
            .decode(&CanFrameData::classic(294, &data))
            .expect("known id");
        assert_eq!(value["Speed"], serde_json::Value::Null);
    }

    #[test]
    fn float_signals_must_have_the_matching_width() {
        let dbc = "VERSION \"\"\n\nNS_ :\n\nBS_:\n\nBU_: E\n\nBO_ 1 M: 8 E\n SG_ S : 0|16@1+ (1,0) [0|0] \"\" E\n\nSIG_VALTYPE_ 1 S : 1;\n";
        let err = Decoder::from_text(dbc).expect_err("16-bit float");
        assert!(err.contains("32 bits"), "{err}");
    }

    #[test]
    fn multiplexed_signals_follow_the_switch() {
        // Mode 1: HeatLevel (little-endian, 0.1) is selected.
        let mut frame = [1u8, 0xC8, 0x01, 0, 0, 0, 0, 99];
        let value = decoder().decode(&CanFrameData::classic(293, &frame));
        let value = value.expect("known id");
        assert_eq!(value["Mode"], json!(1));
        assert_eq!(value["HeatLevel"], json!(45.6));
        assert!(!value.contains_key("FanRpm"));
        assert_eq!(value["AlwaysThere"], json!(99));

        // Mode 2: FanRpm (big-endian bytes 2..3) is selected.
        frame[0] = 2;
        frame[2] = 0x12;
        frame[3] = 0x34;
        let value = decoder().decode(&CanFrameData::classic(293, &frame));
        let value = value.expect("known id");
        assert_eq!(value["FanRpm"], json!(0x1234));
        assert!(!value.contains_key("HeatLevel"));

        // Mode 5: neither multiplexed signal.
        frame[0] = 5;
        let value = decoder().decode(&CanFrameData::classic(293, &frame));
        let value = value.expect("known id");
        assert_eq!(value["Mode"], json!(5));
        assert!(!value.contains_key("HeatLevel"));
        assert!(!value.contains_key("FanRpm"));
        assert_eq!(value["AlwaysThere"], json!(99));
    }

    #[test]
    fn extended_ids_match_only_extended_messages() {
        // Message 2147488308 in the DBC is extended id 0x1234.
        let mut frame = CanFrameData::classic(0x1234, &[77, 0, 0, 0, 0, 0, 0, 0]);
        assert!(decoder().decode(&frame).is_none());
        frame.extended = true;
        assert_eq!(
            decoder().decode(&frame).expect("known id")["ExtSignal"],
            json!(77)
        );
        assert_eq!(decoder().message_name(&frame), Some("ExtendedMsg"));
    }

    #[test]
    fn short_frames_skip_signals_that_do_not_fit() {
        // 4 bytes: Rpm, CoolantTemp, Throttle fit; Crc (bytes 4..5) is skipped.
        let value = decoder().decode(&CanFrameData::classic(291, &[0xE0, 0x2E, 100, 5]));
        let value = value.expect("known id");
        assert_eq!(value["Rpm"], json!(3000.0));
        assert_eq!(value["CoolantTemp"], json!(60.0));
        assert_eq!(value["Throttle"], json!(5));
        assert!(!value.contains_key("Crc"));
    }

    #[test]
    fn unknown_ids_decode_to_none() {
        assert!(
            decoder()
                .decode(&CanFrameData::classic(0x777, &[0; 8]))
                .is_none()
        );
    }

    #[test]
    fn unscaled_signals_stay_integers() {
        let value = decoder().decode(&CanFrameData::classic(
            291,
            &[0, 0, 0, 42, 0xAB, 0xCD, 0, 0],
        ));
        let value = value.expect("known id");
        assert_eq!(value["Throttle"], json!(42));
        assert!(value["Throttle"].is_u64());
        assert!(value["Crc"].is_u64());
    }

    #[test]
    fn invalid_dbc_is_a_named_error() {
        let err = Decoder::from_text("BO_ garbage").expect_err("parse fails");
        assert!(!err.is_empty());
    }

    #[test]
    fn extraction_corners() {
        assert_eq!(sign_extend(0x80, 8), -128);
        assert_eq!(sign_extend(u64::MAX, 64), -1);
        assert_eq!(sign_extend(0x7F, 8), 127);
        // Full-width extraction across byte orders.
        assert_eq!(
            extract_little_endian(&[1, 2, 3, 4, 5, 6, 7, 8], 8, 48),
            0x0706_0504_0302
        );
        assert_eq!(extract_big_endian(&[0xAB, 0xCD], 7, 16), 0xABCD);
    }
}
