//! Event id derivation: `xxh3_128` over a canonical, tagged encoding.

use thadam_core::{AssetPath, EventId, Value, WalEpoch, WalPos};
use smallvec::SmallVec;
use twox_hash::XxHash3_128;

const TAG_POSITIONAL: u8 = 1;
const TAG_CONTENT: u8 = 2;
const TAG_SAMPLE: u8 = 3;
const TAG_WINDOW: u8 = 4;
const NO_SAMPLE: u32 = u32::MAX;

/// A growing byte string that is hashed once at the end.
#[derive(Clone, Debug)]
pub(crate) struct IdBuf(SmallVec<[u8; 96]>);

impl IdBuf {
    fn with_tag(tag: u8) -> Self {
        let mut b = IdBuf(SmallVec::new());
        b.0.push(tag);
        b
    }

    /// Starts a content-derived id for `dataset`.
    pub(crate) fn content(dataset: &str) -> Self {
        let mut b = Self::with_tag(TAG_CONTENT);
        b.str(dataset);
        b
    }

    /// The part of a `window` aggregate's id that every aggregate of one step
    /// shares: the dataset, the window size, and the output column names.
    pub(crate) fn window(dataset: &str, size_ns: i64, columns: &[&str]) -> Self {
        let mut b = Self::with_tag(TAG_WINDOW);
        b.str(dataset);
        b.u64(size_ns as u64);
        for c in columns {
            b.str(c);
        }
        b
    }

    /// The id of the aggregate of `asset`'s window starting at `start_ns`,
    /// derived from content only so a re-read after a restart yields the same
    /// id.
    pub(crate) fn window_event(&self, asset: &AssetPath, start_ns: i64) -> EventId {
        let mut b = self.clone();
        for (_, level) in asset.iter() {
            b.level(level);
        }
        b.u64(start_ns as u64);
        b.finish()
    }

    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    /// Appends a length-prefixed string, so `("ab","c")` and `("a","bc")`
    /// hash differently.
    pub(crate) fn str(&mut self, s: &str) {
        self.u64(s.len() as u64);
        self.0.extend_from_slice(s.as_bytes());
    }

    /// Appends a key value with a type tag, so `U64(1)` and `I64(1)` differ.
    /// Returns `false` for types that cannot be part of a key.
    pub(crate) fn value(&mut self, v: &Value) -> bool {
        match v {
            Value::Bool(b) => self.0.extend_from_slice(&[b'b', u8::from(*b)]),
            Value::I8(x) => self.0.extend_from_slice(&[b'1', *x as u8]),
            Value::I16(x) => {
                self.0.push(b'2');
                self.0.extend_from_slice(&x.to_le_bytes());
            }
            Value::I32(x) => {
                self.0.push(b'4');
                self.0.extend_from_slice(&x.to_le_bytes());
            }
            Value::I64(x) => {
                self.0.push(b'8');
                self.0.extend_from_slice(&x.to_le_bytes());
            }
            Value::U8(x) => self.0.extend_from_slice(&[b'5', *x]),
            Value::U16(x) => {
                self.0.push(b'6');
                self.0.extend_from_slice(&x.to_le_bytes());
            }
            Value::U32(x) => {
                self.0.push(b'7');
                self.0.extend_from_slice(&x.to_le_bytes());
            }
            Value::U64(x) => {
                self.0.push(b'9');
                self.u64(*x);
            }
            Value::F32(x) => {
                self.0.push(b'f');
                self.0.extend_from_slice(&x.to_bits().to_le_bytes());
            }
            Value::F64(x) => {
                self.0.push(b'd');
                self.u64(x.to_bits());
            }
            Value::Str(s) => {
                self.0.push(b's');
                self.str(s.as_str());
            }
            Value::Ts(t) => {
                self.0.push(b't');
                self.0.extend_from_slice(&t.as_nanos().to_le_bytes());
            }
            _ => return false,
        }
        true
    }

    /// Appends an identity level value.
    pub(crate) fn level(&mut self, value: &str) {
        self.0.push(b'L');
        self.str(value);
    }

    pub(crate) fn finish(&self) -> EventId {
        EventId::from_u128(XxHash3_128::oneshot(&self.0))
    }
}

/// The default id: `xxh3_128(epoch, frame position, record index[, sample])`.
/// Deterministic per WAL record, so stable across replay.
pub(crate) fn positional_id(
    epoch: WalEpoch,
    pos: WalPos,
    record_index: u32,
    sample: Option<u32>,
) -> EventId {
    let mut b = IdBuf::with_tag(TAG_POSITIONAL);
    b.u64(epoch.get());
    b.u64(pos.get());
    b.u32(record_index);
    b.u32(sample.unwrap_or(NO_SAMPLE));
    b.finish()
}

/// The id of one exploded sample of a content-identified message.
pub(crate) fn sample_id(message: EventId, sample: u32) -> EventId {
    let mut b = IdBuf::with_tag(TAG_SAMPLE);
    b.0.extend_from_slice(&message.to_bytes());
    b.u32(sample);
    b.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::Value;

    #[test]
    fn positional_ids_are_deterministic_and_position_sensitive() {
        let a = positional_id(WalEpoch::new(1), WalPos::new(2), 3, None);
        assert_eq!(a, positional_id(WalEpoch::new(1), WalPos::new(2), 3, None));
        for other in [
            positional_id(WalEpoch::new(2), WalPos::new(2), 3, None),
            positional_id(WalEpoch::new(1), WalPos::new(3), 3, None),
            positional_id(WalEpoch::new(1), WalPos::new(2), 4, None),
            positional_id(WalEpoch::new(1), WalPos::new(2), 3, Some(0)),
        ] {
            assert_ne!(a, other);
        }
    }

    #[test]
    fn content_ids_are_unambiguous() {
        let id = |parts: &[&str]| {
            let mut b = IdBuf::content("d");
            parts.iter().for_each(|p| b.level(p));
            b.finish()
        };
        assert_eq!(id(&["a", "b"]), id(&["a", "b"]));
        assert_ne!(id(&["ab", "c"]), id(&["a", "bc"]));
        assert_ne!(id(&["a"]), id(&["a", ""]));

        let typed = |v: Value| {
            let mut b = IdBuf::content("d");
            assert!(b.value(&v));
            b.finish()
        };
        assert_ne!(typed(Value::U64(1)), typed(Value::I64(1)));
        assert_ne!(typed(Value::Str("1".into())), typed(Value::U64(1)));
        assert_eq!(typed(Value::U64(7)), typed(Value::U64(7)));
        assert!(!IdBuf::content("d").value(&Value::List(vec![])));
    }

    #[test]
    fn dataset_name_is_part_of_the_id() {
        let mut a = IdBuf::content("telemetry");
        let mut b = IdBuf::content("joints");
        a.level("x");
        b.level("x");
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn window_ids_depend_on_content_only() {
        let asset = |r: &str| AssetPath::new([("robot", r)]).unwrap();
        let base = IdBuf::window("telemetry", 10, &["start", "n"]);
        let a = base.window_event(&asset("a"), 0);
        assert_eq!(a, base.window_event(&asset("a"), 0));
        assert_ne!(a, base.window_event(&asset("b"), 0));
        assert_ne!(a, base.window_event(&asset("a"), 10));
        assert_ne!(
            a,
            IdBuf::window("telemetry", 20, &["start", "n"]).window_event(&asset("a"), 0)
        );
        assert_ne!(
            a,
            IdBuf::window("telemetry", 10, &["start", "m"]).window_event(&asset("a"), 0)
        );
        assert_ne!(
            a,
            IdBuf::window("joints", 10, &["start", "n"]).window_event(&asset("a"), 0)
        );
    }

    #[test]
    fn sample_ids_differ_per_sample_and_per_message() {
        let m = EventId::from_u128(5);
        assert_ne!(sample_id(m, 0), sample_id(m, 1));
        assert_ne!(sample_id(m, 0), sample_id(EventId::from_u128(6), 0));
        assert_eq!(sample_id(m, 3), sample_id(m, 3));
    }
}
