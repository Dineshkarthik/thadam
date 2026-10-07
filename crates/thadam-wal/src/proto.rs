//! The frame payload: a prost-encoded `RawFrame` (schema in
//! `proto/raw_frame.proto`). Decoding from a `Bytes` buffer yields payloads as
//! zero-copy slices of that buffer.

use std::collections::HashMap;

use bytes::Bytes;
use prost::Message;
use thadam_core::{Attributes, RawBatch, RawMessage, SourceId, Timestamp};

#[derive(Clone, PartialEq, Message)]
pub(crate) struct RawFrame {
    #[prost(uint32, tag = "1")]
    pub source_idx: u32,
    #[prost(bool, tag = "2")]
    pub clock_synced: bool,
    #[prost(string, repeated, tag = "3")]
    pub dict: Vec<String>,
    #[prost(message, repeated, tag = "4")]
    pub records: Vec<RawRecord>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct RawRecord {
    #[prost(sfixed64, tag = "1")]
    pub ingest_time_ns: i64,
    #[prost(uint32, repeated, tag = "2")]
    pub attr_refs: Vec<u32>,
    #[prost(bytes = "bytes", tag = "3")]
    pub payload: Bytes,
}

impl RawFrame {
    /// Builds the frame for `batch`. Payloads are shared, not copied; the one
    /// copy happens when the writer encodes the frame into its group buffer.
    pub(crate) fn from_batch(batch: &RawBatch) -> RawFrame {
        let mut interner = Interner::default();
        let source_idx = interner.intern(batch.source.as_str());
        let records = batch
            .messages
            .iter()
            .map(|m| RawRecord {
                ingest_time_ns: m.ingest_time.as_nanos(),
                attr_refs: m
                    .attrs
                    .iter()
                    .flat_map(|(k, v)| [interner.intern(k), interner.intern(v)])
                    .collect(),
                payload: m.payload.clone(),
            })
            .collect();
        RawFrame {
            source_idx,
            clock_synced: batch.clock_synced,
            dict: interner.strings,
            records,
        }
    }

    /// Checks the references a decoded frame carries.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        let n = self.dict.len();
        if self.source_idx as usize >= n {
            return Err("source index out of range");
        }
        for r in &self.records {
            if r.attr_refs.len() % 2 != 0 {
                return Err("odd number of attribute references");
            }
            if r.attr_refs.iter().any(|&i| i as usize >= n) {
                return Err("attribute reference out of range");
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Interner {
    index: HashMap<String, u32>,
    strings: Vec<String>,
}

impl Interner {
    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&i) = self.index.get(s) {
            return i;
        }
        let i = u32::try_from(self.strings.len()).unwrap_or(u32::MAX);
        self.strings.push(s.to_owned());
        self.index.insert(s.to_owned(), i);
        i
    }
}

/// Decodes and validates a frame payload.
pub(crate) fn decode(payload: Bytes) -> Result<RawFrame, &'static str> {
    let frame = RawFrame::decode(payload).map_err(|_| "frame payload does not decode")?;
    frame.validate()?;
    Ok(frame)
}

/// Rebuilds the batch a frame was written from.
pub(crate) fn to_raw_batch(frame: &RawFrame) -> RawBatch {
    let dict = &frame.dict;
    let messages = frame
        .records
        .iter()
        .map(|r| RawMessage {
            ingest_time: Timestamp::from_nanos(r.ingest_time_ns),
            attrs: attr_pairs(dict, &r.attr_refs).collect::<Attributes>(),
            payload: r.payload.clone(),
        })
        .collect();
    RawBatch {
        source: SourceId::new(dict[frame.source_idx as usize].as_str()),
        messages,
        clock_synced: frame.clock_synced,
    }
}

pub(crate) fn attr_pairs<'a>(
    dict: &'a [String],
    refs: &'a [u32],
) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
    refs.as_chunks::<2>()
        .0
        .iter()
        .map(move |[k, v]| (dict[*k as usize].as_str(), dict[*v as usize].as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch() -> RawBatch {
        let mut m1 = RawMessage::new(
            Timestamp::from_nanos(1_700_000_000_123_456_789),
            Bytes::from_static(b"{\"t\":1}"),
        );
        m1.attrs.insert("topic", "robots/r1/telemetry");
        m1.attrs.insert("qos", "1");
        let mut m2 = RawMessage::new(Timestamp::from_nanos(-5), Bytes::from_static(b""));
        m2.attrs.insert("topic", "robots/r1/telemetry");
        RawBatch::new(SourceId::new("robot-mqtt"), vec![m1, m2], true)
    }

    #[test]
    fn round_trips_through_bytes() {
        let b = batch();
        let frame = RawFrame::from_batch(&b);
        assert_eq!(
            frame.dict,
            vec!["robot-mqtt", "topic", "robots/r1/telemetry", "qos", "1"]
        );
        let bytes = Bytes::from(frame.encode_to_vec());
        let decoded = decode(bytes).unwrap();
        assert_eq!(to_raw_batch(&decoded), b);
    }

    #[test]
    fn decoded_payloads_are_slices_of_the_input_buffer() {
        let bytes = Bytes::from(RawFrame::from_batch(&batch()).encode_to_vec());
        let range = bytes.as_ptr() as usize..bytes.as_ptr() as usize + bytes.len();
        let decoded = decode(bytes.clone()).unwrap();
        let p = decoded.records[0].payload.as_ptr() as usize;
        assert!(range.contains(&p), "payload was copied");
    }

    #[test]
    fn encoding_is_pinned() {
        // Pins the wire encoding against proto/raw_frame.proto: field numbers,
        // wire types (sfixed64, packed uint32), and field order.
        let mut m = RawMessage::new(Timestamp::from_nanos(1), Bytes::from_static(b"P"));
        m.attrs.insert("k", "v");
        let b = RawBatch::new(SourceId::new("s"), vec![m], true);
        let bytes = RawFrame::from_batch(&b).encode_to_vec();
        let expected: &[u8] = &[
            0x10, 0x01, // clock_synced = true (source_idx 0 is the default, omitted)
            0x1a, 0x01, b's', // dict[0]
            0x1a, 0x01, b'k', // dict[1]
            0x1a, 0x01, b'v', // dict[2]
            0x22, 0x10, // records[0], 16 bytes
            0x09, 1, 0, 0, 0, 0, 0, 0, 0, // ingest_time_ns sfixed64
            0x12, 0x02, 0x01, 0x02, // attr_refs packed [1, 2]
            0x1a, 0x01, b'P', // payload
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn empty_batch_is_a_valid_frame() {
        let b = RawBatch::new(SourceId::new("s"), vec![], false);
        let bytes = Bytes::from(RawFrame::from_batch(&b).encode_to_vec());
        assert_eq!(to_raw_batch(&decode(bytes).unwrap()), b);
    }

    #[test]
    fn invalid_references_are_rejected() {
        let mut f = RawFrame::from_batch(&batch());
        f.source_idx = 99;
        assert!(decode(Bytes::from(f.encode_to_vec())).is_err());
        let mut f = RawFrame::from_batch(&batch());
        f.records[0].attr_refs.push(0);
        assert!(decode(Bytes::from(f.encode_to_vec())).is_err());
        assert!(decode(Bytes::from_static(&[0xff, 0xff])).is_err());
    }
}
