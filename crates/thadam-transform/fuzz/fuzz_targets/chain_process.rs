//! Feeds arbitrary payloads through a compiled demo chain: it must never panic,
//! and every reject must keep the raw bytes.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use thadam_core::{Attributes, RawMessage, SourceId, Timestamp, WalEpoch, WalPos};
use thadam_transform::{Chain, ChainLimits, ChainSpec, FrameInput, RawRecordView};

const SPEC: &str = include_str!("../demo_chain.json");

fuzz_target!(|data: &[u8]| {
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    let spec = serde_json::from_str(SPEC)
        .ok()
        .and_then(|v| ChainSpec::from_value(v).ok())
        .expect("the fuzz spec is valid");
    let limits = ChainLimits::default().with_max_json_depth(16);
    let mut chain = Chain::compile(&spec, limits).expect("the fuzz spec compiles");

    let stream = if selector & 1 == 0 { "telemetry" } else { "joints" };
    let source = SourceId::new("fuzz");
    let mut attrs = Attributes::new();
    attrs.insert("topic", format!("thadam-demo/v1/s/r/{stream}"));
    let message = RawMessage {
        ingest_time: Timestamp::from_secs(1_000).expect("in range"),
        attrs,
        payload: Bytes::copy_from_slice(payload),
    };
    // The same payload twice exercises dedup, on_change, and sampling state.
    let messages = [message.clone(), message];
    let views: Vec<_> = messages
        .iter()
        .enumerate()
        .map(|(i, m)| RawRecordView::new(&source, m, WalEpoch::new(1), WalPos::new(i as u64), 0))
        .collect();
    let input = FrameInput::new(&views, true);
    let mut out = chain.new_output(input.origin().expect("two records"), true);
    let mut rejects = Vec::new();
    chain.process(&input, &mut out, &mut rejects);
    for r in &rejects {
        assert_eq!(r.payload.as_ref(), payload);
    }
    for b in &out.batches {
        for e in &b.batch.events {
            if let Some(rec) = e.payload().as_record() {
                assert!(rec.validate().is_ok());
            }
        }
    }
});
