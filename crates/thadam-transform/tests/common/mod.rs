//! Shared fixtures: the demo datasets, a frame builder, and a runner.
#![allow(dead_code, missing_docs, clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use serde_json::{Value, json};
use thadam_core::{Attributes, Event, RawMessage, SourceId, Timestamp, WalEpoch, WalPos};
use thadam_transform::{
    Chain, ChainLimits, ChainOutput, ChainSpec, FrameInput, RawRecordView, RejectedRaw,
};

pub const SOURCE: &str = "fleet-mqtt";
pub const EPOCH: u64 = 7;

pub fn telemetry_dataset() -> Value {
    json!({
        "id": "telemetry",
        "match": { "source": SOURCE, "topic": "thadam-demo/v1/+/+/telemetry" },
        "decode": "json",
        "identity": [{ "site": "{topic.2}" }, { "robot": "{topic.3}" }],
        "event_id": { "key": ["site", "robot", "seq"] },
        "dedup": { "window": "10m", "max_keys": "500k" },
        "time": { "field": "/ts_us", "unit": "us", "domain": "utc", "max_skew": "5s" },
        "fields": [
            { "name": "seq", "type": "u64", "from": "/seq", "required": true },
            { "name": "soc_pct", "type": "f32", "from": "/soc_pct" },
            { "name": "voltage_v", "type": "f32", "from": "/voltage_mv", "unit": "mV", "convert_to": "V" },
            { "name": "current_a", "type": "f32", "from": "/current_ma", "unit": "mA", "convert_to": "A" },
            { "name": "cpu_temp_c", "type": "f32", "from": "/cpu_temp_dc", "unit": "dCel", "convert_to": "Cel" },
            { "name": "motor_temp_c", "type": "list<f32>", "from": "/motor_temp_dc",
              "unit": "dCel", "convert_to": "Cel" }
        ]
    })
}

pub fn joints_dataset() -> Value {
    json!({
        "id": "joints",
        "match": { "source": SOURCE, "topic": "thadam-demo/v1/+/+/joints" },
        "decode": "json",
        "identity": [{ "site": "{topic.2}" }, { "robot": "{topic.3}" }],
        "event_id": { "key": ["site", "robot", "seq"] },
        "dedup": { "window": "10m", "max_keys": "500k" },
        "explode": { "path": "/samples", "index_field": "sample_idx" },
        "time": { "field": "/ts_us", "unit": "us", "domain": "utc", "max_skew": "5s" },
        "fields": [
            { "name": "seq", "type": "u64", "from": "^/seq" },
            { "name": "position_rad", "type": "list<f64>", "from": "/pos" },
            { "name": "velocity_rad_s", "type": "list<f64>", "from": "/vel" },
            { "name": "effort_nm", "type": "list<f64>", "from": "/eff" }
        ]
    })
}

pub fn state_dataset() -> Value {
    json!({
        "id": "state",
        "match": { "source": SOURCE, "topic": "thadam-demo/v1/+/+/state" },
        "identity": [{ "site": "{topic.2}" }, { "robot": "{topic.3}" }],
        "time": { "field": "/ts_us", "unit": "us", "domain": "utc" },
        "fields": [
            { "name": "mode", "type": "str" },
            { "name": "cycle_id", "type": "u64" },
            { "name": "program", "type": "str" }
        ]
    })
}

pub fn events_dataset() -> Value {
    json!({
        "id": "events",
        "match": { "source": SOURCE, "topic": "thadam-demo/v1/+/+/events" },
        "identity": [{ "site": "{topic.2}" }, { "robot": "{topic.3}" }],
        "event_id": { "key": ["site", "robot", "seq"] },
        "time": { "field": "/ts_us", "unit": "us", "domain": "utc" },
        "fields": [
            { "name": "seq", "type": "u64", "required": true },
            { "name": "level", "type": "str" },
            { "name": "code", "type": "str" },
            { "name": "message", "type": "str" }
        ]
    })
}

pub fn demo_transforms() -> Value {
    json!([
        { "kind": "validate", "dataset": "telemetry",
          "rules": [{ "field": "motor_temp_c", "min": -40, "max": 150,
                      "flag": "OUT_OF_RANGE", "status": "bad" }] },
        { "kind": "on_change", "dataset": "state",
          "fields": ["mode", "cycle_id", "program"], "heartbeat": "5m" },
        { "kind": "filter", "dataset": "events",
          "where": { "field": "level", "ne": "debug" } }
    ])
}

pub fn demo_spec() -> ChainSpec {
    ChainSpec::from_value(json!({
        "datasets": [telemetry_dataset(), joints_dataset(), state_dataset(), events_dataset()],
        "transforms": demo_transforms(),
    }))
    .unwrap()
}

pub fn demo_chain() -> Chain {
    Chain::compile(&demo_spec(), ChainLimits::default()).unwrap()
}

pub fn telemetry_payload(seq: u64, ts_us: i64) -> String {
    format!(
        r#"{{"v":1,"seq":{seq},"ts_us":{ts_us},"soc_pct":81.5,"voltage_mv":47910,"current_ma":-3120,"cpu_temp_dc":612,"motor_temp_dc":[412,398,455,430,401,388,377]}}"#
    )
}

pub fn joints_payload(seq: u64, base_ts_us: i64, samples: usize) -> String {
    let one = |i: usize| {
        format!(
            r#"{{"ts_us":{},"pos":[0.1,-1.2,0.8,0.0,1.4,-0.3,0.7],"vel":[0,0,0,0,0,0,0],"eff":[1,1,1,1,1,1,1]}}"#,
            base_ts_us + 10_000 * i as i64
        )
    };
    let samples: Vec<String> = (0..samples).map(one).collect();
    format!(r#"{{"v":1,"seq":{seq},"samples":[{}]}}"#, samples.join(","))
}

pub fn message(topic: &str, payload: impl Into<Bytes>, ingest_us: i64) -> RawMessage {
    let mut attrs = Attributes::new();
    attrs.insert("topic", topic);
    RawMessage {
        ingest_time: Timestamp::from_micros(ingest_us).unwrap(),
        attrs,
        payload: payload.into(),
    }
}

pub fn topic(robot: &str, stream: &str) -> String {
    format!("thadam-demo/v1/site-a/{robot}/{stream}")
}

/// Owned frames: one record per WAL position, in order.
pub struct Frames {
    pub source: SourceId,
    pub records: Vec<(u64, u32, RawMessage)>,
}

impl Frames {
    pub fn new() -> Self {
        Frames {
            source: SourceId::new(SOURCE),
            records: Vec::new(),
        }
    }

    pub fn push(&mut self, msg: RawMessage) -> &mut Self {
        let pos = self.records.len() as u64;
        self.records.push((pos, 0, msg));
        self
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> Frames {
        Frames {
            source: self.source.clone(),
            records: self.records[range].to_vec(),
        }
    }

    pub fn views(&self) -> Vec<RawRecordView<'_>> {
        self.records
            .iter()
            .map(|(pos, idx, m)| {
                RawRecordView::new(
                    &self.source,
                    m,
                    WalEpoch::new(EPOCH),
                    WalPos::new(*pos),
                    *idx,
                )
            })
            .collect()
    }
}

pub struct Run {
    pub out: ChainOutput,
    pub rejects: Vec<RejectedRaw>,
}

impl Run {
    pub fn events(&self, dataset: &str) -> &[Event] {
        &self.out.batch(dataset).unwrap().events
    }

    /// Every dataset's events, flattened in dataset order.
    pub fn all_events(&self) -> Vec<Event> {
        self.out
            .batches
            .iter()
            .flat_map(|b| b.batch.events.iter().cloned())
            .collect()
    }
}

pub fn run(chain: &mut Chain, frames: &Frames) -> Run {
    let views = frames.views();
    let input = FrameInput::new(&views, true);
    let origin = input
        .origin()
        .unwrap_or_else(|| thadam_core::WalRange::single(WalEpoch::new(EPOCH), WalPos::new(0)));
    let mut out = chain.new_output(origin, true);
    let mut rejects = Vec::new();
    chain.process(&input, &mut out, &mut rejects);
    Run { out, rejects }
}

pub fn warm(chain: &mut Chain, frames: &Frames) {
    let views = frames.views();
    chain.warm(&FrameInput::new(&views, true));
}
