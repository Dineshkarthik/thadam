use std::time::Duration;

use bytes::Bytes;
use rumqttc::{MqttOptions, Publish, QoS};

pub const MAGIC: u64 = 0x5448_4144_4d5f_5332; // "THADM_S2"

#[derive(Clone, Debug)]
pub struct Target {
    pub host: String,
    pub port: u16,
}

pub fn rand_id(prefix: &str) -> String {
    format!("{prefix}-{:08x}", rand::random::<u32>())
}

pub struct OptsBuilder {
    pub id: String,
    pub target: Target,
    pub keep_alive_s: u64,
    pub clean: bool,
    pub manual_acks: bool,
    pub inflight: u16,
    pub max_packet: usize,
}

impl OptsBuilder {
    pub fn new(id: impl Into<String>, target: &Target) -> Self {
        Self {
            id: id.into(),
            target: target.clone(),
            keep_alive_s: 30,
            clean: true,
            manual_acks: false,
            inflight: 100,
            max_packet: 4 * 1024 * 1024,
        }
    }
    pub fn keep_alive(mut self, s: u64) -> Self {
        self.keep_alive_s = s;
        self
    }
    pub fn clean(mut self, c: bool) -> Self {
        self.clean = c;
        self
    }
    pub fn manual_acks(mut self, m: bool) -> Self {
        self.manual_acks = m;
        self
    }
    pub fn inflight(mut self, i: u16) -> Self {
        self.inflight = i;
        self
    }
    pub fn build(self) -> MqttOptions {
        let mut o = MqttOptions::new(self.id, self.target.host, self.target.port);
        o.set_keep_alive(Duration::from_secs(self.keep_alive_s));
        o.set_clean_session(self.clean);
        o.set_manual_acks(self.manual_acks);
        o.set_inflight(self.inflight);
        o.set_max_packet_size(self.max_packet, self.max_packet);
        o
    }
}

/// Payload layout: 8-byte BE sequence, 8-byte magic, zero padding up to `size`.
pub fn make_payload(seq: u64, size: usize) -> Bytes {
    let size = size.max(16);
    let mut v = vec![0u8; size];
    v[..8].copy_from_slice(&seq.to_be_bytes());
    v[8..16].copy_from_slice(&MAGIC.to_be_bytes());
    Bytes::from(v)
}

pub fn parse_seq(payload: &[u8]) -> Option<u64> {
    if payload.len() < 16 {
        return None;
    }
    let seq = u64::from_be_bytes(payload[..8].try_into().ok()?);
    let magic = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    (magic == MAGIC).then_some(seq)
}

/// A publish with the same pkid/qos but no topic and no payload: all that
/// `AsyncClient::ack` needs.
pub fn strip(p: &Publish) -> Publish {
    Publish {
        dup: false,
        qos: p.qos,
        retain: false,
        topic: String::new(),
        pkid: p.pkid,
        payload: Bytes::new(),
    }
}

pub fn qos_from(n: u8) -> QoS {
    match n {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

pub fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

pub fn json_line(v: serde_json::Value) {
    println!("{}", serde_json::to_string(&v).unwrap());
}

pub fn rss_kib() -> u64 {
    let pid = std::process::id().to_string();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}
