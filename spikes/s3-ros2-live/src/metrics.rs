//! Per-topic capture statistics: counts, decode errors, sequence-gap based
//! drop accounting, and latency histograms.

use std::collections::HashMap;

use hdrhistogram::Histogram;
use serde::Serialize;

#[derive(Default)]
struct SenderStats {
    first_seq: Option<i64>,
    last_seq: Option<i64>,
    received: u64,
    /// Samples whose (gid, seq) was seen twice (query + live overlap).
    duplicates: u64,
    gap_events: u64,
    gap_missing_total: u64,
}

#[derive(Serialize)]
pub struct SenderSummary {
    gid: String,
    first_seq: i64,
    last_seq: i64,
    expected: i64,
    received: u64,
    missing: i64,
    duplicates: u64,
    gap_events: u64,
}

pub struct TopicStats {
    pub topic: String,
    pub ros_type: String,
    pub nominal_hz: f64,
    pub received: u64,
    pub decode_errors: u64,
    pub late_join_received: u64,
    /// Samples whose latency was skipped (missing stamp) — counted, not dropped.
    pub no_stamp: u64,
    /// attachment source_ts -> recv latency
    lat_attachment: Histogram<u64>,
    /// decoded header.stamp -> recv latency
    lat_header: Histogram<u64>,
    senders: HashMap<[u8; 16], SenderStats>,
    /// The first successfully decoded record of the run (evidence), Debug text.
    pub first_decoded: Option<String>,
    /// Up to 3 captured decode error strings (codes only).
    pub decode_error_samples: Vec<String>,
    /// Type hash observed on this topic's samples (last writer wins; all
    /// samples for a topic must agree in a healthy graph).
    pub observed_type_hash: Option<String>,
}

fn latency_histogram() -> Histogram<u64> {
    // 1 us .. 10 min, 3 significant digits.
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("static histogram bounds are valid")
}

impl TopicStats {
    pub fn new(topic: &str, ros_type: &str, nominal_hz: f64) -> Self {
        TopicStats {
            topic: topic.to_string(),
            ros_type: ros_type.to_string(),
            nominal_hz,
            received: 0,
            decode_errors: 0,
            late_join_received: 0,
            no_stamp: 0,
            lat_attachment: latency_histogram(),
            lat_header: latency_histogram(),
            senders: HashMap::new(),
            first_decoded: None,
            decode_error_samples: Vec::new(),
            observed_type_hash: None,
        }
    }

    /// Account one received message. Latency arguments must already be
    /// offset-corrected by the caller; unrepresentable values (no stamp or
    /// negative latency) are counted in `no_stamp`.
    pub fn record_message(
        &mut self,
        msg: &crate::transport::RawRos2Message,
        lat_attachment_ns: Option<i64>,
        lat_header_ns: Option<i64>,
    ) {
        self.received += 1;
        if msg.late_join {
            self.late_join_received += 1;
        }
        if !msg.type_hash.is_empty() {
            self.observed_type_hash = Some(msg.type_hash.clone());
        }
        match lat_attachment_ns {
            Some(ns) if ns >= 0 => {
                let _ = self.lat_attachment.record(ns as u64);
            }
            _ => self.no_stamp += 1,
        }
        if let Some(ns) = lat_header_ns
            && ns >= 0
        {
            let _ = self.lat_header.record(ns as u64);
        }
        if msg.seq >= 0 {
            let sender = self.senders.entry(msg.gid).or_default();
            if sender.first_seq.is_none() {
                sender.first_seq = Some(msg.seq);
            }
            if let Some(last) = sender.last_seq {
                if msg.seq > last + 1 {
                    sender.gap_events += 1;
                    sender.gap_missing_total += (msg.seq - last - 1) as u64;
                } else if msg.seq <= last {
                    sender.duplicates += 1;
                }
            }
            sender.last_seq = Some(sender.last_seq.map_or(msg.seq, |l| l.max(msg.seq)));
            sender.received += 1;
        }
    }

    pub fn record_decode_error(&mut self, code: String) {
        self.decode_errors += 1;
        if self.decode_error_samples.len() < 3 && !self.decode_error_samples.contains(&code) {
            self.decode_error_samples.push(code);
        }
    }

    /// (expected, missing) across all senders, from sequence numbers.
    fn drop_accounting(&self) -> (i64, i64) {
        let mut expected = 0i64;
        let mut missing = 0i64;
        for s in self.senders.values() {
            if let (Some(first), Some(last)) = (s.first_seq, s.last_seq) {
                expected += last - first + 1;
                missing += (last - first + 1) - s.received as i64 - (s.duplicates as i64);
            }
        }
        (expected, missing)
    }
}

#[derive(Serialize)]
pub struct TopicSummary {
    pub topic: String,
    pub ros_type: String,
    pub nominal_hz: f64,
    pub observed_type_hash: Option<String>,
    pub received: u64,
    pub late_join_received: u64,
    pub decode_errors: u64,
    pub decode_error_samples: Vec<String>,
    pub expected_by_seq: i64,
    pub missing_by_seq: i64,
    pub gap_events: u64,
    pub senders: Vec<SenderSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_decoded: Option<String>,
    pub latency_attachment_ns: LatSummary,
    pub latency_header_ns: LatSummary,
}

#[derive(Serialize)]
pub struct LatSummary {
    pub count: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

impl From<&Histogram<u64>> for LatSummary {
    fn from(h: &Histogram<u64>) -> Self {
        LatSummary {
            count: h.len(),
            p50_ns: h.value_at_percentile(50.0),
            p90_ns: h.value_at_percentile(90.0),
            p99_ns: h.value_at_percentile(99.0),
            max_ns: h.max(),
        }
    }
}

#[derive(Serialize)]
pub struct RunSummary {
    pub scenario: String,
    pub duration_secs: u64,
    pub endpoint: String,
    pub session_mode: String,
    pub domain: u32,
    pub clock_offset_ns: i64,
    pub slow_ms: u64,
    pub fifo_cap: usize,
    pub started_ns: i64,
    pub finished_ns: i64,
    pub topics: Vec<TopicSummary>,
}

pub fn summarize(scenario: &str, run: &RunParams, stats: &[TopicStats]) -> RunSummary {
    RunSummary {
        scenario: scenario.to_string(),
        duration_secs: run.duration_secs,
        endpoint: run.endpoint.clone(),
        session_mode: run.session_mode.clone(),
        domain: run.domain,
        clock_offset_ns: run.clock_offset_ns,
        slow_ms: run.slow_ms,
        fifo_cap: run.fifo_cap,
        started_ns: run.started_ns,
        finished_ns: run.finished_ns,
        topics: stats
            .iter()
            .map(|t| {
                let (expected, missing) = t.drop_accounting();
                TopicSummary {
                    topic: t.topic.clone(),
                    ros_type: t.ros_type.clone(),
                    nominal_hz: t.nominal_hz,
                    observed_type_hash: t.observed_type_hash.clone(),
                    received: t.received,
                    late_join_received: t.late_join_received,
                    decode_errors: t.decode_errors,
                    decode_error_samples: t.decode_error_samples.clone(),
                    expected_by_seq: expected,
                    missing_by_seq: missing,
                    gap_events: t.senders.values().map(|s| s.gap_events).sum(),
                    senders: t
                        .senders
                        .iter()
                        .map(|(gid, s)| SenderSummary {
                            gid: crate::transport::hex(gid),
                            first_seq: s.first_seq.unwrap_or(-1),
                            last_seq: s.last_seq.unwrap_or(-1),
                            expected: s
                                .first_seq
                                .zip(s.last_seq)
                                .map(|(f, l)| l - f + 1)
                                .unwrap_or(0),
                            received: s.received,
                            missing: s.first_seq.zip(s.last_seq).map_or(0, |(f, l)| {
                                (l - f + 1) - s.received as i64 - s.duplicates as i64
                            }),
                            duplicates: s.duplicates,
                            gap_events: s.gap_events,
                        })
                        .collect(),
                    first_decoded: t.first_decoded.clone(),
                    latency_attachment_ns: LatSummary::from(&t.lat_attachment),
                    latency_header_ns: LatSummary::from(&t.lat_header),
                }
            })
            .collect(),
    }
}

pub struct RunParams {
    pub duration_secs: u64,
    pub endpoint: String,
    pub session_mode: String,
    pub domain: u32,
    pub clock_offset_ns: i64,
    pub slow_ms: u64,
    pub fifo_cap: usize,
    pub started_ns: i64,
    pub finished_ns: i64,
}
