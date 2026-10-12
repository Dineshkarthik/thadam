//! `capture` subcommand: subscribe the topic matrix, decode, and measure.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thadam_core::Record;
use thadam_ros2_msg::CompiledMessage;

use crate::bridge_impl::BridgeTransport;
use crate::metrics::{self, RunParams, TopicStats};
use crate::msgs;
use crate::transport::{
    Durability, LiveRos2Transport, QosProfile, RawRos2Message, TopicAdvert, TransportError,
};
use crate::zenoh_impl::ZenohTransport;

pub struct CaptureArgs {
    pub endpoint: String,
    pub domain: u32,
    pub duration_secs: u64,
    /// Zenoh session mode: "client" or "peer" (see zenoh_impl::ZenohTransport::connect).
    pub mode: String,
    /// `rmw` = step-1 raw rmw_zenoh mapping; `bridge` = step-2
    /// zenoh-bridge-ros2dds mapping. Both run the identical metrics/decode
    /// pipeline — this dispatch is the whole "one mapping layer, two
    /// transports" answer (see the spike report §11).
    pub transport: String,
    pub bridge_zid: String,
    pub bridge_prefix: String,
    pub slow_ms: u64,
    pub fifo_cap: usize,
    pub only: Option<String>,
    pub clock_offset_ns: i64,
    /// Force all subscriptions to VOLATILE: proves that /tf_static is NOT
    /// received live (the late-join query is what makes transient_local work
    /// on rmw_zenoh; on the bridge the replay arrives regardless, which is
    /// itself the measurement).
    pub no_late_join: bool,
}

/// The two candidate mappings side by side behind the spike trait. Capture
/// and metrics are mapping-agnostic; only connect()/subscribe()/discover()
/// differ.
enum Transport {
    Rmw(ZenohTransport),
    Bridge(BridgeTransport),
}

#[allow(async_fn_in_trait)]
impl LiveRos2Transport for Transport {
    async fn subscribe(
        &self,
        topic: &str,
        ros_type: Option<&str>,
        qos: QosProfile,
    ) -> Result<tokio::sync::mpsc::Receiver<RawRos2Message>, TransportError> {
        match self {
            Transport::Rmw(t) => t.subscribe(topic, ros_type, qos).await,
            Transport::Bridge(t) => t.subscribe(topic, ros_type, qos).await,
        }
    }

    async fn discover(&self, settle: Duration) -> Vec<TopicAdvert> {
        match self {
            Transport::Rmw(t) => t.discover(settle).await,
            Transport::Bridge(t) => t.discover(settle).await,
        }
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Pull a comparable source stamp out of a decoded record: the
/// `header.stamp` `Ts` value for Imu/JointState, or the first transform's
/// stamp for TFMessage. Returns None when absent.
fn extract_header_stamp_ns(record: &Record) -> Option<i64> {
    if let Some(ts) = record.get("header.stamp").and_then(|v| v.as_ts()) {
        return Some(ts.as_nanos());
    }
    let transforms = record.get("transforms").and_then(|v| v.as_list())?;
    transforms
        .first()
        .and_then(|v| v.as_record())
        .and_then(|r| r.get("header.stamp"))
        .and_then(|v| v.as_ts())
        .map(|ts| ts.as_nanos())
}

/// Extract the payload-embedded exact sequence counter (docker/sensor_pub.py)
/// from a decoded record. This is the step-2 drop-accounting basis through
/// zenoh-bridge-ros2dds, which forwards no per-publisher sequence number:
/// imu → `angular_velocity.x`, joint_states → last `effort` element,
/// tf → `transforms[0].transform.rotation.x`. Fields are float64 in the CDR,
/// exact for integer counters far past any spike run length.
fn extract_payload_seq(topic: &str, record: &Record) -> Option<i64> {
    match topic {
        "/imu" => record
            .get("angular_velocity.x")
            .and_then(|v| v.as_f64())
            .map(|f| f as i64),
        "/joint_states" => record
            .get("effort")
            .and_then(|v| v.as_f64s())
            .and_then(|a| a.last())
            .map(|f| *f as i64),
        "/tf" | "/tf_static" => record
            .get("transforms")
            .and_then(|v| v.as_list())?
            .first()?
            .as_record()?
            .get("transform.rotation.x")?
            .as_f64()
            .map(|f| f as i64),
        _ => None,
    }
}

pub async fn run(args: CaptureArgs) -> Result<String, String> {
    let scenario = if args.slow_ms > 0 {
        format!(
            "overload slow_ms={} fifo_cap={}",
            args.slow_ms, args.fifo_cap
        )
    } else if args.no_late_join {
        "volatile-no-late-join".to_string()
    } else {
        "baseline".to_string()
    };

    let topics: Vec<_> = match &args.only {
        Some(list) => {
            let wanted: Vec<&str> = list.split(',').map(str::trim).collect();
            msgs::TOPICS
                .iter()
                .filter(|t| wanted.contains(&t.topic))
                .collect()
        }
        None => msgs::TOPICS.iter().collect(),
    };
    if topics.is_empty() {
        return Err(format!("no topics selected (only={:?})", args.only));
    }

    let mut registry = msgs::build_registry();
    let compiled: Vec<Arc<CompiledMessage>> = topics
        .iter()
        .map(|t| msgs::compile(&mut registry, t.ros_type))
        .collect();

    let bridge_mode = args.transport == "bridge";
    let transport = match args.transport.as_str() {
        "rmw" => Transport::Rmw(
            ZenohTransport::connect(&args.endpoint, args.domain, args.fifo_cap, &args.mode).await?,
        ),
        "bridge" => Transport::Bridge(
            BridgeTransport::connect(
                &args.endpoint,
                &args.bridge_prefix,
                args.fifo_cap,
                &args.mode,
                if args.bridge_zid.is_empty() {
                    None
                } else {
                    Some(args.bridge_zid.clone())
                },
            )
            .await?,
        ),
        other => return Err(format!("unknown transport {other:?} (expected rmw|bridge)")),
    };

    // Discovery snapshot for the report.
    let adverts = transport.discover(Duration::from_secs(3)).await;
    let adverts_json =
        serde_json::to_string_pretty(&adverts).map_err(|e| format!("serialize discovery: {e}"))?;
    eprintln!(
        "[discover] {} advertised topics:\n{}",
        adverts.len(),
        adverts_json
    );

    let started_ns = now_ns();
    let mut stats: Vec<TopicStats> = topics
        .iter()
        .map(|t| TopicStats::new(t.topic, t.ros_type, t.nominal_hz))
        .collect();

    // Fan-in: one pump task per subscription sends (index, msg) into a
    // bounded channel; small capacity so a slow collector back-pressures
    // into the zenoh receive channel (which then back-pressures the
    // router-side session queue, where best_effort samples drop).
    let (fan_tx, mut fan_rx) = tokio::sync::mpsc::channel::<(usize, RawRos2Message)>(32);
    let mut n_subs = 0;
    for (i, spec) in topics.iter().enumerate() {
        let mut qos = spec.qos;
        if args.no_late_join {
            qos.durability = Durability::Volatile;
        }
        let mut rx = transport
            .subscribe(spec.topic, Some(spec.ros_type), qos)
            .await
            .map_err(|e| format!("subscribe {}: {e}", spec.topic))?;
        n_subs += 1;
        let fan_tx = fan_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if fan_tx.send((i, msg)).await.is_err() {
                    return;
                }
            }
        });
    }
    drop(fan_tx);
    eprintln!(
        "[capture] subscribed {} topic(s) for {}s (scenario: {scenario})",
        n_subs, args.duration_secs
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.duration_secs);
    let mut last_progress = tokio::time::Instant::now();
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            break;
        }
        let Some((i, msg)) = tokio::time::timeout(remaining, fan_rx.recv())
            .await
            .ok()
            .flatten()
        else {
            break;
        };
        if args.slow_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.slow_ms)).await;
        }
        let topic_stats = &mut stats[i];
        let mut payload_seq: Option<i64> = None;
        let header_stamp_ns = match compiled[i].decode(&msg.payload) {
            Ok(record) => {
                if topic_stats.first_decoded.is_none() {
                    topic_stats.first_decoded = Some(format!("{record:?}"));
                }
                if bridge_mode {
                    payload_seq = extract_payload_seq(topic_stats.topic.as_str(), &record);
                }
                extract_header_stamp_ns(&record)
            }
            Err(e) => {
                topic_stats.record_decode_error(e.code.to_string());
                None
            }
        };
        let offset = args.clock_offset_ns;
        let lat_attachment = (msg.source_timestamp_ns >= 0)
            .then(|| msg.recv_time_ns - (msg.source_timestamp_ns + offset));
        let lat_header = header_stamp_ns.map(|h| msg.recv_time_ns - (h + offset));
        topic_stats.record_message(&msg, lat_attachment, lat_header, payload_seq);

        if last_progress.elapsed() >= Duration::from_secs(10) {
            last_progress = tokio::time::Instant::now();
            let counts: Vec<String> = stats
                .iter()
                .map(|s| format!("{}={}", s.topic, s.received))
                .collect();
            eprintln!("[progress] {}", counts.join(" "));
        }
    }

    let finished_ns = now_ns();
    let params = RunParams {
        transport: args.transport.clone(),
        duration_secs: args.duration_secs,
        endpoint: args.endpoint.clone(),
        session_mode: args.mode.clone(),
        domain: args.domain,
        clock_offset_ns: args.clock_offset_ns,
        slow_ms: args.slow_ms,
        fifo_cap: args.fifo_cap,
        started_ns,
        finished_ns,
    };
    let summary = metrics::summarize(&scenario, &params, &stats);
    serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize summary: {e}"))
}
