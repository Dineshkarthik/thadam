//! zenoh-bridge-ros2dds mapping of [`LiveRos2Transport`] (spike step 2).
//!
//! Facts verified live 2026-10-12 (zenoh 1.10.1 client ↔
//! `eclipse/zenoh-bridge-ros2dds:1.10.1` in router mode on tcp/0.0.0.0:7447,
//! Fast DDS `rmw_fastrtps_cpp` publishers on `ros:jazzy-ros-base`; raw
//! evidence in `results/sniff-bridge-*.jsonl` and
//! `results/probe-bridge-admin*.json`):
//!
//! * DATA KEY EXPRESSION: the BARE topic name without the leading `/`
//!   (`imu`, `tf_static`, `rosout`, ...). With the plugin `namespace: "/"`
//!   default there is NO domain chunk, NO type name, NO RIHS01 type hash and
//!   NO publisher GID in the data key. Two publishers on one ROS topic share
//!   one zenoh key. A non-root `namespace` would prefix the keys.
//! * NO zenoh attachment on any sample (0 in ~13k sniffed samples) and the
//!   HLC timestamp present on every sample is stamped by the BRIDGE at
//!   ingress (its own container clock, `.../57a9ed`), NOT the DDS publish
//!   time — useless for end-to-end latency; capture.rs uses the decoded
//!   `header.stamp` instead (step 1 cross-checked header stamps against the
//!   rmw_zenoh attachment stamps to <0.1 ms).
//! * NO per-publisher sequence number reaches zenoh (DDS writer sequence
//!   numbers are not forwarded; zenoh SourceInfo/source_sn is not set by the
//!   bridge — and its accessor is unstable-gated client-side, out of the
//!   spike's dep policy). Exact drop accounting from transport metadata
//!   alone is IMPOSSIBLE through the bridge. The spike embeds an exact
//!   monotone counter in the publisher payload (docker/sensor_pub.py),
//!   extracted after CDR decode by capture.rs — measuring the same
//!   end-to-end quantity the rmw_zenoh attachment measures.
//! * TRANSIENT_LOCAL LATE JOIN: no client query at all. The bridge keeps a
//!   DDS-side cache (admin config `transient_local_cache_multiplier: 10` ×
//!   KEEP_LAST depth) and REPLAYS cached samples as ordinary Puts when a new
//!   zenoh subscriber's interest arrives. Verified: a plain `**` subscriber
//!   joining minutes after the one-shot /tf_static publish received exactly
//!   one replayed Put; queries on the data key (`tf_static`, `tf_static/**`)
//!   return 0 replies (no history queryable — probe candidates 6–8).
//!   Replay-addressing (new subscriber only vs broadcast) is measured by the
//!   duo-sniff run documented in the spike report.
//! * DISCOVERY: NO `@ros2_lv` liveliness tokens (those are rmw_zenoh session
//!   internals; probe control candidate returned 0 replies). The bridge
//!   exposes its graph in the ADMIN SPACE under `@/<bridge_zid>/ros2/**`,
//!   served by queryables (admin read=true by default):
//!   - `@/<zid>/ros2/config` and `.../version` (plugin config JSON,
//!     "v1.10.1"),
//!   - `@/<zid>/ros2/route/topic/{pub,sub}/<topic>` — payload JSON has
//!     `ros2_name`, `ros2_type`, `zenoh_key_expr`, `is_active`,
//!     `local_nodes`, `remote_routes` (topic discovery),
//!   - `@/<zid>/ros2/dds/<participant>/{writer,reader}/<gid>/{rt,rq,rr}/<n>`
//!     — payload JSON has `topic_name`, `type_name`, full QoS, and (Jazzy
//!     Fast DDS) the endpoint `user_data` bytes containing
//!     `typehash=RIHS01_<64hex>`; this is the only place the type hash
//!     survives across the bridge.
//!
//! `@`-chunk key expressions are hermetic to `*`/`**`, so the bridge zid
//! must be known: the spike pins it with `-i 57a9ed` (docker/run.sh);
//! production would read it from config or scouted hellos.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use crate::transport::{
    Durability, LiveRos2Transport, QosProfile, RawRos2Message, TopicAdvert, TransportError,
};

/// Samples on a transient_local topic arriving within this window after the
/// subscription is declared are the bridge's cache replay, not live DDS
/// traffic (its one-shot publisher fired before the capture starts).
/// Validated by the late-join runs: exactly one replayed sample per
/// subscription, no false positives on live traffic.
const REPLAY_WINDOW: Duration = Duration::from_secs(1);

/// Backpressure sizing between the zenoh pump task and the metrics
/// collector (mirrors zenoh_impl).
const PUMP_CHANNEL_CAP: usize = 4096;

pub struct BridgeTransport {
    session: zenoh::Session,
    /// Zenoh key-expression prefix produced by the bridge `namespace` option
    /// ("" for the default root namespace, so keys are bare topic names).
    key_prefix: String,
    fifo_cap: usize,
    /// Bridge zenoh id (admin-space root `@/<zid>/ros2`); None disables
    /// admin-space discovery.
    admin_zid: Option<String>,
}

impl BridgeTransport {
    /// Open a zenoh session connected to the bridge's tcp listener.
    /// Client mode for the same reason as zenoh_impl (no listener to dial
    /// back through Docker Desktop's NAT; the router brokers everything).
    pub async fn connect(
        endpoint: &str,
        key_prefix: &str,
        fifo_cap: usize,
        mode: &str,
        admin_zid: Option<String>,
    ) -> Result<Self, TransportError> {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("mode", &format!("\"{mode}\""))
            .map_err(|e| TransportError::Connect(format!("config mode: {e}")))?;
        config
            .insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]"))
            .map_err(|e| TransportError::Connect(format!("config endpoints: {e}")))?;
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .map_err(|e| TransportError::Connect(format!("config multicast: {e}")))?;
        let session = zenoh::open(config)
            .await
            .map_err(|e| TransportError::Connect(format!("zenoh::open: {e}")))?;
        Ok(BridgeTransport {
            session,
            key_prefix: key_prefix.trim_matches('/').to_string(),
            fifo_cap,
            admin_zid,
        })
    }

    /// The subscription key expression for `topic`: the bare prefix+FQN seen
    /// on the wire (e.g. `imu` with the default root namespace).
    pub fn subscription_keyexpr(&self, topic: &str) -> String {
        let fqn = topic.trim_matches('/');
        if self.key_prefix.is_empty() {
            fqn.to_string()
        } else {
            format!("{}/{fqn}", self.key_prefix)
        }
    }

    /// Map a bridge sample to the transport record, given the subscription's
    /// own (topic, ros_type) context — the bridge key carries no metadata.
    pub fn map_sample(
        sample: &zenoh::sample::Sample,
        topic: &str,
        ros_type: &str,
        sub_start: Instant,
        transient_local: bool,
    ) -> Option<RawRos2Message> {
        let payload = sample.payload().to_bytes().into_owned();
        let late_join = transient_local && sub_start.elapsed() < REPLAY_WINDOW;
        Some(RawRos2Message {
            topic: topic.to_string(),
            type_name: ros_type.to_string(),
            // The bridge puts no RIHS01 hash in the key and no attachment;
            // type identity comes from config / admin space (module docs).
            type_hash: String::new(),
            payload,
            source_timestamp_ns: -1,
            recv_time_ns: now_ns(),
            seq: -1,
            gid: [0u8; 16],
            late_join,
        })
    }
}

impl LiveRos2Transport for BridgeTransport {
    async fn subscribe(
        &self,
        topic: &str,
        ros_type: Option<&str>,
        qos: QosProfile,
    ) -> Result<mpsc::Receiver<RawRos2Message>, TransportError> {
        let ke = self.subscription_keyexpr(topic);
        let subscriber = self
            .session
            .declare_subscriber(ke.clone())
            .with(zenoh::handlers::FifoChannel::new(self.fifo_cap))
            .await
            .map_err(|e| TransportError::Subscribe {
                topic: topic.to_string(),
                reason: format!("declare_subscriber: {e}"),
            })?;
        let (tx, rx) = mpsc::channel(PUMP_CHANNEL_CAP);
        let topic_owned = topic.to_string();
        let type_owned = ros_type.unwrap_or_default().to_string();
        let transient_local = qos.durability == Durability::TransientLocal;
        tokio::spawn(async move {
            let sub_start = Instant::now();
            while let Ok(sample) = subscriber.recv_async().await {
                if sample.kind() != zenoh::sample::SampleKind::Put {
                    continue;
                }
                if let Some(msg) = Self::map_sample(
                    &sample,
                    &topic_owned,
                    &type_owned,
                    sub_start,
                    transient_local,
                ) && tx.send(msg).await.is_err()
                {
                    return;
                }
            }
        });
        // transient_local: deliberately NO query here. The bridge replays
        // its DDS-side cache on new subscriber interest (module docs).
        Ok(rx)
    }

    /// Bridge discovery: one query over the admin space
    /// (`@/<zid>/ros2/**`). Topic adverts come from `route/topic/{pub,sub}`
    /// entries; writer entries additionally fill in the RIHS01 type hash
    /// from the DDS endpoint user_data (Jazzy Fast DDS). Payload schemas
    /// banked from live replies (module docs).
    async fn discover(&self, settle: Duration) -> Vec<TopicAdvert> {
        let Some(zid) = &self.admin_zid else {
            return Vec::new();
        };
        let selector = format!("@/{zid}/ros2/**");
        let Ok(replies) = self
            .session
            .get(selector.as_str())
            .target(zenoh::query::QueryTarget::All)
            .consolidation(zenoh::query::ConsolidationMode::Auto)
            .timeout(settle)
            .await
        else {
            return Vec::new();
        };
        let mut routes: HashMap<String, TopicAdvert> = HashMap::new();
        let mut hash_by_topic: HashMap<String, String> = HashMap::new();
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.into_result() else {
                continue;
            };
            let ke = sample.key_expr().as_str().to_string();
            let Ok(payload) = sample.payload().try_to_string() else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(payload.as_ref()) else {
                continue;
            };
            if ke.contains("/route/topic/") {
                let Some(name) = v.get("ros2_name").and_then(|x| x.as_str()) else {
                    continue;
                };
                let ros_type = v
                    .get("ros2_type")
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .to_string();
                // Publisher count: local nodes plus remote bridge routes.
                let pubs = v
                    .get("local_nodes")
                    .and_then(|x| x.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0)
                    + v.get("remote_routes")
                        .and_then(|x| x.as_array())
                        .map(|a| a.len())
                        .unwrap_or(0);
                let advert = routes
                    .entry(name.to_string())
                    .or_insert_with(|| TopicAdvert {
                        topic: name.to_string(),
                        type_name: ros_type,
                        type_hash: String::new(),
                        qos_keyexpr: v
                            .get("zenoh_key_expr")
                            .and_then(|x| x.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        publishers: 0,
                    });
                advert.publishers = advert.publishers.max(pubs);
            } else if ke.contains("/writer/") {
                let Some(topic_name) = v.get("topic_name").and_then(|x| x.as_str()) else {
                    continue;
                };
                // DDS user_data bytes are an ascii `typehash=RIHS01_<hex>`
                // user parameter on Jazzy Fast DDS endpoints.
                let user_data: Vec<u8> = v
                    .get("qos")
                    .and_then(|q| q.get("user_data"))
                    .and_then(|u| u.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|b| b.as_u64().map(|n| n as u8))
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(hash) = parse_typehash_user_data(&user_data) {
                    let topic = topic_name
                        .strip_prefix("rt/")
                        .map(|t| format!("/{t}"))
                        .unwrap_or_else(|| topic_name.to_string());
                    hash_by_topic.entry(topic).or_insert(hash);
                }
            }
        }
        for advert in routes.values_mut() {
            if advert.type_hash.is_empty()
                && let Some(hash) = hash_by_topic.get(&advert.topic)
            {
                advert.type_hash = hash.clone();
            }
        }
        let mut out: Vec<_> = routes.into_values().collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        out
    }
}

/// Extract `RIHS01_<64hex>` from a DDS endpoint user_data blob shaped like
/// `...typehash=RIHS01_<hex>[;...]` (REP-2016 over Fast DDS user data).
pub fn parse_typehash_user_data(bytes: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(bytes).ok()?;
    let start = text.find("typehash=")? + "typehash=".len();
    let end = text[start..]
        .find(';')
        .map(|i| start + i)
        .unwrap_or(text.len());
    let hash = text[start..end].trim();
    hash.starts_with("RIHS01_").then(|| hash.to_string())
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
