//! Raw-zenoh implementation of [`LiveRos2Transport`] for rmw_zenoh (Jazzy).
//!
//! rmw_zenoh key expressions and attachments follow
//! `rmw_zenoh_cpp/docs/design.md` (jazzy branch), verified live by the
//! `sniff` and `probe` subcommands (zenoh 1.10.1 client vs
//! ros-jazzy-rmw-zenoh-cpp 0.2.11, 2026-10-12):
//!
//! * Data key expression: `<domain_id>/<fqn w/o leading />/<dds_type>/<hash>`
//!   e.g. `0/imu/sensor_msgs::msg::dds_::Imu_/RIHS01_<64 hex>`.
//!   A subscriber on `<domain>/<fqn>/**` matches any type/hash and learns
//!   both from the concrete key of each sample.
//! * Liveliness token:
//!   `@ros2_lv/<domain>/<session_id>/<node_id>/<entity_id>/<kind>/...`
//!   with `*`/`**` never matching the hermetic `@ros2_lv` prefix chunk, so
//!   plain data subscribers never see tokens.
//! * Attachment: 33 bytes = int64-LE sequence number, int64-LE source
//!   timestamp (ns since epoch), 1 byte GID length (16), 16-byte GID.
//! * transient_local late join: rmw_zenoh's zenoh-ext AdvancedPublisher
//!   keeps an AdvancedCache answered by a queryable at
//!   `<data_ke>/@adv/pub/<zid>/<eid>(/uhlc)/`. Recovering history needs the
//!   same strictures zenoh-ext's AdvancedSubscriber uses:
//!     - the query key expression must name the `@adv` chunk literally
//!       (`*`/`**` never match `@`-leading chunks),
//!     - `accept_replies(ReplyKeyExpr::Any)` (selector param `_anyke`), or
//!       both the cache-side and the session-side
//!       "reply must intersect the query" checks reject every reply
//!       (verified: without it the cache callback's reply_sample() bails),
//!     - `ConsolidationMode::None` (monotonic consolidation would drop the
//!       replay as a duplicate of a live sample sharing the key),
//!     - QueryTarget::All (the cache declares no completeness flag,
//!       so AllComplete excludes it),
//!     - scope the selector to ONE topic. A graph-wide `**/@adv/**` query
//!       hits every cached publisher with rejected-reply work per cached
//!       sample (hundreds of RX-thread warnings, publisher session killed
//!       once at 09:19 this run).

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use crate::transport::{
    Durability, LiveRos2Transport, QosProfile, RawRos2Message, TopicAdvert, TransportError, hex,
};

/// rmw_zenoh liveliness prefix (hermetic: `*`/`**` never match this chunk).
pub const LIVELINESS_PREFIX: &str = "@ros2_lv";

pub struct ZenohTransport {
    session: zenoh::Session,
    domain: u32,
    /// Capacity of the per-subscription receive channel inside zenoh.
    fifo_cap: usize,
}

impl ZenohTransport {
    /// Open a zenoh session connected to `endpoint` (e.g.
    /// `tcp/127.0.0.1:7447` = a rmw_zenohd router).
    ///
    /// Mode note (empirical, 2026-10-12, zenoh 1.10.1 vs Jazzy rmw_zenoh):
    /// use CLIENT mode. A client attached to rmw_zenohd is brokered by the
    /// router (subscriptions routed through it) and receives all topics at
    /// full rate; the client never advertises a listener, so there is
    /// nothing for the container peers to try (and fail) to dial through
    /// Docker Desktop's NAT. PEER mode measurably FAILS in this topology:
    /// the rmw_zenoh sessions gossip broken locators (e.g. tcp/[::1]:xxxxx,
    /// their container-local listener), `zenoh::open` returns after the
    /// scouting delay without a working peering, and an 8 s sniff saw zero
    /// samples. Multicast scouting stays disabled: nothing must leak onto
    /// the LAN (rmw_zenoh leaves UDP multicast off by default for the same
    /// reason).
    pub async fn connect(
        endpoint: &str,
        domain: u32,
        fifo_cap: usize,
        mode: &str,
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
        Ok(ZenohTransport {
            session,
            domain,
            fifo_cap,
        })
    }

    /// Borrow the underlying session (sniff/discover only).
    pub fn session(&self) -> &zenoh::Session {
        &self.session
    }

    /// The subscription key expression rmw_zenoh uses for `topic`:
    /// `<domain>/<fqn>/**`, matching any type name and hash.
    pub fn subscription_keyexpr(&self, topic: &str) -> String {
        let fqn = topic.trim_matches('/');
        format!("{}/{fqn}/**", self.domain)
    }

    /// Parse a concrete data key expression into (topic, ros_type, hash).
    /// Returns None when the key does not have the rmw_zenoh 4-segment
    /// shape (logs nothing by design; callers count it if they care).
    pub fn parse_data_keyexpr(ke: &str) -> Option<(String, String, String)> {
        if ke.starts_with(LIVELINESS_PREFIX) {
            return None;
        }
        let segs: Vec<&str> = ke.split('/').collect();
        if segs.len() < 4 {
            return None;
        }
        let (head, last_two) = segs.split_at(segs.len() - 2);
        let fqn = head[1..].join("/");
        let dds_type = last_two[0];
        let hash = last_two[1];
        let ros_type = dds_to_ros_type(dds_type)?;
        Some((format!("/{fqn}"), ros_type, hash.to_string()))
    }

    /// Rmw_zenoh publisher attachment: int64-LE seq, int64-LE source
    /// timestamp ns, u8 gid length (16), 16-byte gid.
    pub fn parse_attachment(raw: &[u8]) -> ParsedAttachment {
        let mut out = ParsedAttachment {
            seq: -1,
            source_timestamp_ns: -1,
            gid: [0u8; 16],
            well_formed: false,
        };
        if raw.len() >= 16 {
            out.seq = i64::from_le_bytes(raw[0..8].try_into().unwrap_or([0u8; 8]));
            out.source_timestamp_ns = i64::from_le_bytes(raw[8..16].try_into().unwrap_or([0u8; 8]));
            if raw.len() >= 33 && raw[16] as usize == 16 {
                out.gid.copy_from_slice(&raw[17..33]);
                out.well_formed = true;
            }
        }
        out
    }

    /// Parse an rmw_zenoh liveliness token key expression. Only `MP`/`MS`
    /// entities become [`TopicAdvert`]s; node tokens return None here.
    pub fn parse_liveliness_token(ke: &str) -> Option<LivelinessEntity> {
        let segs: Vec<&str> = ke.split('/').collect();
        // @ros2_lv/<domain>/<session>/<node_id>/<entity_id>/<kind>/<enclave>/<ns>/<node>[...]
        if segs.len() < 9 || segs[0] != LIVELINESS_PREFIX {
            return None;
        }
        let kind = segs[5].to_string();
        let mut entity = LivelinessEntity {
            domain: segs[1].to_string(),
            session_id: segs[2].to_string(),
            node_id: segs[3].to_string(),
            entity_id: segs[4].to_string(),
            kind: kind.clone(),
            namespace: demangle(segs[7]),
            node_name: demangle(segs[8]),
            topic: None,
            type_name: None,
            type_hash: None,
            qos_keyexpr: None,
        };
        if kind != "NN" && segs.len() >= 13 {
            entity.topic = Some(demangle(segs[9]));
            entity.type_name = dds_to_ros_type(segs[10]);
            entity.type_hash = Some(segs[11].to_string());
            entity.qos_keyexpr = Some(segs[12..].join("/"));
        }
        Some(entity)
    }
}

#[derive(Debug, Clone)]
pub struct ParsedAttachment {
    pub seq: i64,
    pub source_timestamp_ns: i64,
    pub gid: [u8; 16],
    /// False when fewer than 33 bytes or a non-16 GID length was seen.
    pub well_formed: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LivelinessEntity {
    pub domain: String,
    pub session_id: String,
    pub node_id: String,
    pub entity_id: String,
    pub kind: String,
    pub namespace: String,
    pub node_name: String,
    pub topic: Option<String>,
    pub type_name: Option<String>,
    pub type_hash: Option<String>,
    pub qos_keyexpr: Option<String>,
}

/// `sensor_msgs::msg::dds_::Imu_` -> `sensor_msgs/msg/Imu`.
pub fn dds_to_ros_type(dds: &str) -> Option<String> {
    // "::" split yields [pkg, kind, "dds_", "Type_"]: the DDS registered
    // name is `pkg::kind::dds_::Type_`.
    let segs: Vec<&str> = dds.split("::").collect();
    if segs.len() != 4 || segs[2] != "dds_" {
        return None;
    }
    if !(segs[1] == "msg" || segs[1] == "srv") {
        return None;
    }
    let name = segs[3].strip_suffix('_').unwrap_or(segs[3]);
    Some(format!("{}/{}/{}", segs[0], segs[1], name))
}

/// rmw_zenoh mangles `/` into `%` inside liveliness token names; ROS names
/// cannot themselves contain `%`. (`%` alone = the root namespace `/`.)
fn demangle(s: &str) -> String {
    s.replace('%', "/")
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Backpressure sizing between the zenoh pump task and the metrics
/// collector. Larger than the zenoh-side fifo so zenoh-side blocking (with
/// a slow collector) is what propagates to the publisher.
const PUMP_CHANNEL_CAP: usize = 4096;

impl LiveRos2Transport for ZenohTransport {
    async fn subscribe(
        &self,
        topic: &str,
        ros_type: Option<&str>,
        qos: QosProfile,
    ) -> Result<mpsc::Receiver<RawRos2Message>, TransportError> {
        let ke = self.subscription_keyexpr(topic);
        let builder = self
            .session
            .declare_subscriber(ke.clone())
            .with(zenoh::handlers::FifoChannel::new(self.fifo_cap));
        let subscriber = builder.await.map_err(|e| TransportError::Subscribe {
            topic: topic.to_string(),
            reason: format!("declare_subscriber: {e}"),
        })?;
        let (tx, rx) = mpsc::channel(PUMP_CHANNEL_CAP);

        // Live delivery pump.
        {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Ok(sample) = subscriber.recv_async().await {
                    if sample.kind() != zenoh::sample::SampleKind::Put {
                        continue;
                    }
                    if let Some(msg) = map_sample(&sample, false) {
                        // Receiver gone: stop pumping.
                        if tx.send(msg).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }

        // transient_local late join: query the zenoh-ext AdvancedCache
        // publication cache. The module docs explain the four strictures;
        // each was a measured failure mode here before it was added.
        if qos.durability == Durability::TransientLocal {
            let session = self.session.clone();
            let ke = format!("{ke}/@adv/**");
            tokio::spawn(async move {
                let replies = session
                    .get(ke.as_str())
                    .target(zenoh::query::QueryTarget::All)
                    .consolidation(zenoh::query::ConsolidationMode::None)
                    .accept_replies(zenoh::query::ReplyKeyExpr::Any)
                    .timeout(Duration::from_secs(2))
                    .await;
                let Ok(replies) = replies else {
                    return;
                };
                while let Ok(reply) = replies.recv_async().await {
                    if let Ok(sample) = reply.into_result()
                        && let Some(msg) = map_sample(&sample, true)
                        && tx.send(msg).await.is_err()
                    {
                        return;
                    }
                }
            });
        }

        let _ = ros_type;
        Ok(rx)
    }

    async fn discover(&self, settle: Duration) -> Vec<TopicAdvert> {
        let ke = format!("{LIVELINESS_PREFIX}/{}/**", self.domain);
        let Ok(subscriber) = self
            .session
            .liveliness()
            .declare_subscriber(ke.as_str())
            .history(true)
            .await
        else {
            return Vec::new();
        };
        let deadline = tokio::time::Instant::now() + settle;
        let mut mp_tokens: HashMap<String, LivelinessEntity> = HashMap::new();
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            match tokio::time::timeout(deadline - now, subscriber.recv_async()).await {
                Ok(Ok(sample)) => {
                    let k = sample.key_expr().as_str().to_string();
                    match sample.kind() {
                        zenoh::sample::SampleKind::Put => {
                            if let Some(ent) = Self::parse_liveliness_token(&k)
                                && ent.kind == "MP"
                            {
                                mp_tokens.insert(k, ent);
                            }
                        }
                        zenoh::sample::SampleKind::Delete => {
                            mp_tokens.remove(&k);
                        }
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }
        // aggregate per (topic, type)
        let mut grouped: HashMap<(String, String), TopicAdvert> = HashMap::new();
        for ent in mp_tokens.into_values() {
            let (Some(topic), Some(ty)) = (ent.topic.clone(), ent.type_name.clone()) else {
                continue;
            };
            let advert = grouped
                .entry((topic.clone(), ty.clone()))
                .or_insert_with(|| TopicAdvert {
                    topic,
                    type_name: ty,
                    type_hash: ent.type_hash.clone().unwrap_or_default(),
                    qos_keyexpr: ent.qos_keyexpr.clone().unwrap_or_default(),
                    publishers: 0,
                });
            advert.publishers += 1;
        }
        let mut out: Vec<_> = grouped.into_values().collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        out
    }
}

/// Map one zenoh sample (put or query reply) to the transport record.
pub fn map_sample(sample: &zenoh::sample::Sample, late_join: bool) -> Option<RawRos2Message> {
    let ke = sample.key_expr().as_str();
    let (topic, type_name, type_hash) = ZenohTransport::parse_data_keyexpr(ke)?;
    let payload = sample.payload().to_bytes().into_owned();
    let attachment = sample.attachment();
    let (seq, source_timestamp_ns, gid) = match attachment {
        Some(a) => {
            let parsed = ZenohTransport::parse_attachment(&a.to_bytes());
            (parsed.seq, parsed.source_timestamp_ns, parsed.gid)
        }
        None => (-1, -1, [0u8; 16]),
    };
    Some(RawRos2Message {
        topic,
        type_name,
        type_hash,
        payload,
        source_timestamp_ns,
        recv_time_ns: now_ns(),
        seq,
        gid,
        late_join,
    })
}

/// Human-readable single-line description of a sample for the sniffer.
pub fn describe_sample(sample: &zenoh::sample::Sample) -> serde_json::Value {
    let ke = sample.key_expr().as_str().to_string();
    let payload = sample.payload().to_bytes();
    let parsed = sample.attachment().map(|a| {
        let raw = a.to_bytes();
        let p = ZenohTransport::parse_attachment(&raw);
        serde_json::json!({
            "hex": hex(&raw),
            "len": raw.len(),
            "seq": p.seq,
            "source_timestamp_ns": p.source_timestamp_ns,
            "gid": hex(&p.gid),
            "well_formed": p.well_formed,
        })
    });
    let parsed_ke = ZenohTransport::parse_data_keyexpr(&ke);
    serde_json::json!({
        "ke": ke,
        "kind": format!("{:?}", sample.kind()),
        "payload_len": payload.len(),
        "payload_hex_first64": hex(&payload[..payload.len().min(64)]),
        "attachment": parsed,
        "topic_type_hash": parsed_ke,
    })
}
