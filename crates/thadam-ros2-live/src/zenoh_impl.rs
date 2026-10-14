//! Raw-zenoh implementation of [`LiveRos2Transport`] for `rmw_zenoh`.
//!
//! rmw_zenoh key expressions and attachments follow
//! `rmw_zenoh_cpp/docs/design.md`, verified live by spike S3 against Jazzy
//! (`ros-jazzy-rmw-zenoh-cpp` 0.2.11) and Lyrical (0.10.6) with a zenoh
//! 1.10.1 client — the mapping is byte-identical between the two distros
//! (spike S3 §12.2):
//!
//! * Data key expression: `<domain_id>/<fqn w/o leading />/<dds_type>/<hash>`,
//!   e.g. `0/imu/sensor_msgs::msg::dds_::Imu_/RIHS01_<64 hex>`. A subscriber
//!   on `<domain>/<fqn>/**` matches any type and hash and learns both from
//!   each sample's concrete key.
//! * Attachment: 33 bytes = int64-LE sequence number, int64-LE source
//!   timestamp (ns since epoch), 1 byte GID length (16), 16-byte GID. The
//!   sequence is per publisher and starts at 1 — the drop-accounting basis.
//! * transient_local late join: rmw_zenoh's zenoh-ext AdvancedPublisher
//!   keeps an AdvancedCache answered by a queryable at
//!   `<data_ke>/@adv/pub/<zid>/<eid>(/uhlc)/`. Recovering history needs the
//!   four strictures below (spike S3 §5.4; each was a measured failure
//!   before it was added), and the query must be scoped to ONE topic: a
//!   graph-wide `**/@adv/**` query fans rejected-reply work out to every
//!   cached publisher.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::transport::{LiveRos2Transport, RawRos2Message, TransportError};

/// Capacity of the per-subscription receive channel inside zenoh; when it
/// fills, zenoh back-pressures the router session, whose egress drops
/// best-effort samples (counted downstream as sequence gaps, spike S3 §6.4).
const ZENOH_FIFO_CAP: usize = 256;

/// Capacity of the channel from the zenoh pump to the subscriber's caller.
/// Never the drop point in practice: the receive task drains it with
/// `try_send` and counts overflow itself.
const PUMP_CHANNEL_CAP: usize = 4096;

/// Time the transient_local late-join query waits for cache replies.
const LATE_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// The `rmw_zenoh` transport: a Zenoh **client** session against a
/// `rmw_zenohd` router.
pub struct ZenohTransport {
    session: zenoh::Session,
    domain: u32,
}

impl ZenohTransport {
    /// Open a zenoh session connected to `endpoint` (e.g.
    /// `tcp/127.0.0.1:7447` = a rmw_zenohd router).
    ///
    /// Mode (empirical, spike S3 §4, zenoh 1.10.1 vs rmw_zenoh): **client**.
    /// A client attached to rmw_zenohd is brokered by the router and
    /// receives all topics; the client never advertises a listener, so
    /// container peers never try (and fail) to dial it through Docker
    /// Desktop's NAT. Peer mode measurably fails in this topology (0 samples
    /// in 8 s: the rmw_zenoh sessions gossip container-local `[::1]`
    /// locators). Multicast scouting stays disabled: nothing must leak onto
    /// the LAN (rmw_zenoh leaves UDP multicast off by default too).
    pub async fn connect(endpoint: &str, domain: u32) -> Result<Self, TransportError> {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("mode", "\"client\"")
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
        Ok(ZenohTransport { session, domain })
    }

    /// The subscription key expression rmw_zenoh uses for `topic`:
    /// `<domain>/<fqn>/**`, matching any type name and hash.
    fn subscription_keyexpr(&self, topic: &str) -> String {
        let fqn = topic.trim_matches('/');
        format!("{}/{fqn}/**", self.domain)
    }

    /// Parse a concrete data key expression into (topic, ros_type, hash).
    /// Returns None when the key does not have the rmw_zenoh 4-segment shape.
    pub fn parse_data_keyexpr(ke: &str) -> Option<(String, String, String)> {
        if ke.starts_with('@') {
            // `@ros2_lv` liveliness tokens and `@adv` cache keys: hermetic
            // to data subscribers, never a data sample.
            return None;
        }
        let segs: Vec<&str> = ke.split('/').collect();
        if segs.len() < 4 {
            return None;
        }
        let (head, last_two) = segs.split_at(segs.len() - 2);
        let fqn = head[1..].join("/");
        let ros_type = dds_to_ros_type(last_two[0])?;
        let hash = last_two[1];
        Some((format!("/{fqn}"), ros_type, hash.to_string()))
    }

    /// rmw_zenoh publisher attachment: int64-LE seq, int64-LE source
    /// timestamp ns, u8 GID length (16), 16-byte GID.
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
}

/// A parsed rmw_zenoh attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAttachment {
    /// Per-publisher sequence number (`-1` when not well-formed).
    pub seq: i64,
    /// Publisher-side source timestamp, ns since the epoch (`-1` when not
    /// well-formed).
    pub source_timestamp_ns: i64,
    /// Publisher GID.
    pub gid: [u8; 16],
    /// False when fewer than 33 bytes or a non-16 GID length was seen.
    pub well_formed: bool,
}

impl LiveRos2Transport for ZenohTransport {
    async fn subscribe(
        &self,
        topic: &str,
        transient_local: bool,
    ) -> Result<mpsc::Receiver<RawRos2Message>, TransportError> {
        let ke = self.subscription_keyexpr(topic);
        let subscriber = self
            .session
            .declare_subscriber(ke.clone())
            .with(zenoh::handlers::FifoChannel::new(ZENOH_FIFO_CAP))
            .await
            .map_err(|e| TransportError::Subscribe {
                topic: topic.to_string(),
                reason: format!("declare_subscriber: {e}"),
            })?;
        let (tx, rx) = mpsc::channel(PUMP_CHANNEL_CAP);

        // Live delivery pump. `send().await` blocks this pump (then the
        // zenoh fifo, then the router egress) under backpressure — the
        // router-side drops that produces are counted as sequence gaps.
        {
            let tx = tx.clone();
            let topic_name = topic.to_string();
            tokio::spawn(async move {
                let _guard = TaskPanicGuard {
                    topic: &topic_name,
                    task: "live_subscribe",
                };
                while let Ok(sample) = subscriber.recv_async().await {
                    if sample.kind() != zenoh::sample::SampleKind::Put {
                        continue;
                    }
                    if let Some(msg) = map_sample(&sample, false)
                        && tx.send(msg).await.is_err()
                    {
                        return;
                    }
                }
                tracing::debug!(topic = %topic_name, "zenoh live subscription stream ended");
            });
        }

        // transient_local late join: query the zenoh-ext AdvancedCache
        // publication cache (module docs: the four strictures, and why the
        // query stays scoped to this one topic).
        if transient_local {
            let session = self.session.clone();
            let ke = format!("{ke}/@adv/**");
            let topic_name = topic.to_string();
            tokio::spawn(async move {
                let _guard = TaskPanicGuard {
                    topic: &topic_name,
                    task: "late_join_query",
                };
                let replies = session
                    .get(ke.as_str())
                    .target(zenoh::query::QueryTarget::All)
                    .consolidation(zenoh::query::ConsolidationMode::None)
                    .accept_replies(zenoh::query::ReplyKeyExpr::Any)
                    .timeout(LATE_JOIN_TIMEOUT)
                    .await;
                let replies = match replies {
                    Ok(replies) => replies,
                    Err(error) => {
                        tracing::warn!(
                            topic = %topic_name,
                            %error,
                            "zenoh transient_local late-join query failed"
                        );
                        return;
                    }
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

        Ok(rx)
    }
}

struct TaskPanicGuard<'a> {
    topic: &'a str,
    task: &'static str,
}

impl Drop for TaskPanicGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!(
                topic = %self.topic,
                task = self.task,
                "ROS 2 zenoh task panicked"
            );
        }
    }
}

/// Map one zenoh sample (live put or query reply) to the transport record.
fn map_sample(sample: &zenoh::sample::Sample, late_join: bool) -> Option<RawRos2Message> {
    let ke = sample.key_expr().as_str();
    let (topic, type_name, type_hash) = ZenohTransport::parse_data_keyexpr(ke)?;
    let payload = sample.payload().to_bytes();
    let (seq, source_timestamp_ns, gid) = match sample.attachment() {
        Some(a) => {
            let parsed = ZenohTransport::parse_attachment(&a.to_bytes());
            if parsed.well_formed {
                (parsed.seq, parsed.source_timestamp_ns, parsed.gid)
            } else {
                (-1, -1, [0u8; 16])
            }
        }
        None => (-1, -1, [0u8; 16]),
    };
    Some(RawRos2Message {
        topic,
        type_name,
        type_hash,
        payload: bytes::Bytes::copy_from_slice(&payload),
        source_timestamp_ns,
        seq,
        gid,
        late_join,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_keyexprs_parse_to_topic_type_hash() {
        let parsed = ZenohTransport::parse_data_keyexpr(
            "0/imu/sensor_msgs::msg::dds_::Imu_/RIHS01_7d9a00ff131080897a5ec7e26e315954b8eae3353c3f995c55faf71574000b5b",
        )
        .expect("parses");
        assert_eq!(parsed.0, "/imu");
        assert_eq!(parsed.1, "sensor_msgs/msg/Imu");
        assert_eq!(
            parsed.2,
            "RIHS01_7d9a00ff131080897a5ec7e26e315954b8eae3353c3f995c55faf71574000b5b"
        );

        // Namespaced topics keep their hierarchy.
        let parsed = ZenohTransport::parse_data_keyexpr(
            "0/robot/arm/joint_states/sensor_msgs::msg::dds_::JointState_/RIHS01_a13ee3a3",
        )
        .expect("parses");
        assert_eq!(parsed.0, "/robot/arm/joint_states");

        // tf_static and tf share a type; both keyed by their own topic.
        let parsed = ZenohTransport::parse_data_keyexpr(
            "3/tf_static/tf2_msgs::msg::dds_::TFMessage_/RIHS01_e369d0f0",
        )
        .expect("parses");
        assert_eq!(parsed.0, "/tf_static");
        assert_eq!(parsed.1, "tf2_msgs/msg/TFMessage");
    }

    #[test]
    fn keyexprs_without_the_rmw_shape_do_not_parse() {
        for ke in [
            "@ros2_lv/0/abc/0/0/NN/%/s3_sensor_pub",
            "0/imu/sensor_msgs::msg::dds_::Imu_",
            "0/imu",
            "imu",
            "0/imu/sensor_msgs::msg::other_::Imu_/RIHS01_00",
            "0/imu/sensor_msgs::act::dds_::Imu_/RIHS01_00",
            "",
        ] {
            assert_eq!(ZenohTransport::parse_data_keyexpr(ke), None, "{ke}");
        }
    }

    #[test]
    fn dds_type_names_map_to_ros_spellings() {
        assert_eq!(
            dds_to_ros_type("sensor_msgs::msg::dds_::Imu_"),
            Some("sensor_msgs/msg/Imu".to_string())
        );
        assert_eq!(
            dds_to_ros_type("tf2_msgs::msg::dds_::TFMessage_"),
            Some("tf2_msgs/msg/TFMessage".to_string())
        );
        assert_eq!(
            dds_to_ros_type("example_interfaces::srv::dds_::AddTwoInts_"),
            Some("example_interfaces/srv/AddTwoInts".to_string())
        );
        // A name without the trailing underscore still parses.
        assert_eq!(
            dds_to_ros_type("std_msgs::msg::dds_::Header"),
            Some("std_msgs/msg/Header".to_string())
        );
    }

    fn attachment(seq: i64, ts: i64, gid: &[u8]) -> Vec<u8> {
        let mut raw = seq.to_le_bytes().to_vec();
        raw.extend_from_slice(&ts.to_le_bytes());
        raw.push(gid.len() as u8);
        raw.extend_from_slice(gid);
        raw
    }

    #[test]
    fn well_formed_attachments_parse() {
        let gid: Vec<u8> = (0u8..16).collect();
        let parsed = ZenohTransport::parse_attachment(&attachment(41, 1_726_000_000_000, &gid));
        assert!(parsed.well_formed);
        assert_eq!(parsed.seq, 41);
        assert_eq!(parsed.source_timestamp_ns, 1_726_000_000_000);
        assert_eq!(parsed.gid.as_slice(), gid.as_slice());
    }

    #[test]
    fn attachment_parsing_tolerates_trailing_bytes_and_rejects_short_ones() {
        let gid: Vec<u8> = (0u8..16).collect();
        // rmw_zenoh may append; the first 33 bytes are authoritative.
        let mut raw = attachment(1, 2, &gid);
        raw.extend_from_slice(&[0xAA, 0xBB]);
        let parsed = ZenohTransport::parse_attachment(&raw);
        assert!(parsed.well_formed);

        for bad in [
            vec![],
            vec![0u8; 8],
            vec![0u8; 16],
            [
                &1i64.to_le_bytes()[..],
                &2i64.to_le_bytes()[..],
                &[12u8],
                &[0u8; 12],
            ]
            .concat(),
        ] {
            let parsed = ZenohTransport::parse_attachment(&bad);
            assert!(!parsed.well_formed, "len {}", bad.len());
            assert_eq!(parsed.gid, [0u8; 16]);
        }
    }
}
