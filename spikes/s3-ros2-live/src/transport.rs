//! Thadam-owned live ROS 2 transport abstraction for spike S3.
//!
//! This is the trait every S3 candidate (raw-zenoh, roslibrust/hiroz,
//! zenoh-bridge-ros2dds, r2r) must sit behind, per ADR-0007's rule
//! "the transport sits behind Thadam's own trait". The spike implements
//! only the raw-zenoh variant in [`crate::zenoh_impl`].

use std::fmt;

/// One received sample, transport-agnostic.
///
/// `payload` is the CDR-serialized ROS message; everything else is metadata
/// that any S3 candidate must be able to supply (RIHS01 type hash and
/// sequence/gid are needed for ADR-0007 decoder keying and drop accounting).
pub struct RawRos2Message {
    /// ROS topic name with leading slash, e.g. `/imu`.
    pub topic: String,
    /// ROS type name, e.g. `sensor_msgs/Imu`.
    pub type_name: String,
    /// REP-2016 type hash string, e.g. `RIHS01_<64 hex>` (`/` if unknown).
    pub type_hash: String,
    /// CDR payload including the 4-byte encapsulation header.
    pub payload: Vec<u8>,
    /// Publisher-side timestamp in ns since the Unix epoch (transport
    /// attachment when available, else the middleware receive stamp).
    pub source_timestamp_ns: i64,
    /// Subscriber-side receipt time in ns since the Unix epoch (host clock).
    pub recv_time_ns: i64,
    /// Per-publisher sequence number (-1 when the transport has none).
    pub seq: i64,
    /// Publisher GID (16 bytes; zeroed when the transport has none).
    pub gid: [u8; 16],
    /// True when this sample came from a transient_local late-join query
    /// rather than a live subscription delivery.
    pub late_join: bool,
}

impl fmt::Debug for RawRos2Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawRos2Message")
            .field("topic", &self.topic)
            .field("type_name", &self.type_name)
            .field("type_hash", &self.type_hash)
            .field("payload_len", &self.payload.len())
            .field("source_timestamp_ns", &self.source_timestamp_ns)
            .field("recv_time_ns", &self.recv_time_ns)
            .field("seq", &self.seq)
            .field("gid", &hex(&self.gid))
            .field("late_join", &self.late_join)
            .finish()
    }
}

/// Subscriber intent. A raw-zenoh subscriber cannot enforce QoS (Zenoh
/// matches any publisher to any subscriber); the profile records intent and
/// drives behaviors the transport emulates (currently: transient_local
/// late-join via a snapshot query).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QosProfile {
    pub reliability: Reliability,
    pub durability: Durability,
    /// KEEP_LAST depth; 0 means "leave to the middleware default".
    pub depth: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reliability {
    Reliable,
    BestEffort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    Volatile,
    TransientLocal,
}

/// One topic advertisement parsed out of rmw_zenoh liveliness tokens (or the
/// zenoh-bridge-ros2dds equivalent during step 2).
#[derive(Clone, Debug, serde::Serialize)]
pub struct TopicAdvert {
    pub topic: String,
    pub type_name: String,
    pub type_hash: String,
    /// QoS compact encoding straight from the token (rmw_zenoh specific).
    pub qos_keyexpr: String,
    /// Number of distinct live publishers seen for this topic.
    pub publishers: usize,
}

/// Errors a transport can raise.
pub enum TransportError {
    Connect(String),
    Subscribe { topic: String, reason: String },
    MalformedKeyExpr(String),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Connect(e) => write!(f, "connect: {e}"),
            TransportError::Subscribe { topic, reason } => {
                write!(f, "subscribe {topic}: {reason}")
            }
            TransportError::MalformedKeyExpr(k) => write!(f, "malformed key expression: {k}"),
        }
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for TransportError {}

impl From<TransportError> for String {
    fn from(e: TransportError) -> String {
        e.to_string()
    }
}

/// The trait a live ROS 2 transport must implement for Thadam.
///
/// `subscribe` returns a stream of raw messages for a ROS topic. Streams are
/// materialized as tokio mpsc receivers in this spike; a production
/// implementation would pin up a boxed `Stream` instead.
#[allow(async_fn_in_trait)]
pub trait LiveRos2Transport {
    /// Subscribe to `topic` (e.g. `/imu`). `ros_type` is advisory (used as a
    /// sanity check when the transport emits something else).
    async fn subscribe(
        &self,
        topic: &str,
        ros_type: Option<&str>,
        qos: QosProfile,
    ) -> Result<tokio::sync::mpsc::Receiver<RawRos2Message>, TransportError>;

    /// Snapshot the currently advertised topics via liveliness/graph
    /// discovery, waiting up to `settle` for tokens to arrive.
    async fn discover(&self, settle: std::time::Duration) -> Vec<TopicAdvert>;
}

/// Lowercase hex for debug output (no dependency).
pub fn hex(bytes: &[u8]) -> String {
    use fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}
