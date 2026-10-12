//! Thadam-owned live ROS 2 transport abstraction (ADR-0007 stage 4).
//!
//! Every live ROS 2 transport sits behind [`LiveRos2Transport`] so the
//! source, the schema handling, and the drop accounting stay
//! transport-neutral. This step ships one implementation, the `rmw_zenoh`
//! transport in [`crate::ZenohTransport`]; spike S3 §11.6 showed a second
//! (zenoh-bridge-ros2dds sidecar) fits the same shape if a later step adds
//! it.

use std::fmt;

use bytes::Bytes;
use tokio::sync::mpsc;

/// One received sample, transport-agnostic.
///
/// `payload` is the CDR-serialized ROS message (including the 4-byte
/// encapsulation header); everything else is metadata a transport supplies
/// for decoder keying and drop accounting.
pub struct RawRos2Message {
    /// ROS topic name with leading slash, e.g. `/imu`.
    pub topic: String,
    /// ROS type name, e.g. `sensor_msgs/msg/Imu`.
    pub type_name: String,
    /// REP-2016 type hash string, e.g. `RIHS01_<64 hex>` (`/` if unknown).
    pub type_hash: String,
    /// CDR payload including the 4-byte encapsulation header.
    pub payload: Bytes,
    /// Publisher-side timestamp in ns since the Unix epoch (`-1` when the
    /// transport has none).
    pub source_timestamp_ns: i64,
    /// Per-publisher sequence number (`-1` when the transport has none).
    pub seq: i64,
    /// Publisher GID (16 bytes; zeroed when the transport has none).
    pub gid: [u8; 16],
    /// True when this sample came from a transient_local late-join query
    /// rather than a live subscription delivery.
    pub late_join: bool,
}

impl RawRos2Message {
    /// Whether the transport attached a well-formed per-publisher envelope
    /// (sequence, source timestamp, GID). Only such samples take part in
    /// sequence-gap drop accounting.
    pub fn well_formed_envelope(&self) -> bool {
        self.seq >= 0
    }
}

impl fmt::Debug for RawRos2Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawRos2Message")
            .field("topic", &self.topic)
            .field("type_name", &self.type_name)
            .field("type_hash", &self.type_hash)
            .field("payload_len", &self.payload.len())
            .field("source_timestamp_ns", &self.source_timestamp_ns)
            .field("seq", &self.seq)
            .field("gid", &hex(&self.gid))
            .field("late_join", &self.late_join)
            .finish()
    }
}

/// Errors a transport can raise.
pub enum TransportError {
    /// The session could not be established.
    Connect(String),
    /// Declaring a subscription failed.
    Subscribe {
        /// The topic that failed.
        topic: String,
        /// The underlying reason.
        reason: String,
    },
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Connect(e) => write!(f, "connect: {e}"),
            TransportError::Subscribe { topic, reason } => {
                write!(f, "subscribe {topic}: {reason}")
            }
        }
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for TransportError {}

/// The trait a live ROS 2 transport must implement for Thadam.
///
/// `subscribe` returns a stream of raw messages for one ROS topic. A client
/// of this trait cannot enforce ROS QoS on the transport (Zenoh matches any
/// publisher to any subscriber); `transient_local` records the one piece of
/// subscriber intent the transport emulates: a late-join history query so a
/// subscriber receives what a `transient_local` publisher sent before it
/// joined (spike S3 §5.4).
// shortcut: no `discover` method — topic lists are explicit config in this
// step; upgrade to liveliness `@ros2_lv` discovery (spike S3 §5.3) when the
// source learns dynamic topics.
#[allow(async_fn_in_trait)]
pub trait LiveRos2Transport {
    /// Subscribe to `topic` (e.g. `/imu`). When `transient_local` is set the
    /// stream also replays the publisher's cached samples (flagged
    /// [`RawRos2Message::late_join`]).
    async fn subscribe(
        &self,
        topic: &str,
        transient_local: bool,
    ) -> Result<mpsc::Receiver<RawRos2Message>, TransportError>;
}

/// Lowercase hex for attributes and logs (no dependency).
pub fn hex(bytes: &[u8]) -> String {
    use fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}
