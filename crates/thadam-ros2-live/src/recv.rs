//! The receive task: zenoh samples in, messages out, drops counted.
//!
//! Like CAN, delivery is at-most-once and the task never waits on the
//! runtime's channel: a message that does not fit the queue is dropped and
//! counted (`dropped_overflow`), and the count travels on later messages as
//! `ros2.dropped`. Losses upstream of this client (the router's egress
//! dropping under backpressure, network loss) show up as per-publisher
//! sequence jumps and are counted in `dropped_gaps` — the two counters are
//! disjoint, because a client-side overflow drop only ever follows an
//! *observed* sequence number, which never jumps. A lost session is
//! re-established with jittered backoff.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use thadam_core::{Clock, RawMessage, Shutdown};
use tokio::sync::mpsc;

use crate::config::ReconnectConfig;
use crate::error::Ros2LiveError;
use crate::gaps::GapTracker;
use crate::transport::{LiveRos2Transport, RawRos2Message, TransportError, hex};

/// Messages the receive task may run ahead of `next_batch`; overflow is
/// dropped and counted.
pub(crate) const CHANNEL_CAPACITY: usize = 4096;

/// Capacity of the fan-in from the per-topic pump tasks. A session whose
/// fan-in stays full back-pressures the zenoh receive channel (then the
/// router egress, counted as sequence gaps).
const FAN_IN_CAPACITY: usize = 4096;

/// A dropped-message warning is logged for the first drop and then every
/// this many drops.
const WARN_EVERY: u64 = 10_000;

/// One configured subscription and what to expect on it.
#[derive(Clone, Debug)]
pub(crate) struct TopicPlan {
    /// The topic, e.g. `/imu`.
    pub name: String,
    /// The declared ROS type, canonical `pkg/msg/Type`.
    pub ros_type: String,
    /// Whether to run the transient_local late-join query.
    pub transient_local: bool,
}

/// Everything the receive task needs to build a batch message: the schema
/// store hashes by canonical type name, the drop counters, and the clock.
pub(crate) struct Parts {
    /// Schema-store hash per canonical type name (empty without a store).
    pub schema_hashes: HashMap<String, Arc<str>>,
    /// Messages proven lost between publishers and this client (seq jumps).
    pub dropped_gaps: Arc<AtomicU64>,
    /// Messages this client dropped because the runtime queue was full.
    pub dropped_overflow: Arc<AtomicU64>,
    /// The pipeline clock for `ingest_time`.
    pub clock: Arc<dyn Clock>,
}

impl Parts {
    /// The running total carried on messages as `ros2.dropped`.
    fn dropped(&self) -> u64 {
        self.dropped_gaps.load(Ordering::Relaxed) + self.dropped_overflow.load(Ordering::Relaxed)
    }
}

/// Runs until `stop` triggers or the receiver hangs up: connect, subscribe
/// every topic, forward until the session dies, then reconnect with
/// backoff. Transport failures never end the task; they reconnect.
pub(crate) async fn run<T, Connect, Fut>(
    endpoint: String,
    connect: Connect,
    topics: Vec<TopicPlan>,
    reconnect: ReconnectConfig,
    parts: Parts,
    tx: mpsc::Sender<RawMessage>,
    stop: Shutdown,
) -> Result<(), Ros2LiveError>
where
    T: LiveRos2Transport,
    Connect: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, TransportError>>,
{
    let mut failures = 0u32;
    let mut wait = Duration::ZERO;
    let min = reconnect.min_backoff.0;
    let max = reconnect.max_backoff.0;
    loop {
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = stop.triggered() => return Ok(()),
        }
        match connect().await {
            Ok(transport) => match forward(transport, &topics, &parts, &tx, &stop).await {
                Ok(()) => return Ok(()), // stop or closed receiver
                Err(error) => {
                    failures += 1;
                    wait = backoff(min, max, failures, random_unit());
                    tracing::warn!(
                        %error,
                        endpoint = %endpoint,
                        backoff_ms = wait.as_millis() as u64,
                        "ROS 2 session lost; reconnecting"
                    );
                }
            },
            Err(detail) => {
                failures += 1;
                wait = backoff(min, max, failures, random_unit());
                let error = Ros2LiveError::Connect {
                    endpoint: endpoint.clone(),
                    detail: detail.to_string(),
                };
                tracing::warn!(%error, backoff_ms = wait.as_millis() as u64, "ROS 2 connect failed; retrying");
            }
        }
    }
}

/// One session's forward loop. Returns `Ok` when `stop` fires or the
/// runtime's receiver is gone; `Err` when the zenoh session ended (every
/// subscription stream closed) and the caller should reconnect.
async fn forward<T: LiveRos2Transport>(
    transport: T,
    topics: &[TopicPlan],
    parts: &Parts,
    tx: &mpsc::Sender<RawMessage>,
    stop: &Shutdown,
) -> Result<(), Ros2LiveError> {
    let (fan_tx, mut fan_rx) = mpsc::channel(FAN_IN_CAPACITY);
    let mut pumps = tokio::task::JoinSet::new();
    for topic in topics {
        let mut rx = transport
            .subscribe(&topic.name, topic.transient_local)
            .await
            .map_err(|e| Ros2LiveError::Transport(format!("subscribe {}: {e}", topic.name)))?;
        let fan_tx = fan_tx.clone();
        pumps.spawn(async move {
            while let Some(raw) = rx.recv().await {
                if fan_tx.send(raw).await.is_err() {
                    return;
                }
            }
        });
    }
    // Only the pumps hold senders now; the fan-in closes when every pump
    // ended (the session's subscription streams all closed).
    drop(fan_tx);

    let mut gaps = GapTracker::new();
    let declared: HashMap<&str, &TopicPlan> = topics.iter().map(|t| (t.name.as_str(), t)).collect();
    let mut mismatches: HashSet<(String, String)> = HashSet::new();
    let outcome = loop {
        let raw = tokio::select! {
            raw = fan_rx.recv() => raw,
            () = stop.triggered() => break Ok(()),
        };
        let Some(raw) = raw else {
            break Err(Ros2LiveError::Transport(
                "zenoh session closed: every subscription stream ended".into(),
            ));
        };
        // The subscription is `<domain>/<fqn>/**`, so nested sub-topics (a
        // `/tf` subscription also matches `/tf/extra`) arrive too; they were
        // never configured, so warn once and skip them.
        let Some(plan) = declared.get(raw.topic.as_str()) else {
            if mismatches.insert((raw.topic.clone(), String::new())) {
                tracing::warn!(
                    topic = %raw.topic,
                    "ROS 2 sub-topic is not in the configured topics; skipping messages"
                );
            }
            continue;
        };
        // A sample whose wire type is not the declared type cannot decode
        // against the stored schema: skip it (warn once per pair). RIHS01
        // hashes travel as attributes; schema keying is by type name (see
        // the crate docs).
        if raw.type_name != plan.ros_type {
            if mismatches.insert((raw.topic.clone(), raw.type_name.clone())) {
                tracing::warn!(
                    topic = %raw.topic,
                    wire_type = %raw.type_name,
                    declared_type = %plan.ros_type,
                    "ROS 2 wire type does not match the configured type; skipping messages"
                );
            }
            continue;
        }
        if let Some(gap) = gaps.observe(&raw) {
            let total = parts.dropped_gaps.fetch_add(gap.missing, Ordering::Relaxed) + gap.missing;
            tracing::warn!(
                topic = %gap.topic,
                gid = %gap.gid,
                after = gap.after,
                before = gap.before,
                missing = gap.missing,
                total = total + parts.dropped_overflow.load(Ordering::Relaxed),
                "ROS 2 sequence gap: messages lost between publisher and thadam"
            );
        }
        let message = to_message(&raw, parts);
        match tx.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let total = parts.dropped_overflow.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total.is_multiple_of(WARN_EVERY) {
                    tracing::warn!(
                        total,
                        "ROS 2 receive queue full; dropping messages at the client"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => break Ok(()),
        }
    };
    // Dropping the transport closes the session; the pumps notice and end.
    pumps.detach_all();
    outcome
}

/// Maps one transport sample to a runtime message with all attributes.
fn to_message(raw: &RawRos2Message, parts: &Parts) -> RawMessage {
    let mut message = RawMessage::new(parts.clock.now(), raw.payload.clone());
    message.attrs.insert("topic", raw.topic.clone());
    message.attrs.insert("ros2.type", raw.type_name.clone());
    message
        .attrs
        .insert("ros2.type_hash", raw.type_hash.clone());
    if raw.well_formed_envelope() {
        message.attrs.insert("ros2.seq", raw.seq.to_string());
        message.attrs.insert("ros2.gid", hex(&raw.gid));
        message
            .attrs
            .insert("ros2.source_time", raw.source_timestamp_ns.to_string());
    }
    if raw.late_join {
        message.attrs.insert("ros2.late_join", "true");
    }
    if let Some(hash) = parts.schema_hashes.get(&raw.type_name) {
        message
            .attrs
            .insert(thadam_core::SCHEMA_HASH_ATTR, hash.as_ref());
    }
    let dropped = parts.dropped();
    if dropped > 0 {
        message.attrs.insert("ros2.dropped", dropped.to_string());
    }
    message
}

/// The first retry waits one `min_backoff`, doubling from there up to
/// `max_backoff`; the jittered delay stays within the upper half.
fn backoff(min: Duration, max: Duration, failures: u32, unit: f64) -> Duration {
    let shift = failures.saturating_sub(1).min(20);
    let window = min.saturating_mul(1 << shift).min(max);
    let half = window / 2;
    half + half.mul_f64(unit)
}

fn random_unit() -> f64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    (RandomState::new().build_hasher().finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// Tests live behind a fake transport on any OS.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportError;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use thadam_core::SystemClock;

    struct FakeTransport {
        topics: StdMutex<HashMap<String, VecDeque<RawRos2Message>>>,
    }

    impl FakeTransport {
        fn with(topic: &str, messages: Vec<RawRos2Message>) -> FakeTransport {
            FakeTransport {
                topics: StdMutex::new(HashMap::from([(
                    topic.to_string(),
                    VecDeque::from(messages),
                )])),
            }
        }
    }

    #[allow(async_fn_in_trait)]
    impl LiveRos2Transport for FakeTransport {
        async fn subscribe(
            &self,
            topic: &str,
            _transient_local: bool,
        ) -> Result<mpsc::Receiver<RawRos2Message>, TransportError> {
            let mut topics = self.topics.lock().expect("lock");
            let queue = topics.remove(topic).unwrap_or_default();
            drop(topics);
            let (tx, rx) = mpsc::channel(queue.len().max(1));
            tokio::spawn(async move {
                for msg in queue {
                    if tx.send(msg).await.is_err() {
                        return;
                    }
                }
                // Keep the stream open: the session ends when the fake is
                // dropped, like a healthy-but-idle zenoh session.
                std::future::pending::<()>().await;
            });
            Ok(rx)
        }
    }

    fn raw(topic: &str, ros_type: &str, gid: u8, seq: i64) -> RawRos2Message {
        RawRos2Message {
            topic: topic.to_string(),
            type_name: ros_type.to_string(),
            type_hash: "RIHS01_00".to_string(),
            payload: Bytes::from_static(&[0, 1, 0, 0]),
            source_timestamp_ns: seq,
            seq,
            gid: [gid; 16],
            late_join: false,
        }
    }

    fn parts() -> (Parts, Arc<AtomicU64>, Arc<AtomicU64>) {
        let gaps = Arc::new(AtomicU64::new(0));
        let overflow = Arc::new(AtomicU64::new(0));
        let parts = Parts {
            schema_hashes: HashMap::new(),
            dropped_gaps: Arc::clone(&gaps),
            dropped_overflow: Arc::clone(&overflow),
            clock: Arc::new(SystemClock::new()),
        };
        (parts, gaps, overflow)
    }

    fn plans() -> Vec<TopicPlan> {
        vec![TopicPlan {
            name: "/imu".into(),
            ros_type: "sensor_msgs/msg/Imu".into(),
            transient_local: false,
        }]
    }

    fn connect_ok(
        transport: FakeTransport,
    ) -> impl Fn() -> std::future::Ready<Result<FakeTransport, TransportError>> {
        let cell = StdMutex::new(Some(transport));
        move || {
            let taken = cell.lock().expect("lock").take();
            std::future::ready(
                taken.ok_or_else(|| TransportError::Connect("already connected".to_string())),
            )
        }
    }

    #[tokio::test(start_paused = false)]
    async fn messages_flow_and_gaps_are_counted() {
        let fake = FakeTransport::with(
            "/imu",
            vec![
                raw("/imu", "sensor_msgs/msg/Imu", 7, 1),
                raw("/imu", "sensor_msgs/msg/Imu", 7, 2),
                raw("/imu", "sensor_msgs/msg/Imu", 7, 5),
            ],
        );
        let (parts, gaps, _overflow) = parts();
        let (tx, mut rx) = mpsc::channel(16);
        let stop = Shutdown::new();
        let task = tokio::spawn(run(
            "tcp/127.0.0.1:7447".to_string(),
            connect_ok(fake),
            plans(),
            ReconnectConfig::default(),
            parts,
            tx,
            stop.clone(),
        ));

        for (want_seq, want_dropped) in [(1, None), (2, None), (5, Some("2"))] {
            let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("message in time")
                .expect("a message");
            assert_eq!(message.attrs.get("topic"), Some("/imu"));
            assert_eq!(message.attrs.get("ros2.type"), Some("sensor_msgs/msg/Imu"));
            assert_eq!(
                message.attrs.get("ros2.seq"),
                Some(want_seq.to_string().as_str())
            );
            assert_eq!(
                message.attrs.get("ros2.gid"),
                Some(hex(&[7u8; 16]).as_str())
            );
            assert_eq!(
                message.attrs.get("ros2.dropped"),
                want_dropped,
                "seq {want_seq}"
            );
        }
        assert_eq!(gaps.load(Ordering::Relaxed), 2);

        stop.trigger();
        task.await.expect("task ends").expect("clean end");
    }

    #[tokio::test(start_paused = false)]
    async fn queue_overflow_is_counted_and_stamped() {
        // Ten messages, two slots in the runtime queue: eight are dropped at
        // the client and counted, and later messages carry the total.
        let messages: Vec<_> = (1..=10)
            .map(|seq| raw("/imu", "sensor_msgs/msg/Imu", 3, seq))
            .collect();
        let fake = FakeTransport::with("/imu", messages);
        let (parts, gaps, overflow) = parts();
        let (tx, mut rx) = mpsc::channel(2);
        let stop = Shutdown::new();
        let task = tokio::spawn(run(
            "tcp/127.0.0.1:7447".to_string(),
            connect_ok(fake),
            plans(),
            ReconnectConfig::default(),
            parts,
            tx,
            stop.clone(),
        ));

        // Read nothing until the drops happened: the two queue slots stay
        // full, so the other eight messages overflow deterministically.
        tokio::time::timeout(Duration::from_secs(5), async {
            while overflow.load(Ordering::Relaxed) != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("eight messages dropped");
        // Client-side overflow drops are not sequence jumps.
        assert_eq!(gaps.load(Ordering::Relaxed), 0);

        let first = rx.try_recv().expect("first");
        let second = rx.try_recv().expect("second");
        // The two queued messages were stamped before any overflow.
        assert!(first.attrs.get("ros2.dropped").is_none());
        assert!(second.attrs.get("ros2.dropped").is_none());

        stop.trigger();
        task.await.expect("task ends").expect("clean end");
    }

    #[tokio::test(start_paused = false)]
    async fn a_stopped_task_returns_cleanly() {
        let fake = FakeTransport::with("/imu", Vec::new());
        let (parts, _gaps, _overflow) = parts();
        let (tx, _rx) = mpsc::channel(4);
        let stop = Shutdown::new();
        let task = tokio::spawn(run(
            "tcp/127.0.0.1:7447".to_string(),
            connect_ok(fake),
            plans(),
            ReconnectConfig::default(),
            parts,
            tx,
            stop.clone(),
        ));
        stop.trigger();
        task.await.expect("task ends").expect("clean end");
    }

    #[test]
    fn backoff_doubles_within_jitter_and_caps() {
        let interval = Duration::from_millis(100);
        let max = Duration::from_secs(5);
        assert_eq!(backoff(interval, max, 1, 0.0), Duration::from_millis(50));
        assert_eq!(backoff(interval, max, 1, 1.0), Duration::from_millis(100));
        assert_eq!(backoff(interval, max, 3, 1.0), Duration::from_millis(400));
        assert_eq!(backoff(interval, max, 30, 1.0), max);
    }

    #[test]
    fn message_attributes_cover_the_wire_metadata() {
        let (parts, _gaps, overflow) = parts();
        let mut raw_msg = raw("/tf_static", "tf2_msgs/msg/TFMessage", 9, 1);
        raw_msg.late_join = true;
        let message = to_message(&raw_msg, &parts);
        assert_eq!(message.attrs.get("topic"), Some("/tf_static"));
        assert_eq!(
            message.attrs.get("ros2.type"),
            Some("tf2_msgs/msg/TFMessage")
        );
        assert_eq!(message.attrs.get("ros2.type_hash"), Some("RIHS01_00"));
        assert_eq!(message.attrs.get("ros2.seq"), Some("1"));
        assert_eq!(message.attrs.get("ros2.source_time"), Some("1"));
        assert_eq!(message.attrs.get("ros2.late_join"), Some("true"));
        assert!(message.attrs.get("ros2.dropped").is_none());

        overflow.store(12, Ordering::Relaxed);
        let message = to_message(&raw_msg, &parts);
        assert_eq!(message.attrs.get("ros2.dropped"), Some("12"));

        // Without a well-formed envelope the seq/gid/source_time attributes
        // stay off.
        let bare = raw("/imu", "sensor_msgs/msg/Imu", 0, -1);
        overflow.store(0, Ordering::Relaxed);
        let message = to_message(&bare, &parts);
        assert!(message.attrs.get("ros2.seq").is_none());
        assert!(message.attrs.get("ros2.gid").is_none());
        assert!(message.attrs.get("ros2.source_time").is_none());
    }
}
