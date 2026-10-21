//! `Ros2LiveSource`: the pull side of the receive task.
//!
//! Mirrors the CAN source's delivery shape: the receive task pushes
//! messages into a bounded channel, dropping and counting when it is full;
//! `next_batch` moves messages from that channel into `pending`, a buffer
//! the source owns, so dropping a `next_batch` future never loses a
//! message. Schema text is assembled (and compile-checked) at build, and
//! written durably to the schema store in `open` — before the first message
//! referencing it can be committed to the WAL.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    AckHandle, AckMode, BatchLimits, Clock, RawBatch, RawMessage, SchemaStore, Shutdown, Source,
    SourceBatch, SourceCapabilities, SourceContext, SourceId, ThadamError,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::Ros2LiveConfig;
use crate::error::Ros2LiveError;
use crate::recv::{self, Parts, TopicPlan};
use crate::schemas::{self, AssembledSchema};

/// How long `close` waits for the receive task to notice the stop signal.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Open,
    Closed,
}

/// A source that subscribes to ROS 2 topics through a `rmw_zenohd` router.
pub struct Ros2LiveSource {
    config: Ros2LiveConfig,
    /// One assembled, compile-checked schema per unique configured type.
    schemas: Vec<AssembledSchema>,
    schema_store: Option<SchemaStore>,
    phase: Phase,
    source_id: Option<SourceId>,
    clock: Option<Arc<dyn Clock>>,
    shutdown: Option<Shutdown>,
    stop: Option<Shutdown>,
    rx: Option<mpsc::Receiver<RawMessage>>,
    task: Option<JoinHandle<Result<(), Ros2LiveError>>>,
    pending: Vec<RawMessage>,
    dropped_gaps: Arc<AtomicU64>,
    dropped_overflow: Arc<AtomicU64>,
}

impl Ros2LiveSource {
    /// A source for `config`, assembling every topic type's `ros2msg`
    /// schema text from `msg_dirs` now, so an unreadable or unresolvable
    /// definition fails the build, not the first batch. Nothing is
    /// connected until `open`.
    pub fn new(config: Ros2LiveConfig) -> Result<Self, Ros2LiveError> {
        let mut schemas: Vec<AssembledSchema> = Vec::new();
        for topic in &config.topics {
            let canonical = canonical_name(&topic.ros_type)?;
            if schemas.iter().any(|s| s.name == canonical) {
                continue;
            }
            schemas.push(schemas::assemble(&config.msg_dirs, &canonical)?);
        }
        Ok(Ros2LiveSource {
            config,
            schemas,
            schema_store: None,
            phase: Phase::Created,
            source_id: None,
            clock: None,
            shutdown: None,
            stop: None,
            rx: None,
            task: None,
            pending: Vec::new(),
            dropped_gaps: Arc::new(AtomicU64::new(0)),
            dropped_overflow: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Sets the schema store the assembled `ros2msg` texts are written to
    /// in `open`, and whose hashes messages carry as
    /// [`thadam_core::SCHEMA_HASH_ATTR`]. The runtime's factory always sets
    /// one; a source without a store emits messages without schema
    /// references, so a chain `decode: ros2` then rejects them with
    /// `decode.schema_ref_missing`.
    #[must_use]
    pub fn with_schema_store(mut self, store: SchemaStore) -> Self {
        self.schema_store = Some(store);
        self
    }

    fn state_error(message: &'static str) -> ThadamError {
        Ros2LiveError::State(message).into()
    }

    /// Writes every assembled schema to the store, returning the store hash
    /// per canonical type name. Empty without a store.
    fn store_schemas(&self) -> Result<HashMap<String, Arc<str>>, Ros2LiveError> {
        let mut hashes = HashMap::new();
        let Some(store) = &self.schema_store else {
            return Ok(hashes);
        };
        for schema in &self.schemas {
            let hash = store
                .put(&schema.name, "ros2msg", &schema.text)
                .map_err(|e| Ros2LiveError::Store(e.to_string()))?;
            hashes.insert(schema.name.clone(), Arc::from(hash.as_str()));
        }
        Ok(hashes)
    }

    /// The queue is empty and closed: shutdown, or a receive task that
    /// ended. The task only ends on its own after a non-recoverable bug;
    /// transport losses reconnect inside it.
    async fn receiver_ended(&mut self) -> Result<Option<SourceBatch>, ThadamError> {
        if self.shutdown.as_ref().is_some_and(Shutdown::is_triggered) {
            return Ok(None);
        }
        let Some(task) = self.task.as_mut() else {
            return Ok(None);
        };
        let result = task.await;
        self.task = None;
        match result {
            Ok(Ok(())) => Ok(None),
            Ok(Err(error)) => Err(error.into()),
            Err(join) => {
                Err(Ros2LiveError::Transport(format!("receive task failed: {join}")).into())
            }
        }
    }
}

/// The canonical `pkg/msg/Type` spelling of a configured type.
fn canonical_name(ros_type: &str) -> Result<String, Ros2LiveError> {
    let name =
        thadam_ros2_msg::msg::TypeName::parse(ros_type).map_err(|e| Ros2LiveError::Schema {
            type_name: ros_type.to_string(),
            detail: e.to_string(),
        })?;
    Ok(format!("{}/msg/{}", name.package(), name.name()))
}

#[async_trait]
impl Source for Ros2LiveSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        if self.phase == Phase::Open {
            return Err(Self::state_error("the ros2_live source is already open"));
        }
        // Durable before any message can be committed: a store write
        // failure fails `open`, never silently degrades schema references.
        let schema_hashes = self.store_schemas()?;

        let topics: Vec<TopicPlan> = self
            .config
            .topics
            .iter()
            .map(|t| {
                Ok(TopicPlan {
                    name: t.name.clone(),
                    ros_type: canonical_name(&t.ros_type)?,
                    transient_local: t.transient_local,
                })
            })
            .collect::<Result<_, Ros2LiveError>>()?;

        let (tx, rx) = mpsc::channel(recv::CHANNEL_CAPACITY);
        let stop = ctx.shutdown.child();
        let endpoint = self.config.endpoint.clone();
        let domain = self.config.domain;
        let reconnect = self.config.reconnect;
        let task = tokio::spawn(recv::run(
            endpoint.clone(),
            move || {
                let endpoint = endpoint.clone();
                async move { crate::ZenohTransport::connect(&endpoint, domain).await }
            },
            topics,
            reconnect,
            Parts {
                schema_hashes,
                dropped_gaps: Arc::clone(&self.dropped_gaps),
                dropped_overflow: Arc::clone(&self.dropped_overflow),
                clock: Arc::clone(&ctx.clock),
            },
            tx,
            stop.clone(),
        ));

        self.pending.clear();
        self.source_id = Some(ctx.source_id.clone());
        self.clock = Some(Arc::clone(&ctx.clock));
        self.shutdown = Some(ctx.shutdown.clone());
        self.stop = Some(stop);
        self.rx = Some(rx);
        self.task = Some(task);
        self.phase = Phase::Open;
        Ok(())
    }

    async fn next_batch(
        &mut self,
        limits: BatchLimits,
    ) -> Result<Option<SourceBatch>, ThadamError> {
        match self.phase {
            Phase::Created => return Err(Self::state_error("next_batch called before open")),
            Phase::Closed => return Ok(None),
            Phase::Open => {}
        }
        let (Some(source_id), Some(clock), Some(shutdown), Some(rx)) = (
            self.source_id.clone(),
            self.clock.clone(),
            self.shutdown.clone(),
            self.rx.as_mut(),
        ) else {
            return Err(Self::state_error("next_batch called before open"));
        };

        let max_messages = limits.max_messages.max(1);
        if self.pending.is_empty() {
            let received = tokio::select! {
                n = rx.recv_many(&mut self.pending, max_messages) => n,
                () = shutdown.triggered() => 0,
            };
            if received == 0 {
                return self.receiver_ended().await;
            }
        }

        let deadline = tokio::time::Instant::now() + limits.linger;
        while self.pending.len() < max_messages && pending_bytes(&self.pending) < limits.max_bytes {
            let room = max_messages - self.pending.len();
            tokio::select! {
                n = rx.recv_many(&mut self.pending, room) => {
                    if n == 0 {
                        break;
                    }
                }
                () = tokio::time::sleep_until(deadline) => break,
                () = shutdown.triggered() => break,
            }
        }

        let take = batch_len(&self.pending, limits);
        let messages: Vec<RawMessage> = self.pending.drain(..take).collect();
        Ok(Some(SourceBatch {
            batch: RawBatch::new(source_id, messages, clock.synced()),
            ack: AckHandle::noop(),
        }))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        if self.phase == Phase::Closed {
            return Ok(());
        }
        self.phase = Phase::Closed;
        if let Some(stop) = &self.stop {
            stop.trigger();
        }
        self.rx = None;
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(CLOSE_GRACE, &mut task).await.is_err()
        {
            task.abort();
        }
        self.pending.clear();
        Ok(())
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::new(AckMode::None)
    }
}

fn pending_bytes(messages: &[RawMessage]) -> usize {
    messages.iter().map(|m| m.payload.len()).sum()
}

/// How many leading messages fit the limits. Always at least one, so an
/// oversized message is still delivered on its own.
fn batch_len(messages: &[RawMessage], limits: BatchLimits) -> usize {
    let mut bytes = 0usize;
    let mut count = 0usize;
    for message in messages.iter().take(limits.max_messages.max(1)) {
        let next = bytes.saturating_add(message.payload.len());
        if count > 0 && next > limits.max_bytes {
            break;
        }
        bytes = next;
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_len_respects_bytes_and_message_limits() {
        let message = |n: usize| RawMessage {
            ingest_time: thadam_core::Timestamp::EPOCH,
            attrs: thadam_core::Attributes::new(),
            payload: bytes::Bytes::from(vec![0u8; n]),
        };
        let messages = vec![message(10), message(10), message(10)];
        let limits = BatchLimits {
            max_messages: 2,
            ..BatchLimits::default()
        };
        assert_eq!(batch_len(&messages, limits), 2);
        let limits = BatchLimits {
            max_messages: 10,
            max_bytes: 15,
            ..BatchLimits::default()
        };
        assert_eq!(batch_len(&messages, limits), 1);
        // An oversized first message still goes alone.
        let limits = BatchLimits {
            max_messages: 10,
            max_bytes: 5,
            ..BatchLimits::default()
        };
        assert_eq!(batch_len(&messages, limits), 1);
    }

    #[test]
    fn sources_do_not_ack() {
        let dir = tempfile::tempdir().expect("temp dir");
        let write = |rel: &str, text: &str| {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(path, text).expect("write");
        };
        write(
            "builtin_interfaces/msg/Time.msg",
            "int32 sec\nuint32 nanosec\n",
        );
        write(
            "std_msgs/msg/Header.msg",
            "builtin_interfaces/Time stamp\nstring frame_id\n",
        );
        write(
            "sensor_msgs/msg/JointState.msg",
            "std_msgs/Header header\nstring[] name\nfloat64[] position\nfloat64[] velocity\nfloat64[] effort\n",
        );
        let config = Ros2LiveConfig::from_value(&serde_json::json!({
            "endpoint": "tcp/127.0.0.1:7447",
            "msg_dirs": [dir.path()],
            "topics": [{"name": "/joint_states", "type": "sensor_msgs/msg/JointState"}],
        }))
        .expect("valid");
        let source = Ros2LiveSource::new(config).expect("build");
        assert_eq!(source.capabilities().ack_mode, AckMode::None);
        assert_eq!(source.schemas.len(), 1);
        assert!(source.schemas[0].text.contains("MSG: std_msgs/Header"));
    }
}
