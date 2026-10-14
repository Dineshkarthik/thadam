//! The publish sink's engine: the poll task and the in-flight tracker.
//!
//! [`start`] runs two tasks per sink:
//!
//! - The poll task owns the client event loop. It awaits `poll()` to completion
//!   and nothing else, forwards each [`LinkEvent`] into an unbounded queue, and
//!   reports connection loss. It never waits on the runtime, a bounded queue, or
//!   the tracker, so acknowledgements are not dropped (ADR-0006 rule 1).
//! - The driver task owns the publish bookkeeping. It takes publish requests and
//!   link events from one unbounded queue and is the only task that calls
//!   `ClientHandle::publish`.
//!
//! Memory bound: a publish holds a permit from a semaphore of `max_in_flight`
//! from submission until it completes, so the engine holds at most
//! `max_in_flight` payloads at once. Link events are not bounded here; the
//! broker's receive window bounds the acknowledgements and written-packet
//! notifications the event loop produces.
//!
//! Ordering assumption: rumqttc reports `Outgoing::Publish` and
//! `Outgoing::AwaitAck` in the order it takes requests from its channel. The
//! driver pushes each publish onto the tracker's queue right after
//! `ClientHandle::publish` returns, and both the requests and the events pass
//! through the same single-consumer queue, so an event can never overtake the
//! request it reports. Written and held events therefore pair with the oldest
//! queued publish.
//!
//! Connection epochs: a connection loss starts a new epoch. Every publish not
//! yet acknowledged fails with a Transient [`MqttError::Network`], and until the
//! next `ConnAck` the tracker discards `PublishWritten`, `PublishHeld` and
//! `PubAck`. An acknowledgement from the dead connection therefore cannot
//! complete a publish issued in the new epoch. Only events after that `ConnAck`
//! can match, and the link delivers `ConnAck` before any event of the new
//! connection, so nothing from the old connection can follow it.
//!
//! After a `ConnAck`, packet ids of the old epoch are still in rumqttc's
//! hands. On a resumed session rumqttc replays unacknowledged packets with
//! their original ids, so those ids are kept and their writes are ignored.
//! Requests that were queued but not yet written keep their slot, because
//! rumqttc sends them in order after the replays. A new session without
//! resumption discards both, matching rumqttc, which clears its pending list.
//!
//! The rule is that duplicates are possible but false acknowledgements are
//! not. A publish completes only on a write or acknowledgement attributed to
//! it. When the tracker cannot attribute an event, it fails the publish rather
//! than completing it, and the caller retries.
//!
//! Residual risk: a QoS 1 publish held by a packet-id collision when the
//! connection dropped stays in rumqttc's collision slot across the reconnect.
//! rumqttc may write it later under its old id. The tracker ignores that
//! write, but if a new publish was assigned the same id in the meantime, the
//! write cannot be told apart from that publish, so the tracker may pair the
//! wrong publish with the next event. The engine therefore reports such a loss
//! through [`Publisher::collision_lost`] and refuses every later publish, so the
//! sink can recreate the client and engine, which starts a fresh session.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::error::MqttError;
use crate::link::{ClientClosed, ClientHandle, Link, LinkError, LinkEvent};

const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

const REFUSED_AFTER_COLLISION: &str = "the connection was lost while a packet id collision was outstanding; the client must be rebuilt";

#[derive(Clone, Copy, Debug)]
pub(crate) struct EngineOptions {
    pub max_in_flight: usize,
    pub request_timeout: Duration,
}

type Outcome = Result<(), MqttError>;

struct Entry {
    qos: u8,
    done: oneshot::Sender<Outcome>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Entry {
    fn finish(self, outcome: Outcome) {
        let _ = self.done.send(outcome);
    }
}

struct Request {
    topic: String,
    payload: Bytes,
    qos: u8,
    retain: bool,
    permit: OwnedSemaphorePermit,
    done: oneshot::Sender<Outcome>,
}

enum Command {
    Link(LinkEvent),
    Lost(String),
    Publish(Request),
}

enum Queued {
    Live(Entry),
    /// Queued in an earlier epoch. Its outcome is already decided, but rumqttc
    /// still holds the request and will write it after the reconnect, so the
    /// slot stays in place to keep the queue aligned with rumqttc's order.
    Stale {
        qos: u8,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Connected,
    /// From a connection loss until the next `ConnAck`.
    Connecting,
}

#[derive(Default)]
pub(crate) struct InflightTracker {
    epoch: u64,
    phase: Phase,
    queued: VecDeque<Queued>,
    held: HashMap<u16, Entry>,
    awaiting_ack: HashMap<u16, Entry>,
    /// Packet ids rumqttc replays on a resumed session.
    replayable: HashSet<u16>,
    /// Packet ids rumqttc may still write after a reconnect, because they were
    /// held by a collision when the connection dropped.
    collided: HashSet<u16>,
}

impl InflightTracker {
    fn enqueued(&mut self, entry: Entry) {
        self.queued.push_back(Queued::Live(entry));
    }

    pub(crate) fn on_event(&mut self, event: LinkEvent) {
        match event {
            LinkEvent::ConnAck { session_present } => {
                self.phase = Phase::Connected;
                if !session_present {
                    self.queued.retain(|q| matches!(q, Queued::Live(_)));
                    self.replayable.clear();
                }
            }
            LinkEvent::PublishWritten { pkid } if self.phase == Phase::Connected => {
                self.written(pkid);
            }
            LinkEvent::PublishHeld { pkid } if self.phase == Phase::Connected => {
                self.held(pkid);
            }
            LinkEvent::PubAck { pkid } if self.phase == Phase::Connected => {
                if let Some(entry) = self.awaiting_ack.remove(&pkid) {
                    entry.finish(Ok(()));
                }
            }
            _ => {}
        }
    }

    fn written(&mut self, pkid: u16) {
        if self.replayable.remove(&pkid) || self.collided.remove(&pkid) {
            return;
        }
        if let Some(entry) = self.held.remove(&pkid) {
            self.awaiting_ack.insert(pkid, entry);
            return;
        }
        match self.queued.pop_front() {
            Some(Queued::Live(entry)) if entry.qos == 0 => entry.finish(Ok(())),
            Some(Queued::Live(entry)) => {
                self.awaiting_ack.insert(pkid, entry);
            }
            Some(Queued::Stale { qos }) if qos > 0 => {
                self.replayable.insert(pkid);
            }
            Some(Queued::Stale { .. }) => {}
            None => {}
        }
    }

    fn held(&mut self, pkid: u16) {
        if self.replayable.contains(&pkid) || self.collided.contains(&pkid) {
            return;
        }
        match self.queued.pop_front() {
            Some(Queued::Live(entry)) => {
                self.held.insert(pkid, entry);
            }
            Some(Queued::Stale { .. }) => {
                self.collided.insert(pkid);
            }
            None => {}
        }
    }

    /// Whether a loss now would leave a packet id that rumqttc may still write
    /// under its old id. Checked before [`Self::lost`], which moves held ids
    /// into `collided`.
    pub(crate) fn collision_pending(&self) -> bool {
        !self.held.is_empty() || !self.collided.is_empty()
    }

    pub(crate) fn lost(&mut self, detail: &str) {
        self.epoch += 1;
        self.phase = Phase::Connecting;
        let failure = || MqttError::Network(format!("connection lost: {detail}"));
        for (pkid, entry) in self.awaiting_ack.drain() {
            self.replayable.insert(pkid);
            entry.finish(Err(failure()));
        }
        for (pkid, entry) in self.held.drain() {
            self.collided.insert(pkid);
            entry.finish(Err(failure()));
        }
        let queued = std::mem::take(&mut self.queued);
        self.queued = queued
            .into_iter()
            .map(|slot| match slot {
                Queued::Live(entry) => {
                    let qos = entry.qos;
                    entry.finish(Err(failure()));
                    Queued::Stale { qos }
                }
                stale @ Queued::Stale { .. } => stale,
            })
            .collect();
    }

    #[cfg(test)]
    fn epoch(&self) -> u64 {
        self.epoch
    }

    #[cfg(test)]
    fn outstanding(&self) -> usize {
        self.queued
            .iter()
            .filter(|q| matches!(q, Queued::Live(_)))
            .count()
            + self.held.len()
            + self.awaiting_ack.len()
    }
}

#[derive(Clone)]
pub(crate) struct Publisher {
    commands: mpsc::UnboundedSender<Command>,
    permits: Arc<Semaphore>,
    request_timeout: Duration,
    collision: Arc<AtomicBool>,
    _stop: Arc<DropGuard>,
}

impl Publisher {
    /// Whether a connection loss left a packet id collision. Once true it stays
    /// true, and every later publish fails until the client is recreated.
    pub(crate) fn collision_lost(&self) -> bool {
        self.collision.load(Ordering::Acquire)
    }

    /// Resolves once the broker acknowledges a QoS 1 publish, or once the
    /// client has written a QoS 0 publish. A `request_timeout` expiry returns
    /// [`MqttError::PublishTimeout`]; the publish keeps its permit until the
    /// engine completes it, so `max_in_flight` still bounds the engine.
    pub(crate) async fn publish(
        &self,
        topic: impl Into<String>,
        payload: Bytes,
        qos: u8,
        retain: bool,
    ) -> Result<(), MqttError> {
        if qos > 1 {
            return Err(MqttError::Config(format!(
                "QoS {qos} is not supported: the sink publishes at QoS 0 or 1"
            )));
        }
        let timeout = self.request_timeout;
        match tokio::time::timeout(timeout, self.submit(topic.into(), payload, qos, retain)).await {
            Ok(outcome) => outcome,
            Err(_) => Err(MqttError::PublishTimeout(timeout)),
        }
    }

    async fn submit(
        &self,
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
    ) -> Result<(), MqttError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| MqttError::State("the publisher is closed"))?;
        let (done, outcome) = oneshot::channel();
        let request = Request {
            topic,
            payload,
            qos,
            retain,
            permit,
            done,
        };
        self.commands
            .send(Command::Publish(request))
            .map_err(|_| stopped())?;
        outcome.await.unwrap_or_else(|_| Err(stopped()))
    }
}

fn stopped() -> MqttError {
    MqttError::Network("the MQTT connection task has stopped".into())
}

/// Waits for the first `ConnAck` on `link` and returns the link for [`start`].
/// The poll runs in its own task, so the timeout abandons the connection
/// instead of dropping a `poll()` mid-flight.
pub(crate) async fn connect(mut link: Link, timeout: Duration) -> Result<Link, MqttError> {
    let task = tokio::spawn(async move {
        loop {
            match link.poll().await {
                Ok(LinkEvent::ConnAck { .. }) => return Ok(link),
                Ok(_) => {}
                Err(error) => return Err(open_error(error)),
            }
        }
    });
    let abort = task.abort_handle();
    match tokio::time::timeout(timeout, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(MqttError::Panic),
        Err(_) => {
            abort.abort();
            Err(MqttError::OpenTimeout(timeout))
        }
    }
}

fn open_error(error: LinkError) -> MqttError {
    match error {
        LinkError::Refused {
            fatal: true,
            detail,
        } => MqttError::Refused(detail),
        LinkError::Refused {
            fatal: false,
            detail,
        } => MqttError::Unavailable(detail),
        LinkError::Tls(detail) => MqttError::Tls(detail),
        other => MqttError::Network(describe(&other)),
    }
}

/// Starts the poll task and the driver for `link` and `client`. The engine
/// stops when the last [`Publisher`] clone is dropped.
pub(crate) fn start(link: Link, client: ClientHandle, options: EngineOptions) -> Publisher {
    let stop = CancellationToken::new();
    let (commands, queue) = mpsc::unbounded_channel();
    let collision = Arc::new(AtomicBool::new(false));
    tokio::spawn(drive(client, queue, Arc::clone(&collision)));
    tokio::spawn(poll_loop(
        link,
        commands.clone(),
        stop.clone(),
        Arc::clone(&collision),
    ));
    Publisher {
        commands,
        permits: Arc::new(Semaphore::new(options.max_in_flight)),
        request_timeout: options.request_timeout,
        collision,
        _stop: Arc::new(stop.drop_guard()),
    }
}

struct PanicGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> PanicGuard<F> {
    fn new(on_panic: F) -> Self {
        Self(Some(on_panic))
    }
}

impl<F: FnOnce()> Drop for PanicGuard<F> {
    fn drop(&mut self) {
        if std::thread::panicking()
            && let Some(on_panic) = self.0.take()
        {
            on_panic();
        }
    }
}

async fn drive(
    client: ClientHandle,
    mut queue: mpsc::UnboundedReceiver<Command>,
    collision: Arc<AtomicBool>,
) {
    let panic_collision = Arc::clone(&collision);
    let _guard = PanicGuard::new(move || {
        panic_collision.store(true, Ordering::Release);
        tracing::error!("MQTT sink driver task panicked");
    });
    let mut tracker = InflightTracker::default();
    while let Some(command) = queue.recv().await {
        match command {
            Command::Link(event) => tracker.on_event(event),
            Command::Lost(detail) => {
                // Set before the tracker fails outstanding publishes, so a caller
                // that observes one of those failures already sees the flag.
                if tracker.collision_pending() {
                    collision.store(true, Ordering::Release);
                }
                tracker.lost(&detail);
            }
            Command::Publish(request) => {
                if collision.load(Ordering::Acquire) {
                    let _ = request
                        .done
                        .send(Err(MqttError::Network(REFUSED_AFTER_COLLISION.into())));
                    continue;
                }
                let Request {
                    topic,
                    payload,
                    qos,
                    retain,
                    permit,
                    done,
                } = request;
                match client.publish(&topic, qos, retain, payload).await {
                    Ok(()) => tracker.enqueued(Entry {
                        qos,
                        done,
                        _permit: Some(permit),
                    }),
                    Err(ClientClosed) => {
                        let _ = done.send(Err(stopped()));
                    }
                }
            }
        }
    }
}

async fn poll_loop(
    mut link: Link,
    events: mpsc::UnboundedSender<Command>,
    stop: CancellationToken,
    collision: Arc<AtomicBool>,
) {
    let panic_events = events.clone();
    let _guard = PanicGuard::new(move || {
        collision.store(true, Ordering::Release);
        let _ = panic_events.send(Command::Lost("MQTT sink poll task panicked".into()));
        tracing::error!("MQTT sink poll task panicked");
    });
    let mut backoff = RECONNECT_MIN;
    loop {
        if stop.is_cancelled() {
            break;
        }
        match link.poll().await {
            Ok(event) => {
                if matches!(event, LinkEvent::ConnAck { .. }) {
                    backoff = RECONNECT_MIN;
                }
                if events.send(Command::Link(event)).is_err() {
                    break;
                }
            }
            Err(error) => {
                if events.send(Command::Lost(describe(&error))).is_err() {
                    break;
                }
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    () = stop.cancelled() => break,
                }
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        }
    }
}

fn describe(error: &LinkError) -> String {
    match error {
        LinkError::Refused { detail, .. } | LinkError::Tls(detail) | LinkError::Network(detail) => {
            detail.clone()
        }
        LinkError::Oversize { size } => format!("a {size} byte packet exceeded the incoming limit"),
    }
}

/// A publisher whose engine is gone, so every publish fails as a transient
/// network error. `collision_lost` sets the engine signal.
#[cfg(test)]
pub(crate) fn fake_publisher(collision_lost: bool) -> Publisher {
    let (commands, _queue) = mpsc::unbounded_channel();
    Publisher {
        commands,
        permits: Arc::new(Semaphore::new(1)),
        request_timeout: Duration::from_secs(1),
        collision: Arc::new(AtomicBool::new(collision_lost)),
        _stop: Arc::new(CancellationToken::new().drop_guard()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use thadam_core::{Classify, Retry};

    use super::*;

    type Receiver = oneshot::Receiver<Outcome>;

    fn entry(qos: u8) -> (Entry, Receiver) {
        let (done, outcome) = oneshot::channel();
        (
            Entry {
                qos,
                done,
                _permit: None,
            },
            outcome,
        )
    }

    fn settled(outcome: &mut Receiver) -> Option<Outcome> {
        outcome.try_recv().ok()
    }

    fn written(pkid: u16) -> LinkEvent {
        LinkEvent::PublishWritten { pkid }
    }

    fn held(pkid: u16) -> LinkEvent {
        LinkEvent::PublishHeld { pkid }
    }

    fn acked(pkid: u16) -> LinkEvent {
        LinkEvent::PubAck { pkid }
    }

    fn connack(session_present: bool) -> LinkEvent {
        LinkEvent::ConnAck { session_present }
    }

    fn test_publisher(
        max_in_flight: usize,
        request_timeout: Duration,
    ) -> (Publisher, mpsc::UnboundedReceiver<Command>) {
        let (commands, queue) = mpsc::unbounded_channel();
        let publisher = Publisher {
            commands,
            permits: Arc::new(Semaphore::new(max_in_flight)),
            request_timeout,
            collision: Arc::new(AtomicBool::new(false)),
            _stop: Arc::new(CancellationToken::new().drop_guard()),
        };
        (publisher, queue)
    }

    #[test]
    fn a_loss_with_a_held_collision_is_reported() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        tracker.enqueued(a);
        tracker.lost("eof");
        assert!(matches!(settled(&mut ra), Some(Err(_))));

        tracker.on_event(LinkEvent::ConnAck {
            session_present: false,
        });
        let (b, _rb) = entry(1);
        tracker.enqueued(b);
        tracker.on_event(held(1));
        assert!(
            tracker.collision_pending(),
            "a held id is still in rumqttc's collision slot"
        );
        tracker.lost("eof");
        assert!(tracker.collision_pending(), "held ids move to collided");
    }

    #[test]
    fn a_loss_without_a_collision_is_not_reported() {
        let mut tracker = InflightTracker::default();
        let (a, _ra) = entry(1);
        tracker.enqueued(a);
        tracker.on_event(written(1));
        assert!(!tracker.collision_pending());
        tracker.lost("eof");
        assert!(!tracker.collision_pending());
    }

    #[tokio::test]
    async fn a_fake_publisher_fails_publishes_transiently() {
        let error = fake_publisher(false)
            .publish("t", Bytes::new(), 1, false)
            .await
            .expect_err("no engine");
        assert_eq!(error.retry(), Retry::Transient);
        assert!(fake_publisher(true).collision_lost());
        assert!(!fake_publisher(false).collision_lost());
    }

    #[test]
    fn out_of_order_acks_complete_the_matching_publish() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        let (b, mut rb) = entry(1);
        let (c, mut rc) = entry(1);
        tracker.enqueued(a);
        tracker.enqueued(b);
        tracker.enqueued(c);
        tracker.on_event(written(1));
        tracker.on_event(written(2));
        tracker.on_event(written(3));

        tracker.on_event(acked(2));
        assert!(matches!(settled(&mut rb), Some(Ok(()))));
        assert!(settled(&mut ra).is_none());
        assert!(settled(&mut rc).is_none());

        tracker.on_event(acked(3));
        assert!(matches!(settled(&mut rc), Some(Ok(()))));
        tracker.on_event(acked(1));
        assert!(matches!(settled(&mut ra), Some(Ok(()))));
        assert_eq!(tracker.outstanding(), 0);
    }

    #[test]
    fn a_held_publish_completes_only_after_it_is_written() {
        let mut tracker = InflightTracker::default();
        let (x, mut rx) = entry(1);
        let (a, mut ra) = entry(1);
        tracker.enqueued(x);
        tracker.on_event(written(1));
        tracker.enqueued(a);

        tracker.on_event(held(1));
        tracker.on_event(acked(1));
        assert!(matches!(settled(&mut rx), Some(Ok(()))));
        assert!(settled(&mut ra).is_none(), "held, not yet on the wire");

        tracker.on_event(written(1));
        assert!(settled(&mut ra).is_none());
        tracker.on_event(acked(1));
        assert!(matches!(settled(&mut ra), Some(Ok(()))));
        assert_eq!(tracker.outstanding(), 0);
    }

    #[test]
    fn connection_loss_fails_every_outstanding_publish_as_transient() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        let (b, mut rb) = entry(1);
        tracker.enqueued(a);
        tracker.on_event(written(1));
        tracker.enqueued(b);

        tracker.lost("reset by peer");
        for outcome in [settled(&mut ra), settled(&mut rb)] {
            let error = outcome.expect("settled").expect_err("failed");
            assert_eq!(error.retry(), Retry::Transient);
            assert!(error.to_string().contains("reset by peer"));
        }
        assert_eq!(tracker.outstanding(), 0);
    }

    #[test]
    fn a_resumed_session_replays_are_ignored_and_new_publishes_match() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        tracker.enqueued(a);
        tracker.on_event(written(1));
        tracker.lost("eof");
        assert!(matches!(settled(&mut ra), Some(Err(_))));

        tracker.on_event(connack(true));
        tracker.on_event(written(1));
        tracker.on_event(acked(1));

        let (c, mut rc) = entry(1);
        tracker.enqueued(c);
        tracker.on_event(written(2));
        tracker.on_event(acked(2));
        assert!(matches!(settled(&mut rc), Some(Ok(()))));
    }

    #[test]
    fn a_new_session_drops_stale_slots_so_they_cannot_steal_events() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        let (b, mut rb) = entry(0);
        tracker.enqueued(a);
        tracker.enqueued(b);
        tracker.lost("eof");
        assert!(matches!(settled(&mut ra), Some(Err(_))));
        assert!(matches!(settled(&mut rb), Some(Err(_))));

        tracker.on_event(connack(false));
        let (c, mut rc) = entry(1);
        tracker.enqueued(c);
        tracker.on_event(written(1));
        tracker.on_event(acked(1));
        assert!(matches!(settled(&mut rc), Some(Ok(()))));
    }

    #[test]
    fn qos0_completes_on_write() {
        let mut tracker = InflightTracker::default();
        let (q, mut rq) = entry(0);
        tracker.enqueued(q);
        assert!(settled(&mut rq).is_none());
        tracker.on_event(written(0));
        assert!(matches!(settled(&mut rq), Some(Ok(()))));
        assert_eq!(tracker.outstanding(), 0);
    }

    #[test]
    fn an_ack_before_the_new_connack_cannot_complete_a_new_epoch_publish() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        tracker.enqueued(a);
        tracker.on_event(written(1));
        tracker.lost("eof");
        assert_eq!(tracker.epoch(), 1);
        assert!(matches!(settled(&mut ra), Some(Err(_))));

        let (b, mut rb) = entry(1);
        tracker.enqueued(b);
        tracker.on_event(acked(1));
        tracker.on_event(written(1));
        tracker.on_event(acked(1));
        assert!(
            settled(&mut rb).is_none(),
            "no event before ConnAck may match"
        );

        tracker.on_event(connack(false));
        tracker.on_event(written(1));
        assert!(
            settled(&mut rb).is_none(),
            "written but not yet acknowledged"
        );
        tracker.on_event(acked(1));
        assert!(matches!(settled(&mut rb), Some(Ok(()))));
    }

    #[test]
    fn a_resumed_replay_after_a_second_loss_is_not_taken_for_a_new_publish() {
        let mut tracker = InflightTracker::default();
        let (b, mut rb) = entry(1);
        tracker.enqueued(b);
        tracker.lost("eof");
        assert!(matches!(settled(&mut rb), Some(Err(_))));

        tracker.on_event(connack(true));
        tracker.on_event(written(7));
        tracker.lost("eof");
        tracker.on_event(connack(true));
        tracker.on_event(written(7));

        let (c, mut rc) = entry(1);
        tracker.enqueued(c);
        tracker.on_event(written(8));
        tracker.on_event(acked(8));
        assert!(matches!(settled(&mut rc), Some(Ok(()))));
    }

    #[test]
    fn a_new_epoch_publish_completes_only_on_an_ack_after_its_own_write() {
        let mut tracker = InflightTracker::default();
        let (a, mut ra) = entry(1);
        tracker.enqueued(a);
        tracker.on_event(written(1));
        tracker.lost("reset");
        assert!(matches!(settled(&mut ra), Some(Err(_))));

        let (c, mut rc) = entry(1);
        tracker.enqueued(c);
        tracker.on_event(acked(1));
        tracker.on_event(connack(true));
        assert!(
            settled(&mut rc).is_none(),
            "a stale ack must not complete c"
        );

        tracker.on_event(written(1));
        assert!(
            settled(&mut rc).is_none(),
            "the replay of packet 1 is not c"
        );
        tracker.on_event(written(2));
        tracker.on_event(acked(1));
        assert!(
            settled(&mut rc).is_none(),
            "the old ack must not complete c"
        );
        tracker.on_event(acked(2));
        assert!(matches!(settled(&mut rc), Some(Ok(()))));
        assert_eq!(tracker.outstanding(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unacknowledged_publish_times_out() {
        let (publisher, _queue) = test_publisher(4, Duration::from_secs(2));
        let error = publisher
            .publish("t", Bytes::from_static(b"x"), 1, false)
            .await
            .expect_err("no broker, so no acknowledgement");
        assert_eq!(error, MqttError::PublishTimeout(Duration::from_secs(2)));
        assert_eq!(error.retry(), Retry::Transient);
    }

    #[tokio::test]
    async fn max_in_flight_bounds_outstanding_publishes() {
        let (publisher, mut queue) = test_publisher(2, Duration::from_secs(60));
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let publisher = publisher.clone();
            tasks.push(tokio::spawn(async move {
                publisher.publish("t", Bytes::new(), 1, false).await
            }));
        }
        let first_task = tasks.remove(0);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let Some(Command::Publish(first)) = queue.try_recv().ok() else {
            panic!("the first publish reaches the queue");
        };
        let Some(Command::Publish(_second)) = queue.try_recv().ok() else {
            panic!("the second publish reaches the queue");
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            queue.try_recv().is_err(),
            "a third publish waits for a permit"
        );

        drop(first);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(queue.try_recv(), Ok(Command::Publish(_))),
            "a completed publish releases its permit"
        );
        assert!(matches!(first_task.await, Ok(Err(_))));
    }

    #[tokio::test]
    async fn qos_above_one_is_rejected() {
        let (publisher, _queue) = test_publisher(1, Duration::from_secs(1));
        let error = publisher
            .publish("t", Bytes::new(), 2, false)
            .await
            .expect_err("QoS 2 is not supported");
        assert!(matches!(error, MqttError::Config(_)));
    }
}
