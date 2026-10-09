//! The poll task: the sole owner of the client event loop.
//!
//! It keeps the connection alive (PINGREQ) however slowly the runtime pulls.
//! Four rules, each from spike S2 and ADR-0006:
//!
//! 1. The only thing it awaits on the client library is `poll()`, to
//!    completion. It is never raced against a token, a timer, or anything
//!    else. Shutdown is a token check between polls.
//! 2. It never blocks on the runtime. Incoming messages go into a queue that is
//!    unbounded in count and bounded only by the broker's window (QoS 1 and 2)
//!    or by a byte budget with a non-blocking drop (QoS 0). There is no
//!    `send().await` into a bounded queue here.
//! 3. Every connection bumps the epoch, so acknowledgements for an earlier
//!    connection are dropped instead of acknowledging the wrong message.
//! 4. A `ConnAck` without a session that was expected raises `SESSION_LOST` and
//!    the SUBSCRIBE is sent again, because the client library does not.
//!
//! The one `select!` below waits out the reconnect backoff *between* polls.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use lru::LruCache;
use thadam_core::{Attributes, Clock, RawMessage, ThadamError, Sym, Utf8Bytes};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::MqttError;
use crate::link::{ClientHandle, InPublish, Link, LinkError, LinkEvent};
use crate::state::{PushOutcome, Shared};
use crate::stats::{SessionLost, SessionLostHook};
use crate::tls::TlsRelay;

const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(5);
const TOPIC_CACHE: usize = 1024;
/// Consecutive over-limit packets (or guard trips) before the connector
/// reports a fatal error instead of reconnecting forever.
const FATAL_STREAK: u64 = 3;
/// A connection that lived this long is not part of a failure streak.
const HEALTHY_AFTER: Duration = Duration::from_secs(60);

pub(crate) struct PollSettings {
    pub client_id: String,
    pub max_message_bytes: usize,
    pub max_incoming_packet_size: usize,
    /// Whether the very first connection should find a session already there
    /// (a previous run left a marker).
    pub session_expected: bool,
    /// Whether the connector asks the broker to keep a session (not a clean start).
    pub persistent: bool,
    pub guard_limit: usize,
}

pub(crate) struct PollTask {
    pub link: Link,
    pub client: ClientHandle,
    pub shared: Arc<Shared>,
    pub stop: CancellationToken,
    pub closing: Arc<AtomicBool>,
    pub clock: Arc<dyn Clock>,
    pub hook: Option<SessionLostHook>,
    pub settings: PollSettings,
    /// Kept alive for as long as the connection loop runs.
    pub relay: Option<TlsRelay>,
}

struct AttrNames {
    topic: Sym,
    qos: Sym,
    retain: Sym,
    dup: Sym,
    qos_values: [Utf8Bytes; 3],
    yes: Utf8Bytes,
    no: Utf8Bytes,
}

impl AttrNames {
    fn new() -> Self {
        AttrNames {
            topic: Sym::from("mqtt.topic"),
            qos: Sym::from("mqtt.qos"),
            retain: Sym::from("mqtt.retain"),
            dup: Sym::from("mqtt.dup"),
            qos_values: [
                Utf8Bytes::from("0"),
                Utf8Bytes::from("1"),
                Utf8Bytes::from("2"),
            ],
            yes: Utf8Bytes::from("true"),
            no: Utf8Bytes::from("false"),
        }
    }

    fn flag(&self, value: bool) -> Utf8Bytes {
        if value {
            self.yes.clone()
        } else {
            self.no.clone()
        }
    }
}

/// Interns topic strings so every message on a topic shares one allocation.
pub(crate) struct TopicInterner(LruCache<String, Sym>);

impl TopicInterner {
    pub fn new(capacity: usize) -> Self {
        TopicInterner(LruCache::new(
            NonZeroUsize::new(capacity.max(1)).unwrap_or(NonZeroUsize::MIN),
        ))
    }

    pub fn intern(&mut self, topic: String) -> Sym {
        if let Some(sym) = self.0.get(topic.as_str()) {
            return Sym::clone(sym);
        }
        let sym: Sym = Sym::from(topic.as_str());
        self.0.put(topic, Sym::clone(&sym));
        sym
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Builds the attributes of a received message.
fn attributes(names: &AttrNames, interner: &mut TopicInterner, p: &InPublish) -> Attributes {
    let mut attrs = Attributes::new();
    let topic = interner.intern(p.topic.clone());
    // The chain's routing contract (`match.topic` and `{topic.N}` identity
    // templates, docs/src/configuration/datasets.md) reads the normalized
    // `topic` attribute; the namespaced `mqtt.topic` documented for
    // attribute-table consumers stays as well.
    attrs.insert(Sym::from("topic"), Utf8Bytes::from(Sym::clone(&topic)));
    attrs.insert(names.topic.clone(), Utf8Bytes::from(topic));
    attrs.insert(
        names.qos.clone(),
        names.qos_values[usize::from(p.qos.min(2))].clone(),
    );
    attrs.insert(names.retain.clone(), names.flag(p.retain));
    attrs.insert(names.dup.clone(), names.flag(p.dup));
    attrs
}

#[derive(PartialEq, Eq)]
enum Flow {
    Continue,
    Stop,
}

impl PollTask {
    pub async fn run(mut self) {
        let names = AttrNames::new();
        let mut interner = TopicInterner::new(TOPIC_CACHE);
        let mut backoff = RECONNECT_MIN;
        let mut state = LoopState {
            session_expected: self.settings.session_expected,
            ..LoopState::default()
        };

        loop {
            if self.stop.is_cancelled() {
                break;
            }
            let polled = self.link.poll().await;
            match polled {
                Ok(event) => {
                    if self.on_event(event, &names, &mut interner, &mut state, &mut backoff)
                        == Flow::Stop
                    {
                        break;
                    }
                }
                Err(error) => {
                    if self.on_error(error, &mut state) == Flow::Stop {
                        break;
                    }
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = self.stop.cancelled() => break,
                    }
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                }
            }
        }
        self.shared.stats.connected.store(false, Ordering::Relaxed);
    }

    fn on_event(
        &mut self,
        event: LinkEvent,
        names: &AttrNames,
        interner: &mut TopicInterner,
        state: &mut LoopState,
        backoff: &mut Duration,
    ) -> Flow {
        match event {
            LinkEvent::ConnAck { session_present } => {
                *backoff = RECONNECT_MIN;
                self.on_connack(session_present, state);
            }
            LinkEvent::SubAck { rejected } => {
                if rejected > 0 {
                    self.shared.fail(
                        MqttError::SubscribeRejected(format!(
                            "{rejected} of the configured topic filters were refused by the broker"
                        ))
                        .into(),
                    );
                    return Flow::Stop;
                }
                self.shared.set_ready();
            }
            LinkEvent::Publish(p) => self.on_publish(&p, names, interner, state),
            LinkEvent::AckWritten => {
                self.shared
                    .stats
                    .acks_written
                    .fetch_add(1, Ordering::Relaxed);
            }
            LinkEvent::DisconnectSent => {
                if self.closing.load(Ordering::Acquire) {
                    return Flow::Stop;
                }
            }
            LinkEvent::Other => {}
        }
        Flow::Continue
    }

    fn on_connack(&mut self, session_present: bool, state: &mut LoopState) {
        let stats = &self.shared.stats;
        stats.connected.store(true, Ordering::Relaxed);
        let connects = stats.connects.fetch_add(1, Ordering::Relaxed) + 1;
        state.connected_at = Some(Instant::now());
        state.guard_tripped = false;

        let effect = self
            .shared
            .on_connack(session_present, state.session_expected);
        if effect.session_lost {
            stats.session_lost.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                client_id = %self.settings.client_id,
                epoch = effect.epoch.get(),
                "SESSION_LOST: the broker did not keep the persistent session; \
                 messages it held for this client are gone, resubscribing"
            );
            if let Some(hook) = &self.hook {
                hook(&SessionLost {
                    at: self.clock.now(),
                    epoch: effect.epoch,
                    client_id: self.settings.client_id.clone(),
                });
            }
        } else if effect.purged_messages > 0 {
            tracing::debug!(
                purged = effect.purged_messages,
                "session resumed: queued messages will be redelivered by the broker"
            );
        }
        // The client library does not resubscribe, and a resumed session
        // keeps its subscriptions. The first connection always subscribes so a
        // changed topic list takes effect.
        if connects == 1 || !session_present {
            self.shared.request_subscribe();
        }
        state.session_expected = self.settings.persistent;
    }

    fn on_publish(
        &mut self,
        p: &InPublish,
        names: &AttrNames,
        interner: &mut TopicInterner,
        state: &mut LoopState,
    ) {
        let stats = &self.shared.stats;
        stats.messages_received.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_received
            .fetch_add(p.payload.len() as u64, Ordering::Relaxed);
        if p.dup {
            stats.duplicates.fetch_add(1, Ordering::Relaxed);
        }
        if p.payload.len() > self.settings.max_message_bytes {
            self.shared.push_dropped_oversize(p.qos, p.pkid);
            return;
        }
        let msg = RawMessage {
            ingest_time: self.clock.now(),
            attrs: attributes(names, interner, p),
            payload: p.payload.clone(),
        };
        if let PushOutcome::Queued {
            guard_tripped: true,
        } = self.shared.push(msg, p.qos, p.pkid)
            && !state.guard_tripped
        {
            state.guard_tripped = true;
            let streak = self.shared.note_guard_trip();
            let unacked = self.shared.snapshot_sizes().unacked;
            if streak >= FATAL_STREAK {
                self.shared.fail(
                    MqttError::UnackedGuard {
                        unacked,
                        limit: self.settings.guard_limit,
                    }
                    .into(),
                );
                self.stop.cancel();
            } else {
                tracing::error!(
                    unacked,
                    limit = self.settings.guard_limit,
                    "unacknowledged messages exceed the guard; disconnecting. \
                     Set expected_broker_window to the broker's inflight window"
                );
                self.client.try_disconnect();
            }
        }
    }

    fn on_error(&mut self, error: LinkError, state: &mut LoopState) -> Flow {
        let stats = &self.shared.stats;
        if stats.connected.swap(false, Ordering::Relaxed) {
            stats.disconnects.fetch_add(1, Ordering::Relaxed);
            self.shared.on_disconnect();
        }
        let lived = state.connected_at.take().map(|at| at.elapsed());
        let error = self.explain(error);
        match error {
            LinkError::Refused {
                fatal: true,
                detail,
            } => {
                self.shared.fail(MqttError::Refused(detail).into());
                return Flow::Stop;
            }
            LinkError::Refused { detail, .. } => {
                tracing::warn!(%detail, "broker unavailable; retrying");
            }
            LinkError::Oversize { size } => {
                stats.oversize_disconnects.fetch_add(1, Ordering::Relaxed);
                if lived.is_some_and(|d| d >= HEALTHY_AFTER) {
                    state.oversize_streak = 0;
                }
                state.oversize_streak += 1;
                tracing::error!(
                    size,
                    limit = self.settings.max_incoming_packet_size,
                    streak = state.oversize_streak,
                    "the broker sent a packet above the incoming limit; the connection was dropped"
                );
                if state.oversize_streak >= FATAL_STREAK {
                    self.shared
                        .fail(ThadamError::from(MqttError::PacketTooLarge {
                            size,
                            limit: self.settings.max_incoming_packet_size,
                        }));
                    return Flow::Stop;
                }
            }
            LinkError::Tls(detail) => tracing::warn!(%detail, "TLS failure; retrying"),
            LinkError::Network(detail) => tracing::warn!(%detail, "connection lost; retrying"),
        }
        Flow::Continue
    }
}

impl PollTask {
    /// The client library only sees the relay's socket close when the broker
    /// cannot be reached; the relay knows why.
    fn explain(&self, error: LinkError) -> LinkError {
        let failure = self.relay.as_ref().and_then(TlsRelay::take_failure);
        match (error, failure) {
            (LinkError::Network(_), Some(failure)) if failure.is_tls => {
                LinkError::Tls(failure.detail)
            }
            (LinkError::Network(_), Some(failure)) => LinkError::Network(failure.detail),
            (error, _) => error,
        }
    }
}

#[derive(Default)]
struct LoopState {
    session_expected: bool,
    connected_at: Option<Instant>,
    guard_tripped: bool,
    oversize_streak: u64,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn publish(topic: &str, qos: u8, retain: bool, dup: bool) -> InPublish {
        InPublish {
            topic: topic.to_owned(),
            payload: Bytes::from_static(b"x"),
            qos,
            retain,
            dup,
            pkid: 1,
        }
    }

    #[test]
    fn attributes_carry_topic_qos_retain_and_dup() {
        let names = AttrNames::new();
        let mut interner = TopicInterner::new(8);
        let attrs = attributes(
            &names,
            &mut interner,
            &publish("robots/r7/telemetry", 1, true, false),
        );
        assert_eq!(attrs.get("topic"), Some("robots/r7/telemetry"));
        assert_eq!(attrs.get("mqtt.topic"), Some("robots/r7/telemetry"));
        assert_eq!(attrs.get("mqtt.qos"), Some("1"));
        assert_eq!(attrs.get("mqtt.retain"), Some("true"));
        assert_eq!(attrs.get("mqtt.dup"), Some("false"));
        assert_eq!(
            attrs.len(),
            5,
            "the normalized `topic` plus the `mqtt.*` attributes"
        );

        let attrs = attributes(&names, &mut interner, &publish("a", 2, false, true));
        assert_eq!(attrs.get("mqtt.qos"), Some("2"));
        assert_eq!(attrs.get("mqtt.dup"), Some("true"));
    }

    #[test]
    fn topics_are_interned_so_messages_share_one_allocation() {
        let names = AttrNames::new();
        let mut interner = TopicInterner::new(8);
        let a = attributes(&names, &mut interner, &publish("t/1", 1, false, false));
        let b = attributes(&names, &mut interner, &publish("t/1", 1, false, false));
        let (ta, tb) = (
            a.iter()
                .find(|(k, _)| *k == "mqtt.topic")
                .map(|(_, v)| v.as_ptr()),
            b.iter()
                .find(|(k, _)| *k == "mqtt.topic")
                .map(|(_, v)| v.as_ptr()),
        );
        assert_eq!(ta, tb, "the same topic text must be the same allocation");
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn the_interner_is_bounded_by_its_capacity() {
        let mut interner = TopicInterner::new(4);
        for i in 0..100 {
            interner.intern(format!("device/{i}"));
        }
        assert_eq!(interner.len(), 4);
        assert_eq!(&*interner.intern("device/99".into()), "device/99");
    }

    #[test]
    fn flag_and_qos_values_are_shared_constants() {
        let names = AttrNames::new();
        assert_eq!(names.flag(true).as_str(), "true");
        assert_eq!(names.flag(false).as_str(), "false");
        assert_eq!(names.qos_values[2].as_str(), "2");
    }
}
