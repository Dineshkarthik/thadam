//! `MqttPublishSink`: the batch sink that publishes each event as one MQTT
//! message.
//!
//! Delivery. QoS 1 is at least once: a batch that fails in flight is retried
//! whole, so consumers must dedupe by `_event_id`, which every payload carries.
//! QoS 0 is at most once: a write that failed in flight may be lost, and it is
//! never applied twice. `open` reports the class that matches the configured
//! QoS.
//!
//! Rebuild rule. The engine tracks acknowledgements by packet id. If the
//! connection drops while a packet-id collision is outstanding, rumqttc may
//! still write the colliding publish under its old id after the reconnect, and
//! the engine cannot tell that write from a newer publish that reused the id.
//! The engine then reports the loss through [`Publisher::collision_lost`] and
//! refuses later publishes. The sink drops the client, relay, and engine, and
//! connects again before its next publish, so the new publish starts a fresh
//! session with no packet id left over from the old one.

use async_trait::async_trait;
use bytes::Bytes;
use thadam_core::{
    BatchSink, DeliveryClass, ErrorDomain, Event, Reject, ThadamError, SinkBatch, SinkCapabilities,
    SinkContext, WriteOutcome, codes,
};
use tokio::task::JoinSet;

use super::config::MqttPublishConfig;
use super::engine::{self, EngineOptions, Publisher};
use super::envelope::{EnvelopeError, event_json};
use super::topic::{TopicError, TopicTemplate};
use crate::config::{Protocol, parse_broker_url};
use crate::error::MqttError;
use crate::link::{self, ClientHandle, Endpoint, LinkParams};
use crate::tls::{self, TlsRelay};

/// The sink only reads acknowledgements, so its incoming packets are small.
const INCOMING_PACKET_LIMIT: usize = 64 * 1024;

/// Publishes one dataset's events. Build it through
/// [`MqttPublishSinkFactory`](super::MqttPublishSinkFactory).
pub struct MqttPublishSink {
    config: MqttPublishConfig,
    dataset: String,
    template: TopicTemplate,
    open: bool,
    session: Option<Session>,
}

/// One client connection and the engine that drives it. Dropping the last
/// publisher stops the engine, and dropping the relay stops the TLS socket.
struct Session {
    publisher: Publisher,
    client: ClientHandle,
    _relay: Option<TlsRelay>,
}

struct Message {
    topic: String,
    payload: Bytes,
}

impl MqttPublishSink {
    pub(crate) fn new(config: MqttPublishConfig, dataset: String, template: TopicTemplate) -> Self {
        MqttPublishSink {
            config,
            dataset,
            template,
            open: false,
            session: None,
        }
    }

    async fn connect(&self) -> Result<Session, MqttError> {
        let (endpoint, relay) = self.endpoint().await?;
        let params = self.link_params(endpoint).await?;
        let (link, client) = link::open(&params);
        let link = engine::connect(link, self.config.connect_timeout.0).await?;
        let options = EngineOptions {
            max_in_flight: window(self.config.max_in_flight),
            request_timeout: self.config.request_timeout.0,
        };
        let publisher = engine::start(link, client.clone(), options);
        Ok(Session {
            publisher,
            client,
            _relay: relay,
        })
    }

    /// Chooses how the client reaches the broker: directly over TCP, or for
    /// `mqtts://` through a TLS relay that this sink owns.
    async fn endpoint(&self) -> Result<(Endpoint, Option<TlsRelay>), MqttError> {
        let broker = parse_broker_url(&self.config.url).map_err(MqttError::Config)?;
        if !broker.tls {
            let endpoint = Endpoint::Tcp {
                host: broker.host,
                port: broker.port,
            };
            return Ok((endpoint, None));
        }
        let tls_config = self.config.tls.clone().unwrap_or_default();
        let client_config = tls::build_client_config(&tls_config).await?;
        let relay = TlsRelay::start(
            client_config,
            &broker.host,
            broker.port,
            self.config.connect_timeout.0,
        )?;
        Ok((Endpoint::Unix(relay.socket().to_owned()), Some(relay)))
    }

    async fn link_params(&self, endpoint: Endpoint) -> Result<LinkParams, MqttError> {
        let credentials = match (&self.config.username, &self.config.password_file) {
            (Some(user), Some(path)) => {
                let raw =
                    tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| MqttError::Credentials {
                            what: "password_file",
                            detail: format!("{}: {e}", path.display()),
                        })?;
                Some((user.clone(), raw.trim_end_matches(['\n', '\r']).to_owned()))
            }
            (Some(user), None) => Some((user.clone(), String::new())),
            _ => None,
        };
        Ok(LinkParams {
            protocol: Protocol::V311,
            client_id: self.config.client_id.clone(),
            endpoint,
            keep_alive: self.config.keep_alive.0,
            connect_timeout_secs: self.config.connect_timeout.0.as_secs(),
            clean_start: true,
            max_incoming_packet_size: INCOMING_PACKET_LIMIT,
            credentials,
            request_channel_capacity: window(self.config.max_in_flight),
            session_expiry_secs: None,
            receive_maximum: None,
        })
    }

    fn publisher(&self) -> Result<Publisher, MqttError> {
        self.session
            .as_ref()
            .map(|s| s.publisher.clone())
            .ok_or(MqttError::State("the MQTT publish sink has no connection"))
    }
}

#[async_trait]
impl BatchSink for MqttPublishSink {
    async fn open(&mut self, _ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        self.session = Some(self.connect().await?);
        self.open = true;
        Ok(SinkCapabilities::new(delivery_for(self.config.qos)))
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        if !self.open {
            return Err(MqttError::State("the MQTT publish sink is not open").into());
        }
        if needs_fresh_session(self.session.as_ref().map(|s| &s.publisher)) {
            self.session = None;
            let session = self.connect().await?;
            self.session = Some(session);
        }
        let publisher = self.publisher()?;
        let (messages, rejects) = render(
            batch.events(),
            &self.template,
            &self.dataset,
            self.config.max_message_bytes,
        );
        publish_all(
            &publisher,
            messages,
            self.config.qos,
            self.config.retain,
            window(self.config.max_in_flight),
        )
        .await?;
        Ok(WriteOutcome::with_rejects(rejects))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.open = false;
        let Some(session) = self.session.take() else {
            return Ok(());
        };
        let timeout = self.config.request_timeout.0;
        if tokio::time::timeout(timeout, session.client.disconnect())
            .await
            .is_err()
        {
            session.client.try_disconnect();
            tracing::warn!(
                client_id = %self.config.client_id,
                "the broker did not take the DISCONNECT within request_timeout"
            );
        }
        Ok(())
    }
}

/// Whether the next write must connect afresh. A missing connection needs
/// one, and so does one whose engine reported a collision loss.
fn needs_fresh_session(session: Option<&Publisher>) -> bool {
    match session {
        None => true,
        Some(publisher) => publisher.collision_lost(),
    }
}

fn delivery_for(qos: u8) -> DeliveryClass {
    if qos == 0 {
        DeliveryClass::AtMostOnce
    } else {
        DeliveryClass::AtLeastOnce
    }
}

fn window(max_in_flight: u32) -> usize {
    usize::try_from(max_in_flight).unwrap_or(usize::MAX).max(1)
}

/// Renders every event. Events that cannot be rendered or encoded, or whose
/// payload is above `max_bytes`, are rejected alone by their index.
fn render(
    events: &[Event],
    template: &TopicTemplate,
    dataset: &str,
    max_bytes: u64,
) -> (Vec<Message>, Vec<Reject>) {
    let mut messages = Vec::with_capacity(events.len());
    let mut rejects = Vec::new();
    for (index, event) in events.iter().enumerate() {
        let index = u32::try_from(index).unwrap_or(u32::MAX);
        match prepare(event, template, dataset, max_bytes) {
            Ok(message) => messages.push(message),
            Err(error) => rejects.push(Reject::new(index, error)),
        }
    }
    (messages, rejects)
}

fn prepare(
    event: &Event,
    template: &TopicTemplate,
    dataset: &str,
    max_bytes: u64,
) -> Result<Message, ThadamError> {
    let topic = template
        .render(dataset, event.asset())
        .map_err(TopicError::into_reject_error)?;
    let json = event_json(event).map_err(EnvelopeError::into_reject_error)?;
    let size = json.len();
    if u64::try_from(size).unwrap_or(u64::MAX) > max_bytes {
        return Err(ThadamError::permanent(
            ErrorDomain::Sink,
            codes::SINK_REJECTED,
            format!("payload of {size} bytes is above max_message_bytes {max_bytes}"),
        ));
    }
    Ok(Message {
        topic,
        payload: Bytes::from(json),
    })
}

/// Publishes every message with at most `window` outstanding, and waits for
/// all of them. After the first failure no new publish starts, and the rest
/// are drained, so the caller can retry the whole batch.
async fn publish_all(
    publisher: &Publisher,
    messages: Vec<Message>,
    qos: u8,
    retain: bool,
    window: usize,
) -> Result<(), MqttError> {
    let window = window.max(1);
    let mut pending = messages.into_iter();
    let mut inflight: JoinSet<Result<(), MqttError>> = JoinSet::new();
    let mut failure = None;
    loop {
        while failure.is_none() && inflight.len() < window {
            let Some(message) = pending.next() else {
                break;
            };
            let publisher = publisher.clone();
            inflight.spawn(async move {
                publisher
                    .publish(message.topic, message.payload, qos, retain)
                    .await
            });
        }
        match inflight.join_next().await {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(error))) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            Some(Err(_)) => {
                if failure.is_none() {
                    failure = Some(MqttError::Panic);
                }
            }
            None => break,
        }
    }
    failure.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::super::engine::fake_publisher;
    use super::*;
    use thadam_core::{
        AssetPath, Classify, EventId, FieldDef, FieldType, Record, Retry, Shape, ShapeRef,
        TimeDomain, Times, Timestamp, Value, WalPos,
    };

    fn event(id: u128, asset: AssetPath) -> Event {
        let shape =
            ShapeRef::new(Shape::new(vec![FieldDef::new("v", FieldType::I64)]).expect("shape"));
        let record = Record::new(shape, vec![Value::I64(7)]).expect("record");
        Event::builder(
            EventId::from_u128(id),
            WalPos::new(1),
            Times::new(Timestamp::from_nanos(1_000), TimeDomain::Utc),
        )
        .asset(asset)
        .payload(record)
        .build()
        .expect("event")
    }

    fn robot(name: &str) -> AssetPath {
        AssetPath::new([("robot", name)]).expect("path")
    }

    fn message(topic: &str) -> Message {
        Message {
            topic: topic.to_owned(),
            payload: Bytes::from_static(b"{}"),
        }
    }

    fn template() -> TopicTemplate {
        TopicTemplate::parse("thadam/{dataset}/{robot}").expect("template")
    }

    #[test]
    fn qos_selects_the_delivery_class() {
        assert_eq!(delivery_for(0), DeliveryClass::AtMostOnce);
        assert_eq!(delivery_for(1), DeliveryClass::AtLeastOnce);
    }

    #[test]
    fn each_bad_event_is_rejected_alone_by_its_index() {
        let events = vec![
            event(1, robot("r7")),
            event(2, AssetPath::new([("site", "s1")]).expect("path")),
            event(3, robot("r8")),
        ];
        let (messages, rejects) = render(&events, &template(), "imu", 1 << 20);
        let topics: Vec<&str> = messages.iter().map(|m| m.topic.as_str()).collect();
        assert_eq!(topics, ["thadam/imu/r7", "thadam/imu/r8"]);
        assert_eq!(rejects.len(), 1);
        assert_eq!(rejects[0].index, 1);
        assert_eq!(rejects[0].error.code, codes::SINK_REJECTED);
        assert_eq!(rejects[0].error.retry, Retry::Permanent);
    }

    #[test]
    fn payloads_above_the_limit_are_rejected_per_event() {
        let events = vec![event(1, robot("r7")), event(2, robot("r8"))];
        let (messages, rejects) = render(&events, &template(), "imu", 1);
        assert!(messages.is_empty());
        let indexes: Vec<u32> = rejects.iter().map(|r| r.index).collect();
        assert_eq!(indexes, [0, 1]);
        assert!(rejects[0].error.message.contains("max_message_bytes"));
    }

    #[tokio::test]
    async fn a_failed_publish_fails_the_whole_batch_transiently() {
        let messages = vec![message("thadam/imu/r7"), message("thadam/imu/r8")];
        let error = publish_all(&fake_publisher(false), messages, 1, false, 2)
            .await
            .expect_err("no engine accepts the publishes");
        assert_eq!(error.retry(), Retry::Transient);
    }

    #[tokio::test]
    async fn an_empty_batch_publishes_nothing_and_succeeds() {
        publish_all(&fake_publisher(false), Vec::new(), 1, false, 0)
            .await
            .expect("nothing to publish");
    }

    #[test]
    fn a_collision_loss_forces_a_fresh_session() {
        assert!(needs_fresh_session(None));
        assert!(needs_fresh_session(Some(&fake_publisher(true))));
        assert!(!needs_fresh_session(Some(&fake_publisher(false))));
    }
}
