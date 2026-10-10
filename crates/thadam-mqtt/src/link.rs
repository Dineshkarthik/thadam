//! The only module that touches the MQTT client library's event loop.
//!
//! Everything above this layer sees [`LinkEvent`], [`LinkError`] and
//! [`ClientHandle`], so no client-library type leaves this file and the
//! library can be replaced without touching the connector (ADR-0006).
//!
//! `Link::poll` awaits the library's `poll()` directly. It is never wrapped in
//! `select!`, `timeout`, or `now_or_never`: `EventLoop::poll` is not
//! cancel-safe on its write path, and a dropped poll loses acknowledgements
//! silently (spike S2, ADR-0002, ADR-0006 rule 1). `tests/poll_discipline.rs`
//! fails the build if that changes.

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;

use crate::config::Protocol;

/// A message from the broker, reduced to what the connector needs.
#[derive(Debug)]
pub(crate) struct InPublish {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
    pub dup: bool,
    pub pkid: u16,
}

/// What the event loop reported.
#[derive(Debug)]
pub(crate) enum LinkEvent {
    ConnAck {
        session_present: bool,
    },
    SubAck {
        rejected: usize,
    },
    Publish(InPublish),
    /// A PUBACK or PUBREC left for the socket.
    AckWritten,
    /// A DISCONNECT left for the socket.
    DisconnectSent,
    /// A PUBLISH left for the socket. `pkid` is 0 for QoS 0.
    PublishWritten {
        pkid: u16,
    },
    /// A QoS 1 PUBLISH was held back because the packet id it would use is
    /// still awaiting its acknowledgement. It leaves later as `PublishWritten`
    /// with the same packet id.
    PublishHeld {
        pkid: u16,
    },
    /// The broker acknowledged a QoS 1 PUBLISH.
    PubAck {
        pkid: u16,
    },
    Other,
}

/// Why the event loop failed. It reconnects on the next poll.
#[derive(Debug)]
pub(crate) enum LinkError {
    /// The broker refused the connection. `fatal` when retrying cannot help.
    Refused {
        fatal: bool,
        detail: String,
    },
    /// A packet exceeded the incoming limit.
    Oversize {
        size: usize,
    },
    /// The TLS relay could not complete a handshake with the broker.
    Tls(String),
    Network(String),
}

/// Where the client library connects. TLS brokers are reached through a local
/// Unix socket served by the TLS relay (`tls.rs`).
#[derive(Clone, Debug)]
pub(crate) enum Endpoint {
    Tcp { host: String, port: u16 },
    Unix(PathBuf),
}

/// Everything needed to open a connection.
pub(crate) struct LinkParams {
    pub protocol: Protocol,
    pub client_id: String,
    pub endpoint: Endpoint,
    pub keep_alive: Duration,
    pub connect_timeout_secs: u64,
    pub clean_start: bool,
    pub max_incoming_packet_size: usize,
    pub credentials: Option<(String, String)>,
    pub request_channel_capacity: usize,
    #[cfg_attr(not(feature = "v5"), allow(dead_code))]
    pub session_expiry_secs: Option<u32>,
    #[cfg_attr(not(feature = "v5"), allow(dead_code))]
    pub receive_maximum: Option<u16>,
}

pub(crate) enum Link {
    V4(Box<rumqttc::EventLoop>),
    #[cfg(feature = "v5")]
    V5(Box<rumqttc::v5::EventLoop>),
}

#[derive(Clone)]
pub(crate) enum ClientHandle {
    V4(rumqttc::AsyncClient),
    #[cfg(feature = "v5")]
    V5(rumqttc::v5::AsyncClient),
}

/// Why a request to the client failed: its channel is closed.
#[derive(Debug)]
pub(crate) struct ClientClosed;

pub(crate) fn open(params: &LinkParams) -> (Link, ClientHandle) {
    match params.protocol {
        Protocol::V311 => {
            let (el, client) = v4::open(params);
            (Link::V4(Box::new(el)), ClientHandle::V4(client))
        }
        #[cfg(feature = "v5")]
        Protocol::V5 => {
            let (el, client) = v5::open(params);
            (Link::V5(Box::new(el)), ClientHandle::V5(client))
        }
        #[cfg(not(feature = "v5"))]
        Protocol::V5 => unreachable!("config validation rejects v5 without the `v5` feature"),
    }
}

impl Link {
    /// Awaits the library's `poll()` to completion. See the module docs.
    pub async fn poll(&mut self) -> Result<LinkEvent, LinkError> {
        match self {
            Link::V4(el) => v4::map(el.poll().await),
            #[cfg(feature = "v5")]
            Link::V5(el) => v5::map(el.poll().await),
        }
    }
}

impl ClientHandle {
    /// Queues the acknowledgement for one message, waiting for room in the
    /// request channel. Only the ack task calls this, never the poll task.
    pub async fn ack(&self, pkid: u16, qos: u8) -> Result<(), ClientClosed> {
        match self {
            ClientHandle::V4(c) => v4::ack(c, pkid, qos).await,
            #[cfg(feature = "v5")]
            ClientHandle::V5(c) => v5::ack(c, pkid, qos).await,
        }
    }

    pub async fn subscribe(&self, topics: &[(String, u8)]) -> Result<(), ClientClosed> {
        match self {
            ClientHandle::V4(c) => v4::subscribe(c, topics).await,
            #[cfg(feature = "v5")]
            ClientHandle::V5(c) => v5::subscribe(c, topics).await,
        }
    }

    /// Queues a PUBLISH for the event loop, waiting for room in the request
    /// channel. The packet id is assigned when the event loop writes it.
    pub async fn publish(
        &self,
        topic: &str,
        qos: u8,
        retain: bool,
        payload: Bytes,
    ) -> Result<(), ClientClosed> {
        match self {
            ClientHandle::V4(c) => v4::publish(c, topic, qos, retain, payload).await,
            #[cfg(feature = "v5")]
            ClientHandle::V5(c) => v5::publish(c, topic, qos, retain, payload).await,
        }
    }

    /// Asks for a DISCONNECT without waiting. Used by the poll task, which must
    /// never wait on anything but `poll`.
    pub fn try_disconnect(&self) {
        let _ = match self {
            ClientHandle::V4(c) => c.try_disconnect().map_err(drop),
            #[cfg(feature = "v5")]
            ClientHandle::V5(c) => c.try_disconnect().map_err(drop),
        };
    }

    pub async fn disconnect(&self) -> Result<(), ClientClosed> {
        match self {
            ClientHandle::V4(c) => c.disconnect().await.map_err(|_| ClientClosed),
            #[cfg(feature = "v5")]
            ClientHandle::V5(c) => c.disconnect().await.map_err(|_| ClientClosed),
        }
    }
}

mod v4 {
    use rumqttc::mqttbytes::Error as CodecError;
    use rumqttc::{
        AsyncClient, ConnectReturnCode, ConnectionError, Event, EventLoop, Incoming, MqttOptions,
        NetworkOptions, Outgoing, Publish, QoS, StateError, SubscribeFilter, SubscribeReasonCode,
        Transport,
    };

    use super::{ClientClosed, Endpoint, InPublish, LinkError, LinkEvent, LinkParams};

    pub(super) fn qos(n: u8) -> QoS {
        match n {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }

    pub(super) fn open(p: &LinkParams) -> (EventLoop, AsyncClient) {
        let mut options = match &p.endpoint {
            Endpoint::Tcp { host, port } => MqttOptions::new(p.client_id.clone(), host, *port),
            Endpoint::Unix(path) => {
                let mut options = MqttOptions::new(p.client_id.clone(), path.to_string_lossy(), 0);
                options.set_transport(Transport::unix());
                options
            }
        };
        options
            .set_keep_alive(p.keep_alive)
            .set_clean_session(p.clean_start)
            .set_manual_acks(true)
            .set_max_packet_size(p.max_incoming_packet_size, p.max_incoming_packet_size)
            .set_pending_throttle(std::time::Duration::ZERO);
        if let Some((user, password)) = &p.credentials {
            options.set_credentials(user.clone(), password.clone());
        }
        let (client, mut event_loop) = AsyncClient::new(options, p.request_channel_capacity);
        let mut network = NetworkOptions::new();
        network.set_connection_timeout(p.connect_timeout_secs);
        network.set_tcp_nodelay(true);
        event_loop.set_network_options(network);
        (event_loop, client)
    }

    pub(super) fn map(result: Result<Event, ConnectionError>) -> Result<LinkEvent, LinkError> {
        match result {
            Ok(Event::Incoming(Incoming::ConnAck(c))) => Ok(LinkEvent::ConnAck {
                session_present: c.session_present,
            }),
            Ok(Event::Incoming(Incoming::SubAck(s))) => Ok(LinkEvent::SubAck {
                rejected: s
                    .return_codes
                    .iter()
                    .filter(|c| matches!(c, SubscribeReasonCode::Failure))
                    .count(),
            }),
            Ok(Event::Incoming(Incoming::Publish(p))) => Ok(LinkEvent::Publish(InPublish {
                topic: p.topic,
                payload: p.payload,
                qos: p.qos as u8,
                retain: p.retain,
                dup: p.dup,
                pkid: p.pkid,
            })),
            Ok(Event::Incoming(Incoming::PubAck(a))) => Ok(LinkEvent::PubAck { pkid: a.pkid }),
            Ok(Event::Outgoing(Outgoing::Publish(pkid))) => Ok(LinkEvent::PublishWritten { pkid }),
            Ok(Event::Outgoing(Outgoing::AwaitAck(pkid))) => Ok(LinkEvent::PublishHeld { pkid }),
            Ok(Event::Outgoing(Outgoing::PubAck(_) | Outgoing::PubRec(_))) => {
                Ok(LinkEvent::AckWritten)
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) => Ok(LinkEvent::DisconnectSent),
            Ok(_) => Ok(LinkEvent::Other),
            Err(e) => Err(classify(e)),
        }
    }

    fn classify(e: ConnectionError) -> LinkError {
        match e {
            ConnectionError::ConnectionRefused(code) => LinkError::Refused {
                fatal: !matches!(code, ConnectReturnCode::ServiceUnavailable),
                detail: format!("{code:?}"),
            },
            ConnectionError::MqttState(StateError::Deserialization(
                CodecError::PayloadSizeLimitExceeded(size),
            )) => LinkError::Oversize { size },
            other => LinkError::Network(other.to_string()),
        }
    }

    pub(super) async fn ack(c: &AsyncClient, pkid: u16, q: u8) -> Result<(), ClientClosed> {
        let stripped = Publish {
            dup: false,
            qos: qos(q),
            retain: false,
            topic: String::new(),
            pkid,
            payload: bytes::Bytes::new(),
        };
        c.ack(&stripped).await.map_err(|_| ClientClosed)
    }

    pub(super) async fn subscribe(
        c: &AsyncClient,
        topics: &[(String, u8)],
    ) -> Result<(), ClientClosed> {
        let filters: Vec<SubscribeFilter> = topics
            .iter()
            .map(|(filter, q)| SubscribeFilter::new(filter.clone(), qos(*q)))
            .collect();
        c.subscribe_many(filters).await.map_err(|_| ClientClosed)
    }

    pub(super) async fn publish(
        c: &AsyncClient,
        topic: &str,
        q: u8,
        retain: bool,
        payload: bytes::Bytes,
    ) -> Result<(), ClientClosed> {
        c.publish(topic, qos(q), retain, payload)
            .await
            .map_err(|_| ClientClosed)
    }
}

#[cfg(feature = "v5")]
mod v5 {
    use std::time::Duration;

    use bytes::Bytes;
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::mqttbytes::v5::{ConnectReturnCode, Filter, Packet, Publish};
    use rumqttc::v5::{AsyncClient, ConnectionError, Event, EventLoop, MqttOptions, StateError};
    use rumqttc::{NetworkOptions, Outgoing, Transport};

    use super::{ClientClosed, Endpoint, InPublish, LinkError, LinkEvent, LinkParams};

    fn qos(n: u8) -> QoS {
        match n {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }

    pub(super) fn open(p: &LinkParams) -> (EventLoop, AsyncClient) {
        let mut options = match &p.endpoint {
            Endpoint::Tcp { host, port } => MqttOptions::new(p.client_id.clone(), host, *port),
            Endpoint::Unix(path) => {
                let mut options = MqttOptions::new(p.client_id.clone(), path.to_string_lossy(), 0);
                options.set_transport(Transport::unix());
                options
            }
        };
        options
            .set_keep_alive(p.keep_alive)
            .set_clean_start(p.clean_start)
            .set_manual_acks(true)
            .set_connection_timeout(p.connect_timeout_secs)
            .set_pending_throttle(Duration::ZERO);
        let max_packet = u32::try_from(p.max_incoming_packet_size).unwrap_or(u32::MAX);
        options.set_max_packet_size(Some(max_packet));
        options.set_session_expiry_interval(p.session_expiry_secs);
        options.set_receive_maximum(p.receive_maximum);
        if let Some((user, password)) = &p.credentials {
            options.set_credentials(user.clone(), password.clone());
        }
        let mut network = NetworkOptions::new();
        network.set_connection_timeout(p.connect_timeout_secs);
        network.set_tcp_nodelay(true);
        options.set_network_options(network);
        let (client, event_loop) = AsyncClient::new(options, p.request_channel_capacity);
        (event_loop, client)
    }

    pub(super) fn map(result: Result<Event, ConnectionError>) -> Result<LinkEvent, LinkError> {
        match result {
            Ok(Event::Incoming(Packet::ConnAck(c))) => Ok(LinkEvent::ConnAck {
                session_present: c.session_present,
            }),
            Ok(Event::Incoming(Packet::SubAck(s))) => Ok(LinkEvent::SubAck {
                rejected: s
                    .return_codes
                    .iter()
                    .filter(|c| {
                        !matches!(
                            c,
                            rumqttc::v5::mqttbytes::v5::SubscribeReasonCode::Success(_)
                        )
                    })
                    .count(),
            }),
            Ok(Event::Incoming(Packet::Publish(p))) => Ok(LinkEvent::Publish(InPublish {
                topic: String::from_utf8_lossy(&p.topic).into_owned(),
                payload: p.payload,
                qos: p.qos as u8,
                retain: p.retain,
                dup: p.dup,
                pkid: p.pkid,
            })),
            Ok(Event::Incoming(Packet::PubAck(a))) => Ok(LinkEvent::PubAck { pkid: a.pkid }),
            Ok(Event::Outgoing(Outgoing::Publish(pkid))) => Ok(LinkEvent::PublishWritten { pkid }),
            Ok(Event::Outgoing(Outgoing::AwaitAck(pkid))) => Ok(LinkEvent::PublishHeld { pkid }),
            Ok(Event::Outgoing(Outgoing::PubAck(_) | Outgoing::PubRec(_))) => {
                Ok(LinkEvent::AckWritten)
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) => Ok(LinkEvent::DisconnectSent),
            Ok(_) => Ok(LinkEvent::Other),
            Err(e) => Err(classify(e)),
        }
    }

    fn classify(e: ConnectionError) -> LinkError {
        match e {
            ConnectionError::ConnectionRefused(code) => LinkError::Refused {
                fatal: !matches!(
                    code,
                    ConnectReturnCode::ServiceUnavailable
                        | ConnectReturnCode::ServerUnavailable
                        | ConnectReturnCode::ServerBusy
                        | ConnectReturnCode::ConnectionRateExceeded
                ),
                detail: format!("{code:?}"),
            },
            ConnectionError::MqttState(StateError::Deserialization(
                rumqttc::v5::mqttbytes::Error::PayloadSizeLimitExceeded { pkt_size, .. },
            )) => LinkError::Oversize { size: pkt_size },
            other => LinkError::Network(other.to_string()),
        }
    }

    pub(super) async fn ack(c: &AsyncClient, pkid: u16, q: u8) -> Result<(), ClientClosed> {
        let stripped = Publish {
            dup: false,
            qos: qos(q),
            retain: false,
            topic: Bytes::new(),
            pkid,
            payload: Bytes::new(),
            properties: None,
        };
        c.ack(&stripped).await.map_err(|_| ClientClosed)
    }

    pub(super) async fn subscribe(
        c: &AsyncClient,
        topics: &[(String, u8)],
    ) -> Result<(), ClientClosed> {
        let filters: Vec<Filter> = topics
            .iter()
            .map(|(filter, q)| Filter::new(filter.clone(), qos(*q)))
            .collect();
        c.subscribe_many(filters).await.map_err(|_| ClientClosed)
    }

    pub(super) async fn publish(
        c: &AsyncClient,
        topic: &str,
        q: u8,
        retain: bool,
        payload: Bytes,
    ) -> Result<(), ClientClosed> {
        c.publish(topic, qos(q), retain, payload)
            .await
            .map_err(|_| ClientClosed)
    }
}
