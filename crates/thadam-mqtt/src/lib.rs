//! MQTT source connector.
//!
//! # Responsibility
//!
//! - An MQTT source implementing the pull `Source` contract.
//! - Persistent sessions and ack-after-commit.
//! - Liveness under backpressure.
//! - TLS, reconnect, and detection of session loss.
//!
//! # Deliberately not exposed
//!
//! - MQTT client library types.
//! - MQTT publishing.
//! - Shared subscriptions (deferred).
//! - Sparkplug B decoding.
//! - Wildcard-per-connection sharding.
//!
//! # Design (ADR-0006, spike S2)
//!
//! Two tasks sit between the client library and the runtime:
//!
//! - The **poll task** owns the event loop. It awaits `poll()` to completion
//!   and nothing else, never races it, and never blocks on the runtime, so
//!   keepalive survives any backpressure. Incoming messages go to a queue that
//!   is unbounded in count: QoS 1 and 2 are bounded by the broker's window
//!   (withholding acknowledgements is the backpressure), QoS 0 by a byte budget
//!   with counted drops.
//! - The **ack task** turns completed [`AckHandle`](thadam_core::AckHandle)s into
//!   PUBACKs, in receive order, holding only a packet identifier and QoS.
//!
//! Every connection bumps the epoch of an [`EpochGate`](thadam_core::EpochGate), so
//! a handle issued before a reconnect or a session loss is dropped and counted
//! instead of acknowledging the wrong message. When the broker did not keep a
//! session that was expected, the connector raises `SESSION_LOST` (a warning, the
//! `session_lost` metric, an optional hook, and a `mqtt.gap` attribute on the
//! first message after the gap) and subscribes again.
//!
//! # TLS
//!
//! `mqtts://` brokers are reached through an in-process relay (`tls.rs`): the
//! client library speaks plain MQTT over a private Unix socket and the relay
//! terminates TLS with `tokio-rustls` and the `ring` provider. The client
//! library is built without its own TLS features because all of them depend on
//! a `rustls-webpki` release that has open security advisories.
//!
//! # Attributes
//!
//! Every message carries `topic` plus `mqtt.topic` (both the interned topic),
//! `mqtt.qos`, `mqtt.retain` and `mqtt.dup`. `topic` is the normalized
//! attribute the chain's `match.topic` filter and `{topic.N}` identity
//! templates read (`website/docs/src/configuration/datasets.md`); the `mqtt.*` keys
//! are the connector's documented attribute table. The first message after a
//! gap also carries `mqtt.gap` (`session_lost`, `queue_full`, `oversize`,
//! comma-joined) and, when messages were dropped, `mqtt.gap_dropped`.
//!
//! # Running the Docker tests
//!
//! Tests that need a broker are `#[ignore]`d so a machine without Docker still
//! passes `cargo nextest run`. With Docker available:
//!
//! ```text
//! cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(conformance) | binary(docker_e2e)'
//! ```

mod ack_task;
mod config;
mod error;
mod factory;
mod link;
mod poll_task;
pub mod sink;
mod source;
mod state;
mod stats;
mod tls;

pub use config::{
    AckModeConfig, DurationSpec, MqttConfig, Protocol, TlsConfig, TopicConfig, config_schema,
};
pub use error::MqttError;
pub use factory::{HandleObserver, MqttSourceFactory};
pub use source::MqttSource;
pub use stats::{MqttHandle, MqttStatsSnapshot, SessionLost, SessionLostHook};
