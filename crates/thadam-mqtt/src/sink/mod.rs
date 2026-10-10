//! MQTT publish sink: the pure building blocks.
//!
//! Configuration, topic templates, the event envelope, the publish engine, and
//! the batch sink built from them.
//!
//! - [`MqttPublishConfig`] is the typed, validated `config:` block.
//! - [`TopicTemplate`] renders the per-event topic from `{dataset}` and the
//!   identity levels.
//! - [`event_json`] encodes one event as the `payload: json` document.
//! - [`MqttPublishSink`] is the [`thadam_core::BatchSink`] and
//!   [`MqttPublishSinkFactory`] builds it for `kind: mqtt_publish`.

mod config;
pub(crate) mod engine;
mod envelope;
mod factory;
#[allow(clippy::module_inception)]
mod sink;
mod topic;

pub use config::{MqttPublishConfig, PayloadFormat, config_schema};
pub use envelope::{EnvelopeError, event_json};
pub use factory::MqttPublishSinkFactory;
pub use sink::MqttPublishSink;
pub use topic::{MAX_TOPIC_BYTES, TopicError, TopicTemplate, TopicTemplateError};
