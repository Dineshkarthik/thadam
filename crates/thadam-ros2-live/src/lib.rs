//! Live ROS 2 source (`kind: ros2_live`, ADR-0007 stage 4).
//!
//! Subscribes to ROS 2 topics through a `rmw_zenohd` router over Zenoh
//! (the `rmw_zenoh` transport; Jazzy and Lyrical wire format, verified by
//! spike S3) and emits raw CDR payloads. Decoding stays with the transform
//! chain's dataset `decode: ros2`: before the first message of a type can be
//! committed, the source writes the type's assembled `ros2msg` schema text —
//! built from the `.msg` files under `msg_dirs` — to the pipeline's schema
//! store and stamps every message with [`thadam_core::SCHEMA_HASH_ATTR`].
//!
//! Delivery is at-most-once (`ack_mode: none`): nothing is acknowledged to
//! the publisher, and under backpressure the receive queue drops and counts
//! instead of blocking the Zenoh callback. Losses are surfaced, never
//! silent: sequence gaps per publisher GID count router/middleware-side
//! drops, queue overflow counts client-side drops, and the running total
//! rides on later messages as `ros2.dropped`.
//!
//! The transport sits behind Thadam's own [`LiveRos2Transport`] trait
//! (ADR-0007); [`ZenohTransport`] is the only implementation in this step.
//! Liveliness/graph discovery is deliberately out of scope: the topic list
//! is explicit in the configuration.

mod config;
mod error;
mod factory;
mod gaps;
mod recv;
mod schemas;
mod source;
mod transport;
mod zenoh_impl;

pub use config::{ReconnectConfig, Ros2LiveConfig, TopicSpec, config_schema};
pub use error::Ros2LiveError;
pub use factory::Ros2LiveSourceFactory;
pub use source::Ros2LiveSource;
pub use transport::{LiveRos2Transport, RawRos2Message, TransportError};
pub use zenoh_impl::ZenohTransport;
