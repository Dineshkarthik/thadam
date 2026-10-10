//! The NATS JetStream batch sink (`kind: nats`).
//!
//! Publishes each event of a batch as one JetStream message with a
//! `Nats-Msg-Id` header set to the event id (hex), so a stream whose
//! `duplicate_window` is positive deduplicates the runtime's retries (the
//! batch driver re-sends the same [`thadam_core::BatchId`]). `open` probes the
//! stream and reports [`thadam_core::DeliveryClass::IdempotentBatch`] only
//! when that window is positive, [`thadam_core::DeliveryClass::AtLeastOnce`]
//! otherwise — the target, not the configuration, decides what is reported.
//!
//! The stream must exist before the sink opens unless `create_stream: true`
//! (default `false`): a missing stream without it is a configuration error,
//! matching the repo's rule that operators own provisioning.
//!
//! Every event travels as one JSON document ([`envelope`]): the `_`-prefixed
//! lineage fields, the asset identity levels, and the record fields merged
//! into one object. One event per message keeps per-event rejection (too
//! large, unrenderable subject) granular: the event lands in quarantine and
//! the rest of the batch proceeds.

pub mod config;
pub mod envelope;
pub mod error;
pub mod factory;
pub mod sink;

pub use config::{Auth, NatsConfig, Secret, TlsConfig, config_schema, parse_config};
pub use factory::NatsSinkFactory;
pub use sink::NatsSink;
