//! The Apache Kafka batch sink (`kind: kafka`).
//!
//! Publishes each event of a batch as one Kafka record: the topic rendered
//! from the `topic` template, the key from the `key` template (default the
//! event id), the value the JSON envelope ([`envelope`]), and an
//! `x-thadam-event-id` header repeating the event id so consumers can dedupe
//! the runtime's retries themselves. `acks: all` is fixed (the client's
//! produce request hardcodes `acks = -1`).
//!
//! Delivery class is [`thadam_core::DeliveryClass::AtLeastOnce`]: rskafka has
//! no idempotent-producer or transaction support (its produce request sends
//! `producer_id = -1`), so a batch retried after an unknown outcome can land
//! twice. The runtime retries a batch whole.
//!
//! # Client choice (owner decision)
//!
//! Pure Rust, no OpenSSL, no librdkafka, no aws-lc-sys (rustls + `ring` only,
//! ADR-0020). Evaluated on 2026-10-12:
//!
//! - **`rskafka` 0.6.0 (chosen)** — InfluxData's client, MIT OR Apache-2.0,
//!   MSRV 1.85. TLS via the `transport-tls`
//!   feature: the client takes a caller-built `Arc<rustls::ClientConfig>`
//!   and compiles no crypto provider of its own, so TLS is the workspace's
//!   rustls 0.23 with the `ring` provider (ADR-0020; the rustls and
//!   tokio-rustls instances unify with the workspace's, aws-lc-sys stays
//!   out). SASL is built in through rsasl:
//!   PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (and OAUTHBEARER, not exposed here)
//!   backstopped by the pure-Rust RustCrypto crates (sha2/hmac/pbkdf2).
//!   Compression is opt-in per codec: gzip (flate2/miniz_oxide) and snappy
//!   (snap) are pure Rust, so they are enabled; lz4 (C `lz4-sys`) and zstd
//!   (C `zstd-sys`) are left off. Produce uses `acks = -1` and record
//!   headers; the client negotiates the broker's API versions. Pinned exactly
//!   (`=0.6.0`, the newest release) in the workspace manifest. Broker
//!   compatibility is tested against `apache/kafka` 4.3.1 only.
//! - `rdkafka` — wraps the C library librdkafka; excluded by policy.
//! - `kafka` (a.k.a. kafka-rust) 0.10 — last release 2023-09, unmaintained,
//!   pre-KRaft API versions only.
//! - `rustfs-kafka` — a kafka-rust fork from one publisher, first released
//!   2026-04-20 (1.x declares Apache-2.0). Its `security` feature selects the
//!   aws-lc-rs provider; the ring variant is `security-ring`. Not chosen for
//!   maturity: months of releases from a single publisher, against rskafka's
//!   InfluxData maintenance.
//! - `kafka-protocol` — the wire protocol only, not a producer client.
//!
//! # Shared code note
//!
//! The JSON envelope code ([`envelope`]) is copied verbatim from
//! `crates/thadam-nats/src/envelope.rs` (minus the NATS subject template): a
//! dependency edge `thadam-kafka -> thadam-nats` would drag `async-nats` into
//! the tree and is forbidden by `scripts/check-deps.sh` (only the CLI may
//! depend on a connector crate). Keep the two copies in sync.

pub mod config;
pub mod envelope;
pub mod error;
pub mod factory;
pub mod sink;
pub mod template;

pub use config::{
    Compression, KafkaConfig, Sasl, SaslMechanism, Secret, TlsConfig, config_schema, parse_config,
};
pub use factory::KafkaSinkFactory;
pub use sink::KafkaSink;
