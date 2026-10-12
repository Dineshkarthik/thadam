//! OPC UA subscription source (`kind: opcua`), client only, over
//! [`async-opcua`](https://crates.io/crates/async-opcua) 0.19 (pure Rust,
//! MPL-2.0 — an owner decision; never `open62541`).
//!
//! The source connects to one OPC UA server and either creates one
//! subscription over the configured nodes (`mode: subscribe`, the default)
//! and emits ONE raw message per publish response, or reads every node on a
//! timer (`mode: poll`) and emits ONE raw message per poll. Each message's
//! payload groups the values:
//! `{ "<name>": value, ..., "ts_us": <microseconds> }` where `ts_us` is the
//! source timestamp of the newest value, failing that the newest server
//! timestamp, failing that the time the response was received.
//!
//! Delivery is at-most-once per publish response (`ack_mode: none`). OPC UA
//! subscriptions have no acknowledgement at the application layer, so a
//! response that was not committed before a crash is gone; the server keeps
//! publishing and server-side per-node queues (`queue_size`) cover short
//! stalls. Dropped responses on the client side are counted and ride on
//! later messages as `opcua.dropped`.
//!
//! A data change with a non-Good status code is never silently lost: its
//! value becomes `null` and the change is named as
//! `name=StatusCode` in the message's `opcua.bad_status` attribute. Values
//! outside the documented mapping (see `docs/src/sources/opcua.md`) become
//! `null` and are named in `opcua.unsupported`.
//!
//! # Security
//!
//! The `basic256sha256`, `aes128_sha256_rsaoaep`, and `aes256_sha256_rsapss`
//! security policies pull `rsa` 0.9 (via `async-opcua-crypto`), which is
//! affected by RUSTSEC-2023-0071 (the Marvin RSA timing attack): a remote
//! observer could, in principle, recover the key used in the handshake by
//! collecting many decryption-time samples. `cargo deny` carries a
//! time-boxed exception (review by 2027-04-01; move to `rsa` 0.10 when
//! stable). The practical mitigations, already in this crate:
//!
//! - prefer the `Aes256_Sha256_RsaPss` and `Aes128_Sha256_RsaOaep` policies,
//!   whose RSA operations are fewer and which are the OPC Foundation's
//!   currently-sanctioned suites;
//! - connection handshakes are rate-limited
//!   (`reconnect.max_handshakes_per_minute`), which bounds the timing
//!   samples an attacker can collect, and authentication failures are fatal
//!   (no retry storms against a misconfigured secret);
//! - RSA is only used on the channel handshake, never per message.
//!
//! Client certificates and trust: the server certificate is rejected unless
//! it is pinned in `cert.trust_server_certs` or the insecure
//! `cert.accept_unknown_server_cert` is set. Certificate user authentication
//! (`auth: {kind: certificate}`) is signed on the secure channel and needs a
//! secured policy.

mod config;
mod error;
mod factory;
mod pki;
mod session;
mod source;
mod value;

pub use config::{
    AuthConfig, CertConfig, DurationSpec, ModeSpec, NodeSpec, OpcUaConfig, ReconnectConfig,
    SecurityModeSpec, SecurityPolicySpec, SubscribeConfig, config_schema,
};
pub use error::OpcUaError;
pub use factory::OpcUaSourceFactory;
pub use source::OpcUaSource;
