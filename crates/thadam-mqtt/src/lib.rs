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
