//! The Thadam demo fleet simulator (PR15).
//!
//! Publishes a seeded, deterministic multi-robot fleet over MQTT on the
//! `thadam-demo/v1/{site}/{robot}/{stream}` topic layout with QoS 1, and writes
//! an append-only NDJSON oracle recording every sample it sent: whether the
//! broker acknowledged the publish and which outcome the configured Thadam
//! pipeline should produce for it (`deliver`, `filtered_debug`, `malformed`,
//! `duplicate_of`, `state_repeat`). Demo level-3 verification (PR18) checks
//! Thadam's output against that oracle.
//!
//! # Determinism
//!
//! Every value drawn from randomness comes from per-(robot, purpose) ChaCha8
//! streams derived from one seed, so a seed fully determines the fleet's
//! behavior for a given tick sequence. Wall time feeds only `ts_us`
//! timestamps; generators always take the current time as an explicit
//! argument, which is what the unit tests pin down.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod behavior;
pub mod chaos;
pub mod config;
pub mod error;
pub mod generate;
pub mod oracle;
pub mod publisher;
pub mod rng;
pub mod runner;
pub mod stats;
pub mod types;

pub use error::SimError;

/// The default seed when neither the fleet file nor `--seed` sets one.
pub const DEFAULT_SEED: u64 = 42;

/// The default topic prefix (plan section 4).
pub const DEFAULT_TOPIC_PREFIX: &str = "thadam-demo/v1";

/// The tick period every robot task runs on. All default stream periods
/// (1 Hz .. 100 ms batches) are whole multiples of it.
pub const TICK: std::time::Duration = std::time::Duration::from_millis(100);
