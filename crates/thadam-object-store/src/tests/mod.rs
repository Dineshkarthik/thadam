//! Unit tests: config lawyering lives in `config.rs`, key layout in
//! `keys.rs`, backoff in `backoff.rs`; here: the shipper over an in-memory
//! store (behavior, conflict/collision, retry) and crash/restart recovery.

mod behavior;
mod crash;
mod fixture;
