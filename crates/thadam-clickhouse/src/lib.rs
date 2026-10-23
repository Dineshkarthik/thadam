//! The ClickHouse batch sink (ADR-0008): `kind: clickhouse`.
//!
//! Inserts one dataset's events into an existing table. Events are encoded
//! with `thadam-arrow` into an Arrow `RecordBatch`, then into a dynamically
//! built ClickHouse *Native* block, and sent with the official `clickhouse`
//! crate's `insert_native()`.
//!
//! # Why the Native path
//!
//! - `Client::insert::<T>()` needs compile-time `Row` types, which dynamic
//!   Thadam schemas cannot provide. The crate's public `native::builder` builds
//!   blocks column by column at runtime, through the public `Encode` trait: the
//!   only vendor-maintained dynamic insertion path in 0.15.3. Authentication,
//!   timeouts, and error parsing stay inside the vendor's HTTP transport.
//! - `clickhouse-ext-arrow` 0.1.0 pins `arrow ^58` while the workspace is on
//!   `arrow 60`; Arrow major versions make `RecordBatch` incompatible, and the
//!   extension is explicitly low-maturity. Re-evaluate when it tracks the
//!   workspace's Arrow major (ADR-0008, B8).
//! - Raw HTTP `INSERT ... FORMAT ArrowStream` would duplicate the vendor's
//!   transport for a path benchmark B8 has not shown to win; the ADR's parity
//!   rule prefers the vendor crate.
//!
//! Known cost: the vendor's `InsertNative` hard-codes block compression off
//! (a `FIXME` in 0.15.3), so `compression: lz4` shapes probe responses only and
//! insert bodies travel uncompressed until the crate supports it.
//!
//! # Contract
//!
//! - **No DDL, ever.** `open()` validates the dataset schema against
//!   `DESCRIBE TABLE`; a mismatch is a `Config` error listing every problem.
//!   The operator owns the table.
//! - **One dataset per sink** (one table). The batch contract hands sinks a
//!   merged batch without per-event dataset ids, so `build` refuses anything
//!   else with a targeted error until ADR-0002's open item lands. Add one sink
//!   entry per dataset.
//! - **Honest delivery claims.** `open()` probes the server version, the
//!   engine, and the deduplication window, and reports `IdempotentBatch` only
//!   for a `Replicated*` engine or a nonzero `non_replicated_deduplication_window`.
//!   The dedup token is `{epoch}-{sink}-{first}-{last}`, a function of the batch
//!   id, so every retry and every replay after a crash reuses it.
//! - **No silent conversions.** ClickHouse turns a value that fails an
//!   implicit conversion into NULL in a nullable column. Every conversion that
//!   could lose data (narrowing, `FixedString` length, `DateTime` range) is
//!   checked in the sink, where it becomes a data error instead.
//! - **Errors.** Parse, type, and constraint errors are Permanent (the runtime
//!   bisects and quarantines the poison events); memory, too-many-parts, and
//!   too-many-queries are Transient; authentication is Fatal; timeouts and
//!   dropped connections are Transient with an unknown outcome and the retry
//!   reuses the token. The exception-code tables live in `error.rs`.
//!
//! TLS is rustls with the `ring` provider only (ADR-0020);
//! `cargo tree -i aws-lc-sys` stays empty.

mod block;
pub mod config;
mod error;
mod factory;
mod norm;
mod probe;
mod sink;
#[cfg(test)]
mod testutil;
mod types;

pub use factory::ClickHouseSinkFactory;
pub use sink::ClickHouseSink;
