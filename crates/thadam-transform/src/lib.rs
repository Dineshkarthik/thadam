//! Synchronous, pure processing from raw WAL frames to event batches.
//!
//! # Responsibility
//!
//! - The dataset router: match on source and an MQTT-style topic filter over
//!   attributes, and extract identity levels from templates such as `{topic.2}`.
//! - JSON decoding to declared shapes through JSON Pointer `from:` paths, with
//!   `^/` parent paths for exploded samples.
//! - Event ids: derived from the WAL position by default, from content with
//!   `event_id.key`.
//! - Time extraction with units, domains, and `max_skew` flagging.
//! - Units with UCUM codes, including element-wise conversion on typed arrays.
//! - The steps: message-level dedup with warm start, `explode`, `validate`,
//!   `on_change` with heartbeat, `sample`, structured `filter`, `enrich`,
//!   `rename`, `project`, and widening-only `cast`.
//! - Per-event errors become [`RejectedRaw`], with the raw bytes kept.
//!
//! # Shape of the API
//!
//! [`ChainSpec`] deserializes from a `serde_json::Value` (the runtime builds
//! it from the parsed pipeline config) and [`Chain::compile`] turns it into a
//! [`Chain`]. The runtime adapts WAL frames to [`RawRecordView`]s, wraps them
//! in a [`FrameInput`], and calls [`Chain::process`] into a [`ChainOutput`]
//! made by [`Chain::new_output`]. [`Chain::warm`] rebuilds step state from
//! earlier records without emitting.
//!
//! # Processing order within a dataset
//!
//! Decode, event id and message-level dedup, `explode`, field extraction and
//! unit conversion, time extraction and skew flagging, then the `transforms:`
//! list in order.
//!
//! # ADR-0018 PR9 details, as implemented
//!
//! 1. **Keys.** `kind: filter` with `where:`; operators `eq`, `ne`, `lt`, `le`,
//!    `gt`, `ge`, `in`, `min`, `max`, `null`; combinators `all`, `any`, `not`.
//!    `expr:` is reserved and refused with a message.
//! 2. **Namespace.** Dataset fields, identity levels, `quality.status`, and
//!    `quality.flags`. A Parquet column such as `_quality_status` is the
//!    sink's spelling of `quality.status`; filters never see prefixed names.
//! 3. **Nested access.** There is none: datasets declare flat fields, and `^/`
//!    has no meaning in a filter, which runs after the parent message is gone.
//! 4. **`validate`** keeps its own `{field, min, max, flag, status}` form and
//!    shares the range semantics (and the literal typing) of the predicate
//!    grammar.
//! 5. **Literal typing** is checked against the declared field type at
//!    compile time: integer fields take integer literals that fit (`u64` stays
//!    exact above `i64::MAX`), float fields compare in their own domain
//!    (`f32` rule), and `in` accepts numeric sets and a separate `null` test.
//! 6. **Null and `NaN`.** A null field (absent or `null`) makes `eq`, the
//!    ordered comparisons, `in`, `min`, and `max` false and `ne` true; `null:
//!    true` is the explicit test. `NaN` is unordered. `min`/`max` on a list
//!    hold for every element. In `validate`, a null field is not checked.
//!    These rules are unit tests in the predicate module.
//! 7. **Cost** is bounded by [`ChainLimits`]: predicate nodes, depth, and `in`
//!    set size, checked while compiling.
//! 8. **Errors.** A compile error carries the config path of the offending key
//!    (`transforms[2].where.all[1].field`) in its `path` context, for the
//!    config layer to map to a line and column. An evaluation that cannot
//!    complete becomes a `transform.eval_failed` reject with the raw bytes.
//!
//! # Determinism
//!
//! Nothing here reads the wall clock or a random source. Steps use arrival
//! order and the ingest time stored in the WAL, so the same records give the
//! same rows: identical event ids, values, and flags.
//!
//! # Deliberately not exposed
//!
//! - A user-defined step trait for external crates (sealed until a plugin ADR).
//! - Windowing and aggregation.
//! - Arrow.
//! - Async.
//! - Any I/O beyond loading enrichment files at compile time and reading
//!   the content-addressed schema store (`decode: ros2`), which is read-only
//!   and deterministic, and the Sparkplug birth store (`decode: sparkplug_b`),
//!   read at compile time and written (fsynced) as BIRTH/DEATH messages are
//!   processed.

mod chain;
mod dataset;
mod decode;
mod dedup;
mod eventid;
mod input;
mod limits;
pub mod paths;
mod predicate;
mod ros2;
mod sparkplug;
mod spec;
mod steps;
pub mod timeparse;
mod timex;
pub mod units;

#[cfg(test)]
mod testutil;

pub use chain::{Chain, ChainOutput, ChainStats, DatasetBatch, DatasetInfo};
pub use dataset::DatasetStats;
pub use input::{FrameInput, RawRecordView, RejectedRaw};
pub use limits::ChainLimits;
pub use spec::{
    ChainSpec, Count, DatasetSelector, DatasetSpec, DecodeSpec, DedupSpec, EnrichFieldSpec,
    EnrichKeySpec, EnrichSpec, EventIdSpec, ExplodeSpec, FieldSpec, FilterSpec, HumanDuration,
    IdentitySpec, Lit, MatchSpec, OnChangeSpec, OnMiss, PredicateSpec, ProjectSpec, RenameSpec,
    RuleSpec, SampleSpec, TimeSpec, TransformSpec, ValidateSpec, WatchSpec, parse_field_type,
};
pub use steps::EnrichmentInfo;
