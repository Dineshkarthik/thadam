//! Envelope column names and their reserved Parquet field IDs.
//!
//! # Field-ID policy (open item O5, decided here)
//!
//! IDs are partitioned by origin so that every one is stable for the life of
//! the format and identical on every node that shares a configuration:
//!
//! | Range | Origin | Assigned by |
//! |---|---|---|
//! | `1..=99` | envelope and lineage columns | fixed by this crate (format v1) |
//! | `100..=199` | identity level columns | explicit, from configuration |
//! | `1000..=4_194_303` | declared top-level fields | explicit, from configuration |
//! | `8_388_608..` | list elements and record children | derived, see below |
//!
//! Declared IDs are explicit (`FieldDef::field_id`), so they do not depend on
//! declaration order, field removal, restarts, or upgrades. Whoever owns the
//! configuration (`thadam-config`) is responsible for never reusing an ID after
//! a field is removed; this crate rejects duplicates and out-of-range IDs but
//! cannot know history.
//!
//! A nested field's ID is derived from its top-level field's ID and its
//! position in that field's type tree (depth-first, pre-order):
//! `8_388_608 + (id - 1000) * 256 + slot`. Nested IDs are therefore unique in
//! the dataset and stable while the field's type is unchanged, including under
//! widening (`list<i32>` to `list<i64>` keeps its shape). Changing a nested
//! type's shape is a new dataset version (ADR-0013 evolution rules), so IDs
//! written on nested `FieldDef`s are ignored.

/// An envelope column: its name and fixed field ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Column {
    /// The column name. Names starting with `_` are reserved for Thadam.
    pub name: &'static str,
    /// The `PARQUET:field_id`.
    pub field_id: u32,
}

const fn col(name: &'static str, field_id: u32) -> Column {
    Column { name, field_id }
}

/// `FixedSizeBinary(16)`, lineage.
pub const EVENT_ID: Column = col("_event_id", 1);
/// Edge receipt time. Never null.
pub const INGEST_TIME: Column = col("_ingest_time", 2);
/// The device-reported time, nullable.
pub const SOURCE_TIME: Column = col("_source_time", 3);
/// The corrected time, else the source time when the domain is UTC, else null.
pub const EVENT_TIME: Column = col("_event_time", 4);
/// The domain source times are read from, a dictionary string.
pub const TIME_DOMAIN: Column = col("_time_domain", 5);
/// `good`, `uncertain`, or `bad`, a dictionary string.
pub const QUALITY_STATUS: Column = col("_quality_status", 6);
/// The quality flag bitset, an unsigned 32-bit integer.
pub const QUALITY_FLAGS: Column = col("_quality_flags", 7);
/// The optional quality reason.
pub const QUALITY_REASON: Column = col("_quality_reason", 8);
/// Whether the clock was synchronized when the data was received.
pub const CLOCK_SYNCED: Column = col("_clock_synced", 9);
/// The WAL epoch, lineage.
pub const WAL_EPOCH: Column = col("_wal_epoch", 10);
/// The WAL frame position, lineage.
pub const WAL_POS: Column = col("_wal_pos", 11);
/// The sample index of exploded datasets.
pub const SAMPLE_IDX: Column = col("sample_idx", 12);
/// Reserved for `_wal_record_idx` (open item O4); not written yet.
pub const RESERVED_WAL_RECORD_IDX: u32 = 13;

/// Metadata key the Parquet writer reads field IDs from.
pub const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";
/// Field metadata key carrying a declared field's UCUM unit.
pub const UNIT_META_KEY: &str = "thadam.unit";
/// Schema metadata key carrying the dataset name.
pub const DATASET_META_KEY: &str = "thadam.dataset";
/// Schema metadata key carrying the schema fingerprint in hex.
pub const SCHEMA_FP_META_KEY: &str = "thadam.schema_fp";

/// Identity level IDs are explicit and must lie in this range.
pub const LEVEL_ID_RANGE: std::ops::RangeInclusive<u32> = 100..=199;
/// The first declared top-level field ID.
pub const FIRST_DECLARED_ID: u32 = 1000;
/// The last declared top-level field ID.
pub const LAST_DECLARED_ID: u32 = 4_194_303;
/// The first derived (nested) ID.
pub const NESTED_ID_BASE: u32 = 1 << 23;
/// Nested fields one top-level field may have.
pub const MAX_NESTED_FIELDS: u32 = 256;
