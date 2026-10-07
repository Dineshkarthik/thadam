//! Encoding options shared by the schema and the encoder.

/// The unit of every timestamp column (D14).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TsUnit {
    /// `Timestamp(Microsecond, "UTC")`. Enough at 1000 Hz and readable by every
    /// target engine.
    #[default]
    Micros,
    /// `Timestamp(Nanosecond, "UTC")`, opt-in per dataset. Some Spark and
    /// Hive-era readers handle it poorly.
    Nanos,
}

/// How unsigned integers are stored (ADR-0013, "Schemas").
///
/// The policy applies to every unsigned column, declared and envelope alike, so
/// `Signed` yields a schema with no unsigned type in it.
///
/// # Default: `Native`
///
/// - It matches the envelope types the ADR and the demo plan name
///   (`_quality_flags` is `UInt32`, `_wal_pos` is `UInt64`).
/// - It is lossless and has no reject path.
/// - It is the only mapping that suits sinks with native unsigned types
///   (ClickHouse, DuckDB, pyarrow).
///
/// Choose `Signed` for lakes read by engines or table formats without unsigned
/// types (Iceberg has none; Athena and some Trino versions are the open `[V]`
/// items). Reader behavior for unsigned Parquet logical types is unverified in
/// this repository.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum UnsignedPolicy {
    /// `UInt8`, `UInt16`, `UInt32`, `UInt64`.
    #[default]
    Native,
    /// The next wider signed type: `Int16`, `Int32`, `Int64`. `u64` stays
    /// `Int64`, so a value above `i64::MAX` is rejected with
    /// `schema.narrowing`.
    Signed,
}

/// Options that shape the Arrow schema of a dataset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArrowOptions {
    /// Unit of every timestamp column. Microseconds by default.
    pub ts_unit: TsUnit,
    /// Whether to write the lineage columns `_event_id`, `_wal_epoch`, and
    /// `_wal_pos`. On by default; a ClickHouse batch may not want them.
    pub lineage_columns: bool,
    /// How unsigned integers are stored.
    pub unsigned: UnsignedPolicy,
}

impl Default for ArrowOptions {
    fn default() -> Self {
        ArrowOptions {
            ts_unit: TsUnit::default(),
            lineage_columns: true,
            unsigned: UnsignedPolicy::default(),
        }
    }
}

impl ArrowOptions {
    /// Sets the timestamp unit.
    #[must_use]
    pub fn with_ts_unit(mut self, ts_unit: TsUnit) -> Self {
        self.ts_unit = ts_unit;
        self
    }

    /// Turns the lineage columns on or off.
    #[must_use]
    pub fn with_lineage_columns(mut self, lineage_columns: bool) -> Self {
        self.lineage_columns = lineage_columns;
        self
    }

    /// Sets the unsigned policy.
    #[must_use]
    pub fn with_unsigned(mut self, unsigned: UnsignedPolicy) -> Self {
        self.unsigned = unsigned;
        self
    }
}
