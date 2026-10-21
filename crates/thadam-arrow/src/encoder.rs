//! The reusable batch encoder.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, StringBuilder, StringDictionaryBuilder};
use arrow_array::types::{Int8Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{
    ArrayRef, DictionaryArray, FixedSizeBinaryArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_buffer::Buffer;
use thadam_core::{
    AssetPath, ErrorDomain, Event, EventBatch, Payload, QualityStatus, Record, Reject, ShapeKey,
    ThadamError, TimeDomain, Timestamp, Value, WalEpoch, codes,
};

use crate::column::{Col, Fail, Prim, UTC};
use crate::options::TsUnit;
use crate::schema::{DatasetSchema, Storage, unsigned_storage};

/// Shapes whose column mapping is cached. Datasets normally see one.
const MAX_CACHED_SHAPES: usize = 8;
/// Largest total variable-length payload one batch may hold. Arrow's 32-bit
/// offsets overflow (and the builders panic) past `i32::MAX`.
const MAX_BATCH_VAR_BYTES: u64 = i32::MAX as u64;

static NULL: Value = Value::Null;

/// Per-batch facts that are not on the [`Event`] itself: the WAL epoch and the
/// clock-sync flag of the batch the events came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchContext {
    /// Written to `_wal_epoch`.
    pub epoch: WalEpoch,
    /// Written to `_clock_synced`.
    pub clock_synced: bool,
}

impl Default for BatchContext {
    /// Epoch 0 and an unsynchronized clock. Callers set the real values with
    /// [`BatchEncoder::set_context`] or [`BatchEncoder::push_batch`].
    fn default() -> Self {
        BatchContext {
            epoch: WalEpoch::new(0),
            clock_synced: false,
        }
    }
}

struct TsCol {
    unit: TsUnit,
    p: Prim<Int64Type>,
}

impl TsCol {
    fn new(unit: TsUnit, cap: usize) -> Self {
        TsCol {
            unit,
            p: Prim::with_capacity(cap),
        }
    }

    #[inline]
    fn convert(&self, t: Timestamp) -> i64 {
        match self.unit {
            TsUnit::Micros => t.as_micros(),
            TsUnit::Nanos => t.as_nanos(),
        }
    }

    #[inline]
    fn push(&mut self, t: Timestamp) {
        let v = self.convert(t);
        self.p.push(v);
    }

    #[inline]
    fn push_opt(&mut self, t: Option<Timestamp>) {
        match t {
            Some(t) => self.push(t),
            None => self.p.push_null(),
        }
    }

    fn finish(&mut self, next_cap: usize) -> ArrayRef {
        let (values, nulls) = self.p.finish_parts(next_cap);
        match self.unit {
            TsUnit::Micros => {
                Arc::new(TimestampMicrosecondArray::new(values, nulls).with_timezone(UTC))
            }
            TsUnit::Nanos => {
                Arc::new(TimestampNanosecondArray::new(values, nulls).with_timezone(UTC))
            }
        }
    }
}

/// An envelope unsigned column, stored per the unsigned policy.
enum UintCol {
    U16(Prim<UInt16Type>),
    U32(Prim<UInt32Type>),
    U64(Prim<UInt64Type>),
    I32(Prim<Int32Type>),
    I64(Prim<Int64Type>),
}

struct Uint {
    /// The largest value of the logical unsigned type.
    logical_max: u64,
    col: UintCol,
}

impl Uint {
    fn new(schema: &DatasetSchema, bits: u8, cap: usize) -> Uint {
        let logical_max = match bits {
            16 => u64::from(u16::MAX),
            32 => u64::from(u32::MAX),
            _ => u64::MAX,
        };
        let col = match unsigned_storage(schema.options().unsigned, bits) {
            Storage::U16 => UintCol::U16(Prim::with_capacity(cap)),
            Storage::U32 => UintCol::U32(Prim::with_capacity(cap)),
            Storage::U64 => UintCol::U64(Prim::with_capacity(cap)),
            Storage::I32 => UintCol::I32(Prim::with_capacity(cap)),
            _ => UintCol::I64(Prim::with_capacity(cap)),
        };
        Uint { logical_max, col }
    }

    fn storage_max(&self) -> u64 {
        match &self.col {
            UintCol::U16(_) => u64::from(u16::MAX),
            UintCol::U32(_) => u64::from(u32::MAX),
            UintCol::U64(_) => u64::MAX,
            UintCol::I32(_) => i32::MAX as u64,
            UintCol::I64(_) => i64::MAX as u64,
        }
    }

    fn fits(&self, x: u64) -> bool {
        x <= self.logical_max && x <= self.storage_max()
    }

    fn fixed_bytes(&self) -> usize {
        match &self.col {
            UintCol::U16(_) => 2,
            UintCol::U32(_) | UintCol::I32(_) => 4,
            UintCol::U64(_) | UintCol::I64(_) => 8,
        }
    }

    /// Appends `x`, which `fits`.
    fn push(&mut self, x: u64) {
        match &mut self.col {
            UintCol::U16(p) => p.push(x as u16),
            UintCol::U32(p) => p.push(x as u32),
            UintCol::U64(p) => p.push(x),
            UintCol::I32(p) => p.push(x as i32),
            UintCol::I64(p) => p.push(x as i64),
        }
    }

    fn push_opt(&mut self, x: Option<u64>) {
        match x {
            Some(x) => self.push(x),
            None => match &mut self.col {
                UintCol::U16(p) => p.push_null(),
                UintCol::U32(p) => p.push_null(),
                UintCol::U64(p) => p.push_null(),
                UintCol::I32(p) => p.push_null(),
                UintCol::I64(p) => p.push_null(),
            },
        }
    }

    fn finish(&mut self, next_cap: usize) -> ArrayRef {
        match &mut self.col {
            UintCol::U16(p) => Arc::new(p.finish(next_cap)),
            UintCol::U32(p) => Arc::new(p.finish(next_cap)),
            UintCol::U64(p) => Arc::new(p.finish(next_cap)),
            UintCol::I32(p) => Arc::new(p.finish(next_cap)),
            UintCol::I64(p) => Arc::new(p.finish(next_cap)),
        }
    }
}

/// A dictionary column over a small fixed vocabulary, written as keys only.
struct FixedDict {
    keys: Prim<Int8Type>,
    values: ArrayRef,
}

impl FixedDict {
    fn new<'a>(vocabulary: impl IntoIterator<Item = &'a str>, cap: usize) -> Self {
        let values: StringArray = vocabulary.into_iter().map(Some).collect();
        FixedDict {
            keys: Prim::with_capacity(cap),
            values: Arc::new(values),
        }
    }

    fn finish(&mut self, next_cap: usize) -> ArrayRef {
        Arc::new(DictionaryArray::<Int8Type>::new(
            self.keys.finish(next_cap),
            Arc::clone(&self.values),
        ))
    }
}

/// One cached mapping from declared columns to a record shape's field
/// positions.
struct ShapePlan {
    key: ShapeKey,
    /// For each declared column, the record field holding it, if any.
    positions: Box<[Option<u32>]>,
}

fn reject_error(code: &'static str, field: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Schema, code, message).with_context("field", field)
}

fn fail_error(fail: Fail, column: &str) -> ThadamError {
    let mut path = column.to_owned();
    for part in fail.path.iter().rev() {
        path.push('.');
        path.push_str(part);
    }
    reject_error(fail.code, &path, fail.message)
}

fn status_key(status: QualityStatus) -> Option<i8> {
    match status {
        QualityStatus::Good => Some(0),
        QualityStatus::Uncertain => Some(1),
        QualityStatus::Bad => Some(2),
        _ => None,
    }
}

const STATUS_VOCABULARY: [&str; 3] = ["good", "uncertain", "bad"];

/// The value of identity level `idx` named `name`. Paths of one device share a
/// level order, so the positional guess nearly always hits.
fn level_value<'a>(asset: &'a AssetPath, idx: usize, name: &str) -> Option<&'a str> {
    match asset.levels().get(idx) {
        Some((k, v)) if **k == *name => Some(v.as_str()),
        _ => asset.get(name),
    }
}

/// Encodes events of one dataset into Arrow record batches.
///
/// An encoder belongs to one open segment or one ClickHouse batch and is
/// single-threaded. [`BatchEncoder::push`] adds a row, [`BatchEncoder::finish`]
/// takes the batch and leaves the encoder empty and ready for the next one.
///
/// # Rejects are atomic
///
/// An event is checked completely before anything is written, so a rejected
/// event adds no partial row and the encoder stays usable.
///
/// # Capacity
///
/// Arrow arrays take ownership of their buffers, so `finish` cannot hand back
/// the allocation it just produced. It instead re-reserves the buffers for
/// `capacity_hint` rows (and for variable-length data, the previous batch's
/// average per row), so the next batch does not regrow from empty.
pub struct BatchEncoder {
    schema: Arc<DatasetSchema>,
    ctx: BatchContext,
    hint: usize,
    rows: usize,
    offered: u32,
    var_bytes: u64,
    guard_bytes: u64,
    fixed_row_bytes: usize,
    plans: Vec<ShapePlan>,
    last_plan: usize,

    event_id: Option<Vec<u8>>,
    ingest: TsCol,
    source: TsCol,
    event_time: TsCol,
    time_domain: FixedDict,
    quality_status: FixedDict,
    flags: Uint,
    reason: StringBuilder,
    clock_synced: BooleanBuilder,
    wal_epoch: Option<Uint>,
    wal_pos: Option<Uint>,
    levels: Vec<StringDictionaryBuilder<Int32Type>>,
    sample_idx: Option<Uint>,
    declared: Vec<Col>,
}

impl BatchEncoder {
    /// An encoder for `schema`, reserving room for `capacity_hint` rows.
    pub fn new(schema: Arc<DatasetSchema>, capacity_hint: usize) -> Self {
        let cap = capacity_hint;
        let opts = *schema.options();
        let lineage = opts.lineage_columns;
        let flags = Uint::new(&schema, 32, cap);
        let wal_epoch = lineage.then(|| Uint::new(&schema, 64, cap));
        let wal_pos = lineage.then(|| Uint::new(&schema, 64, cap));
        let sample_idx = schema.is_exploded().then(|| Uint::new(&schema, 16, cap));
        let declared: Vec<Col> = schema
            .declared()
            .iter()
            .map(|d| Col::new(d.child.field.is_nullable(), &d.child.node, cap))
            .collect();

        let fixed_row_bytes = 8 * 3 // three timestamps
            + 1 + 1 // domain and status keys
            + flags.fixed_bytes()
            + 4 // reason offset
            + 1 // clock_synced
            + if lineage { 16 } else { 0 }
            + wal_epoch.as_ref().map_or(0, Uint::fixed_bytes)
            + wal_pos.as_ref().map_or(0, Uint::fixed_bytes)
            + 4 * schema.levels().len()
            + sample_idx.as_ref().map_or(0, Uint::fixed_bytes)
            + declared.iter().map(Col::fixed_bytes).sum::<usize>();

        BatchEncoder {
            ctx: BatchContext::default(),
            hint: cap,
            rows: 0,
            offered: 0,
            var_bytes: 0,
            guard_bytes: 0,
            fixed_row_bytes,
            plans: Vec::new(),
            last_plan: 0,
            event_id: lineage.then(|| Vec::with_capacity(cap * 16)),
            ingest: TsCol::new(opts.ts_unit, cap),
            source: TsCol::new(opts.ts_unit, cap),
            event_time: TsCol::new(opts.ts_unit, cap),
            time_domain: FixedDict::new(TimeDomain::ALL.iter().map(|d| d.as_str()), cap),
            quality_status: FixedDict::new(STATUS_VOCABULARY, cap),
            flags,
            reason: StringBuilder::with_capacity(cap, 0),
            clock_synced: BooleanBuilder::with_capacity(cap),
            wal_epoch,
            wal_pos,
            levels: schema
                .levels()
                .iter()
                .map(|_| StringDictionaryBuilder::<Int32Type>::with_capacity(cap, 64, 1024))
                .collect(),
            sample_idx,
            declared,
            schema,
        }
    }

    /// The schema batches conform to.
    pub fn schema(&self) -> &Arc<DatasetSchema> {
        &self.schema
    }

    /// The context applied to rows pushed from now on.
    pub fn context(&self) -> BatchContext {
        self.ctx
    }

    /// Sets the WAL epoch and clock-sync flag written to rows pushed from now
    /// on. Until it is called, rows carry [`BatchContext::default`].
    pub fn set_context(&mut self, ctx: BatchContext) {
        self.ctx = ctx;
    }

    /// Rows accepted since the last [`BatchEncoder::finish`].
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether no row has been accepted since the last `finish`.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// An estimate of the bytes the pending batch occupies, cheap enough to
    /// call after every push when sizing row groups by bytes. It counts
    /// fixed-width values and variable-length payload; it ignores validity
    /// bitmaps, dictionary contents, and alignment padding, so it runs low
    /// (about 10% at 8192 rows of the reference telemetry dataset).
    pub fn estimated_bytes(&self) -> usize {
        self.rows * self.fixed_row_bytes + self.var_bytes as usize
    }

    /// Encodes the events of `batch`, setting the context from the batch
    /// first. Returns the rejected events with their index in `batch`; the
    /// rest are encoded.
    pub fn push_batch(&mut self, batch: &EventBatch) -> Vec<Reject> {
        self.ctx = BatchContext {
            epoch: batch.origin.epoch(),
            clock_synced: batch.clock_synced,
        };
        let mut rejects = Vec::new();
        for (i, event) in batch.events.iter().enumerate() {
            if let Err(error) = self.push_event(event) {
                rejects.push(Reject::new(u32::try_from(i).unwrap_or(u32::MAX), error));
            }
        }
        rejects
    }

    /// Encodes one event as a row.
    ///
    /// On failure nothing is written. The [`Reject`]'s `index` is the event's
    /// position among those offered since the last [`BatchEncoder::finish`],
    /// counting rejected ones. The error is a `schema` error:
    /// `schema.type_mismatch`, `schema.missing_field`, or `schema.narrowing`
    /// with the offending column in its `field` context, or `sink.rejected`
    /// when the batch is too large for Arrow's 32-bit offsets (a caller that
    /// calls `finish` by `estimated_bytes` never sees it).
    pub fn push(&mut self, event: &Event) -> Result<(), Reject> {
        let index = self.offered;
        self.offered = self.offered.saturating_add(1);
        self.push_event(event).map_err(|e| Reject::new(index, e))
    }

    fn plan_index(&mut self, rec: &Record) -> usize {
        let key = rec.shape_key();
        if let Some(plan) = self.plans.get(self.last_plan)
            && plan.key == key
        {
            return self.last_plan;
        }
        if let Some(i) = self.plans.iter().position(|p| p.key == key) {
            self.last_plan = i;
            return i;
        }
        let shape = rec.shape();
        let positions = self
            .schema
            .declared()
            .iter()
            .map(|d| shape.index_of(&d.name).map(|i| i.index() as u32))
            .collect();
        let plan = ShapePlan { key, positions };
        let i = if self.plans.len() < MAX_CACHED_SHAPES {
            self.plans.push(plan);
            self.plans.len() - 1
        } else {
            let i = self.last_plan.wrapping_add(1) % MAX_CACHED_SHAPES;
            self.plans[i] = plan;
            i
        };
        self.last_plan = i;
        i
    }

    fn push_event(&mut self, ev: &Event) -> Result<(), ThadamError> {
        let Payload::Record(rec) = ev.payload() else {
            return Err(reject_error(
                codes::SCHEMA_TYPE_MISMATCH,
                "payload",
                "the payload is not a decoded record",
            ));
        };

        let epoch = self.ctx.epoch.get();
        let pos = ev.wal_pos().get();
        let sample = ev.sample_idx().map(u64::from);
        let out_of_range = |name: &str| {
            reject_error(
                codes::SCHEMA_NARROWING,
                name,
                "value is out of range for the column type",
            )
        };
        if let Some(c) = &self.wal_epoch
            && !c.fits(epoch)
        {
            return Err(out_of_range(crate::columns::WAL_EPOCH.name));
        }
        if let Some(c) = &self.wal_pos
            && !c.fits(pos)
        {
            return Err(out_of_range(crate::columns::WAL_POS.name));
        }
        if let (Some(c), Some(x)) = (&self.sample_idx, sample)
            && !c.fits(x)
        {
            return Err(out_of_range(crate::columns::SAMPLE_IDX.name));
        }
        let quality = ev.quality();
        let Some(status) = status_key(quality.status) else {
            return Err(reject_error(
                codes::SCHEMA_UNSUPPORTED_TYPE,
                crate::columns::QUALITY_STATUS.name,
                "unknown quality status",
            ));
        };

        let plan_idx = self.plan_index(rec);
        let positions = &self.plans[plan_idx].positions;
        let values = rec.values();
        let mut bytes = 0u64;
        for (i, (col, decl)) in self.declared.iter().zip(self.schema.declared()).enumerate() {
            let v = match positions[i] {
                Some(p) => values.get(p as usize).unwrap_or(&NULL),
                None => &NULL,
            };
            col.check(v, &mut bytes)
                .map_err(|f| fail_error(f, &decl.name))?;
        }

        let reason = quality.reason.as_deref();
        let reason_len = reason.map_or(0, str::len) as u64;
        let level_len: u64 = self
            .schema
            .levels()
            .iter()
            .enumerate()
            .map(|(i, l)| level_value(ev.asset(), i, &l.name).map_or(0, str::len) as u64)
            .sum();
        let incoming = bytes + reason_len;
        if self.guard_bytes + incoming + level_len > MAX_BATCH_VAR_BYTES {
            return Err(ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_REJECTED,
                "the encoder batch is full; call finish() before pushing more",
            ));
        }

        // Past this point nothing can fail.
        let times = ev.times();
        self.ingest.push(times.ingest());
        self.source.push_opt(times.source());
        self.event_time.push_opt(times.event_time());
        self.time_domain.keys.push(times.domain() as u8 as i8);
        self.quality_status.keys.push(status);
        self.flags.push(u64::from(quality.flags.bits()));
        match reason {
            Some(r) => self.reason.append_value(r),
            None => self.reason.append_null(),
        }
        self.clock_synced.append_value(self.ctx.clock_synced);
        if let Some(ids) = &mut self.event_id {
            ids.extend_from_slice(&ev.id().to_bytes());
        }
        if let Some(c) = &mut self.wal_epoch {
            c.push(epoch);
        }
        if let Some(c) = &mut self.wal_pos {
            c.push(pos);
        }
        for (i, (builder, level)) in self.levels.iter_mut().zip(self.schema.levels()).enumerate() {
            match level_value(ev.asset(), i, &level.name) {
                Some(v) if builder.append(v).is_ok() => {}
                _ => builder.append_null(),
            }
        }
        if let Some(c) = &mut self.sample_idx {
            c.push_opt(sample);
        }
        for (i, col) in self.declared.iter_mut().enumerate() {
            let v = match positions[i] {
                Some(p) => values.get(p as usize).unwrap_or(&NULL),
                None => &NULL,
            };
            col.append(v);
        }

        self.rows += 1;
        self.var_bytes += incoming;
        self.guard_bytes += incoming + level_len;
        Ok(())
    }

    /// Takes the pending rows as a [`RecordBatch`] of
    /// [`DatasetSchema::arrow_schema`] and resets the encoder, keeping the
    /// context. An empty encoder yields a zero-row batch.
    ///
    /// # Panics
    ///
    /// Only if Arrow rejects the arrays this encoder built, which is a bug in
    /// this crate and never a consequence of input data.
    pub fn finish(&mut self) -> RecordBatch {
        let cap = self.hint;
        let mut arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.schema.arrow_schema().fields().len());

        if let Some(ids) = &mut self.event_id {
            let next = Vec::with_capacity(cap * 16);
            let data = std::mem::replace(ids, next);
            arrays.push(Arc::new(FixedSizeBinaryArray::new(
                16,
                Buffer::from_vec(data),
                None,
            )));
        }
        arrays.push(self.ingest.finish(cap));
        arrays.push(self.source.finish(cap));
        arrays.push(self.event_time.finish(cap));
        arrays.push(self.time_domain.finish(cap));
        arrays.push(self.quality_status.finish(cap));
        arrays.push(self.flags.finish(cap));
        let data_cap = self
            .reason
            .values_slice()
            .len()
            .checked_div(self.rows)
            .unwrap_or(0)
            .saturating_mul(cap);
        let reason = self.reason.finish();
        self.reason = StringBuilder::with_capacity(cap, data_cap);
        arrays.push(Arc::new(reason));
        let synced = self.clock_synced.finish();
        self.clock_synced = BooleanBuilder::with_capacity(cap);
        arrays.push(Arc::new(synced));
        if let Some(c) = &mut self.wal_epoch {
            arrays.push(c.finish(cap));
        }
        if let Some(c) = &mut self.wal_pos {
            arrays.push(c.finish(cap));
        }
        for builder in &mut self.levels {
            arrays.push(Arc::new(builder.finish()));
        }
        if let Some(c) = &mut self.sample_idx {
            arrays.push(c.finish(cap));
        }
        for col in &mut self.declared {
            arrays.push(col.finish(cap));
        }

        self.rows = 0;
        self.offered = 0;
        self.var_bytes = 0;
        self.guard_bytes = 0;

        RecordBatch::try_new(self.schema.arrow_schema(), arrays).unwrap_or_else(|e| {
            panic!("thadam-arrow built arrays that do not match its schema: {e}")
        })
    }
}

impl std::fmt::Debug for BatchEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchEncoder")
            .field("dataset", &self.schema.name())
            .field("rows", &self.rows)
            .field("estimated_bytes", &self.estimated_bytes())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use thadam_core::{
        EventId, FieldDef, FieldType, Record, Shape, ShapeRef, Times, Timestamp, WalPos,
    };

    use super::*;
    use crate::ArrowOptions;

    fn event(shape: &ShapeRef, value: &str) -> Event {
        let times = Times::new(Timestamp::EPOCH, TimeDomain::Utc);
        Event::builder(EventId::from_u128(1), WalPos::new(1), times)
            .payload(Record::new(shape.clone(), vec![Value::str(value)]).unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn a_batch_near_the_offset_limit_rejects_instead_of_overflowing() {
        let fields = vec![FieldDef::new("s", FieldType::Str).with_field_id(1000)];
        let shape = ShapeRef::new(Shape::new(fields.clone()).unwrap());
        let schema = DatasetSchema::builder("d")
            .fields(fields)
            .options(ArrowOptions::default())
            .build()
            .unwrap();
        let mut enc = BatchEncoder::new(Arc::new(schema), 4);

        enc.guard_bytes = MAX_BATCH_VAR_BYTES - 10;
        let reject = enc.push(&event(&shape, "0123456789ab")).unwrap_err();
        assert_eq!(reject.error.code, codes::SINK_REJECTED);
        assert_eq!(enc.len(), 0);

        enc.push(&event(&shape, "0123456789")).unwrap();
        assert_eq!(enc.len(), 1);

        enc.finish();
        enc.push(&event(&shape, "0123456789ab")).unwrap();
    }
}
