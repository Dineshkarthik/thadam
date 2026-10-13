//! The quarantine writer: a segment sink with the fixed quarantine schema
//! (ARCHITECTURE.md section 8, PR10's record layout), on its own disk
//! budget.
//!
//! The runtime builds quarantine records itself (`thadam-runtime`'s
//! `Quarantine`); this crate owns only the Arrow schema those records encode
//! into, and the sink that writes them. The schema here mirrors the record
//! shape: `_wal_epoch`, `_wal_pos`, `_record_index`, `_ingest_time`,
//! `source_id`, `topic`, `sink_id`, `dataset`, `error_domain`,
//! `error_code`, `reason`, `raw`, `raw_truncated`. Because `_`-prefixed
//! names are reserved by `thadam-arrow`'s dataset builder, the quarantine
//! schema and encoder are hand-built here rather than routed through
//! [`thadam_arrow::DatasetSchema`].
//!
//! Field IDs follow the field-ID policy of ADR-0013: envelope columns reuse
//! their fixed format-v1 IDs (`_ingest_time` 2, `_wal_epoch` 10, `_wal_pos`
//! 11, and the reserved `_wal_record_idx` slot 13 for `_record_index`, the
//! same concept); the quarantine-specific columns take explicit IDs from
//! 1000 on.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{
    TimestampMicrosecondType, TimestampNanosecondType, UInt32Type, UInt64Type,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use thadam_arrow::TsUnit;
use thadam_arrow::columns as envelope;
use thadam_core::{
    ErrorDomain, Event, Reject, ThadamError, SegmentSink, SegmentSinkFactory, Value, codes,
};
use sha2::{Digest, Sha256};

use crate::config::{self, QuarantineConfig};
use crate::error::SegmentError;
use crate::rows::RowsEncoder;

use crate::sink::{DatasetPlan, EncoderKind, EngineConfig};

const NAMESPACE: &str = "thadam-segments/quarantine-schema/v1";

/// The quarantine column layout: `(name, type, nullable, field id)`.
/// Snapshot-tested below.
pub(crate) fn quarantine_schema(unit: TsUnit) -> (SchemaRef, String) {
    let ts = match unit {
        TsUnit::Micros => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        TsUnit::Nanos => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    };
    let cols: [(&str, DataType, bool, u32); 13] = [
        (
            "_wal_epoch",
            DataType::UInt64,
            false,
            envelope::WAL_EPOCH.field_id,
        ),
        (
            "_wal_pos",
            DataType::UInt64,
            false,
            envelope::WAL_POS.field_id,
        ),
        (
            "_record_index",
            DataType::UInt32,
            true,
            envelope::RESERVED_WAL_RECORD_IDX,
        ),
        (
            "_ingest_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
            envelope::INGEST_TIME.field_id,
        ),
        ("source_id", DataType::Utf8, true, 1000),
        ("topic", DataType::Utf8, true, 1001),
        ("sink_id", DataType::Utf8, false, 1002),
        ("dataset", DataType::Utf8, true, 1003),
        ("error_domain", DataType::Utf8, false, 1004),
        ("error_code", DataType::Utf8, false, 1005),
        ("reason", DataType::Utf8, false, 1006),
        ("raw", DataType::Binary, true, 1007),
        ("raw_truncated", DataType::Boolean, false, 1008),
    ];
    let mut fp = Sha256::new();
    fp.update(NAMESPACE.as_bytes());
    fp.update([0]);
    let fields: Vec<Field> = cols
        .into_iter()
        .map(|(name, ty, nullable, id)| {
            let ty = if matches!(name, "_ingest_time") {
                ts.clone()
            } else {
                ty
            };
            fp.update(name.as_bytes());
            fp.update([u8::from(nullable)]);
            fp.update(format!("{ty:?}").as_bytes());
            fp.update(id.to_le_bytes());
            Field::new(name, ty, nullable).with_metadata(HashMap::from([(
                envelope::PARQUET_FIELD_ID_META_KEY.to_owned(),
                id.to_string(),
            )]))
        })
        .collect();
    let digest: [u8; 32] = fp.finalize().into();
    let fp_hex = crate::catalog::record::hex(&digest[..16]);
    let schema = Arc::new(Schema::new(fields).with_metadata(HashMap::from([
        (envelope::DATASET_META_KEY.to_owned(), QUAR_NAME.to_owned()),
        (envelope::SCHEMA_FP_META_KEY.to_owned(), fp_hex.clone()),
    ])));
    (schema, fp_hex)
}

const QUAR_NAME: &str = thadam_core::QUARANTINE_DATASET;

/// The construction facts of a quarantine encoder.
#[derive(Clone)]
pub(crate) struct QuarantineEncoderSpec {
    schema: SchemaRef,
    fp_hex: String,
    unit: TsUnit,
}

impl QuarantineEncoderSpec {
    /// A spec for quarantine files with timestamps in `unit`.
    pub(crate) fn new(unit: TsUnit) -> Self {
        let (schema, fp_hex) = quarantine_schema(unit);
        QuarantineEncoderSpec {
            schema,
            fp_hex,
            unit,
        }
    }

    /// The schema fingerprint hex written as `thadam.schema_fp`.
    pub(crate) fn fingerprint_hex(&self) -> &str {
        &self.fp_hex
    }

    pub(crate) fn make_encoder(&self) -> Box<dyn RowsEncoder> {
        Box::new(QuarantineEncoder::new(self.clone(), 1024))
    }
}

struct QuarantineEncoder {
    spec: QuarantineEncoderSpec,
    rows: usize,
    offered: u32,
    var_bytes: u64,
    wal_epoch: PrimitiveBuilder<UInt64Type>,
    wal_pos: PrimitiveBuilder<UInt64Type>,
    record_index: PrimitiveBuilder<UInt32Type>,
    ingest_us: PrimitiveBuilder<TimestampMicrosecondType>,
    ingest_ns: PrimitiveBuilder<TimestampNanosecondType>,
    source_id: StringBuilder,
    topic: StringBuilder,
    sink_id: StringBuilder,
    dataset: StringBuilder,
    error_domain: StringBuilder,
    error_code: StringBuilder,
    reason: StringBuilder,
    raw: BinaryBuilder,
    raw_truncated: BooleanBuilder,
}

fn mismatch(field: &'static str, message: &'static str) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Schema, codes::SCHEMA_TYPE_MISMATCH, message)
        .with_context("field", field)
}

impl QuarantineEncoder {
    fn new(spec: QuarantineEncoderSpec, cap: usize) -> Self {
        QuarantineEncoder {
            spec,
            rows: 0,
            offered: 0,
            var_bytes: 0,
            wal_epoch: PrimitiveBuilder::with_capacity(cap),
            wal_pos: PrimitiveBuilder::with_capacity(cap),
            record_index: PrimitiveBuilder::with_capacity(cap),
            ingest_us: PrimitiveBuilder::with_capacity(cap),
            ingest_ns: PrimitiveBuilder::with_capacity(cap),
            source_id: StringBuilder::with_capacity(cap, 0),
            topic: StringBuilder::with_capacity(cap, 0),
            sink_id: StringBuilder::with_capacity(cap, 0),
            dataset: StringBuilder::with_capacity(cap, 0),
            error_domain: StringBuilder::with_capacity(cap, 0),
            error_code: StringBuilder::with_capacity(cap, 0),
            reason: StringBuilder::with_capacity(cap, 0),
            raw: BinaryBuilder::with_capacity(cap, 0),
            raw_truncated: BooleanBuilder::with_capacity(cap),
        }
    }

    fn push_event(&mut self, event: &Event) -> Result<(), ThadamError> {
        let payload = event.payload();
        let Some(rec) = payload.as_record() else {
            return Err(mismatch(
                "payload",
                "quarantine rows must be decoded records",
            ));
        };
        let u64v = |name: &'static str| -> Result<u64, ThadamError> {
            match rec.get(name) {
                Some(Value::U64(x)) => Ok(*x),
                _ => Err(mismatch(name, "expected u64")),
            }
        };
        let str_req = |name: &'static str| -> Result<&str, ThadamError> {
            match rec.get(name) {
                Some(Value::Str(s)) => Ok(s.as_str()),
                _ => Err(mismatch(name, "expected utf8")),
            }
        };

        let epoch = u64v("_wal_epoch")?;
        let pos = u64v("_wal_pos")?;
        let idx: Option<u32> = match rec.get("_record_index") {
            Some(Value::U32(x)) => Some(*x),
            Some(Value::Null) | None => None,
            _ => return Err(mismatch("_record_index", "expected u32 or null")),
        };
        let ingest = match rec.get("_ingest_time") {
            Some(Value::Ts(t)) => *t,
            _ => return Err(mismatch("_ingest_time", "expected a timestamp")),
        };
        let str_opt = |name: &'static str| -> Result<Option<&str>, ThadamError> {
            match rec.get(name) {
                Some(Value::Str(s)) => Ok(Some(s.as_str())),
                Some(Value::Null) | None => Ok(None),
                _ => Err(mismatch(name, "expected utf8 or null")),
            }
        };
        let source_id = str_opt("source_id")?;
        let topic = str_opt("topic")?;
        let sink_id = str_req("sink_id")?;
        let dataset = str_opt("dataset")?;
        let error_domain = str_req("error_domain")?;
        let error_code = str_req("error_code")?;
        let reason = str_req("reason")?;
        let raw: Option<&[u8]> = match rec.get("raw") {
            Some(Value::Bytes(b)) => Some(b.as_ref()),
            Some(Value::Null) | None => None,
            _ => return Err(mismatch("raw", "expected bytes or null")),
        };
        let truncated = match rec.get("raw_truncated") {
            Some(Value::Bool(b)) => *b,
            _ => return Err(mismatch("raw_truncated", "expected bool")),
        };

        // All fallible reads are done; appends cannot fail from here.
        self.wal_epoch.append_value(epoch);
        self.wal_pos.append_value(pos);
        match idx {
            Some(x) => self.record_index.append_value(x),
            None => self.record_index.append_null(),
        }
        match self.spec.unit {
            TsUnit::Micros => self.ingest_us.append_value(ingest.as_micros()),
            TsUnit::Nanos => self.ingest_ns.append_value(ingest.as_nanos()),
        }
        push_str(&mut self.source_id, source_id);
        push_str(&mut self.topic, topic);
        push_str(&mut self.sink_id, Some(sink_id));
        push_str(&mut self.dataset, dataset);
        push_str(&mut self.error_domain, Some(error_domain));
        push_str(&mut self.error_code, Some(error_code));
        push_str(&mut self.reason, Some(reason));
        match raw {
            Some(b) => {
                self.var_bytes += b.len() as u64;
                self.raw.append_value(b);
            }
            None => self.raw.append_null(),
        }
        self.raw_truncated.append_value(truncated);
        self.var_bytes += str_len(source_id)
            + str_len(topic)
            + sink_id.len() as u64
            + str_len(dataset)
            + error_domain.len() as u64
            + error_code.len() as u64
            + reason.len() as u64;
        self.rows += 1;
        Ok(())
    }

    fn finish(&mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.wal_epoch.finish()),
            Arc::new(self.wal_pos.finish()),
            Arc::new(self.record_index.finish()),
            match self.spec.unit {
                TsUnit::Micros => Arc::new(self.ingest_us.finish().with_timezone("UTC")),
                TsUnit::Nanos => Arc::new(self.ingest_ns.finish().with_timezone("UTC")),
            },
            Arc::new(self.source_id.finish()),
            Arc::new(self.topic.finish()),
            Arc::new(self.sink_id.finish()),
            Arc::new(self.dataset.finish()),
            Arc::new(self.error_domain.finish()),
            Arc::new(self.error_code.finish()),
            Arc::new(self.reason.finish()),
            Arc::new(self.raw.finish()),
            Arc::new(self.raw_truncated.finish()),
        ];
        self.rows = 0;
        self.offered = 0;
        self.var_bytes = 0;
        match RecordBatch::try_new(Arc::clone(&self.spec.schema), arrays) {
            Ok(batch) => batch,
            // The arrays above are built column-for-column against the
            // static schema; a mismatch is a bug in this crate, not input.
            Err(e) => {
                tracing::error!(error = %e, "quarantine encoder produced an inconsistent batch; dropping rows");
                RecordBatch::new_empty(Arc::clone(&self.spec.schema))
            }
        }
    }
}

fn str_len(s: Option<&str>) -> u64 {
    s.map_or(0, |s| s.len() as u64)
}

fn push_str(b: &mut StringBuilder, v: Option<&str>) {
    match v {
        Some(s) => b.append_value(s),
        None => b.append_null(),
    }
}

impl RowsEncoder for QuarantineEncoder {
    fn set_context(&mut self, _ctx: thadam_arrow::BatchContext) {
        // Quarantine rows carry their own WAL epoch and ingest time; the
        // clock-sync flag decides the partition, which the sink tracks.
    }

    fn push(&mut self, event: &Event) -> Result<(), Reject> {
        let index = self.offered;
        self.offered = self.offered.saturating_add(1);
        self.push_event(event).map_err(|e| Reject::new(index, e))
    }

    fn arrow_schema(&self) -> SchemaRef {
        Arc::clone(&self.spec.schema)
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, SegmentError> {
        Ok(self.finish())
    }
}

/// The factory for the quarantine sink, registered with
/// `Registry::quarantine_sink` by the CLI. Parses the pipeline's
/// `quarantine:` block (`{"format", "max_disk", ...}`).
#[derive(Clone, Copy, Debug, Default)]
pub struct QuarantineSinkFactory;

impl QuarantineSinkFactory {
    /// The factory instance.
    pub fn new() -> Self {
        QuarantineSinkFactory
    }
}

impl SegmentSinkFactory for QuarantineSinkFactory {
    fn kind(&self) -> &'static str {
        "quarantine"
    }

    fn config_schema(&self) -> serde_json::Value {
        config::quarantine_config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        config::parse_quarantine_config(cfg).map(|_| ())
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &thadam_core::BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        let parsed: QuarantineConfig =
            config::parse_quarantine_config(&cfg).map_err(config::first_error)?;
        let sink = build_quarantine_sink(
            parsed,
            ctx.data_dir.clone(),
            &ctx.component_id,
            &ctx.config_hash,
            &ctx.enrichment_hash,
        )?;
        Ok(Box::new(sink))
    }
}

/// Constructs the quarantine sink: one `_quarantine` dataset with the fixed
/// schema, budgeted under the quarantine category. Its rejects come from the
/// owning sink's chain, so it records that chain's hashes.
pub(crate) fn build_quarantine_sink(
    cfg: QuarantineConfig,
    dir: PathBuf,
    node_id: &str,
    config_hash: &str,
    enrichment_hash: &str,
) -> Result<crate::sink::ParquetSegmentSink, SegmentError> {
    let spec = QuarantineEncoderSpec::new(cfg.timestamp_unit);
    let kvs = vec![
        crate::writer::kv(crate::writer::THADAM_VERSION_KEY, env!("CARGO_PKG_VERSION")),
        crate::writer::kv(envelope::DATASET_META_KEY, QUAR_NAME),
        crate::writer::kv(envelope::SCHEMA_FP_META_KEY, spec.fingerprint_hex()),
        crate::writer::kv(crate::writer::CONFIG_HASH_KEY, config_hash),
        crate::writer::kv(crate::writer::ENRICHMENT_HASH_KEY, enrichment_hash),
    ];
    let props = crate::writer::base_props(cfg.compression, kvs)?;
    let plan = DatasetPlan {
        name: QUAR_NAME.to_owned(),
        props,
        kind: EncoderKind::Quarantine(spec),
    };
    let engine = EngineConfig {
        dir,
        node_id: node_id.to_owned(),
        plans: vec![plan],
        rotation: cfg.rotation,
        max_open_files: 1,
        row_group_bytes: 8 << 20,
        max_disk: cfg.max_disk,
        finalize_on_shutdown: true,
        budget_category: thadam_core::BudgetCategory::Quarantine,
    };
    Ok(crate::sink::ParquetSegmentSink::from_plans(engine))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantine_schema_is_stable() {
        let (schema, fp) = quarantine_schema(TsUnit::Micros);
        let rendered: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let id = f
                    .metadata()
                    .get(envelope::PARQUET_FIELD_ID_META_KEY)
                    .cloned()
                    .unwrap_or_default();
                format!(
                    "{} {:?} nullable={} id={}",
                    f.name(),
                    f.data_type(),
                    f.is_nullable(),
                    id
                )
            })
            .collect();
        insta::assert_snapshot!("quarantine_schema", rendered.join("\n"));
        insta::assert_snapshot!("quarantine_schema_fp", fp);
    }

    #[test]
    fn encodes_a_runtime_quarantine_row() {
        use thadam_core::{
            EventId, FieldDef, FieldType, Record, Shape, ShapeRef, TimeDomain, Times, Timestamp,
            WalPos,
        };
        let shape = ShapeRef::new(
            Shape::new(vec![
                FieldDef::new("_wal_epoch", FieldType::U64).required(),
                FieldDef::new("_wal_pos", FieldType::U64).required(),
                FieldDef::new("_ingest_time", FieldType::Ts),
                FieldDef::new("sink_id", FieldType::Str).required(),
                FieldDef::new("error_domain", FieldType::Str).required(),
                FieldDef::new("error_code", FieldType::Str).required(),
                FieldDef::new("reason", FieldType::Str).required(),
                FieldDef::new("raw_truncated", FieldType::Bool).required(),
            ])
            .unwrap(),
        );
        let rec = Record::new(
            shape,
            vec![
                Value::U64(7),
                Value::U64(4),
                Value::Ts(Timestamp::from_nanos(1_500)),
                Value::str("sink"),
                Value::str("decode"),
                Value::str("decode.bad_json"),
                Value::str("not json"),
                Value::Bool(false),
            ],
        )
        .unwrap();
        let event = Event::builder(
            EventId::from_u128(1),
            WalPos::new(4),
            Times::new(Timestamp::from_nanos(1_500), TimeDomain::Unknown),
        )
        .payload(rec)
        .build()
        .unwrap();
        let spec = QuarantineEncoderSpec::new(TsUnit::Micros);
        let mut enc = spec.make_encoder();
        enc.push(&event).unwrap_or_else(|r| panic!("{r:?}"));
        let batch = enc.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 13);
        // Ingest time survived at the requested unit.
        let col = batch.column(3);
        let ts = col
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .expect("micros by default");
        assert_eq!(ts.value(0), 1);
    }
}
