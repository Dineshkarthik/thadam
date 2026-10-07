//! The unsigned policy: native unsigned types, or the next wider signed type
//! with a range reject for `u64`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use arrow_array::RecordBatch;
use common::{Cell, column, encoder, event, schema, shape, times};
use thadam_arrow::{ArrowOptions, BatchContext, BatchEncoder, DatasetSchema, UnsignedPolicy};
use thadam_core::{EventId, FieldDef, FieldType, Record, Value, WalEpoch, WalPos, codes};

fn fields() -> Vec<FieldDef> {
    vec![
        FieldDef::new("a", FieldType::U8),
        FieldDef::new("b", FieldType::U16),
        FieldDef::new("c", FieldType::U32),
        FieldDef::new("d", FieldType::U64),
    ]
}

fn maxima() -> Vec<Value> {
    vec![
        Value::U8(u8::MAX),
        Value::U16(u16::MAX),
        Value::U32(u32::MAX),
        Value::U64(i64::MAX as u64),
    ]
}

fn encode(
    opts: ArrowOptions,
    values: Vec<Value>,
) -> (BatchEncoder, Result<(), thadam_core::Reject>) {
    let mut enc = encoder(fields(), opts);
    let r = enc.push(&event(&shape(fields()), values));
    (enc, r)
}

fn types(batch: &RecordBatch) -> Vec<String> {
    ["a", "b", "c", "d"]
        .iter()
        .map(|n| {
            batch
                .schema()
                .field_with_name(n)
                .unwrap()
                .data_type()
                .to_string()
        })
        .collect()
}

#[test]
fn native_is_the_default_and_stores_unsigned_types() {
    assert_eq!(ArrowOptions::default().unsigned, UnsignedPolicy::Native);
    let (mut enc, r) = encode(ArrowOptions::default(), maxima());
    r.unwrap();
    let batch = enc.finish();
    assert_eq!(types(&batch), ["UInt8", "UInt16", "UInt32", "UInt64"]);
    assert_eq!(
        (column(&batch, "a"), column(&batch, "c")),
        (vec![Cell::Int(255)], vec![Cell::Int(i128::from(u32::MAX))])
    );
}

#[test]
fn native_stores_u64_above_i64_max() {
    let (mut enc, r) = encode(
        ArrowOptions::default(),
        vec![
            Value::U8(0),
            Value::U16(0),
            Value::U32(0),
            Value::U64(u64::MAX),
        ],
    );
    r.unwrap();
    assert_eq!(
        column(&enc.finish(), "d"),
        vec![Cell::Int(i128::from(u64::MAX))]
    );
}

#[test]
fn signed_widens_to_the_next_signed_type() {
    let opts = ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed);
    let (mut enc, r) = encode(opts, maxima());
    r.unwrap();
    let batch = enc.finish();
    assert_eq!(types(&batch), ["Int16", "Int32", "Int64", "Int64"]);
    assert_eq!(
        column(&batch, "a")
            .into_iter()
            .chain(column(&batch, "b"))
            .chain(column(&batch, "c"))
            .chain(column(&batch, "d"))
            .collect::<Vec<_>>(),
        vec![
            Cell::Int(255),
            Cell::Int(65_535),
            Cell::Int(i128::from(u32::MAX)),
            Cell::Int(i128::from(i64::MAX)),
        ],
        "every unsigned value up to i64::MAX is exact"
    );
}

#[test]
fn signed_rejects_u64_above_i64_max_and_leaves_no_partial_row() {
    let opts = ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed);
    for bad in [i64::MAX as u64 + 1, u64::MAX] {
        let (mut enc, r) = encode(
            opts,
            vec![Value::U8(1), Value::U16(2), Value::U32(3), Value::U64(bad)],
        );
        let reject = r.unwrap_err();
        assert_eq!(reject.error.code, codes::SCHEMA_NARROWING, "{bad}");
        assert_eq!(reject.error.context.get("field"), Some("d"));
        assert!(
            !reject.error.message.contains(&bad.to_string()),
            "no payload in messages"
        );
        assert_eq!(enc.len(), 0);
        let batch = enc.finish();
        assert_eq!(batch.num_rows(), 0);
    }
}

#[test]
fn signed_range_is_checked_inside_lists_and_records_too() {
    let opts = ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed);
    let ty = FieldType::List(Box::new(FieldType::U64));
    let sh = shape(vec![FieldDef::new("l", ty.clone())]);
    let mut enc = encoder(vec![FieldDef::new("l", ty)], opts);
    let bad = Value::List(vec![Value::U64(1), Value::U64(u64::MAX)]);
    let reject = enc.push(&event(&sh, vec![bad])).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_NARROWING);
    let good = Value::List(vec![Value::U64(1), Value::U8(255)]);
    enc.push(&event(&sh, vec![good])).unwrap();
    assert_eq!(
        column(&enc.finish(), "l"),
        vec![Cell::List(vec![Cell::Int(1), Cell::Int(255)])]
    );
}

#[test]
fn signed_checks_the_lineage_columns_but_native_does_not() {
    let sh = shape(vec![FieldDef::new("v", FieldType::I32)]);
    let ev_at = |pos: u64| {
        thadam_core::Event::builder(EventId::from_u128(1), WalPos::new(pos), times())
            .payload(Record::new(sh.clone(), vec![Value::I32(1)]).unwrap())
            .build()
            .unwrap()
    };
    let make = |p: UnsignedPolicy| {
        BatchEncoder::new(schema(vec![], ArrowOptions::default().with_unsigned(p)), 4)
    };

    let mut native = make(UnsignedPolicy::Native);
    native.push(&ev_at(u64::MAX)).unwrap();
    assert_eq!(
        column(&native.finish(), "_wal_pos"),
        vec![Cell::Int(i128::from(u64::MAX))]
    );

    let mut signed = make(UnsignedPolicy::Signed);
    signed.push(&ev_at(i64::MAX as u64)).unwrap();
    let reject = signed.push(&ev_at(i64::MAX as u64 + 1)).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_NARROWING);
    assert_eq!(reject.error.context.get("field"), Some("_wal_pos"));

    signed.set_context(BatchContext {
        epoch: WalEpoch::new(u64::MAX),
        clock_synced: true,
    });
    let reject = signed.push(&ev_at(1)).unwrap_err();
    assert_eq!(reject.error.context.get("field"), Some("_wal_epoch"));
    assert_eq!(signed.len(), 1);
}

#[test]
fn unsigned_policy_is_part_of_the_schema_fingerprint() {
    let build = |p| {
        DatasetSchema::builder("d")
            .field(FieldDef::new("v", FieldType::U32).with_field_id(1000))
            .options(ArrowOptions::default().with_unsigned(p))
            .build()
            .unwrap()
            .fingerprint()
    };
    assert_ne!(build(UnsignedPolicy::Native), build(UnsignedPolicy::Signed));
}
