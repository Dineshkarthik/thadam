//! Encoder lifecycle: reuse, atomic rejects, size estimates, shape handling.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow_array::Array;
use common::{Cell, column, encoder, event, schema, shape};
use thadam_arrow::{ArrowOptions, BatchEncoder};
use thadam_core::{FieldDef, FieldType, Value, codes};

fn telemetry_fields() -> Vec<FieldDef> {
    vec![
        FieldDef::new("seq", FieldType::U32),
        FieldDef::new("temp", FieldType::F32),
        FieldDef::new("robot", FieldType::Str),
        FieldDef::new("joints", FieldType::List(Box::new(FieldType::F64))),
    ]
}

fn telemetry_values(i: u32) -> Vec<Value> {
    vec![
        Value::U32(i),
        Value::F32(i as f32 * 0.5),
        Value::str(format!("robot-{:03}", i % 40)),
        Value::from(vec![f64::from(i); 7]),
    ]
}

#[test]
fn an_encoder_is_reusable_and_each_batch_is_independent() {
    let sh = shape(telemetry_fields());
    let mut enc = encoder(telemetry_fields(), ArrowOptions::default());
    for round in 0..4u32 {
        let n = 5 + round * 3;
        for i in 0..n {
            enc.push(&event(&sh, telemetry_values(round * 100 + i)))
                .unwrap();
        }
        assert_eq!(enc.len(), n as usize);
        let batch = enc.finish();
        assert_eq!(batch.num_rows(), n as usize);
        assert_eq!(enc.len(), 0);
        assert!(enc.is_empty());
        assert_eq!(enc.estimated_bytes(), 0);
        assert_eq!(
            column(&batch, "seq")[0],
            Cell::Int(i128::from(round * 100)),
            "round {round} starts clean"
        );
        assert_eq!(
            column(&batch, "joints").len(),
            n as usize,
            "list column lengths match the row count"
        );
        assert_eq!(batch.schema(), enc.schema().arrow_schema());
    }
}

#[test]
fn an_empty_finish_is_a_zero_row_batch_of_the_right_schema() {
    let mut enc = encoder(telemetry_fields(), ArrowOptions::default());
    let batch = enc.finish();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), enc.schema().arrow_schema());
    let again = enc.finish();
    assert_eq!(again.num_rows(), 0);
}

#[test]
fn capacity_hint_zero_still_works() {
    let sh = shape(telemetry_fields());
    let mut enc = BatchEncoder::new(schema(telemetry_fields(), ArrowOptions::default()), 0);
    for i in 0..100 {
        enc.push(&event(&sh, telemetry_values(i))).unwrap();
    }
    assert_eq!(enc.finish().num_rows(), 100);
}

#[test]
fn rejected_events_leave_every_column_aligned() {
    let sh = shape(telemetry_fields());
    let mut enc = encoder(telemetry_fields(), ArrowOptions::default());
    let mut kept = Vec::new();
    let mut rejected = Vec::new();
    for i in 0..60u32 {
        let mut values = telemetry_values(i);
        // Fail late in the row, after earlier columns would have been written.
        if i % 3 == 0 {
            values[3] = Value::str("not an array");
        }
        match enc.push(&event(&sh, values)) {
            Ok(()) => kept.push(i),
            Err(r) => {
                assert_eq!(r.index, i);
                assert_eq!(r.error.context.get("field"), Some("joints"));
                rejected.push(i);
            }
        }
    }
    assert_eq!(rejected.len(), 20);
    let batch = enc.finish();
    assert_eq!(batch.num_rows(), kept.len());
    for col in batch.columns() {
        assert_eq!(col.len(), kept.len());
    }
    assert_eq!(
        column(&batch, "seq"),
        kept.iter()
            .map(|i| Cell::Int(i128::from(*i)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        column(&batch, "_wal_pos").len(),
        kept.len(),
        "envelope columns are not left ahead of the declared ones"
    );
}

#[test]
fn reject_indexes_restart_after_finish() {
    let sh = shape(vec![FieldDef::new("v", FieldType::I32)]);
    let mut enc = encoder(
        vec![FieldDef::new("v", FieldType::I32)],
        ArrowOptions::default(),
    );
    let bad = || event(&sh, vec![Value::str("x")]);
    assert_eq!(enc.push(&bad()).unwrap_err().index, 0);
    assert_eq!(enc.push(&bad()).unwrap_err().index, 1);
    enc.finish();
    assert_eq!(enc.push(&bad()).unwrap_err().index, 0);
}

#[test]
fn reject_messages_carry_no_payload() {
    let sh = shape(vec![FieldDef::new("v", FieldType::I32)]);
    let mut enc = encoder(
        vec![FieldDef::new("v", FieldType::I32)],
        ArrowOptions::default(),
    );
    let r = enc
        .push(&event(&sh, vec![Value::str("hunter2-secret")]))
        .unwrap_err();
    let text = format!("{r:?}");
    assert!(!text.contains("hunter2"), "{text}");
    assert_eq!(r.error.code, codes::SCHEMA_TYPE_MISMATCH);
    assert_eq!(r.error.message, "expected i32, got str");
}

#[test]
fn estimated_bytes_grows_with_rows_and_tracks_real_size() {
    let sh = shape(telemetry_fields());
    let mut enc = encoder(telemetry_fields(), ArrowOptions::default());
    let mut last = 0;
    for i in 0..1000 {
        enc.push(&event(&sh, telemetry_values(i))).unwrap();
        let now = enc.estimated_bytes();
        assert!(now > last, "estimate grows with every row");
        last = now;
    }
    let estimate = enc.estimated_bytes() as f64;
    let batch = enc.finish();
    let actual = batch.get_array_memory_size() as f64;
    let ratio = estimate / actual;
    assert!(
        (0.5..=1.5).contains(&ratio),
        "estimate {estimate} vs actual {actual} (ratio {ratio:.2})"
    );
}

#[test]
fn estimated_bytes_counts_strings_and_list_elements() {
    let sh = shape(vec![
        FieldDef::new("s", FieldType::Str),
        FieldDef::new("l", FieldType::List(Box::new(FieldType::F64))),
    ]);
    let fields = vec![
        FieldDef::new("s", FieldType::Str),
        FieldDef::new("l", FieldType::List(Box::new(FieldType::F64))),
    ];
    let mut enc = encoder(fields, ArrowOptions::default());
    enc.push(&event(
        &sh,
        vec![Value::str(""), Value::from(Vec::<f64>::new())],
    ))
    .unwrap();
    let base = enc.estimated_bytes();
    enc.push(&event(
        &sh,
        vec![Value::str("x".repeat(1000)), Value::from(vec![0.0f64; 100])],
    ))
    .unwrap();
    let delta = enc.estimated_bytes() - base;
    assert!(delta >= 1000 + 800, "delta {delta}");
}

#[test]
fn records_may_have_a_different_shape_than_the_declared_fields() {
    let declared = vec![
        FieldDef::new("a", FieldType::I64),
        FieldDef::new("b", FieldType::Str),
        FieldDef::new("c", FieldType::F64).required(),
    ];
    let mut enc = encoder(declared.clone(), ArrowOptions::default());

    // Same fields in another order plus an extra one.
    let reordered = shape(vec![
        FieldDef::new("extra", FieldType::Bool),
        FieldDef::new("c", FieldType::F64),
        FieldDef::new("a", FieldType::I32),
        FieldDef::new("b", FieldType::Str),
    ]);
    enc.push(&event(
        &reordered,
        vec![
            Value::Bool(true),
            Value::F64(1.5),
            Value::I32(-4),
            Value::str("x"),
        ],
    ))
    .unwrap();

    // Missing the optional `b` is a null; missing the required `c` is a reject.
    let sparse = shape(vec![
        FieldDef::new("a", FieldType::I64),
        FieldDef::new("c", FieldType::F64),
    ]);
    enc.push(&event(&sparse, vec![Value::I64(9), Value::F64(2.5)]))
        .unwrap();
    let no_c = shape(vec![FieldDef::new("a", FieldType::I64)]);
    let r = enc.push(&event(&no_c, vec![Value::I64(1)])).unwrap_err();
    assert_eq!(r.error.code, codes::SCHEMA_MISSING_FIELD);
    assert_eq!(r.error.context.get("field"), Some("c"));

    let batch = enc.finish();
    assert_eq!(column(&batch, "a"), vec![Cell::Int(-4), Cell::Int(9)]);
    assert_eq!(column(&batch, "b"), vec![Cell::Str("x".into()), Cell::Null]);
    assert_eq!(
        column(&batch, "c"),
        vec![common::float(1.5), common::float(2.5)]
    );
}

#[test]
fn many_shapes_are_handled_beyond_the_plan_cache() {
    let declared = vec![FieldDef::new("v", FieldType::I32)];
    let mut enc = encoder(declared, ArrowOptions::default());
    // 20 distinct shapes (different extra fields), interleaved twice.
    let shapes: Vec<_> = (0..20)
        .map(|i| {
            shape(vec![
                FieldDef::new(format!("pad{i}"), FieldType::Bool),
                FieldDef::new("v", FieldType::I32),
            ])
        })
        .collect();
    for round in 0..2 {
        for (i, sh) in shapes.iter().enumerate() {
            enc.push(&event(
                sh,
                vec![Value::Bool(false), Value::I32((round * 100 + i) as i32)],
            ))
            .unwrap();
        }
    }
    let got = column(&enc.finish(), "v");
    let want: Vec<Cell> = (0..2)
        .flat_map(|r| (0..20).map(move |i| Cell::Int(i128::from(r * 100 + i))))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn schemas_are_shared_between_encoders() {
    let s = schema(telemetry_fields(), ArrowOptions::default());
    let a = BatchEncoder::new(Arc::clone(&s), 4);
    let b = BatchEncoder::new(Arc::clone(&s), 4);
    assert!(Arc::ptr_eq(a.schema(), b.schema()));
    assert!(Arc::ptr_eq(
        &a.schema().arrow_schema(),
        &b.schema().arrow_schema()
    ));
}

#[test]
fn large_batches_of_variable_length_data_round_trip() {
    let sh = shape(vec![
        FieldDef::new("s", FieldType::Str),
        FieldDef::new("b", FieldType::Bytes),
    ]);
    let fields = vec![
        FieldDef::new("s", FieldType::Str),
        FieldDef::new("b", FieldType::Bytes),
    ];
    let mut enc = BatchEncoder::new(schema(fields, ArrowOptions::default()), 8);
    for round in 0..3 {
        for i in 0..2000 {
            let s = format!("{round}-{i}-{}", "x".repeat(i % 50));
            enc.push(&event(
                &sh,
                vec![Value::str(s.clone()), Value::bytes(s.into_bytes().into())],
            ))
            .unwrap();
        }
        let batch = enc.finish();
        assert_eq!(batch.num_rows(), 2000);
        let strs = column(&batch, "s");
        let bins = column(&batch, "b");
        for (i, (s, b)) in strs.iter().zip(&bins).enumerate() {
            let want = format!("{round}-{i}-{}", "x".repeat(i % 50));
            assert_eq!(s, &Cell::Str(want.clone()));
            assert_eq!(b, &Cell::Bytes(want.into_bytes()));
        }
    }
}
