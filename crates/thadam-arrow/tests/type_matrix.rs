//! Every `Value` variant against every declared `FieldType`: which are
//! accepted (exactly or by widening), what they read back as, and which are
//! rejected. The acceptance table below is written by hand, not derived from
//! `thadam-core`, and cross-checked against `ValueType::widens_to` for scalars.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::HashSet;

use bytes::Bytes;
use common::{Cell, cell_of, column, encoder, event, shape};
use thadam_arrow::{ArrowOptions, UnsignedPolicy};
use thadam_core::{FieldDef, FieldType, Record, ShapeRef, Timestamp, Value, ValueType, codes};

fn inner_shape() -> ShapeRef {
    shape(vec![
        FieldDef::new("x", FieldType::F64),
        FieldDef::new("n", FieldType::I32).required(),
    ])
}

fn other_shape() -> ShapeRef {
    shape(vec![FieldDef::new("x", FieldType::F64)])
}

fn list(inner: FieldType) -> FieldType {
    FieldType::List(Box::new(inner))
}

fn declared_types() -> Vec<(&'static str, FieldType)> {
    vec![
        ("bool", FieldType::Bool),
        ("i8", FieldType::I8),
        ("i16", FieldType::I16),
        ("i32", FieldType::I32),
        ("i64", FieldType::I64),
        ("u8", FieldType::U8),
        ("u16", FieldType::U16),
        ("u32", FieldType::U32),
        ("u64", FieldType::U64),
        ("f32", FieldType::F32),
        ("f64", FieldType::F64),
        ("str", FieldType::Str),
        ("bytes", FieldType::Bytes),
        ("ts", FieldType::Ts),
        ("list<f32>", list(FieldType::F32)),
        ("list<f64>", list(FieldType::F64)),
        ("list<i32>", list(FieldType::I32)),
        ("list<i64>", list(FieldType::I64)),
        ("list<str>", list(FieldType::Str)),
        ("record", FieldType::Record(inner_shape())),
    ]
}

fn sample_values() -> Vec<(&'static str, Value)> {
    let rec =
        |shape: ShapeRef, values: Vec<Value>| Value::record(Record::new(shape, values).unwrap());
    vec![
        ("null", Value::Null),
        ("bool", Value::Bool(true)),
        ("i8", Value::I8(-5)),
        ("i16", Value::I16(-300)),
        ("i32", Value::I32(-70_000)),
        ("i64", Value::I64(-5_000_000_000)),
        ("u8", Value::U8(200)),
        ("u16", Value::U16(60_000)),
        ("u32", Value::U32(4_000_000_000)),
        ("u64", Value::U64(10_000_000_000)),
        ("f32", Value::F32(1.5)),
        ("f64", Value::F64(2.25)),
        ("str", Value::str("hello")),
        ("bytes", Value::bytes(Bytes::from_static(&[1, 2, 3]))),
        (
            "ts",
            Value::Ts(Timestamp::from_nanos(1_700_000_000_123_456_789)),
        ),
        ("f32s", Value::from(vec![1.0f32, 2.5])),
        ("f64s", Value::from(vec![1.0f64, -2.5, 3.5])),
        ("i32s", Value::from(vec![1i32, -2])),
        ("i64s", Value::from(vec![5_000_000_000i64, -1])),
        (
            "list_f64",
            Value::List(vec![Value::F64(1.0), Value::Null, Value::F64(2.0)]),
        ),
        ("list_i8", Value::List(vec![Value::I8(1), Value::I8(-2)])),
        ("list_str", Value::List(vec![Value::str("a"), Value::Null])),
        ("list_empty", Value::List(vec![])),
        (
            "record_same",
            rec(inner_shape(), vec![Value::F64(1.5), Value::I32(3)]),
        ),
        ("record_other", rec(other_shape(), vec![Value::F64(1.5)])),
    ]
}

/// Value names a declared type accepts, other than `null`.
fn accepted(declared: &str) -> &'static [&'static str] {
    match declared {
        "bool" => &["bool"],
        "i8" => &["i8"],
        "i16" => &["i8", "i16", "u8"],
        "i32" => &["i8", "i16", "i32", "u8", "u16"],
        "i64" => &["i8", "i16", "i32", "i64", "u8", "u16", "u32"],
        "u8" => &["u8"],
        "u16" => &["u8", "u16"],
        "u32" => &["u8", "u16", "u32"],
        "u64" => &["u8", "u16", "u32", "u64"],
        "f32" => &["f32"],
        "f64" => &["f32", "f64"],
        "str" => &["str"],
        "bytes" => &["bytes"],
        "ts" => &["ts"],
        "list<f32>" => &["f32s", "list_empty"],
        "list<f64>" => &["f32s", "f64s", "list_f64", "list_empty"],
        "list<i32>" => &["i32s", "list_i8", "list_empty"],
        "list<i64>" => &["i32s", "i64s", "list_i8", "list_empty"],
        "list<str>" => &["list_str", "list_empty"],
        "record" => &["record_same"],
        other => panic!("no oracle for {other}"),
    }
}

fn scalar_value_type(declared: &str) -> Option<ValueType> {
    Some(match declared {
        "bool" => ValueType::Bool,
        "i8" => ValueType::I8,
        "i16" => ValueType::I16,
        "i32" => ValueType::I32,
        "i64" => ValueType::I64,
        "u8" => ValueType::U8,
        "u16" => ValueType::U16,
        "u32" => ValueType::U32,
        "u64" => ValueType::U64,
        "f32" => ValueType::F32,
        "f64" => ValueType::F64,
        "str" => ValueType::Str,
        "bytes" => ValueType::Bytes,
        "ts" => ValueType::Ts,
        _ => return None,
    })
}

fn run_matrix(opts: ArrowOptions, ts_divisor: i64) {
    let mut cases = 0;
    for (dname, ty) in declared_types() {
        let rec_shape = shape(vec![FieldDef::new("v", ty.clone())]);
        let ok: HashSet<&str> = accepted(dname).iter().copied().collect();
        for (vname, value) in sample_values() {
            if vname == "null" {
                continue;
            }
            cases += 1;
            let mut enc = encoder(vec![FieldDef::new("v", ty.clone())], opts);
            let result = enc.push(&event(&rec_shape, vec![value.clone()]));

            if let Some(decl_vt) = scalar_value_type(dname) {
                assert_eq!(
                    ok.contains(vname),
                    value.value_type().widens_to(decl_vt),
                    "table disagrees with ValueType::widens_to for {vname} into {dname}"
                );
            }

            if ok.contains(vname) {
                result.unwrap_or_else(|r| panic!("{vname} into {dname}: {r:?}"));
                let batch = enc.finish();
                assert_eq!(
                    column(&batch, "v"),
                    vec![cell_of(&value, ts_divisor)],
                    "{vname} into {dname}"
                );
            } else {
                let reject = result.expect_err(&format!("{vname} into {dname} should reject"));
                assert_eq!(reject.index, 0);
                assert_eq!(
                    reject.error.code,
                    codes::SCHEMA_TYPE_MISMATCH,
                    "{vname} into {dname}"
                );
                assert_eq!(reject.error.context.get("field"), Some("v"));
                assert_eq!(enc.len(), 0, "a reject adds no row ({vname} into {dname})");
                enc.push(&event(&rec_shape, vec![Value::Null])).unwrap();
                let batch = enc.finish();
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(column(&batch, "v"), vec![Cell::Null]);
            }
        }
    }
    assert_eq!(cases, 20 * 24);
}

#[test]
fn every_value_against_every_declared_type_micros() {
    run_matrix(ArrowOptions::default(), 1_000);
}

#[test]
fn every_value_against_every_declared_type_nanos() {
    run_matrix(
        ArrowOptions::default().with_ts_unit(thadam_arrow::TsUnit::Nanos),
        1,
    );
}

#[test]
fn matrix_holds_under_the_signed_policy_except_u64_range() {
    // Signed storage never loses a value the table accepts, except u64 above
    // i64::MAX, covered in the unsigned tests.
    run_matrix(
        ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed),
        1_000,
    );
}

#[test]
fn null_is_accepted_by_nullable_fields_and_refused_by_required_ones() {
    for (dname, ty) in declared_types() {
        let rec_shape = shape(vec![FieldDef::new("v", ty.clone())]);

        let mut nullable = encoder(
            vec![FieldDef::new("v", ty.clone())],
            ArrowOptions::default(),
        );
        nullable
            .push(&event(&rec_shape, vec![Value::Null]))
            .unwrap_or_else(|r| panic!("null into nullable {dname}: {r:?}"));
        assert_eq!(column(&nullable.finish(), "v"), vec![Cell::Null], "{dname}");

        let mut required = encoder(
            vec![FieldDef::new("v", ty.clone()).required()],
            ArrowOptions::default(),
        );
        let reject = required
            .push(&event(&rec_shape, vec![Value::Null]))
            .expect_err(&format!("null into required {dname}"));
        assert_eq!(reject.error.code, codes::SCHEMA_MISSING_FIELD, "{dname}");
        assert_eq!(required.len(), 0);
    }
}

#[test]
fn nested_failures_name_the_path() {
    let ty = FieldType::Record(inner_shape());
    let rec_shape = shape(vec![FieldDef::new("v", ty.clone())]);
    let mut enc = encoder(vec![FieldDef::new("v", ty)], ArrowOptions::default());

    let missing =
        Value::record(Record::new(inner_shape(), vec![Value::F64(1.0), Value::Null]).unwrap());
    let reject = enc.push(&event(&rec_shape, vec![missing])).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_MISSING_FIELD);
    assert_eq!(reject.error.context.get("field"), Some("v.n"));

    let wrong =
        Value::record(Record::new(inner_shape(), vec![Value::str("no"), Value::I32(1)]).unwrap());
    let reject = enc.push(&event(&rec_shape, vec![wrong])).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_TYPE_MISMATCH);
    assert_eq!(reject.error.context.get("field"), Some("v.x"));
    assert_eq!(reject.index, 1, "index counts every offered event");
    assert_eq!(enc.len(), 0);
}

#[test]
fn list_items_are_checked_and_may_be_null() {
    let ty = list(FieldType::I64);
    let rec_shape = shape(vec![FieldDef::new("v", ty.clone())]);
    let mut enc = encoder(vec![FieldDef::new("v", ty)], ArrowOptions::default());

    let bad = Value::List(vec![Value::I8(1), Value::F64(2.0)]);
    let reject = enc.push(&event(&rec_shape, vec![bad])).unwrap_err();
    assert_eq!(reject.error.code, codes::SCHEMA_TYPE_MISMATCH);

    let good = Value::List(vec![Value::I8(1), Value::Null, Value::U32(4_000_000_000)]);
    enc.push(&event(&rec_shape, vec![good])).unwrap();
    assert_eq!(
        column(&enc.finish(), "v"),
        vec![Cell::List(vec![
            Cell::Int(1),
            Cell::Null,
            Cell::Int(4_000_000_000)
        ])]
    );
}

#[test]
fn nested_lists_and_lists_of_records_round_trip() {
    let point = shape(vec![
        FieldDef::new("x", FieldType::F32),
        FieldDef::new("tag", FieldType::Str),
    ]);
    let ty = list(list(FieldType::I32));
    let rec_ty = list(FieldType::Record(point.clone()));
    let rec_shape = shape(vec![
        FieldDef::new("grid", ty.clone()),
        FieldDef::new("points", rec_ty.clone()),
    ]);
    let mut enc = encoder(
        vec![FieldDef::new("grid", ty), FieldDef::new("points", rec_ty)],
        ArrowOptions::default(),
    );
    let grid = Value::List(vec![
        Value::from(vec![1i32, 2]),
        Value::Null,
        Value::List(vec![Value::I8(3)]),
    ]);
    let points = Value::List(vec![
        Value::record(Record::new(point.clone(), vec![Value::F32(1.5), Value::str("a")]).unwrap()),
        Value::Null,
        Value::record(Record::new(point, vec![Value::Null, Value::Null]).unwrap()),
    ]);
    enc.push(&event(&rec_shape, vec![grid.clone(), points.clone()]))
        .unwrap();
    enc.push(&event(&rec_shape, vec![Value::Null, Value::Null]))
        .unwrap();
    let batch = enc.finish();
    assert_eq!(
        column(&batch, "grid"),
        vec![cell_of(&grid, 1000), Cell::Null]
    );
    assert_eq!(
        column(&batch, "points"),
        vec![cell_of(&points, 1000), Cell::Null]
    );
}
