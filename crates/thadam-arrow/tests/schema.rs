//! Schema construction: type mapping, nullability, field IDs, validation.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::HashSet;

use arrow_schema::{DataType, TimeUnit};
use common::{all_ids, shape};
use thadam_arrow::{
    ArrowOptions, DatasetSchema, SchemaError, TsUnit, UnsignedPolicy, columns, field_id,
};
use thadam_core::{Classify, FieldDef, FieldType, Retry, codes};

fn list(inner: FieldType) -> FieldType {
    FieldType::List(Box::new(inner))
}

fn build(fields: Vec<FieldDef>, options: ArrowOptions) -> Result<DatasetSchema, SchemaError> {
    DatasetSchema::builder("d")
        .fields(common::with_ids(fields))
        .options(options)
        .build()
}

fn declared_type(ty: FieldType, options: ArrowOptions) -> DataType {
    let schema = build(vec![FieldDef::new("v", ty)], options).unwrap();
    let arrow = schema.arrow_schema();
    arrow.field_with_name("v").unwrap().data_type().clone()
}

#[test]
fn scalar_types_map_to_arrow_types() {
    let o = ArrowOptions::default();
    let expect = [
        (FieldType::Bool, DataType::Boolean),
        (FieldType::I8, DataType::Int8),
        (FieldType::I16, DataType::Int16),
        (FieldType::I32, DataType::Int32),
        (FieldType::I64, DataType::Int64),
        (FieldType::U8, DataType::UInt8),
        (FieldType::U16, DataType::UInt16),
        (FieldType::U32, DataType::UInt32),
        (FieldType::U64, DataType::UInt64),
        (FieldType::F32, DataType::Float32),
        (FieldType::F64, DataType::Float64),
        (FieldType::Str, DataType::Utf8),
        (FieldType::Bytes, DataType::Binary),
        (
            FieldType::Ts,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ];
    for (ty, dt) in expect {
        assert_eq!(declared_type(ty.clone(), o), dt, "{ty:?}");
    }
}

#[test]
fn unsigned_policy_and_ts_unit_change_the_mapping() {
    let signed = ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed);
    for (ty, dt) in [
        (FieldType::U8, DataType::Int16),
        (FieldType::U16, DataType::Int32),
        (FieldType::U32, DataType::Int64),
        (FieldType::U64, DataType::Int64),
        (FieldType::I32, DataType::Int32),
    ] {
        assert_eq!(declared_type(ty.clone(), signed), dt, "{ty:?}");
    }
    let nanos = ArrowOptions::default().with_ts_unit(TsUnit::Nanos);
    assert_eq!(
        declared_type(FieldType::Ts, nanos),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
}

#[test]
fn lists_and_records_map_to_list_and_struct() {
    let dt = declared_type(list(FieldType::F64), ArrowOptions::default());
    let DataType::List(item) = dt else {
        panic!("not a list");
    };
    assert_eq!(item.name(), "element");
    assert_eq!(item.data_type(), &DataType::Float64);
    assert!(item.is_nullable());

    let inner = shape(vec![
        FieldDef::new("x", FieldType::F32),
        FieldDef::new("n", FieldType::U8).required(),
    ]);
    let DataType::Struct(fields) = declared_type(FieldType::Record(inner), ArrowOptions::default())
    else {
        panic!("not a struct");
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "x");
    assert!(fields[0].is_nullable());
    assert_eq!(fields[1].name(), "n");
    assert_eq!(fields[1].data_type(), &DataType::UInt8);
    assert!(!fields[1].is_nullable());
}

#[test]
fn declared_nullability_and_units_reach_the_arrow_fields() {
    let schema = build(
        vec![
            FieldDef::new("opt", FieldType::F32).with_unit("Cel"),
            FieldDef::new("req", FieldType::F32).required(),
        ],
        ArrowOptions::default(),
    )
    .unwrap();
    let arrow = schema.arrow_schema();
    let opt = arrow.field_with_name("opt").unwrap();
    let req = arrow.field_with_name("req").unwrap();
    assert!(opt.is_nullable());
    assert!(!req.is_nullable());
    assert_eq!(
        opt.metadata()
            .get(columns::UNIT_META_KEY)
            .map(String::as_str),
        Some("Cel")
    );
    assert!(req.metadata().get(columns::UNIT_META_KEY).is_none());
}

#[test]
fn schema_metadata_names_the_dataset_and_carries_the_fingerprint() {
    let schema = build(
        vec![FieldDef::new("v", FieldType::I32)],
        ArrowOptions::default(),
    )
    .unwrap();
    let arrow = schema.arrow_schema();
    assert_eq!(
        arrow
            .metadata()
            .get(columns::DATASET_META_KEY)
            .map(String::as_str),
        Some("d")
    );
    assert_eq!(
        arrow.metadata().get(columns::SCHEMA_FP_META_KEY),
        Some(&schema.fingerprint_hex())
    );
    assert_eq!(schema.fingerprint_hex().len(), 32);
    assert_eq!(schema.name(), "d");
}

fn example_fields() -> Vec<FieldDef> {
    let inner = shape(vec![
        FieldDef::new("x", FieldType::F64),
        FieldDef::new("n", FieldType::I32),
    ]);
    vec![
        FieldDef::new("soc", FieldType::F32).with_field_id(1000),
        FieldDef::new("joints", list(FieldType::F64)).with_field_id(1001),
        FieldDef::new("pose", FieldType::Record(inner)).with_field_id(1002),
        FieldDef::new("grid", list(list(FieldType::I32))).with_field_id(1003),
        FieldDef::new("name", FieldType::Str).with_field_id(1004),
    ]
}

fn example_schema(fields: Vec<FieldDef>) -> DatasetSchema {
    DatasetSchema::builder("d")
        .level("site", 100)
        .level("robot", 101)
        .exploded(true)
        .fields(fields)
        .build()
        .unwrap()
}

#[test]
fn every_field_has_a_unique_id_nested_fields_included() {
    let schema = example_schema(example_fields());
    let ids = all_ids(&schema.arrow_schema());
    let unique: HashSet<i32> = ids.iter().map(|(_, id)| *id).collect();
    assert_eq!(unique.len(), ids.len(), "ids are unique: {ids:?}");
    assert!(ids.iter().all(|(_, id)| *id > 0));
}

#[test]
fn envelope_ids_are_fixed_by_the_format() {
    let schema = example_schema(vec![]);
    let arrow = schema.arrow_schema();
    let by_name = |n: &str| field_id(arrow.field_with_name(n).unwrap()).unwrap();
    let expect = [
        ("_event_id", 1),
        ("_ingest_time", 2),
        ("_source_time", 3),
        ("_event_time", 4),
        ("_time_domain", 5),
        ("_quality_status", 6),
        ("_quality_flags", 7),
        ("_quality_reason", 8),
        ("_clock_synced", 9),
        ("_wal_epoch", 10),
        ("_wal_pos", 11),
        ("sample_idx", 12),
        ("site", 100),
        ("robot", 101),
    ];
    for (name, id) in expect {
        assert_eq!(by_name(name), id, "{name}");
    }
    assert_eq!(columns::RESERVED_WAL_RECORD_IDX, 13);
}

#[test]
fn envelope_ids_do_not_depend_on_options() {
    let ids = |o: ArrowOptions| {
        let mut ids = all_ids(&example_schema_with(o).arrow_schema());
        ids.sort();
        ids
    };
    let base = ids(ArrowOptions::default());
    for o in [
        ArrowOptions::default().with_ts_unit(TsUnit::Nanos),
        ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed),
    ] {
        assert_eq!(ids(o), base);
    }
    let without_lineage = ids(ArrowOptions::default().with_lineage_columns(false));
    for entry in &without_lineage {
        assert!(
            base.contains(entry),
            "{entry:?} unchanged when columns are dropped"
        );
    }
}

fn example_schema_with(o: ArrowOptions) -> DatasetSchema {
    DatasetSchema::builder("d")
        .level("site", 100)
        .exploded(true)
        .fields(example_fields())
        .options(o)
        .build()
        .unwrap()
}

#[test]
fn declared_ids_are_taken_from_the_field_definitions() {
    let schema = example_schema(example_fields());
    let arrow = schema.arrow_schema();
    for (name, id) in [
        ("soc", 1000),
        ("joints", 1001),
        ("pose", 1002),
        ("grid", 1003),
        ("name", 1004),
    ] {
        assert_eq!(field_id(arrow.field_with_name(name).unwrap()), Some(id));
    }
}

#[test]
fn nested_ids_are_derived_from_the_top_level_id_and_type_tree_position() {
    let base = columns::NESTED_ID_BASE as i32;
    let schema = example_schema(example_fields());
    let ids: std::collections::HashMap<String, i32> =
        all_ids(&schema.arrow_schema()).into_iter().collect();
    // joints: id 1001, one element at slot 0.
    assert_eq!(ids["joints.element"], base + 256);
    // pose: id 1002, children x and n at slots 0 and 1.
    assert_eq!(ids["pose.x"], base + 2 * 256);
    assert_eq!(ids["pose.n"], base + 2 * 256 + 1);
    // grid: id 1003, outer element slot 0, inner element slot 1.
    assert_eq!(ids["grid.element"], base + 3 * 256);
    assert_eq!(ids["grid.element.element"], base + 3 * 256 + 1);
}

#[test]
fn ids_survive_reordering_addition_and_removal() {
    let ids_of = |fields: Vec<FieldDef>| -> std::collections::HashMap<String, i32> {
        all_ids(&example_schema(fields).arrow_schema())
            .into_iter()
            .collect()
    };
    let base = ids_of(example_fields());

    // Two nodes that list the same fields in a different order agree on every ID.
    let mut reordered = example_fields();
    reordered.reverse();
    assert_eq!(ids_of(reordered), base);

    // Adding a field changes nothing else.
    let mut added = example_fields();
    added.push(FieldDef::new("extra", FieldType::Bool).with_field_id(2000));
    let with_extra = ids_of(added);
    for (name, id) in &base {
        assert_eq!(with_extra[name], *id, "{name}");
    }
    assert_eq!(with_extra["extra"], 2000);

    // Removing a field leaves the others' IDs alone.
    let mut removed = example_fields();
    removed.remove(1);
    let without = ids_of(removed);
    for (name, id) in &without {
        assert_eq!(base[name], *id, "{name}");
    }
    assert!(!without.contains_key("joints"));
}

#[test]
fn widening_a_list_keeps_its_ids() {
    let ids = |ty: FieldType| {
        let s = build(
            vec![FieldDef::new("a", ty).with_field_id(1500)],
            ArrowOptions::default(),
        )
        .unwrap();
        let mut v = all_ids(&s.arrow_schema());
        v.sort();
        v
    };
    let narrow: Vec<i32> = ids(list(FieldType::I32)).into_iter().map(|x| x.1).collect();
    let wide: Vec<i32> = ids(list(FieldType::I64)).into_iter().map(|x| x.1).collect();
    assert_eq!(narrow, wide);
}

#[test]
fn schema_errors_are_config_errors() {
    let e = build(
        vec![FieldDef::new("_x", FieldType::I32)],
        ArrowOptions::default(),
    )
    .unwrap_err();
    assert_eq!(e.domain(), thadam_core::ErrorDomain::Config);
    assert_eq!(e.code(), codes::CONFIG_INVALID);
    assert_eq!(e.retry(), Retry::Permanent);
}

#[test]
fn invalid_schemas_are_refused() {
    let one = |f: FieldDef| DatasetSchema::builder("d").field(f).build().unwrap_err();
    let ok = |name: &str| FieldDef::new(name, FieldType::I32).with_field_id(1000);

    assert_eq!(
        DatasetSchema::builder("").build().unwrap_err(),
        SchemaError::EmptyDatasetName
    );
    assert_eq!(
        one(FieldDef::new("a", FieldType::I32)),
        SchemaError::MissingFieldId("a".into())
    );
    for id in [0, 999, 4_194_304, u32::MAX] {
        assert!(
            matches!(
                one(ok("a").with_field_id(id)),
                SchemaError::FieldIdOutOfRange { .. }
            ),
            "id {id}"
        );
    }
    for id in [1000, 4_194_303] {
        assert!(
            DatasetSchema::builder("d")
                .field(ok("a").with_field_id(id))
                .build()
                .is_ok()
        );
    }
    assert_eq!(
        one(ok("_hidden")),
        SchemaError::ReservedName("_hidden".into())
    );
    assert_eq!(one(ok("")), SchemaError::EmptyName);

    let dup_name = DatasetSchema::builder("d")
        .field(ok("a"))
        .field(ok("a").with_field_id(1001))
        .build()
        .unwrap_err();
    assert_eq!(dup_name, SchemaError::DuplicateName("a".into()));

    let dup_id = DatasetSchema::builder("d")
        .field(ok("a"))
        .field(ok("b"))
        .build()
        .unwrap_err();
    assert_eq!(
        dup_id,
        SchemaError::DuplicateFieldId {
            id: 1000,
            first: "a".into(),
            second: "b".into()
        }
    );
}

#[test]
fn identity_levels_are_validated() {
    let level = |name: &str, id: u32| DatasetSchema::builder("d").level(name, id).build();
    assert!(level("site", 100).is_ok());
    assert!(level("site", 199).is_ok());
    for id in [0, 99, 200, 1000] {
        assert!(
            matches!(
                level("site", id),
                Err(SchemaError::FieldIdOutOfRange { .. })
            ),
            "id {id}"
        );
    }
    assert_eq!(
        level("_site", 100).unwrap_err(),
        SchemaError::ReservedName("_site".into())
    );
    assert_eq!(
        DatasetSchema::builder("d")
            .level("a", 100)
            .level("a", 101)
            .build()
            .unwrap_err(),
        SchemaError::DuplicateName("a".into())
    );
    assert!(matches!(
        DatasetSchema::builder("d")
            .level("a", 100)
            .level("b", 100)
            .build(),
        Err(SchemaError::DuplicateFieldId { id: 100, .. })
    ));
    let mut b = DatasetSchema::builder("d");
    for i in 0..33 {
        b = b.level(format!("l{i}"), 100 + i);
    }
    assert_eq!(b.build().unwrap_err(), SchemaError::TooManyLevels(33));
}

#[test]
fn declared_names_cannot_collide_with_envelope_columns() {
    let f = |name: &str| FieldDef::new(name, FieldType::I32).with_field_id(1000);
    let exploded = DatasetSchema::builder("d")
        .exploded(true)
        .field(f("sample_idx"));
    assert_eq!(
        exploded.build().unwrap_err(),
        SchemaError::DuplicateName("sample_idx".into())
    );
    // Without explode, sample_idx is an ordinary payload name.
    assert!(
        DatasetSchema::builder("d")
            .field(f("sample_idx"))
            .build()
            .is_ok()
    );
    // A level and a field cannot share a name either.
    assert_eq!(
        DatasetSchema::builder("d")
            .level("robot", 100)
            .field(f("robot"))
            .build()
            .unwrap_err(),
        SchemaError::DuplicateName("robot".into())
    );
}

#[test]
fn type_trees_have_limits() {
    let named = |ty: FieldType| {
        DatasetSchema::builder("d")
            .field(FieldDef::new("a", ty).with_field_id(1000))
            .build()
    };

    assert_eq!(
        named(FieldType::Record(shape(vec![]))).unwrap_err(),
        SchemaError::EmptyRecord("a".into())
    );

    let mut deep = FieldType::I32;
    for _ in 0..6 {
        deep = list(deep);
    }
    assert!(named(deep.clone()).is_ok(), "six levels are allowed");
    assert!(matches!(
        named(list(deep)),
        Err(SchemaError::TooDeep { max: 6, .. })
    ));

    let wide = shape(
        (0..300)
            .map(|i| FieldDef::new(format!("c{i}"), FieldType::Bool))
            .collect(),
    );
    assert_eq!(
        named(FieldType::Record(wide)).unwrap_err(),
        SchemaError::TooManyNestedFields("a".into())
    );
    let ok = shape(
        (0..256)
            .map(|i| FieldDef::new(format!("c{i}"), FieldType::Bool))
            .collect(),
    );
    assert!(
        named(FieldType::Record(ok)).is_ok(),
        "256 nested fields fit"
    );
}
