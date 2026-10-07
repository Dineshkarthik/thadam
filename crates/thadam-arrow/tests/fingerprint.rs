//! Schema fingerprint stability and sensitivity.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use arrow_schema::{DataType, Field, TimeUnit};
use common::shape;
use thadam_arrow::{ArrowOptions, DatasetSchema, TsUnit, UnsignedPolicy, field_id};
use thadam_core::{FieldDef, FieldType};

fn list(inner: FieldType) -> FieldType {
    FieldType::List(Box::new(inner))
}

/// A telemetry-like dataset exercising every column kind: identity levels,
/// exploded, units, required fields, a typed array, a record, a nested list.
fn example() -> DatasetSchema {
    let pose = shape(vec![
        FieldDef::new("x_m", FieldType::F64).with_unit("m"),
        FieldDef::new("frame", FieldType::Str),
    ]);
    DatasetSchema::builder("telemetry")
        .level("site", 100)
        .level("robot", 101)
        .exploded(true)
        .field(
            FieldDef::new("soc_pct", FieldType::F32)
                .with_unit("%")
                .with_field_id(1000),
        )
        .field(
            FieldDef::new("seq", FieldType::U64)
                .required()
                .with_field_id(1001),
        )
        .field(
            FieldDef::new("motor_temp_c", list(FieldType::F32))
                .with_unit("Cel")
                .with_field_id(1002),
        )
        .field(FieldDef::new("pose", FieldType::Record(pose)).with_field_id(1003))
        .field(FieldDef::new("grid", list(list(FieldType::I32))).with_field_id(1004))
        .field(FieldDef::new("mode", FieldType::Str).with_field_id(1005))
        .field(FieldDef::new("captured", FieldType::Ts).with_field_id(1006))
        .build()
        .unwrap()
}

fn render(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "i8".into(),
        DataType::Int16 => "i16".into(),
        DataType::Int32 => "i32".into(),
        DataType::Int64 => "i64".into(),
        DataType::UInt8 => "u8".into(),
        DataType::UInt16 => "u16".into(),
        DataType::UInt32 => "u32".into(),
        DataType::UInt64 => "u64".into(),
        DataType::Float32 => "f32".into(),
        DataType::Float64 => "f64".into(),
        DataType::Utf8 => "utf8".into(),
        DataType::Binary => "binary".into(),
        DataType::FixedSizeBinary(n) => format!("fixed_binary({n})"),
        DataType::Timestamp(unit, tz) => {
            let unit = match unit {
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
                _ => "other",
            };
            format!("ts[{unit},{}]", tz.as_deref().unwrap_or("none"))
        }
        DataType::Dictionary(k, v) => format!("dict<{},{}>", render(k), render(v)),
        DataType::List(f) => format!("list<{}>", render(f.data_type())),
        DataType::Struct(_) => "struct".into(),
        other => format!("{other}"),
    }
}

fn layout(schema: &DatasetSchema) -> String {
    fn walk(out: &mut String, depth: usize, f: &Field) {
        let unit = f
            .metadata()
            .get("thadam.unit")
            .map_or(String::new(), |u| format!(" unit={u}"));
        out.push_str(&format!(
            "{}{:>8}  {}  {}{}{}\n",
            "  ".repeat(depth),
            field_id(f).unwrap(),
            f.name(),
            render(f.data_type()),
            if f.is_nullable() { "" } else { " NOT NULL" },
            unit,
        ));
        match f.data_type() {
            DataType::List(c) => walk(out, depth + 1, c),
            DataType::Struct(fs) => fs.iter().for_each(|c| walk(out, depth + 1, c)),
            _ => {}
        }
    }
    let arrow = schema.arrow_schema();
    let mut out = String::new();
    arrow.fields().iter().for_each(|f| walk(&mut out, 0, f));
    out
}

#[test]
fn fingerprint_is_stable() {
    insta::assert_snapshot!(example().fingerprint_hex());
}

#[test]
fn column_layout_is_stable() {
    insta::assert_snapshot!(layout(&example()));
}

#[test]
fn fingerprint_is_deterministic_and_matches_its_hex() {
    let (a, b) = (example(), example());
    assert_eq!(a.fingerprint(), b.fingerprint());
    assert_eq!(a.fingerprint_hex().len(), 32);
    let hex: String = a.fingerprint().iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, a.fingerprint_hex());
}

fn with(
    f: impl FnOnce(thadam_arrow::DatasetSchemaBuilder) -> thadam_arrow::DatasetSchemaBuilder,
) -> [u8; 16] {
    let base = DatasetSchema::builder("d").level("robot", 100).field(
        FieldDef::new("v", FieldType::F32)
            .with_unit("Cel")
            .with_field_id(1000),
    );
    f(base).build().unwrap().fingerprint()
}

#[test]
fn fingerprint_ignores_the_dataset_name_only() {
    let base = with(|b| b);
    let renamed = DatasetSchema::builder("other")
        .level("robot", 100)
        .field(
            FieldDef::new("v", FieldType::F32)
                .with_unit("Cel")
                .with_field_id(1000),
        )
        .build()
        .unwrap()
        .fingerprint();
    assert_eq!(base, renamed);
}

#[test]
fn fingerprint_changes_with_every_schema_property() {
    let rebuild = |level: (&str, u32), field: FieldDef, opts: ArrowOptions, exploded: bool| {
        DatasetSchema::builder("d")
            .level(level.0, level.1)
            .field(field)
            .exploded(exploded)
            .options(opts)
            .build()
            .unwrap()
            .fingerprint()
    };
    let field = || {
        FieldDef::new("v", FieldType::F32)
            .with_unit("Cel")
            .with_field_id(1000)
    };
    let o = ArrowOptions::default();
    let base = rebuild(("robot", 100), field(), o, false);
    assert_eq!(base, with(|b| b));

    let variants = [
        ("level name", rebuild(("site", 100), field(), o, false)),
        ("level id", rebuild(("robot", 101), field(), o, false)),
        (
            "field name",
            rebuild(
                ("robot", 100),
                FieldDef::new("w", FieldType::F32)
                    .with_unit("Cel")
                    .with_field_id(1000),
                o,
                false,
            ),
        ),
        (
            "field type",
            rebuild(
                ("robot", 100),
                FieldDef::new("v", FieldType::F64)
                    .with_unit("Cel")
                    .with_field_id(1000),
                o,
                false,
            ),
        ),
        (
            "field id",
            rebuild(("robot", 100), field().with_field_id(1001), o, false),
        ),
        (
            "field unit",
            rebuild(
                ("robot", 100),
                FieldDef::new("v", FieldType::F32)
                    .with_unit("K")
                    .with_field_id(1000),
                o,
                false,
            ),
        ),
        (
            "no unit",
            rebuild(
                ("robot", 100),
                FieldDef::new("v", FieldType::F32).with_field_id(1000),
                o,
                false,
            ),
        ),
        (
            "required",
            rebuild(("robot", 100), field().required(), o, false),
        ),
        (
            "list",
            rebuild(
                ("robot", 100),
                FieldDef::new("v", list(FieldType::F32))
                    .with_unit("Cel")
                    .with_field_id(1000),
                o,
                false,
            ),
        ),
        (
            "ts unit",
            rebuild(
                ("robot", 100),
                field(),
                o.with_ts_unit(TsUnit::Nanos),
                false,
            ),
        ),
        (
            "unsigned",
            rebuild(
                ("robot", 100),
                field(),
                o.with_unsigned(UnsignedPolicy::Signed),
                false,
            ),
        ),
        (
            "lineage",
            rebuild(
                ("robot", 100),
                field(),
                o.with_lineage_columns(false),
                false,
            ),
        ),
        ("exploded", rebuild(("robot", 100), field(), o, true)),
    ];
    let mut seen = std::collections::HashSet::from([base]);
    for (what, fp) in variants {
        assert_ne!(fp, base, "{what} must change the fingerprint");
        assert!(seen.insert(fp), "{what} collides with another variant");
    }
}

#[test]
fn column_order_changes_the_fingerprint() {
    let build = |reverse: bool| {
        let mut fields = vec![
            FieldDef::new("a", FieldType::I32).with_field_id(1000),
            FieldDef::new("b", FieldType::I32).with_field_id(1001),
        ];
        if reverse {
            fields.reverse();
        }
        DatasetSchema::builder("d")
            .fields(fields)
            .build()
            .unwrap()
            .fingerprint()
    };
    assert_ne!(build(false), build(true));
}

#[test]
fn nested_shape_changes_change_the_fingerprint() {
    let make = |extra: bool| {
        let mut fs = vec![FieldDef::new("x", FieldType::F64)];
        if extra {
            fs.push(FieldDef::new("y", FieldType::F64));
        }
        DatasetSchema::builder("d")
            .field(FieldDef::new("p", FieldType::Record(shape(fs))).with_field_id(1000))
            .build()
            .unwrap()
            .fingerprint()
    };
    assert_ne!(make(false), make(true));
}
