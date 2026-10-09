//! `.msg` parser tests: primitives, arrays, bounded strings, nested types,
//! constants, defaults, comments, and the multi-definition `ros2msg` format.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use thadam_ros2_msg::msg::{
    ArrayBound, ConstantValue, Primitive, ScalarType, SchemaDocument, TypeKind, TypeName,
};

use crate::common::NAVSAT_STATUS_MSG;

fn tn() -> TypeName {
    TypeName::new("test_msgs", "Demo").unwrap()
}

fn parse(text: &str) -> thadam_ros2_msg::msg::MessageSpec {
    thadam_ros2_msg::msg::MessageSpec::parse(&tn(), text).unwrap()
}

#[test]
fn parses_every_primitive_field_type() {
    let text = "\
bool b
byte by
char c
int8 i8
uint8 u8
int16 i16
uint16 u16
int32 i32
uint32 u32
int64 i64
uint64 u64
float32 f32
float64 f64
string s
wstring ws
string<=16 bounded_s
wstring<=32 bounded_ws
";
    let spec = parse(text);
    assert_eq!(spec.fields().len(), 17);
    let prims: Vec<(Primitive, Option<u32>, bool)> = spec
        .fields()
        .iter()
        .map(|f| match &f.ty {
            TypeKind::Scalar(ScalarType::Primitive { prim, string_bound }) => {
                (*prim, *string_bound, f.default.is_some())
            }
            other => panic!("expected scalar primitive, got {other:?}"),
        })
        .collect();
    let expected = [
        (Primitive::Bool, None),
        (Primitive::Byte, None),
        (Primitive::Char, None),
        (Primitive::I8, None),
        (Primitive::U8, None),
        (Primitive::I16, None),
        (Primitive::U16, None),
        (Primitive::I32, None),
        (Primitive::U32, None),
        (Primitive::I64, None),
        (Primitive::U64, None),
        (Primitive::F32, None),
        (Primitive::F64, None),
        (Primitive::String, None),
        (Primitive::WString, None),
        (Primitive::String, Some(16)),
        (Primitive::WString, Some(32)),
    ];
    assert_eq!(prims.len(), expected.len());
    for (got, want) in prims.iter().zip(expected.iter()) {
        assert_eq!((got.0, got.1), *want);
        assert!(!got.2);
    }
}

#[test]
fn parses_array_bound_forms() {
    let spec = parse("int32[] a\nint32[<=10] b\nint32[4] c\nstring[] d\nbool[3] e\n");
    let bounds = [
        ArrayBound::Unbounded,
        ArrayBound::Bounded(10),
        ArrayBound::Fixed(4),
        ArrayBound::Unbounded,
        ArrayBound::Fixed(3),
    ];
    for (field, expected) in spec.fields().iter().zip(bounds.iter()) {
        let TypeKind::Array { bound, .. } = field.ty else {
            panic!("field {} must be an array", field.name);
        };
        assert_eq!(&bound, expected, "field {}", field.name);
    }
}

#[test]
fn parses_nested_type_references() {
    let spec = parse("Other a\npkg/Other b\npkg/msg/Other c\n");
    let packages: Vec<Option<String>> = spec
        .fields()
        .iter()
        .map(|f| match &f.ty {
            TypeKind::Scalar(ScalarType::Named(n)) => n.package.clone(),
            other => panic!("expected named type, got {other:?}"),
        })
        .collect();
    assert_eq!(
        packages,
        vec![None, Some("pkg".to_owned()), Some("pkg".to_owned()),],
        "a bare name stays package-relative; pkg/msg/Type normalizes"
    );
}

#[test]
fn parses_constants_and_keeps_them() {
    let spec = parse(
        "\
uint8 A=1
int8 B = -2       # spaces around '=' are also constants
uint32 HEX=0xff
float64 PI=3.25
bool YES=true
bool NO=0
string GREETING=hello world
string QUOTED=\"spaced out \"
int32 FIELD 1
",
    );
    assert_eq!(spec.fields().len(), 1, "the int32 with `1` has a default");
    assert_eq!(spec.constants().len(), 8);
    let get = |name: &str| {
        spec.constants()
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.value.clone())
            .unwrap()
    };
    assert_eq!(get("A"), ConstantValue::U64(1));
    assert_eq!(get("B"), ConstantValue::I64(-2));
    assert_eq!(get("HEX"), ConstantValue::U64(255));
    assert_eq!(get("PI"), ConstantValue::F64(3.25));
    assert_eq!(get("YES"), ConstantValue::Bool(true));
    assert_eq!(get("NO"), ConstantValue::Bool(false));
    assert_eq!(get("GREETING"), ConstantValue::Str("hello world".into()));
    assert_eq!(get("QUOTED"), ConstantValue::Str("spaced out ".into()));
}

#[test]
fn constants_of_real_navsat_status_parse() {
    let tn = TypeName::new("sensor_msgs", "NavSatStatus").unwrap();
    let spec = thadam_ros2_msg::msg::MessageSpec::parse(&tn, NAVSAT_STATUS_MSG).unwrap();
    assert_eq!(spec.constants().len(), 8);
    let service: Vec<u64> = spec
        .constants()
        .iter()
        .filter(|c| c.name.starts_with("SERVICE_"))
        .filter_map(|c| match c.value {
            ConstantValue::U64(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(service, vec![1, 2, 4, 8]);
    let no_fix = spec
        .constants()
        .iter()
        .find(|c| c.name == "STATUS_NO_FIX")
        .unwrap();
    assert_eq!(no_fix.value, ConstantValue::I64(-1));
    assert_eq!(
        spec.fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["status", "service"]
    );
}

#[test]
fn parses_defaults_but_ignores_equals_form_as_constant() {
    let spec = parse("float64 x 0\nfloat64 w 1\nstring s \"hello world\"\nuint8 MODE=3\n");
    assert_eq!(spec.fields().len(), 3);
    assert_eq!(
        spec.constants().len(),
        1,
        "MODE is a constant, not a default"
    );
    assert_eq!(spec.fields()[0].default.as_deref(), Some("0"));
    assert_eq!(spec.fields()[1].default.as_deref(), Some("1"));
    assert_eq!(
        spec.fields()[2].default.as_deref(),
        Some("\"hello world\""),
        "defaults keep their raw (quoted) text"
    );
}

#[test]
fn strips_inline_and_full_line_comments() {
    let spec = parse("# leading comment\nint32 x # trailing\n  # indented\nfloat64 y\n");
    assert_eq!(
        spec.fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["x", "y"]
    );
}

#[test]
fn rejects_invalid_definitions() {
    let bad = [
        "int32 2fast",            // name starts with a digit
        "int32 x-y",              // invalid identifier character
        "int32<=4 x",             // <= on a non-string type
        "int32[0] x",             // zero fixed size
        "int32[<=0] x",           // zero bound
        "string<=0 s",            // zero string bound
        "int32[abc] x",           // non-numeric bound
        "int32[4 x",              // unbalanced bracket
        "unknown pkg/T x",        // garbage type
        "uint8 OUT_OF_RANGE=256", // constant overflow
        "bool MAYBE=2",           // bad bool constant
        "int32 x\nfloat64 x",     // duplicate field
        "uint8 A=1\nuint16 A=2",  // duplicate constant
        "uint8 A=1\nint32 A",     // constant and field share a name
    ];
    for text in bad {
        assert!(
            thadam_ros2_msg::msg::MessageSpec::parse(&tn(), text).is_err(),
            "{text:?} must be rejected"
        );
    }
}

#[test]
fn type_name_forms() {
    let t = TypeName::parse("sensor_msgs/JointState").unwrap();
    assert_eq!(t.to_string(), "sensor_msgs/JointState");
    let t2 = TypeName::parse("sensor_msgs/msg/JointState").unwrap();
    assert_eq!(t, t2);
    assert!(TypeName::parse("JointState").is_err(), "needs a package");
    assert!(TypeName::parse("sensor_msgs//JointState").is_err());
}

#[test]
fn schema_document_parses_single_and_multi_definition_texts() {
    let single =
        SchemaDocument::parse(&TypeName::new("test_msgs", "Demo").unwrap(), "int32 x\n").unwrap();
    assert_eq!(single.sections().len(), 1);
    assert_eq!(single.main_type().to_string(), "test_msgs/Demo");

    let sep = "=".repeat(80);
    let text = format!(
        "std_msgs/Header header\n{sep}\nMSG: std_msgs/Header\nbuiltin_interfaces/Time stamp\nstring frame_id\n{sep}\nMSG: builtin_interfaces/msg/Time\nint32 sec\nuint32 nanosec\n"
    );
    let doc = SchemaDocument::parse(
        &TypeName::new("geometry_msgs", "PoseStamped").unwrap(),
        &text,
    )
    .unwrap();
    assert_eq!(doc.sections().len(), 3);
    let names: Vec<String> = doc.sections().iter().map(|(t, _)| t.to_string()).collect();
    assert_eq!(
        names,
        [
            "geometry_msgs/PoseStamped",
            "std_msgs/Header",
            "builtin_interfaces/Time"
        ]
    );
    assert_eq!(doc.sections()[1].1.fields().len(), 2);
}

#[test]
fn schema_document_rejects_bad_layouts() {
    let sep = "=".repeat(80);
    let main = TypeName::new("test_msgs", "Demo").unwrap();

    // Dangling separator.
    let text = format!("int32 x\n{sep}\n");
    assert!(SchemaDocument::parse(&main, &text).is_err());

    // Separator without an MSG: header.
    let text = format!("int32 x\n{sep}\nint32 y\n");
    assert!(SchemaDocument::parse(&main, &text).is_err());

    // The same type defined twice.
    let text = format!(
        "int32 x\n{sep}\nMSG: test_msgs/Other\nint32 y\n{sep}\nMSG: test_msgs/Other\nint32 z\n"
    );
    assert!(SchemaDocument::parse(&main, &text).is_err());

    // An invalid MSG: header.
    let text = format!("int32 x\n{sep}\nMSG: ");
    assert!(SchemaDocument::parse(&main, &text).is_err());
}
