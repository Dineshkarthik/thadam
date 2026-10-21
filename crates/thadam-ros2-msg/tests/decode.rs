//! Golden CDR decode tests against real ROS 2 message definitions, plus
//! endianness, array-bound, malformed-payload, depth, and registry tests.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;
use thadam_core::{FieldType, ShapeRef, Timestamp, Value};
use thadam_ros2_msg::{CompiledMessage, Registry};

use crate::common::{
    Enc, POSE_MSG, POSE_STAMPED_MSG, STAMP_NANOS, STAMP_NANOSEC, STAMP_SEC, TIME_MSG,
    bounded_payload, full_registry, header_payload, imu_payload, joint_state_empty_payload,
    joint_state_payload, pose_stamped_payload,
};

fn registry() -> Registry {
    full_registry()
}

fn compile(registry: &mut Registry, type_name: &str) -> Arc<CompiledMessage> {
    registry.compile(type_name).unwrap()
}

fn decode(compiled: &CompiledMessage, payload: &[u8]) -> thadam_core::Record {
    compiled.decode(payload).unwrap()
}

fn field_kinds(shape: &ShapeRef) -> Vec<(&str, FieldType)> {
    shape
        .fields()
        .iter()
        .map(|f| (&*f.name, f.ty.clone()))
        .collect()
}

#[test]
fn golden_header_decodes_little_and_big_endian_identically() {
    let mut registry = registry();
    let header = compile(&mut registry, "std_msgs/Header");

    let le = decode(&header, &header_payload(true));
    let be = decode(&header, &header_payload(false));
    assert_eq!(le, be, "LE and BE encodings decode to the same record");

    assert_eq!(le.get("stamp.sec"), Some(&Value::I32(STAMP_SEC)));
    assert_eq!(le.get("stamp.nanosec"), Some(&Value::U32(STAMP_NANOSEC)));
    assert_eq!(
        le.get("stamp").and_then(Value::as_ts),
        Some(Timestamp::from_nanos(STAMP_NANOS)),
        "builtin_interfaces/Time adds a Timestamp field next to sec/nanosec"
    );
    assert_eq!(
        le.get("frame_id").and_then(Value::as_str),
        Some("base_link")
    );

    assert_eq!(
        field_kinds(header.shape()),
        vec![
            ("stamp.sec", FieldType::I32),
            ("stamp.nanosec", FieldType::U32),
            ("stamp", FieldType::Ts),
            ("frame_id", FieldType::Str),
        ]
    );
}

#[test]
fn golden_pose_stamped_flattens_nested_scalars() {
    let mut registry = registry();
    let stamped = compile(&mut registry, "geometry_msgs/PoseStamped");
    let record = decode(&stamped, &pose_stamped_payload(true));

    assert_eq!(
        field_kinds(stamped.shape()),
        vec![
            ("header.stamp.sec", FieldType::I32),
            ("header.stamp.nanosec", FieldType::U32),
            ("header.stamp", FieldType::Ts),
            ("header.frame_id", FieldType::Str),
            ("pose.position.x", FieldType::F64),
            ("pose.position.y", FieldType::F64),
            ("pose.position.z", FieldType::F64),
            ("pose.orientation.x", FieldType::F64),
            ("pose.orientation.y", FieldType::F64),
            ("pose.orientation.z", FieldType::F64),
            ("pose.orientation.w", FieldType::F64),
        ]
    );
    assert_eq!(record.get("pose.position.x"), Some(&Value::F64(1.25)));
    assert_eq!(record.get("pose.position.y"), Some(&Value::F64(-2.5)));
    assert_eq!(record.get("pose.position.z"), Some(&Value::F64(3.75)));
    assert_eq!(record.get("pose.orientation.w"), Some(&Value::F64(1.0)));
    assert_eq!(
        record.get("header.stamp").and_then(Value::as_ts),
        Some(Timestamp::from_nanos(STAMP_NANOS))
    );
    assert_eq!(
        record.get("header.frame_id").and_then(Value::as_str),
        Some("map")
    );

    // The same bytes in big-endian produce the same record.
    let be = decode(&stamped, &pose_stamped_payload(false));
    assert_eq!(record, be);
}

#[test]
fn golden_joint_state_decodes_lists_and_typed_arrays() {
    let mut registry = registry();
    let joints = compile(&mut registry, "sensor_msgs/JointState");
    let record = decode(&joints, &joint_state_payload(true));

    assert_eq!(
        field_kinds(joints.shape()),
        vec![
            ("header.stamp.sec", FieldType::I32),
            ("header.stamp.nanosec", FieldType::U32),
            ("header.stamp", FieldType::Ts),
            ("header.frame_id", FieldType::Str),
            ("name", FieldType::List(Box::new(FieldType::Str))),
            ("position", FieldType::List(Box::new(FieldType::F64))),
            ("velocity", FieldType::List(Box::new(FieldType::F64))),
            ("effort", FieldType::List(Box::new(FieldType::F64))),
        ]
    );

    let names: Vec<&str> = record
        .get("name")
        .and_then(Value::as_list)
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(names, ["hip", "knee", "ankle"]);
    assert_eq!(
        record.get("position").and_then(Value::as_f64s),
        Some(&[0.1, -0.2, 0.3][..])
    );
    assert_eq!(
        record.get("velocity").and_then(Value::as_f64s),
        Some(&[1.5, -1.5][..])
    );
    assert_eq!(
        record.get("effort").and_then(Value::as_f64s),
        Some(&[9.8][..])
    );
    assert_eq!(
        record.get("header.frame_id").and_then(Value::as_str),
        Some(""),
        "the empty CDR string is length 1 (NUL only)"
    );

    let be = decode(&joints, &joint_state_payload(false));
    assert_eq!(record, be);
}

#[test]
fn empty_sequences_decode_as_empty_containers() {
    let mut registry = registry();
    let joints = compile(&mut registry, "sensor_msgs/JointState");
    let record = decode(&joints, &joint_state_empty_payload());
    let f64s = |name: &str| record.get(name).and_then(Value::as_f64s).unwrap().len();
    assert_eq!(
        (f64s("position"), f64s("velocity"), f64s("effort")),
        (0, 0, 0)
    );
    assert_eq!(
        record.get("name").and_then(Value::as_list).map(<[_]>::len),
        Some(0)
    );
}

#[test]
fn golden_imu_decodes_fixed_arrays_and_flat_vectors() {
    let mut registry = registry();
    let imu = compile(&mut registry, "sensor_msgs/Imu");
    let record = decode(&imu, &imu_payload(true));

    assert_eq!(
        field_kinds(imu.shape()),
        vec![
            ("header.stamp.sec", FieldType::I32),
            ("header.stamp.nanosec", FieldType::U32),
            ("header.stamp", FieldType::Ts),
            ("header.frame_id", FieldType::Str),
            ("orientation.x", FieldType::F64),
            ("orientation.y", FieldType::F64),
            ("orientation.z", FieldType::F64),
            ("orientation.w", FieldType::F64),
            (
                "orientation_covariance",
                FieldType::List(Box::new(FieldType::F64)),
            ),
            ("angular_velocity.x", FieldType::F64),
            ("angular_velocity.y", FieldType::F64),
            ("angular_velocity.z", FieldType::F64),
            (
                "angular_velocity_covariance",
                FieldType::List(Box::new(FieldType::F64)),
            ),
            ("linear_acceleration.x", FieldType::F64),
            ("linear_acceleration.y", FieldType::F64),
            ("linear_acceleration.z", FieldType::F64),
            (
                "linear_acceleration_covariance",
                FieldType::List(Box::new(FieldType::F64)),
            ),
        ]
    );

    let orientation_covariance = record
        .get("orientation_covariance")
        .and_then(Value::as_f64s)
        .unwrap();
    assert_eq!(orientation_covariance.len(), 9);
    assert_eq!(
        orientation_covariance[0], -1.0,
        "the ROS 'no estimate' marker"
    );

    assert_eq!(record.get("orientation.w"), Some(&Value::F64(1.0)));
    assert_eq!(record.get("angular_velocity.y"), Some(&Value::F64(0.02)));
    assert_eq!(record.get("angular_velocity.z"), Some(&Value::F64(-0.03)));
    let angular_covariance = record
        .get("angular_velocity_covariance")
        .and_then(Value::as_f64s)
        .unwrap();
    let expected_covariance: Vec<f64> = (0..9).map(|i| f64::from(i) * 0.125).collect();
    assert_eq!(angular_covariance, expected_covariance.as_slice());
    assert_eq!(record.get("linear_acceleration.z"), Some(&Value::F64(9.81)));
    assert_eq!(
        record.get("header.frame_id").and_then(Value::as_str),
        Some("imu_link")
    );

    let be = decode(&imu, &imu_payload(false));
    assert_eq!(record, be);
}

#[test]
fn array_bound_forms_and_widening_decode() {
    let mut registry = registry();
    let bounded = compile(&mut registry, "test_msgs/Bounded");
    let record = decode(&bounded, &bounded_payload());

    assert_eq!(
        record.get("coeffs").and_then(Value::as_f32s),
        Some(&[1.5, -2.5, 3.5][..]),
        "float32[<=4] -> F32s"
    );
    assert_eq!(
        record.get("counts").and_then(Value::as_i32s),
        Some(&[7, -8][..]),
        "int32[] -> I32s"
    );
    assert_eq!(
        record.get("label").and_then(Value::as_str),
        Some("ok"),
        "string<=16 -> Str"
    );
    assert_eq!(
        record.get("blob").and_then(Value::as_bytes).map(|b| &b[..]),
        Some(&[0xde, 0xad, 0xbe, 0xef][..]),
        "uint8[] -> Bytes"
    );
    let flags: Vec<bool> = record
        .get("flags")
        .and_then(Value::as_list)
        .unwrap()
        .iter()
        .map(|v| v.as_bool().unwrap())
        .collect();
    assert_eq!(flags, [true, false], "bool[] -> List");
    let ids: Vec<u64> = record
        .get("ids")
        .and_then(Value::as_list)
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(
        ids,
        [1, 2, 3],
        "uint64[3] -> List (fixed, no length prefix)"
    );
    assert_eq!(
        field_kinds(bounded.shape())[5].1,
        FieldType::List(Box::new(FieldType::U64)),
        "uint64 has no lossless typed array"
    );

    // Arrays of nested messages are Lists of Records.
    let polyline = record.get("polyline").and_then(Value::as_list).unwrap();
    assert_eq!(polyline.len(), 1);
    let point = polyline[0].as_record().unwrap();
    assert_eq!(point.get("z"), Some(&Value::F64(3.0)));
    let vertices = record.get("vertices").and_then(Value::as_list).unwrap();
    assert_eq!(vertices.len(), 2);
    assert_eq!(
        vertices[1].as_record().unwrap().get("y"),
        Some(&Value::F64(8.0))
    );

    assert_eq!(
        record.get("gains").and_then(Value::as_i32s),
        Some(&[100, 200][..]),
        "uint16[2] widens losslessly into I32s"
    );
}

#[test]
fn uuid_fixed_byte_array_is_bytes() {
    let mut registry = registry();
    let uuid = compile(&mut registry, "unique_identifier_msgs/UUID");
    let mut enc = Enc::le();
    for b in 0u8..16 {
        enc.u8(b);
    }
    let record = decode(&uuid, &enc.finish());
    assert_eq!(
        field_kinds(uuid.shape()),
        vec![("uuid", FieldType::Bytes)],
        "uint8[16] -> Bytes"
    );
    let bytes: Vec<u8> = record.get("uuid").and_then(Value::as_bytes).unwrap()[..].to_vec();
    assert_eq!(bytes, (0u8..16).collect::<Vec<u8>>());
}

#[test]
fn constants_are_kept_but_never_encoded() {
    let mut registry = registry();
    let status = compile(&mut registry, "sensor_msgs/NavSatStatus");
    assert_eq!(
        field_kinds(status.shape()),
        vec![("status", FieldType::I8), ("service", FieldType::U16)]
    );

    let mut enc = Enc::le();
    enc.u8((-1i8) as u8).u16(5);
    let record = decode(&status, &enc.finish());
    assert_eq!(record.get("status"), Some(&Value::I8(-1)));
    assert_eq!(record.get("service"), Some(&Value::U16(5)));

    let spec = registry.spec("sensor_msgs/NavSatStatus").unwrap();
    assert_eq!(
        spec.constants().len(),
        8,
        "constants stay on the definition"
    );

    // Defaults are parsed too, and also never encoded.
    let quaternion = registry.spec("geometry_msgs/Quaternion").unwrap();
    let defaults: Vec<&str> = quaternion
        .fields()
        .iter()
        .map(|f| f.default.as_deref().unwrap())
        .collect();
    assert_eq!(defaults, ["0", "0", "0", "1"]);
}

#[test]
fn decoding_from_a_bytes_view_works() {
    let mut registry = registry();
    let header = compile(&mut registry, "std_msgs/Header");
    let payload = bytes::Bytes::from(header_payload(true));
    let record = header.decode(payload).unwrap();
    assert_eq!(
        record.get("frame_id").and_then(Value::as_str),
        Some("base_link")
    );
}

#[test]
fn wstring_decodes_with_or_without_trailing_nul() {
    let mut registry = registry();
    registry
        .register(
            "test_msgs",
            "Wide",
            "wstring label\nwstring plain\nwstring<=4 bounded\n",
        )
        .unwrap();
    let wide = compile(&mut registry, "test_msgs/Wide");

    let mut enc = Enc::le();
    enc.u32(6); // "héllo" is 5 UTF-16 units plus a NUL
    for unit in "héllo".encode_utf16() {
        enc.u16(unit);
    }
    enc.u16(0);
    enc.u32(2); // "ab", no NUL
    for unit in "ab".encode_utf16() {
        enc.u16(unit);
    }
    enc.u32(2); // "xy", within the <=4 bound
    for unit in "xy".encode_utf16() {
        enc.u16(unit);
    }
    let record = decode(&wide, &enc.finish());
    assert_eq!(record.get("label").and_then(Value::as_str), Some("héllo"));
    assert_eq!(record.get("plain").and_then(Value::as_str), Some("ab"));
    assert_eq!(record.get("bounded").and_then(Value::as_str), Some("xy"));

    // An unpaired surrogate is an error, never a panic.
    let mut bad = Enc::le();
    bad.u32(1)
        .u16(0xD800)
        .u32(2)
        .u16(b'a' as u16)
        .u16(b'b' as u16);
    let err = wide.decode(bad.finish()).err().unwrap();
    assert_eq!(err.code, "decode.invalid_utf8");
}

#[test]
fn truncated_prefixes_always_error_and_never_panic() {
    let mut registry = registry();
    let cases: Vec<(Arc<CompiledMessage>, Vec<u8>)> = vec![
        (
            compile(&mut registry, "sensor_msgs/JointState"),
            joint_state_payload(true),
        ),
        (compile(&mut registry, "sensor_msgs/Imu"), imu_payload(true)),
        (
            compile(&mut registry, "std_msgs/Header"),
            header_payload(false),
        ),
        (
            compile(&mut registry, "test_msgs/Bounded"),
            bounded_payload(),
        ),
    ];
    for (compiled, payload) in cases {
        for cut in 0..payload.len() {
            assert!(
                compiled.decode(&payload[..cut]).is_err(),
                "{} must not decode a {cut}-byte prefix",
                compiled.type_name()
            );
        }
        assert!(compiled.decode(payload).is_ok(), "the full payload decodes");
    }
}

#[test]
fn malformed_payloads_fail_with_decode_domain_errors() {
    let mut registry = registry();
    let header = compile(&mut registry, "std_msgs/Header");

    // Encapsulation header too short.
    for cut in 0..4 {
        let err = header.decode(&header_payload(true)[..cut]).err().unwrap();
        assert_eq!(err.code, "decode.invalid_payload");
    }

    // Unknown / XCDR2 encapsulation schemes.
    let mut xcdr2 = header_payload(true);
    xcdr2[1] = 0x02;
    let err = header.decode(xcdr2).err().unwrap();
    assert_eq!(err.code, "decode.unsupported_encoding");

    // A string without its NUL terminator.
    let mut missing_nul = Enc::le();
    missing_nul.i32(STAMP_SEC).u32(STAMP_NANOSEC);
    missing_nul.u32(4).bytes(b"abc!");
    let err = header.decode(missing_nul.finish()).err().unwrap();
    assert_eq!(err.code, "decode.invalid_payload");

    // Invalid UTF-8 inside a string.
    let mut bad_utf8 = Enc::le();
    bad_utf8.i32(STAMP_SEC).u32(STAMP_NANOSEC);
    bad_utf8.u32(2).bytes(&[0xFF, 0x00]);
    let err = header.decode(bad_utf8.finish()).err().unwrap();
    assert_eq!(err.code, "decode.invalid_utf8");

    // A declared sequence length that cannot fit in the payload.
    let joints = compile(&mut registry, "sensor_msgs/JointState");
    let mut huge = Enc::le();
    huge.header("x");
    huge.u32(0); // empty name
    huge.u32(u32::MAX); // position[]
    let err = joints.decode(huge.finish()).err().unwrap();
    assert_eq!(err.code, "decode.invalid_payload");
}

#[test]
fn declared_bounds_are_enforced() {
    let mut registry = registry();
    let bounded = compile(&mut registry, "test_msgs/Bounded");

    // Sequence longer than its <=4 bound.
    let mut over = Enc::le();
    over.u32(5);
    for _ in 0..5 {
        over.f32(0.0);
    }
    let err = bounded.decode(over.finish()).err().unwrap();
    assert_eq!(err.code, "decode.invalid_payload");

    // String longer than its <=16 bound.
    let mut wide_label = Enc::le();
    wide_label.u32(0); // coeffs
    wide_label.u32(0); // counts
    wide_label.string("abcdefghijklmnopq"); // 17 > 16
    let err = bounded.decode(wide_label.finish()).err().unwrap();
    assert_eq!(err.code, "decode.invalid_payload");
}

#[test]
fn sequences_of_empty_messages_are_capped_not_unbounded() {
    let mut registry = registry();
    registry
        .register("test_msgs", "Ghosts", "test_msgs/Empty[] ghosts\n")
        .unwrap();
    let ghosts = compile(&mut registry, "test_msgs/Ghosts");

    let mut ok = Enc::le();
    ok.u32(5);
    assert_eq!(
        decode(&ghosts, &ok.finish())
            .get("ghosts")
            .and_then(Value::as_list)
            .map(<[_]>::len),
        Some(5)
    );

    let mut hostile = Enc::le();
    hostile.u32(100_000);
    let err = ghosts.decode(hostile.finish()).err().unwrap();
    assert_eq!(err.code, "decode.too_large");
}

#[test]
fn nested_cycles_and_deep_nesting_are_rejected_at_compile_time() {
    let mut registry = Registry::new();
    registry
        .register("test_msgs", "A", "test_msgs/B b\n")
        .unwrap();
    registry
        .register("test_msgs", "B", "test_msgs/A a\n")
        .unwrap();
    let err = registry.compile("test_msgs/A").err().unwrap();
    assert_eq!(err.code, "schema.invalid_shape");
    assert!(err.message.contains("cycle"), "{err}");

    let mut deep = Registry::new();
    let levels = thadam_ros2_msg::MAX_MESSAGE_DEPTH + 2;
    for i in 0..levels {
        let body = if i + 1 == levels {
            "int32 leaf\n".to_owned()
        } else {
            format!("test_msgs/M{} child\n", i + 1)
        };
        deep.register("test_msgs", &format!("M{i}"), &body).unwrap();
    }
    let err = deep.compile("test_msgs/M0").err().unwrap();
    assert_eq!(err.code, "schema.invalid_shape");

    let err = registry.compile("test_msgs/Missing").err().unwrap();
    assert_eq!(err.code, "schema.unsupported_type");
}

#[test]
fn re_registering_a_type_invalidates_the_compiled_cache() {
    let mut registry = Registry::new();
    registry.register("a_msgs", "Foo", "int32 x\n").unwrap();
    let first = registry.compile("a_msgs/Foo").unwrap();
    assert_eq!(first.shape().len(), 1);

    registry
        .register("a_msgs", "Foo", "int32 x\nint32 y\n")
        .unwrap();
    let second = registry.compile("a_msgs/Foo").unwrap();
    assert_eq!(second.shape().len(), 2);
    assert_ne!(first.shape().fingerprint(), second.shape().fingerprint());
}

#[test]
fn ros2msg_concatenated_document_registers_and_decodes() {
    let sep = "=".repeat(80);
    let text = format!(
        "{POSE_STAMPED_MSG}{sep}\nMSG: std_msgs/Header\n{}\n{sep}\nMSG: geometry_msgs/msg/Pose\n{POSE_MSG}{sep}\nMSG: geometry_msgs/Point\n{}\n{sep}\nMSG: geometry_msgs/Quaternion\n{}\n{sep}\nMSG: builtin_interfaces/Time\n{TIME_MSG}",
        crate::common::HEADER_MSG,
        crate::common::POINT_MSG,
        crate::common::QUATERNION_MSG,
    );

    let mut registry = Registry::new();
    let main = registry
        .register_ros2msg("geometry_msgs/PoseStamped", &text)
        .unwrap();
    assert_eq!(main.to_string(), "geometry_msgs/PoseStamped");
    assert!(registry.spec("geometry_msgs/Point").is_some());
    assert!(registry.spec("builtin_interfaces/Time").is_some());

    let stamped = registry.compile("geometry_msgs/msg/PoseStamped").unwrap();
    let record = decode(&stamped, &pose_stamped_payload(true));
    assert_eq!(record.get("pose.position.x"), Some(&Value::F64(1.25)));

    // A ros2msg document containing declarations of its own fields works for
    // constants too (constants on nested sections survive).
    let sep = "=".repeat(80);
    let with_time =
        format!("builtin_interfaces/Time stamp\n{sep}\nMSG: builtin_interfaces/Time\n{TIME_MSG}");
    let mut other = Registry::new();
    other
        .register_ros2msg("std_msgs/Header", &with_time)
        .unwrap();
    let header = other.compile("std_msgs/Header").unwrap();
    assert_eq!(
        decode(&header, &header_payload(true))
            .get("stamp")
            .and_then(Value::as_ts),
        Some(Timestamp::from_nanos(STAMP_NANOS))
    );
}
