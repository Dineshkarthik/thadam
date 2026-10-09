//! The `decode_ros2` helper: one `ros2msg` schema plus one CDR payload in,
//! one Thadam [`Record`] out.
//!
//! The MCAP source itself never decodes payloads — decoding belongs to the
//! transform chain (ADR-0007 stage 2 keeps the source transport-shaped).
//! This helper is what a future decode transform calls per message, given
//! the schema metadata the source already carries as `mcap.*` attributes.
//!
//! # Why the schema name is a parameter
//!
//! A `ros2msg` schema document does not name its main type (the `MSG:`
//! sections name only the nested types), and [`thadam_ros2_msg`] keys types
//! by name. MCAP schema records always carry a name (`mcap.schema_name`,
//! either `pkg/Type` or the `pkg/msg/Type` spelling), so callers pass it
//! through.
//!
//! # Cost
//!
//! Every call builds a fresh registry. That is right for a per-message
//! helper with unknown cardinality of schemas; a transform that sees a
//! bounded set of schemas should hold one [`thadam_ros2_msg::Registry`] and
//! call `compile` itself, since compiled messages are cached there.

use thadam_core::{Record, ThadamError};

/// Decodes `payload` against the `ros2msg` schema document `schema_text`
/// whose main type is `schema_name`.
///
/// Errors come straight from [`thadam_ros2_msg`]: `schema.*` for definition
/// problems, `decode.*` for payload problems. Messages never contain payload
/// text.
pub fn decode_ros2(
    schema_name: &str,
    schema_text: &str,
    payload: impl AsRef<[u8]>,
) -> Result<Record, ThadamError> {
    let mut registry = thadam_ros2_msg::Registry::new();
    let main = registry.register_ros2msg(schema_name, schema_text)?;
    let compiled = registry.compile(&main.to_string())?;
    compiled.decode(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::Value;

    const TIME_MSG: &str = "int32 sec\nuint32 nanosec\n";
    const HEADER_MSG: &str = "builtin_interfaces/Time stamp\nstring frame_id\n";
    const JOINT_STATE_MSG: &str = "\
std_msgs/Header header
string[] name
float64[] position
float64[] velocity
float64[] effort
";

    /// The `ros2msg` concatenated form an MCAP record carries.
    fn joint_state_schema() -> String {
        let sep = "=".repeat(80);
        format!(
            "{JOINT_STATE_MSG}{sep}\nMSG: std_msgs/Header\n{HEADER_MSG}{sep}\nMSG: builtin_interfaces/Time\n{TIME_MSG}"
        )
    }

    /// The same little-endian XCDR1 payload the `thadam-ros2-msg` fixtures
    /// build: header with frame "", three joints, three positions, two
    /// velocities, one effort.
    fn joint_state_payload() -> Vec<u8> {
        let mut cdr = vec![0x00, 0x01, 0x00, 0x00];
        let push = |cdr: &mut Vec<u8>, bytes: [u8; 8]| {
            let rel = cdr.len() - 4;
            let pad = (8 - rel % 8) % 8;
            cdr.resize(cdr.len() + pad, 0);
            cdr.extend_from_slice(&bytes);
        };
        let u32_ = |cdr: &mut Vec<u8>, v: u32| {
            let rel = cdr.len() - 4;
            let pad = (4 - rel % 4) % 4;
            cdr.resize(cdr.len() + pad, 0);
            cdr.extend_from_slice(&v.to_le_bytes());
        };
        let string = |cdr: &mut Vec<u8>, s: &str| {
            u32_(cdr, s.len() as u32 + 1);
            cdr.extend_from_slice(s.as_bytes());
            cdr.push(0);
        };
        let f64_ = |cdr: &mut Vec<u8>, v: f64| push(cdr, v.to_bits().to_le_bytes());

        // header: stamp (1_758_000_000, 123_456_789), frame_id "".
        u32_(&mut cdr, 1_758_000_000);
        u32_(&mut cdr, 123_456_789);
        string(&mut cdr, "");
        u32_(&mut cdr, 3);
        string(&mut cdr, "hip");
        string(&mut cdr, "knee");
        string(&mut cdr, "ankle");
        u32_(&mut cdr, 3);
        f64_(&mut cdr, 0.1);
        f64_(&mut cdr, -0.2);
        f64_(&mut cdr, 0.3);
        u32_(&mut cdr, 2);
        f64_(&mut cdr, 1.5);
        f64_(&mut cdr, -1.5);
        u32_(&mut cdr, 1);
        f64_(&mut cdr, 9.8);
        cdr
    }

    #[test]
    fn decodes_a_joint_state_record() {
        let record = decode_ros2(
            "sensor_msgs/msg/JointState",
            &joint_state_schema(),
            joint_state_payload(),
        )
        .expect("decode");

        let names: Vec<&str> = record
            .get("name")
            .and_then(Value::as_list)
            .expect("name list")
            .iter()
            .map(|v| v.as_str().expect("string"))
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
        // The Header flattening, including the Time triple.
        assert_eq!(
            record.get("header.stamp"),
            Some(&Value::Ts(thadam_core::Timestamp::from_nanos(
                1_758_000_000_123_456_789
            )))
        );
        assert_eq!(
            record.get("header.frame_id").and_then(Value::as_str),
            Some("")
        );
    }

    #[test]
    fn name_spellings_and_errors_are_classified() {
        // Both `pkg/Type` and `pkg/msg/Type` spellings work.
        for name in ["sensor_msgs/JointState", "sensor_msgs/msg/JointState"] {
            decode_ros2(name, &joint_state_schema(), joint_state_payload())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
        // A bad payload is a decode error, not a panic.
        let err = decode_ros2("sensor_msgs/JointState", &joint_state_schema(), [0u8; 2])
            .expect_err("short payload must fail");
        assert!(err.code.starts_with("decode."), "{err}");
        // A bad schema is a schema error.
        let err = decode_ros2("sensor_msgs/JointState", "not a message", [])
            .expect_err("bad schema must fail");
        assert!(err.code.starts_with("schema."), "{err}");
    }
}
