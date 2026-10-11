//! The dataset `decode: ros2` payload decoder.
//!
//! A ROS 2 CDR message captured from MCAP carries no schema text, only the
//! `mcap.schema_hash` attribute ([`SCHEMA_HASH_ATTR`]) the source stamped it
//! with when it durably wrote the schema to the pipeline's
//! [`SchemaStore`]. [`Ros2Decode`] resolves the schema text by hash,
//! compiles it with `thadam-ros2-msg` (compiled messages are cached per
//! hash — the store itself is content-addressed, so a hash always means
//! one definition), and decodes the payload into a [`Record`] whose fields
//! are the flattened dotted names (`header.stamp`, `pose.position.x`, …).
//!
//! Every failure is a classified reject reason, never a silent drop, and
//! error text never contains payload bytes:
//!
//! - the message carries no `mcap.schema_hash` attribute:
//!   `decode.schema_ref_missing`;
//! - the hash is not in the store (wrong content form counts too):
//!   `decode.unknown_schema`;
//! - the stored encoding is not `ros2msg`: `decode.unsupported_encoding`;
//! - the store itself is damaged: the store's own `buffer.*` errors;
//! - the schema does not compile or the payload does not decode: the
//!   `schema.*` / `decode.*` errors `thadam-ros2-msg` classifies already.
//!
//! [`record_to_json`] then bridges the record into the chain's
//! JSON-pointer-shaped machinery: each flattened dotted name is **one**
//! object key, exactly as decoded, so `/header.stamp` is a single JSON
//! Pointer token. Declaring the dotted names as first-class dataset fields
//! typed by the decode itself (no JSON detour) is a later step; the bridge
//! is what makes `decode: ros2` useful through the declared-field pipeline
//! today.

use std::collections::HashMap;
use std::sync::Arc;

use thadam_core::{
    Attributes, ErrorDomain, Record, ThadamError, SCHEMA_HASH_ATTR, SchemaStore, Value, codes,
};
use thadam_ros2_msg::{CompiledMessage, Registry};
use serde_json::Value as Json;

/// The `ros2` payload decoder: schema-store reads plus a per-hash compiled
/// message cache.
pub(crate) struct Ros2Decode {
    store: SchemaStore,
    registry: Registry,
    compiled: HashMap<Box<str>, Arc<CompiledMessage>>,
}

impl Ros2Decode {
    /// A decoder resolving schemas from `store`.
    pub(crate) fn new(store: SchemaStore) -> Self {
        Ros2Decode {
            store,
            registry: Registry::new(),
            compiled: HashMap::new(),
        }
    }

    /// Decodes one captured message into a record; see the module docs for
    /// the classified failure modes.
    pub(crate) fn decode(
        &mut self,
        attrs: &Attributes,
        payload: &[u8],
    ) -> Result<Record, ThadamError> {
        let hash = attrs.get(SCHEMA_HASH_ATTR).ok_or_else(|| {
            ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_SCHEMA_REF_MISSING,
                format!("the message carries no `{SCHEMA_HASH_ATTR}` attribute"),
            )
        })?;
        let schema = self.store.get(hash)?.ok_or_else(|| {
            ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_UNKNOWN_SCHEMA,
                "the referenced schema is not in the schema store",
            )
            .with_context("schema_hash", hash)
        })?;
        if &*schema.encoding != "ros2msg" {
            return Err(ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_UNSUPPORTED_ENCODING,
                format!(
                    "the stored schema encoding is `{}` where `ros2msg` was expected",
                    schema.encoding
                ),
            )
            .with_context("schema_hash", hash));
        }
        let compiled = match self.compiled.get(hash) {
            Some(c) => Arc::clone(c),
            None => {
                let main = self.registry.register_ros2msg(&schema.name, &schema.text)?;
                let c = self.registry.compile(&main.to_string())?;
                self.compiled.insert(hash.into(), Arc::clone(&c));
                c
            }
        };
        compiled.decode(payload)
    }
}

/// One decoded record as the JSON-root object the dataset pipeline reads;
/// see the module docs for the bridge's exact spelling.
pub(crate) fn record_to_json(record: &Record) -> Json {
    let fields = record.shape().fields();
    let mut map = serde_json::Map::with_capacity(fields.len());
    for field in fields {
        // Every shape field has a value by construction.
        let value = record.get(&field.name).unwrap_or(&Value::Null);
        map.insert(field.name.to_string(), value_to_json(value));
    }
    Json::Object(map)
}

fn value_to_json(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::I8(v) => Json::from(*v),
        Value::I16(v) => Json::from(*v),
        Value::I32(v) => Json::from(*v),
        Value::I64(v) => Json::from(*v),
        Value::U8(v) => Json::from(*v),
        Value::U16(v) => Json::from(*v),
        Value::U32(v) => Json::from(*v),
        Value::U64(v) => Json::from(*v),
        Value::F32(v) => json_f64(f64::from(*v)),
        Value::F64(v) => json_f64(*v),
        Value::Str(s) => Json::from(s.as_str()),
        Value::Bytes(b) => Json::Array(b.iter().map(|v| Json::from(*v)).collect()),
        // Nanoseconds since the Unix epoch, so `unit: ns` time reads work.
        Value::Ts(ts) => Json::from(ts.as_nanos()),
        Value::F32s(vs) => Json::Array(vs.iter().map(|v| json_f64(f64::from(*v))).collect()),
        Value::F64s(vs) => Json::Array(vs.iter().map(|v| json_f64(*v)).collect()),
        Value::I32s(vs) => Json::Array(vs.iter().map(|v| Json::from(*v)).collect()),
        Value::I64s(vs) => Json::Array(vs.iter().map(|v| Json::from(*v)).collect()),
        Value::List(items) => Json::Array(items.iter().map(value_to_json).collect()),
        Value::Record(r) => record_to_json(r),
        // Value is non-exhaustive; a variant added later bridges to null
        // rather than failing messages that carry it.
        _ => Json::Null,
    }
}

/// JSON has no NaN or infinities: they bridge to `null`, which is how the
/// JSON decoder already spells an absent/non-finite value.
fn json_f64(v: f64) -> Json {
    serde_json::Number::from_f64(v).map_or(Json::Null, Json::Number)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use thadam_core::{SourceId, Timestamp, WalEpoch, WalPos};

    use super::*;
    use crate::input::RawRecordView;

    const TIME_MSG: &str = "int32 sec\nuint32 nanosec\n";
    const HEADER_MSG: &str = "builtin_interfaces/Time stamp\nstring frame_id\n";
    const JOINT_STATE_MSG: &str = "\
std_msgs/Header header
string[] name
float64[] position
float64[] velocity
float64[] effort
";
    const JOINT_STATE_NAME: &str = "sensor_msgs/msg/JointState";
    const POSE_STAMPED_NAME: &str = "geometry_msgs/msg/PoseStamped";

    /// The `ros2msg` concatenated form an MCAP record carries.
    fn join_docs(parts: &[(&str, &str)]) -> String {
        let sep = "=".repeat(80);
        let mut out = String::new();
        for (name, text) in parts {
            out.push_str(&format!("{sep}\nMSG: {name}\n{text}"));
        }
        out
    }

    fn joint_state_schema() -> String {
        let rest = join_docs(&[
            ("std_msgs/Header", HEADER_MSG),
            ("builtin_interfaces/Time", TIME_MSG),
        ]);
        format!("{JOINT_STATE_MSG}{rest}")
    }

    fn pose_stamped_schema() -> String {
        let main = "std_msgs/Header header\ngeometry_msgs/Pose pose\n";
        let rest = join_docs(&[
            (
                "geometry_msgs/Pose",
                "geometry_msgs/Point position\ngeometry_msgs/Quaternion orientation\n",
            ),
            ("geometry_msgs/Point", "float64 x\nfloat64 y\nfloat64 z\n"),
            (
                "geometry_msgs/Quaternion",
                "float64 x 0\nfloat64 y 0\nfloat64 z 0\nfloat64 w 1\n",
            ),
            ("std_msgs/Header", HEADER_MSG),
            ("builtin_interfaces/Time", TIME_MSG),
        ]);
        format!("{main}{rest}")
    }

    /// A little-endian XCDR1 encoder, same as the fixture encoders in
    /// `thadam-mcap` and `thadam-ros2-msg` tests.
    struct Enc {
        buf: Vec<u8>,
    }

    impl Enc {
        fn new() -> Self {
            Enc {
                buf: vec![0x00, 0x01, 0x00, 0x00],
            }
        }

        fn pad(&mut self, align: usize) {
            let rel = self.buf.len() - 4;
            let pad = (align - rel % align) % align;
            self.buf.resize(self.buf.len() + pad, 0);
        }

        fn u32(&mut self, v: u32) -> &mut Self {
            self.pad(4);
            self.buf.extend_from_slice(&v.to_le_bytes());
            self
        }

        fn f64(&mut self, v: f64) -> &mut Self {
            self.pad(8);
            self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
            self
        }

        fn string(&mut self, s: &str) -> &mut Self {
            self.u32(s.len() as u32 + 1);
            self.buf.extend_from_slice(s.as_bytes());
            self.buf.push(0);
            self
        }

        fn header(&mut self, sec: i32, nanosec: u32, frame_id: &str) -> &mut Self {
            self.u32(sec as u32);
            self.u32(nanosec);
            self.string(frame_id)
        }

        fn finish(self) -> Vec<u8> {
            self.buf
        }
    }

    fn joint_state_payload() -> Vec<u8> {
        let mut enc = Enc::new();
        enc.header(1_758_000_000, 123_456_789, "arm");
        enc.u32(2).string("hip").string("knee");
        enc.u32(2).f64(0.1).f64(-0.2);
        enc.u32(1).f64(1.5);
        enc.u32(0);
        enc.finish()
    }

    fn pose_stamped_payload() -> Vec<u8> {
        let mut enc = Enc::new();
        enc.header(1_758_000_001, 987_654_321, "map");
        enc.f64(1.0).f64(2.0).f64(3.0);
        enc.f64(0.0).f64(0.0).f64(0.0).f64(1.0);
        enc.finish()
    }

    fn attrs(hash: Option<&str>) -> Attributes {
        let mut attrs = Attributes::new();
        attrs.insert("topic", "/joint_states");
        if let Some(hash) = hash {
            attrs.insert(SCHEMA_HASH_ATTR, hash);
        }
        attrs
    }

    fn store_with(dir: &tempfile::TempDir, schemas: &[(&str, &str, String)]) -> SchemaStore {
        let store = SchemaStore::open(dir.path().join("schemas"));
        for (name, encoding, text) in schemas {
            store.put(name, encoding, text).expect("put");
        }
        store
    }

    #[test]
    fn decodes_joint_state_and_pose_stamped_records() {
        let dir = tempfile::tempdir().unwrap();
        let joint_hash = SchemaStore::hash(JOINT_STATE_NAME, "ros2msg", &joint_state_schema());
        let pose_hash = SchemaStore::hash(POSE_STAMPED_NAME, "ros2msg", &pose_stamped_schema());
        let store = store_with(
            &dir,
            &[
                (JOINT_STATE_NAME, "ros2msg", joint_state_schema()),
                (POSE_STAMPED_NAME, "ros2msg", pose_stamped_schema()),
            ],
        );
        let mut decode = Ros2Decode::new(store);

        let record = decode
            .decode(&attrs(Some(&joint_hash)), &joint_state_payload())
            .expect("joint decode");
        let names: Vec<&str> = record.shape().fields().iter().map(|f| &*f.name).collect();
        assert_eq!(
            names,
            [
                "header.stamp.sec",
                "header.stamp.nanosec",
                "header.stamp",
                "header.frame_id",
                "name",
                "position",
                "velocity",
                "effort",
            ]
        );
        assert_eq!(
            record.get("header.stamp"),
            Some(&Value::Ts(Timestamp::from_nanos(1_758_000_000_123_456_789)))
        );
        assert_eq!(
            record.get("header.frame_id").and_then(Value::as_str),
            Some("arm")
        );
        let joints: Vec<&str> = record
            .get("name")
            .and_then(Value::as_list)
            .expect("name list")
            .iter()
            .map(|v| v.as_str().expect("str"))
            .collect();
        assert_eq!(joints, ["hip", "knee"]);
        assert_eq!(
            record.get("position").and_then(Value::as_f64s),
            Some(&[0.1, -0.2][..])
        );
        assert_eq!(
            record.get("velocity").and_then(Value::as_f64s),
            Some(&[1.5][..])
        );
        assert_eq!(record.get("effort").and_then(Value::as_f64s), Some(&[][..]));

        // The same hash again takes the compiled-message cache.
        decode
            .decode(&attrs(Some(&joint_hash)), &joint_state_payload())
            .expect("cached decode");

        let record = decode
            .decode(&attrs(Some(&pose_hash)), &pose_stamped_payload())
            .expect("pose decode");
        let names: Vec<&str> = record.shape().fields().iter().map(|f| &*f.name).collect();
        assert_eq!(
            names,
            [
                "header.stamp.sec",
                "header.stamp.nanosec",
                "header.stamp",
                "header.frame_id",
                "pose.position.x",
                "pose.position.y",
                "pose.position.z",
                "pose.orientation.x",
                "pose.orientation.y",
                "pose.orientation.z",
                "pose.orientation.w",
            ]
        );
        assert_eq!(
            record.get("header.stamp"),
            Some(&Value::Ts(Timestamp::from_nanos(1_758_000_001_987_654_321)))
        );
        assert_eq!(
            record.get("pose.position.x").and_then(Value::as_f64),
            Some(1.0)
        );
        assert_eq!(
            record.get("pose.position.z").and_then(Value::as_f64),
            Some(3.0)
        );
        assert_eq!(
            record.get("pose.orientation.w").and_then(Value::as_f64),
            Some(1.0)
        );
        assert_eq!(
            record.get("header.frame_id").and_then(Value::as_str),
            Some("map")
        );
    }

    #[test]
    fn missing_attribute_and_unknown_hash_are_classified_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with(&dir, &[]);
        let mut decode = Ros2Decode::new(store);

        let err = decode
            .decode(&attrs(None), &joint_state_payload())
            .expect_err("no attribute");
        assert_eq!(err.code, codes::DECODE_SCHEMA_REF_MISSING);
        assert!(err.message.contains("mcap.schema_hash"), "{err}");

        let err = decode
            .decode(
                &attrs(Some(&SchemaStore::hash("a", "ros2msg", "b"))),
                &joint_state_payload(),
            )
            .expect_err("unknown hash");
        assert_eq!(err.code, codes::DECODE_UNKNOWN_SCHEMA);
        // Not even a hash shape at all: reads as "unknown", never a lookup.
        let err = decode
            .decode(&attrs(Some("../../etc/passwd")), &joint_state_payload())
            .expect_err("malformed hash");
        assert_eq!(err.code, codes::DECODE_UNKNOWN_SCHEMA);
    }

    #[test]
    fn payload_and_encoding_failures_are_classified_and_payload_free() {
        let dir = tempfile::tempdir().unwrap();
        let joint_hash = SchemaStore::hash(JOINT_STATE_NAME, "ros2msg", &joint_state_schema());
        let json_hash = SchemaStore::hash(JOINT_STATE_NAME, "jsonschema", "{}");
        let store = store_with(
            &dir,
            &[
                (JOINT_STATE_NAME, "ros2msg", joint_state_schema()),
                (JOINT_STATE_NAME, "jsonschema", "{}".to_owned()),
            ],
        );
        let mut decode = Ros2Decode::new(store);

        let err = decode
            .decode(&attrs(Some(&joint_hash)), &[0x00, 0x01])
            .expect_err("truncated payload");
        assert!(err.code.starts_with("decode."), "{err}");
        // Never payload bytes in the error, not even behind `{:?}`.
        assert!(!format!("{err:?}").contains("hip"));

        let err = decode
            .decode(&attrs(Some(&json_hash)), &joint_state_payload())
            .expect_err("not a ros2msg schema");
        assert_eq!(err.code, codes::DECODE_UNSUPPORTED_ENCODING);
    }

    #[test]
    fn record_bridges_to_json_with_one_key_per_dotted_field() {
        let dir = tempfile::tempdir().unwrap();
        let joint_hash = SchemaStore::hash(JOINT_STATE_NAME, "ros2msg", &joint_state_schema());
        let store = store_with(&dir, &[(JOINT_STATE_NAME, "ros2msg", joint_state_schema())]);
        let mut decode = Ros2Decode::new(store);
        let record = decode
            .decode(&attrs(Some(&joint_hash)), &joint_state_payload())
            .expect("decode");

        let root = record_to_json(&record);
        // `/header.stamp` is ONE JSON Pointer token (the flattened key).
        assert_eq!(
            root.pointer("/header.stamp").and_then(Json::as_i64),
            Some(1_758_000_000_123_456_789)
        );
        assert_eq!(
            root.pointer("/header.frame_id").and_then(Json::as_str),
            Some("arm")
        );
        assert_eq!(
            root.pointer("/position")
                .and_then(Json::as_array)
                .map(Vec::len),
            Some(2)
        );
        // No accidental nesting from the dots.
        let object = root.as_object().expect("object");
        assert!(object.contains_key("header.stamp"));
        assert!(!object.contains_key("header"));
    }

    #[test]
    fn the_chain_routes_ros2_decode_through_a_dataset() {
        use crate::{Chain, ChainLimits, FrameInput, spec::ChainSpec};

        let dir = tempfile::tempdir().unwrap();
        let joint_hash = SchemaStore::hash(JOINT_STATE_NAME, "ros2msg", &joint_state_schema());
        let store = store_with(&dir, &[(JOINT_STATE_NAME, "ros2msg", joint_state_schema())]);
        let schema_dir = store.dir().to_path_buf();

        let spec = ChainSpec::from_value(serde_json::json!({
            "datasets": [{
                "id": "joints",
                "match": { "topic": "/joint_states" },
                "decode": "ros2",
                "identity": [{ "robot": "arm-1" }],
                "time": { "attr": "mcap.log_time", "unit": "ns", "domain": "utc" },
                "fields": [
                    { "name": "frame", "type": "str", "from": "/header.frame_id" },
                    { "name": "position", "type": "list<f64>", "from": "/position" },
                    { "name": "names", "type": "list<str>", "from": "/name" }
                ]
            }]
        }))
        .expect("spec")
        .with_schema_dir(&schema_dir);
        let mut chain = Chain::compile(&spec, ChainLimits::default()).expect("compile");

        let source = SourceId::new("bag");
        let payload = Bytes::from(joint_state_payload());
        let mut message_attrs = attrs(Some(&joint_hash));
        message_attrs.insert("mcap.log_time", "1758000000000000042");
        let messages = [thadam_core::RawMessage {
            ingest_time: Timestamp::from_nanos(42),
            attrs: message_attrs,
            payload: payload.clone(),
        }];
        let views: Vec<RawRecordView<'_>> = messages
            .iter()
            .enumerate()
            .map(|(i, m)| {
                RawRecordView::new(&source, m, WalEpoch::new(1), WalPos::new(1), i as u32)
            })
            .collect();
        let input = FrameInput::new(&views, true);
        let origin = input.origin().expect("origin");
        let mut out = chain.new_output(origin, true);
        let mut rejects = Vec::new();
        chain.process(&input, &mut out, &mut rejects);

        assert!(rejects.is_empty(), "{rejects:?}");
        let events = &out.batch("joints").expect("joints batch").events;
        assert_eq!(events.len(), 1);
        let record = events[0].payload().as_record().expect("record payload");
        assert_eq!(
            record.get("frame").and_then(Value::as_str),
            Some("arm"),
            "declared field read through the dotted bridge"
        );

        // Unknown hash: quarantined with a classified code, never dropped.
        let mut message_attrs = attrs(Some(&SchemaStore::hash("x", "ros2msg", "y")));
        message_attrs.insert("mcap.log_time", "1758000000000000043");
        let messages = [thadam_core::RawMessage {
            ingest_time: Timestamp::from_nanos(43),
            attrs: message_attrs,
            payload,
        }];
        let views: Vec<RawRecordView<'_>> = messages
            .iter()
            .enumerate()
            .map(|(i, m)| {
                RawRecordView::new(&source, m, WalEpoch::new(1), WalPos::new(2), i as u32)
            })
            .collect();
        let input = FrameInput::new(&views, true);
        let origin = input.origin().expect("origin");
        let mut out = chain.new_output(origin, true);
        let mut rejects = Vec::new();
        chain.process(&input, &mut out, &mut rejects);
        assert_eq!(rejects.len(), 1);
        assert_eq!(rejects[0].error.code, codes::DECODE_UNKNOWN_SCHEMA);
    }

    #[test]
    fn ros2_decode_without_a_schema_dir_fails_compile() {
        use crate::{Chain, ChainLimits, spec::ChainSpec};

        let spec = ChainSpec::from_value(serde_json::json!({
            "datasets": [{ "id": "x", "decode": "ros2", "fields": [] }]
        }))
        .expect("spec");
        let errs = Chain::compile(&spec, ChainLimits::default()).expect_err("compile fails");
        assert!(
            errs.iter()
                .any(|e| e.message.contains("schema") && e.code == codes::CONFIG_MISSING),
            "{errs:?}"
        );
    }
}
