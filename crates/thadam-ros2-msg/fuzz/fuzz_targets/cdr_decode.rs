//! Fuzzes CDR decoding against a fixed set of compiled messages covering
//! strings, wide and bounded strings, nested messages, sequences of nested
//! messages, typed and generic sequences, fixed arrays, and
//! `builtin_interfaces/Time`. The first input byte picks the message.
#![no_main]

use libfuzzer_sys::fuzz_target;
use thadam_ros2_msg::{CompiledMessage, Registry};
use std::sync::{Arc, OnceLock};

const DEFINITIONS: &[(&str, &str, &str)] = &[
    ("builtin_interfaces", "Time", "int32 sec\nuint32 nanosec\n"),
    (
        "std_msgs",
        "Header",
        "builtin_interfaces/Time stamp\nstring frame_id\n",
    ),
    (
        "geometry_msgs",
        "Vector3",
        "float64 x\nfloat64 y\nfloat64 z\n",
    ),
    (
        "geometry_msgs",
        "Quaternion",
        "float64 x\nfloat64 y\nfloat64 z\nfloat64 w\n",
    ),
    (
        "geometry_msgs",
        "Transform",
        "geometry_msgs/Vector3 translation\ngeometry_msgs/Quaternion rotation\n",
    ),
    (
        "geometry_msgs",
        "TransformStamped",
        "std_msgs/Header header\nstring child_frame_id\ngeometry_msgs/Transform transform\n",
    ),
    (
        "tf2_msgs",
        "TFMessage",
        "geometry_msgs/TransformStamped[] transforms\n",
    ),
    (
        "sensor_msgs",
        "JointState",
        "std_msgs/Header header\nstring[] name\nfloat64[] position\nfloat64[] velocity\nfloat64[] effort\n",
    ),
    (
        "sensor_msgs",
        "Imu",
        "std_msgs/Header header\ngeometry_msgs/Quaternion orientation\nfloat64[9] orientation_covariance\ngeometry_msgs/Vector3 angular_velocity\nfloat64[9] angular_velocity_covariance\ngeometry_msgs/Vector3 linear_acceleration\nfloat64[9] linear_acceleration_covariance\n",
    ),
    (
        "fuzz_msgs",
        "Mixed",
        "wstring wide\nstring<=8 short\nint32[<=4] bounded\nbool[] flags\nuint64[] big\nuint8[] blob\nint16[3] fixed\nbuiltin_interfaces/Time[2] stamps\n",
    ),
];

const TARGETS: &[&str] = &[
    "std_msgs/Header",
    "tf2_msgs/TFMessage",
    "sensor_msgs/JointState",
    "sensor_msgs/Imu",
    "fuzz_msgs/Mixed",
];

static COMPILED: OnceLock<Vec<Arc<CompiledMessage>>> = OnceLock::new();

fn compile_all() -> Vec<Arc<CompiledMessage>> {
    let mut registry = Registry::new();
    for (package, name, text) in DEFINITIONS {
        registry.register(package, name, text).unwrap();
    }
    TARGETS
        .iter()
        .map(|name| registry.compile(name).unwrap())
        .collect()
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, payload)) = data.split_first() else {
        return;
    };
    let compiled = COMPILED.get_or_init(compile_all);
    let msg = &compiled[usize::from(selector) % compiled.len()];
    let _ = msg.decode(payload);
});
