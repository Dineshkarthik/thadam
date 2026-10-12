//! Vendored `.msg` definitions (Jazzy) registered into a `thadam-ros2-msg`
//! [`Registry`]. Files live under `msgs/` with upstream provenance in their
//! headers; they are embedded at build time so the spike binary is
//! self-contained.

use std::sync::Arc;

use thadam_ros2_msg::{CompiledMessage, Registry};

macro_rules! register_msg {
    ($registry:expr, $pkg:literal, $name:literal, $path:literal) => {
        $registry
            .register($pkg, $name, include_str!($path))
            .unwrap_or_else(|e| panic!("parse {}/{}: {e}", $pkg, $name))
    };
}

/// A registry preloaded with every message definition the spike decodes.
pub fn build_registry() -> Registry {
    let mut r = Registry::new();
    register_msg!(
        r,
        "builtin_interfaces",
        "Time",
        "../msgs/builtin_interfaces/msg/Time.msg"
    );
    register_msg!(r, "std_msgs", "Header", "../msgs/std_msgs/msg/Header.msg");
    register_msg!(
        r,
        "geometry_msgs",
        "Vector3",
        "../msgs/geometry_msgs/msg/Vector3.msg"
    );
    register_msg!(
        r,
        "geometry_msgs",
        "Quaternion",
        "../msgs/geometry_msgs/msg/Quaternion.msg"
    );
    register_msg!(
        r,
        "geometry_msgs",
        "Transform",
        "../msgs/geometry_msgs/msg/Transform.msg"
    );
    register_msg!(
        r,
        "geometry_msgs",
        "TransformStamped",
        "../msgs/geometry_msgs/msg/TransformStamped.msg"
    );
    register_msg!(r, "sensor_msgs", "Imu", "../msgs/sensor_msgs/msg/Imu.msg");
    register_msg!(
        r,
        "sensor_msgs",
        "JointState",
        "../msgs/sensor_msgs/msg/JointState.msg"
    );
    register_msg!(
        r,
        "tf2_msgs",
        "TFMessage",
        "../msgs/tf2_msgs/msg/TFMessage.msg"
    );
    r
}

/// The (topic, type) matrix this spike subscribes to, in order.
/// `qos` mirrors what the container publishers declare.
pub struct TopicSpec {
    pub topic: &'static str,
    pub ros_type: &'static str,
    pub qos: crate::transport::QosProfile,
    /// Nominal publish rate, for expected-count checks.
    pub nominal_hz: f64,
}

use crate::transport::{Durability, QosProfile, Reliability};

/// `/imu`, `/joint_states`, `/tf`, `/tf_static`.
pub const TOPICS: [TopicSpec; 4] = [
    TopicSpec {
        topic: "/imu",
        ros_type: "sensor_msgs/Imu",
        qos: QosProfile {
            reliability: Reliability::BestEffort,
            durability: Durability::Volatile,
            depth: 5,
        },
        nominal_hz: 1000.0,
    },
    TopicSpec {
        topic: "/joint_states",
        ros_type: "sensor_msgs/JointState",
        qos: QosProfile {
            reliability: Reliability::Reliable,
            durability: Durability::Volatile,
            depth: 10,
        },
        nominal_hz: 100.0,
    },
    TopicSpec {
        topic: "/tf",
        ros_type: "tf2_msgs/TFMessage",
        qos: QosProfile {
            reliability: Reliability::Reliable,
            durability: Durability::Volatile,
            depth: 100,
        },
        nominal_hz: 30.0,
    },
    TopicSpec {
        topic: "/tf_static",
        ros_type: "tf2_msgs/TFMessage",
        qos: QosProfile {
            reliability: Reliability::Reliable,
            durability: Durability::TransientLocal,
            depth: 1,
        },
        nominal_hz: 0.0,
    },
];

/// Compile the decoder for `ros_type`, panicking with a useful message if a
/// dependency definition is missing (a spike authoring bug, not runtime).
pub fn compile(registry: &mut Registry, ros_type: &str) -> Arc<CompiledMessage> {
    registry
        .compile(ros_type)
        .unwrap_or_else(|e| panic!("compile {ros_type}: {e}"))
}
