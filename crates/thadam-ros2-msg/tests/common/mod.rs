//! Hand-rolled XCDR1 encoder and fixture registry for decode tests.
//!
//! Shared between test binaries, some of which use only part of it.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use thadam_ros2_msg::Registry;

pub const TIME_MSG: &str = include_str!("../fixtures/builtin_interfaces/Time.msg");
pub const HEADER_MSG: &str = include_str!("../fixtures/std_msgs/Header.msg");
pub const POINT_MSG: &str = include_str!("../fixtures/geometry_msgs/Point.msg");
pub const VECTOR3_MSG: &str = include_str!("../fixtures/geometry_msgs/Vector3.msg");
pub const QUATERNION_MSG: &str = include_str!("../fixtures/geometry_msgs/Quaternion.msg");
pub const POSE_MSG: &str = include_str!("../fixtures/geometry_msgs/Pose.msg");
pub const POSE_STAMPED_MSG: &str = include_str!("../fixtures/geometry_msgs/PoseStamped.msg");
pub const JOINT_STATE_MSG: &str = include_str!("../fixtures/sensor_msgs/JointState.msg");
pub const IMU_MSG: &str = include_str!("../fixtures/sensor_msgs/Imu.msg");
pub const NAVSAT_STATUS_MSG: &str = include_str!("../fixtures/sensor_msgs/NavSatStatus.msg");
pub const UUID_MSG: &str = include_str!("../fixtures/unique_identifier_msgs/UUID.msg");

/// A synthetic message covering every array bound form and the typed-array
/// widening table.
pub const BOUNDED_MSG: &str = "\
float32[<=4] coeffs
int32[] counts
string<=16 label
uint8[] blob
bool[] flags
uint64[3] ids
geometry_msgs/Point[] polyline
geometry_msgs/Point[<=3] vertices
uint16[2] gains
";

/// An empty message definition: zero minimum wire bytes per element.
pub const EMPTY_MSG: &str = "# nothing to see here\n";

pub const STAMP_SEC: i32 = 1_758_000_000;
pub const STAMP_NANOSEC: u32 = 123_456_789;
pub const STAMP_NANOS: i64 = 1_758_000_000_123_456_789;

/// Registers every fixture message plus the synthetic ones.
pub fn full_registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register("builtin_interfaces", "Time", TIME_MSG)
        .unwrap();
    registry.register("std_msgs", "Header", HEADER_MSG).unwrap();
    registry
        .register("geometry_msgs", "Point", POINT_MSG)
        .unwrap();
    registry
        .register("geometry_msgs", "Vector3", VECTOR3_MSG)
        .unwrap();
    registry
        .register("geometry_msgs", "Quaternion", QUATERNION_MSG)
        .unwrap();
    registry
        .register("geometry_msgs", "Pose", POSE_MSG)
        .unwrap();
    registry
        .register("geometry_msgs", "PoseStamped", POSE_STAMPED_MSG)
        .unwrap();
    registry
        .register("sensor_msgs", "JointState", JOINT_STATE_MSG)
        .unwrap();
    registry.register("sensor_msgs", "Imu", IMU_MSG).unwrap();
    registry
        .register("sensor_msgs", "NavSatStatus", NAVSAT_STATUS_MSG)
        .unwrap();
    registry
        .register("unique_identifier_msgs", "UUID", UUID_MSG)
        .unwrap();
    registry
        .register("test_msgs", "Bounded", BOUNDED_MSG)
        .unwrap();
    registry.register("test_msgs", "Empty", EMPTY_MSG).unwrap();
    registry
}

/// A little- or big-endian XCDR1 encoder. Alignment counts from the start of
/// the payload after the 4-byte encapsulation header, matching the decoder.
pub struct Enc {
    pub buf: Vec<u8>,
    little: bool,
}

impl Enc {
    /// Little-endian encapsulation (`0x0001`).
    pub fn le() -> Self {
        Enc {
            buf: vec![0x00, 0x01, 0x00, 0x00],
            little: true,
        }
    }

    /// Big-endian encapsulation (`0x0000`).
    pub fn be() -> Self {
        Enc {
            buf: vec![0x00, 0x00, 0x00, 0x00],
            little: false,
        }
    }

    pub fn pad(&mut self, align: usize) -> &mut Self {
        let rel = self.buf.len() - 4;
        let pad = (align - rel % align) % align;
        self.buf.resize(self.buf.len() + pad, 0);
        self
    }

    fn push<const N: usize>(&mut self, bytes: [u8; N]) -> &mut Self {
        self.pad(N);
        self.buf.extend_from_slice(&bytes);
        self
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u8(u8::from(v))
    }

    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.push(if self.little {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        })
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.push(if self.little {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        })
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.u32(v as u32)
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.push(if self.little {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        })
    }

    pub fn f32(&mut self, v: f32) -> &mut Self {
        self.u32(v.to_bits())
    }

    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.u64(v.to_bits())
    }

    /// A CDR `string`: u32 length including the NUL, bytes, NUL.
    pub fn string(&mut self, s: &str) -> &mut Self {
        self.u32(s.len() as u32 + 1);
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
        self
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    /// The `std_msgs/Header` prefix used by every golden payload.
    pub fn header(&mut self, frame_id: &str) -> &mut Self {
        self.i32(STAMP_SEC);
        self.u32(STAMP_NANOSEC);
        self.string(frame_id)
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// `std_msgs/Header` payload: stamp + frame_id "base_link".
pub fn header_payload(little: bool) -> Vec<u8> {
    let mut enc = if little { Enc::le() } else { Enc::be() };
    enc.header("base_link");
    enc.finish()
}

/// `geometry_msgs/PoseStamped`: header frame "map", position (1.25, -2.5,
/// 3.75), orientation (0, 0, 0, 1).
pub fn pose_stamped_payload(little: bool) -> Vec<u8> {
    let mut enc = if little { Enc::le() } else { Enc::be() };
    enc.header("map")
        .f64(1.25)
        .f64(-2.5)
        .f64(3.75)
        .f64(0.0)
        .f64(0.0)
        .f64(0.0)
        .f64(1.0);
    enc.finish()
}

/// `sensor_msgs/JointState`: three joints, one empty `velocity` of the usual
/// kind is NOT used here; velocity has two entries and effort one, to cover
/// varied sequence lengths. Empty arrays are exercised separately.
pub fn joint_state_payload(little: bool) -> Vec<u8> {
    let mut enc = if little { Enc::le() } else { Enc::be() };
    enc.header("");
    enc.u32(3).string("hip").string("knee").string("ankle");
    enc.u32(3).f64(0.1).f64(-0.2).f64(0.3);
    enc.u32(2).f64(1.5).f64(-1.5);
    enc.u32(1).f64(9.8);
    enc.finish()
}

/// `sensor_msgs/JointState` with empty `name`, `position`, `velocity`, and
/// `effort`.
pub fn joint_state_empty_payload() -> Vec<u8> {
    let mut enc = Enc::le();
    enc.header("arm");
    enc.u32(0).u32(0).u32(0).u32(0);
    enc.finish()
}

/// `sensor_msgs/Imu`: orientation (0, 0, 0, 1), orientation_covariance
/// marked "no estimate" (-1 at [0]), angular_velocity (0.01, 0.02, -0.03)
/// with a diagonal-ish covariance, linear_acceleration (0, 0, 9.81) with a
/// zero covariance.
pub fn imu_payload(little: bool) -> Vec<u8> {
    let mut enc = if little { Enc::le() } else { Enc::be() };
    enc.header("imu_link");
    enc.f64(0.0).f64(0.0).f64(0.0).f64(1.0);
    enc.f64(-1.0);
    for _ in 0..8 {
        enc.f64(0.0);
    }
    enc.f64(0.01).f64(0.02).f64(-0.03);
    for i in 0..9 {
        enc.f64(f64::from(i) * 0.125);
    }
    enc.f64(0.0).f64(0.0).f64(9.81);
    for _ in 0..9 {
        enc.f64(0.0);
    }
    enc.finish()
}

/// `test_msgs/Bounded`: one entry per array bound form.
pub fn bounded_payload() -> Vec<u8> {
    let mut enc = Enc::le();
    enc.u32(3).f32(1.5).f32(-2.5).f32(3.5); // coeffs
    enc.u32(2).i32(7).i32(-8); // counts
    enc.string("ok"); // label
    enc.u32(4).bytes(&[0xde, 0xad, 0xbe, 0xef]); // blob
    enc.u32(2).bool(true).bool(false); // flags
    enc.u64(1).u64(2).u64(3); // ids (fixed, no length)
    enc.u32(1).f64(1.0).f64(2.0).f64(3.0); // polyline
    enc.u32(2)
        .f64(4.0)
        .f64(5.0)
        .f64(6.0)
        .f64(7.0)
        .f64(8.0)
        .f64(9.0); // vertices
    enc.u16(100).u16(200); // gains (fixed, no length)
    enc.finish()
}
