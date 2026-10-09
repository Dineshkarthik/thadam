//! CDR decode throughput for two reference ROS 2 sensor messages.
//!
//! `JointState` exercises string lists plus three typed `f64` arrays;
//! `Imu` exercises nested flattening (`Time`, `Quaternion`, `Vector3`) and
//! four fixed `float64[9]` arrays. Each iteration decodes one pre-built
//! XCDR1 little-endian payload; schema compilation and payload encoding are
//! outside the timed section.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use thadam_ros2_msg::Registry;

const TIME_MSG: &str = include_str!("../tests/fixtures/builtin_interfaces/Time.msg");
const HEADER_MSG: &str = include_str!("../tests/fixtures/std_msgs/Header.msg");
const VECTOR3_MSG: &str = include_str!("../tests/fixtures/geometry_msgs/Vector3.msg");
const QUATERNION_MSG: &str = include_str!("../tests/fixtures/geometry_msgs/Quaternion.msg");
const JOINT_STATE_MSG: &str = include_str!("../tests/fixtures/sensor_msgs/JointState.msg");
const IMU_MSG: &str = include_str!("../tests/fixtures/sensor_msgs/Imu.msg");

/// Minimal little-endian XCDR1 encoder (payload-relative alignment).
struct Enc {
    buf: Vec<u8>,
}

impl Enc {
    fn le() -> Self {
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

    fn i32(&mut self, v: i32) -> &mut Self {
        self.u32(v as u32)
    }

    fn f64(&mut self, v: f64) -> &mut Self {
        self.pad(8);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    fn string(&mut self, s: &str) -> &mut Self {
        self.u32(s.len() as u32 + 1);
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
        self
    }

    fn header(&mut self, frame_id: &str) -> &mut Self {
        self.i32(1_758_000_000).u32(123_456_789).string(frame_id)
    }
}

/// A JointState with six joints: 7 strings/arrays plus the header.
fn joint_state_payload() -> Vec<u8> {
    let names = ["hip", "knee", "ankle", "shoulder", "elbow", "wrist"];
    let mut enc = Enc::le();
    enc.header("");
    enc.u32(names.len() as u32);
    for name in names {
        enc.string(name);
    }
    for (i, base) in [(0.1f64), (1.5), (9.8)].into_iter().enumerate() {
        enc.u32(6);
        for j in 0..6 {
            enc.f64(base + f64::from(j) * -0.05 + f64::from(i as i32));
        }
    }
    enc.buf
}

/// An Imu reading with the "no orientation estimate" marker.
fn imu_payload() -> Vec<u8> {
    let mut enc = Enc::le();
    enc.header("imu_link");
    for v in [0.0, 0.0, 0.0, 1.0] {
        enc.f64(v);
    }
    enc.f64(-1.0);
    for _ in 0..8 {
        enc.f64(0.0);
    }
    for v in [0.01, 0.02, -0.03] {
        enc.f64(v);
    }
    for i in 0..9 {
        enc.f64(f64::from(i) * 0.125);
    }
    for v in [0.0, 0.0, 9.81] {
        enc.f64(v);
    }
    for _ in 0..9 {
        enc.f64(0.0);
    }
    enc.buf
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register("builtin_interfaces", "Time", TIME_MSG)
        .unwrap();
    registry.register("std_msgs", "Header", HEADER_MSG).unwrap();
    registry
        .register("geometry_msgs", "Vector3", VECTOR3_MSG)
        .unwrap();
    registry
        .register("geometry_msgs", "Quaternion", QUATERNION_MSG)
        .unwrap();
    registry
        .register("sensor_msgs", "JointState", JOINT_STATE_MSG)
        .unwrap();
    registry.register("sensor_msgs", "Imu", IMU_MSG).unwrap();
    registry
}

fn bench_decode(c: &mut Criterion) {
    let mut registry = registry();
    let joint_state = registry.compile("sensor_msgs/JointState").unwrap();
    let imu = registry.compile("sensor_msgs/Imu").unwrap();
    let joint_payload = joint_state_payload();
    let imu_bytes = imu_payload();

    let mut group = c.benchmark_group("cdr_decode");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sensor_msgs/JointState", |b| {
        b.iter(|| {
            let record = joint_state.decode(black_box(&joint_payload)).unwrap();
            black_box(record);
        });
    });
    group.bench_function("sensor_msgs/Imu", |b| {
        b.iter(|| {
            let record = imu.decode(black_box(&imu_bytes)).unwrap();
            black_box(record);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
