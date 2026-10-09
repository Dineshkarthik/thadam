//! Property tests: decoding arbitrary bytes never panics, and decode errors
//! always live in the Decode domain.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::{Arc, OnceLock};

use proptest::prelude::*;
use thadam_core::ErrorDomain;
use thadam_ros2_msg::CompiledMessage;

use crate::common::{full_registry, joint_state_payload};

const SHAPES: [&str; 5] = [
    "std_msgs/Header",
    "geometry_msgs/PoseStamped",
    "sensor_msgs/JointState",
    "sensor_msgs/Imu",
    "test_msgs/Bounded",
];

fn compiled() -> &'static Vec<Arc<CompiledMessage>> {
    static COMPILED: OnceLock<Vec<Arc<CompiledMessage>>> = OnceLock::new();
    COMPILED.get_or_init(|| {
        let mut registry = full_registry();
        SHAPES
            .iter()
            .map(|name| registry.compile(name).unwrap())
            .collect()
    })
}

proptest! {
    /// Arbitrary bytes against every compiled shape: decode never panics and
    /// every error is a Decode-domain error.
    #[test]
    fn decoding_arbitrary_bytes_never_panics(
        bytes in prop::collection::vec(any::<u8>(), 0..=512),
        shape_idx in 0..SHAPES.len(),
    ) {
        let result = compiled()[shape_idx].decode(bytes);
        if let Err(err) = result {
            prop_assert_eq!(err.domain, ErrorDomain::Decode);
        }
    }

    /// Bytes that happen to start with a valid encapsulation header exercise
    /// deeper decode paths; still never a panic.
    #[test]
    fn payloads_with_valid_headers_never_panic(
        mut bytes in prop::collection::vec(any::<u8>(), 0..=512),
        little in any::<bool>(),
        shape_idx in 0..SHAPES.len(),
    ) {
        bytes.splice(0..0, [0x00, u8::from(little), 0x00, 0x00]);
        let result = compiled()[shape_idx].decode(bytes);
        if let Err(err) = result {
            prop_assert_eq!(err.domain, ErrorDomain::Decode);
        }
    }

    /// A valid payload stays decodable with arbitrary trailing bytes
    /// appended (transports may pad).
    #[test]
    fn trailing_bytes_are_tolerated(suffix in prop::collection::vec(any::<u8>(), 0..=64)) {
        let compiled = &compiled()[2]; // sensor_msgs/JointState
        let mut payload = joint_state_payload(true);
        payload.extend_from_slice(&suffix);
        prop_assert!(compiled.decode(payload).is_ok());
    }
}
