//! Fuzzes `Registry::register_ros2msg` and `compile` with arbitrary UTF-8
//! input.
#![no_main]

use libfuzzer_sys::fuzz_target;
use thadam_ros2_msg::Registry;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let mut registry = Registry::new();
    let Ok(type_name) = registry.register_ros2msg("fuzz_msgs/msg/Fuzz", text) else {
        return;
    };
    let _ = registry.compile(&type_name.to_string());
});
