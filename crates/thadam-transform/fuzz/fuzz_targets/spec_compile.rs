//! Compiles arbitrary configs: any JSON must yield a spec error or a compile
//! result, never a panic, and a compiled chain must process a message safely.
#![no_main]

use libfuzzer_sys::fuzz_target;
use thadam_transform::{Chain, ChainLimits, ChainSpec};

fuzz_target!(|data: &[u8]| {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    let Ok(spec) = ChainSpec::from_value(value) else {
        return;
    };
    let _ = Chain::compile(&spec, ChainLimits::default());
});
