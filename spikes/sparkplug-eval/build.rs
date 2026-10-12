//! Compiles the vendored `sparkplug_b.proto` (Eclipse Tahu, EPL-2.0) with
//! `protox` — pure Rust, so the build needs no `protoc` binary. The proto2
//! `extensions N to max` ranges have no accessors in prost; fields written
//! into them decode as unknown and are skipped.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/sparkplug_b.proto";
    println!("cargo:rerun-if-changed={PROTO}");
    let descriptors = protox::compile([PROTO], ["proto"])?;
    prost_build::compile_fds(descriptors)?;
    Ok(())
}
