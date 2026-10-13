//! Compiles `sparkplug_b.proto` with `protox` — pure Rust, so the build does
//! not depend on a `protoc` binary (the `thadam-grpc` pattern). The proto2
//! `extensions N to max` ranges have no accessors in prost; fields written
//! into them decode as unknown and are skipped.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/sparkplug_b.proto";
    println!("cargo:rerun-if-changed={PROTO}");
    let descriptors = protox::compile([PROTO], ["proto"])?;
    prost_build::compile_fds(descriptors)?;
    Ok(())
}
