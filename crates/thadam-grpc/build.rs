//! Generates the ingest service from the `.proto` contract. `protox` parses the
//! file in pure Rust, so the build does not depend on a `protoc` binary.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/thadam/ingest/v1/ingest.proto";
    println!("cargo:rerun-if-changed={PROTO}");
    let descriptors = protox::compile([PROTO], ["proto"])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(descriptors)?;
    Ok(())
}
