//! The `thadam` binary and composition root.
//!
//! # Responsibility
//!
//! - Build the connector registry and construct the Tokio runtime.
//! - Command-line commands and exit-code mapping.
//!
//! # Deliberately not exposed
//!
//! - A library target.
//! - A `bench` subcommand; benchmarks live in `tools/thadam-bench`.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version" | "-V") => {
            println!("thadam {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("usage: thadam --version");
            ExitCode::from(2)
        }
    }
}
