//! `thadam-bench`: a reproducible benchmark runner for the edge-capture demo.
//!
//! `run` starts Mosquitto, the simulator, and `thadam run` for one scenario,
//! samples them, and writes a JSON result. `report` renders a directory of
//! results as a markdown table. See `benchmarks/README.md`.

mod binaries;
mod broker;
mod environment;
mod error;
mod metrics;
mod process;
mod report;
mod result;
mod run;
mod scenario;
mod storage;
mod summary;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

/// Benchmarks for the Thadam edge-capture demo.
#[derive(Debug, Parser)]
#[command(name = "thadam-bench", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run one scenario and write its JSON result.
    Run {
        /// The scenario file, e.g. `benchmarks/scenarios/m1-steady-state.yaml`.
        scenario: PathBuf,
        /// Directory for the JSON result.
        #[arg(long, value_name = "DIR", default_value = "benchmarks/results")]
        out: PathBuf,
        /// Replaces the scenario's duration (for smoke runs), e.g. `10s`.
        #[arg(long, value_name = "DUR", value_parser = humantime::parse_duration)]
        duration: Option<Duration>,
    },
    /// Print a markdown table of every result in a directory.
    Report {
        /// A directory of result JSON files.
        results: PathBuf,
    },
}

fn main() -> ExitCode {
    match dispatch(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("thadam-bench: {e}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch(cli: Cli) -> error::Result<()> {
    match cli.command {
        Command::Run {
            scenario,
            out,
            duration,
        } => {
            let opts = run::RunOptions {
                scenario,
                out_dir: out,
                duration,
            };
            let (path, result) = run::run(&opts)?;
            println!("{}", run::summary_line(&result, &path));
            Ok(())
        }
        Command::Report { results } => {
            let loaded = report::load_dir(&results)?;
            if loaded.is_empty() {
                println!("no results in {}", results.display());
            } else {
                print!("{}", report::render(&loaded));
            }
            Ok(())
        }
    }
}
