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
//!
//! # Exit codes
//!
//! 0 ok, 1 runtime failure, 2 invalid configuration or usage, 3 data directory
//! locked or incompatible, 130 interrupted.

mod exit;
mod registry;
mod validate;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use thadam_config::{CONFIG_HASH_VERSION, CONFIG_VERSION, ProcessEnv};

use crate::exit::Exit;

#[derive(Parser)]
#[command(name = "thadam", version, about = "Edge-first data capture")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check a configuration file and print the memory estimate.
    Validate(ValidateArgs),
    /// Print version and format information.
    Version,
    /// Print the JSON Schema of the configuration file.
    Schema,
}

#[derive(Args)]
struct ValidateArgs {
    /// The pipeline configuration file.
    path: PathBuf,
    /// Treat an unset `${env:}` or unreadable `${file:}` as a warning, so a
    /// configuration can be checked on a machine that lacks the secrets.
    #[arg(long)]
    allow_unresolved: bool,
}

fn version_text() -> String {
    format!(
        "thadam {}\nconfig version: {CONFIG_VERSION}\nconfig hash version: {CONFIG_HASH_VERSION}\n",
        env!("CARGO_PKG_VERSION")
    )
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            let code = if e.use_stderr() {
                Exit::InvalidConfig.code()
            } else {
                Exit::Ok.code()
            };
            let _ = e.print();
            return ExitCode::from(code);
        }
    };

    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    let (mut out, mut err) = (stdout.lock(), stderr.lock());
    let exit = match cli.command {
        Command::Validate(args) => validate::run(
            &args.path,
            args.allow_unresolved,
            &ProcessEnv,
            &mut out,
            &mut err,
        ),
        Command::Version => {
            let _ = write!(out, "{}", version_text());
            Exit::Ok
        }
        Command::Schema => match serde_json::to_string_pretty(&thadam_config::json_schema()) {
            Ok(text) => {
                let _ = writeln!(out, "{text}");
                Exit::Ok
            }
            Err(e) => {
                let _ = writeln!(err, "cannot render the schema: {e}");
                Exit::RuntimeFailure
            }
        },
    };
    exit.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_text_names_the_formats() {
        let text = version_text();
        assert!(text.starts_with("thadam "));
        assert!(text.contains("config version: 1"));
        assert!(text.contains("config hash version: 1"));
    }
}
