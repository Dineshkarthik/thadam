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
mod run;
mod status;
mod validate;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use thadam_config::{CONFIG_HASH_VERSION, CONFIG_VERSION, ProcessEnv};
use thadam_runtime::LogFormat;

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
    /// Run a pipeline configuration.
    Run(RunArgs),
    /// Check the admin server status.
    Status(StatusArgs),
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

#[derive(Args)]
struct RunArgs {
    /// The pipeline configuration file.
    path: PathBuf,
    /// Run in repair mode, accepting a `thadam.json` mismatch (pipeline name or
    /// WAL binding) and rewriting it. Never accepts a format version newer than
    /// this binary knows.
    #[arg(long)]
    repair: bool,
    /// The log output format: `json` or `pretty`.
    #[arg(long, value_name = "FORMAT", default_value = "pretty")]
    log_format: LogFormatArg,
    /// The minimum log level (e.g., `info`, `debug`, `warn`).
    #[arg(long, value_name = "LEVEL", default_value = "info")]
    log_level: String,
    /// The admin server bind address (e.g., `127.0.0.1:9464`).
    #[arg(long, value_name = "ADDR")]
    admin_bind: Option<String>,
    /// Path to a file containing the admin server bearer token.
    #[arg(long, value_name = "PATH")]
    admin_token_file: Option<PathBuf>,
}

#[derive(Args)]
struct StatusArgs {
    /// The admin server URL (default: http://127.0.0.1:9464).
    #[arg(long, value_name = "URL")]
    admin_url: Option<String>,
    /// Path to a file containing the admin server bearer token.
    #[arg(long, value_name = "PATH")]
    token_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
enum LogFormatArg {
    Json,
    Pretty,
}

impl LogFormatArg {
    fn into_runtime_format(self) -> LogFormat {
        match self {
            LogFormatArg::Json => LogFormat::Json,
            LogFormatArg::Pretty => LogFormat::Pretty,
        }
    }
}

impl std::str::FromStr for LogFormatArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(LogFormatArg::Json),
            "pretty" => Ok(LogFormatArg::Pretty),
            _ => Err(format!(
                "invalid log format `{s}`; expected `json` or `pretty`"
            )),
        }
    }
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
        Command::Run(args) => run::run(
            &args.path,
            args.repair,
            args.log_format.into_runtime_format(),
            &args.log_level,
            args.admin_bind.as_deref(),
            args.admin_token_file.as_deref(),
            &mut err,
        ),
        Command::Status(args) => status::run(
            args.admin_url.as_deref(),
            args.token_file.as_deref(),
            &mut out,
            &mut err,
        ),
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
