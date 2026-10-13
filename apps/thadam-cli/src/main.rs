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

mod data_dir;
mod exit;
mod export;
mod inspect;
mod point;
mod registry;
mod replay;
mod run;
mod schemas;
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
    /// Re-run a sink's chain over a retained WAL range into a separate directory.
    Replay(replay::ReplayArgs),
    /// Export a retained WAL range to one MCAP file.
    Export(export::ExportArgs),
    /// Read-only views of a data directory: WAL, cursors, segments, samples.
    Inspect(inspect::InspectArgs),
}

#[derive(Args)]
struct ValidateArgs {
    /// The pipeline configuration file.
    path: PathBuf,
    /// Treat an unset `${env:}` or unreadable `${file:}` as a warning, so a
    /// configuration can be checked on a machine that lacks the secrets.
    #[arg(long)]
    allow_unresolved: bool,
    /// Also check the configuration the way `run` would start it: every
    /// connector's configuration, every chain, and the data directory
    /// `pipeline.data_dir` (read-only). Needs the real secrets, so it cannot be
    /// combined with `--allow-unresolved`.
    #[arg(long)]
    against_data_dir: bool,
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
    /// The log output format: `json` or `pretty`. Defaults to
    /// `observability.logs.format` in the config file (`json` when the
    /// section is absent).
    #[arg(long, value_name = "FORMAT")]
    log_format: Option<LogFormatArg>,
    /// The minimum log level (e.g., `info`, `debug`, `warn`).
    #[arg(long, value_name = "LEVEL", default_value = "info")]
    log_level: String,
    /// The admin server bind address (e.g., `127.0.0.1:9464`).
    #[arg(long, value_name = "ADDR")]
    admin_bind: Option<String>,
    /// Path to a file containing the admin server bearer token.
    #[arg(long, value_name = "PATH")]
    admin_token_file: Option<PathBuf>,
    /// Exit once every source has reached end of stream, after draining and
    /// finalizing. For finite inputs such as a recording; live sources never
    /// end, so leave this off for them.
    #[arg(long)]
    exit_when_sources_finish: bool,
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
    // object_store's reqwest is built `rustls-no-provider` (ADR-0020): the
    // composition root installs the ring provider process-wide, which the
    // shipper's S3 client and the MQTT TLS relay both build on. A second
    // install would be rejected, so the result is intentionally dropped.
    let _ = rustls::crypto::ring::default_provider().install_default();

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
            args.against_data_dir,
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
        // The run's logs go through a background writer that takes the
        // process-wide stderr lock for every line. Holding `StderrLock` across
        // the run would block that writer until exit, so logs would go missing
        // when stderr is not a TTY. Drop the lock and pass a handle that locks
        // per write instead.
        Command::Run(args) => {
            drop(err);
            let mut run_err = std::io::stderr();
            run::run(&args, &mut run_err)
        }
        Command::Status(args) => status::run(
            args.admin_url.as_deref(),
            args.token_file.as_deref(),
            &mut out,
            &mut err,
        ),
        Command::Replay(args) => replay::run(&args, &mut out, &mut err),
        Command::Export(args) => export::run(&args, &mut out, &mut err),
        Command::Inspect(args) => inspect::run(&args, &mut out, &mut err),
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
