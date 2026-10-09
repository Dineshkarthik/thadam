//! The `thadam-simulator` binary: a seeded MQTT fleet for the Thadam
//! edge-capture demo.
//!
//! Exit codes mirror `thadam`: 0 ok, 1 runtime failure or a failed `verify`,
//! 2 usage or configuration error.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use thadam_simulator::SimError;
use thadam_simulator::config::FleetConfig;
use thadam_simulator::oracle::OracleStats;
use thadam_simulator::runner::{self, RunParams, parse_broker_url};
use thadam_simulator::verify;

#[derive(Parser)]
#[command(
    name = "thadam-simulator",
    version,
    about = "Seeded MQTT fleet simulator for the Thadam edge-capture demo",
    long_about = "Publishes a deterministic robot fleet on thadam-demo/v1/{site}/{robot}/{stream} \
topics (QoS 1) and writes an append-only NDJSON oracle of every sample with its \
broker-ack and expected-pipeline-outcome status."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the fleet until `--duration` ends or a signal arrives.
    Run(RunArgs),
    /// Aggregate an oracle file: row counts per stream and expected outcome.
    OracleStats(StatsArgs),
    /// Check delivered Parquet against an oracle (read only). Exits 1 when
    /// any check fails.
    Verify(VerifyArgs),
}

#[derive(Args)]
struct RunArgs {
    /// The fleet definition file; built-in defaults are used when absent.
    #[arg(long, value_name = "PATH")]
    fleet: Option<PathBuf>,
    /// The broker, `mqtt://host[:port]` (plaintext; no TLS in this build).
    #[arg(long, value_name = "URL", default_value = "mqtt://localhost:1883")]
    broker: String,
    /// The MQTT client id (clean session; no broker state is kept).
    #[arg(long, value_name = "ID", default_value = "thadam-simulator")]
    client_id: String,
    /// The oracle file to create (must not exist).
    #[arg(long, value_name = "PATH", default_value = "oracle.ndjson")]
    oracle: PathBuf,
    /// Master seed override (fleet file value otherwise).
    #[arg(long, value_name = "N")]
    seed: Option<u64>,
    /// Robots-per-site multiplier override (fleet file value otherwise).
    #[arg(long, value_name = "N")]
    scale: Option<u32>,
    /// Stop generating after this long (e.g. `30s`, `5m`); runs until
    /// signalled without it.
    #[arg(long, value_name = "DUR")]
    duration: Option<humantime::Duration>,
    /// How long to wait for broker PUBACKs after generation stops.
    #[arg(long, value_name = "DUR", default_value = "8s")]
    drain_timeout: humantime::Duration,
    /// Malformed-message rate override (0..1).
    #[arg(long, value_name = "P")]
    malformed_rate: Option<f64>,
    /// Duplicate-publish rate override (0..1).
    #[arg(long, value_name = "P")]
    duplicate_rate: Option<f64>,
    /// Out-of-range motor temperature rate override (0..1).
    #[arg(long, value_name = "P")]
    out_of_range_rate: Option<f64>,
    /// Publish every stream one sample per message (benchmarks).
    #[arg(long)]
    unbatched: bool,
    /// Log output: `pretty` for terminals, `json` for collection.
    #[arg(long, value_name = "FMT", default_value = "pretty")]
    log_format: LogFormat,
    /// Log level (a tracing env-filter, e.g. `info`, `debug`, `warn`).
    #[arg(long, value_name = "LEVEL", default_value = "info")]
    log_level: String,
}

#[derive(Args)]
struct StatsArgs {
    /// The oracle NDJSON file.
    path: PathBuf,
    /// Print JSON instead of a table.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct VerifyArgs {
    /// The oracle NDJSON file written by `run`.
    #[arg(long, value_name = "PATH")]
    oracle: PathBuf,
    /// The sink directory whose `ready/` tree is verified (one subdirectory
    /// per dataset).
    #[arg(long, value_name = "DIR")]
    ready: PathBuf,
    /// A quarantine directory; its Parquet rows are counted against the
    /// oracle's malformed messages.
    #[arg(long, value_name = "DIR")]
    quarantine: Option<PathBuf>,
    /// Print a JSON report instead of a table.
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Copy, Debug)]
enum LogFormat {
    Pretty,
    Json,
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pretty" | "text" => Ok(LogFormat::Pretty),
            "json" => Ok(LogFormat::Json),
            _ => Err(format!(
                "invalid log format `{s}`; expected `pretty` or `json`"
            )),
        }
    }
}

fn load_fleet(args: &RunArgs) -> Result<FleetConfig, SimError> {
    let mut config = match &args.fleet {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| SimError::Config(format!("cannot read `{}`: {e}", path.display())))?;
            FleetConfig::from_yaml(&text)
                .map_err(|e| SimError::Config(format!("{}: {e}", path.display())))?
        }
        None => FleetConfig::default(),
    };
    if let Some(seed) = args.seed {
        config.seed = seed;
    }
    if let Some(scale) = args.scale {
        config.scale = scale;
    }
    if let Some(rate) = args.malformed_rate {
        config.chaos.malformed_rate = rate;
    }
    if let Some(rate) = args.duplicate_rate {
        config.chaos.duplicate_publish_rate = rate;
    }
    if let Some(rate) = args.out_of_range_rate {
        config.chaos.out_of_range_rate = rate;
    }
    if args.unbatched {
        config.rates.joints_batch = 1;
        config.rates.imu_batch = 1;
        config.rates.pose_batch = 1;
    }
    let problems = config.problems();
    if !problems.is_empty() {
        return Err(SimError::Config(problems.join("; ")));
    }
    Ok(config)
}

fn init_logging(format: LogFormat, level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let result = match format {
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init(),
    };
    if result.is_err() {
        eprintln!("thadam-simulator: tracing already initialized");
    }
}

fn run_fleet(args: &RunArgs) -> Result<(), SimError> {
    init_logging(args.log_format, &args.log_level);
    let config = load_fleet(args)?;
    let fleet = config
        .build_fleet()
        .map_err(|e| SimError::Config(format!("fleet: {e}")))?;
    let broker = parse_broker_url(&args.broker)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(SimError::Io)?;
    let summary = runtime.block_on(runner::run(RunParams {
        fleet,
        broker_host: broker.host,
        broker_port: broker.port,
        client_id: args.client_id.clone(),
        oracle_path: args.oracle.clone(),
        duration: args.duration.map(|d| *d),
        drain_timeout: *args.drain_timeout,
    }))?;
    println!("{}", summary.stats.summary_line());
    println!(
        "oracle: {} ({} samples: {} deliver, {} filtered_debug, {} malformed, {} duplicate_of, {} state_repeat)",
        summary.oracle_path.display(),
        summary.oracle.total(),
        summary
            .oracle
            .count_expected(thadam_simulator::oracle::Expected::Deliver),
        summary
            .oracle
            .count_expected(thadam_simulator::oracle::Expected::FilteredDebug),
        summary
            .oracle
            .count_expected(thadam_simulator::oracle::Expected::Malformed),
        summary
            .oracle
            .count_expected(thadam_simulator::oracle::Expected::DuplicateOf),
        summary
            .oracle
            .count_expected(thadam_simulator::oracle::Expected::StateRepeat),
    );
    Ok(())
}

fn oracle_stats(args: &StatsArgs) -> Result<(), SimError> {
    let stats = OracleStats::load(&args.path)?;
    if args.json {
        let text = serde_json::to_string_pretty(&stats.to_json())
            .map_err(|e| SimError::Config(format!("render stats: {e}")))?;
        println!("{text}");
    } else {
        print!("{}", stats.render());
    }
    Ok(())
}

fn verify_delivery(args: &VerifyArgs) -> Result<bool, SimError> {
    let report = verify::verify(
        &args.oracle,
        &args.ready.join("ready"),
        args.quarantine.as_deref(),
    )?;
    if args.json {
        let text = serde_json::to_string_pretty(&report.to_json())
            .map_err(|e| SimError::Config(format!("render report: {e}")))?;
        println!("{text}");
    } else {
        print!("{}", report.render());
        println!("verify: {}", if report.passed() { "PASS" } else { "FAIL" });
    }
    Ok(report.passed())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Run(args) => run_fleet(args).map(|()| true),
        Command::OracleStats(args) => oracle_stats(args).map(|()| true),
        Command::Verify(args) => verify_delivery(args),
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) if e.is_config() => {
            eprintln!("thadam-simulator: {e}");
            ExitCode::from(2)
        }
        Err(e) => {
            eprintln!("thadam-simulator: {e}");
            ExitCode::FAILURE
        }
    }
}
