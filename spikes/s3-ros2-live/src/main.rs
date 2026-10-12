//! Thadam spike S3 step 1: raw-zenoh subscriber against Jazzy rmw_zenoh.
//!
//! Subcommands:
//!   sniff     dump raw samples + liveliness tokens as JSON lines (discovery)
//!   discover  list advertised topics from rmw_zenoh liveliness tokens
//!   capture   subscribe the sensor-mix matrix, decode with thadam-ros2-msg,
//!             and print a JSON measurement summary on stdout
//!   probe     query-selector reachability matrix (late-join mapping)

mod capture;
mod metrics;
mod msgs;
mod probe;
mod sniff;
mod transport;
mod zenoh_impl;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "s3-ros2-live",
    about = "S3 spike: raw zenoh vs Jazzy rmw_zenoh"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Sniff(TimingArgs),
    Discover(DiscoverArgs),
    Capture(CaptureCliArgs),
    Probe(ProbeArgs),
}

#[derive(Args)]
struct ProbeArgs {
    #[arg(long, default_value = "tcp/127.0.0.1:7447")]
    endpoint: String,
    #[arg(long, default_value = "client")]
    mode: String,
    /// Declare probe/self, settle 1 s, then a single default-config get().
    #[arg(long, default_value_t = false)]
    selftest: bool,
    /// Declare probe/self and hold it open for N seconds (no gets).
    #[arg(long, default_value_t = 0)]
    hold_secs: u64,
}

#[derive(Args)]
struct TimingArgs {
    #[arg(long, default_value = "tcp/127.0.0.1:7447")]
    endpoint: String,
    #[arg(long, default_value_t = 0)]
    domain: u32,
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value = "client")]
    mode: String,
}

#[derive(Args)]
struct DiscoverArgs {
    #[arg(long, default_value = "tcp/127.0.0.1:7447")]
    endpoint: String,
    #[arg(long, default_value_t = 0)]
    domain: u32,
    #[arg(long, default_value_t = 3)]
    settle_secs: u64,
    #[arg(long, default_value = "client")]
    mode: String,
}

#[derive(Args)]
struct CaptureCliArgs {
    #[arg(long, default_value = "tcp/127.0.0.1:7447")]
    endpoint: String,
    #[arg(long, default_value_t = 0)]
    domain: u32,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    #[arg(long, default_value = "client")]
    mode: String,
    /// Artificial per-message processing delay (overload test).
    #[arg(long, default_value_t = 0)]
    slow_ms: u64,
    /// Per-subscription zenoh receive channel capacity.
    #[arg(long, default_value_t = 256)]
    fifo_cap: usize,
    /// Comma-separated restrict list, e.g. --only /imu,/tf_static.
    #[arg(long)]
    only: Option<String>,
    /// host_ns - container_ns offset applied to source timestamps when
    /// computing end-to-end latency (measured by scripts/run-capture.sh).
    #[arg(long, default_value_t = 0)]
    clock_offset_ns: i64,
    /// Force VOLATILE durability everywhere (proves late-join query need).
    #[arg(long, default_value_t = false)]
    no_late_join: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Sniff(a) => sniff::run(&a.endpoint, a.domain, a.duration_secs, &a.mode)
            .await
            .map(|_| String::new()),
        Command::Discover(a) => discover(&a).await,
        Command::Capture(a) => {
            capture::run(capture::CaptureArgs {
                endpoint: a.endpoint,
                domain: a.domain,
                duration_secs: a.duration_secs,
                mode: a.mode,
                slow_ms: a.slow_ms,
                fifo_cap: a.fifo_cap,
                only: a.only,
                clock_offset_ns: a.clock_offset_ns,
                no_late_join: a.no_late_join,
            })
            .await
        }
        Command::Probe(a) => probe::run(&a.endpoint, &a.mode, a.selftest, a.hold_secs).await,
    };
    match result {
        Ok(json) => {
            if !json.is_empty() {
                println!("{json}");
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

async fn discover(a: &DiscoverArgs) -> Result<String, String> {
    let transport = zenoh_impl::ZenohTransport::connect(&a.endpoint, a.domain, 256, &a.mode)
        .await
        .map_err(|e| e.to_string())?;
    let adverts = transport::LiveRos2Transport::discover(
        &transport,
        std::time::Duration::from_secs(a.settle_secs),
    )
    .await;
    serde_json::to_string_pretty(&adverts).map_err(|e| format!("serialize: {e}"))
}
