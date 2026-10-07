mod cancel;
mod common;
mod keepalive;
mod order;
mod oversize;
mod session;
mod stall;
mod strip;
mod throughput;
mod v5;
mod wire;

use anyhow::Result;
use clap::{Parser, Subcommand};

use common::Target;

#[derive(Parser)]
#[command(name = "s2", about = "Thadam spike S2: MQTT source experiments")]
struct Cli {
    #[arg(long, global = true, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, global = true, default_value_t = 18831)]
    port: u16,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Q1: ack-window throughput.
    Throughput(throughput::Args),
    /// Baseline publish->PUBACK round trip on the path used for Q1.
    Rtt(throughput::RttArgs),
    /// Q2: keepalive under backpressure (withhold acks) and the stop-polling anti-pattern.
    Keepalive(keepalive::Args),
    /// Q3: drop `EventLoop::poll` futures at random points.
    Cancel(cancel::Args),
    /// Q3 (targeted): drop poll futures while the socket write side is blocked.
    CancelStall(stall::Args),
    /// MQTT 5 module smoke test (receive_maximum, sessions, stripped acks).
    V5(v5::Args),
    /// Extra E2: oversized message vs the default 10 KiB incoming packet limit.
    Oversize(oversize::Args),
    /// Q4: ack with a payload-stripped Publish.
    Strip(strip::Args),
    /// Q6: PUBACK wire order vs receive order.
    Order(order::Args),
    /// Q5 helper: persistent-session subscriber process (kill -9 target).
    SessSub(session::SubArgs),
    /// Q5 helper: publish N QoS 1 messages and count the publisher's PUBACKs.
    SessPub(session::PubArgs),
    /// Q5 helper: summarise sess-sub logs against an expected sequence range.
    SessReport(session::ReportArgs),
    /// Extra E1: stale acks after session loss.
    StaleAck(session::StaleArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let t = Target {
        host: cli.host,
        port: cli.port,
    };
    match cli.cmd {
        Cmd::Throughput(a) => throughput::run(&t, a).await,
        Cmd::Rtt(a) => throughput::rtt(&t, a).await,
        Cmd::Keepalive(a) => keepalive::run(&t, a).await,
        Cmd::Cancel(a) => cancel::run(&t, a).await,
        Cmd::CancelStall(a) => stall::run(&t, a).await,
        Cmd::V5(a) => v5::run(&t, a).await,
        Cmd::Oversize(a) => oversize::run(&t, a).await,
        Cmd::Strip(a) => strip::run(&t, a).await,
        Cmd::Order(a) => order::run(&t, a).await,
        Cmd::SessSub(a) => session::sess_sub(&t, a).await,
        Cmd::SessPub(a) => session::sess_pub(&t, a).await,
        Cmd::SessReport(a) => session::sess_report(a),
        Cmd::StaleAck(a) => session::stale_ack(&t, a).await,
    }
}
