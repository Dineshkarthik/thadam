//! Run orchestration: fleet fan-out, signals, and the drain sequence.
//!
//! Shutdown order: stop robot tasks (they stop generating), let the
//! publisher flush scheduled sends and collect PUBACKs up to the drain
//! deadline, then DISCONNECT, fsync the oracle, and summarize.

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rumqttc::{AsyncClient, MqttOptions, NetworkOptions};
use tokio_util::sync::CancellationToken;

use crate::TICK;
use crate::chaos::{self, ChaosOutcome};
use crate::config::Fleet;
use crate::error::SimError;
use crate::generate::{Generated, RobotGen};
use crate::oracle::{Expected, OracleRow, OracleStats, OracleWriter};
use crate::publisher::{LinkEvent, PubItem, Publisher, poll_eventloop};
use crate::stats::SimStats;
use crate::types::topic;

/// Everything one run needs.
#[derive(Debug)]
pub struct RunParams {
    /// The fleet to simulate.
    pub fleet: Fleet,
    /// Broker host.
    pub broker_host: String,
    /// Broker port.
    pub broker_port: u16,
    /// The MQTT client id (stable for one run; clean session).
    pub client_id: String,
    /// The oracle file (`O_EXCL`; must not exist).
    pub oracle_path: PathBuf,
    /// Optional wall-clock cap on generation; `None` runs until signalled.
    pub duration: Option<Duration>,
    /// How long to wait for PUBACKs after generation stops.
    pub drain_timeout: Duration,
}

/// What a run produced.
#[derive(Debug)]
pub struct RunSummary {
    /// Live publish counters.
    pub stats: SimStats,
    /// The oracle aggregation (re-read from the written file, so the summary
    /// describes what is actually on disk).
    pub oracle: OracleStats,
    /// The oracle path.
    pub oracle_path: PathBuf,
}

/// Wall clock in microseconds; an input to generators so tests can pin time.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// A parsed `mqtt://host[:port]` broker address (no TLS on purpose: rumqttc
/// is built without TLS features workspace-wide, and the demo broker is
/// plaintext).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerAddr {
    /// Host name or IP.
    pub host: String,
    /// Port.
    pub port: u16,
}

/// Parses `mqtt://host[:port]`; everything else is a clear error.
pub fn parse_broker_url(url: &str) -> Result<BrokerAddr, SimError> {
    let rest = url.strip_prefix("mqtt://").ok_or_else(|| {
        SimError::BrokerUrl(format!(
            "`{url}` must be `mqtt://host[:port]` (TLS is not built into this binary)"
        ))
    })?;
    if rest.contains(['@', '/', '?', '#']) {
        return Err(SimError::BrokerUrl(format!(
            "`{url}` must be `mqtt://host[:port]` with no credentials or path"
        )));
    }
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => (
            host.to_owned(),
            port.parse::<u16>()
                .ok()
                .filter(|p| *p != 0)
                .ok_or_else(|| SimError::BrokerUrl(format!("`{url}` has an invalid port")))?,
        ),
        _ => (rest.to_owned(), 1883),
    };
    if host.is_empty() {
        return Err(SimError::BrokerUrl(format!("`{url}` has no host")));
    }
    Ok(BrokerAddr { host, port })
}

/// Mirrors the pipeline's `on_change` transform for `expected` prediction
/// (see `examples/edge-capture/thadam/pipeline*.yaml`).
#[derive(Debug)]
struct OnChangeSim {
    heartbeat_us: u64,
    last_passed: Option<((String, u64, String), u64)>,
}

impl OnChangeSim {
    fn new(heartbeat_seconds: f64) -> OnChangeSim {
        OnChangeSim {
            heartbeat_us: (heartbeat_seconds * 1e6) as u64,
            last_passed: None,
        }
    }

    fn decide(&mut self, triple: (String, u64, String), now_us: u64) -> Expected {
        match &self.last_passed {
            Some((last, at_us))
                if *last == triple && now_us.saturating_sub(*at_us) < self.heartbeat_us =>
            {
                Expected::StateRepeat
            }
            _ => {
                self.last_passed = Some((triple, now_us));
                Expected::Deliver
            }
        }
    }
}

/// One robot's task state.
struct RobotTask {
    robot: RobotGen,
    on_change: OnChangeSim,
    chaos_rng: crate::rng::SimRng,
    tx: tokio::sync::mpsc::Sender<PubItem>,
    tick: u64,
}

impl RobotTask {
    fn new(
        robot: RobotGen,
        on_change_heartbeat_seconds: f64,
        seed: u64,
        tx: tokio::sync::mpsc::Sender<PubItem>,
    ) -> RobotTask {
        let robot_index = robot.spec().index;
        RobotTask {
            robot,
            on_change: OnChangeSim::new(on_change_heartbeat_seconds),
            chaos_rng: crate::rng::SimRng::new(seed, &format!("robot-{robot_index}/chaos")),
            tx,
            tick: 0,
        }
    }

    /// One tick: generate, apply chaos, build publish items, enqueue.
    async fn tick_once(&mut self, fleet: &Fleet) {
        self.tick += 1;
        let now = now_us();
        let generated = {
            let rates = fleet.config.rates;
            self.robot.tick(self.tick, now, &rates)
        };
        for mut generated in generated {
            let outcome = chaos::apply(
                &fleet.config.chaos,
                &self.robot.spec().name,
                &mut generated,
                &mut self.chaos_rng,
            );
            let items = self.build_items(fleet, generated, outcome, now);
            for item in items {
                if self.tx.send(item).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Builds the original publish plus the optional scheduled duplicate,
    /// with oracle rows for both.
    fn build_items(
        &mut self,
        fleet: &Fleet,
        generated: Generated,
        outcome: ChaosOutcome,
        now_us: u64,
    ) -> Vec<PubItem> {
        let base_expected = if outcome.malformed {
            Expected::Malformed
        } else if generated.event_level == Some("debug") {
            Expected::FilteredDebug
        } else if let Some(triple) = generated.state_fields.clone() {
            self.on_change.decide(triple, now_us)
        } else {
            Expected::Deliver
        };
        let mut flags: Vec<String> = generated
            .flags
            .iter()
            .chain(outcome.extra_flags.iter())
            .map(|f| (*f).to_owned())
            .collect();
        flags.sort();
        flags.dedup();
        let (site, robot) = (self.robot.spec().site.as_str(), &*self.robot.spec().name);
        let rows = oracle_rows(site, robot, &generated, base_expected, &flags);
        let send_at = Instant::now() + outcome.delay;
        let topic = topic(&fleet.config.topic_prefix, site, robot, generated.stream);
        let mut items = Vec::with_capacity(2);
        items.push(PubItem {
            topic: topic.clone(),
            stream: generated.stream,
            bytes: outcome.bytes.clone(),
            send_at,
            rows,
        });
        if let Some(dup_delay) = outcome.duplicate_after {
            let dup_rows = oracle_rows(site, robot, &generated, Expected::DuplicateOf, &flags);
            items.push(PubItem {
                topic,
                stream: generated.stream,
                bytes: outcome.bytes,
                send_at: send_at + dup_delay,
                rows: dup_rows,
            });
        }
        items
    }
}

fn oracle_rows(
    site: &str,
    robot: &str,
    generated: &Generated,
    expected: Expected,
    flags: &[String],
) -> Vec<OracleRow> {
    generated
        .samples
        .iter()
        .map(|(sample_idx, ts_us)| OracleRow {
            site: site.to_owned(),
            robot: robot.to_owned(),
            stream: generated.stream.as_str().to_owned(),
            seq: generated.seq,
            sample_idx: *sample_idx,
            ts_us: *ts_us,
            publish_acked: false,
            expected,
            flags: flags.to_vec(),
        })
        .collect()
}

/// Runs the fleet until `--duration` ends or a signal arrives; then drains,
/// fsyncs, and summarizes.
pub async fn run(params: RunParams) -> Result<RunSummary, SimError> {
    let oracle = OracleWriter::create(&params.oracle_path)?;

    let mut options = MqttOptions::new(
        params.client_id.clone(),
        params.broker_host.clone(),
        params.broker_port,
    );
    options
        .set_keep_alive(Duration::from_secs(15))
        .set_clean_session(true)
        .set_pending_throttle(Duration::ZERO);
    let mut network = NetworkOptions::new();
    network.set_connection_timeout(10);
    network.set_tcp_nodelay(true);

    // Fail fast on an unresolvable name instead of retrying forever inside
    // the event loop (a typo'd broker should exit, not spin).
    let addr = format!("{}:{}", params.broker_host, params.broker_port);
    if addr
        .to_socket_addrs()
        .map(|mut i| i.next().is_some())
        .unwrap_or(false)
    {
        // resolved
    } else {
        return Err(SimError::BrokerUrl(format!("cannot resolve `{addr}`")));
    }

    let (client, mut event_loop) = AsyncClient::new(options, 4096);
    event_loop.set_network_options(network);
    let (link_tx, link_rx) = tokio::sync::mpsc::unbounded_channel::<LinkEvent>();
    let poll_task = tokio::spawn(poll_eventloop(event_loop, link_tx));

    let (pub_tx, pub_rx) = tokio::sync::mpsc::channel::<PubItem>(8192);
    let drain_deadline = CancellationToken::new();
    let publisher = Publisher::new(client, pub_rx, link_rx, oracle);
    let publisher_task = tokio::spawn(publisher.run(drain_deadline.clone()));

    let shutdown = CancellationToken::new();
    let mut robot_tasks = Vec::with_capacity(params.fleet.robots.len());
    for (i, spec) in params.fleet.robots.iter().enumerate() {
        let robot = RobotGen::new(spec.clone(), &params.fleet.config);
        let on_change_hb = params.fleet.config.state.on_change_heartbeat_seconds;
        let seed = params.fleet.config.seed;
        let mut task = RobotTask::new(robot, on_change_hb, seed, pub_tx.clone());
        let fleet = params.fleet.clone();
        let robot_shutdown = shutdown.clone();
        robot_tasks.push(tokio::spawn(async move {
            // Stagger robots deterministically to avoid a herd at t=0.
            tokio::time::sleep(Duration::from_millis(i as u64 % 10 * 7)).await;
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = robot_shutdown.cancelled() => break,
                    _ = interval.tick() => task.tick_once(&fleet).await,
                }
            }
            task.tick
        }));
    }
    drop(pub_tx);

    // Wait for the duration cap or a termination signal.
    let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(sig) => Some(sig),
        Err(e) => {
            tracing::warn!(%e, "cannot listen for SIGTERM; SIGINT still works");
            None
        }
    };
    let duration = params.duration;
    tokio::select! {
        _ = async {
            if let Some(d) = duration {
                tokio::time::sleep(d).await
            } else {
                std::future::pending::<()>().await
            }
        } => {
            tracing::info!("duration reached; draining");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received SIGINT; draining");
        }
        _ = async {
            if let Some(sig) = &mut sigterm {
                let _ = sig.recv().await;
            } else {
                std::future::pending::<()>().await
            }
        } => {
            tracing::info!("received SIGTERM; draining");
        }
    }

    shutdown.cancel();
    let mut ticks = 0u64;
    for handle in robot_tasks {
        if let Ok(t) = handle.await {
            ticks += t;
        }
    }
    // The last robot dropping its sender closes the publisher's input; it
    // now has the rest of the drain budget to publish and collect PUBACKs.
    let deadline_trigger = drain_deadline.clone();
    let drain_timeout = params.drain_timeout;
    tokio::spawn(async move {
        tokio::time::sleep(drain_timeout).await;
        deadline_trigger.cancel();
    });
    let stats = publisher_task
        .await
        .map_err(|e| SimError::Publish(format!("publisher task failed: {e}")))?;
    poll_task.abort();
    let _ = poll_task.await;

    let oracle_stats = OracleStats::load(&params.oracle_path)?;
    tracing::info!(ticks, "{}", stats.summary_line());
    Ok(RunSummary {
        stats,
        oracle: oracle_stats,
        oracle_path: params.oracle_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_url_parsing() {
        let addr = parse_broker_url("mqtt://localhost").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            addr,
            BrokerAddr {
                host: "localhost".to_owned(),
                port: 1883
            }
        );
        let addr = parse_broker_url("mqtt://mosquitto:21883").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            addr,
            BrokerAddr {
                host: "mosquitto".to_owned(),
                port: 21883
            }
        );
        for bad in [
            "mqtts://broker:8883",
            "tcp://broker",
            "mqtt://",
            "mqtt://h:0",
            "mqtt://h:99999",
            "mqtt://user:pw@h",
            "mqtt://h/path",
        ] {
            assert!(parse_broker_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn on_change_first_seen_and_changes_deliver() {
        let mut sim = OnChangeSim::new(300.0);
        let idle = ("IDLE".to_owned(), 0_u64, "p".to_owned());
        let running = ("RUNNING".to_owned(), 1_u64, "p".to_owned());
        assert_eq!(sim.decide(idle.clone(), 1_000_000), Expected::Deliver);
        // Identical triple within the heartbeat window is a repeat.
        assert_eq!(sim.decide(idle.clone(), 60_000_000), Expected::StateRepeat);
        // Heartbeat republishes every 60 s stay repeats while the window is 5 m.
        assert_eq!(sim.decide(idle.clone(), 120_000_000), Expected::StateRepeat);
        // A change delivers even inside the window.
        assert_eq!(sim.decide(running.clone(), 130_000_000), Expected::Deliver);
        // ...and the last passed moves to the new triple.
        assert_eq!(sim.decide(idle, 140_000_000), Expected::Deliver);
        // Same triple after the heartbeat window passes again (deliver).
        assert_eq!(sim.decide(running.clone(), 431_000_000), Expected::Deliver);
    }
}
