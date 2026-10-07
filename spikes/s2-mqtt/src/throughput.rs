//! Q1: ack-window throughput. One subscriber connection (manual acks, QoS 1) whose
//! acks are delayed by a simulated commit latency, fed by a saturating publisher
//! that is throttled only by a backlog cap (so the broker always has data queued).

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Args as ClapArgs;
use rumqttc::{AsyncClient, Event, Incoming, Outgoing, QoS};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, sleep_until};

use crate::common::*;

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    /// Label of the broker under test (reported only).
    #[arg(long)]
    pub broker: String,
    /// The broker's configured max inflight (reported + used for the predicted ceiling).
    #[arg(long)]
    pub broker_inflight: u32,
    /// Simulated commit latency in ms (ack is sent at receive_time + latency).
    #[arg(long, default_value_t = 0)]
    pub latency_ms: u64,
    /// Seconds of warm-up before the measured window.
    #[arg(long, default_value_t = 3)]
    pub warmup: u64,
    /// Seconds in the measured window.
    #[arg(long, default_value_t = 10)]
    pub duration: u64,
    #[arg(long, default_value_t = 256)]
    pub payload: usize,
    #[arg(long, default_value_t = 4)]
    pub publishers: usize,
    /// Max (published - received) before publishers pause.
    #[arg(long, default_value_t = 20_000)]
    pub backlog_cap: u64,
    /// Baseline: let rumqttc auto-ack on receive (manual_acks=false).
    #[arg(long, default_value_t = false)]
    pub auto_ack: bool,
    /// rumqttc request channel capacity for the subscriber (plan default: 64).
    #[arg(long, default_value_t = 64)]
    pub req_cap: usize,
}

#[derive(Default)]
struct Ctr {
    received: AtomicU64,
    wire_acks: AtomicU64,
    unacked: AtomicI64,
    max_unacked: AtomicI64,
    connacks: AtomicU64,
    errors: AtomicU64,
    suback: AtomicBool,
}

struct Item {
    stripped: rumqttc::Publish,
    at: Instant,
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    let topic = rand_id("s2/tp");
    let ctr = Arc::new(Ctr::default());
    let published = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let opts = OptsBuilder::new(rand_id("s2-tp-sub"), t)
        .manual_acks(!a.auto_ack)
        .build();
    let (client, mut el) = AsyncClient::new(opts, a.req_cap);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<Item>();
    let liveness = {
        let ctr = ctr.clone();
        let manual = !a.auto_ack;
        tokio::spawn(async move {
            loop {
                match el.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        ctr.received.fetch_add(1, Relaxed);
                        if manual {
                            let u = ctr.unacked.fetch_add(1, Relaxed) + 1;
                            ctr.max_unacked.fetch_max(u, Relaxed);
                            let _ = tx.send(Item {
                                stripped: strip(&p),
                                at: Instant::now(),
                            });
                        }
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        ctr.connacks.fetch_add(1, Relaxed);
                    }
                    Ok(Event::Incoming(Incoming::SubAck(_))) => ctr.suback.store(true, Relaxed),
                    Ok(Event::Outgoing(Outgoing::PubAck(_))) => {
                        ctr.wire_acks.fetch_add(1, Relaxed);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        ctr.errors.fetch_add(1, Relaxed);
                        eprintln!("subscriber poll error: {e}");
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        })
    };

    let t0 = Instant::now();
    let latency = Duration::from_millis(a.latency_ms);
    let acker = {
        let ctr = ctr.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let mut delays: Vec<(u64, u64)> = Vec::new();
            while let Some(it) = rx.recv().await {
                if !latency.is_zero() {
                    let due = it.at + latency;
                    if due > Instant::now() {
                        sleep_until(tokio::time::Instant::from_std(due)).await;
                    }
                }
                if client.ack(&it.stripped).await.is_err() {
                    break;
                }
                ctr.unacked.fetch_sub(1, Relaxed);
                let now = Instant::now();
                delays.push((
                    now.duration_since(t0).as_millis() as u64,
                    now.duration_since(it.at).as_micros() as u64,
                ));
            }
            delays
        })
    };

    while !ctr.suback.load(Relaxed) {
        sleep(Duration::from_millis(10)).await;
    }

    let payload = make_payload(0, a.payload);
    let mut pubs = Vec::new();
    for i in 0..a.publishers {
        pubs.push(spawn_publisher(
            t.clone(),
            i,
            topic.clone(),
            payload.clone(),
            published.clone(),
            ctr.clone(),
            a.backlog_cap,
            stop.clone(),
        ));
    }

    sleep(Duration::from_secs(a.warmup)).await;
    let w0 = Instant::now();
    let s0 = (ctr.received.load(Relaxed), ctr.wire_acks.load(Relaxed));
    let mut backlog_min = u64::MAX;
    let mut unacked_samples: Vec<i64> = Vec::new();
    while w0.elapsed() < Duration::from_secs(a.duration) {
        sleep(Duration::from_millis(50)).await;
        let b = published
            .load(Relaxed)
            .saturating_sub(ctr.received.load(Relaxed));
        backlog_min = backlog_min.min(b);
        unacked_samples.push(ctr.unacked.load(Relaxed));
    }
    let el_s = w0.elapsed().as_secs_f64();
    let s1 = (ctr.received.load(Relaxed), ctr.wire_acks.load(Relaxed));
    let w_end_ms = w0.elapsed().as_millis() as u64 + w0.duration_since(t0).as_millis() as u64;
    let w_start_ms = w0.duration_since(t0).as_millis() as u64;

    stop.store(true, Relaxed);
    let max_unacked = ctr.max_unacked.load(Relaxed);
    let connacks = ctr.connacks.load(Relaxed);
    let errors = ctr.errors.load(Relaxed);
    for p in pubs {
        p.abort();
    }
    liveness.abort();
    let mut delays = acker.await.unwrap_or_default();
    delays.retain(|(t, _)| *t >= w_start_ms && *t <= w_end_ms);
    let mut d: Vec<u64> = delays.iter().map(|(_, d)| *d).collect();
    d.sort_unstable();
    let mean_us = if d.is_empty() {
        0.0
    } else {
        d.iter().sum::<u64>() as f64 / d.len() as f64
    };

    let recv_rate = (s1.0 - s0.0) as f64 / el_s;
    let ack_rate = (s1.1 - s0.1) as f64 / el_s;
    let nominal = if a.latency_ms > 0 {
        Some(a.broker_inflight as f64 / (a.latency_ms as f64 / 1000.0))
    } else {
        None
    };
    let mean_unacked = if unacked_samples.is_empty() {
        0.0
    } else {
        unacked_samples.iter().sum::<i64>() as f64 / unacked_samples.len() as f64
    };
    json_line(json!({
        "test": "throughput",
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "latency_ms": a.latency_ms,
        "auto_ack": a.auto_ack,
        "payload": a.payload,
        "publishers": a.publishers,
        "window_s": el_s,
        "recv_per_s": recv_rate,
        "wire_ack_per_s": ack_rate,
        "predicted_ceiling_nominal": nominal,
        "ratio_to_nominal": nominal.map(|n| recv_rate / n),
        "ack_delay_mean_us": mean_us,
        "ack_delay_p50_us": pct(&d, 0.5),
        "ack_delay_p99_us": pct(&d, 0.99),
        "predicted_ceiling_from_measured_delay": if mean_us > 0.0 { Some(a.broker_inflight as f64 / (mean_us / 1e6)) } else { None },
        "max_unacked_observed": max_unacked,
        "mean_unacked_in_window": mean_unacked,
        "min_backlog_in_window": backlog_min,
        "supply_limited": backlog_min < a.broker_inflight as u64 * 2,
        "connacks": connacks,
        "poll_errors": errors,
    }));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_publisher(
    t: Target,
    idx: usize,
    topic: String,
    payload: Bytes,
    published: Arc<AtomicU64>,
    ctr: Arc<Ctr>,
    backlog_cap: u64,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let opts = OptsBuilder::new(rand_id(&format!("s2-tp-pub{idx}")), &t)
            .inflight(1000)
            .build();
        let (client, mut el) = AsyncClient::new(opts, 1000);
        tokio::spawn(async move {
            loop {
                if el.poll().await.is_err() {
                    sleep(Duration::from_millis(200)).await;
                }
            }
        });
        while !stop.load(Relaxed) {
            if published
                .load(Relaxed)
                .saturating_sub(ctr.received.load(Relaxed))
                > backlog_cap
            {
                sleep(Duration::from_millis(1)).await;
                continue;
            }
            if client
                .publish_bytes(topic.clone(), QoS::AtLeastOnce, false, payload.clone())
                .await
                .is_err()
            {
                break;
            }
            published.fetch_add(1, Relaxed);
        }
    })
}

#[derive(ClapArgs, Clone, Debug)]
pub struct RttArgs {
    #[arg(long, default_value_t = 1000)]
    pub n: usize,
}

/// Round-trip of a QoS 1 publish to its PUBACK, sequentially: baseline network +
/// broker latency of the (Docker port-forwarded) path used for Q1.
pub async fn rtt(t: &Target, a: RttArgs) -> Result<()> {
    let opts = OptsBuilder::new(rand_id("s2-rtt"), t).build();
    let (client, mut el) = AsyncClient::new(opts, 16);
    let topic = rand_id("s2/rtt");
    let mut samples = Vec::new();
    let mut sent = 0usize;
    let mut sent_at = Instant::now();
    let mut connected = false;
    while samples.len() < a.n {
        match el.poll().await? {
            Event::Incoming(Incoming::ConnAck(_)) => connected = true,
            Event::Incoming(Incoming::PubAck(_)) => {
                samples.push(sent_at.elapsed().as_micros() as u64);
                if sent < a.n {
                    sent_at = Instant::now();
                    client
                        .publish_bytes(
                            topic.clone(),
                            QoS::AtLeastOnce,
                            false,
                            Bytes::from_static(b"x"),
                        )
                        .await?;
                    sent += 1;
                }
            }
            _ => {}
        }
        if connected && sent == 0 {
            sent_at = Instant::now();
            client
                .publish_bytes(
                    topic.clone(),
                    QoS::AtLeastOnce,
                    false,
                    Bytes::from_static(b"x"),
                )
                .await?;
            sent += 1;
        }
    }
    samples.sort_unstable();
    json_line(json!({
        "test": "rtt_publish_to_puback",
        "n": samples.len(),
        "p50_us": pct(&samples, 0.5),
        "p90_us": pct(&samples, 0.9),
        "p99_us": pct(&samples, 0.99),
        "min_us": samples.first(),
    }));
    Ok(())
}
