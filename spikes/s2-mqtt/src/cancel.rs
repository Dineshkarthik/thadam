//! Q3: empirical cancel-safety of `EventLoop::poll`.
//!
//! The poll future is dropped at random points (timeout / select! / poll-once-then-drop)
//! while a QoS 1 stream with manual acks is flowing. We check, per run:
//!   * every published sequence is delivered at least once (no loss)
//!   * no non-DUP duplicates (a duplicate without the DUP flag means the client or the
//!     broker duplicated a delivery outside of redelivery)
//!   * the connection never breaks (connacks == 1, no poll errors)
//!   * every ack reached the broker (a fresh persistent session sees zero redelivery)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args as ClapArgs;
use futures_util::FutureExt;
use rand::Rng;
use rumqttc::{AsyncClient, Event, Incoming, Outgoing, QoS};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use crate::common::*;

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    /// none | timeout | select | now_or_never | mixed
    #[arg(long)]
    pub style: String,
    /// Upper bound (exclusive) of the random cancel deadline, in microseconds.
    #[arg(long, default_value_t = 1000)]
    pub max_us: u64,
    #[arg(long, default_value_t = 100_000)]
    pub count: u64,
    #[arg(long, default_value_t = 128)]
    pub payload: usize,
    #[arg(long)]
    pub broker: String,
    #[arg(long)]
    pub broker_inflight: u32,
    #[arg(long, default_value_t = 180)]
    pub max_secs: u64,
    #[arg(long, default_value_t = 2)]
    pub keepalive: u64,
    /// Apply cancellation from the very first poll (including the connect phase).
    #[arg(long, default_value_t = false)]
    pub from_start: bool,
    /// Publisher rate limit in msgs/s (0 = unpaced).
    #[arg(long, default_value_t = 0)]
    pub pub_rate: u64,
    /// With --count 0: run idle for this many seconds (keepalive under cancellation).
    #[arg(long, default_value_t = 30)]
    pub idle_secs: u64,
}

enum Out {
    Done,
    Pending,
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    let topic = rand_id("s2/cx");
    let cid = rand_id("s2-cx-sub");
    let opts = OptsBuilder::new(&cid, t)
        .keep_alive(a.keepalive)
        .clean(false)
        .manual_acks(true)
        .build();
    let (client, mut el) = AsyncClient::new(opts, 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;

    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<rumqttc::Publish>();
    let acker = {
        let client = client.clone();
        tokio::spawn(async move {
            while let Some(p) = ack_rx.recv().await {
                if client.ack(&p).await.is_err() {
                    break;
                }
            }
        })
    };

    let received = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicU64::new(0));
    let pub_done = Arc::new(AtomicBool::new(false));
    let publisher = {
        let (received, ready, published, pub_done) = (
            received.clone(),
            ready.clone(),
            published.clone(),
            pub_done.clone(),
        );
        let (t, topic) = (t.clone(), topic.clone());
        let (count, size, rate) = (a.count, a.payload, a.pub_rate);
        tokio::spawn(async move {
            let opts = OptsBuilder::new(rand_id("s2-cx-pub"), &t)
                .inflight(1000)
                .build();
            let (c, mut pel) = AsyncClient::new(opts, 1000);
            tokio::spawn(async move {
                loop {
                    if pel.poll().await.is_err() {
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            });
            while !ready.load(Relaxed) {
                sleep(Duration::from_millis(5)).await;
            }
            let pstart = Instant::now();
            for seq in 0..count {
                if rate > 0 {
                    let due = Duration::from_secs_f64(seq as f64 / rate as f64);
                    if let Some(w) = due.checked_sub(pstart.elapsed()) {
                        sleep(w).await;
                    }
                }
                while published
                    .load(Relaxed)
                    .saturating_sub(received.load(Relaxed))
                    > 3_000
                {
                    sleep(Duration::from_millis(1)).await;
                }
                if c.publish_bytes(
                    topic.clone(),
                    QoS::AtLeastOnce,
                    false,
                    make_payload(seq, size),
                )
                .await
                .is_err()
                {
                    break;
                }
                published.fetch_add(1, Relaxed);
            }
            pub_done.store(true, Relaxed);
        })
    };

    let mut seqs: HashMap<u64, (u32, u32)> = HashMap::new(); // seq -> (non-dup deliveries, dup-flag deliveries)
    let (mut connacks, mut errors, mut suback, mut pingresp, mut wire_acks) =
        (0u64, 0u64, false, 0u64, 0u64);
    let (mut cancelled, mut completed) = (0u64, 0u64);
    let mut inflight_state_max = 0u16;
    let start = Instant::now();
    let mut rng = rand::rng();
    let cancel_enabled = |connacks: u64, suback: bool| a.from_start || (connacks > 0 && suback);

    loop {
        let enabled = a.style != "none" && cancel_enabled(connacks, suback);
        let r = if !enabled {
            Some(el.poll().await)
        } else {
            let d = Duration::from_nanos(rng.random_range(0..a.max_us.max(1) * 1000));
            let eff = if a.style == "mixed" {
                ["now_or_never", "timeout", "select", "plain"][rng.random_range(0..4)]
            } else {
                a.style.as_str()
            };
            match eff {
                "plain" => Some(el.poll().await),
                "timeout" => timeout(d, el.poll()).await.ok(),
                "select" => tokio::select! {
                    r = el.poll() => Some(r),
                    _ = sleep(d) => None,
                },
                "now_or_never" => {
                    let r = el.poll().now_or_never();
                    if r.is_none() {
                        tokio::task::yield_now().await;
                    }
                    r
                }
                s => anyhow::bail!("unknown style {s}"),
            }
        };
        let out = match r {
            None => Out::Pending,
            Some(r) => {
                match r {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        if let Some(s) = parse_seq(&p.payload) {
                            let e = seqs.entry(s).or_default();
                            if p.dup {
                                e.1 += 1;
                            } else {
                                e.0 += 1;
                            }
                        }
                        received.fetch_add(1, Relaxed);
                        let _ = ack_tx.send(strip(&p));
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => connacks += 1,
                    Ok(Event::Incoming(Incoming::SubAck(_))) => {
                        suback = true;
                        ready.store(true, Relaxed);
                    }
                    Ok(Event::Incoming(Incoming::PingResp)) => pingresp += 1,
                    Ok(Event::Outgoing(Outgoing::PubAck(_))) => wire_acks += 1,
                    Ok(_) => {}
                    Err(e) => {
                        errors += 1;
                        eprintln!("poll error: {e}");
                        sleep(Duration::from_millis(100)).await;
                    }
                }
                inflight_state_max = inflight_state_max.max(el.state.inflight());
                Out::Done
            }
        };
        match out {
            Out::Done => completed += 1,
            Out::Pending => cancelled += 1,
        }

        if a.count == 0 {
            if start.elapsed() > Duration::from_secs(a.idle_secs) {
                break;
            }
        } else {
            let uniq = seqs.len() as u64;
            let total_rx: u64 = seqs.values().map(|(a, b)| (*a + *b) as u64).sum();
            if pub_done.load(Relaxed) && uniq >= a.count && wire_acks >= total_rx {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(a.max_secs) {
            eprintln!("max_secs reached");
            break;
        }
    }
    let elapsed = start.elapsed();
    let uniq = seqs.len() as u64;
    let missing: Vec<u64> = (0..a.count)
        .filter(|s| !seqs.contains_key(s))
        .take(10)
        .collect();
    let missing_n = (0..a.count).filter(|s| !seqs.contains_key(s)).count();
    let non_dup_dups: u64 = seqs
        .values()
        .map(|(n, _)| (*n as u64).saturating_sub(1))
        .sum();
    let dup_flagged: u64 = seqs.values().map(|(_, d)| *d as u64).sum();

    // Let in-flight acks drain, then drop the session abruptly and look at the broker's view.
    sleep(Duration::from_millis(500)).await;
    acker.abort();
    publisher.abort();
    drop(el);
    drop(client);
    let vopts = OptsBuilder::new(&cid, t)
        .clean(false)
        .manual_acks(false)
        .build();
    let (vc, mut vel) = AsyncClient::new(vopts, 16);
    let _ = vc.subscribe(&topic, QoS::AtLeastOnce).await;
    let vstart = Instant::now();
    let mut redelivered = 0u64;
    let mut session_present = None;
    while vstart.elapsed() < Duration::from_secs(3) {
        if let Ok(Ok(ev)) = timeout(Duration::from_millis(200), vel.poll()).await {
            match ev {
                Event::Incoming(Incoming::Publish(_)) => redelivered += 1,
                Event::Incoming(Incoming::ConnAck(c)) => session_present = Some(c.session_present),
                _ => {}
            }
        }
    }

    json_line(json!({
        "test": "cancel_safety",
        "style": a.style,
        "max_us": a.max_us,
        "from_start": a.from_start,
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "published": published.load(Relaxed),
        "unique_delivered": uniq,
        "missing_count": missing_n,
        "missing_first": missing,
        "non_dup_duplicates": non_dup_dups,
        "dup_flagged_deliveries": dup_flagged,
        "poll_cancelled": cancelled,
        "poll_completed": completed,
        "connacks": connacks,
        "poll_errors": errors,
        "pingresp": pingresp,
        "wire_acks_emitted": wire_acks,
        "max_outgoing_inflight_state": inflight_state_max,
        "verify_session_present": session_present,
        "verify_redelivered_after_reconnect": redelivered,
        "elapsed_s": elapsed.as_secs_f64(),
    }));
    Ok(())
}
