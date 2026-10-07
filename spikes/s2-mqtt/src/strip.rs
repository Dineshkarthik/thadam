//! Q4: ack with a payload-stripped Publish.
//!
//! `--mode stripped` keeps only `strip(&publish)` (same pkid/qos, empty topic and payload)
//! for the held-until-commit window; `--mode retained` keeps the full Publish. Run as
//! separate processes so RSS is comparable. Wire truth comes from the proxy; the broker's
//! view is checked by reconnecting the persistent session and counting redeliveries.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args as ClapArgs;
use rumqttc::{AsyncClient, Event, Incoming, Outgoing, Publish};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use crate::common::*;
use crate::wire::{self, Dir, Pkt};

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    /// stripped | retained
    #[arg(long)]
    pub mode: String,
    #[arg(long, default_value_t = 1)]
    pub qos: u8,
    #[arg(long, default_value_t = 64)]
    pub payload_kib: usize,
    #[arg(long, default_value_t = 3000)]
    pub count: u64,
    #[arg(long)]
    pub broker: String,
    #[arg(long)]
    pub broker_inflight: u32,
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    let (proxy_port, wlog) = wire::start(format!("{}:{}", t.host, t.port)).await?;
    let via = Target {
        host: "127.0.0.1".into(),
        port: proxy_port,
    };
    let qos = qos_from(a.qos);
    let topic = rand_id("s2/strip");
    let cid = rand_id("s2-strip-sub");
    let rss_base = rss_kib();

    let opts = OptsBuilder::new(&cid, &via)
        .clean(false)
        .manual_acks(true)
        .build();
    let (client, mut el) = AsyncClient::new(opts, 64);
    client.subscribe(&topic, qos).await?;

    // Publisher starts once subscribed.
    let mut subscribed = false;
    while !subscribed {
        if let Event::Incoming(Incoming::SubAck(_)) = el.poll().await? {
            subscribed = true;
        }
    }
    // Publisher runs as a separate process so its buffers do not pollute this process's RSS.
    let publisher = tokio::process::Command::new(std::env::current_exe()?)
        .args([
            "--host",
            &t.host,
            "--port",
            &t.port.to_string(),
            "sess-pub",
            "--topic",
            &topic,
            "--count",
            &a.count.to_string(),
            "--size",
            &(a.payload_kib * 1024).to_string(),
            "--qos",
            &a.qos.to_string(),
        ])
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;

    // Phase A: hold every delivery unacked until the broker stops sending.
    let mut held: Vec<Publish> = Vec::new();
    let mut seqs: HashMap<u64, u32> = HashMap::new();
    let mut last_rx = Instant::now();
    let want_hold = a.count.min(a.broker_inflight as u64) as usize;
    loop {
        match timeout(Duration::from_millis(200), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                if let Some(s) = parse_seq(&p.payload) {
                    *seqs.entry(s).or_default() += 1;
                }
                held.push(if a.mode == "stripped" { strip(&p) } else { p });
                last_rx = Instant::now();
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => anyhow::bail!("poll error during hold: {e}"),
            Err(_) => {}
        }
        if held.len() >= want_hold && last_rx.elapsed() > Duration::from_millis(1500) {
            break;
        }
        if last_rx.elapsed() > Duration::from_secs(10) && !held.is_empty() {
            break;
        }
    }
    let held_n = held.len();
    let rss_hold = rss_kib();
    let expected_mib = (held_n * a.payload_kib) as f64 / 1024.0;

    // Phase B: ack in receive order and keep draining.
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<Publish>();
    for p in held.drain(..) {
        let _ = ack_tx.send(p);
    }
    let acker = {
        let c = client.clone();
        tokio::spawn(async move {
            while let Some(p) = ack_rx.recv().await {
                if c.ack(&p).await.is_err() {
                    break;
                }
            }
        })
    };
    let mut wire_acks = 0u64;
    let mut comps = 0u64;
    let drain_start = Instant::now();
    loop {
        match timeout(Duration::from_millis(200), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                if let Some(s) = parse_seq(&p.payload) {
                    *seqs.entry(s).or_default() += 1;
                }
                let _ = ack_tx.send(strip(&p));
            }
            Ok(Ok(Event::Outgoing(Outgoing::PubAck(_) | Outgoing::PubRec(_)))) => wire_acks += 1,
            Ok(Ok(Event::Outgoing(Outgoing::PubComp(_)))) => comps += 1,
            Ok(Ok(_)) => {}
            Ok(Err(e)) => anyhow::bail!("poll error during drain: {e}"),
            Err(_) => {}
        }
        if seqs.len() as u64 >= a.count && wire_acks >= a.count && (a.qos != 2 || comps >= a.count)
        {
            break;
        }
        if drain_start.elapsed() > Duration::from_secs(120) {
            eprintln!("drain timeout");
            break;
        }
    }
    sleep(Duration::from_secs(1)).await;
    acker.abort();
    drop(publisher);
    let ev = wlog.snapshot();
    drop(el);
    drop(client);

    // Broker's view: a persistent-session reconnect must not redeliver anything.
    let vopts = OptsBuilder::new(&cid, t)
        .clean(false)
        .manual_acks(false)
        .build();
    let (vc, mut vel) = AsyncClient::new(vopts, 16);
    let _ = vc.subscribe(&topic, qos).await;
    let vstart = Instant::now();
    let mut redelivered = 0u64;
    let mut session_present = None;
    while vstart.elapsed() < Duration::from_secs(4) {
        if let Ok(Ok(e)) = timeout(Duration::from_millis(200), vel.poll()).await {
            match e {
                Event::Incoming(Incoming::Publish(_)) => redelivered += 1,
                Event::Incoming(Incoming::ConnAck(c)) => session_present = Some(c.session_present),
                _ => {}
            }
        }
    }

    let c2s = |f: fn(&Pkt) -> bool| {
        ev.iter()
            .filter(|e| e.dir == Dir::ClientToBroker && f(&e.pkt))
            .count()
    };
    let order = wire::order_report(&ev, a.qos == 2);
    let dups: u64 = seqs.values().map(|c| (*c as u64).saturating_sub(1)).sum();
    json_line(json!({
        "test": "strip_ack",
        "mode": a.mode,
        "qos": a.qos,
        "payload_kib": a.payload_kib,
        "count": a.count,
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "held_unacked_at_peak": held_n,
        "rss_baseline_kib": rss_base,
        "rss_at_hold_kib": rss_hold,
        "rss_delta_mib": (rss_hold as f64 - rss_base as f64) / 1024.0,
        "payload_bytes_held_if_retained_mib": expected_mib,
        "unique_delivered": seqs.len(),
        "duplicate_deliveries": dups,
        "wire_puback_c2s": c2s(|p| matches!(p, Pkt::PubAck(_))),
        "wire_pubrec_c2s": c2s(|p| matches!(p, Pkt::PubRec(_))),
        "wire_pubcomp_c2s": c2s(|p| matches!(p, Pkt::PubComp(_))),
        "order": order,
        "verify_session_present": session_present,
        "verify_redelivered_after_reconnect": redelivered,
    }));
    Ok(())
}
