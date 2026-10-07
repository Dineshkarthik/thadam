//! Q6: are PUBACKs sent on the wire in receive order when acks are issued in order?
//!
//! `in_order`: each message "commits" after a random 0-20 ms; a completion-ordered tracker
//! releases acks strictly in receive order (the design in the architecture review).
//! `shuffled`: control experiment that deliberately issues acks reversed within batches of
//! 10 to show the wire order simply follows issue order and how the broker reacts.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args as ClapArgs;
use rand::Rng;
use rumqttc::{AsyncClient, Event, Incoming, Publish, QoS};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::common::*;
use crate::wire::{self, Dir, Pkt};

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    /// in_order | shuffled
    #[arg(long)]
    pub mode: String,
    #[arg(long, default_value_t = 2000)]
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
    let topic = rand_id("s2/order");
    let opts = OptsBuilder::new(rand_id("s2-order-sub"), &via)
        .manual_acks(true)
        .build();
    let (client, mut el) = AsyncClient::new(opts, 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<(Publish, Arc<AtomicBool>)>();
    let acked = Arc::new(AtomicU64::new(0));
    let acker = {
        let (c, acked, mode) = (client.clone(), acked.clone(), a.mode.clone());
        tokio::spawn(async move {
            let mut q: VecDeque<(Publish, Arc<AtomicBool>)> = VecDeque::new();
            loop {
                while let Ok(i) = rx.try_recv() {
                    q.push_back(i);
                }
                if mode == "shuffled" {
                    if q.len() >= 10 {
                        let mut batch: Vec<_> = q.drain(..10).collect();
                        batch.reverse();
                        for (p, _) in batch {
                            if c.ack(&p).await.is_err() {
                                return;
                            }
                            acked.fetch_add(1, Relaxed);
                        }
                        continue;
                    }
                } else if q.front().is_some_and(|(_, done)| done.load(Relaxed)) {
                    let (p, _) = q.pop_front().unwrap();
                    if c.ack(&p).await.is_err() {
                        return;
                    }
                    acked.fetch_add(1, Relaxed);
                    continue;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let mut got = 0u64;
    let mut subscribed = false;
    let mut publisher = None;
    let start = Instant::now();
    while acked.load(Relaxed) < a.count && start.elapsed() < Duration::from_secs(90) {
        let Ok(ev) = tokio::time::timeout(Duration::from_millis(100), el.poll()).await else {
            continue;
        };
        match ev {
            Ok(Event::Incoming(Incoming::SubAck(_))) if !subscribed => {
                subscribed = true;
                let (t, topic, n) = (t.clone(), topic.clone(), a.count);
                publisher = Some(tokio::spawn(async move {
                    let opts = OptsBuilder::new(rand_id("s2-order-pub"), &t)
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
                    for seq in 0..n {
                        let _ = c
                            .publish_bytes(
                                topic.clone(),
                                QoS::AtLeastOnce,
                                false,
                                make_payload(seq, 64),
                            )
                            .await;
                    }
                    sleep(Duration::from_secs(600)).await;
                }));
            }
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                got += 1;
                let done = Arc::new(AtomicBool::new(false));
                let d2 = done.clone();
                let delay = Duration::from_micros(rand::rng().random_range(0..20_000));
                tokio::spawn(async move {
                    sleep(delay).await;
                    d2.store(true, Relaxed);
                });
                let _ = tx.send((strip(&p), done));
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("poll error: {e}");
                break;
            }
        }
    }
    // Acks only reach the wire while the EventLoop is polled: keep polling to flush them.
    let flush_until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < flush_until {
        let _ = tokio::time::timeout(Duration::from_millis(100), el.poll()).await;
    }
    let ev = wlog.snapshot();
    let report = wire::order_report(&ev, false);
    let broker_closes = ev
        .iter()
        .filter(|e| e.dir == Dir::BrokerToClient && matches!(e.pkt, Pkt::Closed))
        .count();
    acker.abort();
    if let Some(p) = publisher {
        p.abort();
    }
    json_line(json!({
        "test": "ack_order",
        "mode": a.mode,
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "count": a.count,
        "received": got,
        "acks_issued": acked.load(Relaxed),
        "wire": report,
        "broker_closed_connection_during_run": broker_closes,
    }));
    Ok(())
}
