//! Q2: keepalive under backpressure.
//!
//! Modes
//!   withhold : keep polling the EventLoop, but the consumer (stand-in for the stalled
//!              WAL) neither drains the queue nor acks for hold = N x keepalive.
//!              `--queue-cap` bounds the liveness->consumer queue; a cap smaller than the
//!              broker window makes the liveness task block in `send().await`.
//!   nopoll   : the anti-pattern. Stop polling the EventLoop for hold = N x keepalive.
//!
//! The client connects through the wire proxy so PINGREQs and broker-side closes are
//! observed independently of rumqttc.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args as ClapArgs;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, Outgoing, Publish, QoS};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::common::*;
use crate::wire::{self, Dir, Pkt};

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    #[arg(long)]
    pub mode: String,
    #[arg(long)]
    pub broker: String,
    #[arg(long)]
    pub broker_inflight: u32,
    #[arg(long, default_value_t = 5)]
    pub keepalive: u64,
    #[arg(long, default_value_t = 3)]
    pub hold_multiple: u64,
    /// 0 = effectively unbounded queue between liveness task and consumer.
    #[arg(long, default_value_t = 0)]
    pub queue_cap: usize,
    /// Published messages per second while holding.
    #[arg(long, default_value_t = 1000)]
    pub pub_rate: u64,
    /// Use a clean session instead of a persistent one.
    #[arg(long, default_value_t = false)]
    pub clean: bool,
}

#[derive(Default)]
struct Ctr {
    received: AtomicU64,
    unacked: AtomicI64,
    max_unacked: AtomicI64,
    connacks: AtomicU64,
    errors: AtomicU64,
    pingresp: AtomicU64,
    suback: AtomicBool,
    seqs: Mutex<HashMap<u64, u32>>,
    events: Mutex<Vec<(u64, String)>>,
    post_resume_publishes: AtomicU64,
    resumed: AtomicBool,
    liveness_in_send: AtomicBool,
    consumer_in_ack: AtomicBool,
    acks_issued: AtomicU64,
}

impl Ctr {
    fn ev(&self, t0: Instant, s: impl Into<String>) {
        self.events
            .lock()
            .unwrap()
            .push((t0.elapsed().as_millis() as u64, s.into()));
    }
}

struct Item {
    stripped: Publish,
}

async fn liveness(
    mut el: EventLoop,
    ctr: Arc<Ctr>,
    tx: mpsc::Sender<Item>,
    pause_after_first: Option<Duration>,
    t0: Instant,
) {
    let mut paused_once = false;
    loop {
        match el.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                ctr.received.fetch_add(1, Relaxed);
                if ctr.resumed.load(Relaxed) {
                    ctr.post_resume_publishes.fetch_add(1, Relaxed);
                }
                if let Some(s) = parse_seq(&p.payload) {
                    *ctr.seqs.lock().unwrap().entry(s).or_default() += 1;
                }
                let u = ctr.unacked.fetch_add(1, Relaxed) + 1;
                ctr.max_unacked.fetch_max(u, Relaxed);
                // The anti-pattern lives here when the queue is bounded: this await stops polling.
                let blocked_from = Instant::now();
                ctr.liveness_in_send.store(true, Relaxed);
                let _ = tx
                    .send(Item {
                        stripped: strip(&p),
                    })
                    .await;
                ctr.liveness_in_send.store(false, Relaxed);
                let blocked = blocked_from.elapsed();
                if blocked > Duration::from_millis(500) {
                    ctr.ev(
                        t0,
                        format!(
                            "liveness task blocked {} ms in queue send().await",
                            blocked.as_millis()
                        ),
                    );
                }
                if let (Some(d), false) = (pause_after_first, paused_once) {
                    paused_once = true;
                    ctr.ev(t0, "NOT POLLING: pause begins");
                    sleep(d).await;
                    ctr.ev(t0, "NOT POLLING: pause ends, polling resumes");
                    ctr.resumed.store(true, Relaxed);
                }
            }
            Ok(Event::Incoming(Incoming::ConnAck(c))) => {
                let n = ctr.connacks.fetch_add(1, Relaxed) + 1;
                ctr.ev(
                    t0,
                    format!("ConnAck #{n} session_present={}", c.session_present),
                );
            }
            Ok(Event::Incoming(Incoming::SubAck(_))) => ctr.suback.store(true, Relaxed),
            Ok(Event::Incoming(Incoming::PingResp)) => {
                ctr.pingresp.fetch_add(1, Relaxed);
            }
            Ok(Event::Outgoing(Outgoing::PubAck(_))) => {}
            Ok(_) => {}
            Err(e) => {
                ctr.errors.fetch_add(1, Relaxed);
                ctr.ev(t0, format!("poll error: {e}"));
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    let (proxy_port, wlog) = wire::start(format!("{}:{}", t.host, t.port)).await?;
    let via = Target {
        host: "127.0.0.1".into(),
        port: proxy_port,
    };
    let topic = rand_id("s2/ka");
    let ctr = Arc::new(Ctr::default());
    let t0 = Instant::now();
    let hold = Duration::from_secs(a.keepalive * a.hold_multiple);

    let opts = OptsBuilder::new(rand_id("s2-ka-sub"), &via)
        .keep_alive(a.keepalive)
        .clean(a.clean)
        .manual_acks(true)
        .build();
    let (client, el) = AsyncClient::new(opts, 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;

    let cap = if a.queue_cap == 0 {
        50_000_000
    } else {
        a.queue_cap
    };
    let (tx, mut rx) = mpsc::channel::<Item>(cap);
    let pause = (a.mode == "nopoll").then_some(hold);
    let live = tokio::spawn(liveness(el, ctr.clone(), tx, pause, t0));

    while !ctr.suback.load(Relaxed) {
        sleep(Duration::from_millis(10)).await;
    }
    ctr.ev(t0, "SubAck");

    // Publisher: constant rate for the whole hold, then stop.
    let published = Arc::new(AtomicU64::new(0));
    let pub_done = Arc::new(AtomicBool::new(false));
    let publisher = {
        let (published, pub_done, t) = (published.clone(), pub_done.clone(), t.clone());
        let topic = topic.clone();
        let total_s = hold.as_secs();
        let rate = a.pub_rate;
        tokio::spawn(async move {
            let opts = OptsBuilder::new(rand_id("s2-ka-pub"), &t)
                .inflight(1000)
                .build();
            let (c, mut el) = AsyncClient::new(opts, 1000);
            tokio::spawn(async move {
                loop {
                    if el.poll().await.is_err() {
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            });
            let start = Instant::now();
            let mut seq = 0u64;
            let tick = Duration::from_millis(10);
            let per_tick = (rate / 100).max(1);
            while start.elapsed() < Duration::from_secs(total_s) {
                for _ in 0..per_tick {
                    let _ = c
                        .publish_bytes(
                            topic.clone(),
                            QoS::AtLeastOnce,
                            false,
                            make_payload(seq, 128),
                        )
                        .await;
                    seq += 1;
                    published.fetch_add(1, Relaxed);
                }
                sleep(tick).await;
            }
            pub_done.store(true, Relaxed);
        })
    };

    // Timeline sampler (1 Hz).
    let timeline = Arc::new(Mutex::new(Vec::<(u64, u64, i64)>::new()));
    let sampler = {
        let (ctr, timeline) = (ctr.clone(), timeline.clone());
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                timeline.lock().unwrap().push((
                    t0.elapsed().as_secs(),
                    ctr.received.load(Relaxed),
                    ctr.unacked.load(Relaxed),
                ));
            }
        })
    };

    let hold_start_ms = wlog.now_ms();
    let consumer = {
        let (ctr, client, withhold) = (ctr.clone(), client.clone(), a.mode == "withhold");
        tokio::spawn(async move {
            if withhold {
                sleep(hold).await;
                ctr.ev(t0, "consumer resumes: draining queue and acking in order");
            }
            while let Some(it) = rx.recv().await {
                ctr.consumer_in_ack.store(true, Relaxed);
                let r = client.ack(&it.stripped).await;
                ctr.consumer_in_ack.store(false, Relaxed);
                if r.is_err() {
                    break;
                }
                ctr.acks_issued.fetch_add(1, Relaxed);
                ctr.unacked.fetch_sub(1, Relaxed);
            }
        })
    };

    let at_hold_end_received;
    sleep(hold).await;
    at_hold_end_received = ctr.received.load(Relaxed);
    let hold_end_ms = wlog.now_ms();
    let pub_total_at_hold_end = published.load(Relaxed);

    // Settle: wait for delivery of everything published (or no progress for 20 s).
    let mut last = (0usize, Instant::now());
    loop {
        sleep(Duration::from_millis(500)).await;
        let uniq = ctr.seqs.lock().unwrap().len();
        let total = published.load(Relaxed) as usize;
        if pub_done.load(Relaxed) && uniq >= total {
            break;
        }
        if uniq != last.0 {
            last = (uniq, Instant::now());
        } else if last.1.elapsed() > Duration::from_secs(20) && pub_done.load(Relaxed) {
            break;
        }
        if t0.elapsed() > Duration::from_secs(a.keepalive * a.hold_multiple + 120) {
            break;
        }
    }
    sleep(Duration::from_secs(1)).await;

    let uniq = ctr.seqs.lock().unwrap().len() as u64;
    let total = published.load(Relaxed);
    let dups: u64 = ctr
        .seqs
        .lock()
        .unwrap()
        .values()
        .map(|c| (*c as u64).saturating_sub(1))
        .sum();
    let missing = total.saturating_sub(uniq);

    let ev = wlog.snapshot();
    let in_hold = |t: u64| t >= hold_start_ms && t <= hold_end_ms;
    let pingreqs: Vec<u64> = ev
        .iter()
        .filter(|e| e.dir == Dir::ClientToBroker && matches!(e.pkt, Pkt::PingReq))
        .map(|e| e.t_ms)
        .collect();
    let pingreq_in_hold = pingreqs.iter().filter(|t| in_hold(**t)).count();
    let pingresp_in_hold = ev
        .iter()
        .filter(|e| {
            e.dir == Dir::BrokerToClient && matches!(e.pkt, Pkt::PingResp) && in_hold(e.t_ms)
        })
        .count();
    let broker_closes: Vec<(u32, u64)> = ev
        .iter()
        .filter(|e| e.dir == Dir::BrokerToClient && matches!(e.pkt, Pkt::Closed))
        .map(|e| (e.conn, e.t_ms))
        .collect();
    let conns = ev.iter().map(|e| e.conn).max().unwrap_or(0);
    let last_c2s_before_close: Option<u64> = broker_closes.first().and_then(|(c, tc)| {
        ev.iter()
            .filter(|e| {
                e.conn == *c
                    && e.dir == Dir::ClientToBroker
                    && !matches!(e.pkt, Pkt::Closed)
                    && e.t_ms <= *tc
            })
            .map(|e| e.t_ms)
            .max()
    });
    let first_close_after_last_c2s_ms = broker_closes
        .first()
        .zip(last_c2s_before_close)
        .map(|((_, tc), l)| tc - l);
    let broker_publishes_in_hold = ev
        .iter()
        .filter(|e| {
            e.dir == Dir::BrokerToClient && matches!(e.pkt, Pkt::Publish { .. }) && in_hold(e.t_ms)
        })
        .count();
    let puback_total = ev
        .iter()
        .filter(|e| e.dir == Dir::ClientToBroker && matches!(e.pkt, Pkt::PubAck(_)))
        .count();
    let max_unacked = ctr.max_unacked.load(Relaxed);
    let events = ctr.events.lock().unwrap().clone();
    let timeline = timeline.lock().unwrap().clone();

    live.abort();
    consumer.abort();
    sampler.abort();
    publisher.abort();

    json_line(json!({
        "test": "keepalive",
        "mode": a.mode,
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "keepalive_s": a.keepalive,
        "hold_s": hold.as_secs(),
        "queue_cap": a.queue_cap,
        "clean_session": a.clean,
        "tcp_connections_seen_by_proxy": conns,
        "client_connacks": ctr.connacks.load(Relaxed),
        "poll_errors": ctr.errors.load(Relaxed),
        "broker_side_closes": broker_closes,
        "broker_close_ms_after_last_client_packet": first_close_after_last_c2s_ms,
        "pingreq_on_wire_total": pingreqs.len(),
        "pingreq_on_wire_during_hold": pingreq_in_hold,
        "pingresp_during_hold": pingresp_in_hold,
        "max_unacked_observed": max_unacked,
        "received_at_hold_end": at_hold_end_received,
        "broker_publish_packets_during_hold": broker_publishes_in_hold,
        "published_total": total,
        "published_at_hold_end": pub_total_at_hold_end,
        "unique_delivered": uniq,
        "duplicate_deliveries": dups,
        "missing": missing,
        "puback_on_wire_total": puback_total,
        "post_resume_publishes": ctr.post_resume_publishes.load(Relaxed),
        "acks_issued_by_consumer": ctr.acks_issued.load(Relaxed),
        "liveness_task_blocked_in_queue_send_at_end": ctr.liveness_in_send.load(Relaxed),
        "consumer_blocked_in_client_ack_at_end": ctr.consumer_in_ack.load(Relaxed),
        "events": events,
        "timeline_s_received_unacked": timeline,
    }));
    Ok(())
}
