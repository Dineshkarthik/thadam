//! Q3 (targeted): cancel `EventLoop::poll` while the *write* side is blocked.
//!
//! The random-drop runs in `cancel.rs` can only drop poll futures at points that are
//! reachable on a healthy socket (mostly "waiting for network read"). Source reading shows
//! `poll()` has await points after it has already consumed a request from its channel and
//! recorded an `Outgoing::*` event but before the packet is serialised
//! (`network.write(outgoing).await`, which can be Pending when the socket is backed up).
//! Here the proxy stops reading from the client so the client's TCP send buffer fills, big
//! QoS 0 publishes keep the buffer full, and the poll future is dropped every time it
//! pends. Evidence = `Outgoing::PubAck` events emitted by rumqttc vs PUBACK packets that
//! actually reached the wire (proxy) and the broker (redelivery on session resume).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Args as ClapArgs;
use futures_util::FutureExt;
use rumqttc::{AsyncClient, Event, Incoming, Outgoing, QoS};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use crate::common::*;
use crate::session::publish_n;
use crate::wire::{self, Dir, Pkt};

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    /// none (control: plain poll) | now_or_never | timeout (50 ms)
    #[arg(long)]
    pub style: String,
    #[arg(long)]
    pub broker: String,
    #[arg(long)]
    pub broker_inflight: u32,
    #[arg(long, default_value_t = 300)]
    pub msgs: u64,
    #[arg(long, default_value_t = 3)]
    pub stall_secs: u64,
    #[arg(long, default_value_t = 40)]
    pub fillers: u64,
    #[arg(long, default_value_t = 256)]
    pub filler_kib: usize,
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    let (proxy_port, wlog, stall) =
        wire::start_with_stall(format!("{}:{}", t.host, t.port)).await?;
    let via = Target {
        host: "127.0.0.1".into(),
        port: proxy_port,
    };
    let topic = rand_id("s2/stall");
    let fill_topic = rand_id("s2/stallfill");
    let cid = rand_id("s2-stall-sub");
    let opts = OptsBuilder::new(&cid, &via)
        .keep_alive(30)
        .clean(false)
        .manual_acks(true)
        .build();
    let (client, mut el) = AsyncClient::new(opts, 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;
    let mut subscribed = false;
    while !subscribed {
        if let Event::Incoming(Incoming::SubAck(_)) = el.poll().await? {
            subscribed = true;
        }
    }

    // Everything the broker sends now piles up in socket buffers until we poll again.
    stall.store(true, Relaxed);
    let pubacks = publish_n(t, &topic, 0, a.msgs, 64).await?;
    sleep(Duration::from_millis(500)).await;

    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<rumqttc::Publish>();
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
    let filler = {
        let c = client.clone();
        let (n, kib, ft) = (a.fillers, a.filler_kib, fill_topic.clone());
        tokio::spawn(async move {
            for _ in 0..n {
                if c.publish_bytes(
                    ft.clone(),
                    QoS::AtMostOnce,
                    false,
                    Bytes::from(vec![0u8; kib * 1024]),
                )
                .await
                .is_err()
                {
                    break;
                }
            }
        })
    };

    let mut seqs: BTreeMap<u64, u32> = BTreeMap::new();
    let (mut ack_events, mut pub_events, mut errors, mut connacks) = (0u64, 0u64, 0u64, 0u64);
    let mut err_texts: Vec<String> = Vec::new();
    let (mut cancelled, mut completed) = (0u64, 0u64);
    let started = Instant::now();
    let released_flag = Arc::new(AtomicBool::new(false));
    {
        let (stall, released_flag, secs) = (stall.clone(), released_flag.clone(), a.stall_secs);
        tokio::spawn(async move {
            sleep(Duration::from_secs(secs)).await;
            stall.store(false, Relaxed);
            released_flag.store(true, Relaxed);
        });
    }
    let end = Duration::from_secs(a.stall_secs + 6);
    while started.elapsed() < end {
        let released = released_flag.load(Relaxed);
        let r = if released {
            // After release only idle polls are dropped; nothing is write-blocked any more.
            timeout(Duration::from_millis(100), el.poll()).await.ok()
        } else {
            match a.style.as_str() {
                "now_or_never" => {
                    let r = el.poll().now_or_never();
                    if r.is_none() {
                        tokio::task::yield_now().await;
                    }
                    r
                }
                "timeout" => timeout(Duration::from_millis(50), el.poll()).await.ok(),
                _ => Some(el.poll().await),
            }
        };
        let Some(r) = r else {
            cancelled += 1;
            continue;
        };
        completed += 1;
        match r {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                if let Some(s) = parse_seq(&p.payload) {
                    *seqs.entry(s).or_default() += 1;
                }
                let _ = ack_tx.send(strip(&p));
            }
            Ok(Event::Incoming(Incoming::ConnAck(_))) => connacks += 1,
            Ok(Event::Outgoing(Outgoing::PubAck(_))) => ack_events += 1,
            Ok(Event::Outgoing(Outgoing::Publish(_))) => pub_events += 1,
            Ok(_) => {}
            Err(e) => {
                errors += 1;
                if err_texts.len() < 3 {
                    err_texts.push(e.to_string());
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    acker.abort();
    filler.abort();
    let ev = wlog.snapshot();
    drop(el);
    drop(client);
    let wire_acks = ev
        .iter()
        .filter(|e| e.dir == Dir::ClientToBroker && matches!(e.pkt, Pkt::PubAck(_)))
        .count() as u64;
    let wire_qos0_pubs = ev
        .iter()
        .filter(|e| e.dir == Dir::ClientToBroker && matches!(e.pkt, Pkt::Publish { qos: 0, .. }))
        .count() as u64;

    let vopts = OptsBuilder::new(&cid, t)
        .clean(false)
        .manual_acks(false)
        .build();
    let (vc, mut vel) = AsyncClient::new(vopts, 16);
    let _ = vc.subscribe(&topic, QoS::AtLeastOnce).await;
    let vstart = Instant::now();
    let mut redelivered = 0u64;
    while vstart.elapsed() < Duration::from_secs(4) {
        if let Ok(Ok(Event::Incoming(Incoming::Publish(_)))) =
            timeout(Duration::from_millis(200), vel.poll()).await
        {
            redelivered += 1;
        }
    }
    let dups: u64 = seqs.values().map(|c| (*c as u64).saturating_sub(1)).sum();
    json_line(json!({
        "test": "cancel_while_write_blocked",
        "style": a.style,
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "messages_published_to_subscriber": a.msgs,
        "publisher_pubacks": pubacks,
        "unique_received": seqs.len(),
        "duplicate_deliveries": dups,
        "stall_secs": a.stall_secs,
        "filler_publishes_requested": a.fillers,
        "filler_kib": a.filler_kib,
        "poll_cancelled": cancelled,
        "poll_completed": completed,
        "outgoing_puback_events_emitted_by_rumqttc": ack_events,
        "puback_packets_on_wire_at_proxy": wire_acks,
        "acks_lost_between_event_and_wire": ack_events as i64 - wire_acks as i64,
        "outgoing_publish_events_emitted": pub_events,
        "qos0_filler_packets_on_wire": wire_qos0_pubs,
        "fillers_lost_between_event_and_wire": pub_events as i64 - wire_qos0_pubs as i64,
        "connacks": connacks,
        "poll_errors": errors,
        "poll_error_samples": err_texts,
        "redelivered_to_fresh_session_after_run": redelivered,
        "expected_redelivery_if_lost_acks_are_real": a.msgs as i64 - wire_acks as i64,
    }));
    Ok(())
}
