//! Q5: persistent sessions, kill -9 recovery, broker queue limits, session-loss detection.
//!
//! `sess-sub` / `sess-pub` / `sess-report` are small processes driven by scripts/run_q5.sh
//! (the subscriber is a separate OS process so it can be killed with SIGKILL).
//! `stale-ack` is an in-process experiment on acking with handles from a lost session.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args as ClapArgs;
use rumqttc::{AsyncClient, Event, Incoming, Publish, QoS};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use crate::common::*;

#[derive(ClapArgs, Clone, Debug)]
pub struct SubArgs {
    #[arg(long)]
    pub id: String,
    #[arg(long)]
    pub topic: String,
    #[arg(long)]
    pub log: PathBuf,
    /// Ack only the first K deliveries of this process (default 0 = ack nothing).
    #[arg(long, default_value_t = 0)]
    pub ack_first: u64,
    /// Ack every delivery.
    #[arg(long, default_value_t = false)]
    pub ack_all: bool,
    #[arg(long, default_value_t = 3600)]
    pub run_secs: u64,
    #[arg(long, default_value_t = 30)]
    pub keepalive: u64,
}

pub async fn sess_sub(t: &Target, a: SubArgs) -> Result<()> {
    let mut log = OpenOptions::new().create(true).append(true).open(&a.log)?;
    let t0 = Instant::now();
    let mut w = |v: Value| {
        let mut v = v;
        v["t_ms"] = json!(t0.elapsed().as_millis() as u64);
        let _ = writeln!(log, "{}", serde_json::to_string(&v).unwrap());
        let _ = log.flush();
    };
    let opts = OptsBuilder::new(&a.id, t)
        .keep_alive(a.keepalive)
        .clean(false)
        .manual_acks(true)
        .build();
    let (client, mut el) = AsyncClient::new(opts, 64);
    client.subscribe(&a.topic, QoS::AtLeastOnce).await?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Publish>();
    {
        let c = client.clone();
        tokio::spawn(async move {
            while let Some(p) = rx.recv().await {
                if c.ack(&p).await.is_err() {
                    break;
                }
            }
        });
    }
    let mut n = 0u64;
    while t0.elapsed() < Duration::from_secs(a.run_secs) {
        match timeout(Duration::from_millis(500), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::ConnAck(c)))) => {
                w(json!({"ev":"connack","session_present":c.session_present}))
            }
            Ok(Ok(Event::Incoming(Incoming::SubAck(_)))) => w(json!({"ev":"suback"})),
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                n += 1;
                w(json!({"ev":"pub","seq":parse_seq(&p.payload),"pkid":p.pkid,"dup":p.dup}));
                if a.ack_all || n <= a.ack_first {
                    let _ = tx.send(strip(&p));
                    w(json!({"ev":"ack_issued","pkid":p.pkid}));
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                w(json!({"ev":"poll_error","err":e.to_string()}));
                sleep(Duration::from_millis(300)).await;
            }
            Err(_) => {}
        }
    }
    Ok(())
}

#[derive(ClapArgs, Clone, Debug)]
pub struct PubArgs {
    #[arg(long)]
    pub topic: String,
    #[arg(long)]
    pub count: u64,
    #[arg(long, default_value_t = 0)]
    pub start_seq: u64,
    #[arg(long, default_value_t = 64)]
    pub size: usize,
    #[arg(long, default_value_t = 1)]
    pub qos: u8,
}

pub async fn publish_n(
    t: &Target,
    topic: &str,
    start: u64,
    count: u64,
    size: usize,
) -> Result<u64> {
    publish_n_qos(t, topic, start, count, size, 1).await
}

pub async fn publish_n_qos(
    t: &Target,
    topic: &str,
    start: u64,
    count: u64,
    size: usize,
    qos: u8,
) -> Result<u64> {
    let opts = OptsBuilder::new(rand_id("s2-pub"), t)
        .inflight(1000)
        .build();
    let (c, mut el) = AsyncClient::new(opts, 1000);
    let topic = topic.to_string();
    let feeder = tokio::spawn(async move {
        for seq in start..start + count {
            if c.publish_bytes(topic.clone(), qos_from(qos), false, make_payload(seq, size))
                .await
                .is_err()
            {
                break;
            }
        }
        sleep(Duration::from_secs(3600)).await;
    });
    let mut pubacks = 0u64;
    let deadline = Instant::now() + Duration::from_secs(120);
    while pubacks < count && Instant::now() < deadline {
        match timeout(Duration::from_millis(200), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::PubAck(_) | Incoming::PubComp(_)))) => pubacks += 1,
            _ => {}
        }
    }
    feeder.abort();
    Ok(pubacks)
}

pub async fn sess_pub(t: &Target, a: PubArgs) -> Result<()> {
    let pubacks = publish_n_qos(t, &a.topic, a.start_seq, a.count, a.size, a.qos).await?;
    json_line(
        json!({"test":"sess_pub","published":a.count,"pubacks_received_by_publisher":pubacks}),
    );
    Ok(())
}

#[derive(ClapArgs, Clone, Debug)]
pub struct ReportArgs {
    #[arg(long)]
    pub expect_from: u64,
    #[arg(long)]
    pub expect_to: u64,
    #[arg(long, num_args = 1..)]
    pub logs: Vec<PathBuf>,
    #[arg(long, default_value = "")]
    pub label: String,
}

fn ranges(set: &BTreeSet<u64>, from: u64, to: u64) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for s in from..to {
        if !set.contains(&s) {
            match out.last_mut() {
                Some(r) if r.1 + 1 == s => r.1 = s,
                _ => out.push((s, s)),
            }
        }
    }
    out
}

pub fn sess_report(a: ReportArgs) -> Result<()> {
    let mut all: BTreeSet<u64> = BTreeSet::new();
    let mut per_log = Vec::new();
    for path in &a.logs {
        let mut sp = None::<bool>;
        let (mut pubs, mut dups, mut acks, mut errs) = (0u64, 0u64, 0u64, 0u64);
        let mut seqs = BTreeSet::new();
        let mut dup_seqs = BTreeSet::new();
        let mut first_seq_order: Vec<u64> = Vec::new();
        for line in BufReader::new(File::open(path)?).lines() {
            let v: Value = serde_json::from_str(&line?)?;
            match v["ev"].as_str() {
                Some("connack") => sp = v["session_present"].as_bool(),
                Some("pub") => {
                    pubs += 1;
                    if let Some(s) = v["seq"].as_u64() {
                        seqs.insert(s);
                        all.insert(s);
                        if v["dup"].as_bool() == Some(true) {
                            dups += 1;
                            dup_seqs.insert(s);
                        }
                        if first_seq_order.len() < 5 {
                            first_seq_order.push(s);
                        }
                    }
                }
                Some("ack_issued") => acks += 1,
                Some("poll_error") => errs += 1,
                _ => {}
            }
        }
        per_log.push(json!({
            "log": path.file_name().and_then(|s| s.to_str()),
            "session_present_in_connack": sp,
            "deliveries": pubs,
            "dup_flagged_deliveries": dups,
            "dup_flagged_seq_range": dup_seqs.first().zip(dup_seqs.last()).map(|(a, b)| (*a, *b)),
            "unique_seqs": seqs.len(),
            "seq_range": seqs.first().zip(seqs.last()).map(|(a, b)| (*a, *b)),
            "first_seqs": first_seq_order,
            "acks_issued": acks,
            "poll_errors": errs,
        }));
    }
    let missing = ranges(&all, a.expect_from, a.expect_to);
    let missing_n: u64 = missing.iter().map(|(a, b)| b - a + 1).sum();
    let expected = a.expect_to - a.expect_from;
    json_line(json!({
        "test": "sess_report",
        "label": a.label,
        "expected": expected,
        "unique_delivered_overall": all.len(),
        "missing_count": missing_n,
        "missing_ranges_first10": missing.iter().take(10).collect::<Vec<_>>(),
        "logs": per_log,
    }));
    Ok(())
}

#[derive(ClapArgs, Clone, Debug)]
pub struct StaleArgs {
    #[arg(long)]
    pub broker: String,
    /// Path to scripts/broker.sh (used to wipe the broker: restart-mosquitto-fresh).
    #[arg(long)]
    pub broker_script: PathBuf,
    /// Send acks for pkids obtained before the session was lost.
    #[arg(long, default_value_t = false)]
    pub stale_ack: bool,
    #[arg(long, default_value_t = 10)]
    pub count: u64,
}

/// E1: after session loss (broker restarted with no persistence), pkids restart. An ack
/// issued later for a pre-loss delivery carries a pkid that now names a *different*,
/// uncommitted message.
pub async fn stale_ack(t: &Target, a: StaleArgs) -> Result<()> {
    let topic = rand_id("s2/stale");
    let cid = rand_id("s2-stale-sub");
    let opts = OptsBuilder::new(&cid, t)
        .keep_alive(5)
        .clean(false)
        .manual_acks(true)
        .build();
    let (client, mut el) = AsyncClient::new(opts, 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;
    let mut notes: Vec<String> = Vec::new();
    let mut connacks: Vec<bool> = Vec::new();
    let mut pre: Vec<Publish> = Vec::new();
    let mut subscribed = false;
    let mut published_pre = false;

    // Phase 1: receive `count` messages on the first session, hold them unacked.
    let phase_end = Instant::now() + Duration::from_secs(15);
    while pre.len() < a.count as usize && Instant::now() < phase_end {
        if let Ok(Ok(ev)) = timeout(Duration::from_millis(200), el.poll()).await {
            match ev {
                Event::Incoming(Incoming::ConnAck(c)) => connacks.push(c.session_present),
                Event::Incoming(Incoming::SubAck(_)) => subscribed = true,
                Event::Incoming(Incoming::Publish(p)) => pre.push(p),
                _ => {}
            }
        }
        if subscribed && !published_pre {
            published_pre = true;
            publish_n(t, &topic, 0, a.count, 64).await?;
        }
    }
    notes.push(format!(
        "phase1: held {} unacked deliveries, pkids {:?}",
        pre.len(),
        pre.iter().map(|p| p.pkid).collect::<Vec<_>>()
    ));
    let stale: Vec<Publish> = pre.iter().map(strip).collect();

    // Phase 2: wipe the broker (session loss) and let rumqttc reconnect.
    let status = std::process::Command::new("bash")
        .arg(&a.broker_script)
        .arg("restart-mosquitto-fresh")
        .status()?;
    anyhow::ensure!(status.success(), "broker restart failed");
    let phase_end = Instant::now() + Duration::from_secs(20);
    let mut reconnected = false;
    let mut errors = 0u64;
    while !reconnected && Instant::now() < phase_end {
        match timeout(Duration::from_millis(300), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::ConnAck(c)))) => {
                connacks.push(c.session_present);
                reconnected = true;
            }
            Ok(Err(_)) => {
                errors += 1;
                sleep(Duration::from_millis(300)).await;
            }
            _ => {}
        }
    }
    notes.push(format!("phase2: reconnected={reconnected} poll_errors_while_down={errors} connack session_present history={connacks:?}"));

    // Phase 3: rumqttc does not resubscribe by itself. Publish first (no resubscribe) to show that.
    publish_n(t, &topic, 100, a.count, 64).await?;
    let mut rx_without_resub = 0u64;
    let end = Instant::now() + Duration::from_secs(2);
    while Instant::now() < end {
        if let Ok(Ok(Event::Incoming(Incoming::Publish(_)))) =
            timeout(Duration::from_millis(100), el.poll()).await
        {
            rx_without_resub += 1;
        }
    }
    notes.push(format!(
        "phase3: deliveries received after session loss WITHOUT resubscribe = {rx_without_resub}"
    ));

    // Phase 4: resubscribe and publish new messages (pkids restart from 1).
    client.subscribe(&topic, QoS::AtLeastOnce).await?;
    let mut sub2 = false;
    let mut published_post = false;
    let mut post: Vec<Publish> = Vec::new();
    let end = Instant::now() + Duration::from_secs(15);
    while post.len() < a.count as usize && Instant::now() < end {
        if let Ok(Ok(ev)) = timeout(Duration::from_millis(200), el.poll()).await {
            match ev {
                Event::Incoming(Incoming::SubAck(_)) => sub2 = true,
                Event::Incoming(Incoming::Publish(p)) => post.push(p),
                _ => {}
            }
        }
        if sub2 && !published_post {
            published_post = true;
            publish_n(t, &topic, 200, a.count, 64).await?;
        }
    }
    let post_pkids: Vec<u16> = post.iter().map(|p| p.pkid).collect();
    let overlap = stale
        .iter()
        .filter(|s| post_pkids.contains(&s.pkid))
        .count();
    notes.push(format!("phase4: held {} new deliveries (seq 200..), pkids {:?}; {} of the stale pkids collide with them", post.len(), post_pkids, overlap));

    // Phase 5: the app "finishes committing" the OLD messages and acks them with old handles.
    if a.stale_ack {
        for s in &stale {
            client.ack(s).await?;
        }
        notes.push(format!(
            "phase5: issued {} stale acks from the lost session",
            stale.len()
        ));
    } else {
        notes.push("phase5: control, no stale acks issued".into());
    }
    let end = Instant::now() + Duration::from_secs(2);
    while Instant::now() < end {
        let _ = timeout(Duration::from_millis(100), el.poll()).await;
    }
    drop(el);
    drop(client);

    // Phase 6: broker's view. The new messages were NEVER acked by the app for real.
    let vopts = OptsBuilder::new(&cid, t)
        .clean(false)
        .manual_acks(false)
        .build();
    let (vc, mut vel) = AsyncClient::new(vopts, 16);
    let _ = vc.subscribe(&topic, QoS::AtLeastOnce).await;
    let mut redelivered: BTreeMap<u64, u32> = BTreeMap::new();
    let end = Instant::now() + Duration::from_secs(4);
    while Instant::now() < end {
        if let Ok(Ok(Event::Incoming(Incoming::Publish(p)))) =
            timeout(Duration::from_millis(200), vel.poll()).await
        {
            if let Some(s) = parse_seq(&p.payload) {
                *redelivered.entry(s).or_default() += 1;
            }
        }
    }
    let lost = a.count as usize
        - redelivered
            .keys()
            .filter(|s| (200..200 + a.count).contains(s))
            .count();
    json_line(json!({
        "test": "stale_ack_after_session_loss",
        "broker": a.broker,
        "stale_ack": a.stale_ack,
        "notes": notes,
        "new_messages_never_really_acked": a.count,
        "redelivered_on_reconnect": redelivered.len(),
        "new_messages_silently_lost": lost,
    }));
    Ok(())
}
