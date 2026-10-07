//! Integration tests against Mosquitto 2 and EMQX 5.8.6 in Docker.
//!
//! Every scenario runs once per broker. They are `#[ignore]`d so
//! `cargo nextest run` passes without Docker; run them with
//!
//! ```text
//! cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(docker_e2e)'
//! ```
//!
//! Containers are labelled `thadam-mqtt-test` and removed when the test ends,
//! including on panic.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::broker::{Broker, Kind, Opts, TlsDir};
use common::certs::Certs;
use common::proxy::{Pkt, Proxy};
use common::{
    Opened, Publisher, base_config, build_source, eventually, limits, open_source, payload_ids,
    pull_ids, sized_payload, try_open, unique, with, within,
};
use thadam_core::{AckOutcome, AckStatus, BatchLimits, Source, SourceBatch, codes};
use thadam_testkit::{SplitMix64, poll_once, poll_up_to};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde_json::json;

macro_rules! per_broker {
    ($mosquitto:ident, $emqx:ident, $scenario:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "needs Docker (Mosquitto 2)"]
        async fn $mosquitto() {
            $scenario(Kind::Mosquitto).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "needs Docker (EMQX 5.8.6)"]
        async fn $emqx() {
            $scenario(Kind::Emqx).await;
        }
    };
}

async fn start(kind: Kind, opts: Opts) -> Broker {
    tokio::task::spawn_blocking(move || Broker::start(kind, &opts))
        .await
        .unwrap()
}

fn host_port(addr: &str) -> (&str, u16) {
    let (host, port) = addr.rsplit_once(':').unwrap();
    (host, port.parse().unwrap())
}

/// Connects as `client_id` with a clean session, which makes the broker discard
/// whatever session that identifier had, then disconnects.
async fn wipe_session(addr: &str, client_id: &str) {
    let (host, port) = host_port(addr);
    let mut options = MqttOptions::new(client_id, host, port);
    options
        .set_clean_session(true)
        .set_keep_alive(Duration::from_secs(5));
    let (client, mut event_loop) = AsyncClient::new(options, 8);
    within(
        Duration::from_secs(20),
        "the session-wiping client to connect",
        async {
            loop {
                if let Ok(Event::Incoming(Incoming::ConnAck(_))) = event_loop.poll().await {
                    break;
                }
            }
        },
    )
    .await;
    client.disconnect().await.unwrap();
    for _ in 0..5 {
        if event_loop.poll().await.is_err() {
            break;
        }
    }
}

/// Reconnects as `client_id` with its persistent session and auto-acking, and
/// returns the (id, dup) of everything the broker delivers within `listen`.
async fn collect_redelivery(
    addr: &str,
    client_id: &str,
    topic: &str,
    listen: Duration,
) -> Vec<(u64, bool)> {
    let (host, port) = host_port(addr);
    let mut options = MqttOptions::new(client_id, host, port);
    options
        .set_clean_session(false)
        .set_keep_alive(Duration::from_secs(5));
    let (client, mut event_loop) = AsyncClient::new(options, 64);
    client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    let mut got = Vec::new();
    let end = tokio::time::Instant::now() + listen;
    while tokio::time::Instant::now() < end {
        if let Ok(Ok(Event::Incoming(Incoming::Publish(p)))) =
            tokio::time::timeout(Duration::from_millis(200), event_loop.poll()).await
            && let Some(id) = thadam_testkit::payload_id(&p.payload)
        {
            got.push((id, p.dup));
        }
    }
    got
}

async fn open_via_proxy(broker: &Broker, topic: &str, client_id: &str) -> (Proxy, Opened) {
    let proxy = Proxy::start(broker.addr()).await;
    let config = base_config(&proxy.url(), client_id, topic);
    let opened = open_source(&config).await;
    (proxy, opened)
}

// ---------------------------------------------------------------------------
// QoS 1 durability across kill -9 (a real child process)
// ---------------------------------------------------------------------------

/// The Thadam process that gets killed. It runs only when the parent test
/// spawns it with `THADAM_CHILD_CONFIG`; otherwise it returns at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "helper process for the kill -9 scenario"]
async fn child_subscriber() {
    let Ok(config) = std::env::var("THADAM_CHILD_CONFIG") else {
        return;
    };
    let log_path = std::env::var("THADAM_CHILD_LOG").unwrap();
    let ack_delay = Duration::from_millis(
        std::env::var("THADAM_CHILD_ACK_DELAY_MS")
            .unwrap()
            .parse()
            .unwrap(),
    );
    let config: serde_json::Value = serde_json::from_str(&config).unwrap();
    let mut opened = open_source(&config).await;
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    writeln!(log, "READY").unwrap();
    log.sync_data().unwrap();

    tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(180)).await;
        std::process::exit(3);
    });
    loop {
        let Some(SourceBatch { batch, ack }) = opened.source.next_batch(limits(16)).await.unwrap()
        else {
            return;
        };
        for m in &batch.messages {
            let id = thadam_testkit::payload_id(&m.payload).unwrap();
            writeln!(log, "{id} {}", m.attrs.get("mqtt.dup").unwrap_or("false")).unwrap();
        }
        // The "WAL commit": the delivery is durable before it is acknowledged.
        log.sync_data().unwrap();
        tokio::time::sleep(ack_delay).await;
        ack.complete(AckOutcome::Committed);
    }
}

fn spawn_child(config: &serde_json::Value, log: &Path, ack_delay_ms: u64) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_subscriber", "--ignored", "--nocapture"])
        .env("THADAM_CHILD_CONFIG", config.to_string())
        .env("THADAM_CHILD_LOG", log)
        .env("THADAM_CHILD_ACK_DELAY_MS", ack_delay_ms.to_string())
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn the child process")
}

fn read_log(path: &Path) -> (bool, Vec<(u64, bool)>) {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let ready = text.lines().any(|l| l == "READY");
    let entries = text
        .lines()
        .filter_map(|l| {
            let (id, dup) = l.split_once(' ')?;
            Some((id.parse().ok()?, dup == "true"))
        })
        .collect();
    (ready, entries)
}

async fn kill9_durability(kind: Kind) {
    const TOTAL: u64 = 600;
    let broker = start(
        kind,
        Opts {
            persistence: true,
            ..Opts::default()
        },
    )
    .await;
    let topic = unique("thadam/kill9");
    let client_id = unique("kill9");
    let config = with(
        base_config(&broker.url(), &client_id, &topic),
        "expected_broker_window",
        json!(broker.window),
    );
    let dir = tempfile::tempdir().unwrap();
    let (log1, log2) = (dir.path().join("first.log"), dir.path().join("second.log"));

    let mut first = spawn_child(&config, &log1, 40);
    eventually(
        Duration::from_secs(120),
        "the first child to subscribe",
        || read_log(&log1).0,
    )
    .await;

    let publisher = Publisher::connect(&broker.addr(), 1);
    let feeder_topic = topic.clone();
    let feeder = tokio::spawn(async move {
        for id in 0..TOTAL {
            publisher
                .publish(&feeder_topic, sized_payload(id, 64))
                .await;
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        publisher.flush().await;
    });

    eventually(
        Duration::from_secs(60),
        "the first child to receive 150 messages",
        || read_log(&log1).1.len() >= 150,
    )
    .await;
    first.kill().unwrap();
    first.wait().unwrap();

    // Publishing continues while no Thadam process is running.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut second = spawn_child(&config, &log2, 5);
    eventually(
        Duration::from_secs(180),
        "every message to reach one of the two processes",
        || {
            let seen: BTreeSet<u64> = read_log(&log1)
                .1
                .into_iter()
                .chain(read_log(&log2).1)
                .map(|(id, _)| id)
                .collect();
            seen.len() as u64 == TOTAL
        },
    )
    .await;
    second.kill().unwrap();
    second.wait().unwrap();
    within(Duration::from_secs(30), "the publisher to finish", feeder)
        .await
        .unwrap();

    let all: Vec<(u64, bool)> = read_log(&log1)
        .1
        .into_iter()
        .chain(read_log(&log2).1)
        .collect();
    let distinct: BTreeSet<u64> = all.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        distinct,
        (0..TOTAL).collect(),
        "messages were lost across kill -9"
    );
    let duplicates = all.len() - distinct.len();
    let dup_flagged = all.iter().filter(|(_, dup)| *dup).count();
    assert!(
        dup_flagged >= 1,
        "the broker redelivers unacknowledged messages with DUP"
    );
    assert!(
        duplicates <= broker.window as usize + 16,
        "{duplicates} duplicates is more than the broker window ({}) plus one batch",
        broker.window
    );
    // Duplicates are exactly the DUP-flagged redeliveries.
    let redelivered: BTreeMap<u64, usize> = all.iter().fold(BTreeMap::new(), |mut m, (id, _)| {
        *m.entry(*id).or_default() += 1;
        m
    });
    for (id, count) in redelivered {
        assert!(count <= 2, "message {id} was delivered {count} times");
    }
}

per_broker!(
    mosquitto_kill9_loses_nothing,
    emqx_kill9_loses_nothing,
    kill9_durability
);

// ---------------------------------------------------------------------------
// Liveness with acks withheld for 3x the keepalive
// ---------------------------------------------------------------------------

async fn liveness_with_withheld_acks(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let window = u64::from(broker.window);
    let total = window * 3;
    let topic = unique("thadam/liveness");
    let proxy = Proxy::start(broker.addr()).await;
    let config = with(
        base_config(&proxy.url(), &unique("liveness"), &topic),
        "expected_broker_window",
        json!(broker.window),
    );
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..total, 64).await;
    publisher.flush().await;

    // Three keepalives (2 s each) with every acknowledgement withheld.
    let (held_ids, held) = pull_ids(
        &mut opened.source,
        total as usize,
        Duration::from_secs(6),
        false,
    )
    .await;
    assert_eq!(
        held_ids.len() as u64,
        window,
        "the broker must stop at exactly its inflight window while nothing is acknowledged"
    );
    let stats = opened.handle.stats();
    assert_eq!(
        stats.unacked_max, window,
        "the observed maximum unacknowledged count is the broker window"
    );
    assert_eq!(
        proxy.connects_seen(),
        1,
        "the connection must not be re-established"
    );
    assert_eq!(
        proxy.broker_closes(),
        0,
        "the broker must not drop the connection"
    );
    assert_eq!(stats.disconnects, 0);
    assert!(
        proxy.pingreqs() >= 2,
        "PINGREQs must keep flowing while acks are withheld: {}",
        proxy.pingreqs()
    );
    assert_eq!(proxy.keepalive_violations(Duration::from_secs(2)), 0);

    for handle in held {
        assert_eq!(handle.complete(AckOutcome::Committed), AckStatus::Delivered);
    }
    let (rest, _) = pull_ids(
        &mut opened.source,
        (total - window) as usize,
        Duration::from_secs(60),
        true,
    )
    .await;
    let mut all = held_ids;
    all.extend(rest);
    assert_eq!(
        all,
        (0..total).collect::<Vec<_>>(),
        "every message exactly once, in order"
    );

    let stats = opened.handle.stats();
    assert_eq!(stats.duplicates, 0);
    assert_eq!(stats.disconnects, 0);
    assert_eq!(proxy.connects_seen(), 1);
    assert_eq!(proxy.broker_closes(), 0);
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_liveness_with_withheld_acks,
    emqx_liveness_with_withheld_acks,
    liveness_with_withheld_acks
);

// ---------------------------------------------------------------------------
// Ack ordering on the wire
// ---------------------------------------------------------------------------

async fn ack_ordering(kind: Kind) {
    const TOTAL: u64 = 300;
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/order");
    let proxy = Proxy::start(broker.addr()).await;
    let config = with(
        base_config(&proxy.url(), &unique("order"), &topic),
        "expected_broker_window",
        json!(broker.window),
    );
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..TOTAL, 64).await;

    let mut rng = SplitMix64::new(11);
    let mut got = Vec::new();
    while (got.len() as u64) < TOTAL {
        let batch_limits = BatchLimits {
            max_messages: rng.between(1, 9) as usize,
            ..limits(8)
        };
        let batch = within(
            Duration::from_secs(30),
            "the next batch",
            opened.source.next_batch(batch_limits),
        )
        .await
        .unwrap()
        .unwrap();
        got.extend(payload_ids(&batch.batch));
        // The commit takes a random time; completion stays in receive order.
        tokio::time::sleep(Duration::from_millis(rng.between(0, 15))).await;
        batch.ack.complete(AckOutcome::Committed);
    }
    eventually(
        Duration::from_secs(10),
        "all PUBACKs to reach the broker",
        || proxy.pubacks_seen() as u64 == TOTAL,
    )
    .await;

    assert_eq!(got, (0..TOTAL).collect::<Vec<_>>());
    let order = proxy.ack_order();
    assert_eq!(
        (order.publishes as u64, order.acks as u64),
        (TOTAL, TOTAL),
        "{order:?}"
    );
    assert_eq!(order.unmatched, 0, "{order:?}");
    assert_eq!(
        order.descents, 0,
        "PUBACKs left out of receive order: {order:?}"
    );
    let acked: Vec<u64> = proxy
        .acked_payloads()
        .iter()
        .map(|p| thadam_testkit::payload_id(p).unwrap())
        .collect();
    assert_eq!(acked, (0..TOTAL).collect::<Vec<_>>());
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_acks_in_receive_order,
    emqx_acks_in_receive_order,
    ack_ordering
);

// ---------------------------------------------------------------------------
// QoS 0 overflow accounting
// ---------------------------------------------------------------------------

async fn qos0_overflow(kind: Kind) {
    const SENT: u64 = 400;
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/qos0");
    let config = json!({
        "url": broker.url(),
        "client_id": unique("qos0"),
        "topics": [{ "filter": topic, "qos": 0 }],
        "keep_alive": "2s",
        "max_message_bytes": 1024,
        "qos0_queue_bytes": 4096,
    });
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 0);
    publisher.publish_ids(&topic, 0..SENT, 512).await;
    publisher.flush().await;

    // Nobody pulls while the whole burst arrives, so the byte budget overflows.
    let handle = opened.handle.clone();
    eventually(Duration::from_secs(30), "the burst to arrive", || {
        handle.stats().messages_received >= SENT
    })
    .await;
    let stats = opened.handle.stats();
    assert_eq!(
        stats.messages_received, SENT,
        "the broker delivered every QoS 0 message"
    );
    assert!(
        stats.dropped_queue_full > 0,
        "twice the budget must overflow it"
    );
    assert_eq!(
        stats.queued_messages + stats.dropped_queue_full,
        SENT,
        "sent = queued + dropped"
    );
    assert!(stats.queued_bytes <= 4096);

    let (got, _) = pull_ids(
        &mut opened.source,
        usize::MAX,
        Duration::from_millis(500),
        true,
    )
    .await;
    assert_eq!(got.len() as u64, stats.queued_messages);
    assert_eq!(
        got,
        (0..stats.queued_messages).collect::<Vec<_>>(),
        "the newest messages are the ones dropped"
    );

    publisher.publish(&topic, sized_payload(9999, 512)).await;
    let batch = within(
        Duration::from_secs(10),
        "the next message",
        opened.source.next_batch(limits(8)),
    )
    .await
    .unwrap()
    .unwrap();
    let first = &batch.batch.messages[0];
    assert_eq!(
        first.attrs.get("mqtt.gap"),
        Some("queue_full"),
        "the gap is marked on the next message"
    );
    assert_eq!(
        first.attrs.get("mqtt.gap_dropped").map(str::to_owned),
        Some(stats.dropped_queue_full.to_string())
    );
    batch.ack.complete(AckOutcome::Committed);

    let end = opened.handle.stats();
    assert_eq!(
        end.messages_delivered + end.dropped_queue_full,
        SENT + 1,
        "sent = received + dropped, exactly"
    );
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_qos0_overflow_is_accounted,
    emqx_qos0_overflow_is_accounted,
    qos0_overflow
);

// ---------------------------------------------------------------------------
// Session loss: SESSION_LOST, resubscribe, and the stale-epoch ack drop
// ---------------------------------------------------------------------------

/// Cuts the connection, wipes the broker's session while the source cannot
/// reconnect, then lets it reconnect. Returns once it is connected again.
async fn lose_session(broker: &Broker, proxy: &Proxy, opened: &Opened, client_id: &str) {
    let connects = opened.handle.stats().connects;
    proxy.set_blocked(true);
    proxy.kill_connections();
    let handle = opened.handle.clone();
    eventually(
        Duration::from_secs(15),
        "the source to notice the cut",
        || !handle.stats().connected,
    )
    .await;
    wipe_session(&broker.addr(), client_id).await;
    proxy.set_blocked(false);
    let handle = opened.handle.clone();
    eventually(
        Duration::from_secs(60),
        "the source to reconnect",
        move || {
            let s = handle.stats();
            s.connected && s.connects > connects
        },
    )
    .await;
}

async fn session_lost_resubscribes(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/session");
    let client_id = unique("session");
    let proxy = Proxy::start(broker.addr()).await;
    let config = base_config(&proxy.url(), &client_id, &topic);

    let lost = Arc::new(Mutex::new(Vec::new()));
    let sink = lost.clone();
    let (source, handle) = build_source(&config);
    let source = source.with_session_lost_hook(Arc::new(move |event| {
        sink.lock().unwrap().push(event.clone())
    }));
    let mut opened = Opened {
        source,
        handle,
        shutdown: thadam_core::Shutdown::new(),
    };
    opened
        .source
        .open(&common::context(&opened.shutdown))
        .await
        .unwrap();

    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..5, 32).await;
    let (before, _) = pull_ids(&mut opened.source, 5, Duration::from_secs(20), true).await;
    assert_eq!(before, [0, 1, 2, 3, 4]);
    assert_eq!(
        opened.handle.stats().session_lost,
        0,
        "an intact session is not a loss"
    );

    lose_session(&broker, &proxy, &opened, &client_id).await;
    let stats = opened.handle.stats();
    assert_eq!(stats.session_lost, 1, "SESSION_LOST metric");
    {
        let events = lost.lock().unwrap();
        assert_eq!(events.len(), 1, "SESSION_LOST hook");
        assert_eq!(events[0].client_id, client_id);
    }

    // rumqttc does not resubscribe by itself: these only arrive if we did.
    publisher.publish_ids(&topic, 100..105, 32).await;
    let batch = within(
        Duration::from_secs(30),
        "messages after the session loss",
        opened.source.next_batch(limits(16)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(payload_ids(&batch.batch)[0], 100, "the source resubscribed");
    assert_eq!(
        batch.batch.messages[0].attrs.get("mqtt.gap"),
        Some("session_lost"),
        "the gap marker rides on the first message after the loss"
    );
    batch.ack.complete(AckOutcome::Committed);
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_session_loss_resubscribes,
    emqx_session_loss_resubscribes,
    session_lost_resubscribes
);

async fn stale_epoch_acks_are_dropped(kind: Kind) {
    const OLD: u64 = 10;
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/stale");
    let client_id = unique("stale");
    let (proxy, mut opened) = open_via_proxy(&broker, &topic, &client_id).await;
    let publisher = Publisher::connect(&broker.addr(), 1);

    publisher.publish_ids(&topic, 0..OLD, 32).await;
    let (old_ids, old_handles) = pull_ids(
        &mut opened.source,
        OLD as usize,
        Duration::from_secs(20),
        false,
    )
    .await;
    assert_eq!(old_ids.len() as u64, OLD);

    lose_session(&broker, &proxy, &opened, &client_id).await;
    publisher.publish_ids(&topic, 100..100 + OLD, 32).await;
    let (new_ids, new_handles) = pull_ids(
        &mut opened.source,
        OLD as usize,
        Duration::from_secs(30),
        false,
    )
    .await;
    assert_eq!(new_ids, (100..100 + OLD).collect::<Vec<_>>());

    // The packet identifiers of the new session restart at 1 and collide with
    // the old ones, so acknowledging with an old handle would acknowledge a
    // message nobody committed.
    let stale = old_handles.len() as u64;
    for handle in old_handles {
        assert_eq!(handle.complete(AckOutcome::Committed), AckStatus::Stale);
    }
    let stats = opened.handle.stats();
    assert_eq!(stats.stale_acks, stale);
    assert_eq!(
        stats.acks_requested, 0,
        "no stale acknowledgement may reach the client"
    );
    let wire_acks_for_new: Vec<u64> = proxy
        .acked_payloads()
        .iter()
        .filter_map(|p| thadam_testkit::payload_id(p))
        .filter(|id| *id >= 100)
        .collect();
    assert!(
        wire_acks_for_new.is_empty(),
        "uncommitted messages were acknowledged: {wire_acks_for_new:?}"
    );

    // Crash without committing the new session's messages: the broker must
    // still hold every one of them.
    drop(new_handles);
    drop(opened);
    let redelivered =
        collect_redelivery(&broker.addr(), &client_id, &topic, Duration::from_secs(6)).await;
    let ids: BTreeSet<u64> = redelivered.iter().map(|(id, _)| *id).collect();
    let expected: BTreeSet<u64> = (100..100 + OLD).collect();
    assert!(
        expected.is_subset(&ids),
        "messages were silently lost after the session loss: missing {:?}",
        expected.difference(&ids).collect::<Vec<_>>()
    );
}

per_broker!(
    mosquitto_stale_epoch_acks_lose_nothing,
    emqx_stale_epoch_acks_lose_nothing,
    stale_epoch_acks_are_dropped
);

// ---------------------------------------------------------------------------
// Broker restarts
// ---------------------------------------------------------------------------

async fn broker_restart(kind: Kind) {
    let broker = start(
        kind,
        Opts {
            persistence: true,
            ..Opts::default()
        },
    )
    .await;
    let topic = unique("thadam/restart");
    let config = base_config(&broker.url(), &unique("restart"), &topic);
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..5, 32).await;
    let (before, _) = pull_ids(&mut opened.source, 5, Duration::from_secs(20), true).await;
    assert_eq!(before.len(), 5);
    drop(publisher);

    tokio::task::block_in_place(|| broker.restart());
    let handle = opened.handle.clone();
    eventually(
        Duration::from_secs(90),
        "the source to reconnect after the broker restart",
        move || {
            let s = handle.stats();
            s.connected && s.connects >= 2
        },
    )
    .await;

    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 10..15, 32).await;
    let (after, _) = pull_ids(&mut opened.source, 5, Duration::from_secs(60), true).await;
    assert_eq!(
        after,
        [10, 11, 12, 13, 14],
        "the source resubscribed or kept its subscription"
    );
    let stats = opened.handle.stats();
    match kind {
        // `persistence true` keeps the session across the restart.
        Kind::Mosquitto => assert_eq!(
            stats.session_lost, 0,
            "Mosquitto with persistence keeps sessions"
        ),
        // EMQX's default configuration keeps sessions in memory only (ADR-0006).
        Kind::Emqx => assert_eq!(
            stats.session_lost, 1,
            "EMQX loses sessions across a restart"
        ),
    }
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_survives_a_broker_restart,
    emqx_survives_a_broker_restart,
    broker_restart
);

// ---------------------------------------------------------------------------
// Oversized messages
// ---------------------------------------------------------------------------

async fn oversize_aligned_with_the_broker(kind: Kind) {
    let broker = start(
        kind,
        Opts {
            message_size_limit: Some(2048),
            ..Opts::default()
        },
    )
    .await;
    let topic = unique("thadam/oversize-aligned");
    let config = with(
        base_config(&broker.url(), &unique("aligned"), &topic),
        "max_message_bytes",
        json!(2048),
    );
    let mut opened = open_source(&config).await;

    // The broker refuses the oversized message, so the source never sees it.
    let big = Publisher::connect(&broker.addr(), 1);
    big.publish(&topic, sized_payload(1, 20_000)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(big);
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish(&topic, sized_payload(2, 512)).await;
    let (got, _) = pull_ids(&mut opened.source, 1, Duration::from_secs(20), true).await;
    assert_eq!(got, [2]);
    let stats = opened.handle.stats();
    assert_eq!(
        (
            stats.oversize_disconnects,
            stats.disconnects,
            stats.dropped_oversize
        ),
        (0, 0, 0)
    );
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_oversize_aligned,
    emqx_oversize_aligned,
    oversize_aligned_with_the_broker
);

async fn oversize_unaligned_is_reported_not_looped(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/oversize-unaligned");
    let config = with(
        base_config(&broker.url(), &unique("unaligned"), &topic),
        "max_message_bytes",
        json!(1024),
    );
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish(&topic, sized_payload(1, 300_000)).await;

    // A persistent session redelivers the poison message after every
    // reconnect; the source must say so instead of looping forever.
    let error = match within(
        Duration::from_secs(60),
        "the fatal oversize error",
        opened.source.next_batch(limits(8)),
    )
    .await
    {
        Ok(Some(batch)) => panic!("the oversized message was delivered: {}", batch.batch.len()),
        Ok(None) => panic!("end of stream"),
        Err(e) => e,
    };
    assert_eq!(error.code, codes::CONNECTOR_MESSAGE_TOO_LARGE, "{error}");
    assert_eq!(error.retry, thadam_core::Retry::Fatal);
    assert!(opened.handle.stats().oversize_disconnects >= 3);
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_oversize_unaligned,
    emqx_oversize_unaligned,
    oversize_unaligned_is_reported_not_looped
);

async fn oversize_within_the_packet_slack_is_a_counted_drop(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/oversize-slack");
    let proxy = Proxy::start(broker.addr()).await;
    let config = with(
        base_config(&proxy.url(), &unique("slack"), &topic),
        "max_message_bytes",
        json!(1024),
    );
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish(&topic, sized_payload(1, 4096)).await;
    publisher.publish(&topic, sized_payload(2, 200)).await;

    let (got, held) = pull_ids(&mut opened.source, 1, Duration::from_secs(20), false).await;
    assert_eq!(got, [2], "the oversized message is not delivered");
    let stats = opened.handle.stats();
    assert_eq!(
        (
            stats.dropped_oversize,
            stats.oversize_disconnects,
            stats.disconnects
        ),
        (1, 0, 0)
    );
    for h in held {
        h.complete(AckOutcome::Committed);
    }
    // Both were acknowledged, in order, so the broker never redelivers the dropped one.
    eventually(Duration::from_secs(10), "both PUBACKs", || {
        proxy.pubacks_seen() == 2
    })
    .await;
    let acked: Vec<u64> = proxy
        .acked_payloads()
        .iter()
        .map(|p| thadam_testkit::payload_id(p).unwrap())
        .collect();
    assert_eq!(acked, [1, 2]);
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_oversize_slack,
    emqx_oversize_slack,
    oversize_within_the_packet_slack_is_a_counted_drop
);

// ---------------------------------------------------------------------------
// Cancel-safety under load
// ---------------------------------------------------------------------------

async fn cancel_safety_under_load(kind: Kind) {
    const TOTAL: u64 = 1500;
    let broker = start(
        kind,
        Opts {
            max_inflight: Some(1000),
            ..Opts::default()
        },
    )
    .await;
    let topic = unique("thadam/cancel");
    let config = base_config(&broker.url(), &unique("cancel"), &topic);
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    let feeder_topic = topic.clone();
    // Paced, so the consumer often finds the queue empty and its futures
    // really are dropped mid-wait.
    let feeder = tokio::spawn(async move {
        for id in 0..TOTAL {
            publisher
                .publish(&feeder_topic, sized_payload(id, 128))
                .await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        publisher.flush().await;
    });

    let mut rng = SplitMix64::new(2026);
    let mut got: Vec<u64> = Vec::new();
    let (mut dropped, mut polls) = (0u32, 0u32);
    within(
        Duration::from_secs(120),
        "all messages despite random cancellation",
        async {
            while (got.len() as u64) < TOTAL {
                polls += 1;
                let batch_limits = BatchLimits {
                    max_messages: rng.between(1, 40) as usize,
                    linger: Duration::from_micros(rng.between(0, 2000)),
                    ..BatchLimits::default()
                };
                let outcome = match rng.below(4) {
                    0 => poll_once(opened.source.next_batch(batch_limits)).await,
                    1 => tokio::time::timeout(
                        Duration::from_micros(rng.between(1, 800)),
                        opened.source.next_batch(batch_limits),
                    )
                    .await
                    .ok(),
                    2 => {
                        poll_up_to(
                            opened.source.next_batch(batch_limits),
                            rng.between(1, 8) as u32,
                        )
                        .await
                    }
                    _ => tokio::time::timeout(
                        Duration::from_millis(50),
                        opened.source.next_batch(batch_limits),
                    )
                    .await
                    .ok(),
                };
                match outcome {
                    None => dropped += 1,
                    Some(Ok(Some(batch))) => {
                        got.extend(payload_ids(&batch.batch));
                        batch.ack.complete(AckOutcome::Committed);
                    }
                    Some(Ok(None)) => panic!("end of stream"),
                    Some(Err(e)) => panic!("next_batch failed: {e}"),
                }
            }
        },
    )
    .await;
    feeder.await.unwrap();
    assert!(
        dropped >= 100,
        "only {dropped} of {polls} futures were dropped"
    );
    assert_eq!(
        got,
        (0..TOTAL).collect::<Vec<_>>(),
        "messages lost or reordered after {dropped} dropped futures"
    );
    let stats = opened.handle.stats();
    assert_eq!((stats.disconnects, stats.duplicates), (0, 0));
    let handle = opened.handle.clone();
    eventually(
        Duration::from_secs(20),
        "every ack to be written",
        move || handle.stats().acks_written == TOTAL,
    )
    .await;
    opened.source.close().await.unwrap();
}

per_broker!(
    mosquitto_cancel_safety_under_load,
    emqx_cancel_safety_under_load,
    cancel_safety_under_load
);

// ---------------------------------------------------------------------------
// The unacknowledged-message guard
// ---------------------------------------------------------------------------

async fn unacked_guard(kind: Kind) {
    let broker = start(
        kind,
        Opts {
            max_inflight: Some(1000),
            ..Opts::default()
        },
    )
    .await;
    let topic = unique("thadam/guard");
    let config = with(
        with(
            base_config(&broker.url(), &unique("guard"), &topic),
            "expected_broker_window",
            json!(2),
        ),
        "unacked_guard_multiple",
        json!(2),
    );
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..40, 32).await;

    // Never committing anything makes the broker push past the guard every time.
    let mut held: Vec<SourceBatch> = Vec::new();
    let error = within(Duration::from_secs(60), "the guard to give up", async {
        loop {
            match opened.source.next_batch(limits(8)).await {
                Ok(Some(batch)) => held.push(batch),
                Ok(None) => panic!("end of stream"),
                Err(e) => return e,
            }
        }
    })
    .await;
    assert_eq!(error.code, codes::CONNECTOR_PROTOCOL, "{error}");
    assert!(error.message.contains("expected_broker_window"), "{error}");
    assert!(opened.handle.stats().guard_trips >= 3);
    drop(held);
    opened.source.close().await.unwrap();
}

per_broker!(mosquitto_unacked_guard, emqx_unacked_guard, unacked_guard);

// ---------------------------------------------------------------------------
// Graceful close
// ---------------------------------------------------------------------------

async fn close_keeps_unacked_messages(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/close");
    let client_id = unique("close");
    let (proxy, mut opened) = open_via_proxy(&broker, &topic, &client_id).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..10, 32).await;
    let handle = opened.handle.clone();
    eventually(
        Duration::from_secs(20),
        "all ten messages to queue",
        move || handle.stats().queued_messages == 10,
    )
    .await;

    let first = opened.source.next_batch(limits(5)).await.unwrap().unwrap();
    let second = opened.source.next_batch(limits(5)).await.unwrap().unwrap();
    assert_eq!(payload_ids(&first.batch), [0, 1, 2, 3, 4]);
    assert_eq!(payload_ids(&second.batch), [5, 6, 7, 8, 9]);
    first.ack.complete(AckOutcome::Committed);
    // The second batch is never committed: shutdown abandons it.
    second.ack.complete(AckOutcome::Abandoned);
    opened.source.close().await.unwrap();

    let stats = opened.handle.stats();
    assert_eq!(
        stats.acks_written, 5,
        "close waits for the committed acknowledgements to be written"
    );
    eventually(
        Duration::from_secs(5),
        "a clean close sends DISCONNECT",
        || {
            proxy
                .events()
                .iter()
                .any(|e| matches!(e.pkt, Pkt::Disconnect))
        },
    )
    .await;
    let redelivered =
        collect_redelivery(&broker.addr(), &client_id, &topic, Duration::from_secs(4)).await;
    let ids: Vec<u64> = redelivered.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        ids,
        [5, 6, 7, 8, 9],
        "only the unacknowledged batch is redelivered"
    );
    assert!(redelivered.iter().all(|(_, dup)| *dup));
}

per_broker!(
    mosquitto_close_keeps_unacked,
    emqx_close_keeps_unacked,
    close_keeps_unacked_messages
);

// ---------------------------------------------------------------------------
// TLS and mutual TLS with rustls + ring
// ---------------------------------------------------------------------------

async fn tls_and_mtls(kind: Kind) {
    let certs = Certs::generate();
    let broker = start(
        kind,
        Opts {
            tls: Some(TlsDir {
                dir: certs.dir.path().to_owned(),
            }),
            ..Opts::default()
        },
    )
    .await;
    let topic = unique("thadam/tls");
    let publisher = Publisher::connect(&broker.addr(), 1);
    let ca = certs.path("ca.pem");
    let tls_url = format!("mqtts://localhost:{}", broker.tls_port.unwrap());
    let mtls_url = format!("mqtts://localhost:{}", broker.mtls_port.unwrap());

    // Server authentication with the broker's CA.
    let config = with(
        base_config(&tls_url, &unique("tls"), &topic),
        "tls",
        json!({ "ca": ca }),
    );
    let mut opened = open_source(&config).await;
    publisher.publish(&topic, sized_payload(1, 32)).await;
    let (got, _) = pull_ids(&mut opened.source, 1, Duration::from_secs(20), true).await;
    assert_eq!(got, [1], "TLS");
    opened.source.close().await.unwrap();

    // Mutual TLS with a client certificate.
    let config = with(
        base_config(&mtls_url, &unique("mtls"), &topic),
        "tls",
        json!({ "ca": ca, "cert": certs.path("client.pem"), "key": certs.path("client.key") }),
    );
    let mut opened = open_source(&config).await;
    publisher.publish(&topic, sized_payload(2, 32)).await;
    let (got, _) = pull_ids(&mut opened.source, 1, Duration::from_secs(20), true).await;
    assert_eq!(got, [2], "mutual TLS");
    opened.source.close().await.unwrap();

    // A CA the broker's certificate does not chain to is refused.
    let config = with(
        with(
            base_config(&tls_url, &unique("badca"), &topic),
            "open_timeout",
            json!("4s"),
        ),
        "tls",
        json!({ "ca": certs.path("other-ca.pem") }),
    );
    let (mut bad, result) = try_open(&config).await;
    let error = result.expect_err("an untrusted broker certificate must not connect");
    assert_eq!(error.code, codes::CONNECTOR_TIMEOUT, "{error}");
    assert!(error.is_transient());
    bad.source.close().await.unwrap();

    // The mutual-TLS listener refuses a client without a certificate.
    let config = with(
        with(
            base_config(&mtls_url, &unique("nocert"), &topic),
            "open_timeout",
            json!("4s"),
        ),
        "tls",
        json!({ "ca": ca }),
    );
    let (mut bad, result) = try_open(&config).await;
    assert!(
        result.is_err(),
        "mutual TLS without a client certificate must not connect"
    );
    bad.source.close().await.unwrap();
}

per_broker!(mosquitto_tls_and_mtls, emqx_tls_and_mtls, tls_and_mtls);

// ---------------------------------------------------------------------------
// MQTT 5 (experimental, behind the `v5` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "v5")]
async fn v5_smoke(kind: Kind) {
    let broker = start(kind, Opts::default()).await;
    let topic = unique("thadam/v5");
    let config = with(
        with(
            with(
                base_config(&broker.url(), &unique("v5"), &topic),
                "protocol",
                json!("v5"),
            ),
            "keep_alive",
            json!("5s"),
        ),
        "receive_maximum",
        json!(10),
    );
    let mut opened = open_source(&config).await;
    let publisher = Publisher::connect(&broker.addr(), 1);
    publisher.publish_ids(&topic, 0..30, 32).await;

    // Receive Maximum is the client's window: with nothing acknowledged the
    // broker stops at the 10 the source asked for.
    let (held_ids, held) = pull_ids(&mut opened.source, 30, Duration::from_secs(3), false).await;
    assert_eq!(held_ids, (0..10).collect::<Vec<_>>());
    assert_eq!(opened.handle.stats().unacked_max, 10);
    for handle in held {
        handle.complete(AckOutcome::Committed);
    }
    let (rest, _) = pull_ids(&mut opened.source, 20, Duration::from_secs(30), true).await;
    assert_eq!(rest, (10..30).collect::<Vec<_>>());
    assert_eq!(opened.handle.stats().duplicates, 0);
    opened.source.close().await.unwrap();
}

#[cfg(feature = "v5")]
per_broker!(mosquitto_v5_smoke, emqx_v5_smoke, v5_smoke);
