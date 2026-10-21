//! Tests against a scripted in-process MQTT 3.1.1 broker: no Docker needed.
//!
//! The broker answers exactly as the test tells it to, which makes the paths a
//! real broker rarely takes deterministic: refused credentials, rejected
//! subscriptions, `session_present` flipping across a reconnect, and the exact
//! bytes of a PUBACK.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{
    base_config, build_source, context, eventually, limits, open_source, payload_ids, try_open,
    with, within,
};
use serde_json::json;
use thadam_core::{AckOutcome, AckStatus, Retry, Shutdown, Source, codes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Seen {
    connects: u32,
    clean_flags: Vec<bool>,
    subscribes: Vec<Vec<(String, u8)>>,
    pubacks: Vec<u16>,
    disconnects: u32,
    pingreqs: u32,
}

struct Script {
    connack_rc: AtomicU8,
    session_present: AtomicBool,
    suback_code: AtomicU8,
    seen: Mutex<Seen>,
    to_send: Mutex<Vec<Vec<u8>>>,
    kill: Mutex<CancellationToken>,
}

struct Scripted {
    port: u16,
    script: Arc<Script>,
}

fn publish_packet(
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    dup: bool,
    pkid: u16,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&u16::try_from(topic.len()).unwrap().to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&pkid.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let mut packet = vec![0x30 | (u8::from(dup) << 3) | (qos << 1) | u8::from(retain)];
    let mut len = body.len();
    loop {
        let mut byte = u8::try_from(len % 128).unwrap();
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if len == 0 {
            break;
        }
    }
    packet.extend(body);
    packet
}

impl Scripted {
    async fn start() -> Scripted {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let script = Arc::new(Script {
            connack_rc: AtomicU8::new(0),
            session_present: AtomicBool::new(false),
            suback_code: AtomicU8::new(1),
            seen: Mutex::new(Seen::default()),
            to_send: Mutex::new(Vec::new()),
            kill: Mutex::new(CancellationToken::new()),
        });
        let s = script.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let token = s.kill.lock().unwrap().clone();
                let s = s.clone();
                tokio::spawn(async move {
                    let _ = tokio::select! {
                        r = serve(stream, s) => r,
                        () = token.cancelled() => Ok(()),
                    };
                });
            }
        });
        Scripted { port, script }
    }

    fn url(&self) -> String {
        format!("mqtt://127.0.0.1:{}", self.port)
    }

    fn queue(&self, packet: Vec<u8>) {
        self.script.to_send.lock().unwrap().push(packet);
    }

    fn drop_connections(&self) {
        let mut kill = self.script.kill.lock().unwrap();
        kill.cancel();
        *kill = CancellationToken::new();
    }

    fn seen<T>(&self, f: impl FnOnce(&Seen) -> T) -> T {
        f(&self.script.seen.lock().unwrap())
    }
}

async fn serve(mut stream: TcpStream, script: Arc<Script>) -> std::io::Result<()> {
    loop {
        let mut head = [0u8; 1];
        stream.read_exact(&mut head).await?;
        let (mut len, mut mult) = (0usize, 1usize);
        loop {
            let mut b = [0u8; 1];
            stream.read_exact(&mut b).await?;
            len += usize::from(b[0] & 0x7f) * mult;
            mult *= 128;
            if b[0] & 0x80 == 0 {
                break;
            }
        }
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await?;
        match head[0] >> 4 {
            1 => {
                let name = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let flags = body[2 + name + 1];
                {
                    let mut seen = script.seen.lock().unwrap();
                    seen.connects += 1;
                    seen.clean_flags.push(flags & 0x02 != 0);
                }
                let rc = script.connack_rc.load(Ordering::Relaxed);
                let sp = u8::from(script.session_present.load(Ordering::Relaxed));
                stream.write_all(&[0x20, 0x02, sp, rc]).await?;
                if rc != 0 {
                    return Ok(());
                }
            }
            8 => {
                let pkid = [body[0], body[1]];
                let mut filters = Vec::new();
                let mut at = 2;
                while at < body.len() {
                    let n = usize::from(u16::from_be_bytes([body[at], body[at + 1]]));
                    let filter = String::from_utf8(body[at + 2..at + 2 + n].to_vec()).unwrap();
                    filters.push((filter, body[at + 2 + n]));
                    at += 3 + n;
                }
                let code = script.suback_code.load(Ordering::Relaxed);
                let mut ack = vec![
                    0x90,
                    u8::try_from(2 + filters.len()).unwrap(),
                    pkid[0],
                    pkid[1],
                ];
                ack.extend(std::iter::repeat_n(code, filters.len()));
                script.seen.lock().unwrap().subscribes.push(filters);
                stream.write_all(&ack).await?;
                let pending: Vec<Vec<u8>> = std::mem::take(&mut *script.to_send.lock().unwrap());
                for packet in pending {
                    stream.write_all(&packet).await?;
                }
            }
            4 => script
                .seen
                .lock()
                .unwrap()
                .pubacks
                .push(u16::from_be_bytes([body[0], body[1]])),
            12 => {
                script.seen.lock().unwrap().pingreqs += 1;
                stream.write_all(&[0xd0, 0x00]).await?;
            }
            14 => {
                script.seen.lock().unwrap().disconnects += 1;
                return Ok(());
            }
            _ => {}
        }
    }
}

fn config(broker: &Scripted) -> serde_json::Value {
    with(
        base_config(&broker.url(), &common::unique("scripted"), "robots/#"),
        "open_timeout",
        json!("3s"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_credentials_fail_open_fatally_with_an_auth_error() {
    for rc in [4u8, 5] {
        let broker = Scripted::start().await;
        broker.script.connack_rc.store(rc, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let (mut opened, result) = try_open(&config(&broker)).await;
        let error = result.expect_err("a refused connection must fail open");
        assert_eq!(error.code, codes::AUTH_FAILED, "rc {rc}: {error}");
        assert_eq!(error.retry, Retry::Fatal);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "fatal refusals must not wait out the open timeout"
        );
        assert_eq!(
            broker.seen(|s| s.connects),
            1,
            "a fatal refusal is not retried"
        );
        opened.source.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_is_unavailable_is_retried_until_the_open_timeout() {
    let broker = Scripted::start().await;
    broker.script.connack_rc.store(3, Ordering::Relaxed);
    let (mut opened, result) = try_open(&with(config(&broker), "open_timeout", json!("2s"))).await;
    let error = result.expect_err("an unavailable broker never becomes ready");
    assert_eq!(error.code, codes::CONNECTOR_TIMEOUT);
    assert!(error.is_transient());
    assert!(
        broker.seen(|s| s.connects) >= 2,
        "transient refusals are retried"
    );
    opened.source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_subscription_fails_open_with_a_forbidden_error() {
    let broker = Scripted::start().await;
    broker.script.suback_code.store(0x80, Ordering::Relaxed);
    let (mut opened, result) = try_open(&config(&broker)).await;
    let error = result.expect_err("a refused subscription must fail open");
    assert_eq!(error.code, codes::AUTH_FORBIDDEN, "{error}");
    assert_eq!(error.retry, Retry::Fatal);
    opened.source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_carry_topic_qos_retain_and_dup_and_acks_name_the_packet() {
    let broker = Scripted::start().await;
    broker.queue(publish_packet(
        "robots/r7/telemetry",
        &common::sized_payload(1, 20),
        1,
        true,
        false,
        41,
    ));
    broker.queue(publish_packet(
        "robots/r7/telemetry",
        &common::sized_payload(2, 20),
        0,
        false,
        false,
        0,
    ));
    broker.queue(publish_packet(
        "robots/r8/pose",
        &common::sized_payload(3, 20),
        1,
        false,
        true,
        42,
    ));
    let mut opened = open_source(&config(&broker)).await;

    let batch = within(
        Duration::from_secs(5),
        "the first batch",
        opened.source.next_batch(limits(10)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(payload_ids(&batch.batch), [1, 2, 3]);
    let attrs: Vec<_> = batch
        .batch
        .messages
        .iter()
        .map(|m| {
            (
                m.attrs.get("mqtt.topic").unwrap().to_owned(),
                m.attrs.get("mqtt.qos").unwrap().to_owned(),
                m.attrs.get("mqtt.retain").unwrap().to_owned(),
                m.attrs.get("mqtt.dup").unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        attrs,
        [
            ("robots/r7/telemetry", "1", "true", "false"),
            ("robots/r7/telemetry", "0", "false", "false"),
            ("robots/r8/pose", "1", "false", "true"),
        ]
        .map(|(a, b, c, d)| (a.to_owned(), b.to_owned(), c.to_owned(), d.to_owned()))
    );
    assert!(
        batch
            .batch
            .messages
            .iter()
            .all(|m| m.ingest_time.as_nanos() > 0)
    );
    assert!(
        batch
            .batch
            .messages
            .iter()
            .all(|m| m.attrs.get("mqtt.gap").is_none())
    );

    assert_eq!(
        broker.seen(|s| s.pubacks.clone()),
        Vec::<u16>::new(),
        "nothing is acknowledged before commit"
    );
    assert_eq!(
        batch.ack.complete(AckOutcome::Committed),
        AckStatus::Delivered
    );
    eventually(Duration::from_secs(5), "the PUBACKs", || {
        broker.seen(|s| s.pubacks.len()) == 2
    })
    .await;
    assert_eq!(
        broker.seen(|s| s.pubacks.clone()),
        [41, 42],
        "PUBACKs follow receive order and QoS 0 gets none"
    );
    assert_eq!(opened.handle.stats().duplicates, 1);
    opened.source.close().await.unwrap();
    eventually(Duration::from_secs(5), "close sends DISCONNECT", || {
        broker.seen(|s| s.disconnects) == 1
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_session_raises_session_lost_and_resubscribes() {
    let broker = Scripted::start().await;
    let lost = Arc::new(Mutex::new(Vec::new()));
    let sink = lost.clone();
    let (source, handle) = build_source(&config(&broker));
    let mut source =
        source.with_session_lost_hook(Arc::new(move |e| sink.lock().unwrap().push(e.clone())));
    source.open(&context(&Shutdown::new())).await.unwrap();
    assert_eq!(broker.seen(|s| s.subscribes.len()), 1);
    assert_eq!(
        handle.stats().session_lost,
        0,
        "a first connection without a session is normal"
    );

    // The broker restarts without persistence: the next connection has no session.
    broker.queue(publish_packet(
        "robots/a",
        &common::sized_payload(7, 16),
        1,
        false,
        false,
        1,
    ));
    broker
        .script
        .session_present
        .store(false, Ordering::Relaxed);
    broker.drop_connections();
    eventually(Duration::from_secs(10), "the reconnect", || {
        handle.stats().connects == 2
    })
    .await;
    eventually(Duration::from_secs(5), "the second SUBSCRIBE", || {
        broker.seen(|s| s.subscribes.len()) == 2
    })
    .await;
    assert_eq!(handle.stats().session_lost, 1);
    assert_eq!(lost.lock().unwrap().len(), 1);
    assert_eq!(lost.lock().unwrap()[0].epoch.get(), handle.stats().epoch);
    assert_eq!(
        broker.seen(|s| s.subscribes[1].clone()),
        [("robots/#".to_owned(), 1)]
    );

    let batch = within(
        Duration::from_secs(5),
        "the message after the loss",
        source.next_batch(limits(4)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        batch.batch.messages[0].attrs.get("mqtt.gap"),
        Some("session_lost")
    );
    batch.ack.complete(AckOutcome::Committed);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_session_neither_warns_nor_resubscribes() {
    let broker = Scripted::start().await;
    let (mut source, handle) = build_source(&config(&broker));
    source.open(&context(&Shutdown::new())).await.unwrap();

    broker.script.session_present.store(true, Ordering::Relaxed);
    broker.drop_connections();
    eventually(Duration::from_secs(10), "the reconnect", || {
        handle.stats().connects == 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(handle.stats().session_lost, 0);
    assert_eq!(
        broker.seen(|s| s.subscribes.len()),
        1,
        "a resumed session keeps its subscriptions"
    );
    assert_eq!(handle.stats().disconnects, 1);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_start_never_expects_a_session() {
    let broker = Scripted::start().await;
    let config = with(
        with(config(&broker), "clean_start", json!(true)),
        "ack_mode",
        json!("on_receive"),
    );
    let (mut source, handle) = build_source(&config);
    source.open(&context(&Shutdown::new())).await.unwrap();
    broker.drop_connections();
    eventually(Duration::from_secs(10), "the reconnect", || {
        handle.stats().connects == 2
    })
    .await;
    assert_eq!(handle.stats().session_lost, 0);
    assert_eq!(broker.seen(|s| s.clean_flags.clone()), [true, true]);
    assert_eq!(
        broker.seen(|s| s.subscribes.len()),
        2,
        "every clean session must subscribe again"
    );
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_receive_acknowledges_when_the_runtime_pulls() {
    let broker = Scripted::start().await;
    broker.queue(publish_packet(
        "t",
        &common::sized_payload(1, 16),
        1,
        false,
        false,
        9,
    ));
    let config = with(config(&broker), "ack_mode", json!("on_receive"));
    let mut opened = open_source(&config).await;
    assert_eq!(
        opened.source.capabilities().ack_mode,
        thadam_core::AckMode::OnReceive
    );
    let batch = within(
        Duration::from_secs(5),
        "the batch",
        opened.source.next_batch(limits(4)),
    )
    .await
    .unwrap()
    .unwrap();
    eventually(
        Duration::from_secs(5),
        "the PUBACK before any commit",
        || broker.seen(|s| s.pubacks.len()) == 1,
    )
    .await;
    assert_eq!(broker.seen(|s| s.pubacks.clone()), [9]);
    drop(batch);
    opened.source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_stays_unacknowledged_until_its_batch_commits_even_across_a_reconnect_race() {
    let broker = Scripted::start().await;
    broker.queue(publish_packet(
        "t",
        &common::sized_payload(1, 16),
        1,
        false,
        false,
        3,
    ));
    let mut opened = open_source(&config(&broker)).await;
    let batch = within(
        Duration::from_secs(5),
        "the batch",
        opened.source.next_batch(limits(4)),
    )
    .await
    .unwrap()
    .unwrap();
    broker.drop_connections();
    let handle = opened.handle.clone();
    eventually(Duration::from_secs(10), "the reconnect", move || {
        handle.stats().connects == 2
    })
    .await;
    assert_eq!(batch.ack.complete(AckOutcome::Committed), AckStatus::Stale);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        broker.seen(|s| s.pubacks.is_empty()),
        "a handle from a dead connection acknowledged something"
    );
    assert_eq!(opened.handle.stats().stale_acks, 1);
    opened.source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_open_cancels_it() {
    let broker = Scripted::start().await;
    broker.script.connack_rc.store(3, Ordering::Relaxed);
    let (mut source, _handle) = build_source(&with(config(&broker), "open_timeout", json!("30s")));
    let shutdown = Shutdown::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        trigger.trigger();
    });
    let started = std::time::Instant::now();
    let error = source
        .open(&context(&shutdown))
        .await
        .expect_err("cancelled");
    assert_eq!(error.code, codes::CONNECTOR_CANCELLED);
    assert!(started.elapsed() < Duration::from_secs(5));
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_is_not_usable_before_open_and_not_reopenable_after_close() {
    let broker = Scripted::start().await;
    let (mut source, _handle) = build_source(&config(&broker));
    let error = source
        .next_batch(limits(1))
        .await
        .expect_err("next_batch before open");
    assert_eq!(error.code, codes::CONNECTOR_PROTOCOL);
    source.open(&context(&Shutdown::new())).await.unwrap();
    assert!(
        source.open(&context(&Shutdown::new())).await.is_err(),
        "open twice"
    );
    source.close().await.unwrap();
    assert!(
        source.next_batch(limits(1)).await.unwrap().is_none(),
        "end of stream after close"
    );
    assert!(
        source.open(&context(&Shutdown::new())).await.is_err(),
        "reopen after close"
    );
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_credential_files_fail_open_with_a_config_error() {
    let broker = Scripted::start().await;
    let config = with(
        with(config(&broker), "username", json!("thadam")),
        "password_file",
        json!("/nonexistent/password"),
    );
    let (mut opened, result) = try_open(&config).await;
    let error = result.expect_err("an unreadable password file");
    assert_eq!(error.code, codes::CONFIG_INVALID);
    assert!(error.message.contains("password_file"), "{error}");
    assert_eq!(
        broker.seen(|s| s.connects),
        0,
        "nothing connects with broken credentials"
    );
    opened.source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_broker_times_out_open_as_a_transient_error() {
    let config = with(
        base_config("mqtt://127.0.0.1:1", &common::unique("unreachable"), "t"),
        "open_timeout",
        json!("2s"),
    );
    let (mut opened, result) = try_open(&config).await;
    let error = result.expect_err("nothing listens on port 1");
    assert_eq!(error.code, codes::CONNECTOR_TIMEOUT);
    assert!(error.is_transient());
    opened.source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_source_survives_a_consumer_that_never_pulls_while_the_broker_keeps_pinging() {
    let broker = Scripted::start().await;
    let mut opened = open_source(&config(&broker)).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        broker.seen(|s| s.pingreqs) >= 2,
        "PINGREQs must flow without anyone calling next_batch: {}",
        broker.seen(|s| s.pingreqs)
    );
    assert_eq!(broker.seen(|s| s.connects), 1);
    opened.source.close().await.unwrap();
}
