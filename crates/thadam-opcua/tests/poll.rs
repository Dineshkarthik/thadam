//! End-to-end poll-mode tests against an in-process OPC UA server: one
//! message per poll carrying every node, a Bad value, and recovery after a
//! server restart.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::time::Duration;

use thadam_core::Shutdown;
use serde_json::{Value, json};
use support::{
    TestNodes, TestServer, bad_sensor_value, collect_until, nodes_json, open_source, payload,
    spin_ticks, start_server,
};
use tokio::sync::Mutex;

const T: Duration = Duration::from_secs(10);

fn poll_config(server: &TestServer, nodes: &TestNodes, interval: &str) -> Value {
    json!({
        "endpoint": server.endpoint,
        "security_policy": "none",
        "security_mode": "none",
        "mode": "poll",
        "interval": interval,
        "reconnect": {"min_backoff": "100ms", "max_backoff": "500ms",
                      "max_handshakes_per_minute": 6000},
        "nodes": nodes_json(nodes)
    })
}

const NODE_NAMES: [&str; 5] = ["temp", "running", "label", "accel", "counter"];

#[tokio::test]
async fn poll_emits_one_message_per_poll_with_every_node() {
    let (server, nodes) = start_server(0).await;
    let shutdown = Shutdown::new();
    let ticks = spin_ticks(server.control(), nodes.clone(), Duration::from_millis(50));
    let source = Mutex::new(open_source(poll_config(&server, &nodes, "100ms"), &shutdown).await);

    let started = tokio::time::Instant::now();
    let mut seen = 0usize;
    let observed = collect_until(&source, T, |_| {
        seen += 1;
        seen >= 6
    })
    .await;
    let elapsed = started.elapsed();
    assert!(observed.len() >= 6, "only {} polls", observed.len());
    // Five 100ms gaps at least: the interval paces the reads.
    assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");

    let mut counters = std::collections::BTreeSet::new();
    for message in &observed {
        let body = payload(message);
        for name in NODE_NAMES {
            assert!(body.get(name).is_some(), "missing {name} in {body}");
        }
        assert!(body["ts_us"].is_i64(), "{body}");
        if let Some(c) = body["counter"].as_i64() {
            counters.insert(c);
        }
        assert_eq!(
            message.attrs.get("opcua.endpoint"),
            Some(server.endpoint.as_str())
        );
        assert_eq!(message.attrs.get("topic"), Some("opcua/127.0.0.1/poll"));
        assert!(message.attrs.get("opcua.subscription_id").is_none());
        assert!(message.attrs.get("opcua.bad_status").is_none());
    }
    assert!(counters.len() >= 2, "counter never moved: {counters:?}");

    let now_us = chrono::Utc::now().timestamp_micros();
    let ts = payload(&observed[0])["ts_us"].as_i64().expect("ts_us");
    assert!(
        (now_us - ts).abs() < Duration::from_secs(60).as_micros() as i64,
        "ts_us {ts} is not near now {now_us}"
    );

    ticks.cancel();
    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn poll_reports_a_bad_value_as_null_and_names_it() {
    let (server, nodes) = start_server(0).await;
    let shutdown = Shutdown::new();
    let control = server.control();
    let source = Mutex::new(open_source(poll_config(&server, &nodes, "100ms"), &shutdown).await);

    let _ = collect_until(&source, T, |m| payload(m)["running"].is_boolean()).await;
    control.set_value(&nodes.temp, bad_sensor_value(0.0));

    let observed = collect_until(&source, T, |m| {
        m.attrs
            .get("opcua.bad_status")
            .is_some_and(|s| s.contains("temp=BadSensorFailure"))
    })
    .await;
    let poisoned = observed
        .iter()
        .find(|m| m.attrs.get("opcua.bad_status").is_some())
        .expect("a poll naming the bad status");
    assert_eq!(
        poisoned.attrs.get("opcua.bad_status"),
        Some("temp=BadSensorFailure")
    );
    let body = payload(poisoned);
    assert_eq!(body["temp"], Value::Null, "{body}");
    // Polls carry every node, so the rest of the message is still there.
    assert_eq!(body["running"], json!(true), "{body}");
    assert_eq!(body["label"], json!("pump"), "{body}");

    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn poll_recovers_after_a_server_restart() {
    let (server, nodes) = start_server(0).await;
    let port = server.addr.port();
    let shutdown = Shutdown::new();
    let source = Mutex::new(open_source(poll_config(&server, &nodes, "100ms"), &shutdown).await);

    let observed = collect_until(&source, T, |m| payload(m)["running"].is_boolean()).await;
    assert!(!observed.is_empty());
    server.stop().await;

    let (server2, nodes2) = start_server(port).await;
    server2.control().tick(&nodes2, 777);
    let recovered = collect_until(&source, Duration::from_secs(20), |m| {
        payload(m)["counter"].as_i64() == Some(777)
    })
    .await;
    assert!(!recovered.is_empty(), "no polls after the server came back");
    assert_eq!(
        payload(recovered.last().expect("message"))["counter"],
        json!(777)
    );

    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server2.stop().await;
}
