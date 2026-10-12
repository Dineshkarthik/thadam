//! End-to-end tests against an in-process OPC UA server: subscribe and
//! receive, value mapping, Bad statuses, server restart with reconnect and
//! resubscribe, and `basic256sha256`/`sign_and_encrypt` with username auth.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use thadam_core::SourceFactory;
use thadam_core::{
    BatchLimits, RawMessage, ThadamError, Shutdown, Source, SourceContext, SourceId, SystemClock,
    codes,
};
use thadam_opcua::OpcUaSourceFactory;
use serde_json::{Value, json};
use support::{
    PASSWORD, ServerKind, TestNodes, TestServer, USERNAME, bad_sensor_value, make_client_cert,
    spin_ticks, start_server,
};
use tokio::sync::Mutex;

const T: Duration = Duration::from_secs(10);
/// RSA handshakes in an unoptimized test build take seconds, so encrypted
/// cases get a wider budget than the plaintext ones.
const ENCRYPTED_T: Duration = Duration::from_secs(60);

fn ctx(shutdown: &Shutdown) -> SourceContext {
    SourceContext::new(
        SourceId::new("opcua-test"),
        shutdown.clone(),
        Arc::new(SystemClock::new()),
    )
}

fn base_config(server: &TestServer, nodes: &TestNodes) -> serde_json::Value {
    json!({
        "endpoint": server.endpoint,
        "security_policy": "none",
        "security_mode": "none",
        "subscribe": {"publishing_interval": "100ms", "sampling_interval": "25ms"},
        "reconnect": {"min_backoff": "100ms", "max_backoff": "500ms",
                      "max_handshakes_per_minute": 6000},
        "nodes": [
            {"node_id": TestNodes::id_str(&nodes.temp), "name": "temp"},
            {"node_id": TestNodes::id_str(&nodes.running), "name": "running"},
            {"node_id": TestNodes::id_str(&nodes.label), "name": "label"},
            {"node_id": TestNodes::id_str(&nodes.accel), "name": "accel"},
            {"node_id": TestNodes::id_str(&nodes.counter), "name": "counter"}
        ]
    })
}

fn limits() -> BatchLimits {
    BatchLimits {
        max_messages: 16,
        linger: Duration::from_millis(5),
        ..BatchLimits::default()
    }
}

/// Builds and opens a source from a config JSON document.
async fn open_source(cfg: serde_json::Value, shutdown: &Shutdown) -> Box<dyn Source> {
    let ctx = thadam_core::BuildContext::new(
        "opcua-1",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::new(),
    );
    let mut source = OpcUaSourceFactory::new()
        .build(cfg, &ctx)
        .expect("config builds");
    source.open(&self::ctx(shutdown)).await.expect("open");
    source
}

/// Collects messages until `until` returns true or the timeout passes.
async fn collect_until(
    source: &Mutex<Box<dyn Source>>,
    within: Duration,
    mut until: impl FnMut(&RawMessage) -> bool,
) -> Vec<RawMessage> {
    let deadline = tokio::time::Instant::now() + within;
    let mut out = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return out;
        }
        let batch = {
            let mut guard = source.lock().await;
            match tokio::time::timeout(remaining, guard.next_batch(limits())).await {
                Ok(Ok(Some(batch))) => batch.batch.messages,
                Ok(Ok(None)) => Vec::new(),
                Ok(Err(error)) => panic!("next_batch failed: {error}"),
                Err(_) => return out, // publish gap longer than the remaining budget
            }
        };
        let done = batch.iter().any(&mut until);
        out.extend(batch);
        if done {
            return out;
        }
    }
}

fn payload(message: &RawMessage) -> Value {
    serde_json::from_slice(&message.payload).expect("json payload")
}

#[tokio::test]
async fn subscribe_groups_publish_responses_and_maps_values() {
    let (server, nodes) = start_server(ServerKind::Plain, 0).await;
    let shutdown = Shutdown::new();
    let ticks = spin_ticks(server.control(), nodes.clone(), Duration::from_millis(50));
    let source = Mutex::new(open_source(base_config(&server, &nodes), &shutdown).await);

    // The first publish response carries the initial values for every
    // monitored item; later ones carry only the timer's changes. Collect
    // until the counter has taken two distinct values.
    let mut counters = std::collections::BTreeSet::new();
    let observed = collect_until(&source, T, |m| {
        if let Some(c) = payload(m)["counter"].as_i64() {
            counters.insert(c);
        }
        counters.len() >= 2
    })
    .await;
    assert!(!observed.is_empty(), "no publish responses received");
    let first = payload(&observed[0]);
    assert_eq!(first["running"], json!(true));
    assert_eq!(first["label"], json!("pump"));
    assert_eq!(first["accel"], json!([1.5, -2.5]));
    assert!(first["ts_us"].is_i64(), "{first}");

    let temps: Vec<f64> = observed
        .iter()
        .filter_map(|m| payload(m)["temp"].as_f64())
        .collect();
    assert!(!temps.is_empty(), "temp never reported");
    assert!(counters.len() >= 2, "counter values: {counters:?}");

    // Every message carries the documented attributes.
    let message = &observed[0];
    assert_eq!(
        message.attrs.get("opcua.endpoint"),
        Some(server.endpoint.as_str())
    );
    let sub_id = message
        .attrs
        .get("opcua.subscription_id")
        .expect("subscription id attr");
    assert!(sub_id.parse::<u32>().is_ok(), "{sub_id}");
    assert_eq!(
        message.attrs.get("topic"),
        Some(format!("opcua/127.0.0.1/{sub_id}").as_str())
    );
    assert!(message.attrs.get("opcua.bad_status").is_none());

    ticks.cancel();
    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn a_bad_status_becomes_null_and_is_named_in_the_attributes() {
    let (server, nodes) = start_server(ServerKind::Plain, 0).await;
    let shutdown = Shutdown::new();
    let control = server.control();
    let source = Mutex::new(open_source(base_config(&server, &nodes), &shutdown).await);

    // Wait for the initial values before poisoning `temp`.
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
        .expect("a message naming the bad status");
    let bad_status = poisoned.attrs.get("opcua.bad_status").expect("attr");
    assert_eq!(bad_status, "temp=BadSensorFailure");
    let body = payload(poisoned);
    assert_eq!(body["temp"], Value::Null, "{body}");
    // Publish responses carry only changes, so `counter` is absent here.
    assert!(body.get("counter").is_none(), "{body}");

    // Recovery: a Good value flows again.
    control.set_value(&nodes.temp, opcua::types::DataValue::new_now(21.5f64));
    let recovered = collect_until(&source, T, |m| payload(m)["temp"].as_f64().is_some()).await;
    assert!(!recovered.is_empty(), "no good value after the bad one");

    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn server_restart_reconnects_and_resubscribes() {
    let (server, nodes) = start_server(ServerKind::Plain, 0).await;
    let port = server.addr.port();
    let shutdown = Shutdown::new();
    let source = Mutex::new(open_source(base_config(&server, &nodes), &shutdown).await);

    let observed = collect_until(&source, T, |m| payload(m)["running"].is_boolean()).await;
    assert!(!observed.is_empty());
    server.stop().await;

    // A new server on the same port with fresh values: the source must
    // reconnect and resubscribe on its own.
    let (server2, nodes2) = start_server(ServerKind::Plain, port).await;
    server2.control().tick(&nodes2, 777);
    let recovered = collect_until(&source, Duration::from_secs(20), |m| {
        payload(m)["counter"].as_i64() == Some(777)
    })
    .await;
    assert!(
        !recovered.is_empty(),
        "no publish responses after the restart"
    );
    let message = recovered.last().expect("recovered message");
    assert_eq!(payload(message)["counter"], json!(777));
    assert!(
        message
            .attrs
            .get("opcua.subscription_id")
            .is_some_and(|s| s.parse::<u32>().is_ok())
    );
    assert!(payload(message)["ts_us"].as_i64().is_some_and(|ts| ts > 0));

    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server2.stop().await;
}

#[tokio::test]
async fn basic256sha256_sign_and_encrypt_with_username_auth() {
    let (server, nodes) = start_server(ServerKind::Encrypted, 0).await;
    let shutdown = Shutdown::new();
    let pki = tempfile::tempdir().expect("client pki");
    let (cert, key, pki_dir) = make_client_cert(pki.path());

    let mut cfg = base_config(&server, &nodes);
    cfg["security_policy"] = json!("basic256sha256");
    cfg["security_mode"] = json!("sign_and_encrypt");
    cfg["auth"] = json!({"kind": "username", "username": USERNAME, "password": PASSWORD});
    cfg["cert"] = json!({
        "cert": cert,
        "key": key,
        "pki_dir": pki_dir,
        "trust_server": true
    });

    let source = Mutex::new(open_source(cfg, &shutdown).await);
    let observed =
        collect_until(&source, ENCRYPTED_T, |m| payload(m)["running"].is_boolean()).await;
    assert!(
        !observed.is_empty(),
        "no publish responses over the encrypted channel"
    );
    assert_eq!(payload(&observed[0])["label"], json!("pump"));

    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn a_rejected_password_is_fatal_not_a_retry_storm() {
    let (server, nodes) = start_server(ServerKind::Encrypted, 0).await;
    let shutdown = Shutdown::new();
    let pki = tempfile::tempdir().expect("client pki");
    let (cert, key, pki_dir) = make_client_cert(pki.path());

    let mut cfg = base_config(&server, &nodes);
    cfg["security_policy"] = json!("basic256sha256");
    cfg["security_mode"] = json!("sign_and_encrypt");
    cfg["auth"] = json!({"kind": "username", "username": USERNAME, "password": "wrong"});
    cfg["cert"] = json!({
        "cert": cert,
        "key": key,
        "pki_dir": pki_dir,
        "trust_server": true
    });

    let mut source = open_source(cfg, &shutdown).await;
    let error: ThadamError = tokio::time::timeout(ENCRYPTED_T, async {
        loop {
            match source.next_batch(limits()).await {
                Ok(Some(batch)) => panic!("unexpected batch: {:?}", batch.batch.len()),
                Ok(None) => {}
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("an error within the timeout");
    assert_eq!(error.code, codes::AUTH_FAILED, "{error}");
    source.close().await.expect("close");
    server.stop().await;
}

/// Over a `none` channel there is no channel crypto, but a wrong username
/// must still be rejected by the server, and that rejection is fatal.
#[tokio::test]
async fn a_wrong_password_over_none_is_fatal_too() {
    let (server, nodes) = start_server(ServerKind::Plain, 0).await;
    let shutdown = Shutdown::new();
    let mut cfg = base_config(&server, &nodes);
    cfg["auth"] = json!({"kind": "username", "username": USERNAME, "password": "wrong"});

    let mut source = open_source(cfg, &shutdown).await;
    let result: StdResult<(), ThadamError> = tokio::time::timeout(T, async {
        loop {
            match source.next_batch(limits()).await {
                Ok(Some(_)) => {}
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("next_batch never surfaced the rejection"));
    assert_eq!(result.expect_err("fatal").code, codes::AUTH_FAILED);
    source.close().await.expect("close");
    server.stop().await;
}
