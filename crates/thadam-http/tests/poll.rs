//! Poll mode against a local axum server.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use thadam_core::{AckMode, BatchLimits, Shutdown, Source, SourceContext, SourceId};
use thadam_http::HttpSource;
use thadam_testkit::FakeClock;
use tokio::net::TcpListener;

#[derive(Default)]
struct Probe {
    hits: AtomicUsize,
    flaky_hits: AtomicUsize,
    slow_hits: AtomicUsize,
    times: Mutex<Vec<Instant>>,
}

async fn spawn_server() -> (SocketAddr, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let router = Router::new()
        .route("/ok", get(ok))
        .route("/flaky", get(flaky))
        .route("/slow", get(slow))
        .with_state(Arc::clone(&probe));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (addr, probe)
}

async fn ok(axum::extract::State(probe): axum::extract::State<Arc<Probe>>) -> Response {
    let n = probe.hits.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain")],
        format!("hello-{n}"),
    )
        .into_response()
}

async fn flaky(axum::extract::State(probe): axum::extract::State<Arc<Probe>>) -> Response {
    probe.times.lock().unwrap().push(Instant::now());
    let n = probe.flaky_hits.fetch_add(1, Ordering::SeqCst);
    if n < 3 {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    } else {
        (StatusCode::OK, "recovered").into_response()
    }
}

async fn slow(axum::extract::State(probe): axum::extract::State<Arc<Probe>>) -> Response {
    probe.slow_hits.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(2)).await;
    (StatusCode::OK, "too late").into_response()
}

async fn open(config: Value) -> HttpSource {
    let config = thadam_http::HttpConfig::from_value(&config)
        .unwrap_or_else(|errors| panic!("config: {errors:?}"));
    let mut source = HttpSource::new(config);
    let ctx = SourceContext::new(
        SourceId::new("poll-test"),
        Shutdown::new(),
        FakeClock::default().shared(),
    );
    source.open(&ctx).await.expect("open");
    source
}

fn limits() -> BatchLimits {
    BatchLimits {
        max_messages: 1,
        linger: Duration::ZERO,
        ..BatchLimits::default()
    }
}

async fn next(source: &mut HttpSource) -> thadam_core::SourceBatch {
    tokio::time::timeout(Duration::from_secs(10), source.next_batch(limits()))
        .await
        .expect("no message within 10s")
        .expect("next_batch failed")
        .expect("unexpected end of stream")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_responses_become_messages() {
    let (addr, _probe) = spawn_server().await;
    let mut source = open(json!({
        "mode": "poll",
        "poll": {
            "urls": [format!("http://{addr}/ok?token=secret")],
            "interval": "20ms"
        }
    }))
    .await;
    assert_eq!(source.capabilities().ack_mode, AckMode::OnReceive);

    let batch = next(&mut source).await;
    let message = &batch.batch.messages[0];
    assert_eq!(message.payload.as_ref(), b"hello-0");
    assert_eq!(message.attrs.get("http.status"), Some("200"));
    assert_eq!(message.attrs.get("http.content_type"), Some("text/plain"));
    let url = message.attrs.get("http.url").unwrap();
    assert!(url.ends_with("/ok"), "{url}");
    assert!(!url.contains("secret"), "query string leaked: {url}");
    batch.ack.complete(thadam_core::AckOutcome::Committed);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors_back_off_and_then_recover() {
    let (addr, probe) = spawn_server().await;
    let mut source = open(json!({
        "mode": "poll",
        "poll": {
            "urls": [format!("http://{addr}/flaky")],
            "interval": "20ms",
            "backoff_max": "1s"
        }
    }))
    .await;

    let batch = next(&mut source).await;
    assert_eq!(batch.batch.messages[0].payload.as_ref(), b"recovered");
    assert_eq!(
        batch.batch.messages[0].attrs.get("http.status"),
        Some("200")
    );
    batch.ack.complete(thadam_core::AckOutcome::Committed);

    let times = probe.times.lock().unwrap().clone();
    assert!(
        times.len() >= 4,
        "expected 3 failures then success, saw {}",
        times.len()
    );
    let first_gap = times[1] - times[0];
    let third_gap = times[3] - times[2];
    assert!(
        third_gap >= Duration::from_millis(40),
        "third retry came after {third_gap:?}, expected the backoff window to have doubled"
    );
    assert!(third_gap > first_gap, "{first_gap:?} vs {third_gap:?}");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_responses_produce_no_message() {
    let (addr, probe) = spawn_server().await;
    let mut source = open(json!({
        "mode": "poll",
        "poll": {
            "urls": [format!("http://{addr}/slow"), format!("http://{addr}/ok")],
            "interval": "20ms",
            "timeout": "100ms"
        }
    }))
    .await;

    for _ in 0..3 {
        let batch = next(&mut source).await;
        let url = batch.batch.messages[0].attrs.get("http.url").unwrap();
        assert!(
            url.ends_with("/ok"),
            "a timed-out response leaked a message: {url}"
        );
        batch.ack.complete(thadam_core::AckOutcome::Committed);
    }
    assert!(
        probe.slow_hits.load(Ordering::SeqCst) >= 1,
        "slow URL was never polled"
    );
    source.close().await.unwrap();
}
