//! Admin HTTP API tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::body::Body;
use http_body_util::BodyExt;
use thadam_runtime::{AdminConfig, MetricsSink, PrometheusMetrics, router, validate_admin_config};
use thadam_testkit::TempDataDir;
use serde_json::Value;
use tower::ServiceExt;

use crate::common::{Kit, json_n, spec, yaml};

/// Creates a test pipeline handle using memory connectors.
///
/// This helper builds a minimal pipeline with a memory source and
/// a recording segment sink, then returns the handle without running it.
fn create_test_pipeline_handle() -> thadam_runtime::PipelineHandle {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let up = kit.sources.scripted("src", (1..=10).map(json_n));
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: recseg, config: { frames_per_file: 3 } }",
        "",
    );
    let registry = kit.registry();
    let opts = common::fast_opts();
    let pipeline = thadam_runtime::Pipeline::build(spec(&y), &registry, opts)
        .expect("failed to build pipeline");
    let handle = pipeline.handle();
    // Drop the pipeline to avoid running it
    drop(pipeline);
    drop(up);
    drop(kit);
    handle
}

/// One `recseg` sink; `frames` changes the configuration and so its hash.
fn recseg_yaml(data_dir: &std::path::Path, frames: u32) -> String {
    yaml(
        data_dir,
        &format!("  - {{ id: out, kind: recseg, config: {{ frames_per_file: {frames} }} }}"),
        "",
    )
}

/// Runs `yaml` to completion, recording into `metrics`, and returns its handle.
async fn run_once(yaml: &str, metrics: &Arc<PrometheusMetrics>) -> thadam_runtime::PipelineHandle {
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=3).map(json_n));
    let mut opts = common::fast_opts();
    opts.metrics = metrics.clone();
    opts.stop_when_sources_finish = true;
    let pipeline = thadam_runtime::Pipeline::build(spec(yaml), &kit.registry(), opts)
        .expect("failed to build pipeline");
    let handle = pipeline.handle();
    let report = pipeline
        .run(thadam_core::Shutdown::new())
        .await
        .expect("run failed to start");
    assert_eq!(report.exit, thadam_runtime::ExitStatus::Ok);
    handle
}

async fn get_body(router: axum::Router, uri: &str) -> String {
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{uri}");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&body).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_identifies_the_node_build_and_configuration() {
    let dir = TempDataDir::new().unwrap();
    let y = recseg_yaml(dir.path(), 3);
    let metrics = Arc::new(PrometheusMetrics::new());
    let handle = run_once(&y, &metrics).await;

    let body = get_body(router(handle, metrics, None), "/status").await;
    let status: Value = serde_json::from_str(&body).expect("status should be valid JSON");
    assert_eq!(status["node_id"], "node-1", "{body}");
    assert_eq!(status["pipeline"], "test", "{body}");
    assert_eq!(status["thadam_version"], env!("CARGO_PKG_VERSION"), "{body}");
    assert_eq!(status["config_hash"], spec(&y).config_hash_hex(), "{body}");
    assert_eq!(status["config_hash_version"], 1, "{body}");
    assert_eq!(status["previous_config_hash"], Value::Null, "{body}");
    assert!(
        status["started_at"].as_i64().is_some_and(|t| t > 0),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_names_the_configuration_of_the_run_before() {
    let dir = TempDataDir::new().unwrap();
    let first = recseg_yaml(dir.path(), 3);
    let second = recseg_yaml(dir.path(), 4);
    let metrics = Arc::new(PrometheusMetrics::new());
    run_once(&first, &metrics).await;
    let handle = run_once(&second, &metrics).await;

    let body = get_body(router(handle, metrics, None), "/status").await;
    let status: Value = serde_json::from_str(&body).expect("status should be valid JSON");
    assert_eq!(
        status["config_hash"],
        spec(&second).config_hash_hex(),
        "{body}"
    );
    assert_eq!(
        status["previous_config_hash"],
        spec(&first).config_hash_hex(),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_info_gauge_names_the_running_configuration() {
    let dir = TempDataDir::new().unwrap();
    let y = recseg_yaml(dir.path(), 3);
    let metrics = Arc::new(PrometheusMetrics::new());
    let handle = run_once(&y, &metrics).await;

    let body = get_body(router(handle, metrics, None), "/metrics").await;
    let want = format!(
        "thadam_build_info{{version=\"{}\",config_hash=\"{}\",node_id=\"node-1\",pipeline=\"test\"}} 1",
        env!("CARGO_PKG_VERSION"),
        spec(&y).config_hash_hex()
    );
    assert!(
        body.lines().any(|line| line == want),
        "missing `{want}` in:\n{body}"
    );
}

#[tokio::test]
async fn healthz_returns_200_ok() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let router = router(handle, metrics, None);

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"ok");
}

#[tokio::test]
async fn metrics_returns_thadam_metrics() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let router = router(handle, metrics.clone(), None);

    // Record a sample metric to ensure there's data
    metrics.record(thadam_runtime::MetricEvent::SourceRestart { source: "test" });

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let content_type = response.headers().get("content-type").unwrap();
    assert_eq!(
        content_type,
        "application/openmetrics-text; version=1.0.0; charset=utf-8"
    );

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("thadam_"),
        "metrics should contain thadam_ prefix: {body_str}"
    );
}

#[tokio::test]
async fn status_returns_valid_json() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let router = router(handle, metrics, None);

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);

    // Parse as JSON to verify it's valid
    let json: Value = serde_json::from_str(&body_str).expect("status should be valid JSON");

    // Check that expected fields are present
    let obj = json.as_object().expect("status should be a JSON object");
    assert!(
        obj.contains_key("pipeline"),
        "status should have 'pipeline' field"
    );
    assert!(
        obj.contains_key("phase"),
        "status should have 'phase' field"
    );
    assert!(
        obj.contains_key("components"),
        "status should have 'components' field"
    );
    assert!(
        obj.contains_key("cursors"),
        "status should have 'cursors' field"
    );
}

#[tokio::test]
async fn readyz_returns_status_based_on_readiness() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let router = router(handle.clone(), metrics, None);

    // Initially the pipeline should not be ready (not running)
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should be 503 since pipeline is not running
    assert_eq!(response.status(), 503);
}

#[tokio::test]
async fn status_with_token_requires_auth() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let token = "test-secret-token".to_string();
    let router = router(handle, metrics, Some(token));

    // Request without auth header should get 401
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn status_with_correct_token_succeeds() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let token = "test-secret-token".to_string();
    let router = router(handle, metrics, Some(token.clone()));

    // Request with correct auth header should succeed
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/status")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body);

    // Parse as JSON to verify it's valid
    let json: Value = serde_json::from_str(&body_str).expect("status should be valid JSON");
    let obj = json.as_object().expect("status should be a JSON object");
    assert!(obj.contains_key("pipeline"));
}

#[tokio::test]
async fn metrics_with_token_requires_auth() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let token = "test-secret-token".to_string();
    let router = router(handle, metrics, Some(token));

    // Request without auth header should get 401
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn readyz_with_token_requires_auth() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let token = "test-secret-token".to_string();
    let router = router(handle, metrics, Some(token));

    // Request without auth header should get 401
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn healthz_never_requires_auth() {
    let handle = create_test_pipeline_handle();
    let metrics = Arc::new(PrometheusMetrics::new());
    let token = "test-secret-token".to_string();
    let router = router(handle, metrics, Some(token));

    // healthz should work without auth even when token is configured
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[test]
fn validate_admin_config_rejects_non_loopback_without_token() {
    let cfg = AdminConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9464),
        token: None,
    };
    let err = validate_admin_config(&cfg).unwrap_err();
    assert_eq!(err.domain, thadam_core::ErrorDomain::Config);
    assert_eq!(err.code, thadam_core::codes::CONFIG_INCOMPATIBLE);
}

#[test]
fn validate_admin_config_accepts_loopback_without_token() {
    let cfg = AdminConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9464),
        token: None,
    };
    validate_admin_config(&cfg).expect("loopback without token should be accepted");
}

#[test]
fn validate_admin_config_accepts_non_loopback_with_token() {
    let cfg = AdminConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9464),
        token: Some("token".to_string()),
    };
    validate_admin_config(&cfg).expect("non-loopback with token should be accepted");
}
