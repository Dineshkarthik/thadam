//! Listen mode over real HTTP, with reqwest as the client.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};
use thadam_core::{AckOutcome, BatchLimits, Shutdown, Source, SourceContext, SourceId};
use thadam_http::HttpSource;
use thadam_testkit::FakeClock;

async fn open(config: Value) -> (HttpSource, SocketAddr) {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let config = thadam_http::HttpConfig::from_value(&config)
        .unwrap_or_else(|errors| panic!("config: {errors:?}"));
    let mut source = HttpSource::new(config);
    let ctx = SourceContext::new(
        SourceId::new("http-test"),
        Shutdown::new(),
        FakeClock::default().shared(),
    );
    source.open(&ctx).await.expect("open");
    let addr = source.local_addr().expect("bound address");
    (source, addr)
}

fn limits(max_messages: usize) -> BatchLimits {
    BatchLimits {
        max_messages,
        linger: Duration::from_millis(5),
        ..BatchLimits::default()
    }
}

async fn next(source: &mut HttpSource, max: usize) -> thadam_core::SourceBatch {
    tokio::time::timeout(Duration::from_secs(5), source.next_batch(limits(max)))
        .await
        .expect("next_batch timed out")
        .expect("next_batch failed")
        .expect("unexpected end of stream")
}

fn url(addr: SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_is_sent_only_after_the_batch_is_committed() {
    let (mut source, addr) = open(json!({"listen": {"bind": "127.0.0.1:0"}})).await;
    let client = reqwest::Client::new();
    let post = tokio::spawn({
        let client = client.clone();
        let target = url(addr, "/ingest");
        async move {
            client
                .post(target)
                .header("content-type", "application/octet-stream")
                .body("hello")
                .send()
                .await
        }
    });

    let batch = next(&mut source, 8).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !post.is_finished(),
        "response sent before the batch committed"
    );

    let message = &batch.batch.messages[0];
    assert_eq!(message.payload.as_ref(), b"hello");
    assert_eq!(message.attrs.get("http.path"), Some("/ingest"));
    assert_eq!(
        message.attrs.get("http.content_type"),
        Some("application/octet-stream")
    );
    assert!(
        message
            .attrs
            .get("http.remote_addr")
            .is_some_and(|a| a.starts_with("127.0.0.1:"))
    );

    batch.ack.complete(AckOutcome::Committed);
    let response = post.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_batch_answers_503_with_retry_after() {
    let (mut source, addr) = open(json!({"listen": {"bind": "127.0.0.1:0"}})).await;
    let post = tokio::spawn(async move {
        reqwest::Client::new()
            .post(url(addr, "/ingest"))
            .body("doomed")
            .send()
            .await
    });

    let batch = next(&mut source, 8).await;
    batch.ack.complete(AckOutcome::Abandoned);
    let response = post.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response.headers().contains_key("retry-after"));
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_queue_answers_503_without_blocking_the_accept_loop() {
    let (mut source, addr) = open(json!({
        "listen": {"bind": "127.0.0.1:0"},
        "queue_capacity": 16
    }))
    .await;

    let mut held = Vec::new();
    for i in 0..16 {
        let target = url(addr, "/ingest");
        held.push(tokio::spawn(async move {
            reqwest::Client::new()
                .post(target)
                .body(format!("m{i}"))
                .send()
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let overflow = reqwest::Client::new()
        .post(url(addr, "/ingest"))
        .body("overflow")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("overflow request must be answered, not stalled");
    assert_eq!(overflow.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(overflow.headers().contains_key("retry-after"));

    let batch = next(&mut source, 16).await;
    assert_eq!(batch.batch.messages.len(), 16);
    batch.ack.complete(AckOutcome::Committed);
    for post in held {
        let response = post.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_bodies_are_rejected_with_413() {
    let (mut source, addr) = open(json!({
        "listen": {"bind": "127.0.0.1:0"},
        "max_message_bytes": 8
    }))
    .await;
    let client = reqwest::Client::new();

    let declared = client
        .post(url(addr, "/ingest"))
        .body("123456789")
        .send()
        .await
        .unwrap();
    assert_eq!(declared.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let at_limit = tokio::spawn({
        let client = client.clone();
        let target = url(addr, "/ingest");
        async move { client.post(target).body("12345678").send().await }
    });
    let batch = next(&mut source, 1).await;
    assert_eq!(batch.batch.messages[0].payload.as_ref(), b"12345678");
    batch.ack.complete(AckOutcome::Committed);
    assert_eq!(
        at_limit.await.unwrap().unwrap().status(),
        StatusCode::NO_CONTENT
    );
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_token_is_required_when_configured() {
    let (mut source, addr) = open(json!({
        "listen": {"bind": "127.0.0.1:0", "bearer_token": "s3cret"}
    }))
    .await;
    let client = reqwest::Client::new();
    let target = url(addr, "/ingest");

    let missing = client.post(&target).body("x").send().await.unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert!(missing.headers().contains_key("www-authenticate"));

    let wrong = client
        .post(&target)
        .bearer_auth("s3cre")
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let right = tokio::spawn({
        let target = target.clone();
        async move {
            reqwest::Client::new()
                .post(target)
                .bearer_auth("s3cret")
                .body("authorized")
                .send()
                .await
        }
    });
    let batch = next(&mut source, 4).await;
    assert_eq!(batch.batch.messages[0].payload.as_ref(), b"authorized");
    batch.ack.complete(AckOutcome::Committed);
    assert_eq!(
        right.await.unwrap().unwrap().status(),
        StatusCode::NO_CONTENT
    );
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_posts_keep_per_connection_order() {
    const CLIENTS: usize = 4;
    const PER_CLIENT: usize = 10;
    let (mut source, addr) = open(json!({
        "listen": {"bind": "127.0.0.1:0", "extract_headers": ["X-Client"]}
    }))
    .await;

    let mut posts = Vec::new();
    for client_no in 0..CLIENTS {
        let target = url(addr, "/ingest");
        posts.push(tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut statuses = Vec::new();
            for seq in 0..PER_CLIENT {
                let response = client
                    .post(&target)
                    .header("x-client", client_no.to_string())
                    .body(format!("{client_no}:{seq}"))
                    .send()
                    .await
                    .unwrap();
                statuses.push(response.status());
            }
            statuses
        }));
    }

    let mut seen: Vec<(String, String)> = Vec::new();
    while seen.len() < CLIENTS * PER_CLIENT {
        let batch = next(&mut source, 7).await;
        for message in &batch.batch.messages {
            let client = message
                .attrs
                .get("http.header.x-client")
                .unwrap()
                .to_owned();
            let body = String::from_utf8(message.payload.to_vec()).unwrap();
            seen.push((client, body));
        }
        batch.ack.complete(AckOutcome::Committed);
    }

    for post in posts {
        for status in post.await.unwrap() {
            assert_eq!(status, StatusCode::NO_CONTENT);
        }
    }
    for client_no in 0..CLIENTS {
        let prefix = format!("{client_no}:");
        let seqs: Vec<usize> = seen
            .iter()
            .filter(|(c, _)| c == &client_no.to_string())
            .map(|(_, body)| body.strip_prefix(&prefix).unwrap().parse().unwrap())
            .collect();
        assert_eq!(seqs, (0..PER_CLIENT).collect::<Vec<_>>());
    }
    source.close().await.unwrap();
}

/// A throwaway PKI: a self-signed CA and a server certificate for `localhost`.
struct TestPki {
    ca_pem: String,
    server_pem: String,
    server_key_pem: String,
}

fn test_pki() -> TestPki {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let server_key = rcgen::KeyPair::generate().unwrap();
    let server_params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    let server = server_params.signed_by(&server_key, &issuer).unwrap();
    TestPki {
        ca_pem: ca.pem(),
        server_pem: server.pem(),
        server_key_pem: server_key.serialize_pem(),
    }
}

fn client_trusting(ca_pem: &str) -> reqwest::Client {
    use rustls_pki_types::CertificateDer;
    use rustls_pki_types::pem::PemObject;
    use tokio_rustls::rustls::crypto::ring;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let cert = CertificateDer::from_pem_slice(ca_pem.as_bytes()).expect("CA is valid PEM");
    let mut roots = RootCertStore::empty();
    roots.add(cert).expect("CA is a valid root");
    let tls = ClientConfig::builder_with_provider(std::sync::Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .no_proxy()
        .build()
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listen_serves_https_with_ring_tls_and_committed_204() {
    let pki = test_pki();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("server.pem");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, &pki.server_pem).unwrap();
    std::fs::write(&key_path, &pki.server_key_pem).unwrap();

    let (mut source, addr) = open(json!({
        "listen": {
            "bind": "127.0.0.1:0",
            "tls": { "cert": cert_path.to_str().unwrap(), "key": key_path.to_str().unwrap() }
        }
    }))
    .await;

    // A client that does not trust the CA cannot establish TLS at all.
    let untrusted = reqwest::Client::builder()
        .no_proxy()
        .use_preconfigured_tls({
            use tokio_rustls::rustls::crypto::ring;
            use tokio_rustls::rustls::{ClientConfig, RootCertStore};
            ClientConfig::builder_with_provider(std::sync::Arc::new(ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("TLS versions")
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth()
        })
        .build()
        .unwrap();
    let refused = untrusted
        .post(format!("https://localhost:{}/ingest", addr.port()))
        .body("x")
        .send()
        .await;
    assert!(refused.is_err(), "an untrusted CA must fail the handshake");

    let post = tokio::spawn({
        let client = client_trusting(&pki.ca_pem);
        async move {
            client
                .post(format!("https://localhost:{}/ingest", addr.port()))
                .body("over-tls")
                .send()
                .await
        }
    });

    let batch = next(&mut source, 4).await;
    assert_eq!(batch.batch.messages[0].payload.as_ref(), b"over-tls");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!post.is_finished(), "no 204 before the commit");
    batch.ack.complete(AckOutcome::Committed);
    assert_eq!(
        post.await.unwrap().unwrap().status(),
        StatusCode::NO_CONTENT
    );
    source.close().await.unwrap();
}
