//! TLS over a real tonic client: ring-backed rustls on both sides.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{config, next, open, publish, test_pki};
use serde_json::json;
use thadam_core::{AckOutcome, Source};
use thadam_grpc::proto::ingest_client::IngestClient;
use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

async fn tls_client(
    addr: std::net::SocketAddr,
    ca_pem: &str,
) -> Result<IngestClient<Channel>, tonic::transport::Error> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .domain_name("localhost");
    let channel = Endpoint::from_shared(format!("https://127.0.0.1:{}", addr.port()))
        .expect("endpoint")
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(IngestClient::new(channel))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_succeeds_over_tls_when_the_ca_is_trusted() {
    let pki = test_pki();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("server.pem");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, &pki.server_pem).unwrap();
    std::fs::write(&key_path, &pki.server_key_pem).unwrap();

    let (mut source, addr) = open(config(json!({
        "tls": { "cert": cert_path, "key": key_path }
    })))
    .await;

    let mut client = tls_client(addr, &pki.ca_pem)
        .await
        .expect("trusted TLS connects");
    let call = tokio::spawn(async move {
        client
            .publish(Request::new(publish("s", b"over-tls")))
            .await
    });

    let batch = next(&mut source, 4).await;
    assert_eq!(batch.batch.messages[0].payload.as_ref(), b"over-tls");
    batch.ack.complete(AckOutcome::Committed);
    call.await.unwrap().expect("publish over TLS succeeds");
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untrusted_ca_fails_the_handshake() {
    let pki = test_pki();
    let other = test_pki();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("server.pem");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, &pki.server_pem).unwrap();
    std::fs::write(&key_path, &pki.server_key_pem).unwrap();

    let (mut source, addr) = open(config(json!({
        "tls": { "cert": cert_path, "key": key_path }
    })))
    .await;

    assert!(
        tls_client(addr, &other.ca_pem).await.is_err(),
        "a server certificate from an untrusted CA must fail the handshake"
    );
    source.close().await.unwrap();
}
