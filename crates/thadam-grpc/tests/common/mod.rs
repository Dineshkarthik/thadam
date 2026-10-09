#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use thadam_core::{BatchLimits, Shutdown, Source, SourceBatch, SourceContext, SourceId};
use thadam_grpc::proto::PublishRequest;
use thadam_grpc::proto::ingest_client::IngestClient;
use thadam_grpc::{GrpcConfig, GrpcSource};
use thadam_testkit::FakeClock;
use serde_json::{Value, json};
use tonic::transport::Channel;

pub async fn open(config: Value) -> (GrpcSource, SocketAddr) {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let config =
        GrpcConfig::from_value(&config).unwrap_or_else(|errors| panic!("config: {errors:?}"));
    let mut source = GrpcSource::new(config);
    let ctx = SourceContext::new(
        SourceId::new("grpc-test"),
        Shutdown::new(),
        FakeClock::default().shared(),
    );
    source.open(&ctx).await.expect("open");
    let addr = source.local_addr().expect("bound address");
    (source, addr)
}

pub fn limits(max_messages: usize) -> BatchLimits {
    BatchLimits {
        max_messages,
        linger: Duration::from_millis(5),
        ..BatchLimits::default()
    }
}

pub async fn next(source: &mut GrpcSource, max: usize) -> SourceBatch {
    tokio::time::timeout(Duration::from_secs(5), source.next_batch(limits(max)))
        .await
        .expect("next_batch timed out")
        .expect("next_batch failed")
        .expect("unexpected end of stream")
}

pub async fn client(addr: SocketAddr) -> IngestClient<Channel> {
    IngestClient::connect(format!("http://{addr}"))
        .await
        .expect("connect")
}

pub fn publish(stream: &str, payload: &[u8]) -> PublishRequest {
    PublishRequest {
        stream: stream.to_owned(),
        payload: payload.to_vec(),
        content_type: String::new(),
        attributes: HashMap::new(),
    }
}

pub fn config(extra: Value) -> Value {
    let mut base = json!({ "bind": "127.0.0.1:0" });
    if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            base.insert(k.clone(), v.clone());
        }
    }
    base
}

pub struct TestPki {
    pub ca_pem: String,
    pub server_pem: String,
    pub server_key_pem: String,
}

pub fn test_pki() -> TestPki {
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
