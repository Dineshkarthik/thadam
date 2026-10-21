//! Docker tests for the secured listeners: TLS with a CA file, mutual TLS, and
//! SASL over TLS (PLAIN and SCRAM) with the StandardAuthorizer. Each test
//! starts its own broker, the same way the plaintext tests do.

use std::collections::BTreeSet;

use serde_json::{Value, json};
use tempfile::TempDir;
use thadam_core::{ErrorDomain, Retry, codes};

use super::{TOPIC, want_ids};
use crate::common::certs::Pki;
use crate::common::*;

/// The `tls` block of a sink config that trusts the test CA.
fn ca_tls(dir: &TempDir) -> Value {
    json!({"ca_file": dir.path().join("ca.pem")})
}

async fn tls_broker(tag: &'static str, pki: &Pki, client_auth: bool) -> Broker {
    Broker::start(Opts {
        listener: Listener::Tls {
            pki: pki.clone(),
            client_auth,
        },
        ..Opts::new(tag)
    })
    .await
}

async fn sasl_broker(tag: &'static str, pki: &Pki) -> Broker {
    let broker = Broker::start(Opts {
        listener: Listener::SaslTls { pki: pki.clone() },
        ..Opts::new(tag)
    })
    .await;
    broker.add_scram_user("alice", "alice-secret");
    broker.add_scram_user("bob", "bob-secret");
    broker
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn tls_publishes_and_consumes_back_with_the_ca_file() {
    init_tracing();
    let pki = Pki::generate();
    let broker = tls_broker("tls", &pki, false).await;
    let client = broker.client().await;
    create_topic(&client, TOPIC, 3).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    let mut sink = build_sink(&broker, json!({"tls": ca_tls(&dir)}));
    sink.open(&sink_context(&dir)).await.expect("open over TLS");
    let outcome = sink.write(&make_batch(0, 5, "r7")).await.expect("write");
    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);

    let stored = read_all(&client, TOPIC).await;
    assert_eq!(stored.len(), 5, "one record per event");
    let ids: BTreeSet<String> = stored.iter().map(key_of).collect();
    assert_eq!(ids, want_ids(5), "every event stored");
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn tls_untrusted_ca_is_a_fatal_config_error() {
    init_tracing();
    let pki = Pki::generate();
    let broker = tls_broker("untrusted", &pki, false).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    let mut sink = build_sink(
        &broker,
        json!({"tls": {"ca_file": dir.path().join("other-ca.pem")}}),
    );
    let e = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("the broker's certificate is not signed by the configured CA");
    assert_eq!(
        (e.domain, e.retry),
        (ErrorDomain::Config, Retry::Fatal),
        "{e}"
    );
    assert!(e.message.contains("tls.ca_file"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn mtls_client_certificate_publishes() {
    init_tracing();
    let pki = Pki::generate();
    let broker = tls_broker("mtls", &pki, true).await;
    let client = broker.client().await;
    create_topic(&client, TOPIC, 1).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    let mut sink = build_sink(
        &broker,
        json!({"tls": {
            "ca_file": dir.path().join("ca.pem"),
            "client_cert": dir.path().join("client.pem"),
            "client_key": dir.path().join("client.key"),
        }}),
    );
    sink.open(&sink_context(&dir))
        .await
        .expect("open with a client certificate");
    let outcome = sink.write(&make_batch(0, 3, "r7")).await.expect("write");
    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
    assert_eq!(read_all(&client, TOPIC).await.len(), 3);
    sink.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn mtls_missing_client_certificate_is_a_fatal_auth_error() {
    init_tracing();
    let pki = Pki::generate();
    let broker = tls_broker("nocert", &pki, true).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    let mut sink = build_sink(&broker, json!({"tls": ca_tls(&dir)}));
    let e = sink
        .open(&sink_context(&dir))
        .await
        .expect_err("the broker requires a client certificate");
    assert_eq!(
        (e.domain, e.retry),
        (ErrorDomain::Auth, Retry::Fatal),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn sasl_plain_and_scram_publish_over_tls() {
    init_tracing();
    let pki = Pki::generate();
    let broker = sasl_broker("sasl-ok", &pki).await;
    broker.allow("alice", &["Describe", "Write"], TOPIC);
    let client = broker.client().await;
    create_topic(&client, TOPIC, 1).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    for (i, mechanism) in ["plain", "scram-sha-256", "scram-sha-512"]
        .into_iter()
        .enumerate()
    {
        let mut sink = build_sink(
            &broker,
            json!({
                "tls": ca_tls(&dir),
                "sasl": {"mechanism": mechanism, "user": "alice", "password": "alice-secret"},
            }),
        );
        sink.open(&sink_context(&dir))
            .await
            .unwrap_or_else(|e| panic!("{mechanism}: open: {e}"));
        let batch = make_batch(2 * i as u64, 2, "r7");
        let outcome = sink
            .write(&batch)
            .await
            .unwrap_or_else(|e| panic!("{mechanism}: write: {e}"));
        assert!(
            outcome.rejected.is_empty(),
            "{mechanism}: {:?}",
            outcome.rejected
        );
        sink.close().await.expect("close");
    }

    let stored = read_all(&client, TOPIC).await;
    let ids: BTreeSet<String> = stored.iter().map(key_of).collect();
    assert_eq!(stored.len(), 6, "one record per event, per mechanism");
    assert_eq!(ids, want_ids(6), "every event stored");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn sasl_wrong_password_is_a_fatal_auth_error() {
    init_tracing();
    let pki = Pki::generate();
    let broker = sasl_broker("sasl-badpw", &pki).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    for mechanism in ["plain", "scram-sha-256", "scram-sha-512"] {
        let mut sink = build_sink(
            &broker,
            json!({
                "tls": ca_tls(&dir),
                "sasl": {"mechanism": mechanism, "user": "alice", "password": "not-the-password"},
            }),
        );
        let e = sink
            .open(&sink_context(&dir))
            .await
            .expect_err("the password is wrong");
        assert_eq!(
            (e.domain, e.code, e.retry),
            (ErrorDomain::Auth, codes::AUTH_FAILED, Retry::Fatal),
            "{mechanism}: {e}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn sasl_principal_without_write_is_a_fatal_authorization_error() {
    init_tracing();
    let pki = Pki::generate();
    let broker = sasl_broker("sasl-authz", &pki).await;
    broker.allow("alice", &["Describe", "Write"], TOPIC);
    broker.allow("bob", &["Describe"], TOPIC);
    let client = broker.client().await;
    create_topic(&client, TOPIC, 1).await;

    let dir = TempDir::new().expect("tempdir");
    pki.write_files(dir.path());
    let sasl = |user: &str, password: &str| {
        json!({"tls": ca_tls(&dir), "sasl": {
            "mechanism": "scram-sha-512", "user": user, "password": password,
        }})
    };

    // Bob authenticates but may only describe the topic: the produce is denied.
    let mut bob = build_sink(&broker, sasl("bob", "bob-secret"));
    bob.open(&sink_context(&dir))
        .await
        .expect("bob authenticates");
    let e = bob
        .write(&make_batch(0, 2, "r7"))
        .await
        .expect_err("bob has no WRITE on the topic");
    assert_eq!(
        (e.domain, e.code, e.retry),
        (ErrorDomain::Auth, codes::AUTH_FORBIDDEN, Retry::Fatal),
        "{e}"
    );
    assert!(read_all(&client, TOPIC).await.is_empty(), "nothing stored");

    // Alice, who holds WRITE, publishes through the same broker.
    let mut alice = build_sink(&broker, sasl("alice", "alice-secret"));
    alice
        .open(&sink_context(&dir))
        .await
        .expect("alice authenticates");
    let outcome = alice
        .write(&make_batch(0, 2, "r7"))
        .await
        .expect("alice writes");
    assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
    assert_eq!(read_all(&client, TOPIC).await.len(), 2);
    alice.close().await.expect("close");
    bob.close().await.expect("close");
}
