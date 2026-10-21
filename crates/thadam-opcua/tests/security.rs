//! End-to-end secure-channel and certificate tests against the in-process
//! server: the two preferred policies with `sign_and_encrypt` and `sign`,
//! certificate user authentication, the fatal rejections (no retry storm),
//! and server-certificate trust.
//!
//! RSA handshakes in an unoptimized build take seconds, so every case here
//! gets a wide budget.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Value, json};
use support::{
    ClientFiles, CountingProxy, Material, PASSWORD, TestNodes, TestServer, USERNAME, base_config,
    bind, collect_until, limits, open_source, payload, start_on, start_server,
};
use thadam_core::{Shutdown, Source, ThadamError, codes};
use tokio::sync::Mutex;

const ENCRYPTED_T: Duration = Duration::from_secs(60);

/// A subscribe-mode config over the secured endpoint `policy`/`mode`, with
/// `trust` as the explicit server-certificate trust list.
fn secured_config(
    server: &TestServer,
    nodes: &TestNodes,
    policy: &str,
    mode: &str,
    client: &ClientFiles,
    trust: &[PathBuf],
) -> Value {
    let mut cfg = base_config(server, nodes);
    cfg["security_policy"] = json!(policy);
    cfg["security_mode"] = json!(mode);
    cfg["cert"] = client.cert_block(trust);
    cfg
}

/// Starts a server behind a counting proxy, advertising the proxy's port,
/// so the client's connection attempts are visible to the test.
async fn server_behind_proxy() -> (TestServer, TestNodes, CountingProxy) {
    let listener = bind(0).await;
    let target = listener.local_addr().expect("server addr");
    let proxy = CountingProxy::start(target).await;
    let (server, nodes) = start_on(listener, proxy.port).await;
    (server, nodes, proxy)
}

/// Opens `cfg` and returns the first publish response's payload.
async fn first_publish(cfg: Value) -> Value {
    let shutdown = Shutdown::new();
    let source = Mutex::new(open_source(cfg, &shutdown).await);
    let observed =
        collect_until(&source, ENCRYPTED_T, |m| payload(m)["running"].is_boolean()).await;
    assert!(
        !observed.is_empty(),
        "no publish responses over the channel"
    );
    shutdown.trigger();
    source.lock().await.close().await.expect("close");
    payload(&observed[0])
}

/// Drives `source` until it fails and returns that error.
async fn next_error(source: &mut Box<dyn Source>) -> ThadamError {
    tokio::time::timeout(ENCRYPTED_T, async {
        loop {
            match source.next_batch(limits()).await {
                Ok(Some(batch)) => {
                    panic!(
                        "unexpected batch of {} messages",
                        batch.batch.messages.len()
                    )
                }
                Ok(None) => {}
                Err(error) => return error,
            }
        }
    })
    .await
    .expect("an error within the timeout")
}

#[tokio::test]
async fn aes256_sha256_rsapss_sign_and_encrypt_delivers_publish_responses() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let cfg = secured_config(
        &server,
        &nodes,
        "aes256_sha256_rsapss",
        "sign_and_encrypt",
        &client,
        std::slice::from_ref(&material.server_cert),
    );
    let first = first_publish(cfg).await;
    assert_eq!(first["label"], json!("pump"));
    server.stop().await;
}

#[tokio::test]
async fn aes128_sha256_rsaoaep_sign_and_encrypt_delivers_publish_responses() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let cfg = secured_config(
        &server,
        &nodes,
        "aes128_sha256_rsaoaep",
        "sign_and_encrypt",
        &client,
        std::slice::from_ref(&material.server_cert),
    );
    let first = first_publish(cfg).await;
    assert_eq!(first["label"], json!("pump"));
    server.stop().await;
}

#[tokio::test]
async fn aes128_sha256_rsaoaep_sign_delivers_publish_responses() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let cfg = secured_config(
        &server,
        &nodes,
        "aes128_sha256_rsaoaep",
        "sign",
        &client,
        std::slice::from_ref(&material.server_cert),
    );
    let first = first_publish(cfg).await;
    assert_eq!(first["running"], json!(true));
    server.stop().await;
}

#[tokio::test]
async fn basic256sha256_sign_and_encrypt_with_username_auth() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let mut cfg = secured_config(
        &server,
        &nodes,
        "basic256sha256",
        "sign_and_encrypt",
        &client,
        std::slice::from_ref(&material.server_cert),
    );
    cfg["auth"] = json!({"kind": "username", "username": USERNAME, "password": PASSWORD});
    let first = first_publish(cfg).await;
    assert_eq!(first["label"], json!("pump"));
    server.stop().await;
}

#[tokio::test]
async fn certificate_user_auth_succeeds_over_aes256() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let mut cfg = secured_config(
        &server,
        &nodes,
        "aes256_sha256_rsapss",
        "sign_and_encrypt",
        &client,
        std::slice::from_ref(&material.server_cert),
    );
    cfg["auth"] = json!({
        "kind": "certificate",
        "cert": material.user_cert,
        "key": material.user_key,
    });
    let first = first_publish(cfg).await;
    assert_eq!(first["label"], json!("pump"));
    server.stop().await;
}

#[tokio::test]
async fn a_rejected_user_certificate_is_fatal_and_not_retried() {
    let material = Material::shared();
    let (server, nodes, proxy) = server_behind_proxy().await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let mut cfg = secured_config(
        &server,
        &nodes,
        "aes256_sha256_rsapss",
        "sign_and_encrypt",
        &client,
        std::slice::from_ref(&material.server_cert),
    );
    // The client's channel certificate is trusted; the user token is not.
    cfg["auth"] = json!({
        "kind": "certificate",
        "cert": material.rogue_cert,
        "key": material.rogue_key,
    });

    let shutdown = Shutdown::new();
    let mut source = open_source(cfg, &shutdown).await;
    let error = next_error(&mut source).await;
    assert_eq!(error.code, codes::AUTH_FAILED, "{error}");

    let after_error = proxy.accepted();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        proxy.accepted(),
        after_error,
        "a fatal rejection must not start more connection attempts"
    );
    source.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn an_untrusted_client_certificate_is_fatal_and_not_retried() {
    let material = Material::shared();
    let (server, nodes, proxy) = server_behind_proxy().await;
    let rogue = ClientFiles::new(&material.rogue_cert, &material.rogue_key);
    let cfg = secured_config(
        &server,
        &nodes,
        "aes256_sha256_rsapss",
        "sign_and_encrypt",
        &rogue,
        std::slice::from_ref(&material.server_cert),
    );

    let shutdown = Shutdown::new();
    let mut source = open_source(cfg, &shutdown).await;
    let error = next_error(&mut source).await;
    assert_eq!(error.code, codes::AUTH_FAILED, "{error}");

    let after_error = proxy.accepted();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        proxy.accepted(),
        after_error,
        "a fatal rejection must not start more connection attempts"
    );
    source.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn an_unknown_server_certificate_is_rejected_by_default() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let cfg = secured_config(
        &server,
        &nodes,
        "aes256_sha256_rsapss",
        "sign_and_encrypt",
        &client,
        &[],
    );

    let shutdown = Shutdown::new();
    let mut source = open_source(cfg, &shutdown).await;
    let error = next_error(&mut source).await;
    assert_eq!(error.code, codes::AUTH_FAILED, "{error}");

    let trusted = client.pki_dir.join("trusted");
    let rejected = client.pki_dir.join("rejected");
    assert_eq!(
        std::fs::read_dir(&trusted).map(|d| d.count()).unwrap_or(0),
        0,
        "nothing trusted by default"
    );
    assert_eq!(
        std::fs::read_dir(&rejected).expect("rejected dir").count(),
        1,
        "the refused server certificate is kept for review"
    );
    source.close().await.expect("close");
    server.stop().await;
}

#[tokio::test]
async fn accept_unknown_server_cert_is_an_explicit_opt_in() {
    let material = Material::shared();
    let (server, nodes) = start_server(0).await;
    let client = ClientFiles::new(&material.client_cert, &material.client_key);
    let mut cfg = secured_config(
        &server,
        &nodes,
        "aes256_sha256_rsapss",
        "sign_and_encrypt",
        &client,
        &[],
    );
    cfg["cert"]["accept_unknown_server_cert"] = json!(true);
    let first = first_publish(cfg).await;
    assert_eq!(first["label"], json!("pump"));
    assert_eq!(
        std::fs::read_dir(client.pki_dir.join("trusted"))
            .expect("trusted dir")
            .count(),
        1,
        "the accepted server certificate is stored as trusted"
    );
    server.stop().await;
}
