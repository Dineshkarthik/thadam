//! TLS through rustls with the `ring` provider, terminated in this crate.
//!
//! Every TLS feature of `rumqttc` 0.25.1 (including `use-rustls-no-provider`)
//! has a hard dependency on `rustls-webpki ^0.102.8`, a range that cannot
//! reach the patched 0.103.x line (RUSTSEC-2026-0049, -0098, -0099, -0104). The
//! client library is therefore built without any TLS feature and speaks plain
//! MQTT over a private Unix socket to a [`TlsRelay`]. The relay dials the broker
//! with `tokio-rustls` and the `ring` provider (no `aws-lc-rs`, no OpenSSL;
//! ADR-0020, `deny.toml`) and copies bytes both ways. The socket lives in a
//! `0700` directory only this process can enter.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::task::{AbortHandle, JoinSet};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};

use crate::config::TlsConfig;
use crate::error::MqttError;

const SOCKET_NAME: &str = "broker.sock";

/// Why the relay could not reach the broker. The client library only sees the
/// socket close, so the relay keeps the reason for the poll task to report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelayFailure {
    pub is_tls: bool,
    pub detail: String,
}

#[derive(Default)]
struct FailureSlot(Mutex<Option<RelayFailure>>);

impl FailureSlot {
    fn record(&self, failure: RelayFailure) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(failure);
    }

    fn take(&self) -> Option<RelayFailure> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

struct Dialer {
    host: String,
    port: u16,
    server_name: ServerName<'static>,
    connector: TlsConnector,
    budget: Duration,
    failures: FailureSlot,
}

impl Dialer {
    async fn dial(&self) -> Result<tokio_rustls::client::TlsStream<TcpStream>, RelayFailure> {
        let network = |detail: String| RelayFailure {
            is_tls: false,
            detail,
        };
        let tcp = tokio::time::timeout(
            self.budget,
            TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .map_err(|_| {
            network(format!(
                "connecting to {}:{} timed out",
                self.host, self.port
            ))
        })?
        .map_err(|e| network(format!("{}:{}: {e}", self.host, self.port)))?;
        tcp.set_nodelay(true).map_err(|e| network(e.to_string()))?;
        tokio::time::timeout(
            self.budget,
            self.connector.connect(self.server_name.clone(), tcp),
        )
        .await
        .map_err(|_| RelayFailure {
            is_tls: true,
            detail: "the TLS handshake timed out".into(),
        })?
        .map_err(|e| RelayFailure {
            is_tls: true,
            detail: e.to_string(),
        })
    }

    async fn serve(&self, mut local: UnixStream) {
        match self.dial().await {
            Ok(mut remote) => {
                let _ = copy_bidirectional(&mut local, &mut remote).await;
            }
            Err(failure) => self.failures.record(failure),
        }
    }
}

/// An in-process TLS terminator for one source. Dropping it stops the relay
/// and removes the socket.
pub(crate) struct TlsRelay {
    socket: PathBuf,
    dialer: Arc<Dialer>,
    accept: AbortHandle,
    _dir: tempfile::TempDir,
}

impl TlsRelay {
    /// Binds the relay socket and starts accepting. Must run inside a Tokio
    /// runtime.
    pub fn start(
        config: Arc<ClientConfig>,
        host: &str,
        port: u16,
        connect_timeout: Duration,
    ) -> Result<TlsRelay, MqttError> {
        let server_name = ServerName::try_from(host.to_owned())
            .map_err(|e| MqttError::Tls(format!("`{host}` is not a valid server name: {e}")))?;
        let dir = tempfile::Builder::new()
            .prefix("thadam-mqtt-")
            .tempdir()
            .map_err(|e| MqttError::Tls(format!("cannot create the relay directory: {e}")))?;
        let socket = dir.path().join(SOCKET_NAME);
        let listener = UnixListener::bind(&socket).map_err(|e| {
            MqttError::Tls(format!(
                "cannot bind the relay socket {}: {e}",
                socket.display()
            ))
        })?;
        let dialer = Arc::new(Dialer {
            host: host.to_owned(),
            port,
            server_name,
            connector: TlsConnector::from(config),
            // Shorter than the client's own connect timeout, so the reason for
            // a failed dial is recorded before the client gives up.
            budget: connect_timeout.mul_f32(0.8),
            failures: FailureSlot::default(),
        });
        let accept = tokio::spawn(accept_loop(listener, dialer.clone())).abort_handle();
        Ok(TlsRelay {
            socket,
            dialer,
            accept,
            _dir: dir,
        })
    }

    /// The path the client library connects to.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// The reason the last dial failed, if it did since the last call.
    pub fn take_failure(&self) -> Option<RelayFailure> {
        self.dialer.failures.take()
    }
}

impl Drop for TlsRelay {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn accept_loop(listener: UnixListener, dialer: Arc<Dialer>) {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let dialer = dialer.clone();
                    sessions.spawn(async move { dialer.serve(stream).await });
                }
                Err(e) => {
                    dialer.failures.record(RelayFailure {
                        is_tls: false,
                        detail: format!("relay accept failed: {e}"),
                    });
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
}
/// Reads the PEM files named in `tls` and builds the client configuration.
pub(crate) async fn build_client_config(tls: &TlsConfig) -> Result<Arc<ClientConfig>, MqttError> {
    let ca = match &tls.ca {
        Some(path) => Some(read(path, "tls.ca").await?),
        None => None,
    };
    let identity = match (&tls.cert, &tls.key) {
        (Some(cert), Some(key)) => {
            Some((read(cert, "tls.cert").await?, read(key, "tls.key").await?))
        }
        _ => None,
    };
    client_config_from_pem(
        ca.as_deref(),
        identity.as_ref().map(|(c, k)| (c.as_slice(), k.as_slice())),
    )
}

async fn read(path: &std::path::Path, what: &'static str) -> Result<Vec<u8>, MqttError> {
    tokio::fs::read(path)
        .await
        .map_err(|e| MqttError::Credentials {
            what,
            detail: format!("{}: {e}", path.display()),
        })
}

/// Builds a client configuration from PEM bytes: `ca` replaces the platform
/// trust roots, `identity` is the client certificate chain and its key.
pub(crate) fn client_config_from_pem(
    ca: Option<&[u8]>,
    identity: Option<(&[u8], &[u8])>,
) -> Result<Arc<ClientConfig>, MqttError> {
    let mut roots = RootCertStore::empty();
    match ca {
        Some(pem) => {
            let mut added = 0usize;
            for cert in CertificateDer::pem_slice_iter(pem) {
                let cert =
                    cert.map_err(|e| MqttError::Tls(format!("tls.ca is not valid PEM: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| MqttError::Tls(format!("tls.ca holds a bad certificate: {e}")))?;
                added += 1;
            }
            if added == 0 {
                return Err(MqttError::Tls("tls.ca contains no certificates".into()));
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                return Err(MqttError::Tls(
                    "no platform trust roots could be loaded; set tls.ca".into(),
                ));
            }
        }
    }

    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| MqttError::Tls(format!("protocol versions: {e}")))?
        .with_root_certificates(roots);

    let config = match identity {
        Some((cert_pem, key_pem)) => {
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
                .collect::<Result<_, _>>()
                .map_err(|e| MqttError::Tls(format!("tls.cert is not valid PEM: {e}")))?;
            if chain.is_empty() {
                return Err(MqttError::Tls("tls.cert contains no certificates".into()));
            }
            let key = PrivateKeyDer::from_pem_slice(key_pem)
                .map_err(|e| MqttError::Tls(format!("tls.key holds no usable private key: {e}")))?;
            builder.with_client_auth_cert(chain, key).map_err(|e| {
                MqttError::Tls(format!("client certificate and key do not match: {e}"))
            })?
        }
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pem_pair() -> (String, String, String) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf_params = rcgen::CertificateParams::new(vec!["thadam-edge".to_owned()]).unwrap();
        let leaf = leaf_params.signed_by(&leaf_key, &issuer).unwrap();
        (ca.pem(), leaf.pem(), leaf_key.serialize_pem())
    }

    #[test]
    fn builds_server_auth_and_mutual_auth_configs_with_ring() {
        let (ca, cert, key) = pem_pair();
        let server_only = client_config_from_pem(Some(ca.as_bytes()), None).unwrap();
        assert!(!server_only.client_auth_cert_resolver.has_certs());

        let mutual =
            client_config_from_pem(Some(ca.as_bytes()), Some((cert.as_bytes(), key.as_bytes())))
                .unwrap();
        assert!(mutual.client_auth_cert_resolver.has_certs());
        assert!(!mutual.crypto_provider().cipher_suites.is_empty());
    }

    #[test]
    fn rejects_empty_and_invalid_material_with_a_clear_message() {
        let (ca, cert, key) = pem_pair();
        let err = client_config_from_pem(Some(b"not pem at all"), None).unwrap_err();
        assert!(err.to_string().contains("tls.ca"), "{err}");

        let err =
            client_config_from_pem(Some(ca.as_bytes()), Some((b"", key.as_bytes()))).unwrap_err();
        assert!(err.to_string().contains("tls.cert"), "{err}");

        let err =
            client_config_from_pem(Some(ca.as_bytes()), Some((cert.as_bytes(), b""))).unwrap_err();
        assert!(err.to_string().contains("tls.key"), "{err}");
    }

    #[test]
    fn a_mismatched_key_is_reported() {
        let (ca, cert, _) = pem_pair();
        let other = rcgen::KeyPair::generate().unwrap().serialize_pem();
        let err = client_config_from_pem(
            Some(ca.as_bytes()),
            Some((cert.as_bytes(), other.as_bytes())),
        )
        .unwrap_err();
        assert!(err.to_string().contains("do not match"), "{err}");
    }

    struct Pki {
        ca_pem: String,
        server_chain: Vec<CertificateDer<'static>>,
        server_key: PrivateKeyDer<'static>,
    }

    fn pki() -> Pki {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
            .unwrap()
            .signed_by(&leaf_key, &issuer)
            .unwrap();
        Pki {
            ca_pem: ca.pem(),
            server_chain: vec![leaf.der().clone()],
            server_key: PrivateKeyDer::try_from(leaf_key.serialize_der()).unwrap(),
        }
    }

    /// A TLS server that echoes every byte back. Returns its port.
    async fn echo_server(pki: Pki) -> u16 {
        let config =
            rustls::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(pki.server_chain, pki.server_key)
                .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut tls) = acceptor.accept(tcp).await {
                        let (mut r, mut w) = tokio::io::split(&mut tls);
                        let _ = tokio::io::copy(&mut r, &mut w).await;
                    }
                });
            }
        });
        port
    }

    async fn through_relay(relay: &TlsRelay, bytes: &[u8]) -> std::io::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut socket = UnixStream::connect(relay.socket()).await?;
        socket.write_all(bytes).await?;
        let mut echoed = vec![0u8; bytes.len()];
        socket.read_exact(&mut echoed).await?;
        Ok(echoed)
    }

    #[tokio::test]
    async fn the_relay_carries_bytes_both_ways_over_verified_tls() {
        let pki = pki();
        let config = client_config_from_pem(Some(pki.ca_pem.as_bytes()), None).unwrap();
        let port = echo_server(pki).await;
        let relay = TlsRelay::start(config, "localhost", port, Duration::from_secs(5)).unwrap();

        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let echoed = through_relay(&relay, &payload).await.unwrap();
        assert_eq!(echoed, payload);
        assert_eq!(relay.take_failure(), None);

        let socket = relay.socket().to_owned();
        drop(relay);
        assert!(
            UnixStream::connect(&socket).await.is_err(),
            "dropping the relay removes its socket"
        );
    }

    #[tokio::test]
    async fn an_untrusted_broker_certificate_is_a_recorded_tls_failure() {
        let broker_pki = pki();
        let other = pki();
        let config = client_config_from_pem(Some(other.ca_pem.as_bytes()), None).unwrap();
        let port = echo_server(broker_pki).await;
        let relay = TlsRelay::start(config, "localhost", port, Duration::from_secs(5)).unwrap();

        assert!(through_relay(&relay, b"hello").await.is_err());
        let failure = relay.take_failure().expect("the relay records why");
        assert!(failure.is_tls, "{failure:?}");
        assert!(relay.take_failure().is_none(), "the slot is taken once");
    }

    #[tokio::test]
    async fn an_unreachable_broker_is_a_recorded_network_failure() {
        let pki = pki();
        let config = client_config_from_pem(Some(pki.ca_pem.as_bytes()), None).unwrap();
        let closed_port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().port()
        };
        let relay =
            TlsRelay::start(config, "localhost", closed_port, Duration::from_secs(5)).unwrap();

        assert!(through_relay(&relay, b"hello").await.is_err());
        let failure = relay.take_failure().expect("the relay records why");
        assert!(!failure.is_tls, "{failure:?}");
    }

    #[test]
    fn an_invalid_server_name_is_rejected_up_front() {
        let pki = pki();
        let config = client_config_from_pem(Some(pki.ca_pem.as_bytes()), None).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _guard = rt.enter();
        let err = TlsRelay::start(config, "not a host", 8883, Duration::from_secs(1))
            .err()
            .expect("a name with spaces is not a server name");
        assert!(err.to_string().contains("server name"), "{err}");
    }

    #[tokio::test]
    async fn missing_files_name_the_config_key() {
        let tls = TlsConfig {
            ca: Some("/nonexistent/ca.pem".into()),
            ..TlsConfig::default()
        };
        let err = build_client_config(&tls).await.unwrap_err();
        assert!(
            matches!(err, MqttError::Credentials { what: "tls.ca", .. }),
            "{err}"
        );
    }
}
