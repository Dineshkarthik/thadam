//! The [`BatchSink`] that publishes JSON envelopes to JetStream.
//!
//! Delivery is per event: one message per event, `Nats-Msg-Id` header set to
//! the event id (hex). `write` submits at most `max_in_flight` publishes
//! before awaiting the oldest acknowledgement, and returns only when every
//! event is acknowledged or individually rejected. An unknown outcome
//! (timeout, lost connection) is transient: the driver retries the same
//! batch and the stream's duplicate window deduplicates the resend.
//!
//! `open` connects and probes the named stream: `duplicate_window > 0`
//! upgrades the reported delivery class to
//! [`DeliveryClass::IdempotentBatch`]. A missing stream is created only when
//! `create_stream: true` and is otherwise a configuration error, matching
//! the rule that operators own provisioning.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::context::{Context, PublishAckFuture};
use async_nats::jetstream::{self};
use async_nats::{ConnectOptions, HeaderMap};
use async_trait::async_trait;
use bytes::Bytes;
use thadam_core::{
    BatchSink, DeliveryClass, ErrorDomain, Reject, SinkBatch, SinkCapabilities, SinkContext,
    ThadamError, WriteOutcome, codes,
};
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::config::{Auth, NatsConfig, TlsConfig};
use crate::envelope::event_json;
use crate::error::{
    PublishFailure, ServerDenials, classify_connect, classify_create_stream, classify_get_stream,
    classify_publish, stream_not_found,
};

/// The reported delivery class for the probed duplicate window: only a
/// positive window makes the runtime's retries idempotent server-side.
fn delivery_class(duplicate_window: &Duration) -> DeliveryClass {
    if duplicate_window.is_zero() {
        DeliveryClass::AtLeastOnce
    } else {
        DeliveryClass::IdempotentBatch
    }
}

/// A batch sink publishing one dataset's events to a JetStream stream.
pub struct NatsSink {
    config: NatsConfig,
    dataset: String,
    sink_id: String,
    connection: Option<async_nats::Client>,
    jetstream: Option<Context>,
    delivery: DeliveryClass,
    denials: ServerDenials,
}

impl NatsSink {
    /// A sink for one dataset (`dataset` comes from the build context's
    /// selector, never from event data).
    pub fn new(config: NatsConfig, dataset: String, sink_id: String) -> Self {
        NatsSink {
            config,
            dataset,
            sink_id,
            connection: None,
            jetstream: None,
            delivery: DeliveryClass::AtLeastOnce,
            denials: ServerDenials::default(),
        }
    }

    /// The dataset this sink renders subjects for.
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    async fn connect_options(&self) -> Result<ConnectOptions, ThadamError> {
        let denials = self.denials.clone();
        let mut options = ConnectOptions::new()
            .name(format!("thadam-sink-{}", self.sink_id))
            .request_timeout(Some(self.config.request_timeout))
            .event_callback(move |event| {
                denials.observe(&event);
                async {}
            });
        options = match &self.config.auth {
            Auth::None => options,
            Auth::Token(token) => options.token(token.reveal().to_owned()),
            Auth::UserPassword { user, password } => {
                options.user_and_password(user.clone(), password.reveal().to_owned())
            }
            Auth::CredsFile(path) => options.credentials_file(path).await.map_err(|e| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    format!("cannot load the credentials file {}: {e}", path.display()),
                )
                .with_context("path", "creds_file")
            })?,
        };
        if let Some(tls) = &self.config.tls {
            options = options.tls_client_config(build_tls_config(tls).await?);
        }
        if self.config.require_tls() {
            options = options.require_tls(true);
        }
        Ok(options)
    }

    async fn probe(&mut self) -> Result<(), ThadamError> {
        let options = self.connect_options().await?;
        let client = options
            .connect(&self.config.url)
            .await
            .map_err(|e| classify_connect(&e))?;
        let jetstream = jetstream::ContextBuilder::new()
            .timeout(self.config.request_timeout)
            .max_ack_inflight(self.config.max_in_flight)
            .backpressure_on_inflight(true)
            .build(client.clone());

        let duplicate_window = match jetstream.get_stream(&self.config.stream).await {
            Ok(stream) => stream.cached_info().config.duplicate_window,
            Err(e) if stream_not_found(&e) => {
                if !self.config.create_stream {
                    return Err(ThadamError::permanent(
                        ErrorDomain::Config,
                        codes::CONFIG_INVALID,
                        format!(
                            "stream `{}` does not exist; create it or set `create_stream: true`",
                            self.config.stream
                        ),
                    )
                    .with_context("path", "stream"));
                }
                let created = jetstream
                    .create_stream(jetstream::stream::Config {
                        name: self.config.stream.clone(),
                        subjects: vec![self.config.subject.wildcard()],
                        duplicate_window: self.config.duplicate_window,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| classify_create_stream(&e, &self.config.stream))?;
                created.cached_info().config.duplicate_window
            }
            Err(e) => return Err(classify_get_stream(&e, &self.config.stream)),
        };

        self.delivery = delivery_class(&duplicate_window);
        tracing::info!(
            sink = %self.sink_id,
            stream = %self.config.stream,
            duplicate_window_ms = duplicate_window.as_millis(),
            delivery = self.delivery.as_str(),
            "nats sink open"
        );
        self.connection = Some(client);
        self.jetstream = Some(jetstream);
        Ok(())
    }

    /// Publishes every event and waits for all acknowledgements. Failures
    /// are returned unescalated; [`BatchSink::write`] applies the server's
    /// announced denials.
    async fn publish_batch(&self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        let Some(jetstream) = self.jetstream.as_ref() else {
            return Err(ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_WRITE_FAILED,
                "the sink is not open; the driver must call open() first",
            ));
        };

        let mut rejected = Vec::new();
        let mut pending: VecDeque<(u32, PublishAckFuture)> = VecDeque::new();

        for (index, event) in batch.events().iter().enumerate() {
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            let subject = match self.config.subject.render(&self.dataset, event.asset()) {
                Ok(subject) => subject,
                Err(e) => {
                    rejected.push(Reject::new(index, e.into_reject_error()));
                    continue;
                }
            };
            let payload = match event_json(event) {
                Ok(payload) => payload,
                Err(e) => {
                    rejected.push(Reject::new(index, e.into_reject_error()));
                    continue;
                }
            };
            let mut headers = HeaderMap::new();
            headers.insert(
                async_nats::header::NATS_MESSAGE_ID,
                event.id().to_string().as_str(),
            );
            match jetstream
                .publish_with_headers(subject, headers, Bytes::from(payload))
                .await
            {
                Ok(future) => {
                    pending.push_back((index, future));
                    if pending.len() >= self.config.max_in_flight
                        && let Some((oldest, future)) = pending.pop_front()
                    {
                        Self::ack_one(oldest, future, &mut rejected).await?;
                    }
                }
                Err(e) => match classify_publish(&e) {
                    PublishFailure::Event(error) => rejected.push(Reject::new(index, error)),
                    PublishFailure::Batch(error) => return Err(error),
                },
            }
        }
        while let Some((oldest, future)) = pending.pop_front() {
            Self::ack_one(oldest, future, &mut rejected).await?;
        }
        rejected.sort_by_key(|reject| reject.index);
        Ok(WriteOutcome::with_rejects(rejected))
    }

    /// Awaits one acknowledgement; a batch-level failure short-circuits the
    /// write (remaining futures drop, their dedup-protected resend happens on
    /// the driver's retry).
    async fn ack_one(
        index: u32,
        future: PublishAckFuture,
        rejected: &mut Vec<Reject>,
    ) -> Result<(), ThadamError> {
        match future.await {
            Ok(ack) => {
                if ack.duplicate {
                    tracing::debug!(index, "server deduplicated a resent event");
                }
                Ok(())
            }
            Err(e) => match classify_publish(&e) {
                PublishFailure::Event(error) => {
                    rejected.push(Reject::new(index, error));
                    Ok(())
                }
                PublishFailure::Batch(error) => Err(error),
            },
        }
    }
}

#[async_trait]
impl BatchSink for NatsSink {
    async fn open(&mut self, _ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        if self.config.sends_credentials_unsecured() {
            tracing::warn!(
                sink = %self.sink_id,
                url = %self.config.url,
                "the NATS token or password is sent over plain nats:// without TLS; use tls:// or set tls outside local setups"
            );
        }
        // `open` is retried by the driver after transient failures; drop any
        // earlier half-probe before reconnecting.
        self.connection = None;
        self.jetstream = None;
        self.denials = ServerDenials::default();
        self.probe()
            .await
            .map_err(|error| self.denials.escalate(error))?;
        Ok(SinkCapabilities::new(self.delivery))
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        self.publish_batch(batch)
            .await
            .map_err(|error| self.denials.escalate(error))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.jetstream = None;
        if let Some(client) = self.connection.take() {
            // A flush against a broker that is down would otherwise wait for
            // the reconnect loop; shutdown must not hang on it.
            match tokio::time::timeout(self.config.request_timeout, client.flush()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::debug!(sink = %self.sink_id, error = %e, "final flush failed");
                }
                Err(_) => tracing::debug!(sink = %self.sink_id, "final flush timed out"),
            }
        }
        Ok(())
    }
}

fn config_read_error(key: &'static str, path: &Path, detail: String) -> ThadamError {
    ThadamError::permanent(
        ErrorDomain::Config,
        codes::CONFIG_INVALID,
        format!("cannot read `{}` {}: {detail}", key, path.display()),
    )
    .with_context("path", key)
}

/// Builds the rustls client configuration for `tls` with the `ring`
/// provider: a CA file replaces the platform roots, a cert/key pair enables
/// mutual TLS (the thadam-mqtt semantics over PEM files).
async fn build_tls_config(tls: &TlsConfig) -> Result<ClientConfig, ThadamError> {
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};

    let mut roots = RootCertStore::empty();
    match &tls.ca_file {
        Some(path) => {
            let pem = tokio::fs::read(path)
                .await
                .map_err(|e| config_read_error("tls.ca_file", path, e.to_string()))?;
            let mut added = 0usize;
            for cert in CertificateDer::pem_slice_iter(&pem) {
                let cert = cert.map_err(|e| {
                    ThadamError::permanent(
                        ErrorDomain::Config,
                        codes::CONFIG_INVALID,
                        format!("`tls.ca_file` is not valid PEM: {e}"),
                    )
                    .with_context("path", "tls.ca_file")
                })?;
                let _ = roots.add(cert).map_err(|e| {
                    tracing::warn!(error = %e, "skipping an unusable CA certificate");
                });
                added += 1;
            }
            if added == 0 {
                return Err(ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "`tls.ca_file` contains no certificates",
                )
                .with_context("path", "tls.ca_file"));
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                return Err(ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "no platform trust roots could be loaded; set `tls.ca_file`",
                )
                .with_context("path", "tls.ca_file"));
            }
        }
    }

    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                format!("TLS protocol versions: {e}"),
            )
        })?
        .with_root_certificates(roots);

    match (&tls.client_cert, &tls.client_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = tokio::fs::read(cert_path)
                .await
                .map_err(|e| config_read_error("tls.client_cert", cert_path, e.to_string()))?;
            let key_pem = tokio::fs::read(key_path)
                .await
                .map_err(|e| config_read_error("tls.client_key", key_path, e.to_string()))?;
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    ThadamError::permanent(
                        ErrorDomain::Config,
                        codes::CONFIG_INVALID,
                        format!("`tls.client_cert` is not valid PEM: {e}"),
                    )
                    .with_context("path", "tls.client_cert")
                })?;
            if chain.is_empty() {
                return Err(ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "`tls.client_cert` contains no certificates",
                )
                .with_context("path", "tls.client_cert"));
            }
            let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    format!("`tls.client_key` holds no usable private key: {e}"),
                )
                .with_context("path", "tls.client_key")
            })?;
            builder.with_client_auth_cert(chain, key).map_err(|e| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    format!("client certificate and key do not match: {e}"),
                )
                .with_context("path", "tls.client_cert")
            })
        }
        _ => Ok(builder.with_no_client_auth()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> NatsConfig {
        crate::config::parse_config(&serde_json::json!({
            "url": url,
            "subject": "thadam.{dataset}",
            "stream": "THADAM"
        }))
        .expect("valid")
    }

    #[test]
    fn write_before_open_fails() {
        let mut sink = NatsSink::new(config("nats://127.0.0.1:4222"), "imu".into(), "s".into());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let origin = thadam_core::WalRange::new(
                thadam_core::WalEpoch::new(1),
                thadam_core::WalPos::new(0),
                thadam_core::WalPos::new(0),
            )
            .expect("range");
            let batch = SinkBatch::new(thadam_core::EventBatch::new(Vec::new(), origin, true));
            let err = sink.write(&batch).await.expect_err("not open");
            assert_eq!(err.code, codes::SINK_WRITE_FAILED);
            assert!(!err.is_transient());
        });
    }

    #[test]
    fn delivery_class_follows_the_probed_window() {
        use std::time::Duration;
        assert_eq!(delivery_class(&Duration::ZERO), DeliveryClass::AtLeastOnce);
        assert_eq!(
            delivery_class(&Duration::from_millis(1)),
            DeliveryClass::IdempotentBatch
        );
    }

    fn pem_files() -> (tempfile::TempDir, TlsConfig) {
        let ca_key = rcgen::KeyPair::generate().expect("key");
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).expect("ca");
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let client_key = rcgen::KeyPair::generate().expect("key");
        let client = rcgen::CertificateParams::new(vec!["client".to_owned()])
            .expect("params")
            .signed_by(&client_key, &issuer)
            .expect("client cert");

        let dir = tempfile::tempdir().expect("tempdir");
        let write = |name: &str, pem: String| {
            let path = dir.path().join(name);
            std::fs::write(&path, pem).expect("write");
            path
        };
        let tls = TlsConfig {
            ca_file: Some(write("ca.pem", ca.pem())),
            client_cert: Some(write("client.pem", client.pem())),
            client_key: Some(write("client.key", client_key.serialize_pem())),
        };
        (dir, tls)
    }

    #[tokio::test]
    async fn tls_config_loads_ca_and_client_identity() {
        let (_dir, tls) = pem_files();
        build_tls_config(&tls)
            .await
            .expect("CA plus client identity");

        let ca_only = TlsConfig {
            ca_file: tls.ca_file.clone(),
            ..TlsConfig::default()
        };
        build_tls_config(&ca_only).await.expect("CA only");
    }

    #[tokio::test]
    async fn tls_config_failures_are_config_errors_naming_the_key() {
        let (dir, good) = pem_files();
        let garbage = dir.path().join("garbage.pem");
        std::fs::write(&garbage, "not a certificate").expect("write");

        let cases = [
            (
                "tls.ca_file",
                TlsConfig {
                    ca_file: Some(dir.path().join("missing.pem")),
                    ..TlsConfig::default()
                },
            ),
            (
                "tls.ca_file",
                TlsConfig {
                    ca_file: Some(garbage.clone()),
                    ..TlsConfig::default()
                },
            ),
            (
                "tls.client_cert",
                TlsConfig {
                    client_cert: Some(garbage.clone()),
                    ..good.clone()
                },
            ),
            (
                "tls.client_key",
                TlsConfig {
                    client_key: Some(garbage.clone()),
                    ..good.clone()
                },
            ),
        ];
        for (key, tls) in cases {
            let err = build_tls_config(&tls).await.expect_err(key);
            assert_eq!(err.domain, ErrorDomain::Config, "{key}: {err}");
            assert!(!err.is_transient(), "{key}: {err}");
            assert_eq!(err.context.get("path"), Some(key), "{err:?}");
        }
    }

    #[tokio::test]
    async fn connect_options_reflect_auth_and_tls_requirement() {
        let sink = NatsSink::new(config("nats://127.0.0.1:4222"), "imu".into(), "s".into());
        let options = sink.connect_options().await.expect("options");
        let shown = format!("{options:?}");
        assert!(shown.contains("thadam-sink-s"), "{shown}");

        let mut with_tls = config("tls://127.0.0.1:4222");
        assert!(with_tls.require_tls());
        with_tls.tls = Some(TlsConfig::default());
        assert!(with_tls.require_tls());
    }
}
