//! The [`BatchSink`] that publishes JSON envelopes to Apache Kafka.
//!
//! `write` renders topic, key, and envelope per event, groups the batch by
//! (topic, partition), and sends one produce request per group with
//! `acks = -1` (fixed; the client hardcodes it) before it returns. At most
//! `max_in_flight` produce requests are unanswered at any moment. A failure
//! aborts the write with its classified error: the runtime retries a batch
//! whole, and because the client has no idempotent producer, a retried
//! batch can land twice — [`DeliveryClass::AtLeastOnce`], always.
//!
//! Partitioning follows the record key with the Java client's default
//! partitioner (murmur2, `& 0x7fffffff`, modulo the partition count), so
//! keyed interop with other producers routes the same way. Topic partition
//! counts are learned once from metadata (a topic's partition count changes
//! only when an operator re-partitions it).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use rskafka::BackoffConfig;
use rskafka::client::error::Error as ClientError;
use rskafka::client::partition::{
    Compression as WireCompression, PartitionClient, UnknownTopicHandling,
};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record;
use thadam_core::{
    BatchSink, DeliveryClass, ErrorDomain, Reject, SinkBatch, SinkCapabilities, SinkContext,
    ThadamError, WriteOutcome, codes,
};
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::config::{Compression, KafkaConfig, SaslMechanism, TlsConfig};
use crate::envelope::event_json;
use crate::error::{
    ProduceFailure, classify_connect, classify_produce, is_isolable, lookup_error, missing_topic,
    protocol_of,
};

/// The record header carrying the event id (hex) so consumers can dedupe
/// the runtime's retries. Same hex string as the envelope's `_event_id`.
pub const EVENT_ID_HEADER: &str = "x-thadam-event-id";

/// A batch sink publishing one dataset's events to Kafka topics.
pub struct KafkaSink {
    config: KafkaConfig,
    dataset: String,
    sink_id: String,
    client: Option<Client>,
    /// Partition counts learned from metadata, per fully-rendered topic.
    topics: HashMap<String, usize>,
    /// Partition clients created on demand.
    producers: HashMap<(String, i32), PartitionClient>,
}

/// How one group produce ended.
enum GroupOutcome {
    /// Every record of the group was acknowledged.
    Applied,
    /// The produce failed; the records are kept for an isolation retry.
    Failed {
        /// Where the group was produced.
        key: (String, i32),
        /// The events of the group, with their batch indices.
        records: Vec<(u32, Record)>,
        /// The client error (`None` is the in-house deadline: an unknown
        /// outcome the batch driver must retry).
        error: Option<ClientError>,
    },
}

impl KafkaSink {
    /// A sink for one dataset (`dataset` comes from the build context's
    /// selector, never from event data).
    pub fn new(config: KafkaConfig, dataset: String, sink_id: String) -> Self {
        KafkaSink {
            config,
            dataset,
            sink_id,
            client: None,
            topics: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    /// The dataset this sink renders topics for.
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    fn wire_compression(&self) -> WireCompression {
        match self.config.compression {
            Compression::None => WireCompression::NoCompression,
            Compression::Gzip => WireCompression::Gzip,
            Compression::Snappy => WireCompression::Snappy,
        }
    }

    async fn connect(&mut self) -> Result<(), ThadamError> {
        let mut builder = ClientBuilder::new(self.config.bootstrap.clone())
            .client_id(format!("thadam-sink-{}", self.sink_id))
            .backoff_config(BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: self.config.request_timeout.min(Duration::from_secs(1)),
                base: 3.0,
                // Internal retries (leader detection, reconnects) stop at the
                // configured timeout and surface the last error instead of
                // retrying forever inside one `write`.
                deadline: Some(self.config.request_timeout),
            });
        if let Some(tls) = &self.config.tls {
            builder = builder.tls_config(Arc::new(build_tls_config(tls).await?));
        }
        if let Some(sasl) = &self.config.sasl {
            let credentials =
                Credentials::new(sasl.user.clone(), sasl.password.reveal().to_owned());
            builder = builder.sasl_config(match sasl.mechanism {
                SaslMechanism::Plain => SaslConfig::Plain(credentials),
                SaslMechanism::ScramSha256 => SaslConfig::ScramSha256(credentials),
                SaslMechanism::ScramSha512 => SaslConfig::ScramSha512(credentials),
            });
        }
        // rskafka retries a failed connect for one `request_timeout` of
        // backoff, plus the time its attempts take (a SCRAM attempt is a
        // PBKDF2 round). The bound leaves room for that so the cause, a
        // certificate or SASL rejection, gets through; only a handshake that
        // never answers reaches it.
        let bound = self.config.request_timeout * 3;
        let client = match tokio::time::timeout(bound, builder.build()).await {
            Ok(Ok(client)) => client,
            Ok(Err(e)) => return Err(classify_connect(&e)),
            Err(_) => {
                return Err(ThadamError::transient(
                    ErrorDomain::Connector,
                    codes::CONNECTOR_TIMEOUT,
                    format!("connecting to the bootstrap brokers took longer than {bound:?}"),
                ));
            }
        };
        self.client = Some(client);
        Ok(())
    }

    /// The partition count of a fully-rendered topic, from the unfiltered
    /// metadata snapshot. No named lookup is ever made: rskafka always sends
    /// `allow_auto_topic_creation=true`, so asking by name would let a broker
    /// with auto-creation create the topic. A missing topic is a fatal config
    /// error instead.
    // shortcut: rskafka's own leader re-discovery is a named lookup with auto-creation on, so a topic deleted mid-run can be re-created by the broker, upgrade when rskafka exposes allow_auto_topic_creation
    async fn topic_partitions(&mut self, topic: &str) -> Result<usize, ThadamError> {
        if let Some(count) = self.topics.get(topic) {
            return Ok(*count);
        }
        let Some(client) = self.client.as_ref() else {
            return Err(not_open_error());
        };
        let topics = bounded(self.config.request_timeout, client.list_topics()).await?;
        let count = topics
            .iter()
            .find(|t| t.name == topic)
            .map(|t| t.partitions.len().max(1))
            .ok_or_else(|| missing_topic(topic))?;
        self.topics.insert(topic.to_owned(), count);
        Ok(count)
    }

    /// Ensures the partition client for (topic, partition) exists.
    async fn producer_for(&mut self, topic: &str, partition: i32) -> Result<(), ThadamError> {
        let key = (topic.to_owned(), partition);
        if self.producers.contains_key(&key) {
            return Ok(());
        }
        let Some(client) = self.client.as_ref() else {
            return Err(not_open_error());
        };
        let producer = bounded(
            self.config.request_timeout,
            client.partition_client(topic, partition, UnknownTopicHandling::Error),
        )
        .await?;
        self.producers.insert(key, producer);
        Ok(())
    }

    /// One produce request for a group, bounded by `request_timeout`.
    async fn produce_group(&self, key: (String, i32), records: Vec<(u32, Record)>) -> GroupOutcome {
        let Some(producer) = self.producers.get(&key) else {
            return GroupOutcome::Failed {
                key,
                records,
                error: Some(ClientError::InvalidResponse(
                    "internal error: the produce group has no partition client".to_owned(),
                )),
            };
        };
        // The produce frame consumes its records, so the wire copy is a
        // clone; the originals stay for a possible isolation retry.
        let wire: Vec<Record> = records.iter().map(|(_, record)| record.clone()).collect();
        let attempt = producer.produce(wire, self.wire_compression());
        match tokio::time::timeout(self.config.request_timeout, attempt).await {
            Ok(Ok(_)) => GroupOutcome::Applied,
            Ok(Err(error)) => GroupOutcome::Failed {
                key,
                records,
                error: Some(error),
            },
            Err(_) => GroupOutcome::Failed {
                key,
                records,
                error: None,
            },
        }
    }

    /// Topic, key, envelope, headers, and timestamp for one event.
    fn render_event(&self, event: &thadam_core::Event) -> Result<(String, Record), ThadamError> {
        let topic = self
            .config
            .topic
            .render(&self.dataset, event.asset())
            .map_err(|e| e.into_reject_error())?;
        let key = match &self.config.key {
            Some(template) => template
                .render(&self.dataset, &event.id(), event.asset())
                .map_err(|e| e.into_reject_error())?,
            None => event.id().to_string().into_bytes(),
        };
        let value = event_json(event).map_err(|e| e.into_reject_error())?;
        let mut headers = BTreeMap::new();
        headers.insert(
            EVENT_ID_HEADER.to_owned(),
            event.id().to_string().into_bytes(),
        );
        let record = Record {
            key: Some(key),
            value: Some(value),
            headers,
            timestamp: chrono::DateTime::from_timestamp_millis(
                event.times().ingest().as_nanos().div_euclid(1_000_000),
            )
            .unwrap_or_default(),
        };
        Ok((topic, record))
    }

    /// Re-drives a group's records one at a time to isolate the record the
    /// broker refused: per-event rejects for the culprits, applied for the
    /// rest. Any batch-level failure propagates (already-applied duplicates
    /// on the driver's retry are what `at_least_once` means).
    async fn isolate_failure(
        &self,
        key: (String, i32),
        records: Vec<(u32, Record)>,
        rejected: &mut Vec<Reject>,
    ) -> Result<(), ThadamError> {
        for (index, record) in records {
            match self.produce_group(key.clone(), vec![(index, record)]).await {
                GroupOutcome::Applied => {}
                GroupOutcome::Failed { error: None, .. } => {
                    return Err(elapsed_error(self.config.request_timeout));
                }
                GroupOutcome::Failed { error: Some(e), .. } => match classify_produce(&e) {
                    ProduceFailure::Batch(err) => return Err(err),
                    ProduceFailure::Event(err) => rejected.push(Reject::new(index, err)),
                },
            }
        }
        Ok(())
    }
}

fn not_open_error() -> ThadamError {
    ThadamError::permanent(
        ErrorDomain::Sink,
        codes::SINK_WRITE_FAILED,
        "the sink is not open; the driver must call open() first",
    )
}

/// A deadline the client cannot report itself (its futures are wrapped in
/// ours). rskafka does not bound its own connection handshakes, so a broker
/// that accepts connections but never answers would otherwise hang a write.
fn elapsed_error(timeout: Duration) -> ThadamError {
    ThadamError::transient(
        ErrorDomain::Sink,
        codes::SINK_TIMEOUT,
        format!("the broker did not answer within `request_timeout` ({timeout:?})"),
    )
}

/// Awaits one client call, bounded by `timeout`.
async fn bounded<T>(
    timeout: Duration,
    call: impl std::future::Future<Output = Result<T, ClientError>>,
) -> Result<T, ThadamError> {
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => result.map_err(|e| lookup_error(&e)),
        Err(_) => Err(elapsed_error(timeout)),
    }
}

/// Kafka's murmur2 — the Java client's `Utils.murmur2`, seed `0x9747b28c`
/// (the default partitioner's hash; vectors in the tests below come from
/// Kafka's own `UtilsTest.testMurmur2`).
fn murmur2(data: &[u8]) -> i32 {
    const SEED: i32 = 0x9747_b28c_u32 as i32;
    const M: i32 = 0x5bd1_e995_u32 as i32;
    let mut h: i32 = SEED ^ (data.len() as i32);
    let (blocks, rem) = data.as_chunks::<4>();
    for block in blocks {
        let mut k = i32::from_le_bytes(*block);
        k = k.wrapping_mul(M);
        k ^= (k as u32 >> 24) as i32;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if rem.len() == 3 {
        h ^= (rem[2] as i32) << 16;
    }
    if rem.len() >= 2 {
        h ^= (rem[1] as i32) << 8;
    }
    if !rem.is_empty() {
        h ^= rem[0] as i32;
        h = h.wrapping_mul(M);
    }
    h ^= (h as u32 >> 13) as i32;
    h = h.wrapping_mul(M);
    h ^ (h as u32 >> 15) as i32
}

/// Where the Java default partitioner would put this key.
fn partition_for(key: Option<&[u8]>, partitions: usize) -> i32 {
    let hash = key.map(murmur2).unwrap_or(0) & 0x7fff_ffff;
    (hash as usize % partitions) as i32
}

#[async_trait]
impl BatchSink for KafkaSink {
    async fn open(&mut self, _ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        // `open` is retried by the driver after transient failures; drop any
        // earlier half-probe before reconnecting.
        self.client = None;
        self.producers.clear();
        self.connect().await?;
        tracing::info!(
            sink = %self.sink_id,
            bootstrap = ?self.config.bootstrap,
            compression = ?self.config.compression,
            delivery = DeliveryClass::AtLeastOnce.as_str(),
            "kafka sink open"
        );
        Ok(SinkCapabilities::new(DeliveryClass::AtLeastOnce))
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        if self.client.is_none() {
            return Err(not_open_error());
        }

        // 1. Render and encode every event; per-event failures reject it.
        let mut rejected: Vec<Reject> = Vec::new();
        let mut groups: BTreeMap<(String, i32), Vec<(u32, Record)>> = BTreeMap::new();
        for (index, event) in batch.events().iter().enumerate() {
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            let rendered = self.render_event(event);
            let (topic, record) = match rendered {
                Ok(ok) => ok,
                Err(error) => {
                    rejected.push(Reject::new(index, error));
                    continue;
                }
            };
            if record.approximate_size() > self.config.max_message_bytes {
                rejected.push(Reject::new(
                    index,
                    ThadamError::permanent(
                        ErrorDomain::Sink,
                        codes::SINK_REJECTED,
                        format!(
                            "event message is {} bytes, over `max_message_bytes` ({})",
                            record.approximate_size(),
                            self.config.max_message_bytes
                        ),
                    ),
                ));
                continue;
            }
            // The metadata lookups can fail with a batch-level classified
            // error (including the missing-topic fatal): escape the loop.
            let partitions = self.topic_partitions(&topic).await?;
            let partition = partition_for(record.key.as_deref(), partitions);
            self.producer_for(&topic, partition).await?;
            groups
                .entry((topic, partition))
                .or_default()
                .push((index, record));
        }

        // 2. One produce request per group, at most `max_in_flight` airborne.
        let mut batches = groups.into_iter();
        let mut pending: FuturesUnordered<_> = FuturesUnordered::new();
        loop {
            while pending.len() < self.config.max_in_flight {
                match batches.next() {
                    Some((key, records)) => pending.push(self.produce_group(key, records)),
                    None => break,
                }
            }
            let Some(outcome) = pending.next().await else {
                break;
            };
            let GroupOutcome::Failed {
                key,
                records,
                error,
            } = outcome
            else {
                continue;
            };
            let Some(err) = error else {
                // The in-house deadline: an unknown outcome the driver must
                // retry (it may already be applied).
                return Err(elapsed_error(self.config.request_timeout));
            };
            match classify_produce(&err) {
                ProduceFailure::Batch(e) => return Err(e),
                ProduceFailure::Event(e) if records.len() == 1 => {
                    rejected.push(Reject::new(records[0].0, e));
                }
                ProduceFailure::Event(_) if protocol_of(&err).is_some_and(is_isolable) => {
                    self.isolate_failure(key, records, &mut rejected).await?;
                }
                // Not isolable (InvalidTopicException): every record of the
                // group shares one topic, so the group shares the verdict.
                ProduceFailure::Event(e) => {
                    for (index, _) in &records {
                        rejected.push(Reject::new(*index, e.clone()));
                    }
                }
            }
        }

        rejected.sort_by_key(|reject| reject.index);
        Ok(WriteOutcome::with_rejects(rejected))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.client = None;
        self.producers.clear();
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
/// mutual TLS (the thadam-mqtt semantics over PEM files, as thadam-nats).
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
    use thadam_core::{SinkBatch, codes};

    fn config(bootstrap: &str) -> KafkaConfig {
        crate::config::parse_config(&serde_json::json!({
            "bootstrap": [bootstrap],
            "topic": "telemetry.{dataset}"
        }))
        .expect("valid")
    }

    #[test]
    fn write_before_open_fails() {
        let mut sink = KafkaSink::new(config("127.0.0.1:9092"), "imu".into(), "s".into());
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
    fn murmur2_matches_the_java_client_vectors() {
        // apache/kafka `clients/.../UtilsTest.java`, testMurmur2.
        assert_eq!(murmur2(b"21"), -973932308);
        assert_eq!(murmur2(b"foobar"), -790332482);
        assert_eq!(murmur2(b"a-little-bit-long-string"), -985981536);
        assert_eq!(murmur2(b"a-little-bit-longer-string"), -1486304829);
        assert_eq!(
            murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"),
            -58897971
        );
        assert_eq!(murmur2(b"abc"), 479470107);
    }

    #[test]
    fn partition_selection_is_modulo_and_keyless_is_zero() {
        assert_eq!(partition_for(None, 7), 0);
        // An empty key hashes like any other key; only a missing key pins 0.
        let want = (murmur2(b"") & 0x7fff_ffff) as usize % 5;
        assert_eq!(partition_for(Some(b""), 5), want as i32);
        let p = partition_for(Some(b"key"), 3);
        assert!((0..3).contains(&p));
        // Matches Utils.toPositive(murmur2) % partitions for a known key.
        assert_eq!(
            partition_for(Some(b"21"), 12),
            (-973932308i32 & 0x7fff_ffff) % 12
        );
    }

    #[test]
    fn tls_config_loads_and_reports_bad_pem() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let ca_key = rcgen::KeyPair::generate().expect("key");
            let mut ca_params =
                rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = ca_params.self_signed(&ca_key).expect("ca");
            let path = dir.path().join("ca.pem");
            std::fs::write(&path, ca.pem()).expect("write");
            let tls = TlsConfig {
                ca_file: Some(path),
                ..TlsConfig::default()
            };
            build_tls_config(&tls).await.expect("ca loads");

            let bad = dir.path().join("bad.pem");
            std::fs::write(&bad, "not a certificate").expect("write");
            let err = build_tls_config(&TlsConfig {
                ca_file: Some(bad),
                ..TlsConfig::default()
            })
            .await
            .expect_err("garbage pem");
            assert_eq!(err.domain, ErrorDomain::Config, "{err}");
            assert!(!err.is_transient(), "{err}");
            assert_eq!(err.context.get("path"), Some("tls.ca_file"), "{err:?}");
        });
    }
}
