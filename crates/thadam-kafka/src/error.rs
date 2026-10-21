//! Classification of rskafka failures into the shared error model
//! (ARCHITECTURE.md section 13):
//!
//! - **Transient** — timeouts, leader changes, dropped connections, broker
//!   or quota backpressure, and unknown error codes: the batch driver
//!   retries the same batch. (Kafka has no server-side dedupe for a
//!   non-idempotent producer, so a retried batch may land twice — the
//!   reported `at_least_once`.)
//! - **Fatal** — SASL authentication, authorization denials, a certificate
//!   the sink does not trust or a client certificate the broker refuses, a
//!   topic that does not exist (the sink never creates topics), and
//!   capability/config mismatches: retrying cannot help; the component is
//!   marked `Failed` for an operator fix.
//! - **Per-event permanent** — an illegal topic name that survived client
//!   validation, a record over the broker's size limit, or a record the
//!   broker refuses on its own merits: the event is rejected into
//!   quarantine and the rest of the batch proceeds.

use rskafka::client::error::{Error, ProtocolError, RequestContext, RequestError};
use rskafka::{BackoffError, ConnectionError, SaslError};
use thadam_core::{ErrorDomain, ThadamError, codes};

/// What a failed produce request means for the batch the events belong to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProduceFailure {
    /// The whole write fails; the retry class is baked into the error.
    Batch(ThadamError),
    /// A single event fails, permanently; it goes to quarantine. The batch
    /// driver bisects these when a whole produce report carries one.
    Event(ThadamError),
}

fn transient(code: &'static str, message: impl Into<String>) -> ThadamError {
    ThadamError::transient(ErrorDomain::Sink, code, message)
}

fn fatal_auth(code: &'static str, message: impl Into<String>) -> ThadamError {
    ThadamError::fatal(ErrorDomain::Auth, code, message)
}

fn fatal_config(message: impl Into<String>) -> ThadamError {
    ThadamError::fatal(ErrorDomain::Config, codes::CONFIG_INVALID, message)
}

fn reject(message: impl Into<String>) -> ProduceFailure {
    ProduceFailure::Event(ThadamError::permanent(
        ErrorDomain::Sink,
        codes::SINK_REJECTED,
        message,
    ))
}

/// The topic a server error names, for messages and for the missing-topic
/// check.
fn error_topic(request: &RequestContext) -> Option<&str> {
    match request {
        RequestContext::Topic(topic) => Some(topic),
        RequestContext::Partition(topic, _) => Some(topic),
        _ => None,
    }
}

/// Whether `p` is a broker-side auth failure.
fn forbidden(p: &ProtocolError) -> bool {
    matches!(
        p,
        ProtocolError::TopicAuthorizationFailed
            | ProtocolError::ClusterAuthorizationFailed
            | ProtocolError::GroupAuthorizationFailed
            | ProtocolError::SaslAuthenticationFailed
    )
}

/// Whether `p` means the topic does not exist.
fn unknown_topic(p: &ProtocolError) -> bool {
    matches!(p, ProtocolError::UnknownTopicOrPartition)
}

/// A topic that is missing or was deleted. Fatal config: the sink never
/// creates topics (rskafka asks brokers for auto-creation on every named
/// lookup, so the sink avoids named lookups for topics it has not seen in the
/// metadata snapshot), and retrying cannot make the operator's topic appear.
pub(crate) fn missing_topic(topic: &str) -> ThadamError {
    fatal_config(format!(
        "topic `{topic}` is missing or was deleted; the sink never creates topics, so restore or create it, then restart the sink"
    ))
}

/// The verdicts rskafka 0.6.0 reports only as text. Its connect and
/// reconnect loops retry every failure until their deadline, then return
/// `RetryFailed`, which flattens the broker's answer into a message. A
/// rejected certificate or SASL login cannot be fixed by retrying, so the
/// message decides it.
// shortcut: matches rskafka 0.6.0 message text, pinned by the docker_kafka secure tests; switch to typed `ConnectionError` matches once rskafka stops flattening its retries
fn retry_verdict(err: &Error) -> Option<ThadamError> {
    if !matches!(
        err,
        Error::RetryFailed(_) | Error::Connection(ConnectionError::RetryFailed(_))
    ) {
        return None;
    }
    let text = err.to_string();
    if text.contains("invalid peer certificate") {
        return Some(ThadamError::fatal(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            "the broker's certificate is not trusted: check `tls.ca_file` and that the certificate names the bootstrap host",
        ));
    }
    if ["CertificateRequired", "BadCertificate", "UnknownCA"]
        .iter()
        .any(|alert| text.contains(&format!("received fatal alert: {alert}")))
    {
        return Some(fatal_auth(
            codes::AUTH_FAILED,
            "the broker rejected the TLS client certificate: check `tls.client_cert` and `tls.client_key`, and that the broker trusts their CA",
        ));
    }
    if text.contains("Sasl handshake failed: unsupported sasl mechanism")
        || text.contains("Sasl handshake failed: API error: UnsupportedSaslMechanism")
    {
        return Some(fatal_config(
            "the broker does not offer the configured `sasl.mechanism`",
        ));
    }
    if text.contains("Sasl handshake failed: API error: SaslAuthenticationFailed")
        || text.contains("Sasl handshake failed: Sasl session error")
    {
        return Some(fatal_auth(
            codes::AUTH_FAILED,
            "the broker rejected the SASL credentials",
        ));
    }
    None
}

/// The batch-level verdict for a client error outside a produce request
/// (metadata and partition lookups never reject a single event).
pub(crate) fn lookup_error(err: &Error) -> ThadamError {
    match classify_produce(err) {
        ProduceFailure::Batch(e) | ProduceFailure::Event(e) => e,
    }
}

/// Whether an isolation retry (one record per produce request) can split a
/// multi-record broker refusal into per-event rejects.
pub(crate) fn is_isolable(p: &ProtocolError) -> bool {
    matches!(
        p,
        ProtocolError::MessageTooLarge
            | ProtocolError::RecordListTooLarge
            | ProtocolError::InvalidRecord
    )
}

/// Classifies a broker protocol error from a produce path.
fn protocol(p: &ProtocolError, err: &Error) -> ProduceFailure {
    use ProtocolError as P;
    if forbidden(p) {
        return ProduceFailure::Batch(fatal_auth(
            codes::AUTH_FORBIDDEN,
            format!("the broker denied the produce: {err}"),
        ));
    }
    let Some(request) = server_request(err) else {
        return ProduceFailure::Batch(transient(
            codes::SINK_WRITE_FAILED,
            format!("produce failed: {err}"),
        ));
    };
    match p {
        _ if unknown_topic(p) => {
            ProduceFailure::Batch(missing_topic(error_topic(request).unwrap_or("<unknown>")))
        }
        P::InvalidTopicException => reject(format!(
            "the rendered topic name is not a legal Kafka topic: {err}"
        )),
        P::MessageTooLarge | P::RecordListTooLarge | P::InvalidRecord => {
            reject(format!("the broker refused the record: {err}"))
        }
        P::PolicyViolation => reject(format!(
            "the broker's record policy refused the event: {err}"
        )),
        P::RequestTimedOut => ProduceFailure::Batch(transient(
            codes::SINK_TIMEOUT,
            format!("produce timed out waiting for the leader: {err}"),
        )),
        P::NotLeaderOrFollower
        | P::LeaderNotAvailable
        | P::FencedLeaderEpoch
        | P::UnknownLeaderEpoch
        | P::EligibleLeadersNotAvailable
        | P::PreferredLeaderNotAvailable
        | P::BrokerNotAvailable
        | P::ReplicaNotAvailable
        | P::OffsetNotAvailable
        | P::StaleControllerEpoch
        | P::StaleBrokerEpoch
        | P::NetworkException
        | P::CoordinatorLoadInProgress
        | P::CoordinatorNotAvailable
        | P::NotCoordinator
        | P::NotController
        | P::NotEnoughReplicas
        | P::NotEnoughReplicasAfterAppend
        | P::ReassignmentInProgress
        | P::LogDirNotFound
        | P::KafkaStorageError
        | P::ElectionNotNeeded
        | P::UnknownServerError
        | P::ThrottlingQuotaExceeded => ProduceFailure::Batch(transient(
            codes::SINK_UNAVAILABLE,
            format!("the broker cannot apply the produce right now: {err}"),
        )),
        P::UnsupportedCompressionType => ProduceFailure::Batch(fatal_config(format!(
            "the broker refuses the configured `compression` codec: {err}"
        ))),
        P::UnsupportedForMessageFormat => ProduceFailure::Batch(fatal_config(format!(
            "the topic's log message format predates v2 (record headers need v2): {err}"
        ))),
        _ => ProduceFailure::Batch(transient(
            codes::SINK_WRITE_FAILED,
            format!("produce failed with an unclassified broker error: {err}"),
        )),
    }
}

/// The request context of a server error, also through an exhausted retry
/// (its last error is exactly that server error).
fn server_request(err: &Error) -> Option<&RequestContext> {
    match err {
        Error::ServerError { request, .. } => Some(request),
        Error::RetryFailed(BackoffError::DeadlineExceded { source, .. }) => {
            source.downcast_ref::<Error>().and_then(server_request)
        }
        _ => None,
    }
}

/// The protocol error anywhere an exhausted retry could hide it (also used
/// by the sink's isolation-retry decision).
pub(crate) fn protocol_of(err: &Error) -> Option<&ProtocolError> {
    match err {
        Error::ServerError { protocol_error, .. } => Some(protocol_error),
        Error::RetryFailed(BackoffError::DeadlineExceded { source, .. }) => source
            .downcast_ref::<Error>()
            .and_then(|inner| match inner {
                Error::ServerError { protocol_error, .. } => Some(protocol_error),
                _ => None,
            }),
        _ => None,
    }
}

/// Classifies a SASL failure (connect- or reconnect-time).
fn classify_sasl(sasl: &SaslError, err: &Error) -> ThadamError {
    match sasl {
        SaslError::ApiError(ProtocolError::SaslAuthenticationFailed)
        | SaslError::SaslSessionError(_) => fatal_auth(
            codes::AUTH_FAILED,
            format!("the broker rejected the SASL credentials: {err}"),
        ),
        SaslError::ApiError(ProtocolError::UnsupportedSaslMechanism)
        | SaslError::UnsupportedSaslMechanism
        | SaslError::InvalidSaslMechanism(_)
        | SaslError::InvalidConfig(_) => fatal_config(format!(
            "the configured `sasl.mechanism` does not work against this broker: {err}"
        )),
        SaslError::RequestError(_) | SaslError::Callback(_) | SaslError::ApiError(_) => transient(
            codes::CONNECTOR_CONNECT_FAILED,
            format!("SASL negotiation with the broker failed: {err}"),
        ),
    }
}

/// Classifies any client error of the produce/metadata paths, never panics.
pub(crate) fn classify_produce(err: &Error) -> ProduceFailure {
    if let Some(verdict) = retry_verdict(err) {
        return ProduceFailure::Batch(verdict);
    }
    if let Error::ServerError { .. } = err {
        return protocol(
            &protocol_of(err)
                .copied()
                .unwrap_or(ProtocolError::Unknown(-1)),
            err,
        );
    }
    if let Error::RetryFailed(_) = err {
        if let Some(p) = protocol_of(err) {
            return protocol(p, err);
        }
        return ProduceFailure::Batch(transient(
            codes::SINK_TIMEOUT,
            format!("the request did not succeed within `request_timeout`: {err}"),
        ));
    }
    match err {
        Error::Connection(ConnectionError::SaslFailed(sasl)) => {
            ProduceFailure::Batch(classify_sasl(sasl, err))
        }
        Error::Connection(_) => ProduceFailure::Batch(transient(
            codes::CONNECTOR_DISCONNECTED,
            format!("the connection to the broker dropped: {err}"),
        )),
        Error::Request(RequestError::IO(_) | RequestError::Poisoned(_)) => {
            ProduceFailure::Batch(transient(
                codes::CONNECTOR_DISCONNECTED,
                format!("the broker connection failed mid-request: {err}"),
            ))
        }
        Error::Timeout => ProduceFailure::Batch(transient(
            codes::SINK_TIMEOUT,
            format!("the produce request timed out: {err}"),
        )),
        Error::InvalidResponse(_) | Error::Request(_) => ProduceFailure::Batch(transient(
            codes::SINK_WRITE_FAILED,
            format!("the broker's reply was not usable: {err}"),
        )),
        // ServerError and RetryFailed returned above; anything added to the
        // client's error enum later defaults to a retryable batch failure.
        _ => ProduceFailure::Batch(transient(
            codes::SINK_WRITE_FAILED,
            format!("produce failed: {err}"),
        )),
    }
}

/// Classifies a client-build failure at `open`.
pub(crate) fn classify_connect(err: &Error) -> ThadamError {
    if let Some(verdict) = retry_verdict(err) {
        return verdict;
    }
    match err {
        Error::Connection(ConnectionError::SaslFailed(sasl)) => classify_sasl(sasl, err),
        Error::Timeout => ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_TIMEOUT,
            format!("connecting to the bootstrap brokers timed out: {err}"),
        ),
        Error::Connection(_) | Error::Request(_) | Error::RetryFailed(_) => ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_CONNECT_FAILED,
            format!("could not reach a bootstrap broker: {err}"),
        ),
        Error::InvalidResponse(_) | Error::ServerError { .. } => ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_PROTOCOL,
            format!("the bootstrap broker's reply was not usable: {err}"),
        ),
        // Anything added to the client's error enum later: retryable.
        _ => ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_CONNECT_FAILED,
            format!("could not connect to a bootstrap broker: {err}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::Retry;

    fn server_error(p: ProtocolError) -> Error {
        Error::ServerError {
            protocol_error: p,
            error_message: None,
            request: RequestContext::Partition("telemetry.imu.r7".to_owned(), 0),
            response: None,
            is_virtual: false,
        }
    }

    fn unwrap_batch(result: ProduceFailure) -> ThadamError {
        let ProduceFailure::Batch(e) = result else {
            panic!("expected a batch failure");
        };
        e
    }

    fn unwrap_event(result: ProduceFailure) -> ThadamError {
        let ProduceFailure::Event(e) = result else {
            panic!("expected a per-event failure");
        };
        e
    }

    #[test]
    fn retriable_broker_errors_are_transient() {
        for p in [
            ProtocolError::NotLeaderOrFollower,
            ProtocolError::LeaderNotAvailable,
            ProtocolError::FencedLeaderEpoch,
            ProtocolError::NetworkException,
            ProtocolError::NotEnoughReplicas,
            ProtocolError::NotEnoughReplicasAfterAppend,
            ProtocolError::ThrottlingQuotaExceeded,
            ProtocolError::UnknownServerError,
            ProtocolError::KafkaStorageError,
        ] {
            let e = unwrap_batch(classify_produce(&server_error(p)));
            assert_eq!(e.code, codes::SINK_UNAVAILABLE, "{p}");
            assert!(e.is_transient(), "{p}");
        }
        let e = unwrap_batch(classify_produce(&server_error(
            ProtocolError::RequestTimedOut,
        )));
        assert_eq!(e.code, codes::SINK_TIMEOUT);
        assert!(e.is_transient());
    }

    #[test]
    fn authorization_failures_are_fatal() {
        for p in [
            ProtocolError::TopicAuthorizationFailed,
            ProtocolError::ClusterAuthorizationFailed,
            ProtocolError::SaslAuthenticationFailed,
        ] {
            let e = unwrap_batch(classify_produce(&server_error(p)));
            assert_eq!(
                (e.domain, e.code, e.retry),
                (ErrorDomain::Auth, codes::AUTH_FORBIDDEN, Retry::Fatal),
                "{p}"
            );
        }
    }

    #[test]
    fn a_missing_topic_is_a_fatal_config_error_naming_it() {
        let e = unwrap_batch(classify_produce(&server_error(
            ProtocolError::UnknownTopicOrPartition,
        )));
        assert_eq!(
            (e.domain, e.retry),
            (ErrorDomain::Config, Retry::Fatal),
            "{e}"
        );
        assert!(e.message.contains("telemetry.imu.r7"), "{e}");
        assert!(e.message.contains("never creates topics"), "{e}");
        assert!(e.message.contains("was deleted"), "{e}");

        let e = missing_topic("telemetry.imu.r9");
        assert_eq!((e.domain, e.retry), (ErrorDomain::Config, Retry::Fatal));
        assert!(e.message.contains("telemetry.imu.r9"), "{e}");
    }

    fn retry_failed(cause: &str) -> Error {
        Error::Connection(ConnectionError::RetryFailed(
            BackoffError::DeadlineExceded {
                deadline: std::time::Duration::from_secs(1),
                source: cause.into(),
            },
        ))
    }

    #[test]
    fn retried_rejections_are_fatal_with_their_domain() {
        // rskafka 0.6.0's texts. The first three were seen from the broker in
        // the docker_kafka secure tests; the session error follows its format.
        let cases = [
            (
                "error connecting to broker \"127.0.0.1:9094\": IO Error: invalid peer certificate: UnknownIssuer",
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
            ),
            (
                "cannot sync versions: Request error: Connection is poisoned: Cannot read data: received fatal alert: CertificateRequired",
                ErrorDomain::Auth,
                codes::AUTH_FAILED,
            ),
            (
                "Sasl handshake failed: API error: SaslAuthenticationFailed",
                ErrorDomain::Auth,
                codes::AUTH_FAILED,
            ),
            (
                "Sasl handshake failed: Sasl session error: invalid proof",
                ErrorDomain::Auth,
                codes::AUTH_FAILED,
            ),
            (
                "Sasl handshake failed: unsupported sasl mechanism",
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
            ),
        ];
        for (cause, domain, code) in cases {
            let e = classify_connect(&retry_failed(cause));
            assert_eq!(
                (e.domain, e.code, e.retry),
                (domain, code, Retry::Fatal),
                "{cause}"
            );
            let e = unwrap_batch(classify_produce(&retry_failed(cause)));
            assert_eq!(
                (e.domain, e.code, e.retry),
                (domain, code, Retry::Fatal),
                "{cause}"
            );
        }
    }

    #[test]
    fn a_sasl_io_failure_mid_handshake_stays_transient() {
        let e = classify_connect(&retry_failed(
            "Sasl handshake failed: Request error: IO Error: connection reset",
        ));
        assert!(e.is_transient(), "{e}");
    }

    #[test]
    fn per_event_permanent_refusals() {
        for p in [
            ProtocolError::MessageTooLarge,
            ProtocolError::RecordListTooLarge,
            ProtocolError::InvalidRecord,
        ] {
            let e = unwrap_event(classify_produce(&server_error(p)));
            assert_eq!(
                (e.code, e.retry),
                (codes::SINK_REJECTED, Retry::Permanent),
                "{p}"
            );
            assert!(is_isolable(&p), "{p} is isolation-retryable");
        }
        let e = unwrap_event(classify_produce(&server_error(
            ProtocolError::InvalidTopicException,
        )));
        assert_eq!((e.code, e.retry), (codes::SINK_REJECTED, Retry::Permanent),);
        assert!(e.message.contains("legal Kafka topic"), "{e}");
    }

    #[test]
    fn codec_and_format_mismatches_are_fatal_config() {
        for p in [
            ProtocolError::UnsupportedCompressionType,
            ProtocolError::UnsupportedForMessageFormat,
        ] {
            let e = unwrap_batch(classify_produce(&server_error(p)));
            assert_eq!(
                (e.domain, e.retry),
                (ErrorDomain::Config, Retry::Fatal),
                "{p}"
            );
        }
    }

    #[test]
    fn unknown_codes_default_to_transient() {
        let e = unwrap_batch(classify_produce(&server_error(ProtocolError::Unknown(
            5000,
        ))));
        assert_eq!(e.code, codes::SINK_WRITE_FAILED);
        assert!(e.is_transient());
    }

    #[test]
    fn transport_failures_and_timeouts_are_transient() {
        let e = classify_connect(&Error::Timeout);
        assert_eq!(e.code, codes::CONNECTOR_TIMEOUT);
        assert!(e.is_transient());

        let e = unwrap_batch(classify_produce(&Error::Timeout));
        assert_eq!(e.code, codes::SINK_TIMEOUT);

        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        let e = unwrap_batch(classify_produce(&Error::Request(RequestError::IO(io))));
        assert_eq!(e.code, codes::CONNECTOR_DISCONNECTED);
        assert!(e.is_transient());
    }

    #[test]
    fn an_exhausted_retry_surfaces_the_last_protocol_error() {
        let deadline = BackoffError::DeadlineExceded {
            deadline: std::time::Duration::from_secs(1),
            source: Box::new(server_error(ProtocolError::UnknownTopicOrPartition)),
        };
        let e = unwrap_batch(classify_produce(&Error::RetryFailed(deadline)));
        assert_eq!(
            (e.domain, e.retry),
            (ErrorDomain::Config, Retry::Fatal),
            "{e}"
        );

        let deadline = BackoffError::DeadlineExceded {
            deadline: std::time::Duration::from_secs(1),
            source: Box::new(server_error(ProtocolError::NotLeaderOrFollower)),
        };
        let e = unwrap_batch(classify_produce(&Error::RetryFailed(deadline)));
        assert!(e.is_transient(), "{e}");
    }

    #[test]
    fn sasl_classification() {
        let err = Error::Connection(ConnectionError::SaslFailed(SaslError::ApiError(
            ProtocolError::SaslAuthenticationFailed,
        )));
        let e = classify_connect(&err);
        assert_eq!(
            (e.domain, e.code, e.retry),
            (ErrorDomain::Auth, codes::AUTH_FAILED, Retry::Fatal)
        );

        let err = Error::Connection(ConnectionError::SaslFailed(SaslError::ApiError(
            ProtocolError::UnsupportedSaslMechanism,
        )));
        let e = classify_connect(&err);
        assert_eq!(
            (e.domain, e.retry),
            (ErrorDomain::Config, Retry::Fatal),
            "{e}"
        );

        let io = std::io::Error::new(std::io::ErrorKind::TimedOut, "t");
        let err = Error::Connection(ConnectionError::SaslFailed(SaslError::RequestError(
            RequestError::IO(io),
        )));
        let e = classify_connect(&err);
        assert!(e.is_transient(), "{e}");
    }
}
