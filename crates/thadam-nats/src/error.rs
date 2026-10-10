//! Classification of `async-nats` failures into the shared error model
//! (ARCHITECTURE.md section 13):
//!
//! - **Transient** — timeouts, no responders, a lost connection, DNS/IO/TLS
//!   connect failures, and unknown server replies: the batch driver retries
//!   with the same batch id and the `Nats-Msg-Id` dedup absorbs resends.
//! - **Fatal** — rejected credentials and permission violations: retrying
//!   cannot help and the component is marked `Failed`.
//! - **Per-event permanent** — a payload exceeds the server's `max_payload`
//!   (or a bound stream's message limit): the event is rejected into
//!   quarantine and the rest of the batch proceeds.

use std::sync::{Arc, Mutex, PoisonError};

use async_nats::jetstream::context::{
    CreateStreamError, CreateStreamErrorKind, GetStreamError, GetStreamErrorKind, PublishError,
    PublishErrorKind,
};
use async_nats::{ConnectError, ConnectErrorKind, Event, ServerError};
use thadam_core::{ErrorDomain, ThadamError, codes};

/// Remembers the first authorization or permission denial the server
/// announced on an established connection.
///
/// A denied publish or request is not answered: the server sends an
/// asynchronous `-ERR 'Permissions Violation ...'` and the client only
/// observes an acknowledgement timeout. Without this record that timeout would
/// be classified transient and retried forever; with it, [`Self::escalate`]
/// turns the timeout into the fatal error it really is.
#[derive(Clone, Debug, Default)]
pub(crate) struct ServerDenials(Arc<Mutex<Option<String>>>);

/// The denial text of a server `-ERR`, if it is a credential or permission
/// denial.
fn denial_text(error: &ServerError) -> Option<String> {
    match error {
        ServerError::AuthorizationViolation => Some(error.to_string()),
        ServerError::Other(text) => {
            let lower = text.to_ascii_lowercase();
            [
                "permissions violation",
                "authentication expired",
                "authentication revoked",
            ]
            .iter()
            .any(|needle| lower.contains(needle))
            .then(|| text.clone())
        }
        _ => None,
    }
}

impl ServerDenials {
    /// Feeds a connection event; keeps the first denial.
    pub(crate) fn observe(&self, event: &Event) {
        if let Event::ServerError(error) = event
            && let Some(text) = denial_text(error)
        {
            let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            slot.get_or_insert(text);
        }
    }

    /// The first recorded denial, if any.
    pub(crate) fn recorded(&self) -> Option<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Upgrades a transient failure to fatal when the server announced a
    /// denial; any other error passes through unchanged.
    pub(crate) fn escalate(&self, error: ThadamError) -> ThadamError {
        match self.recorded() {
            Some(denial) if error.is_transient() => fatal_auth(
                codes::AUTH_FORBIDDEN,
                format!("the server denied the operation: {denial} (observed as: {error})"),
            ),
            _ => error,
        }
    }
}

/// What a failed publish means for the batch the event belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishFailure {
    /// The whole write fails; the retry class is baked into the error.
    Batch(ThadamError),
    /// Only this event fails, permanently; it goes to quarantine.
    Event(ThadamError),
}

fn transient(code: &'static str, message: impl Into<String>) -> ThadamError {
    ThadamError::transient(ErrorDomain::Sink, code, message)
}

fn fatal_auth(code: &'static str, message: impl Into<String>) -> ThadamError {
    ThadamError::fatal(ErrorDomain::Auth, code, message)
}

/// The server-replied JetStream API error anywhere in the source chain.
pub(crate) fn server_error<'a>(
    mut err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a async_nats::jetstream::Error> {
    loop {
        if let Some(js) = err.downcast_ref::<async_nats::jetstream::Error>() {
            return Some(js);
        }
        err = std::error::Error::source(err)?;
    }
}

/// Whether a server reply is an authentication or permission failure.
fn is_forbidden(js: &async_nats::jetstream::Error) -> bool {
    matches!(js.code(), 401 | 403)
}

/// Whether `error` reports a payload that exceeds the stream's or the
/// server's size limit (NATS err code 10054).
fn exceeds_size_limit(js: &async_nats::jetstream::Error) -> bool {
    js.error_code() == async_nats::jetstream::ErrorCode::STREAM_MESSAGE_EXCEEDS_MAXIMUM
}

/// Classifies a publish submit- or ack-failure from [`PublishErrorKind`]
/// plus any server reply in its source chain.
pub(crate) fn classify_publish(err: &PublishError) -> PublishFailure {
    let reject = |message: String| {
        PublishFailure::Event(ThadamError::permanent(
            ErrorDomain::Sink,
            codes::SINK_REJECTED,
            message,
        ))
    };
    match err.kind() {
        PublishErrorKind::MaxPayloadExceeded => reject(format!(
            "event payload exceeds the server's max_payload: {err}"
        )),
        PublishErrorKind::TimedOut => PublishFailure::Batch(transient(
            codes::SINK_TIMEOUT,
            format!("publish acknowledgement timed out: {err}"),
        )),
        PublishErrorKind::BrokenPipe => PublishFailure::Batch(transient(
            codes::CONNECTOR_DISCONNECTED,
            format!("the connection dropped before the acknowledgement: {err}"),
        )),
        PublishErrorKind::StreamNotFound => PublishFailure::Batch(transient(
            codes::SINK_UNAVAILABLE,
            format!("no responders for the publish subject: {err}"),
        )),
        PublishErrorKind::MaxAckPending => PublishFailure::Batch(transient(
            codes::SINK_UNAVAILABLE,
            format!("too many unacknowledged publishes: {err}"),
        )),
        PublishErrorKind::WrongLastMessageId | PublishErrorKind::WrongLastSequence => {
            PublishFailure::Batch(transient(
                codes::SINK_WRITE_FAILED,
                format!("stream state conflict: {err}"),
            ))
        }
        PublishErrorKind::Other => {
            let source = std::error::Error::source(err);
            if let Some(js) = source.and_then(server_error) {
                if exceeds_size_limit(js) {
                    return reject(format!(
                        "event message exceeds the stream's size limit: {js}"
                    ));
                }
                if is_forbidden(js) {
                    return PublishFailure::Batch(fatal_auth(
                        codes::AUTH_FORBIDDEN,
                        format!("publish permission denied: {js}"),
                    ));
                }
            }
            PublishFailure::Batch(transient(
                codes::SINK_WRITE_FAILED,
                format!("publish failed: {err}"),
            ))
        }
    }
}

/// Classifies a connect failure. Bad credentials are fatal; everything to do
/// with reaching a server that is not there (yet) is transient.
pub(crate) fn classify_connect(err: &ConnectError) -> ThadamError {
    match err.kind() {
        ConnectErrorKind::Authentication => fatal_auth(
            codes::AUTH_FAILED,
            format!("the server rejected the credentials: {err}"),
        ),
        ConnectErrorKind::AuthorizationViolation => fatal_auth(
            codes::AUTH_FORBIDDEN,
            format!("the account is not authorized: {err}"),
        ),
        ConnectErrorKind::TimedOut => ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_TIMEOUT,
            format!("connecting to the server timed out: {err}"),
        ),
        ConnectErrorKind::ServerParse => ThadamError::permanent(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            format!("the `url` is not a usable server address: {err}"),
        )
        .with_context("path", "url"),
        ConnectErrorKind::Dns
        | ConnectErrorKind::Tls
        | ConnectErrorKind::Io
        | ConnectErrorKind::MaxReconnects => ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_CONNECT_FAILED,
            format!("could not connect to the server: {err}"),
        ),
    }
}

/// Whether a `get_stream` failure says the stream does not exist.
pub(crate) fn stream_not_found(err: &GetStreamError) -> bool {
    match err.kind() {
        GetStreamErrorKind::JetStream(js) => {
            js.error_code() == async_nats::jetstream::ErrorCode::STREAM_NOT_FOUND
                || js.code() == 404
        }
        _ => false,
    }
}

/// Classifies a `get_stream` failure that is not "stream not found" (the
/// caller handles that case itself around `create_stream`).
pub(crate) fn classify_get_stream(err: &GetStreamError, stream: &str) -> ThadamError {
    match err.kind() {
        GetStreamErrorKind::JetStream(js) if is_forbidden(&js) => fatal_auth(
            codes::AUTH_FORBIDDEN,
            format!("permission to stream `{stream}` denied: {js}"),
        ),
        GetStreamErrorKind::JetStream(js) => transient(
            codes::SINK_UNAVAILABLE,
            format!("stream `{stream}` lookup failed: {js}"),
        ),
        GetStreamErrorKind::Request => transient(
            codes::SINK_TIMEOUT,
            format!("stream `{stream}` lookup failed: {err}"),
        ),
        GetStreamErrorKind::EmptyName | GetStreamErrorKind::InvalidStreamName => {
            ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                format!("`{stream}` is not a usable JetStream stream name"),
            )
            .with_context("path", "stream")
        }
    }
}

/// Classifies a `create_stream` failure. A name that appeared after the
/// probe is transient: retrying `open` finds it on the next `get_stream`.
pub(crate) fn classify_create_stream(err: &CreateStreamError, stream: &str) -> ThadamError {
    match err.kind() {
        CreateStreamErrorKind::JetStream(js) if is_forbidden(&js) => fatal_auth(
            codes::AUTH_FORBIDDEN,
            format!("permission to create stream `{stream}` denied: {js}"),
        ),
        CreateStreamErrorKind::EmptyStreamName | CreateStreamErrorKind::InvalidStreamName => {
            ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                format!("`{stream}` is not a usable JetStream stream name"),
            )
            .with_context("path", "stream")
        }
        _ => transient(
            codes::SINK_UNAVAILABLE,
            format!("creating stream `{stream}` failed: {err}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::Retry;

    fn js_error(code: usize, err_code: u64, description: &str) -> async_nats::jetstream::Error {
        serde_json::from_value(serde_json::json!({
            "code": code,
            "err_code": err_code,
            "description": description,
        }))
        .expect("jetstream error")
    }

    #[test]
    fn timeouts_and_dropped_connections_are_transient() {
        let err = PublishError::new(PublishErrorKind::TimedOut);
        let PublishFailure::Batch(e) = classify_publish(&err) else {
            panic!("timed out is a batch failure");
        };
        assert_eq!((e.code, e.retry), (codes::SINK_TIMEOUT, Retry::Transient));

        let err = PublishError::new(PublishErrorKind::BrokenPipe);
        let PublishFailure::Batch(e) = classify_publish(&err) else {
            panic!("broken pipe is a batch failure");
        };
        assert_eq!(e.code, codes::CONNECTOR_DISCONNECTED);
        assert!(e.is_transient());

        let err = PublishError::new(PublishErrorKind::StreamNotFound);
        let PublishFailure::Batch(e) = classify_publish(&err) else {
            panic!("no responders is a batch failure");
        };
        assert_eq!(e.code, codes::SINK_UNAVAILABLE);
        assert!(e.is_transient());
    }

    #[test]
    fn oversized_payloads_are_per_event_rejects() {
        let err = PublishError::new(PublishErrorKind::MaxPayloadExceeded);
        let PublishFailure::Event(e) = classify_publish(&err) else {
            panic!("max payload is a per-event reject");
        };
        assert_eq!((e.code, e.retry), (codes::SINK_REJECTED, Retry::Permanent));

        let js = js_error(413, 10054, "message size exceeds maximum allowed");
        let err = PublishError::with_source(PublishErrorKind::Other, js);
        let PublishFailure::Event(e) = classify_publish(&err) else {
            panic!("stream size limit is a per-event reject");
        };
        assert_eq!(e.code, codes::SINK_REJECTED);
        assert!(e.message.contains("size limit"));
    }

    #[test]
    fn permission_denied_is_fatal() {
        let js = js_error(403, 10118, "permissions violation");
        let err = PublishError::with_source(PublishErrorKind::Other, js);
        let PublishFailure::Batch(e) = classify_publish(&err) else {
            panic!("permission failure is a batch failure");
        };
        assert_eq!(
            (e.domain, e.code, e.retry),
            (ErrorDomain::Auth, codes::AUTH_FORBIDDEN, Retry::Fatal)
        );
    }

    #[test]
    fn unknown_server_replies_are_transient() {
        let js = js_error(500, 10077, "stream store failed");
        let err = PublishError::with_source(PublishErrorKind::Other, js);
        let PublishFailure::Batch(e) = classify_publish(&err) else {
            panic!("unknown reply is a batch failure");
        };
        assert_eq!(
            (e.code, e.retry),
            (codes::SINK_WRITE_FAILED, Retry::Transient)
        );
    }

    #[test]
    fn connect_classification() {
        let err = ConnectError::new(ConnectErrorKind::Authentication);
        let e = classify_connect(&err);
        assert_eq!(
            (e.domain, e.code, e.retry),
            (ErrorDomain::Auth, codes::AUTH_FAILED, Retry::Fatal)
        );

        let err = ConnectError::new(ConnectErrorKind::AuthorizationViolation);
        let e = classify_connect(&err);
        assert_eq!((e.domain, e.retry), (ErrorDomain::Auth, Retry::Fatal));

        let err = ConnectError::new(ConnectErrorKind::TimedOut);
        let e = classify_connect(&err);
        assert_eq!(
            (e.code, e.retry),
            (codes::CONNECTOR_TIMEOUT, Retry::Transient)
        );

        for kind in [
            ConnectErrorKind::Dns,
            ConnectErrorKind::Tls,
            ConnectErrorKind::Io,
            ConnectErrorKind::MaxReconnects,
        ] {
            let e = classify_connect(&ConnectError::new(kind));
            assert_eq!(e.code, codes::CONNECTOR_CONNECT_FAILED, "{kind}");
            assert!(e.is_transient(), "{kind}");
        }

        let e = classify_connect(&ConnectError::new(ConnectErrorKind::ServerParse));
        assert_eq!((e.domain, e.retry), (ErrorDomain::Config, Retry::Permanent));
    }

    #[test]
    fn stream_lookup_classification() {
        let js = js_error(404, 10059, "stream not found");
        let err = GetStreamError::new(GetStreamErrorKind::JetStream(js));
        assert!(stream_not_found(&err));

        let js = js_error(403, 10118, "permissions violation");
        let err = GetStreamError::new(GetStreamErrorKind::JetStream(js));
        let e = classify_get_stream(&err, "THADAM");
        assert_eq!(
            (e.domain, e.code, e.retry),
            (ErrorDomain::Auth, codes::AUTH_FORBIDDEN, Retry::Fatal)
        );
        assert!(!stream_not_found(&err));

        let js = js_error(500, 10076, "jetstream not enabled");
        let err = GetStreamError::new(GetStreamErrorKind::JetStream(js));
        let e = classify_get_stream(&err, "THADAM");
        assert_eq!(
            (e.code, e.retry),
            (codes::SINK_UNAVAILABLE, Retry::Transient)
        );
    }

    #[test]
    fn announced_denials_escalate_timeouts_to_fatal() {
        let denials = ServerDenials::default();
        let timeout = || transient(codes::SINK_TIMEOUT, "ack timed out");

        let untouched = denials.escalate(timeout());
        assert!(untouched.is_transient(), "no denial recorded yet");

        denials.observe(&Event::ServerError(ServerError::SlowConsumer(3)));
        denials.observe(&Event::ServerError(ServerError::Other(
            "maximum connections exceeded".into(),
        )));
        assert!(denials.recorded().is_none(), "unrelated server errors");

        denials.observe(&Event::ServerError(ServerError::Other(
            "Permissions Violation for Publish to \"thadam.imu.r7\"".into(),
        )));
        denials.observe(&Event::ServerError(ServerError::AuthorizationViolation));
        let first = denials.recorded().expect("recorded");
        assert!(
            first.contains("Permissions Violation"),
            "first one wins: {first}"
        );

        let escalated = denials.escalate(timeout());
        assert_eq!(
            (escalated.domain, escalated.code, escalated.retry),
            (ErrorDomain::Auth, codes::AUTH_FORBIDDEN, Retry::Fatal)
        );
        assert!(escalated.message.contains("Permissions Violation"));

        let permanent = ThadamError::permanent(ErrorDomain::Sink, codes::SINK_REJECTED, "x");
        assert_eq!(denials.escalate(permanent).retry, Retry::Permanent);
    }

    #[test]
    fn authorization_and_expiry_texts_count_as_denials() {
        for error in [
            ServerError::AuthorizationViolation,
            ServerError::Other("User Authentication Expired".into()),
            ServerError::Other("User Authentication Revoked".into()),
        ] {
            let denials = ServerDenials::default();
            denials.observe(&Event::ServerError(error.clone()));
            assert!(denials.recorded().is_some(), "{error:?}");
        }
        let denials = ServerDenials::default();
        denials.observe(&Event::Disconnected);
        assert!(denials.recorded().is_none());
    }

    #[test]
    fn create_stream_races_are_transient() {
        let js = js_error(400, 10058, "stream name already in use");
        let err = CreateStreamError::new(CreateStreamErrorKind::JetStream(js));
        let e = classify_create_stream(&err, "THADAM");
        assert!(
            e.is_transient(),
            "a re-create race resolves on the next open"
        );
    }
}
