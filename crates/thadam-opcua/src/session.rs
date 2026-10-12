//! The session task: connect, then either subscribe and forward publish
//! responses (`mode: subscribe`) or read every node on a timer (`mode:
//! poll`), reconnecting on loss.
//!
//! Reconnects are supervised here, not inside `async-opcua`, so the cadence
//! is fully controlled: an exponential backoff with jitter between failed
//! handshakes plus a hard rate limit on connection attempts
//! (`reconnect.max_handshakes_per_minute`). Each handshake performs RSA
//! operations, and the rate limit is what bounds the number of timing
//! samples an observer can collect toward RUSTSEC-2023-0071 (the `rsa` 0.9
//! Marvin advisory; see the crate docs).
//!
//! Subscribe mode: one publish response becomes one raw message. The
//! subscription callback runs inside the client event loop, so it never
//! awaits: it maps values, builds the message, and `try_send`s it. When the
//! runtime falls far enough behind that the bounded channel is full, whole
//! publish responses are dropped and counted — the running total rides on
//! later messages as `opcua.dropped`. A non-Good status is never lost: it
//! becomes a `null` value named in `opcua.bad_status`.
//!
//! Poll mode: one Read service call per tick, one message per poll. The
//! poll sends with backpressure rather than dropping, since the interval
//! already paces it.

use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use opcua::client::{
    Client, ClientBuilder, IdentityToken, MonitoredItemMap, OnSubscriptionNotificationCore,
    Session, SessionPollResult,
};
use opcua::types::{
    AttributeId, DataChangeNotification, DataValue, EndpointDescription, MessageSecurityMode,
    MonitoredItemCreateRequest, MonitoringMode, MonitoringParameters, NotificationMessage,
    ReadValueId, TimestampsToReturn, UAString,
};
use thadam_core::{Classify, Clock, RawMessage, Retry, Shutdown, Timestamp};
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt;
use tracing::{debug, warn};

use crate::config::{AuthConfig, ModeSpec, OpcUaConfig, SecurityModeSpec, TIMESTAMP_KEY};
use crate::error::{OpcUaError, from_connect, from_status, status_name};
use crate::pki;
use crate::value::{MappedValue, map_data_value, newest_timestamp_us};

/// Publish responses the session task may run ahead of `next_batch`.
/// Responses beyond this queue are dropped and counted as `opcua.dropped`.
pub(crate) const CHANNEL_CAPACITY: usize = 64;

/// A dropped-response warning is logged for the first drop and then every
/// this many drops.
const WARN_EVERY: u64 = 1_000;

/// The first retry waits `min_backoff`, doubling from there; the jittered
/// delay stays within the upper half of the computed window. Copied from
/// the Modbus source so reconnect behavior is uniform across connectors.
pub(crate) fn backoff(min: Duration, max: Duration, failures: u32, unit: f64) -> Duration {
    let shift = failures.saturating_sub(1).min(20);
    let window = min.saturating_mul(1 << shift).min(max);
    let half = window / 2;
    half + half.mul_f64(unit)
}

fn random_unit() -> f64 {
    (RandomState::new().build_hasher().finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// GCRA-style spacing for connection handshakes: one token at a time,
/// refilled every `60s / per_minute`. The first attempt is immediate;
/// bursts are impossible, which is the point.
pub(crate) struct HandshakeLimiter {
    interval: Duration,
    next_at: Option<tokio::time::Instant>,
}

impl HandshakeLimiter {
    /// A limiter allowing `per_minute` handshakes per minute, one at a time.
    pub(crate) fn new(per_minute: u32) -> Self {
        HandshakeLimiter {
            interval: Duration::from_secs(60) / per_minute.max(1),
            next_at: None,
        }
    }

    /// Waits until the next handshake is due, then consumes it.
    pub(crate) async fn acquire(&mut self) {
        let now = tokio::time::Instant::now();
        if let Some(next) = self.next_at
            && next > now
        {
            tokio::time::sleep_until(next).await;
        }
        let now = tokio::time::Instant::now();
        let base = self.next_at.map_or(now, |n| n.max(now));
        self.next_at = Some(base + self.interval);
    }
}

/// How one supervised connection attempt ended.
pub(crate) enum Attempt {
    /// The session established, ran, and then ended. The failure count
    /// resets; the rate limiter paces the next attempt.
    Established(OpcUaError),
    /// The session never established (handshake failed). Backoff grows.
    Connecting(OpcUaError),
}

fn is_fatal(error: &OpcUaError) -> bool {
    matches!(Classify::retry(error), Retry::Fatal)
}

/// The reconnect supervisor: acquire a handshake token, run `connect` to
/// completion, back off after failed handshakes, stop on `stop` or a fatal
/// error. Pulled out of `run` so tests can drive it with a fake connector.
pub(crate) async fn supervise<Connect, Fut>(
    config: Arc<OpcUaConfig>,
    stop: Shutdown,
    mut connect: Connect,
) -> Result<(), OpcUaError>
where
    Connect: FnMut() -> Fut,
    Fut: Future<Output = Result<Attempt, OpcUaError>>,
{
    let min = config.reconnect.min_backoff.0;
    let max = config.reconnect.max_backoff.0;
    let mut limiter = HandshakeLimiter::new(config.reconnect.max_handshakes_per_minute);
    let mut failures: u32 = 0;
    loop {
        tokio::select! {
            () = limiter.acquire() => {}
            () = stop.triggered() => return Ok(()),
        }
        let attempt = tokio::select! {
            attempt = connect() => attempt,
            () = stop.triggered() => return Ok(()),
        };
        match attempt {
            Err(error) => {
                warn!(endpoint = %config.endpoint, %error, "OPC UA source failed");
                return Err(error);
            }
            Ok(Attempt::Established(error)) => {
                if is_fatal(&error) {
                    warn!(endpoint = %config.endpoint, %error, "OPC UA session ended fatally");
                    return Err(error);
                }
                failures = 0;
                debug!(
                    endpoint = %config.endpoint,
                    %error,
                    "OPC UA session ended; reconnecting"
                );
            }
            Ok(Attempt::Connecting(error)) => {
                if is_fatal(&error) {
                    warn!(endpoint = %config.endpoint, %error, "OPC UA connect failed fatally");
                    return Err(error);
                }
                failures = failures.saturating_add(1);
                let wait = backoff(min, max, failures, random_unit());
                warn!(
                    endpoint = %config.endpoint,
                    failures,
                    retry_in = ?wait,
                    %error,
                    "OPC UA connect failed"
                );
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = stop.triggered() => return Ok(()),
                }
            }
        }
    }
}

/// Runs the supervised reconnect loop until `stop` or a fatal error.
pub(crate) async fn run(
    config: OpcUaConfig,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
    stop: Shutdown,
) -> Result<(), OpcUaError> {
    let config = Arc::new(config);
    let closure_config = Arc::clone(&config);
    supervise(Arc::clone(&config), stop, move || {
        one_session(Arc::clone(&closure_config), Arc::clone(&clock), tx.clone())
    })
    .await
}

/// Builds the `async-opcua` client from the configuration. Trust entries
/// are installed into the PKI store first, so the client finds them when
/// it checks the server certificate.
fn build_client(config: &OpcUaConfig) -> Result<Client, OpcUaError> {
    let keep_alive = (config.subscribe_settings().publishing_interval.0 * 3)
        .clamp(Duration::from_secs(2), Duration::from_secs(30));
    let mut builder = ClientBuilder::new()
        .application_name("thadam")
        .application_uri("urn:thadam:opcua")
        .product_uri("urn:thadam:opcua")
        .create_sample_keypair(false)
        .session_name(config.endpoint.clone())
        .session_retry_limit(0) // reconnects are supervised in `supervise`
        .recreate_subscriptions(false)
        .keep_alive_interval(keep_alive);
    if let Some(cert) = &config.cert {
        let trusted: Vec<_> = cert
            .trust_server_certs
            .iter()
            .map(|path| cert.resolve(path))
            .collect();
        pki::install_trusted(&cert.pki_dir, &trusted).map_err(OpcUaError::Config)?;
        builder = builder
            .pki_dir(&cert.pki_dir)
            .certificate_path(cert.resolve(&cert.cert))
            .private_key_path(cert.resolve(&cert.key))
            .trust_server_certs(cert.accept_unknown_server_cert)
            .verify_server_certs(true);
    }
    builder
        .client()
        .map_err(|problems| OpcUaError::Config(problems.join("; ")))
}

/// The endpoint the client matches against discovery: exactly the
/// configured URL, policy, and mode.
fn endpoint_description(config: &OpcUaConfig) -> EndpointDescription {
    EndpointDescription {
        endpoint_url: UAString::from(config.endpoint.clone()),
        security_policy_uri: UAString::from(config.security_policy.uri()),
        security_mode: match config.security_mode {
            SecurityModeSpec::None => MessageSecurityMode::None,
            SecurityModeSpec::Sign => MessageSecurityMode::Sign,
            SecurityModeSpec::SignAndEncrypt => MessageSecurityMode::SignAndEncrypt,
        },
        ..Default::default()
    }
}

fn identity_token(config: &OpcUaConfig) -> Result<IdentityToken, OpcUaError> {
    match &config.auth {
        AuthConfig::Anonymous => Ok(IdentityToken::Anonymous),
        AuthConfig::Username { username, password } => Ok(IdentityToken::UserName(
            username.clone(),
            password.clone().into(),
        )),
        AuthConfig::Certificate { cert, key } => {
            let Some(material) = config.cert.as_ref() else {
                return Err(OpcUaError::Config(
                    "auth kind certificate needs the cert block".into(),
                ));
            };
            IdentityToken::new_x509_path(material.resolve(cert), material.resolve(key))
                .map_err(|e| OpcUaError::Config(format!("cannot load the user certificate: {e}")))
        }
    }
}

/// Lifecycle events the event-loop driver forwards to the supervisor.
/// Transport-level noise (per-message polls) is dropped in the driver.
enum SessionEvent {
    /// The first secure channel + session came up.
    Connected,
    /// An established connection dropped.
    Lost(opcua::types::StatusCode),
    /// The event loop ended with an error.
    Failed(opcua::types::StatusCode),
    /// The event loop ended without an error.
    Ended,
}

/// Drives the client event loop in its own task: service calls
/// (`create_subscription`, `create_monitored_items`, `read`) only complete
/// while the loop is being polled, and subscription notifications fire from
/// inside it. Connect/disconnect liveness events flow over `events`.
async fn drive(
    event_loop: opcua::client::SessionEventLoop<opcua::client::transport::TcpConnector>,
    events: mpsc::Sender<SessionEvent>,
) {
    let mut stream = Box::pin(event_loop.enter());
    while let Some(item) = stream.next().await {
        let event = match item {
            Ok(SessionPollResult::Reconnected(_)) => Some(SessionEvent::Connected),
            Ok(SessionPollResult::ConnectionLost(code)) => Some(SessionEvent::Lost(code)),
            Ok(SessionPollResult::FinishedDisconnect) => Some(SessionEvent::Ended),
            Ok(_) => None,
            Err(status) => Some(SessionEvent::Failed(status)),
        };
        let Some(event) = event else { continue };
        if events.send(event).await.is_err() {
            return;
        }
    }
    let _ = events.send(SessionEvent::Ended).await;
}

/// Aborts the event-loop driver when a cycle ends, so a failed cycle never
/// leaves a live connection behind it.
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The attempt a lifecycle event ends, or `None` when the event is
/// informational.
fn session_ended(event: Option<SessionEvent>) -> Option<Attempt> {
    match event {
        Some(SessionEvent::Connected) => None,
        Some(SessionEvent::Lost(code)) => Some(Attempt::Established(OpcUaError::Transport(
            format!("connection lost ({})", status_name(&code)),
        ))),
        Some(SessionEvent::Failed(status)) => Some(Attempt::Established(from_status(status))),
        Some(SessionEvent::Ended) | None => Some(Attempt::Established(OpcUaError::Transport(
            "the session event loop ended".into(),
        ))),
    }
}

/// Performs one full connect-and-forward cycle. `Ok` reports how far the
/// cycle got; `Err` is always fatal and ends the source.
async fn one_session(
    config: Arc<OpcUaConfig>,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
) -> Result<Attempt, OpcUaError> {
    let mut client = build_client(&config)?;
    let identity = identity_token(&config)?;
    let (session, event_loop) = match client
        .connect_to_matching_endpoint(endpoint_description(&config), identity)
        .await
    {
        Ok(pair) => pair,
        Err(e) => {
            return Ok(Attempt::Connecting(from_connect(
                &config.endpoint,
                e.status(),
                e.to_string(),
            )));
        }
    };

    let (events_tx, mut events_rx) = mpsc::channel(8);
    let driver = tokio::spawn(drive(event_loop, events_tx)).abort_handle();
    let _stop_driver = AbortOnDrop(driver);

    // Wait for the first connect, driven by the spawned event loop.
    match events_rx.recv().await {
        Some(SessionEvent::Connected) => {}
        Some(SessionEvent::Failed(status)) => {
            return Ok(Attempt::Connecting(from_status(status)));
        }
        _ => {
            return Ok(Attempt::Connecting(OpcUaError::Transport(
                "the session event loop ended before connecting".into(),
            )));
        }
    }

    match config.mode {
        ModeSpec::Subscribe => subscribe_cycle(&session, &config, clock, tx, &mut events_rx).await,
        ModeSpec::Poll => poll_cycle(&session, &config, clock, tx, &mut events_rx).await,
    }
}

/// Creates the subscription and monitored items, then forwards publish
/// responses until the connection ends.
async fn subscribe_cycle(
    session: &Session,
    config: &OpcUaConfig,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
    events: &mut mpsc::Receiver<SessionEvent>,
) -> Result<Attempt, OpcUaError> {
    let settings = config.subscribe_settings();
    let forward = Forwarder::new(config, clock, tx);
    let published_id = forward.subscription_id();
    let sub_result = session
        .create_subscription(settings.publishing_interval.0, 100, 20, 0, 0, true, forward)
        .await;
    let subscription_id = match sub_result {
        Ok(id) => id,
        Err(e) => return Ok(Attempt::Established(from_status(e.status()))),
    };
    published_id.store(subscription_id, Ordering::Relaxed);

    let requests: Vec<MonitoredItemCreateRequest> = config
        .nodes
        .iter()
        .enumerate()
        .map(|(i, spec)| MonitoredItemCreateRequest {
            item_to_monitor: ReadValueId {
                node_id: spec.node_id.clone(),
                attribute_id: AttributeId::Value as u32,
                ..Default::default()
            },
            monitoring_mode: MonitoringMode::Reporting,
            requested_parameters: MonitoringParameters {
                client_handle: i as u32 + 1,
                sampling_interval: settings.sampling_interval.0.as_secs_f64() * 1000.0,
                queue_size: settings.queue_size,
                discard_oldest: true,
                ..Default::default()
            },
        })
        .collect();
    match session
        .create_monitored_items(subscription_id, TimestampsToReturn::Both, requests)
        .await
    {
        Ok(results) => {
            for (spec, result) in config.nodes.iter().zip(results.iter()) {
                let status = &result.result.status_code;
                if !status.is_good() {
                    warn!(
                        endpoint = %config.endpoint,
                        name = %spec.name,
                        node_id = %spec.node_id,
                        status = %status_name(status),
                        "OPC UA monitored item refused"
                    );
                }
            }
        }
        Err(e) => return Ok(Attempt::Established(from_status(e.status()))),
    }

    debug!(
        endpoint = %config.endpoint,
        subscription_id,
        nodes = config.nodes.len(),
        "OPC UA subscription established"
    );

    loop {
        if let Some(attempt) = session_ended(events.recv().await) {
            return Ok(attempt);
        }
    }
}

/// Reads every node in one Read call per `interval` and emits one message
/// per poll, until the connection ends.
async fn poll_cycle(
    session: &Session,
    config: &OpcUaConfig,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
    events: &mut mpsc::Receiver<SessionEvent>,
) -> Result<Attempt, OpcUaError> {
    let Some(interval) = config.interval.map(|i| i.0) else {
        return Err(OpcUaError::Config("mode: poll needs interval".into()));
    };
    let reads: Vec<ReadValueId> = config
        .nodes
        .iter()
        .map(|spec| ReadValueId {
            node_id: spec.node_id.clone(),
            attribute_id: AttributeId::Value as u32,
            ..Default::default()
        })
        .collect();
    let names: Vec<&str> = config.nodes.iter().map(|n| n.name.as_str()).collect();
    let topic = format!("opcua/{}/poll", host_of(&config.endpoint));

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            event = events.recv() => {
                if let Some(attempt) = session_ended(event) {
                    return Ok(attempt);
                }
                continue;
            }
        }
        let read = tokio::select! {
            result = session.read(&reads, TimestampsToReturn::Both, 0.0) => result,
            event = events.recv() => {
                if let Some(attempt) = session_ended(event) {
                    return Ok(attempt);
                }
                continue;
            }
        };
        let values = match read {
            Ok(values) => values,
            Err(e) => return Ok(Attempt::Established(from_status(e.status()))),
        };
        if values.len() != names.len() {
            return Ok(Attempt::Established(OpcUaError::Protocol(format!(
                "the server answered {} values for {} nodes",
                values.len(),
                names.len()
            ))));
        }

        let mut message = grouped_message(clock.now(), &names, &values);
        message
            .attrs
            .insert("opcua.endpoint", config.endpoint.clone());
        message.attrs.insert("topic", topic.clone());
        if tx.send(message).await.is_err() {
            return Err(OpcUaError::Cancelled);
        }
    }
}

/// The host part of an endpoint URL, or the URL itself when it does not
/// parse.
fn host_of(endpoint: &str) -> String {
    url::Url::parse(endpoint)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
        .unwrap_or_else(|| endpoint.to_owned())
}

/// One message grouping `values`: each `names[i]` maps to `values[i]`'s JSON
/// value, with `ts_us` and the `opcua.bad_status` and `opcua.unsupported`
/// attributes when they apply. Callers add the source attributes.
fn grouped_message(received: Timestamp, names: &[&str], values: &[DataValue]) -> RawMessage {
    let mut object = Map::new();
    let mut bad: Vec<String> = Vec::new();
    let mut unsupported: Vec<String> = Vec::new();
    for (name, dv) in names.iter().zip(values) {
        let MappedValue {
            json,
            bad_status,
            unsupported: kind,
        } = map_data_value(dv);
        object.insert((*name).to_owned(), json);
        if let Some(status) = bad_status {
            bad.push(format!("{name}={status}"));
        }
        if let Some(kind) = kind {
            unsupported.push(format!("{name}={kind}"));
        }
    }
    let ts_us = newest_timestamp_us(values).unwrap_or_else(|| received.as_micros());
    object.insert(TIMESTAMP_KEY.to_owned(), Value::from(ts_us));
    let payload = serde_json::to_vec(&Value::Object(object)).unwrap_or_else(|_| b"{}".to_vec());

    let mut message = RawMessage::new(received, Bytes::from(payload));
    if !bad.is_empty() {
        message.attrs.insert("opcua.bad_status", bad.join(","));
    }
    if !unsupported.is_empty() {
        message
            .attrs
            .insert("opcua.unsupported", unsupported.join(","));
    }
    message
}

/// Maps subscription publish responses to raw messages inside the client
/// event loop.
///
/// The callback is synchronous, so it does every conversion eagerly and
/// hands finished messages to the runtime's channel with `try_send`: a
/// channel-full drop falls to `opcua.dropped`.
struct Forwarder {
    names: Vec<String>,
    endpoint: String,
    host: String,
    subscription_id: Arc<AtomicU32>,
    dropped: Arc<AtomicU64>,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
}

impl Forwarder {
    fn new(config: &OpcUaConfig, clock: Arc<dyn Clock>, tx: mpsc::Sender<RawMessage>) -> Self {
        Forwarder {
            names: config.nodes.iter().map(|n| n.name.clone()).collect(),
            endpoint: config.endpoint.clone(),
            host: host_of(&config.endpoint),
            subscription_id: Arc::new(AtomicU32::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
            clock,
            tx,
        }
    }

    fn subscription_id(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.subscription_id)
    }

    fn data_change(&self, notification: DataChangeNotification) {
        let subscription_id = self.subscription_id.load(Ordering::Relaxed);
        let mut names: Vec<&str> = Vec::new();
        let mut values: Vec<DataValue> = Vec::new();
        for item in notification.monitored_items.into_iter().flatten() {
            let index = item
                .client_handle
                .checked_sub(1)
                .and_then(|i| usize::try_from(i).ok())
                .unwrap_or(usize::MAX);
            let Some(name) = self.names.get(index) else {
                warn!(
                    client_handle = item.client_handle,
                    "OPC UA notification for an unknown monitored item"
                );
                continue;
            };
            names.push(name.as_str());
            values.push(item.value);
        }
        if values.is_empty() {
            return;
        }

        let mut message = grouped_message(self.clock.now(), &names, &values);
        message
            .attrs
            .insert("opcua.endpoint", self.endpoint.clone());
        message
            .attrs
            .insert("opcua.subscription_id", subscription_id.to_string());
        message
            .attrs
            .insert("topic", format!("opcua/{}/{subscription_id}", self.host));
        let total = self.dropped.load(Ordering::Relaxed);
        if total > 0 {
            message.attrs.insert("opcua.dropped", total.to_string());
        }
        match self.tx.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped == 1 || dropped.is_multiple_of(WARN_EVERY) {
                    warn!(
                        endpoint = %self.endpoint,
                        dropped,
                        "OPC UA queue full; dropping publish responses"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

impl OnSubscriptionNotificationCore for Forwarder {
    fn on_subscription_notification(
        &mut self,
        notification: NotificationMessage,
        _monitored_items: MonitoredItemMap<'_>,
    ) {
        let Some(datas) = notification.notification_data else {
            return;
        };
        for obj in datas {
            if obj.inner_is::<DataChangeNotification>() {
                let Some(notif) = obj.into_inner_as::<DataChangeNotification>() else {
                    continue;
                };
                self.data_change(*notif);
            }
            // Event and status-change notifications are not consumed.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn backoff_doubles_within_jitter_and_caps() {
        let min = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        assert_eq!(backoff(min, max, 1, 0.0), Duration::from_millis(50));
        assert_eq!(backoff(min, max, 1, 1.0), min);
        assert_eq!(backoff(min, max, 3, 1.0), Duration::from_millis(400));
        assert_eq!(backoff(min, max, 30, 1.0), max);
    }

    fn config_for_limit(per_minute: u32) -> Arc<OpcUaConfig> {
        Arc::new(
            OpcUaConfig::from_value(&json!({
                "endpoint": "opc.tcp://127.0.0.1:4840",
                "security_policy": "none",
                "security_mode": "none",
                "nodes": [{"node_id": "ns=2;i=1", "name": "t"}],
                "reconnect": {"min_backoff": "1ms", "max_backoff": "1ms",
                              "max_handshakes_per_minute": per_minute}
            }))
            .expect("valid"),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn the_limiter_allows_one_immediately_then_spaces_attempts() {
        let mut limiter = HandshakeLimiter::new(120); // one per 500ms
        limiter.acquire().await; // t=0: immediate
        let start = tokio::time::Instant::now();
        limiter.acquire().await; // t=500
        assert_eq!(start.elapsed(), Duration::from_millis(500));
        limiter.acquire().await; // t=1000
        assert_eq!(start.elapsed(), Duration::from_millis(1000));
        // An idle period is not banked: the first acquire after it is
        // immediate, the next is spaced again.
        tokio::time::advance(Duration::from_secs(60)).await;
        let start = tokio::time::Instant::now();
        limiter.acquire().await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        limiter.acquire().await;
        assert_eq!(start.elapsed(), Duration::from_millis(500));
    }

    /// Advances the paused clock through every handshake slot, one slot at
    /// a time; a big single jump would skip work scheduled between slots.
    async fn advance_slots(slot: Duration, count: u32) {
        for _ in 0..count {
            tokio::time::advance(slot).await;
            tokio::task::yield_now().await;
        }
    }

    /// The handshake rate limit, counting connection attempts while the
    /// "server" is down: every attempt fails, so the loop is paced by the
    /// limiter, not by whatever a backoff would do on its own.
    #[tokio::test(start_paused = true)]
    async fn supervise_counts_handshakes_while_the_server_is_down() {
        let attempts = Arc::new(AtomicU32::new(0));
        let stop = Shutdown::new();
        let connect = {
            let attempts = Arc::clone(&attempts);
            move || {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    Ok(Attempt::Connecting(OpcUaError::Transport(
                        "server is down".into(),
                    )))
                }
            }
        };
        let task = tokio::spawn(supervise(
            config_for_limit(6000), // one handshake per 10ms
            stop.clone(),
            connect,
        ));
        tokio::task::yield_now().await;
        // Attempts at t = 0, 10, ..., 990 ms regardless of the failures.
        advance_slots(Duration::from_millis(10), 99).await;
        assert_eq!(attempts.load(Ordering::Relaxed), 100);
        stop.trigger();
        task.await.expect("supervise ends").expect("clean end");
    }

    #[tokio::test(start_paused = true)]
    async fn supervise_retries_after_an_established_session_too() {
        let attempts = Arc::new(AtomicU32::new(0));
        let stop = Shutdown::new();
        let connect = {
            let attempts = Arc::clone(&attempts);
            move || {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    Ok(Attempt::Established(OpcUaError::Transport(
                        "session dropped".into(),
                    )))
                }
            }
        };
        let task = tokio::spawn(supervise(config_for_limit(6000), stop.clone(), connect));
        tokio::task::yield_now().await;
        advance_slots(Duration::from_millis(10), 99).await;
        assert_eq!(attempts.load(Ordering::Relaxed), 100);
        stop.trigger();
        task.await.expect("supervise ends").expect("clean end");
    }

    #[tokio::test(start_paused = true)]
    async fn supervise_stops_on_a_fatal_error() {
        let attempts = Arc::new(AtomicU32::new(0));
        let stop = Shutdown::new();
        let connect = {
            let attempts = Arc::clone(&attempts);
            move || {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    Ok(Attempt::Connecting(OpcUaError::Authenticate(
                        "bad credentials".into(),
                    )))
                }
            }
        };
        let result = supervise(config_for_limit(6000), stop, connect).await;
        assert_eq!(
            result,
            Err(OpcUaError::Authenticate("bad credentials".into()))
        );
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_poll_message_groups_every_node_and_names_bad_values() {
        use opcua::types::{DateTime, StatusCode, Variant};
        let stamp: DateTime = chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000)
            .expect("in range")
            .into();
        let good = DataValue {
            value: Some(Variant::Int32(7)),
            status: Some(StatusCode::Good),
            source_timestamp: Some(stamp),
            ..DataValue::default()
        };
        let bad = DataValue {
            value: Some(Variant::Double(0.0)),
            status: Some(StatusCode::BadSensorFailure),
            ..DataValue::default()
        };
        let received = Timestamp::from_micros(42).expect("in range");
        let message = grouped_message(received, &["count", "temp"], &[good, bad]);
        let body: Value = serde_json::from_slice(&message.payload).expect("json");
        assert_eq!(body["count"], json!(7));
        assert_eq!(body["temp"], Value::Null);
        assert_eq!(body[TIMESTAMP_KEY], json!(1_700_000_000_000_000i64));
        assert_eq!(
            message.attrs.get("opcua.bad_status"),
            Some("temp=BadSensorFailure")
        );
    }

    #[test]
    fn a_poll_message_without_timestamps_uses_the_receive_time() {
        let received = Timestamp::from_micros(42).expect("in range");
        let message = grouped_message(received, &["x"], &[DataValue::default()]);
        let body: Value = serde_json::from_slice(&message.payload).expect("json");
        assert_eq!(body[TIMESTAMP_KEY], json!(42));
        assert!(message.attrs.get("opcua.bad_status").is_none());
    }
}
