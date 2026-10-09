//! `mode: poll`: periodically fetch URLs and emit each successful response as
//! one message.
//!
//! Polling is at-most-once per response. The upstream is never told about the
//! delivery, so a response whose message was not committed before a crash is
//! gone. The upstream is only re-read if the operator arranges that with an
//! idempotent endpoint.

use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{HeaderName, HeaderValue};
use thadam_core::{Attributes, Shutdown};
use tokio::task::JoinSet;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::config::{PollConfig, PollMethod};
use crate::error::HttpError;
use crate::queue::{Message, Shared};

/// The first retry after a failure waits one `interval`, doubling from there.
/// The jittered delay stays within the upper half of the computed window.
fn backoff(interval: Duration, max: Duration, failures: u32, unit: f64) -> Duration {
    let shift = failures.saturating_sub(1).min(20);
    let window = interval.saturating_mul(1 << shift).min(max);
    let half = window / 2;
    half + half.mul_f64(unit)
}

fn random_unit() -> f64 {
    (RandomState::new().build_hasher().finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// Removes credentials and the query string, which may carry secrets, before
/// a URL reaches an attribute, a log line, or an error.
pub(crate) fn redact(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(mut url) => {
            url.set_query(None);
            url.set_fragment(None);
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.to_string()
        }
        Err(_) => "<invalid url>".to_owned(),
    }
}

/// The shared client: rustls with the `ring` provider and the platform's
/// trust roots. Redirects and environment proxies are not followed.
pub(crate) fn client() -> Result<reqwest::Client, HttpError> {
    let mut roots = RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    let tls = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| HttpError::Tls(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|e| HttpError::Tls(format!("cannot build the HTTP client: {e}")))
}

struct Fetched {
    status: u16,
    content_type: Option<String>,
    body: Bytes,
}

async fn fetch(
    client: &reqwest::Client,
    poll: &PollConfig,
    headers: &[(HeaderName, HeaderValue)],
    url: &str,
    limit: usize,
) -> Result<Fetched, HttpError> {
    let safe = redact(url);
    let timeout = poll.timeout.0;
    let request = match poll.method {
        PollMethod::Get => client.get(url),
        PollMethod::Post => {
            let request = client.post(url);
            match &poll.body {
                Some(body) => request.body(body.clone()),
                None => request,
            }
        }
    };
    let mut request = request.timeout(timeout);
    for (name, value) in headers {
        request = request.header(name.clone(), value.clone());
    }

    let fail = |e: reqwest::Error| {
        if e.is_timeout() {
            HttpError::Timeout(timeout)
        } else {
            HttpError::Request {
                url: safe.clone(),
                detail: e.without_url().to_string(),
            }
        }
    };

    let mut response = request.send().await.map_err(fail)?;
    let status = response.status();
    if !status.is_success() {
        return Err(HttpError::Status {
            url: safe,
            status: status.as_u16(),
        });
    }
    if response
        .content_length()
        .is_some_and(|len| len > limit as u64)
    {
        return Err(HttpError::TooLarge { url: safe, limit });
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(fail)? {
        if body.len() + chunk.len() > limit {
            return Err(HttpError::TooLarge { url: safe, limit });
        }
        body.extend_from_slice(&chunk);
    }

    Ok(Fetched {
        status: status.as_u16(),
        content_type,
        body: Bytes::from(body),
    })
}

fn message(url: &str, fetched: Fetched) -> Message {
    let mut attrs = Attributes::new();
    attrs.insert("http.url", redact(url));
    attrs.insert("http.status", fetched.status.to_string());
    if let Some(content_type) = fetched.content_type {
        attrs.insert("http.content_type", content_type);
    }
    Message {
        payload: fetched.body,
        attrs,
        reply: None,
    }
}

/// Spawns one schedule per configured URL.
pub(crate) fn spawn_all(
    poll: &PollConfig,
    client: &reqwest::Client,
    limit: usize,
    queue: &Arc<Shared>,
    stop: &Shutdown,
    tasks: &mut JoinSet<()>,
) {
    let headers: Vec<(HeaderName, HeaderValue)> = poll
        .headers
        .iter()
        .filter_map(|(name, value)| {
            Some((
                HeaderName::from_bytes(name.as_bytes()).ok()?,
                HeaderValue::from_str(value).ok()?,
            ))
        })
        .collect();
    for url in &poll.urls {
        tasks.spawn(run_url(
            client.clone(),
            url.clone(),
            poll.clone(),
            headers.clone(),
            limit,
            Arc::clone(queue),
            stop.clone(),
        ));
    }
}

async fn run_url(
    client: reqwest::Client,
    url: String,
    poll: PollConfig,
    headers: Vec<(HeaderName, HeaderValue)>,
    limit: usize,
    queue: Arc<Shared>,
    stop: Shutdown,
) {
    let interval = poll.interval.0;
    let mut wait = Duration::ZERO;
    let mut failures: u32 = 0;
    loop {
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = stop.triggered() => return,
        }

        let result = tokio::select! {
            result = fetch(&client, &poll, &headers, &url, limit) => result,
            () = stop.triggered() => return,
        };
        match result {
            Ok(fetched) => {
                failures = 0;
                wait = interval;
                if !queue.push_when_space(message(&url, fetched), &stop).await {
                    return;
                }
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                wait = backoff(interval, poll.backoff_max.0, failures, random_unit());
                tracing::warn!(
                    url = %redact(&url),
                    failures,
                    retry_in = ?wait,
                    error = %error,
                    "HTTP poll failed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_within_jitter_and_caps() {
        let interval = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        assert_eq!(backoff(interval, max, 1, 0.0), Duration::from_millis(50));
        assert_eq!(backoff(interval, max, 1, 1.0), Duration::from_millis(100));
        assert_eq!(backoff(interval, max, 3, 1.0), Duration::from_millis(400));
        assert_eq!(backoff(interval, max, 30, 1.0), max);
    }

    #[test]
    fn redaction_drops_credentials_and_query() {
        assert_eq!(
            redact("https://user:secret@example.com/a/b?token=abc#frag"),
            "https://example.com/a/b"
        );
        assert_eq!(redact("not a url"), "<invalid url>");
    }
}
