//! Admin HTTP API server.
//!
//! Provides health, readiness, metrics, and status endpoints for monitoring
//! and operational purposes.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use thadam_core::{ThadamError, codes};
use tokio::net::TcpListener;

use crate::observability::PrometheusMetrics;
use crate::status::PipelineHandle;

/// Configuration for the admin HTTP server.
///
/// Specifies the bind address and optional authentication token.
/// When binding to a non-loopback address, a token is required for security.
#[derive(Clone, Debug)]
pub struct AdminConfig {
    /// The socket address to bind to.
    pub bind: SocketAddr,
    /// Optional bearer token for authentication.
    /// Required when binding to a non-loopback address.
    pub token: Option<String>,
}

/// Validates the admin server configuration.
///
/// Returns an error if binding to a non-loopback address without a token,
/// as this would expose the admin API to unauthorized access.
///
/// # Errors
///
/// Returns a [`ThadamError`] with domain [`ErrorDomain::Config`](thadam_core::ErrorDomain::Config)
/// and code [`CONFIG_INCOMPATIBLE`](thadam_core::codes::CONFIG_INCOMPATIBLE)
/// if `bind` is not loopback and `token` is `None`.
pub fn validate_admin_config(cfg: &AdminConfig) -> Result<(), ThadamError> {
    if !cfg.bind.ip().is_loopback() && cfg.token.is_none() {
        return Err(ThadamError::permanent(
            thadam_core::ErrorDomain::Config,
            codes::CONFIG_INCOMPATIBLE,
            "binding to non-loopback address requires an authentication token",
        ));
    }
    Ok(())
}

/// State shared by all admin routes.
struct AdminState {
    handle: PipelineHandle,
    metrics: Arc<PrometheusMetrics>,
    token: Option<String>,
}

/// Creates the admin router with all configured routes.
///
/// # Routes
///
/// - `GET /healthz` - Always returns 200 "ok"
/// - `GET /readyz` - Returns 200 if pipeline is ready, 503 otherwise
/// - `GET /metrics` - Returns Prometheus metrics in OpenMetrics format
/// - `GET /status` - Returns JSON status snapshot of the pipeline
///
/// # Authentication
///
/// When `token` is `Some`, all routes except `/healthz` require the header
/// `Authorization: Bearer <token>`. Missing or invalid tokens return 401.
pub fn router(
    handle: PipelineHandle,
    metrics: Arc<PrometheusMetrics>,
    token: Option<String>,
) -> Router {
    let state = Arc::new(AdminState {
        handle,
        metrics,
        token,
    });

    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route("/status", get(status_handler))
        .with_state(state)
}

/// Health check endpoint - always returns 200 "ok".
///
/// This endpoint does not require authentication.
async fn healthz() -> impl IntoResponse {
    "ok"
}

/// Readiness check endpoint.
///
/// Returns 200 if the pipeline is ready, 503 otherwise.
/// Requires authentication if a token is configured.
async fn readyz(State(state): State<Arc<AdminState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(response) = check_auth(&state.token, &headers) {
        return response.into_response();
    }

    if state.handle.is_ready() {
        (StatusCode::OK, "ok").into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

/// Metrics endpoint returning OpenMetrics format.
///
/// Returns Prometheus metrics with content type
/// `application/openmetrics-text; version=1.0.0; charset=utf-8`.
/// Requires authentication if a token is configured.
async fn metrics_handler(
    State(state): State<Arc<AdminState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(response) = check_auth(&state.token, &headers) {
        return response.into_response();
    }

    let metrics = state.metrics.encode();
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        metrics,
    )
        .into_response()
}

/// Status endpoint returning JSON snapshot of the pipeline.
///
/// Returns a comprehensive status snapshot including components,
/// cursors, WAL status, and budget information.
/// Requires authentication if a token is configured.
async fn status_handler(
    State(state): State<Arc<AdminState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(response) = check_auth(&state.token, &headers) {
        return response.into_response();
    }

    let status = state.handle.status();
    Json(status).into_response()
}

/// Checks authentication if a token is configured.
///
/// Returns Ok(()) if no token is required or if the Authorization header
/// matches the configured token. Returns 401 Unauthorized otherwise.
fn check_auth(token: &Option<String>, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if let Some(expected) = token {
        let auth_header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());

        match auth_header {
            Some(auth) if auth.starts_with("Bearer ") => {
                let provided = &auth[7..];
                if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                    Ok(())
                } else {
                    Err(Box::new(StatusCode::UNAUTHORIZED.into_response()))
                }
            }
            _ => Err(Box::new(StatusCode::UNAUTHORIZED.into_response())),
        }
    } else {
        Ok(())
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Starts the admin HTTP server.
///
/// Binds to the configured address and serves requests until the shutdown
/// signal is triggered. Validates the configuration before starting.
///
/// # Errors
///
/// Returns a [`ThadamError`] if:
/// - Configuration validation fails
/// - Binding to the address fails
/// - The server encounters an error during operation
pub async fn serve(
    cfg: AdminConfig,
    handle: PipelineHandle,
    metrics: Arc<PrometheusMetrics>,
    shutdown: thadam_core::Shutdown,
) -> Result<(), ThadamError> {
    // Validate configuration
    validate_admin_config(&cfg)?;

    // Create the router
    let app = router(handle, metrics, cfg.token);

    // Bind to the address
    let listener = TcpListener::bind(cfg.bind).await.map_err(|e| {
        ThadamError::permanent(
            thadam_core::ErrorDomain::Config,
            codes::CONFIG_INVALID,
            format!("failed to bind admin server to {}: {}", cfg.bind, e),
        )
    })?;

    // Serve with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.triggered().await;
        })
        .await
        .map_err(|e| {
            ThadamError::permanent(
                thadam_core::ErrorDomain::Sink,
                codes::SINK_IO,
                format!("admin server error: {}", e),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn validate_loopback_without_token() {
        let cfg = AdminConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9464),
            token: None,
        };
        assert!(validate_admin_config(&cfg).is_ok());
    }

    #[test]
    fn validate_non_loopback_without_token_rejected() {
        let cfg = AdminConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9464),
            token: None,
        };
        let err = validate_admin_config(&cfg).unwrap_err();
        assert_eq!(err.domain, thadam_core::ErrorDomain::Config);
        assert_eq!(err.code, codes::CONFIG_INCOMPATIBLE);
    }

    #[test]
    fn validate_non_loopback_with_token() {
        let cfg = AdminConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9464),
            token: Some("secret".to_string()),
        };
        assert!(validate_admin_config(&cfg).is_ok());
    }

    fn with_auth(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::AUTHORIZATION, value.parse().unwrap());
        headers
    }

    #[test]
    fn check_auth_requires_the_exact_bearer_token() {
        let token = Some("secret".to_string());
        assert!(check_auth(&token, &with_auth("Bearer secret")).is_ok());
        assert!(check_auth(&token, &with_auth("Bearer secre")).is_err());
        assert!(check_auth(&token, &with_auth("Bearer secrets")).is_err());
        assert!(check_auth(&token, &with_auth("secret")).is_err());
        assert!(check_auth(&token, &HeaderMap::new()).is_err());
        assert!(check_auth(&None, &HeaderMap::new()).is_ok());
    }
}
