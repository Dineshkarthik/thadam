//! Server-side TLS through rustls with the `ring` provider (ADR-0020).

use std::sync::Arc;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{self, ServerConfig};

use crate::config::TlsConfig;
use crate::error::HttpError;

/// Builds the acceptor for a listen endpoint from PEM files.
pub(crate) fn server_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor, HttpError> {
    let cert_pem = read(&tls.cert, "certificate")?;
    let key_pem = read(&tls.key, "private key")?;

    let chain = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| HttpError::Tls(format!("{}: {e}", tls.cert.display())))?;
    if chain.is_empty() {
        return Err(HttpError::Tls(format!(
            "{} contains no certificate",
            tls.cert.display()
        )));
    }
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|e| HttpError::Tls(format!("{}: {e}", tls.key.display())))?;

    let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| HttpError::Tls(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e: rustls::Error| HttpError::Tls(e.to_string()))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn read(path: &std::path::Path, what: &str) -> Result<Vec<u8>, HttpError> {
    std::fs::read(path)
        .map_err(|e| HttpError::Tls(format!("cannot read {what} {}: {e}", path.display())))
}
