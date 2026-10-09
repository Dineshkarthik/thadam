//! Server TLS. The PEM pair is checked through rustls with the `ring` provider
//! (ADR-0020) when the source opens, so a bad certificate fails `open` rather
//! than the first handshake. tonic then serves with the same pair.

use std::path::Path;
use std::sync::Arc;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{self, ServerConfig};
use tonic::transport::{Identity, ServerTlsConfig};

use crate::config::TlsConfig;
use crate::error::GrpcError;

pub(crate) fn server_tls(tls: &TlsConfig) -> Result<ServerTlsConfig, GrpcError> {
    let cert_pem = read(&tls.cert, "certificate")?;
    let key_pem = read(&tls.key, "private key")?;
    validate(&cert_pem, &key_pem, tls)?;
    Ok(ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem)))
}

fn validate(cert_pem: &[u8], key_pem: &[u8], tls: &TlsConfig) -> Result<(), GrpcError> {
    let chain = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| GrpcError::Tls(format!("{}: {e}", tls.cert.display())))?;
    if chain.is_empty() {
        return Err(GrpcError::Tls(format!(
            "{} contains no certificate",
            tls.cert.display()
        )));
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|e| GrpcError::Tls(format!("{}: {e}", tls.key.display())))?;

    ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| GrpcError::Tls(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e: rustls::Error| GrpcError::Tls(e.to_string()))?;
    Ok(())
}

fn read(path: &Path, what: &str) -> Result<Vec<u8>, GrpcError> {
    std::fs::read(path)
        .map_err(|e| GrpcError::Tls(format!("cannot read {what} {}: {e}", path.display())))
}
