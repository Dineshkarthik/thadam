//! Certificate and key material: checks run at config time, and the trust
//! store entries the client installs before it connects.
//!
//! The client's PKI layout is `async-opcua`'s own: `<pki_dir>/trusted` holds
//! server certificates the client accepts, `<pki_dir>/rejected` holds the
//! ones it refused so an operator can review them. A certificate is trusted
//! only when a file with its exact DER bytes sits in `trusted/`, named by
//! `CertificateStore::cert_file_name`.

use std::fs;
use std::path::{Path, PathBuf};

use opcua::crypto::{CertificateStore, KeySize, X509};

/// Signed by the key and checked against the certificate's public key, so
/// a mismatched pair fails before any connection is attempted.
const KEY_CHECK_MESSAGE: &[u8] = b"thadam opcua key pair check";

/// Reads a DER or PEM certificate, naming the path on failure.
pub(crate) fn read_certificate(path: &Path) -> Result<X509, String> {
    CertificateStore::read_cert(path)
        .map_err(|e| format!("cannot read certificate {}: {e}", path.display()))
}

/// Checks that `key` is the private key of `cert`. The error names the
/// files but never their contents.
pub(crate) fn check_key_pair(cert: &Path, key: &Path) -> Result<(), String> {
    let x509 = read_certificate(cert)?;
    let private = CertificateStore::read_pkey(key)
        .map_err(|e| format!("cannot read private key {}: {e}", key.display()))?;
    let public = x509.public_key().map_err(|e| {
        format!(
            "certificate {} does not carry an RSA public key: {e}",
            cert.display()
        )
    })?;
    let mut signature = vec![0u8; private.size()];
    let written = private
        .sign_sha256(KEY_CHECK_MESSAGE, &mut signature)
        .map_err(|e| format!("cannot use private key {}: {e}", key.display()))?;
    let matches = public
        .verify_sha256(KEY_CHECK_MESSAGE, &signature[..written])
        .map_err(|e| format!("cannot check private key {}: {e}", key.display()))?;
    if matches {
        Ok(())
    } else {
        Err(format!(
            "private key {} does not match certificate {}",
            key.display(),
            cert.display()
        ))
    }
}

/// Copies each certificate into `<pki_dir>/trusted` under the name the
/// client looks it up by. An explicit entry overrides an earlier rejection
/// of the same certificate, which is removed from `rejected/`.
pub(crate) fn install_trusted(pki_dir: &Path, certs: &[PathBuf]) -> Result<(), String> {
    if certs.is_empty() {
        return Ok(());
    }
    let trusted = pki_dir.join("trusted");
    let rejected = pki_dir.join("rejected");
    fs::create_dir_all(&trusted)
        .map_err(|e| format!("cannot create {}: {e}", trusted.display()))?;
    for path in certs {
        let cert = read_certificate(path)?;
        let name = CertificateStore::cert_file_name(&cert);
        let der = cert
            .to_der()
            .map_err(|e| format!("cannot encode certificate {}: {e}", path.display()))?;
        let target = trusted.join(&name);
        if fs::read(&target).ok().as_deref() != Some(der.as_slice()) {
            fs::write(&target, &der)
                .map_err(|e| format!("cannot write {}: {e}", target.display()))?;
        }
        let _ = fs::remove_file(rejected.join(&name));
    }
    Ok(())
}
