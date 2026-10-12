//! A throwaway PKI for the TLS and SASL broker tests: a CA, a broker
//! certificate for `localhost` and `127.0.0.1`, a client certificate the CA
//! signed, and a second CA that no broker trusts. Everything is PEM text.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    SanType,
};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

#[derive(Clone, Debug)]
pub struct Pki {
    pub ca_pem: String,
    pub other_ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
}

fn ca_params(name: &str) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params
}

impl Pki {
    pub fn generate() -> Pki {
        let ca_key = KeyPair::generate().expect("ca key");
        let root = ca_params("thadam-kafka-test-ca");
        let ca = root.self_signed(&ca_key).expect("ca cert");
        let issuer = Issuer::new(root, ca_key);

        let server_key = KeyPair::generate().expect("server key");
        let mut server_params =
            CertificateParams::new(vec!["localhost".to_owned()]).expect("server params");
        server_params
            .subject_alt_names
            .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server = server_params
            .signed_by(&server_key, &issuer)
            .expect("server cert");

        let client_key = KeyPair::generate().expect("client key");
        let mut client_params =
            CertificateParams::new(vec!["thadam-kafka-client".to_owned()]).expect("client params");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client = client_params
            .signed_by(&client_key, &issuer)
            .expect("client cert");

        let other_key = KeyPair::generate().expect("other ca key");
        let other = ca_params("thadam-kafka-untrusted-ca")
            .self_signed(&other_key)
            .expect("other ca");

        Pki {
            ca_pem: ca.pem(),
            other_ca_pem: other.pem(),
            server_cert_pem: server.pem(),
            server_key_pem: server_key.serialize_pem(),
            client_cert_pem: client.pem(),
            client_key_pem: client_key.serialize_pem(),
        }
    }

    /// Writes `ca.pem`, `other-ca.pem`, `client.pem`, and `client.key` into
    /// `dir`, the files a sink's `tls` block names.
    pub fn write_files(&self, dir: &Path) {
        std::fs::write(dir.join("ca.pem"), &self.ca_pem).expect("write ca");
        std::fs::write(dir.join("other-ca.pem"), &self.other_ca_pem).expect("write other ca");
        std::fs::write(dir.join("client.pem"), &self.client_cert_pem).expect("write client");
        std::fs::write(dir.join("client.key"), &self.client_key_pem).expect("write key");
    }

    /// A rustls client configuration that trusts this CA, presenting the
    /// client certificate when `client_identity` is set.
    pub fn client_config(&self, client_identity: bool) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(self.ca_pem.as_bytes()) {
            roots.add(cert.expect("ca pem")).expect("trust ca");
        }
        let builder = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_root_certificates(roots);
        if !client_identity {
            return builder.with_no_client_auth();
        }
        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(self.client_cert_pem.as_bytes())
                .collect::<Result<_, _>>()
                .expect("client cert pem");
        let key =
            PrivateKeyDer::from_pem_slice(self.client_key_pem.as_bytes()).expect("client key pem");
        builder
            .with_client_auth_cert(chain, key)
            .expect("client identity")
    }
}
