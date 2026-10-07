//! A throwaway CA, a broker certificate for `localhost` / `127.0.0.1`, and a
//! client certificate, written as PEM files.

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    SanType,
};

pub struct Certs {
    pub dir: tempfile::TempDir,
}

fn write(dir: &Path, name: &str, pem: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, pem).expect("write pem");
    // The broker container runs as an unprivileged user.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    }
    path
}

impl Certs {
    pub fn generate() -> Certs {
        let dir = tempfile::tempdir().expect("cert dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
                .expect("chmod dir");
        }

        let ca_key = KeyPair::generate().expect("ca key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "thadam-test-ca");
        let ca = ca_params.self_signed(&ca_key).expect("ca cert");
        let issuer = Issuer::new(ca_params, ca_key);

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
            CertificateParams::new(vec!["thadam-edge".to_owned()]).expect("client params");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client = client_params
            .signed_by(&client_key, &issuer)
            .expect("client cert");

        write(dir.path(), "ca.pem", &ca.pem());
        write(dir.path(), "server.pem", &server.pem());
        write(dir.path(), "server.key", &server_key.serialize_pem());
        write(dir.path(), "client.pem", &client.pem());
        write(dir.path(), "client.key", &client_key.serialize_pem());

        // A second CA the broker does not trust, with a client cert, for the
        // negative cases.
        let other_key = KeyPair::generate().expect("other ca key");
        let mut other_params = CertificateParams::new(Vec::<String>::new()).expect("other params");
        other_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let other = other_params.self_signed(&other_key).expect("other ca");
        write(dir.path(), "other-ca.pem", &other.pem());

        Certs { dir }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
}
