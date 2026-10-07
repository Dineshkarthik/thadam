//! Brokers in Docker, driven through the `docker` CLI.
//!
//! Every container carries the label `thadam-mqtt-test` and is removed when its
//! [`Broker`] drops (including on panic), so a failed test does not leave
//! containers behind. Images are pinned: Mosquitto 2 and the last open-source
//! EMQX line, 5.8.6 (ADR-0006).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

pub const MOSQUITTO_IMAGE: &str = "eclipse-mosquitto:2";
pub const EMQX_IMAGE: &str = "emqx/emqx:5.8.6";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Mosquitto,
    Emqx,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Mosquitto => "mosquitto",
            Kind::Emqx => "emqx",
        }
    }

    /// The inflight window the broker uses when nothing configures it
    /// (verified in spike S2).
    pub fn default_window(self) -> u32 {
        match self {
            Kind::Mosquitto => 20,
            Kind::Emqx => 32,
        }
    }
}

/// PEM files mounted into the broker for the TLS listeners.
#[derive(Clone, Debug)]
pub struct TlsDir {
    pub dir: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub struct Opts {
    pub max_inflight: Option<u32>,
    pub max_queued: Option<u32>,
    /// Mosquitto only: write the session database. EMQX keeps sessions in
    /// memory regardless.
    pub persistence: bool,
    /// The broker's own limit on message size, in bytes.
    pub message_size_limit: Option<usize>,
    pub tls: Option<TlsDir>,
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

pub struct Broker {
    pub kind: Kind,
    name: String,
    pub port: u16,
    pub tls_port: Option<u16>,
    pub mtls_port: Option<u16>,
    conf: tempfile::TempDir,
    pub window: u32,
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("cannot run `docker {}`: {e}", args.join(" ")))
}

fn docker_ok(args: &[&str]) -> String {
    let out = docker(args);
    assert!(
        out.status.success(),
        "`docker {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("free port")
        .local_addr()
        .expect("addr")
        .port()
}

/// Whether `docker` can talk to a daemon.
pub fn docker_available() -> bool {
    Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .is_ok_and(|o| o.status.success())
}

/// A raw MQTT 3.1.1 CONNECT that waits for CONNACK, to know a broker really serves.
fn handshake(port: u16) -> bool {
    let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let connect = [
        0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x02, b'r',
        b'v',
    ];
    if s.write_all(&connect).is_err() {
        return false;
    }
    let mut ack = [0u8; 4];
    s.read_exact(&mut ack).is_ok() && ack[0] == 0x20 && ack[3] == 0
}

impl Broker {
    pub fn start(kind: Kind, opts: &Opts) -> Broker {
        assert!(
            docker_available(),
            "Docker is not available: run these tests with Docker Desktop running, or skip \
             them (they are #[ignore]d by default)"
        );
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("thadam-mqtt-test-{}-{n}-{}", std::process::id(), kind.name());
        let conf = tempfile::tempdir().expect("broker conf dir");
        let port = free_port();
        let window = opts.max_inflight.unwrap_or(kind.default_window());
        let (tls_port, mtls_port) = match &opts.tls {
            Some(_) => (Some(free_port()), Some(free_port())),
            None => (None, None),
        };

        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--label".into(),
            "thadam-mqtt-test=1".into(),
            "--name".into(),
            name.clone(),
            "-p".into(),
            format!("127.0.0.1:{port}:1883"),
        ];
        if let Some(p) = tls_port {
            args.extend(["-p".into(), format!("127.0.0.1:{p}:8883")]);
        }
        if let Some(p) = mtls_port {
            args.extend(["-p".into(), format!("127.0.0.1:{p}:8884")]);
        }
        if let Some(tls) = &opts.tls {
            let dir = std::fs::canonicalize(&tls.dir).expect("tls dir");
            args.extend(["-v".into(), format!("{}:/certs:ro", dir.display())]);
        }

        match kind {
            Kind::Mosquitto => {
                let conf_path = conf.path().join("mosquitto.conf");
                std::fs::write(&conf_path, mosquitto_conf(opts, window)).expect("write conf");
                let conf_dir = std::fs::canonicalize(conf.path()).expect("conf dir");
                args.extend([
                    "-v".into(),
                    format!("{}:/mosquitto/config:ro", conf_dir.display()),
                    MOSQUITTO_IMAGE.into(),
                ]);
            }
            Kind::Emqx => {
                for (key, value) in emqx_env(opts, window) {
                    args.extend(["-e".into(), format!("{key}={value}")]);
                }
                args.push(EMQX_IMAGE.into());
            }
        }
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        docker_ok(&refs);

        let broker = Broker {
            kind,
            name,
            port,
            tls_port,
            mtls_port,
            conf,
            window,
        };
        broker.wait_ready();
        broker
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !handshake(self.port) {
            assert!(
                Instant::now() < deadline,
                "{} did not become ready:\n{}",
                self.kind.name(),
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(250));
        }
        if self.kind == Kind::Emqx {
            // The listener opens before the node finishes starting.
            for _ in 0..60 {
                let status = docker(&["exec", &self.name, "emqx", "ctl", "status"]);
                if String::from_utf8_lossy(&status.stdout).contains("is started") {
                    break;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }

    /// `host:port` of the plain listener.
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn url(&self) -> String {
        format!("mqtt://{}", self.addr())
    }

    pub fn restart(&self) {
        docker_ok(&["restart", "-t", "2", &self.name]);
        self.wait_ready();
    }

    pub fn logs(&self) -> String {
        let out = docker(&["logs", "--tail", "60", &self.name]);
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }

    pub fn conf_dir(&self) -> &Path {
        self.conf.path()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

fn mosquitto_conf(opts: &Opts, window: u32) -> String {
    let mut c = String::new();
    c.push_str("listener 1883\nallow_anonymous true\n");
    c.push_str(&format!("max_inflight_messages {window}\n"));
    c.push_str(&format!(
        "max_queued_messages {}\n",
        opts.max_queued.unwrap_or(100_000)
    ));
    if opts.persistence {
        c.push_str(
            "persistence true\npersistence_location /mosquitto/data/\nautosave_interval 1\n",
        );
    } else {
        c.push_str("persistence false\n");
    }
    if let Some(limit) = opts.message_size_limit {
        c.push_str(&format!("message_size_limit {limit}\n"));
    }
    c.push_str("log_type error\nlog_type warning\nlog_type notice\n");
    if opts.tls.is_some() {
        let tls = "cafile /certs/ca.pem\ncertfile /certs/server.pem\nkeyfile /certs/server.key\n";
        c.push_str(&format!(
            "listener 8883\nallow_anonymous true\n{tls}require_certificate false\n"
        ));
        c.push_str(&format!(
            "listener 8884\nallow_anonymous true\n{tls}require_certificate true\n"
        ));
    }
    c
}

fn emqx_env(opts: &Opts, window: u32) -> Vec<(String, String)> {
    let mut env = vec![
        ("EMQX_MQTT__MAX_INFLIGHT".to_owned(), window.to_string()),
        (
            "EMQX_MQTT__MAX_MQUEUE_LEN".to_owned(),
            opts.max_queued.unwrap_or(100_000).to_string(),
        ),
        ("EMQX_LOG__CONSOLE__LEVEL".to_owned(), "warning".to_owned()),
        // The default (1000) killed the session in spike S2 once the window
        // approached it.
        (
            "EMQX_FORCE_SHUTDOWN__MAX_MAILBOX_SIZE".to_owned(),
            "100000".to_owned(),
        ),
    ];
    if let Some(limit) = opts.message_size_limit {
        env.push(("EMQX_MQTT__MAX_PACKET_SIZE".to_owned(), limit.to_string()));
    }
    if opts.tls.is_some() {
        let ssl = |listener: &str, key: &str, value: &str| {
            (
                format!("EMQX_LISTENERS__SSL__{listener}__{key}"),
                value.to_owned(),
            )
        };
        for (listener, bind, verify, fail) in [
            ("DEFAULT", "0.0.0.0:8883", "verify_none", "false"),
            ("MTLS", "0.0.0.0:8884", "verify_peer", "true"),
        ] {
            env.push(ssl(listener, "BIND", bind));
            env.push(ssl(listener, "SSL_OPTIONS__CACERTFILE", "/certs/ca.pem"));
            env.push(ssl(listener, "SSL_OPTIONS__CERTFILE", "/certs/server.pem"));
            env.push(ssl(listener, "SSL_OPTIONS__KEYFILE", "/certs/server.key"));
            env.push(ssl(listener, "SSL_OPTIONS__VERIFY", verify));
            env.push(ssl(listener, "SSL_OPTIONS__FAIL_IF_NO_PEER_CERT", fail));
        }
    }
    env
}

/// Removes every container this test run could have left behind (a killed
/// test process cannot run `Drop`). Safe to call from a cleanup script.
#[allow(dead_code)]
pub fn remove_leftovers() {
    let ids = docker_ok(&["ps", "-aq", "--filter", "label=thadam-mqtt-test"]);
    for id in ids.split_whitespace() {
        let _ = docker(&["rm", "-f", id]);
    }
}
