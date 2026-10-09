//! The Mosquitto broker, run as the public `eclipse-mosquitto:2` image with the
//! demo's `mosquitto.conf`. The container is labelled and removed on drop.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::error::{BenchError, Result};
use crate::process::free_port;

/// Pinned to the same line the demo and the simulator's e2e test use.
pub const DEFAULT_IMAGE: &str = "eclipse-mosquitto:2";
/// The label every bench container carries, for manual cleanup.
pub const LABEL: &str = "thadam-bench";
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// A running broker container bound to a loopback port.
pub struct Broker {
    name: String,
    port: u16,
}

impl Broker {
    /// Starts the broker with `config` mounted read-only and waits until its
    /// port accepts TCP connections.
    ///
    /// The image defaults to [`DEFAULT_IMAGE`] and can be replaced with
    /// `THADAM_BENCH_MQTT_IMAGE`.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Run`] when Docker refuses the container or the
    /// port never opens.
    pub fn start(config: &Path) -> Result<Broker> {
        let port = free_port()?;
        let name = format!("thadam-bench-{}-{port}", std::process::id());
        let image =
            std::env::var("THADAM_BENCH_MQTT_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_owned());
        let mount = format!("{}:/mosquitto/config/mosquitto.conf:ro", config.display());
        let publish = format!("127.0.0.1:{port}:1883");
        let out = Command::new("docker")
            .args([
                "run", "-d", "--name", &name, "--label", LABEL, "-p", &publish, "-v", &mount,
                &image,
            ])
            .output()
            .map_err(|e| BenchError::Run(format!("docker: {e}")))?;
        if !out.status.success() {
            return Err(BenchError::Run(format!(
                "docker run {image} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let broker = Broker { name, port };
        broker.wait_ready()?;
        Ok(broker)
    }

    /// The host port the broker is published on.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    fn wait_ready(&self) -> Result<()> {
        let started = Instant::now();
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return Ok(());
            }
            if started.elapsed() >= READY_TIMEOUT {
                return Err(BenchError::Run(format!(
                    "mosquitto on port {} did not accept connections within {}",
                    self.port,
                    humantime::format_duration(READY_TIMEOUT)
                )));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", "-v", &self.name])
            .output();
    }
}
