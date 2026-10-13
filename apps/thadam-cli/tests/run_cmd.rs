//! Tests for the `thadam run` command.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/thadam-config/tests/fixtures")
        .join(name)
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("exited normally")
}

#[test]
fn run_help_lists_flags() {
    let out = thadam(&["run", "--help"]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("thadam run"), "{stdout}");
    assert!(stdout.contains("--repair"), "{stdout}");
    assert!(stdout.contains("--log-format"), "{stdout}");
    assert!(stdout.contains("--log-level"), "{stdout}");
    assert!(stdout.contains("--admin-bind"), "{stdout}");
    assert!(stdout.contains("--admin-token-file"), "{stdout}");
    assert!(stdout.contains("--exit-when-sources-finish"), "{stdout}");
}

#[test]
fn run_nonexistent_file_exits_2() {
    let out = thadam(&["run", "/no/such/thadam.yaml"]);
    assert_eq!(
        code(&out),
        2,
        "stdout: {}\nstderr: {}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("cannot read") || stderr.contains("No such file"),
        "{stderr}"
    );
}

#[test]
fn run_invalid_config_exits_2() {
    let path = fixture("invalid/multi_error.yaml");
    let out = thadam(&["run", path.to_str().unwrap()]);
    assert_eq!(
        code(&out),
        2,
        "stdout: {}\nstderr: {}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("error") || stderr.contains("invalid"),
        "{stderr}"
    );
}

/// A config that loads but cannot build reports its errors on stderr via
/// `eprintln!`, not the async tracing logger. The async logger may drop lines
/// at exit, so structured logging is not asserted here.
#[test]
fn run_logs_pipeline_build_errors_to_stderr() {
    let dir = std::env::temp_dir().join(format!(
        "thadam-cli-logging-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("pipeline.yaml");
    let yaml = r#"version: 1
pipeline: { name: logtest, node_id: n1, data_dir: DATA_DIR }
sources:
  - { id: src, kind: nosuchkind }
wal: { max_disk: 1GiB, on_full: block }
datasets:
  - id: d
    match: { source: src }
    decode: json
    fields:
      - { id: 1000, name: n, type: u64, from: /n, required: true }
sinks:
  - { id: out, kind: alsomissing }
"#
    .replace("DATA_DIR", &dir.join("data").display().to_string());
    std::fs::write(&config, yaml).unwrap();

    let out = thadam(&["run", config.to_str().unwrap()]);
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(code(&out), 2, "stderr: {}", text(&out.stderr));
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("error: pipeline build failed: "),
        "build errors are printed to stderr:\n{stderr}"
    );
}

/// Long-lived `run` exercises: spawn the binary, watch its admin API and its
/// piped logs, stop it with SIGTERM. Unix-only for `kill -TERM`.
#[cfg(unix)]
mod supervised {
    use super::*;
    use std::io::Read;
    use std::process::{ExitStatus, Stdio};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn two_ports() -> (u16, u16) {
        let first = free_port();
        let mut second = free_port();
        while second == first {
            second = free_port();
        }
        (first, second)
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A minimal pipeline that runs until signalled: one HTTP source into one
    /// local Parquet sink. The admin API is configured only through
    /// `observability.admin`, never through CLI flags.
    fn write_config(dir: &Path, http_port: u16, admin_port: u16) -> PathBuf {
        let config = dir.join("pipeline.yaml");
        let yaml = format!(
            r#"version: 1
pipeline: {{ name: runlog, node_id: n1, data_dir: "{data}" }}
sources:
  - id: src
    kind: http
    config:
      listen: {{ bind: "127.0.0.1:{http_port}" }}
wal: {{ max_disk: 256MiB, on_full: block }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
sinks:
  - id: out
    kind: parquet
    config: {{ datasets: [d] }}
observability:
  admin: {{ bind: "127.0.0.1:{admin_port}" }}
"#,
            data = dir.join("data").display(),
        );
        std::fs::write(&config, yaml).unwrap();
        config
    }

    /// Spawn `thadam run` with piped stderr (as under `docker logs`) and a
    /// reader thread streaming it into a shared buffer.
    fn spawn_run(
        config: &Path,
    ) -> (
        std::process::Child,
        Arc<Mutex<String>>,
        std::thread::JoinHandle<()>,
    ) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_thadam"))
            .args(["run", config.to_str().unwrap()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn thadam run");
        let mut pipe = child.stderr.take().unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let sink = Arc::clone(&captured);
        let reader = std::thread::spawn(move || {
            loop {
                let mut chunk = [0u8; 4096];
                match pipe.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => sink
                        .lock()
                        .unwrap()
                        .push_str(&String::from_utf8_lossy(&chunk[..n])),
                }
            }
        });
        (child, captured, reader)
    }

    fn wait_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }

    fn sigterm(child: &mut std::process::Child) {
        if child.try_wait().unwrap().is_none() {
            let status = Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .expect("kill -TERM");
            assert!(status.success(), "kill -TERM: {status}");
        }
    }

    fn wait_exit(child: &mut std::process::Child, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("thadam run did not exit within {timeout:?}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn healthz_ok(port: u16) -> bool {
        use std::io::Write as _;
        let Some(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)).ok() else {
            return false;
        };
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        if stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return false;
        }
        let mut buf = Vec::new();
        // `Connection: close` ends the response at EOF; a read timeout still
        // leaves any complete response in the buffer.
        let _ = stream.read_to_end(&mut buf);
        let text = String::from_utf8_lossy(&buf);
        text.starts_with("HTTP/1.1 200") && text.trim_end().ends_with("ok")
    }

    /// `observability.admin` alone (no `--admin-bind`) must start the admin
    /// API: the config section is the documented home of the bind and token.
    #[test]
    fn run_serves_admin_api_from_config_section() {
        let dir = test_dir("thadam-cli-admin");
        let (http_port, admin_port) = two_ports();
        let config = write_config(&dir, http_port, admin_port);
        let (mut child, captured, reader) = spawn_run(&config);

        let served = wait_until(|| healthz_ok(admin_port), Duration::from_secs(30));
        sigterm(&mut child);
        let status = wait_exit(&mut child, Duration::from_secs(45));
        reader.join().unwrap();
        let logs = captured.lock().unwrap().clone();
        let _ = std::fs::remove_dir_all(&dir);

        assert!(
            served,
            "admin API never answered on 127.0.0.1:{admin_port}:\n{logs}"
        );
        assert_eq!(status.code(), Some(0), "logs:\n{logs}");
    }

    /// With stderr piped (no TTY, as in `docker logs`), runtime lines must
    /// appear while the pipeline still runs, and the shutdown lines must be
    /// flushed before the process exits.
    #[test]
    fn run_logs_promptly_when_piped_and_flushes_on_sigterm() {
        let dir = test_dir("thadam-cli-logs");
        let (http_port, admin_port) = two_ports();
        let config = write_config(&dir, http_port, admin_port);
        let (mut child, captured, reader) = spawn_run(&config);

        let prompt = wait_until(
            || captured.lock().unwrap().contains("pipeline running"),
            Duration::from_secs(30),
        );
        sigterm(&mut child);
        let status = wait_exit(&mut child, Duration::from_secs(45));
        reader.join().unwrap();
        let logs = captured.lock().unwrap().clone();
        let _ = std::fs::remove_dir_all(&dir);

        assert!(
            prompt,
            "no log lines while running with piped stderr:\n{logs}"
        );
        assert_eq!(status.code(), Some(0), "logs:\n{logs}");
        assert!(logs.contains("received SIGTERM"), "logs:\n{logs}");
        assert!(
            logs.contains("pipeline stopped"),
            "shutdown lines were not flushed before exit:\n{logs}"
        );
    }
}
