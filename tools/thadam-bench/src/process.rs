//! Child processes and the small probes the sampler uses: HTTP status and
//! body via `curl`, and CPU and RSS via `ps`, which behave the same on macOS
//! and Linux.

use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::error::{BenchError, Result};

/// A spawned child with its output redirected to a log file. Killed and reaped
/// on drop, so a failed run never leaves `thadam` or the simulator behind.
pub struct Proc {
    child: Child,
    log: PathBuf,
}

impl Proc {
    /// Spawns `cmd` with stdout and stderr appended to `log`.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Io`] when the log cannot be created or the child
    /// cannot start.
    pub fn spawn(mut cmd: Command, log: &Path) -> Result<Proc> {
        let out = File::create(log)?;
        let err = out.try_clone()?;
        cmd.stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        let child = cmd.spawn()?;
        Ok(Proc {
            child,
            log: log.to_path_buf(),
        })
    }

    /// The OS process id.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The log file this process writes to.
    #[must_use]
    pub fn log(&self) -> &Path {
        &self.log
    }

    /// The exit status when the process has ended, without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Io`] when the status cannot be queried.
    pub fn try_exit(&mut self) -> Result<Option<ExitStatus>> {
        Ok(self.child.try_wait()?)
    }

    /// Sends SIGKILL and reaps the process.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Io`] when the signal or wait fails.
    pub fn kill_hard(&mut self) -> Result<ExitStatus> {
        self.child.kill()?;
        Ok(self.child.wait()?)
    }

    /// Sends SIGTERM (graceful shutdown) and waits up to `within`.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Run`] when the signal fails or the process is
    /// still running at the deadline; the process is then killed.
    pub fn terminate(&mut self, within: Duration) -> Result<ExitStatus> {
        let pid = self.pid().to_string();
        let sent = Command::new("kill").args(["-TERM", &pid]).status()?;
        if !sent.success() {
            return Err(BenchError::Run(format!("kill -TERM {pid} failed")));
        }
        let started = Instant::now();
        loop {
            if let Some(status) = self.try_exit()? {
                return Ok(status);
            }
            if started.elapsed() >= within {
                let _ = self.kill_hard();
                return Err(BenchError::Run(format!(
                    "pid {pid} did not exit within {}; killed. log: {}",
                    humantime::format_duration(within),
                    self.log.display()
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The last `lines` lines of the log, for error messages.
    #[must_use]
    pub fn log_tail(&self, lines: usize) -> String {
        log_tail(&self.log, lines)
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The last `lines` lines of a file, or an empty string if it cannot be read.
#[must_use]
pub fn log_tail(path: &Path, lines: usize) -> String {
    let mut buf = String::new();
    if let Ok(mut f) = File::open(path) {
        let _ = f.read_to_string(&mut buf);
    }
    let all: Vec<&str> = buf.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Fetches `url` with `curl` and returns the body when the status is 2xx.
#[must_use]
pub fn http_get_ok(url: &str) -> Option<String> {
    let out = Command::new("curl")
        .args(["-sf", "--max-time", "3", url])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Whether `url` answers with a 2xx status, e.g. `/readyz`.
#[must_use]
pub fn is_ok(url: &str) -> bool {
    http_get_ok(url).is_some()
}

/// One `ps` reading for a process: `%cpu` as `ps` reports it, and RSS in KiB.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PsSample {
    /// Percent of one core, as `ps -o %cpu` reports it.
    pub cpu_percent: f64,
    /// Resident set size in kibibytes.
    pub rss_kib: f64,
}

/// Samples `pid` with `ps -o %cpu=,rss= -p <pid>`. `None` when the process is
/// gone or `ps` printed nothing usable.
#[must_use]
pub fn ps_sample(pid: u32) -> Option<PsSample> {
    let out = Command::new("ps")
        .args(["-o", "%cpu=,rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_ps(&String::from_utf8_lossy(&out.stdout))
}

/// Parses one `ps` line: `"  12.5  34567"`.
#[must_use]
pub fn parse_ps(text: &str) -> Option<PsSample> {
    let mut fields = text.split_whitespace();
    let cpu_percent = fields.next()?.parse().ok()?;
    let rss_kib = fields.next()?.parse().ok()?;
    Some(PsSample {
        cpu_percent,
        rss_kib,
    })
}

/// A free loopback TCP port. There is a small race between releasing it and
/// the child binding it; the run fails loudly if that happens.
///
/// # Errors
///
/// Returns [`BenchError::Io`] when no port can be bound.
pub fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn ps_output_parses_with_padding() {
        assert_eq!(
            parse_ps("  12.5  34567\n"),
            Some(PsSample {
                cpu_percent: 12.5,
                rss_kib: 34567.0
            })
        );
        assert_eq!(parse_ps(""), None);
        assert_eq!(parse_ps("abc 1"), None);
        assert_eq!(parse_ps("1.0"), None);
    }

    #[test]
    fn log_tail_keeps_the_last_lines() {
        let dir = std::env::temp_dir().join(format!("thadam-bench-tail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("log.txt");
        std::fs::write(&path, "a\nb\nc\nd\n").expect("write");
        assert_eq!(log_tail(&path, 2), "c\nd");
        assert_eq!(log_tail(&dir.join("missing"), 2), "");
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
