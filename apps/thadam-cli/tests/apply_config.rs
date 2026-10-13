//! `deploy/apply-config.sh` end to end against a real `thadam run`. The restart
//! command is a shell stand-in for `systemctl restart`: it stops the running
//! process with SIGTERM and starts the new one on the live file. Unix only,
//! because the script and the stand-in are POSIX shell.

#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use thadam_testkit::TempDataDir;
use serde_json::{Value, json};

const RESTART_SH: &str = r#"#!/bin/sh
# $1 is how a candidate starts: `start` runs it, `fail` exits 1, `silent` starts
# nothing and exits 0. Any other configuration starts normally.
echo restart >> "$THADAM_TEST_STATE/restarts"
if [ -f "$THADAM_TEST_STATE/pid" ]; then
  old=$(cat "$THADAM_TEST_STATE/pid")
  kill -TERM "$old" 2>/dev/null
  waited=0
  while kill -0 "$old" 2>/dev/null && [ "$waited" -lt 200 ]; do
    sleep 0.05
    waited=$((waited + 1))
  done
  if kill -0 "$old" 2>/dev/null; then kill -KILL "$old"; fi
fi
if [ "$1" != start ] && grep -q '^# candidate' "$THADAM_TEST_LIVE"; then
  [ "$1" = fail ] && exit 1
  exit 0
fi
"$THADAM_TEST_BIN" run "$THADAM_TEST_LIVE" </dev/null >>"$THADAM_TEST_STATE/thadam.log" 2>&1 &
echo $! > "$THADAM_TEST_STATE/pid"
"#;

const EXTRA_FIELD: &str = "      - { id: 1001, name: site, type: str, from: /site }\n";

fn thadam(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(args)
        .output()
        .expect("run thadam")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("exited normally")
}

fn apply_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/apply-config.sh")
}

fn free_ports() -> (u16, u16) {
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    (
        a.local_addr().unwrap().port(),
        b.local_addr().unwrap().port(),
    )
}

/// The hash `thadam validate` prints for a configuration file.
fn config_hash_of(path: &Path) -> String {
    let out = thadam(&["validate", path.to_str().unwrap()]);
    assert_eq!(code(&out), 0, "{}", text(&out.stderr));
    text(&out.stdout)
        .lines()
        .find_map(|l| l.trim().strip_prefix("config hash: "))
        .and_then(|rest| rest.split_whitespace().next())
        .expect("validate prints a config hash")
        .to_owned()
}

fn wait_for(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    false
}

/// One node: a data directory, the live file, and the `thadam run` process the
/// stand-in starts. The process is killed on drop.
struct Node {
    dir: TempDataDir,
    http: u16,
    admin: u16,
}

impl Node {
    fn new() -> Self {
        let dir = TempDataDir::new().unwrap();
        std::fs::write(dir.join("restart.sh"), RESTART_SH).unwrap();
        let (http, admin) = free_ports();
        Node { dir, http, admin }
    }

    fn live(&self) -> PathBuf {
        self.dir.join("pipeline.yaml")
    }

    fn prev(&self) -> PathBuf {
        self.dir.join("pipeline.yaml.prev")
    }

    fn new_file(&self) -> PathBuf {
        self.dir.join("pipeline.yaml.new")
    }

    fn admin_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin)
    }

    fn config(&self, name: &str, extra_field: &str) -> String {
        format!(
            r#"version: 1
pipeline: {{ name: {name}, node_id: n1, data_dir: "{data}" }}
sources:
  - id: src
    kind: http
    config:
      listen: {{ bind: "127.0.0.1:{http}" }}
wal: {{ max_disk: 256MiB, on_full: block }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
{extra_field}sinks:
  - id: out
    kind: parquet
    config: {{ datasets: [d] }}
observability:
  admin: {{ bind: "127.0.0.1:{admin}" }}
"#,
            data = self.dir.join("data").display(),
            http = self.http,
            admin = self.admin,
        )
    }

    /// A candidate file. The marker line tells the stand-in it is not the live
    /// configuration; comments do not change the config hash.
    fn candidate(&self, yaml: &str) -> PathBuf {
        let path = self.dir.join("candidate.yaml");
        std::fs::write(&path, format!("# candidate\n{yaml}")).unwrap();
        path
    }

    fn pid(&self) -> Option<u32> {
        std::fs::read_to_string(self.dir.join("pid"))
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    fn restarts(&self) -> usize {
        std::fs::read_to_string(self.dir.join("restarts")).map_or(0, |s| s.lines().count())
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("thadam.log")).unwrap_or_default()
    }

    fn status(&self) -> Option<Value> {
        let out = thadam(&["status", "--admin-url", &self.admin_url()]);
        out.status
            .success()
            .then(|| serde_json::from_slice(&out.stdout).expect("status JSON"))
    }

    /// Waits until `/status` reports `phase: running`, `ready: true`, and `hash`.
    fn wait_running(&self, hash: &str) -> Value {
        let mut last = None;
        let up = wait_for(Duration::from_secs(30), || {
            last = self.status();
            last.as_ref().is_some_and(|s| {
                s["phase"] == "running" && s["ready"] == true && s["config_hash"] == hash
            })
        });
        assert!(
            up,
            "node never reported config {hash}; last status: {last:?}\nlog:\n{}",
            self.log()
        );
        last.unwrap()
    }

    /// The restart stand-in, run as the host would run `systemctl restart`.
    fn stub(&self, mode: &str) -> Command {
        let mut cmd = Command::new("sh");
        cmd.arg(self.dir.join("restart.sh"))
            .arg(mode)
            .env("THADAM_TEST_BIN", env!("CARGO_BIN_EXE_thadam"))
            .env("THADAM_TEST_LIVE", self.live())
            .env("THADAM_TEST_STATE", self.dir.path());
        cmd
    }

    /// Writes `yaml` as the live file and starts it, as a host does at boot.
    fn start(&self, yaml: &str) -> String {
        std::fs::write(self.live(), yaml).unwrap();
        let hash = config_hash_of(&self.live());
        assert!(self.stub("start").status().unwrap().success());
        self.wait_running(&hash);
        hash
    }

    /// The apply script for `candidate`, with the restart stand-in in `mode`
    /// (`start`, `fail`, or `silent`) for the candidate.
    fn apply(&self, candidate: &Path, mode: &str, ready_timeout_secs: u32) -> Command {
        let mut cmd = Command::new(apply_script());
        cmd.arg(candidate)
            .arg(self.live())
            .env("THADAM_BIN", env!("CARGO_BIN_EXE_thadam"))
            .env(
                "THADAM_RESTART_CMD",
                format!("sh \"$THADAM_TEST_STATE/restart.sh\" {mode}"),
            )
            .env("THADAM_ADMIN_URL", self.admin_url())
            .env("THADAM_READY_TIMEOUT_SECS", ready_timeout_secs.to_string())
            .env("THADAM_TEST_BIN", env!("CARGO_BIN_EXE_thadam"))
            .env("THADAM_TEST_LIVE", self.live())
            .env("THADAM_TEST_STATE", self.dir.path())
            .env_remove("THADAM_ADMIN_TOKEN_FILE");
        cmd
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(pid) = self.pid() {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .output();
        }
    }
}

fn assert_rolled_back(node: &Node, old_yaml: &str, old_hash: &str, out: &Output) {
    let detail = format!(
        "stdout: {}\nstderr: {}\nlog:\n{}",
        text(&out.stdout),
        text(&out.stderr),
        node.log()
    );
    assert_eq!(code(out), 1, "{detail}");
    assert_eq!(
        std::fs::read_to_string(node.live()).unwrap(),
        old_yaml,
        "{detail}"
    );
    assert!(!node.prev().exists(), "rollback moves .prev back: {detail}");
    assert!(!node.new_file().exists(), "{detail}");
    assert_eq!(node.restarts(), 3, "start, candidate, rollback: {detail}");
    node.wait_running(old_hash);
}

#[test]
fn a_valid_change_is_applied_and_the_node_comes_back_on_the_new_hash() {
    let node = Node::new();
    let old_yaml = node.config("apply-demo", "");
    let old_hash = node.start(&old_yaml);
    let candidate = node.candidate(&node.config("apply-demo", EXTRA_FIELD));
    let new_hash = config_hash_of(&candidate);
    assert_ne!(old_hash, new_hash);

    let apply_began = Instant::now();
    let apply = node
        .apply(&candidate, "start", 30)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    assert!(
        wait_for(Duration::from_secs(30), || node.restarts() == 2),
        "the restart command never ran"
    );
    // shortcut: the restart is timed by polling `thadam status` as a process, so each reading includes a spawn (a few ms); upgrade to a direct HTTP poll before quoting the figure to the millisecond.
    let restart_began = Instant::now();
    let status = node.wait_running(&new_hash);
    let restart = restart_began.elapsed();
    let out = apply.wait_with_output().unwrap();
    let apply_total = apply_began.elapsed();
    let detail = format!(
        "stdout: {}\nstderr: {}\nlog:\n{}",
        text(&out.stdout),
        text(&out.stderr),
        node.log()
    );
    assert_eq!(code(&out), 0, "{detail}");

    assert_eq!(status["config_hash"], json!(new_hash));
    assert_eq!(status["previous_config_hash"], json!(old_hash));
    assert_eq!(std::fs::read_to_string(node.prev()).unwrap(), old_yaml);
    assert_eq!(
        std::fs::read_to_string(node.live()).unwrap(),
        std::fs::read_to_string(&candidate).unwrap()
    );
    assert!(!node.new_file().exists());
    assert_eq!(node.restarts(), 2);

    println!(
        "restart (restart command to /status ready on {new_hash}): {} ms; apply total: {} ms",
        restart.as_millis(),
        apply_total.as_millis()
    );
}

#[test]
fn a_candidate_the_data_directory_refuses_leaves_the_node_alone() {
    let node = Node::new();
    let old_yaml = node.config("apply-demo", "");
    let old_hash = node.start(&old_yaml);
    let pid = node.pid();
    let candidate = node.candidate(&node.config("renamed", ""));

    let out = node.apply(&candidate, "start", 30).output().unwrap();
    let stderr = text(&out.stderr);
    assert_eq!(code(&out), 3, "stderr: {stderr}");
    assert!(stderr.contains("candidate rejected"), "{stderr}");
    assert!(stderr.contains("apply-demo"), "{stderr}");
    assert_eq!(std::fs::read_to_string(node.live()).unwrap(), old_yaml);
    assert!(!node.prev().exists());
    assert!(!node.new_file().exists());
    assert_eq!(node.restarts(), 1, "the restart command must not run");
    assert_eq!(node.pid(), pid);
    assert_eq!(
        node.status().expect("still serving")["config_hash"],
        json!(old_hash)
    );
}

#[test]
fn a_restart_that_fails_on_the_candidate_rolls_the_file_back() {
    let node = Node::new();
    let old_yaml = node.config("apply-demo", "");
    let old_hash = node.start(&old_yaml);
    let candidate = node.candidate(&node.config("apply-demo", EXTRA_FIELD));

    let out = node.apply(&candidate, "fail", 30).output().unwrap();
    assert_rolled_back(&node, &old_yaml, &old_hash, &out);
}

#[test]
fn a_node_that_never_reports_the_candidate_hash_rolls_back() {
    let node = Node::new();
    let old_yaml = node.config("apply-demo", "");
    let old_hash = node.start(&old_yaml);
    let candidate = node.candidate(&node.config("apply-demo", EXTRA_FIELD));

    let out = node.apply(&candidate, "silent", 2).output().unwrap();
    assert_rolled_back(&node, &old_yaml, &old_hash, &out);
}
