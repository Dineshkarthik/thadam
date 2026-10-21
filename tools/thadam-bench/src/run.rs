//! `thadam-bench run`: starts the broker, `thadam run`, and the simulator; samples
//! `/metrics` and `ps` once a second; optionally SIGKILLs and restarts
//! `thadam`; then stops everything and writes one JSON result.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime};

use crate::binaries::{Binaries, workspace_root};
use crate::broker::Broker;
use crate::environment::Environment;
use crate::error::{BenchError, Result};
use crate::metrics::parse_exposition;
use crate::process::{Proc, PsSample, free_port, http_get_ok, is_ok, ps_sample};
use crate::result::{
    LatencyReport, Recovery, RunRecord, RunResult, SCHEMA_VERSION, ScenarioRecord,
};
use crate::scenario::{FsyncMode, Measure, Scenario};
use crate::storage::walk_parquet;
use crate::summary::{
    Instance, SINK_BATCH_WRITE, SOURCE_ACK, Scrape, WAL_COMMIT, histogram, resources, throughput,
};

/// The broker config the demo ships, relative to the workspace root.
const MOSQUITTO_CONF: &str = "examples/edge-capture/mosquitto/mosquitto.conf";
/// The broker URL the demo pipeline names; rewritten to the bench's port.
const DEMO_BROKER_URL: &str = "url: mqtt://localhost:21883";
/// The data directory the demo pipeline names; rewritten to a scratch dir.
const DEMO_DATA_DIR: &str = "data_dir: ./data";
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
const READY_TIMEOUT: Duration = Duration::from_secs(120);
const STOP_TIMEOUT: Duration = Duration::from_secs(120);
const SIM_SLACK: Duration = Duration::from_secs(120);

/// The inputs to one run.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// The scenario file, resolved against the current directory.
    pub scenario: PathBuf,
    /// Directory for the JSON result; created when missing.
    pub out_dir: PathBuf,
    /// Replaces the scenario's `duration`, for smoke runs.
    pub duration: Option<Duration>,
}

/// Runs the scenario and writes its JSON result. Returns the path written and
/// the result itself.
///
/// # Errors
///
/// Any failing step ends the run with its error. The scratch directory with
/// every log is kept and its path is printed to stderr.
pub fn run(opts: &RunOptions) -> Result<(PathBuf, RunResult)> {
    let cwd = std::env::current_dir()?;
    let root = workspace_root(&cwd)?;
    let mut scenario = Scenario::load(&opts.scenario)?;
    if let Some(d) = opts.duration {
        scenario = scenario.with_duration(d)?;
    }
    let binaries = Binaries::resolve(&root)?;
    let work = scratch_dir(&scenario.name)?;
    std::fs::create_dir_all(&work)?;

    let outcome = execute(&root, &scenario, opts, &binaries, &work);
    match outcome {
        Ok(written) => {
            std::fs::remove_dir_all(&work)?;
            Ok(written)
        }
        Err(e) => {
            eprintln!("thadam-bench: logs kept in {}", work.display());
            Err(e)
        }
    }
}

fn scratch_dir(name: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| BenchError::Run(format!("clock: {e}")))?
        .as_millis();
    Ok(std::env::temp_dir().join(format!(
        "thadam-bench-{name}-{}-{stamp}",
        std::process::id()
    )))
}

fn execute(
    root: &Path,
    scenario: &Scenario,
    opts: &RunOptions,
    binaries: &Binaries,
    work: &Path,
) -> Result<(PathBuf, RunResult)> {
    let started_at = humantime::format_rfc3339_seconds(SystemTime::now()).to_string();
    let data_dir = work.join("data");
    let config = work.join("pipeline.yaml");
    let oracle = work.join("oracle.ndjson");

    let pipeline_src = root.join(&scenario.pipeline);
    let template = std::fs::read_to_string(&pipeline_src)
        .map_err(|e| BenchError::Missing(format!("pipeline {}: {e}", pipeline_src.display())))?;

    let broker = Broker::start(&root.join(MOSQUITTO_CONF))?;
    let rendered = render_pipeline(&template, broker.port(), &data_dir, scenario.fsync)?;
    std::fs::write(&config, rendered)?;

    let admin_port = free_port()?;
    let admin = format!("http://127.0.0.1:{admin_port}");
    let started = Instant::now();
    let mut rec = Recorder::new(started);
    let mut thadam = spawn_thadam(
        &binaries.thadam,
        &config,
        work,
        admin_port,
        &work.join("thadam-1.log"),
    )?;
    rec.instances.push(Instance::default());
    wait_ready(&admin, READY_TIMEOUT, &thadam)?;

    let fleet = root.join(&scenario.simulator.fleet);
    let sim_args = SimArgs {
        bin: &binaries.simulator,
        fleet: &fleet,
        broker_port: broker.port(),
        oracle: &oracle,
        duration: scenario.duration,
    };
    let mut sim = spawn_simulator(scenario, &sim_args, &work.join("simulator.log"))?;
    let sim_started = Instant::now();
    let sim_deadline = scenario.duration + scenario.simulator.drain_timeout + SIM_SLACK;

    let mut restart_ms = Vec::new();
    let mut killed = false;
    loop {
        rec.tick(thadam.pid(), &format!("{admin}/metrics"))?;
        if let Some(status) = sim.try_exit()? {
            if !status.success() {
                return Err(BenchError::Run(format!(
                    "simulator exited with {status}:\n{}",
                    sim.log_tail(40)
                )));
            }
            break;
        }
        if sim_started.elapsed() >= sim_deadline {
            return Err(BenchError::Run(format!(
                "simulator still running after {}; log: {}",
                humantime::format_duration(sim_deadline),
                sim.log().display()
            )));
        }
        if let Some(kill_after) = scenario.recovery_kill_after
            && !killed
            && sim_started.elapsed() >= kill_after
        {
            killed = true;
            thadam.kill_hard()?;
            let restart = Instant::now();
            let index = rec.instances.len() + 1;
            thadam = spawn_thadam(
                &binaries.thadam,
                &config,
                work,
                admin_port,
                &work.join(format!("thadam-{index}.log")),
            )?;
            rec.instances.push(Instance::default());
            wait_ready(&admin, READY_TIMEOUT, &thadam)?;
            restart_ms.push(restart.elapsed().as_secs_f64() * 1000.0);
        }
        std::thread::sleep(SAMPLE_EVERY);
    }

    let drain_until = Instant::now() + scenario.drain;
    while Instant::now() < drain_until {
        rec.tick(thadam.pid(), &format!("{admin}/metrics"))?;
        std::thread::sleep(SAMPLE_EVERY);
    }
    rec.tick(thadam.pid(), &format!("{admin}/metrics"))?;
    let stop = thadam.terminate(STOP_TIMEOUT)?;
    if !stop.success() {
        return Err(BenchError::Run(format!(
            "thadam exited with {stop} on shutdown:\n{}",
            thadam.log_tail(60)
        )));
    }
    let finished_at = humantime::format_rfc3339_seconds(SystemTime::now()).to_string();

    let result = assemble(&Assembly {
        root,
        scenario,
        overridden: opts.duration.is_some(),
        binaries,
        data_dir: &data_dir,
        recorder: &rec,
        restart_ms,
        started_at,
        finished_at,
    })?;
    drop(sim);
    drop(thadam);
    drop(broker);
    let path = write_result(&opts.out_dir, &scenario.name, &result)?;
    Ok((path, result))
}

struct Assembly<'a> {
    root: &'a Path,
    scenario: &'a Scenario,
    overridden: bool,
    binaries: &'a Binaries,
    data_dir: &'a Path,
    recorder: &'a Recorder,
    restart_ms: Vec<f64>,
    started_at: String,
    finished_at: String,
}

fn assemble(a: &Assembly<'_>) -> Result<RunResult> {
    let measure = &a.scenario.measure;
    let has = |m: Measure| measure.contains(&m);
    let instances = &a.recorder.instances;

    let throughput = if has(Measure::Throughput) {
        let t = throughput(instances);
        if t.is_none() {
            eprintln!("thadam-bench: warning: source counters not exported; throughput omitted");
        }
        t
    } else {
        None
    };
    let latency = has(Measure::Latency).then(|| LatencyReport {
        source_ack: histogram(instances, SOURCE_ACK),
        wal_commit: histogram(instances, WAL_COMMIT),
        sink_batch_write: histogram(instances, SINK_BATCH_WRITE),
    });
    let resources = if has(Measure::Resources) {
        resources(&a.recorder.resources)
    } else {
        None
    };
    let storage = if has(Measure::Storage) {
        Some(walk_parquet(&a.data_dir.join("sinks"))?)
    } else {
        None
    };
    let recovery = has(Measure::Recovery).then(|| Recovery {
        kills: u32::try_from(a.restart_ms.len()).unwrap_or(u32::MAX),
        restart_to_ready_ms: a.restart_ms.clone(),
    });

    Ok(RunResult {
        schema_version: SCHEMA_VERSION,
        scenario: ScenarioRecord::new(a.scenario, a.overridden),
        run: RunRecord {
            started_at: a.started_at.clone(),
            finished_at: a.finished_at.clone(),
            thadam_instances: u32::try_from(instances.len()).unwrap_or(u32::MAX),
            metric_scrapes: a.recorder.scrapes,
            thadam_bin: a.binaries.thadam.display().to_string(),
            simulator_bin: a.binaries.simulator.display().to_string(),
        },
        environment: Environment::collect(a.root),
        throughput,
        latency,
        resources,
        storage,
        recovery,
    })
}

fn write_result(out_dir: &Path, name: &str, result: &RunResult) -> Result<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let stamp = result.run.started_at.replace(':', "-");
    let path = out_dir.join(format!("{name}-{stamp}.json"));
    if path.exists() {
        return Err(BenchError::Run(format!(
            "{} already exists; results are never overwritten",
            path.display()
        )));
    }
    let mut text = serde_json::to_string_pretty(result)?;
    text.push('\n');
    std::fs::write(&path, text)?;
    Ok(path)
}

/// Everything sampled during a run, keyed by `thadam` instance.
struct Recorder {
    started: Instant,
    instances: Vec<Instance>,
    resources: Vec<PsSample>,
    scrapes: u32,
}

impl Recorder {
    fn new(started: Instant) -> Recorder {
        Recorder {
            started,
            instances: Vec::new(),
            resources: Vec::new(),
            scrapes: 0,
        }
    }

    /// One sample: a `/metrics` scrape (skipped while the process is down) and
    /// a `ps` reading for `pid`.
    fn tick(&mut self, pid: u32, metrics_url: &str) -> Result<()> {
        let at_s = self.started.elapsed().as_secs_f64();
        if let Some(body) = http_get_ok(metrics_url) {
            let series = parse_exposition(&body)?;
            if let Some(current) = self.instances.last_mut() {
                current.scrapes.push(Scrape { at_s, series });
                self.scrapes += 1;
            }
        }
        if let Some(sample) = ps_sample(pid) {
            self.resources.push(sample);
        }
        Ok(())
    }
}

fn spawn_thadam(
    bin: &Path,
    config: &Path,
    cwd: &Path,
    admin_port: u16,
    log: &Path,
) -> Result<Proc> {
    let mut cmd = Command::new(bin);
    cmd.args([
        "run",
        &config.to_string_lossy(),
        "--log-format",
        "json",
        "--log-level",
        "info",
        "--admin-bind",
        &format!("127.0.0.1:{admin_port}"),
    ])
    .current_dir(cwd);
    Proc::spawn(cmd, log)
}

fn wait_ready(admin: &str, within: Duration, thadam: &Proc) -> Result<()> {
    let started = Instant::now();
    let url = format!("{admin}/readyz");
    loop {
        if is_ok(&url) {
            return Ok(());
        }
        if started.elapsed() >= within {
            return Err(BenchError::Run(format!(
                "{url} not ready within {}; thadam log tail:\n{}",
                humantime::format_duration(within),
                thadam.log_tail(60)
            )));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct SimArgs<'a> {
    bin: &'a Path,
    fleet: &'a Path,
    broker_port: u16,
    oracle: &'a Path,
    duration: Duration,
}

fn spawn_simulator(scenario: &Scenario, args: &SimArgs<'_>, log: &Path) -> Result<Proc> {
    let sim = &scenario.simulator;
    let mut cmd = Command::new(args.bin);
    cmd.arg("run")
        .args([
            "--broker",
            &format!("mqtt://127.0.0.1:{}", args.broker_port),
        ])
        .arg("--fleet")
        .arg(args.fleet)
        .arg("--oracle")
        .arg(args.oracle)
        .args([
            "--duration",
            &humantime::format_duration(args.duration).to_string(),
        ])
        .args([
            "--drain-timeout",
            &humantime::format_duration(sim.drain_timeout).to_string(),
        ])
        .args([
            "--client-id",
            "thadam-bench-sim",
            "--log-format",
            "json",
            "--log-level",
            "warn",
        ]);
    if let Some(seed) = sim.seed {
        cmd.args(["--seed", &seed.to_string()]);
    }
    if let Some(scale) = sim.scale {
        cmd.args(["--scale", &scale.to_string()]);
    }
    if sim.unbatched {
        cmd.arg("--unbatched");
    }
    for (flag, rate) in [
        ("--malformed-rate", sim.malformed_rate),
        ("--duplicate-rate", sim.duplicate_rate),
        ("--out-of-range-rate", sim.out_of_range_rate),
    ] {
        if let Some(r) = rate {
            cmd.args([flag, &r.to_string()]);
        }
    }
    Proc::spawn(cmd, log)
}

/// Rewrites the demo pipeline for one run: the broker port, a scratch data
/// directory, and, when set, the `wal.fsync` override. Each anchor must appear
/// exactly as the demo ships it, so a changed demo fails loudly here.
///
/// # Errors
///
/// Returns [`BenchError::Config`] when an anchor is missing or `fsync:` is
/// not found exactly once.
pub fn render_pipeline(
    template: &str,
    broker_port: u16,
    data_dir: &Path,
    fsync: Option<FsyncMode>,
) -> Result<String> {
    for anchor in [DEMO_BROKER_URL, DEMO_DATA_DIR] {
        if !template.contains(anchor) {
            return Err(BenchError::Config(format!(
                "pipeline does not contain `{anchor}`; the bench rewrites that line"
            )));
        }
    }
    let text = template
        .replace(
            DEMO_BROKER_URL,
            &format!("url: mqtt://127.0.0.1:{broker_port}"),
        )
        .replace(DEMO_DATA_DIR, &format!("data_dir: {}", data_dir.display()));

    let Some(mode) = fsync else {
        return Ok(text);
    };
    let fsync_lines: Vec<usize> = text
        .lines()
        .enumerate()
        .filter(|(_, l)| {
            let t = l.trim_start();
            !t.starts_with('#') && t.starts_with("fsync:")
        })
        .map(|(i, _)| i)
        .collect();
    let [line_idx] = fsync_lines.as_slice() else {
        return Err(BenchError::Config(format!(
            "expected exactly one `fsync:` line in the pipeline, found {}",
            fsync_lines.len()
        )));
    };
    let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
    let indent: String = lines[*line_idx]
        .chars()
        .take_while(|c| c.is_whitespace())
        .collect();
    lines[*line_idx] = format!("{indent}fsync: {}", mode.render());
    let mut out = lines.join("\n");
    if text.ends_with('\n') {
        out.push('\n');
    }
    Ok(out)
}

/// One line for the terminal after a run.
#[must_use]
pub fn summary_line(r: &RunResult, path: &Path) -> String {
    let mut parts = Vec::new();
    if let Some(t) = &r.throughput {
        parts.push(format!(
            "{:.1} msgs/s, {:.3} MB/s",
            t.source_messages_per_s, t.mb_per_s
        ));
    }
    if let Some(l) = &r.latency
        && let Some(p99) = l.source_ack.p99_ms
    {
        parts.push(format!("ack p99 {p99:.2} ms"));
    }
    if let Some(s) = &r.resources {
        parts.push(format!(
            "peak RSS {:.1} MiB, mean CPU {:.1}%",
            s.peak_rss_bytes / 1_048_576.0,
            s.mean_cpu_percent
        ));
    }
    if let Some(s) = &r.storage {
        parts.push(format!(
            "{} parquet files, {:.1} MiB",
            s.parquet_files,
            s.parquet_bytes as f64 / 1_048_576.0
        ));
    }
    if let Some(rc) = &r.recovery {
        let ms: Vec<String> = rc
            .restart_to_ready_ms
            .iter()
            .map(|v| format!("{v:.0}"))
            .collect();
        parts.push(format!("restart to ready {} ms", ms.join(", ")));
    }
    format!(
        "{}: {} -> {}",
        r.scenario.name,
        if parts.is_empty() {
            "no measurements".to_owned()
        } else {
            parts.join("; ")
        },
        path.display()
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const DEMO: &str = include_str!("../../../examples/edge-capture/thadam/pipeline.local.yaml");

    #[test]
    fn demo_pipeline_is_rewritten_for_the_run() {
        let text = render_pipeline(DEMO, 40123, Path::new("/tmp/work/data"), None).expect("render");
        assert!(text.contains("url: mqtt://127.0.0.1:40123"));
        assert!(text.contains("data_dir: /tmp/work/data"));
        assert!(text.contains("fsync: { interval: 50ms }"));
        assert!(text.ends_with('\n'));
    }

    #[test]
    fn fsync_override_replaces_only_the_wal_line() {
        let text = render_pipeline(
            DEMO,
            40123,
            Path::new("/tmp/work/data"),
            Some(FsyncMode::Always),
        )
        .expect("render");
        assert!(text.contains("\n  fsync: always\n"));
        assert!(!text.contains("interval: 50ms"));
        assert_eq!(text.lines().count(), DEMO.lines().count());
    }

    #[test]
    fn missing_anchor_or_fsync_line_is_a_config_error() {
        let no_url = DEMO.replace(DEMO_BROKER_URL, "url: mqtt://elsewhere");
        assert!(render_pipeline(&no_url, 1, Path::new("/x"), None).is_err());
        let no_fsync = DEMO.replace("fsync: { interval: 50ms }", "");
        assert!(render_pipeline(&no_fsync, 1, Path::new("/x"), Some(FsyncMode::Never)).is_err());
    }

    #[test]
    fn summary_line_names_the_scenario_and_path() {
        let result = RunResult {
            schema_version: SCHEMA_VERSION,
            scenario: ScenarioRecord::new(
                &Scenario::parse("name: m1-steady-state\nduration: 10s\npipeline: p.yaml\n")
                    .expect("scenario"),
                true,
            ),
            run: RunRecord {
                started_at: "2026-10-09T00:00:00Z".into(),
                finished_at: "2026-10-09T00:00:10Z".into(),
                thadam_instances: 1,
                metric_scrapes: 10,
                thadam_bin: "thadam".into(),
                simulator_bin: "thadam-simulator".into(),
            },
            environment: Environment {
                git_sha: "unknown".into(),
                git_dirty: None,
                rustc: "rustc".into(),
                os: "linux".into(),
                kernel: "Linux".into(),
                arch: "x86_64".into(),
                cpu_model: "unknown".into(),
                cpu_cores: 4,
            },
            throughput: Some(crate::result::Throughput {
                window_s: 10.0,
                messages: 100.0,
                bytes: 1_000_000.0,
                source_messages_per_s: 10.0,
                mb_per_s: 0.1,
            }),
            latency: None,
            resources: None,
            storage: None,
            recovery: None,
        };
        let line = summary_line(&result, Path::new("out/x.json"));
        assert!(
            line.starts_with("m1-steady-state: 10.0 msgs/s, 0.100 MB/s -> out/x.json"),
            "{line}"
        );
    }
}
