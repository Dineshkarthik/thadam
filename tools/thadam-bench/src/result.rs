//! The JSON document one run writes. Field names are the contract: `report`
//! reads them back, and anyone quoting a number must quote this file too.

use serde::{Deserialize, Serialize};

use crate::environment::Environment;
use crate::scenario::{FsyncMode, Measure, Scenario};
use crate::storage::Storage;

/// Bumped whenever a field changes meaning or is removed.
pub const SCHEMA_VERSION: u32 = 1;

/// One benchmark run, end to end.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// [`SCHEMA_VERSION`] at the time of writing.
    pub schema_version: u32,
    /// The scenario as it actually ran, including any `--duration` override.
    pub scenario: ScenarioRecord,
    /// When and how the run executed.
    pub run: RunRecord,
    /// Where it ran. Required context for every number in this file.
    pub environment: Environment,
    /// Source ingest rates; absent unless `throughput` was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput: Option<Throughput>,
    /// Latency percentiles per histogram; absent unless `latency` was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<LatencyReport>,
    /// Thadam CPU and RSS; absent unless `resources` was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// Parquet output on disk; absent unless `storage` was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<Storage>,
    /// Restart timing; absent unless `recovery` was measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<Recovery>,
}

/// The scenario fields that affect the numbers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioRecord {
    /// Scenario name.
    pub name: String,
    /// Scenario description.
    pub description: String,
    /// Effective generation window in seconds.
    pub duration_s: f64,
    /// True when `--duration` replaced the scenario's own duration.
    pub duration_overridden: bool,
    /// Seconds sampled after the simulator exits, before `thadam` is stopped.
    pub drain_s: f64,
    /// The pipeline file, relative to the workspace root.
    pub pipeline: String,
    /// The fsync override as rendered into `wal.fsync`, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fsync: Option<String>,
    /// The simulator flags.
    pub simulator: SimulatorRecord,
    /// The measurements taken.
    pub measure: Vec<Measure>,
    /// Seconds into generation at which `thadam` was SIGKILLed, when recovery ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_kill_after_s: Option<f64>,
}

/// The simulator flags a run used. Unset optional flags are omitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimulatorRecord {
    /// The fleet file, relative to the workspace root.
    pub fleet: String,
    /// `--seed`, when overridden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    /// `--scale`, when overridden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<u32>,
    /// Whether `--unbatched` was passed.
    pub unbatched: bool,
    /// `--malformed-rate`, when overridden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub malformed_rate: Option<f64>,
    /// `--duplicate-rate`, when overridden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_rate: Option<f64>,
    /// `--out-of-range-rate`, when overridden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub out_of_range_rate: Option<f64>,
    /// `--drain-timeout` in seconds.
    pub drain_timeout_s: f64,
}

impl ScenarioRecord {
    /// Records `scenario` as it runs with `duration`.
    #[must_use]
    pub fn new(scenario: &Scenario, overridden: bool) -> ScenarioRecord {
        let sim = &scenario.simulator;
        ScenarioRecord {
            name: scenario.name.clone(),
            description: scenario.description.clone(),
            duration_s: scenario.duration.as_secs_f64(),
            duration_overridden: overridden,
            drain_s: scenario.drain.as_secs_f64(),
            pipeline: scenario.pipeline.display().to_string(),
            fsync: scenario.fsync.as_ref().map(FsyncMode::render),
            simulator: SimulatorRecord {
                fleet: sim.fleet.display().to_string(),
                seed: sim.seed,
                scale: sim.scale,
                unbatched: sim.unbatched,
                malformed_rate: sim.malformed_rate,
                duplicate_rate: sim.duplicate_rate,
                out_of_range_rate: sim.out_of_range_rate,
                drain_timeout_s: sim.drain_timeout.as_secs_f64(),
            },
            measure: scenario.measure.clone(),
            recovery_kill_after_s: scenario.recovery_kill_after.map(|d| d.as_secs_f64()),
        }
    }
}

/// Timing and process facts of the run itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    /// Wall-clock start, RFC 3339 UTC.
    pub started_at: String,
    /// Wall-clock end, RFC 3339 UTC.
    pub finished_at: String,
    /// Number of `thadam run` processes used; more than one means a restart.
    pub thadam_instances: u32,
    /// Successful `/metrics` scrapes across all instances.
    pub metric_scrapes: u32,
    /// The `thadam` binary that ran.
    pub thadam_bin: String,
    /// The simulator binary that ran.
    pub simulator_bin: String,
}

/// Source ingest rates over the sampled window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Throughput {
    /// Seconds of `thadam` uptime covered by the deltas (restart gaps excluded).
    pub window_s: f64,
    /// Increase of `thadam_source_messages_total` over the window.
    pub messages: f64,
    /// Increase of `thadam_source_bytes_total` over the window.
    pub bytes: f64,
    /// Source messages per second. A message is a simulator batch, not one
    /// sample; the simulator's `--unbatched` flag makes the two equal.
    pub source_messages_per_s: f64,
    /// Source bytes per second in MB (10^6 bytes).
    pub mb_per_s: f64,
}

/// Percentiles for each latency histogram the runner knows about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyReport {
    /// Source ack latency, `thadam_source_ack_latency_seconds`.
    pub source_ack: Histogram,
    /// WAL commit latency, `thadam_wal_commit_seconds`; not exported today.
    pub wal_commit: Histogram,
    /// Sink batch write latency, `thadam_batch_write_seconds`, all sinks merged.
    pub sink_batch_write: Histogram,
}

/// One histogram's percentiles over the window, in milliseconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Histogram {
    /// `ok`, `no observations`, or `not exported`.
    pub status: String,
    /// Observations inside the window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<f64>,
    /// 50th percentile in ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<f64>,
    /// 95th percentile in ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_ms: Option<f64>,
    /// 99th percentile in ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p99_ms: Option<f64>,
}

/// CPU and memory of the `thadam run` process, sampled once per second.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    /// Number of `ps` samples.
    pub samples: u32,
    /// Mean `%cpu` as `ps` reports it (percent of one core).
    pub mean_cpu_percent: f64,
    /// Largest RSS seen, in bytes.
    pub peak_rss_bytes: f64,
    /// Mean RSS, in bytes.
    pub mean_rss_bytes: f64,
}

/// Time from SIGKILL-and-restart to the first `/readyz` 200.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Recovery {
    /// Number of SIGKILLs issued.
    pub kills: u32,
    /// Milliseconds from spawning the replacement process to its first ready
    /// response, for each kill.
    pub restart_to_ready_ms: Vec<f64>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn unset_optional_sections_are_omitted_from_json() {
        let result = RunResult {
            schema_version: SCHEMA_VERSION,
            scenario: ScenarioRecord {
                name: "m1".into(),
                description: String::new(),
                duration_s: 60.0,
                duration_overridden: false,
                drain_s: 5.0,
                pipeline: "p.yaml".into(),
                fsync: None,
                simulator: SimulatorRecord {
                    fleet: "f.yaml".into(),
                    seed: None,
                    scale: None,
                    unbatched: false,
                    malformed_rate: None,
                    duplicate_rate: None,
                    out_of_range_rate: None,
                    drain_timeout_s: 8.0,
                },
                measure: vec![Measure::Throughput],
                recovery_kill_after_s: None,
            },
            run: RunRecord {
                started_at: "2026-10-09T00:00:00Z".into(),
                finished_at: "2026-10-09T00:01:00Z".into(),
                thadam_instances: 1,
                metric_scrapes: 60,
                thadam_bin: "thadam".into(),
                simulator_bin: "thadam-simulator".into(),
            },
            environment: Environment {
                git_sha: "abc1234".into(),
                git_dirty: Some(false),
                rustc: "rustc 1.99.0".into(),
                os: "linux".into(),
                kernel: "Linux 6.x".into(),
                arch: "aarch64".into(),
                cpu_model: "Generic CPU".into(),
                cpu_cores: 8,
            },
            throughput: Some(Throughput {
                window_s: 60.0,
                messages: 600.0,
                bytes: 6e6,
                source_messages_per_s: 10.0,
                mb_per_s: 0.1,
            }),
            latency: None,
            resources: None,
            storage: None,
            recovery: None,
        };
        let value: serde_json::Value = serde_json::to_value(&result).expect("json");
        let keys: Vec<&str> = value
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            vec![
                "environment",
                "run",
                "scenario",
                "schema_version",
                "throughput"
            ]
        );
        assert_eq!(value["throughput"]["source_messages_per_s"], 10.0);
        assert_eq!(value["environment"]["git_dirty"], false);
        assert_eq!(value["scenario"]["measure"][0], "throughput");
        let back: RunResult = serde_json::from_value(value).expect("round trip");
        assert_eq!(back, result);
    }
}
