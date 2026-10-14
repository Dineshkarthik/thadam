//! `thadam-bench report`: a markdown table over a results directory. Every row
//! carries its host, so no number is shown without the machine it came from.

use std::fmt::Write as _;
use std::path::Path;

use crate::error::{BenchError, Result};
use crate::result::{Histogram, RunResult};

/// Reads every `*.json` result in `dir`, sorted by scenario then start time.
///
/// # Errors
///
/// Returns [`BenchError::Io`] when the directory cannot be read, and
/// [`BenchError::Run`] naming the file when a result does not parse.
pub fn load_dir(dir: &Path) -> Result<Vec<RunResult>> {
    let mut results = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let result: RunResult = serde_json::from_str(&text).map_err(|e| {
            BenchError::Run(format!(
                "{} is not a thadam-bench result: {e}",
                path.display()
            ))
        })?;
        results.push(result);
    }
    results.sort_by(|a, b| {
        (&a.scenario.name, &a.run.started_at).cmp(&(&b.scenario.name, &b.run.started_at))
    });
    Ok(results)
}

/// Renders `results` as a markdown table followed by the caveats.
#[must_use]
pub fn render(results: &[RunResult]) -> String {
    let mut out = String::new();
    out.push_str(
        "| Scenario | Started (UTC) | Commit | Host | Msgs/s | MB/s | Ack p50 / p95 / p99 (ms) \
         | WAL commit p99 (ms) | Sink write p99 (ms) | Peak RSS (MiB) | Mean CPU % \
         | Parquet files | Parquet (MiB) | Restart to ready (ms) |\n",
    );
    out.push_str("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for r in results {
        let commit = format!(
            "{}{}",
            r.environment.git_sha,
            match r.environment.git_dirty {
                Some(true) => " (dirty)",
                _ => "",
            }
        );
        let host = format!(
            "{}, {} cores",
            r.environment.cpu_model, r.environment.cpu_cores
        );
        let row = [
            r.scenario.name.clone(),
            r.run.started_at.clone(),
            commit,
            host,
            opt(r.throughput.as_ref().map(|t| t.source_messages_per_s), 1),
            opt(r.throughput.as_ref().map(|t| t.mb_per_s), 3),
            ack_cell(r.latency.as_ref().map(|l| &l.source_ack)),
            hist_p99(r.latency.as_ref().map(|l| &l.wal_commit)),
            hist_p99(r.latency.as_ref().map(|l| &l.sink_batch_write)),
            opt(
                r.resources.as_ref().map(|s| s.peak_rss_bytes / 1_048_576.0),
                1,
            ),
            opt(r.resources.as_ref().map(|s| s.mean_cpu_percent), 1),
            opt(r.storage.as_ref().map(|s| s.parquet_files as f64), 0),
            opt(
                r.storage
                    .as_ref()
                    .map(|s| s.parquet_bytes as f64 / 1_048_576.0),
                1,
            ),
            recovery_cell(
                r.recovery
                    .as_ref()
                    .map(|c| c.restart_to_ready_ms.as_slice()),
            ),
        ];
        let _ = writeln!(out, "| {} |", row.join(" | "));
    }
    out.push_str(
        "\nNumbers are specific to the host and commit in each row. Do not quote a number \
         without its JSON result file, which records the environment and every scenario \
         setting. Msgs/s counts source messages, which are simulator batches unless the \
         scenario sets `unbatched`. A dash means the measurement was not taken.\n",
    );
    out
}

fn opt(value: Option<f64>, decimals: usize) -> String {
    value.map_or_else(|| "-".to_owned(), |v| format!("{v:.decimals$}"))
}

fn ack_cell(h: Option<&Histogram>) -> String {
    match h {
        None => "-".to_owned(),
        Some(h) if h.status != "ok" => h.status.clone(),
        Some(h) => format!(
            "{} / {} / {}",
            opt(h.p50_ms, 2),
            opt(h.p95_ms, 2),
            opt(h.p99_ms, 2)
        ),
    }
}

fn hist_p99(h: Option<&Histogram>) -> String {
    match h {
        None => "-".to_owned(),
        Some(h) if h.status != "ok" => h.status.clone(),
        Some(h) => opt(h.p99_ms, 2),
    }
}

fn recovery_cell(ms: Option<&[f64]>) -> String {
    match ms {
        None | Some([]) => "-".to_owned(),
        Some(values) => values
            .iter()
            .map(|v| format!("{v:.0}"))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::environment::Environment;
    use crate::result::{
        LatencyReport, Recovery, Resources, RunRecord, SCHEMA_VERSION, ScenarioRecord,
        SimulatorRecord, Throughput,
    };
    use crate::storage::Storage;

    fn sample() -> RunResult {
        let ok = |p99: f64| Histogram {
            status: "ok".into(),
            count: Some(100.0),
            p50_ms: Some(1.0),
            p95_ms: Some(2.0),
            p99_ms: Some(p99),
        };
        RunResult {
            schema_version: SCHEMA_VERSION,
            scenario: ScenarioRecord {
                name: "m1-steady-state".into(),
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
                measure: vec![],
                recovery_kill_after_s: None,
            },
            run: RunRecord {
                started_at: "2026-10-09T10:00:00Z".into(),
                finished_at: "2026-10-09T10:01:00Z".into(),
                thadam_instances: 1,
                metric_scrapes: 60,
                thadam_bin: "thadam".into(),
                simulator_bin: "thadam-simulator".into(),
            },
            environment: Environment {
                git_sha: "b477274".into(),
                git_dirty: Some(true),
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
                bytes: 3_000_000.0,
                source_messages_per_s: 10.0,
                mb_per_s: 0.05,
            }),
            latency: Some(LatencyReport {
                source_ack: ok(4.5),
                wal_commit: Histogram {
                    status: "not exported".into(),
                    count: None,
                    p50_ms: None,
                    p95_ms: None,
                    p99_ms: None,
                },
                sink_batch_write: ok(12.0),
            }),
            resources: Some(Resources {
                samples: 60,
                mean_cpu_percent: 42.0,
                peak_rss_bytes: 200.0 * 1_048_576.0,
                mean_rss_bytes: 150.0 * 1_048_576.0,
            }),
            storage: Some(Storage {
                parquet_files: 6,
                parquet_bytes: 2 * 1_048_576,
            }),
            recovery: Some(Recovery {
                kills: 1,
                restart_to_ready_ms: vec![812.0],
            }),
        }
    }

    #[test]
    fn table_row_carries_host_and_marks_missing_pieces() {
        let mut r = sample();
        r.resources = None;
        let text = render(&[r]);
        let row = text.lines().nth(2).expect("row");
        assert!(
            row.starts_with(
                "| m1-steady-state | 2026-10-09T10:00:00Z | b477274 (dirty) | Generic CPU, 8 cores |"
            ),
            "{row}"
        );
        assert!(row.contains("| 10.0 | 0.050 |"), "{row}");
        assert!(row.contains("| 1.00 / 2.00 / 4.50 |"), "{row}");
        assert!(row.contains("| not exported |"), "{row}");
        assert!(row.contains("| - | - | 6 | 2.0 | 812 |"), "{row}");
        assert!(text.contains("Do not quote a number"));
    }

    #[test]
    fn loads_json_results_and_rejects_garbage() {
        let dir = std::env::temp_dir().join(format!("thadam-bench-report-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let good = serde_json::to_string(&sample()).expect("json");
        std::fs::write(dir.join("a.json"), &good).expect("write");
        std::fs::write(dir.join("notes.txt"), "ignored").expect("write");
        let loaded = load_dir(&dir).expect("load");
        assert_eq!(loaded, vec![sample()]);

        std::fs::write(dir.join("bad.json"), "{}").expect("write");
        let err = load_dir(&dir).expect_err("garbage");
        assert!(err.to_string().contains("bad.json"), "{err}");
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
