//! Scenario files: what to run, for how long, with which simulator flags, and
//! which measurements to take. Paths in a scenario are relative to the
//! workspace root.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{BenchError, Result};

/// The fleet every scenario uses unless it names another one.
pub const DEFAULT_FLEET: &str = "examples/edge-capture/simulator/fleet.yaml";

/// The measurements a scenario can ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Measure {
    /// Ingest rates from the source counters over the generation window.
    Throughput,
    /// Ack and sink-write latency percentiles from the exported histograms.
    Latency,
    /// CPU and RSS of the `thadam run` process.
    Resources,
    /// Parquet files and bytes on disk after shutdown.
    Storage,
    /// SIGKILL of `thadam run` and restart-to-ready time via `/readyz`.
    Recovery,
}

/// The default measurement set: everything except recovery, which needs a
/// `recovery` block.
const DEFAULT_MEASURE: [Measure; 4] = [
    Measure::Throughput,
    Measure::Latency,
    Measure::Resources,
    Measure::Storage,
];

/// The WAL fsync policy override, rendered into `wal.fsync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// `wal.fsync: always`.
    Always,
    /// `wal.fsync: never`.
    Never,
    /// `wal.fsync: { interval: <d> }`.
    Interval(Duration),
}

impl FsyncMode {
    /// The YAML value written after `fsync:`.
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            FsyncMode::Always => "always".to_owned(),
            FsyncMode::Never => "never".to_owned(),
            FsyncMode::Interval(d) => format!("{{ interval: {} }}", humantime::format_duration(*d)),
        }
    }

    fn parse(text: &str) -> Result<FsyncMode> {
        match text.trim() {
            "always" => Ok(FsyncMode::Always),
            "never" => Ok(FsyncMode::Never),
            other => parse_duration(other, "fsync")
                .map(FsyncMode::Interval)
                .map_err(|_| {
                    BenchError::Config(format!(
                        "fsync: expected `always`, `never`, or an interval such as `50ms`, got `{other}`"
                    ))
                }),
        }
    }
}

/// Simulator flags, resolved from the scenario's `simulator` block.
#[derive(Debug, Clone, PartialEq)]
pub struct SimulatorArgs {
    /// The fleet file, relative to the workspace root.
    pub fleet: PathBuf,
    /// `--seed`, when set.
    pub seed: Option<u64>,
    /// `--scale`, when set.
    pub scale: Option<u32>,
    /// `--unbatched`.
    pub unbatched: bool,
    /// `--malformed-rate`, when set.
    pub malformed_rate: Option<f64>,
    /// `--duplicate-rate`, when set.
    pub duplicate_rate: Option<f64>,
    /// `--out-of-range-rate`, when set.
    pub out_of_range_rate: Option<f64>,
    /// `--drain-timeout`: how long to wait for PUBACKs after generation.
    pub drain_timeout: Duration,
}

/// A validated scenario.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    /// The scenario name; part of every result file name.
    pub name: String,
    /// What the scenario demonstrates.
    pub description: String,
    /// How long the simulator generates samples.
    pub duration: Duration,
    /// How long to keep sampling after generation stops, so chains drain.
    pub drain: Duration,
    /// The pipeline file, relative to the workspace root.
    pub pipeline: PathBuf,
    /// The WAL fsync override, when set.
    pub fsync: Option<FsyncMode>,
    /// The simulator flags.
    pub simulator: SimulatorArgs,
    /// The measurements to take, in report order.
    pub measure: Vec<Measure>,
    /// When recovery is measured: how long into generation to SIGKILL thadam.
    pub recovery_kill_after: Option<Duration>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawScenario {
    name: String,
    #[serde(default)]
    description: String,
    duration: String,
    #[serde(default)]
    drain: Option<String>,
    pipeline: PathBuf,
    #[serde(default)]
    fsync: Option<String>,
    #[serde(default)]
    simulator: RawSimulator,
    #[serde(default)]
    measure: Option<Vec<Measure>>,
    #[serde(default)]
    recovery: Option<RawRecovery>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSimulator {
    #[serde(default)]
    fleet: Option<PathBuf>,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default)]
    scale: Option<u32>,
    #[serde(default)]
    unbatched: bool,
    #[serde(default)]
    malformed_rate: Option<f64>,
    #[serde(default)]
    duplicate_rate: Option<f64>,
    #[serde(default)]
    out_of_range_rate: Option<f64>,
    #[serde(default)]
    drain_timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecovery {
    kill_after: String,
}

impl Scenario {
    /// Parses and validates a scenario document.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Config`] for YAML errors, unknown keys, bad
    /// durations or rates, and inconsistent measurement settings.
    pub fn parse(text: &str) -> Result<Scenario> {
        let raw: RawScenario = serde_saphyr::from_str(text)
            .map_err(|e| BenchError::Config(format!("scenario YAML: {e}")))?;
        Scenario::from_raw(raw)
    }

    /// Loads and validates a scenario file.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Io`] when the file cannot be read, otherwise as
    /// [`Scenario::parse`].
    pub fn load(path: &Path) -> Result<Scenario> {
        let text = std::fs::read_to_string(path)?;
        Scenario::parse(&text).map_err(|e| BenchError::Config(format!("{}: {e}", path.display())))
    }

    /// Replaces the generation window and re-checks the recovery timing.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Config`] when the kill point is no longer inside
    /// the window.
    pub fn with_duration(mut self, duration: Duration) -> Result<Scenario> {
        self.duration = duration;
        check_recovery_timing(self.duration, self.recovery_kill_after)?;
        Ok(self)
    }

    fn from_raw(raw: RawScenario) -> Result<Scenario> {
        validate_name(&raw.name)?;
        let duration = parse_duration(&raw.duration, "duration")?;
        if duration.is_zero() {
            return Err(BenchError::Config(
                "duration: must be greater than zero".into(),
            ));
        }
        let drain = parse_duration(raw.drain.as_deref().unwrap_or("5s"), "drain")?;
        let fsync = raw.fsync.as_deref().map(FsyncMode::parse).transpose()?;
        let simulator = resolve_simulator(raw.simulator)?;

        let recovery_kill_after = match &raw.recovery {
            Some(r) => Some(parse_duration(&r.kill_after, "recovery.kill_after")?),
            None => None,
        };
        let measure = match raw.measure {
            Some(list) => dedup_measures(list)?,
            None => {
                let mut list = DEFAULT_MEASURE.to_vec();
                if recovery_kill_after.is_some() {
                    list.push(Measure::Recovery);
                }
                list
            }
        };
        let listed_recovery = measure.contains(&Measure::Recovery);
        match (listed_recovery, recovery_kill_after.is_some()) {
            (true, false) => {
                return Err(BenchError::Config(
                    "measure: `recovery` needs a `recovery: { kill_after: <duration> }` block"
                        .into(),
                ));
            }
            (false, true) => {
                return Err(BenchError::Config(
                    "recovery: block is set but `measure` does not list `recovery`".into(),
                ));
            }
            _ => {}
        }
        check_recovery_timing(duration, recovery_kill_after)?;

        Ok(Scenario {
            name: raw.name,
            description: raw.description,
            duration,
            drain,
            pipeline: raw.pipeline,
            fsync,
            simulator,
            measure,
            recovery_kill_after,
        })
    }
}

fn validate_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if ok {
        Ok(())
    } else {
        Err(BenchError::Config(format!(
            "name: `{name}` must be non-empty and use only ASCII letters, digits, `-`, `_`, `.`"
        )))
    }
}

fn parse_duration(text: &str, field: &str) -> Result<Duration> {
    humantime::parse_duration(text.trim())
        .map_err(|e| BenchError::Config(format!("{field}: `{text}` is not a duration ({e})")))
}

fn check_recovery_timing(duration: Duration, kill_after: Option<Duration>) -> Result<()> {
    match kill_after {
        Some(k) if k.is_zero() => Err(BenchError::Config(
            "recovery.kill_after: must be greater than zero".into(),
        )),
        Some(k) if k >= duration => Err(BenchError::Config(format!(
            "recovery.kill_after ({}) must be shorter than duration ({})",
            humantime::format_duration(k),
            humantime::format_duration(duration)
        ))),
        _ => Ok(()),
    }
}

fn dedup_measures(list: Vec<Measure>) -> Result<Vec<Measure>> {
    if list.is_empty() {
        return Err(BenchError::Config(
            "measure: must list at least one measurement".into(),
        ));
    }
    let mut out = Vec::with_capacity(list.len());
    for m in list {
        if !out.contains(&m) {
            out.push(m);
        }
    }
    Ok(out)
}

fn resolve_simulator(raw: RawSimulator) -> Result<SimulatorArgs> {
    for (name, rate) in [
        ("simulator.malformed_rate", raw.malformed_rate),
        ("simulator.duplicate_rate", raw.duplicate_rate),
        ("simulator.out_of_range_rate", raw.out_of_range_rate),
    ] {
        if let Some(r) = rate
            && !(0.0..=1.0).contains(&r)
        {
            return Err(BenchError::Config(format!(
                "{name}: must be within 0..=1, got {r}"
            )));
        }
    }
    if raw.scale == Some(0) {
        return Err(BenchError::Config(
            "simulator.scale: must be at least 1".into(),
        ));
    }
    let drain_timeout = parse_duration(
        raw.drain_timeout.as_deref().unwrap_or("8s"),
        "simulator.drain_timeout",
    )?;
    Ok(SimulatorArgs {
        fleet: raw.fleet.unwrap_or_else(|| PathBuf::from(DEFAULT_FLEET)),
        seed: raw.seed,
        scale: raw.scale,
        unbatched: raw.unbatched,
        malformed_rate: raw.malformed_rate,
        duplicate_rate: raw.duplicate_rate,
        out_of_range_rate: raw.out_of_range_rate,
        drain_timeout,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const M1: &str = "
name: m1-steady-state
description: default fleet
duration: 60s
pipeline: examples/edge-capture/thadam/pipeline.local.yaml
";

    #[test]
    fn minimal_scenario_gets_defaults() {
        let s = Scenario::parse(M1).expect("parse");
        assert_eq!(s.name, "m1-steady-state");
        assert_eq!(s.duration, Duration::from_secs(60));
        assert_eq!(s.drain, Duration::from_secs(5));
        assert_eq!(s.simulator.fleet, PathBuf::from(DEFAULT_FLEET));
        assert_eq!(s.simulator.drain_timeout, Duration::from_secs(8));
        assert!(!s.simulator.unbatched);
        assert_eq!(s.fsync, None);
        assert_eq!(
            s.measure,
            vec![
                Measure::Throughput,
                Measure::Latency,
                Measure::Resources,
                Measure::Storage
            ]
        );
        assert_eq!(s.recovery_kill_after, None);
    }

    #[test]
    fn full_scenario_parses_every_field() {
        let text = "
name: m3-recovery
duration: 60s
drain: 3s
pipeline: p.yaml
fsync: 10ms
simulator:
  fleet: f.yaml
  seed: 7
  scale: 2
  unbatched: true
  malformed_rate: 0.01
  duplicate_rate: 0.02
  out_of_range_rate: 0.005
  drain_timeout: 4s
recovery:
  kill_after: 30s
";
        let s = Scenario::parse(text).expect("parse");
        assert_eq!(s.drain, Duration::from_secs(3));
        assert_eq!(
            s.fsync,
            Some(FsyncMode::Interval(Duration::from_millis(10)))
        );
        assert_eq!(s.simulator.seed, Some(7));
        assert_eq!(s.simulator.scale, Some(2));
        assert!(s.simulator.unbatched);
        assert_eq!(s.simulator.malformed_rate, Some(0.01));
        assert_eq!(s.simulator.drain_timeout, Duration::from_secs(4));
        assert_eq!(s.recovery_kill_after, Some(Duration::from_secs(30)));
        assert!(s.measure.contains(&Measure::Recovery));
    }

    #[test]
    fn recovery_block_adds_recovery_to_default_measures() {
        let text = format!("{M1}recovery: {{ kill_after: 30s }}\n");
        let s = Scenario::parse(&text).expect("parse");
        assert_eq!(s.measure.last(), Some(&Measure::Recovery));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let text = format!("{M1}bogus: 1\n");
        assert!(matches!(Scenario::parse(&text), Err(BenchError::Config(_))));
        let text = format!("{M1}simulator:\n  seeed: 1\n");
        assert!(Scenario::parse(&text).is_err());
    }

    #[test]
    fn bad_duration_and_zero_duration_are_rejected() {
        assert!(Scenario::parse(&M1.replace("60s", "soon")).is_err());
        assert!(Scenario::parse(&M1.replace("60s", "0s")).is_err());
    }

    #[test]
    fn rates_and_scale_are_range_checked() {
        let bad_rate = format!("{M1}simulator:\n  malformed_rate: 1.5\n");
        assert!(Scenario::parse(&bad_rate).is_err());
        let zero_scale = format!("{M1}simulator:\n  scale: 0\n");
        assert!(Scenario::parse(&zero_scale).is_err());
    }

    #[test]
    fn recovery_measure_and_block_must_agree() {
        let measure_without_block = format!("{M1}measure: [recovery]\n");
        assert!(Scenario::parse(&measure_without_block).is_err());
        let block_without_measure =
            format!("{M1}measure: [throughput]\nrecovery: {{ kill_after: 30s }}\n");
        assert!(Scenario::parse(&block_without_measure).is_err());
    }

    #[test]
    fn kill_point_must_be_inside_the_window() {
        let text = format!("{M1}recovery: {{ kill_after: 60s }}\n");
        assert!(Scenario::parse(&text).is_err());
        let s = Scenario::parse(&format!("{M1}recovery: {{ kill_after: 30s }}\n")).expect("parse");
        assert!(s.clone().with_duration(Duration::from_secs(10)).is_err());
        assert!(s.with_duration(Duration::from_secs(40)).is_ok());
    }

    #[test]
    fn duplicate_measures_collapse_and_empty_list_fails() {
        let text = format!("{M1}measure: [latency, latency, storage]\n");
        let s = Scenario::parse(&text).expect("parse");
        assert_eq!(s.measure, vec![Measure::Latency, Measure::Storage]);
        assert!(Scenario::parse(&format!("{M1}measure: []\n")).is_err());
    }

    #[test]
    fn scenario_names_must_be_file_safe() {
        assert!(Scenario::parse(&M1.replace("m1-steady-state", "bad name")).is_err());
        assert!(Scenario::parse(&M1.replace("m1-steady-state", "../etc")).is_err());
    }

    #[test]
    fn fsync_renders_each_mode() {
        assert_eq!(FsyncMode::parse("always").expect("p").render(), "always");
        assert_eq!(FsyncMode::parse("never").expect("p").render(), "never");
        assert_eq!(
            FsyncMode::parse("50ms").expect("p").render(),
            "{ interval: 50ms }"
        );
        assert!(FsyncMode::parse("sometimes").is_err());
    }
}
