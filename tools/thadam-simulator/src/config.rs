//! The fleet definition (`fleet.yaml`): fleet size, stream rates, the
//! behavior model, and every chaos knob (plan section 3).
//!
//! Parsed with the workspace's YAML crate (`serde-saphyr`, ADR-0012) directly
//! into the typed structs below; unknown keys are rejected.

use serde::Deserialize;

use crate::error::SimError;
use crate::{DEFAULT_SEED, DEFAULT_TOPIC_PREFIX};

/// A probability or rate, expected in `0.0..=1.0` where it is a probability.
type Fraction = f64;

/// The whole fleet configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetConfig {
    /// The master seed. One seed fully determines fleet behavior.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// The MQTT topic prefix; levels below it are `{site}/{robot}/{stream}`.
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    /// Robots-per-site multiplier (benchmarks raise this).
    #[serde(default = "one")]
    pub scale: u32,
    /// Site names; robots are numbered globally in site order
    /// (`site-a/robot-01..03`, `site-b/robot-04..06` at the defaults).
    #[serde(default = "default_sites")]
    pub sites: Vec<String>,
    /// Robots per site before the `scale` multiplier.
    #[serde(default = "default_robots_per_site")]
    pub robots_per_site: u32,
    /// Stream rates and batching.
    #[serde(default)]
    pub rates: Rates,
    /// The operating-mode behavior model.
    #[serde(default)]
    pub behavior: Behavior,
    /// The state stream: publish heartbeat and the pipeline `on_change`
    /// heartbeat the oracle mirrors to predict `state_repeat`.
    #[serde(default)]
    pub state: StateConfig,
    /// Fault-injection knobs. Every decision is seeded and oracle-recorded.
    #[serde(default)]
    pub chaos: ChaosConfig,
}

/// Stream rates and per-message batch sizes.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rates {
    /// Telemetry messages per second per robot (one sample each).
    #[serde(default = "telemetry_hz")]
    pub telemetry_hz: f64,
    /// Joint sample rate in Hz.
    #[serde(default = "joints_hz")]
    pub joints_hz: f64,
    /// Joint samples per message.
    #[serde(default = "joints_batch")]
    pub joints_batch: u32,
    /// IMU sample rate in Hz.
    #[serde(default = "imu_hz")]
    pub imu_hz: f64,
    /// IMU samples per message.
    #[serde(default = "imu_batch")]
    pub imu_batch: u32,
    /// Pose sample rate in Hz.
    #[serde(default = "pose_hz")]
    pub pose_hz: f64,
    /// Pose samples per message.
    #[serde(default = "pose_batch")]
    pub pose_batch: u32,
    /// Background (non-behavior) event rate per robot, ~Poisson.
    #[serde(default = "events_per_second")]
    pub events_per_second: f64,
    /// The share of background events with `level: debug` (pipeline-filtered).
    #[serde(default = "debug_event_share")]
    pub debug_event_share: Fraction,
}

/// The operating-mode state machine pacing, in seconds.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Behavior {
    /// `[min, max]` dwell in `IDLE` before starting a cycle.
    #[serde(default = "idle_seconds")]
    pub idle_seconds: [f64; 2],
    /// `[min, max]` cycle (`RUNNING`) duration when uninterrupted.
    #[serde(default = "cycle_seconds")]
    pub cycle_seconds: [f64; 2],
    /// `[min, max]` dwell in `PAUSED` before resuming.
    #[serde(default = "pause_seconds")]
    pub pause_seconds: [f64; 2],
    /// `[min, max]` dwell in `FAULT` before recovering to `IDLE`.
    #[serde(default = "fault_seconds")]
    pub fault_seconds: [f64; 2],
    /// `[min, max]` dwell in `ESTOP` before clearing to `IDLE`.
    #[serde(default = "estop_seconds")]
    pub estop_seconds: [f64; 2],
    /// Per-cycle probability of a mid-cycle fault.
    #[serde(default = "fault_probability")]
    pub fault_probability: Fraction,
    /// Per-cycle probability of a mid-cycle pause.
    #[serde(default = "pause_probability")]
    pub pause_probability: Fraction,
    /// Per-cycle probability of a mid-cycle emergency stop.
    #[serde(default = "estop_probability")]
    pub estop_probability: Fraction,
}

/// The `state` stream pacing.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    /// Republish the current state this often even when nothing changed.
    #[serde(default = "state_heartbeat_seconds")]
    pub heartbeat_seconds: f64,
    /// The `on_change` transform's heartbeat (pipeline config), mirrored here
    /// so the oracle can predict which repeats still pass as `deliver`.
    #[serde(default = "on_change_heartbeat_seconds")]
    pub on_change_heartbeat_seconds: f64,
    /// The program names; robot `i` runs `programs[i % len]`.
    #[serde(default = "default_programs")]
    pub programs: Vec<String>,
}

/// Clock skew: one robot publishes timestamps offset from its real clock.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockSkewRule {
    /// The robot name.
    pub robot: String,
    /// Added to every `ts_us` the robot emits (seconds; may be negative).
    pub offset_seconds: f64,
}

/// One robot drops `ts_us` from a share of one stream's messages.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingTsRule {
    /// The robot name.
    pub robot: String,
    /// The stream it applies to.
    pub stream: String,
    /// The share of affected messages.
    pub rate: Fraction,
}

/// One robot delays a share of one stream's messages (reordering).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutOfOrderRule {
    /// The robot name.
    pub robot: String,
    /// The stream it applies to.
    pub stream: String,
    /// The share of affected messages.
    pub rate: Fraction,
    /// The added delay in milliseconds.
    pub delay_ms: u64,
}

/// The chaos knobs (plan section 3). Rates are per-message probabilities.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChaosConfig {
    /// Probability a message is published as truncated JSON (quarantined).
    #[serde(default = "malformed_rate")]
    pub malformed_rate: Fraction,
    /// Probability a healthy message is re-published 0..3 s later with the
    /// same `seq` (exercises message-level dedup).
    #[serde(default = "duplicate_publish_rate")]
    pub duplicate_publish_rate: Fraction,
    /// `[min, max]` delay of a duplicate re-publish, seconds.
    #[serde(default = "duplicate_delay_seconds")]
    pub duplicate_delay_seconds: [f64; 2],
    /// Probability a telemetry `motor_temp` reads 400 C (validate-flagged).
    #[serde(default = "out_of_range_rate")]
    pub out_of_range_rate: Fraction,
    /// One robot runs with a skewed clock (default `robot-04`, +90 s).
    #[serde(default = "default_clock_skew")]
    pub clock_skew: Option<ClockSkewRule>,
    /// One robot omits `ts_us` on a share of one stream (default
    /// `robot-05`, 1% of telemetry).
    #[serde(default = "default_missing_ts")]
    pub missing_ts: Option<MissingTsRule>,
    /// One robot reorders a share of one stream (default `robot-03`,
    /// 1% of pose by 500 ms).
    #[serde(default = "default_out_of_order")]
    pub out_of_order: Option<OutOfOrderRule>,
}

/// One robot in the built fleet: its position in topic order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RobotSpec {
    /// The 0-based global index (drives per-robot RNG derivation).
    pub index: u32,
    /// The site name.
    pub site: String,
    /// The robot name, `robot-{n:02}` with `n` numbering across the fleet.
    pub name: String,
}

/// The fleet: validated configuration plus the expanded roster.
#[derive(Clone, Debug)]
pub struct Fleet {
    /// The configuration the fleet was built from.
    pub config: FleetConfig,
    /// The roster in `site_a robot-01..` then `site_b ...` order.
    pub robots: Vec<RobotSpec>,
}

fn default_seed() -> u64 {
    DEFAULT_SEED
}
fn default_topic_prefix() -> String {
    DEFAULT_TOPIC_PREFIX.to_owned()
}
const fn one() -> u32 {
    1
}
fn default_sites() -> Vec<String> {
    vec!["site-a".to_owned(), "site-b".to_owned()]
}
const fn default_robots_per_site() -> u32 {
    3
}
const fn telemetry_hz() -> f64 {
    10.0
}
const fn joints_hz() -> f64 {
    100.0
}
const fn joints_batch() -> u32 {
    10
}
const fn imu_hz() -> f64 {
    200.0
}
const fn imu_batch() -> u32 {
    20
}
const fn pose_hz() -> f64 {
    50.0
}
const fn pose_batch() -> u32 {
    5
}
const fn events_per_second() -> f64 {
    0.5
}
const fn debug_event_share() -> f64 {
    0.4
}
fn idle_seconds() -> [f64; 2] {
    [2.0, 8.0]
}
fn cycle_seconds() -> [f64; 2] {
    [5.0, 20.0]
}
fn pause_seconds() -> [f64; 2] {
    [1.0, 5.0]
}
fn fault_seconds() -> [f64; 2] {
    [2.0, 6.0]
}
fn estop_seconds() -> [f64; 2] {
    [5.0, 15.0]
}
const fn fault_probability() -> f64 {
    0.02
}
const fn pause_probability() -> f64 {
    0.03
}
const fn estop_probability() -> f64 {
    0.005
}
const fn state_heartbeat_seconds() -> f64 {
    60.0
}
const fn on_change_heartbeat_seconds() -> f64 {
    300.0
}
fn default_programs() -> Vec<String> {
    vec![
        "pick-and-place".to_owned(),
        "palletize".to_owned(),
        "line-inspect".to_owned(),
    ]
}
const fn malformed_rate() -> f64 {
    0.001
}
const fn duplicate_publish_rate() -> f64 {
    0.005
}
fn duplicate_delay_seconds() -> [f64; 2] {
    [0.0, 3.0]
}
const fn out_of_range_rate() -> f64 {
    0.0005
}

fn default_clock_skew() -> Option<ClockSkewRule> {
    Some(ClockSkewRule {
        robot: "robot-04".to_owned(),
        offset_seconds: 90.0,
    })
}

fn default_missing_ts() -> Option<MissingTsRule> {
    Some(MissingTsRule {
        robot: "robot-05".to_owned(),
        stream: "telemetry".to_owned(),
        rate: 0.01,
    })
}

fn default_out_of_order() -> Option<OutOfOrderRule> {
    Some(OutOfOrderRule {
        robot: "robot-03".to_owned(),
        stream: "pose".to_owned(),
        rate: 0.01,
        delay_ms: 500,
    })
}

impl Default for Rates {
    fn default() -> Self {
        Self {
            telemetry_hz: telemetry_hz(),
            joints_hz: joints_hz(),
            joints_batch: joints_batch(),
            imu_hz: imu_hz(),
            imu_batch: imu_batch(),
            pose_hz: pose_hz(),
            pose_batch: pose_batch(),
            events_per_second: events_per_second(),
            debug_event_share: debug_event_share(),
        }
    }
}

impl Default for Behavior {
    fn default() -> Self {
        Self {
            idle_seconds: idle_seconds(),
            cycle_seconds: cycle_seconds(),
            pause_seconds: pause_seconds(),
            fault_seconds: fault_seconds(),
            estop_seconds: estop_seconds(),
            fault_probability: fault_probability(),
            pause_probability: pause_probability(),
            estop_probability: estop_probability(),
        }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            heartbeat_seconds: state_heartbeat_seconds(),
            on_change_heartbeat_seconds: on_change_heartbeat_seconds(),
            programs: default_programs(),
        }
    }
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            malformed_rate: malformed_rate(),
            duplicate_publish_rate: duplicate_publish_rate(),
            duplicate_delay_seconds: duplicate_delay_seconds(),
            out_of_range_rate: out_of_range_rate(),
            clock_skew: default_clock_skew(),
            missing_ts: default_missing_ts(),
            out_of_order: default_out_of_order(),
        }
    }
}

impl Default for FleetConfig {
    fn default() -> Self {
        Self {
            seed: default_seed(),
            topic_prefix: default_topic_prefix(),
            scale: one(),
            sites: default_sites(),
            robots_per_site: default_robots_per_site(),
            rates: Rates::default(),
            behavior: Behavior::default(),
            state: StateConfig::default(),
            chaos: ChaosConfig::default(),
        }
    }
}

fn check_probability(problems: &mut Vec<String>, field: &str, value: f64) {
    if !(0.0..=1.0).contains(&value) {
        problems.push(format!("{field}: {value} is not a probability (0..=1)"));
    }
}

fn check_rate(problems: &mut Vec<String>, field: &str, value: f64) {
    if !(value.is_finite() && value > 0.0) {
        problems.push(format!("{field}: {value} must be positive"));
    }
}

fn check_range(problems: &mut Vec<String>, field: &str, range: [f64; 2]) {
    if !(range[0].is_finite() && range[1].is_finite() && range[0] >= 0.0 && range[0] <= range[1]) {
        problems.push(format!(
            "{field}: [{}, {}] must satisfy 0 <= min <= max",
            range[0], range[1]
        ));
    }
}

impl FleetConfig {
    /// Parses a `fleet.yaml` document. Unknown keys, duplicate keys, and
    /// non-strict YAML (`yes`/`no` booleans, tags) are errors, matching the
    /// dialect Thadam itself enforces (ADR-0012).
    pub fn from_yaml(text: &str) -> Result<FleetConfig, SimError> {
        let options = serde_saphyr::options! {
            strict_booleans: true,
            reject_unsupported_tags: true,
            with_snippet: false,
            emit_comments: false,
            duplicate_keys: serde_saphyr::options::DuplicateKeyPolicy::Error,
            merge_keys: serde_saphyr::options::MergeKeyPolicy::AsOrdinary,
            budget: serde_saphyr::budget! { max_nodes: 10_000, max_depth: 64 },
        };
        let config: FleetConfig = serde_saphyr::from_str_with_options(text, options)
            .map_err(|e| SimError::Config(e.to_string()))?;
        let problems = config.problems();
        if problems.is_empty() {
            Ok(config)
        } else {
            Err(SimError::Config(problems.join("; ")))
        }
    }

    /// Every validation problem, empty when the configuration is sound.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.scale == 0 {
            out.push("scale: must be at least 1".to_owned());
        }
        if self.robots_per_site == 0 {
            out.push("robots_per_site: must be at least 1".to_owned());
        }
        if self.sites.is_empty() {
            out.push("sites: at least one site is required".to_owned());
        }
        for site in &self.sites {
            if site.is_empty() || site.contains('/') {
                out.push(format!(
                    "sites: `{site}` must be a single non-empty topic level"
                ));
            }
        }
        if self.topic_prefix.is_empty() || self.topic_prefix.starts_with('/') {
            out.push("topic_prefix: must be a non-empty relative prefix".to_owned());
        }
        let r = &self.rates;
        check_rate(&mut out, "rates.telemetry_hz", r.telemetry_hz);
        check_rate(&mut out, "rates.joints_hz", r.joints_hz);
        check_rate(&mut out, "rates.imu_hz", r.imu_hz);
        check_rate(&mut out, "rates.pose_hz", r.pose_hz);
        check_rate(&mut out, "rates.events_per_second", r.events_per_second);
        check_probability(&mut out, "rates.debug_event_share", r.debug_event_share);
        for (name, batch) in [
            ("rates.joints_batch", r.joints_batch),
            ("rates.imu_batch", r.imu_batch),
            ("rates.pose_batch", r.pose_batch),
        ] {
            if batch == 0 || batch > 10_000 {
                out.push(format!("{name}: must be 1..=10000, got {batch}"));
            }
        }
        let b = &self.behavior;
        check_range(&mut out, "behavior.idle_seconds", b.idle_seconds);
        check_range(&mut out, "behavior.cycle_seconds", b.cycle_seconds);
        check_range(&mut out, "behavior.pause_seconds", b.pause_seconds);
        check_range(&mut out, "behavior.fault_seconds", b.fault_seconds);
        check_range(&mut out, "behavior.estop_seconds", b.estop_seconds);
        check_probability(&mut out, "behavior.fault_probability", b.fault_probability);
        check_probability(&mut out, "behavior.pause_probability", b.pause_probability);
        check_probability(&mut out, "behavior.estop_probability", b.estop_probability);
        if self.state.programs.is_empty() {
            out.push("state.programs: at least one program is required".to_owned());
        }
        check_rate(
            &mut out,
            "state.heartbeat_seconds",
            self.state.heartbeat_seconds,
        );
        check_rate(
            &mut out,
            "state.on_change_heartbeat_seconds",
            self.state.on_change_heartbeat_seconds,
        );
        let c = &self.chaos;
        check_probability(&mut out, "chaos.malformed_rate", c.malformed_rate);
        check_probability(
            &mut out,
            "chaos.duplicate_publish_rate",
            c.duplicate_publish_rate,
        );
        check_probability(&mut out, "chaos.out_of_range_rate", c.out_of_range_rate);
        check_range(
            &mut out,
            "chaos.duplicate_delay_seconds",
            c.duplicate_delay_seconds,
        );
        if let Some(m) = &c.missing_ts {
            check_probability(&mut out, "chaos.missing_ts.rate", m.rate);
        }
        if let Some(o) = &c.out_of_order {
            check_probability(&mut out, "chaos.out_of_order.rate", o.rate);
        }
        out
    }

    /// Expands and validates the roster. Chaos robot and stream names must
    /// resolve against it.
    pub fn build_fleet(&self) -> Result<Fleet, SimError> {
        let problems = self.problems();
        if !problems.is_empty() {
            return Err(SimError::Config(problems.join("; ")));
        }
        let per_site = self.robots_per_site.saturating_mul(self.scale);
        let mut robots = Vec::with_capacity(self.sites.len() * per_site as usize);
        let mut n = 0u32;
        for site in &self.sites {
            for _ in 0..per_site {
                n += 1;
                robots.push(RobotSpec {
                    index: n - 1,
                    site: site.clone(),
                    name: format!("robot-{n:02}"),
                });
            }
        }
        let mut bad = Vec::new();
        let names: Vec<&str> = robots.iter().map(|r| r.name.as_str()).collect();
        let check_stream = |bad: &mut Vec<String>, stream: &str, rule: &str| {
            if crate::types::Stream::parse(stream).is_none() {
                bad.push(format!(
                    "chaos.{rule}.stream: `{stream}` is not a stream name"
                ));
            }
        };
        let check_robot = |bad: &mut Vec<String>, names: &[&str], robot: &str, rule: &str| {
            if !names.contains(&robot) {
                bad.push(format!(
                    "chaos.{rule}.robot: `{robot}` is not in the fleet ({})",
                    names.join(", ")
                ));
            }
        };
        let c = &self.chaos;
        if let Some(s) = &c.clock_skew {
            check_robot(&mut bad, &names, &s.robot, "clock_skew");
        }
        if let Some(m) = &c.missing_ts {
            check_robot(&mut bad, &names, &m.robot, "missing_ts");
            check_stream(&mut bad, &m.stream, "missing_ts");
        }
        if let Some(o) = &c.out_of_order {
            check_robot(&mut bad, &names, &o.robot, "out_of_order");
            check_stream(&mut bad, &o.stream, "out_of_order");
        }
        if !bad.is_empty() {
            return Err(SimError::Config(bad.join("; ")));
        }
        Ok(Fleet {
            config: self.clone(),
            robots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_build_the_documented_fleet() {
        let fleet = FleetConfig::default()
            .build_fleet()
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(fleet.robots.len(), 6);
        let names: Vec<(&str, &str)> = fleet
            .robots
            .iter()
            .map(|r| (r.site.as_str(), r.name.as_str()))
            .collect();
        assert_eq!(
            names,
            [
                ("site-a", "robot-01"),
                ("site-a", "robot-02"),
                ("site-a", "robot-03"),
                ("site-b", "robot-04"),
                ("site-b", "robot-05"),
                ("site-b", "robot-06"),
            ]
        );
    }

    #[test]
    fn scale_multiplies_robots_per_site() {
        let config = FleetConfig {
            scale: 2,
            ..FleetConfig::default()
        };
        let fleet = config.build_fleet().unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(fleet.robots.len(), 12);
        assert_eq!(fleet.robots[5].name, "robot-06");
        assert_eq!(fleet.robots[6].site, "site-b");
        assert_eq!(fleet.robots[11].name, "robot-12");
    }

    #[test]
    fn yaml_round_trip_with_overrides() {
        let yaml = r#"
seed: 7
scale: 1
rates:
  telemetry_hz: 20
  joints_batch: 5
chaos:
  malformed_rate: 0.01
  clock_skew:
    robot: robot-02
    offset_seconds: -30
"#;
        let config = FleetConfig::from_yaml(yaml).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(config.seed, 7);
        assert_eq!(config.rates.telemetry_hz, 20.0);
        assert_eq!(config.rates.joints_batch, 5);
        assert_eq!(config.chaos.malformed_rate, 0.01);
        let skew = config.chaos.clock_skew.unwrap_or_else(|| panic!("skew"));
        assert_eq!(skew.robot, "robot-02");
        assert_eq!(skew.offset_seconds, -30.0);
        // Untouched sections keep defaults.
        assert_eq!(config.rates.joints_hz, 100.0);
        assert!(config.chaos.out_of_order.is_some());
    }

    #[test]
    fn unknown_and_invalid_keys_are_rejected() {
        assert!(FleetConfig::from_yaml("nope: 1").is_err());
        assert!(FleetConfig::from_yaml("scale: 0").is_err());
        assert!(FleetConfig::from_yaml("chaos:\n  malformed_rate: 1.5\n").is_err());
        assert!(FleetConfig::from_yaml("seed: 1\nseed: 2\n").is_err());
    }

    #[test]
    fn chaos_roles_must_reference_real_robots_and_streams() {
        let config = FleetConfig {
            chaos: ChaosConfig {
                clock_skew: Some(ClockSkewRule {
                    robot: "robot-99".to_owned(),
                    offset_seconds: 1.0,
                }),
                ..ChaosConfig::default()
            },
            ..FleetConfig::default()
        };
        let err = config.build_fleet().err().map(|e| e.to_string());
        assert!(
            err.as_ref().is_some_and(|e| e.contains("robot-99")),
            "{err:?}"
        );

        let config = FleetConfig {
            chaos: ChaosConfig {
                missing_ts: Some(MissingTsRule {
                    robot: "robot-01".to_owned(),
                    stream: "lidar".to_owned(),
                    rate: 0.5,
                }),
                ..ChaosConfig::default()
            },
            ..FleetConfig::default()
        };
        assert!(config.build_fleet().is_err());
    }
}
