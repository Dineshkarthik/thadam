//! Payload generation: one `v: 1` JSON document per message, shaped exactly
//! the way `examples/edge-capture/thadam/pipeline.yaml` decodes them.
//!
//! Determinism contract: given the same seed, robot index, and tick sequence
//! (with `now_us` supplied), the byte content of every payload is identical.
//! All random values come from the robot's pinned RNG streams, and the draw
//! order inside each generator is fixed and documented; timestamps are the
//! only externally supplied input.

use serde_json::{Value, json};

use crate::behavior::{ALARM_CODES, ESTOP_CODE, Kinematics, Model, ModelEvent};
use crate::config::{FleetConfig, Rates, RobotSpec};
use crate::rng::SimRng;
use crate::types::{Stream, flag};

/// The payload format version (plan section 4).
pub const PAYLOAD_VERSION: u64 = 1;

/// The number of joints (7-DOF arms).
pub const JOINT_COUNT: usize = 7;

/// A generated message, before any chaos is applied.
#[derive(Clone, Debug)]
pub struct Generated {
    /// Which stream (topic level and dataset).
    pub stream: Stream,
    /// The per-(robot, stream) sequence number.
    pub seq: u64,
    /// The JSON payload.
    pub payload: Value,
    /// One entry per pipeline row the message explodes to:
    /// `(sample_idx, ts_us)` with the clock-skew offset (if any) applied.
    /// The missing-timestamp chaos rule blanks entries to `None`.
    pub samples: Vec<(u32, Option<u64>)>,
    /// For `state` messages: `(mode, cycle_id, program)`, consumed by the
    /// `on_change` expectation model.
    pub state_fields: Option<(String, u64, String)>,
    /// For `events` messages: the severity; `debug` is pipeline-filtered.
    pub event_level: Option<&'static str>,
    /// Quality flags the pipeline assigns on this content (still delivered).
    pub flags: Vec<&'static str>,
}

/// Per-robot, per-tick message factory: behavior model plus sequence numbers
/// plus kinematic constants, all seeded.
#[derive(Debug)]
pub struct RobotGen {
    spec: RobotSpec,
    model: Model,
    seq: [u64; 6],
    rng_values: SimRng,
    rng_events: SimRng,
    /// Clock-skew offset added to every `ts_us` this robot writes (chaos).
    ts_offset_us: i64,
    home: [f64; JOINT_COUNT],
    amps: [f64; JOINT_COUNT],
    state_published: bool,
    state_next_heartbeat_tick: u64,
    state_heartbeat_ticks: u64,
    telemetry_every: u64,
    joints_every: u64,
    imu_every: u64,
    pose_every: u64,
    last_tick: Option<u64>,
}

fn stream_index(stream: Stream) -> usize {
    match stream {
        Stream::Telemetry => 0,
        Stream::Joints => 1,
        Stream::Imu => 2,
        Stream::Pose => 3,
        Stream::State => 4,
        Stream::Events => 5,
    }
}

/// Ticks between messages for a batched stream, at least one.
fn batch_interval_ticks(hz: f64, batch: u32) -> u64 {
    ((batch as f64 / hz) * crate::behavior::TICKS_PER_SECOND as f64)
        .round()
        .max(1.0) as u64
}

/// Ticks between single-sample messages, at least one.
fn interval_ticks(hz: f64) -> u64 {
    (crate::behavior::TICKS_PER_SECOND as f64 / hz)
        .round()
        .max(1.0) as u64
}

impl RobotGen {
    /// Builds the generator for one robot of the fleet.
    pub fn new(spec: RobotSpec, cfg: &FleetConfig) -> RobotGen {
        let idx = spec.index;
        let program = cfg.state.programs[idx as usize % cfg.state.programs.len()].clone();
        let model = Model::new(cfg.seed, idx, &cfg.behavior, program);
        let mut rng_setup = SimRng::new(cfg.seed, &format!("robot-{idx}/setup"));
        let mut home = [0.0; JOINT_COUNT];
        let mut amps = [0.0; JOINT_COUNT];
        let base_home = [0.1, -1.2, 0.8, 0.0, 1.4, -0.3, 0.7];
        let base_amps = [0.55, 0.5, 0.4, 0.35, 0.5, 0.45, 0.55];
        for j in 0..JOINT_COUNT {
            home[j] = base_home[j] + rng_setup.noise(0.05);
            amps[j] = base_amps[j] + rng_setup.noise(0.05).abs();
        }
        let ts_offset_us = cfg
            .chaos
            .clock_skew
            .as_ref()
            .filter(|rule| rule.robot == spec.name)
            .map_or(0, |rule| (rule.offset_seconds * 1e6).round() as i64);
        let r = &cfg.rates;
        RobotGen {
            spec,
            model,
            seq: [0; 6],
            rng_values: SimRng::new(cfg.seed, &format!("robot-{idx}/values")),
            rng_events: SimRng::new(cfg.seed, &format!("robot-{idx}/events")),
            ts_offset_us,
            home,
            amps,
            state_published: false,
            state_next_heartbeat_tick: 0,
            state_heartbeat_ticks: crate::behavior::seconds_to_ticks(cfg.state.heartbeat_seconds),
            telemetry_every: interval_ticks(r.telemetry_hz),
            joints_every: batch_interval_ticks(r.joints_hz, r.joints_batch),
            imu_every: batch_interval_ticks(r.imu_hz, r.imu_batch),
            pose_every: batch_interval_ticks(r.pose_hz, r.pose_batch),
            last_tick: None,
        }
    }

    /// The robot identity.
    pub fn spec(&self) -> &RobotSpec {
        &self.spec
    }

    /// The current operating mode (runner uses it for status logs).
    pub fn mode(&self) -> crate::behavior::Mode {
        self.model.mode()
    }

    /// The clock-skew offset applied to this robot's timestamps (0 for most).
    pub fn ts_offset_us(&self) -> i64 {
        self.ts_offset_us
    }

    fn next_seq(&mut self, stream: Stream) -> u64 {
        let seq = &mut self.seq[stream_index(stream)];
        *seq += 1;
        *seq
    }

    /// Applies the clock-skew offset to a wall timestamp.
    fn stamp(&self, now_us: u64) -> u64 {
        now_us.saturating_add_signed(self.ts_offset_us)
    }

    fn base_flags(&self) -> Vec<&'static str> {
        if self.ts_offset_us != 0 {
            vec![flag::CLOCK_SKEW]
        } else {
            Vec::new()
        }
    }

    /// Generates every message due on this tick, in a fixed stream order
    /// (telemetry, joints, imu, pose, state, events) so RNG draws are
    /// deterministic.
    pub fn tick(&mut self, tick: u64, now_us: u64, rates: &Rates) -> Vec<Generated> {
        let dt_us = self
            .last_tick
            .map_or(100_000, |prev| tick.saturating_sub(prev).max(1) * 100_000);
        self.last_tick = Some(tick);

        let events = self.model.tick(tick, now_us);
        self.model.advance(dt_us);

        let mut out = Vec::new();
        if tick.is_multiple_of(self.telemetry_every) {
            out.push(self.telemetry(now_us));
        }
        if tick.is_multiple_of(self.joints_every) {
            out.push(self.joints(now_us, rates));
        }
        if tick.is_multiple_of(self.imu_every) {
            out.push(self.imu(now_us, rates));
        }
        if tick.is_multiple_of(self.pose_every) {
            out.push(self.pose(now_us, rates));
        }
        if let Some(state) = self.maybe_state(tick, now_us, &events) {
            out.push(state);
        }
        self.events(now_us, rates, &events, &mut out);
        out
    }

    /// 10 Hz, one sample: battery, rails, temperatures.
    fn telemetry(&mut self, now_us: u64) -> Generated {
        let kin = *self.model.kinematics_mut();
        let load = self.load(&kin);
        // Draw order is fixed and part of the determinism contract:
        // voltage, current, cpu, then the seven motor temperatures.
        let voltage_mv = (42_000.0 + kin.soc * 84.0 + self.rng_values.noise(60.0)).round();
        let current_ma = if self.model.is_running() {
            (2_500.0 + 1_500.0 * load + self.rng_values.noise(300.0)).round()
        } else {
            (-1_200.0 + self.rng_values.noise(120.0)).round()
        };
        let cpu_temp_dc = ((45.0 + 25.0 * load + self.rng_values.noise(2.0)) * 10.0).round();
        let mut motor_temp_dc = [0.0_f64; JOINT_COUNT];
        for (j, t) in motor_temp_dc.iter_mut().enumerate() {
            let phase = kin.phase + j as f64 * 0.7;
            *t = ((38.0 + 8.0 * load + 2.0 * phase.sin() + self.rng_values.noise(1.5)) * 10.0)
                .round();
        }
        let ts = self.stamp(now_us);
        let seq = self.next_seq(Stream::Telemetry);
        let payload = json!({
            "v": PAYLOAD_VERSION,
            "seq": seq,
            "ts_us": ts,
            "soc_pct": round1(kin.soc),
            "voltage_mv": voltage_mv,
            "current_ma": current_ma,
            "cpu_temp_dc": cpu_temp_dc,
            "motor_temp_dc": motor_temp_dc,
        });
        Generated {
            stream: Stream::Telemetry,
            seq,
            payload,
            samples: vec![(0, Some(ts))],
            state_fields: None,
            event_level: None,
            flags: self.base_flags(),
        }
    }

    /// The instantaneous work factor 0..1 driving loads and drains.
    fn load(&self, kin: &Kinematics) -> f64 {
        if self.model.is_running() {
            0.5 + 0.4 * (kin.phase * 0.5).sin().abs()
        } else {
            0.05
        }
    }

    /// One batched stream message: `samples_us` step between samples,
    /// covering the interval right before `now_us`.
    fn batch_samples(
        &mut self,
        now_us: u64,
        count: u32,
        step_us: u64,
        mut fill: impl FnMut(&mut Self, u64) -> Value,
    ) -> (Vec<Value>, Vec<(u32, Option<u64>)>) {
        let total = u64::from(count - 1) * step_us;
        let mut values = Vec::with_capacity(count as usize);
        let mut stamps = Vec::with_capacity(count as usize);
        for i in 0..count {
            let at = now_us.saturating_sub(total) + u64::from(i) * step_us;
            values.push(fill(self, at));
            stamps.push((i, Some(self.stamp(at))));
        }
        (values, stamps)
    }

    /// Batched 7-DOF joint positions/velocities/efforts.
    fn joints(&mut self, now_us: u64, rates: &Rates) -> Generated {
        let step_us = (1e6 / rates.joints_hz).round() as u64;
        let count = rates.joints_batch;
        let dphase = step_us as f64 / 1e6 * 1.1;
        let (values, stamps) = self.batch_samples(now_us, count, step_us, |me, _at| {
            let running = f64::from(u8::from(me.model.is_running()));
            let idle_blend = 0.02 + 0.98 * running;
            let mut pos = [0.0; JOINT_COUNT];
            let mut vel = [0.0; JOINT_COUNT];
            let mut eff = [0.0; JOINT_COUNT];
            let phase = me.model.kinematics().phase;
            for j in 0..JOINT_COUNT {
                let arg = phase + j as f64 * 0.9;
                let omega = if me.model.is_running() { 1.1 } else { 0.03 };
                pos[j] = me.home[j] + me.amps[j] * arg.sin() * idle_blend;
                vel[j] = me.amps[j] * omega * arg.cos() * running + me.rng_values.noise(0.002);
                eff[j] = 12.0 * vel[j] + me.rng_values.noise(0.6) * (0.3 + running);
            }
            me.model.kinematics_mut().phase += dphase;
            json!({ "ts_us": 0, "pos": pos, "vel": vel, "eff": eff })
        });
        // Stamp real timestamps into the placeholder slots.
        let mut values = values;
        for (value, (_, ts)) in values.iter_mut().zip(stamps.iter()) {
            value["ts_us"] = json!(ts);
        }
        let seq = self.next_seq(Stream::Joints);
        Generated {
            stream: Stream::Joints,
            seq,
            payload: json!({ "v": PAYLOAD_VERSION, "seq": seq, "samples": values }),
            samples: stamps,
            state_fields: None,
            event_level: None,
            flags: self.base_flags(),
        }
    }

    /// Batched IMU: accel in g, gyro in deg/s, quaternion.
    fn imu(&mut self, now_us: u64, rates: &Rates) -> Generated {
        let step_us = (1e6 / rates.imu_hz).round() as u64;
        let count = rates.imu_batch;
        let dtheta = step_us as f64 / 1e6 * 0.25;
        let (values, stamps) = self.batch_samples(now_us, count, step_us, |me, _at| {
            let running = f64::from(u8::from(me.model.is_running()));
            let theta = me.model.kinematics().theta;
            let yaw = theta + std::f64::consts::FRAC_PI_2;
            let yaw_rate_dps = if me.model.is_running() { 14.3 } else { 0.86 };
            let ax = 0.02 * yaw_rate_dps * running + me.rng_values.noise(0.02);
            let ay = me.rng_values.noise(0.02);
            let az = 1.0
                + 0.05 * running * (me.model.kinematics().phase * 2.0).sin()
                + me.rng_values.noise(0.01);
            let gx = me.rng_values.noise(0.3);
            let gy = me.rng_values.noise(0.3);
            let gz = yaw_rate_dps + me.rng_values.noise(0.5);
            me.model.kinematics_mut().theta += dtheta;
            json!({
                "ts_us": 0,
                "accel": [round3(ax), round3(ay), round3(az)],
                "gyro": [round2(gx), round2(gy), round2(gz)],
                "quat": quat_from_yaw(yaw),
            })
        });
        let mut values = values;
        for (value, (_, ts)) in values.iter_mut().zip(stamps.iter()) {
            value["ts_us"] = json!(ts);
        }
        let seq = self.next_seq(Stream::Imu);
        Generated {
            stream: Stream::Imu,
            seq,
            payload: json!({ "v": PAYLOAD_VERSION, "seq": seq, "samples": values }),
            samples: stamps,
            state_fields: None,
            event_level: None,
            flags: self.base_flags(),
        }
    }

    /// Batched map-frame pose along the robot's closed loop.
    fn pose(&mut self, now_us: u64, rates: &Rates) -> Generated {
        let step_us = (1e6 / rates.pose_hz).round() as u64;
        let count = rates.pose_batch;
        let dtheta = step_us as f64 / 1e6 * 0.25;
        let (values, stamps) = self.batch_samples(now_us, count, step_us, |me, _at| {
            let kin = *me.model.kinematics_mut();
            let theta = kin.theta;
            let yaw = theta + std::f64::consts::FRAC_PI_2;
            let x = kin.path_center.0 + kin.path_radius * theta.cos();
            let y = kin.path_center.1 + kin.path_radius * theta.sin();
            let z = 0.02 + 0.01 * (theta * 3.0).sin();
            me.model.kinematics_mut().theta += dtheta;
            json!({
                "ts_us": 0,
                "frame": "map",
                "pos": [round4(x), round4(y), round4(z)],
                "quat": quat_from_yaw(yaw),
            })
        });
        let mut values = values;
        for (value, (_, ts)) in values.iter_mut().zip(stamps.iter()) {
            value["ts_us"] = json!(ts);
        }
        let seq = self.next_seq(Stream::Pose);
        Generated {
            stream: Stream::Pose,
            seq,
            payload: json!({ "v": PAYLOAD_VERSION, "seq": seq, "samples": values }),
            samples: stamps,
            state_fields: None,
            event_level: None,
            flags: self.base_flags(),
        }
    }

    /// The `state` stream: on the first tick, on transitions, and on the
    /// configured heartbeat.
    fn maybe_state(&mut self, tick: u64, now_us: u64, events: &[ModelEvent]) -> Option<Generated> {
        let heartbeat_due = tick >= self.state_next_heartbeat_tick && self.state_published;
        if self.state_published && events.is_empty() && !heartbeat_due {
            return None;
        }
        self.state_published = true;
        self.state_next_heartbeat_tick = tick + self.state_heartbeat_ticks;
        let ts = self.stamp(now_us);
        let mode = self.model.mode().as_str().to_owned();
        let triple = (mode, self.model.cycle_id, self.model.program.clone());
        let seq = self.next_seq(Stream::State);
        let payload = json!({
            "v": PAYLOAD_VERSION,
            "seq": seq,
            "ts_us": ts,
            "mode": triple.0,
            "cycle_id": triple.1,
            "program": triple.2,
        });
        Some(Generated {
            stream: Stream::State,
            seq,
            payload,
            samples: vec![(0, Some(ts))],
            state_fields: Some(triple),
            event_level: None,
            flags: self.base_flags(),
        })
    }

    /// The `events` stream: behavior transitions deterministically, plus
    /// ~Poisson background chatter (alarms and debug noise).
    fn events(
        &mut self,
        now_us: u64,
        rates: &Rates,
        model_events: &[ModelEvent],
        out: &mut Vec<Generated>,
    ) {
        for ev in model_events {
            let (etype, level, code, message, cycle) = match *ev {
                ModelEvent::CycleStart { cycle } => (
                    "cycle_start",
                    "info",
                    None,
                    format!("cycle {cycle} started"),
                    Some(cycle),
                ),
                ModelEvent::CycleEnd { cycle } => (
                    "cycle_end",
                    "info",
                    None,
                    format!("cycle {cycle} completed"),
                    Some(cycle),
                ),
                ModelEvent::FaultStart { code, cycle } => (
                    "fault",
                    "error",
                    Some(code),
                    format!("fault {code} raised during cycle {cycle}"),
                    Some(cycle),
                ),
                ModelEvent::FaultEnd => ("fault", "info", None, "fault cleared".to_owned(), None),
                ModelEvent::EstopStart { cycle } => (
                    "estop",
                    "error",
                    Some(ESTOP_CODE),
                    format!("emergency stop during cycle {cycle}"),
                    Some(cycle),
                ),
                ModelEvent::EstopEnd => (
                    "estop",
                    "info",
                    Some(ESTOP_CODE),
                    "emergency stop released".to_owned(),
                    None,
                ),
                ModelEvent::PauseStart => (
                    "pause",
                    "info",
                    None,
                    format!("cycle {} paused", self.model.cycle_id),
                    Some(self.model.cycle_id),
                ),
                ModelEvent::PauseEnd => (
                    "pause",
                    "info",
                    None,
                    format!("cycle {} resumed", self.model.cycle_id),
                    Some(self.model.cycle_id),
                ),
            };
            out.push(self.event(now_us, etype, level, code, &message, cycle));
        }

        let ticks_per_second = crate::behavior::TICKS_PER_SECOND as f64;
        let p = rates.events_per_second / ticks_per_second;
        if !self.rng_events.bernoulli(p) {
            return;
        }
        let running = self.model.is_running();
        let cycle = running.then_some(self.model.cycle_id);
        if self.rng_events.bernoulli(rates.debug_event_share) {
            let n = self.rng_events.range_u64(1, 10_000);
            let message = format!("trace checkpoint {n}");
            out.push(self.event(now_us, "debug", "debug", None, &message, cycle));
        } else {
            let code = *self.rng_events.pick(&ALARM_CODES);
            let message = format!("alarm {code} active");
            out.push(self.event(now_us, "alarm", "warn", Some(code), &message, cycle));
        }
    }

    /// One event message.
    fn event(
        &mut self,
        now_us: u64,
        etype: &'static str,
        level: &'static str,
        code: Option<&str>,
        message: &str,
        cycle: Option<u64>,
    ) -> Generated {
        let ts = self.stamp(now_us);
        let seq = self.next_seq(Stream::Events);
        let payload = json!({
            "v": PAYLOAD_VERSION,
            "seq": seq,
            "ts_us": ts,
            "type": etype,
            "level": level,
            "code": code,
            "message": message,
            "cycle_id": cycle,
        });
        Generated {
            stream: Stream::Events,
            seq,
            payload,
            samples: vec![(0, Some(ts))],
            state_fields: None,
            event_level: Some(level),
            flags: self.base_flags(),
        }
    }
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}
fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}
fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
fn round4(v: f64) -> f64 {
    (v * 10_000.0).round() / 10_000.0
}

/// Unit quaternion for a heading around +z.
fn quat_from_yaw(yaw: f64) -> [f64; 4] {
    let (s, c) = (yaw * 0.5).sin_cos();
    [round4(c), 0.0, 0.0, round4(s)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ChaosConfig, ClockSkewRule, FleetConfig};

    fn cfg() -> FleetConfig {
        FleetConfig::default()
    }

    fn spec(index: u32, site: &str, name: &str) -> RobotSpec {
        RobotSpec {
            index,
            site: site.to_owned(),
            name: name.to_owned(),
        }
    }

    /// Serialized payloads for `ticks` ticks of one robot, stream by stream.
    fn run_gen(cfg: &FleetConfig, spec: &RobotSpec, ticks: u64) -> Vec<Vec<u8>> {
        let rates = cfg.rates;
        let mut robot = RobotGen::new(spec.clone(), cfg);
        let mut out = Vec::new();
        for tick in 0..ticks {
            let now_us = 1_800_000_000_000_000 + tick * 100_000;
            for generated in robot.tick(tick, now_us, &rates) {
                out.push(
                    serde_json::to_vec(&generated.payload)
                        .ok()
                        .unwrap_or_default(),
                );
            }
        }
        out
    }

    #[test]
    fn same_seed_same_robot_same_payloads() {
        let cfg = cfg();
        let robot = spec(0, "site-a", "robot-01");
        let a = run_gen(&cfg, &robot, 2_000);
        let b = run_gen(&cfg, &robot, 2_000);
        assert_eq!(a, b, "same seed must reproduce payloads byte for byte");
        assert!(a.len() > 120, "2k ticks produce every stream: {}", a.len());
    }

    #[test]
    fn different_seed_or_robot_differs() {
        let cfg = cfg();
        let a = run_gen(&cfg, &spec(0, "site-a", "robot-01"), 500);
        let other_robot = run_gen(&cfg, &spec(1, "site-a", "robot-02"), 500);
        assert_ne!(a, other_robot);
        let cfg_b = FleetConfig {
            seed: 43,
            ..FleetConfig::default()
        };
        let b = run_gen(&cfg_b, &spec(0, "site-a", "robot-01"), 500);
        assert_ne!(a, b);
    }

    #[test]
    fn payload_shapes_carry_every_pipeline_field() {
        let config = cfg();
        let rates = config.rates;
        let mut robot = RobotGen::new(spec(0, "site-a", "robot-01"), &config);
        let mut by_stream: std::collections::BTreeMap<String, Value> = Default::default();
        for tick in 0..500 {
            for g in robot.tick(tick, 1_800_000_000_000_000 + tick * 100_000, &rates) {
                by_stream
                    .entry(g.stream.as_str().to_owned())
                    .or_insert(g.payload);
            }
        }
        let telemetry = &by_stream["telemetry"];
        for key in [
            "v",
            "seq",
            "ts_us",
            "soc_pct",
            "voltage_mv",
            "current_ma",
            "cpu_temp_dc",
            "motor_temp_dc",
        ] {
            assert!(telemetry.get(key).is_some(), "telemetry missing {key}");
        }
        assert_eq!(telemetry["motor_temp_dc"].as_array().map(Vec::len), Some(7));

        let joints = &by_stream["joints"];
        let samples = joints["samples"].as_array().cloned().unwrap_or_default();
        assert_eq!(samples.len(), 10, "joints batch");
        for key in ["ts_us", "pos", "vel", "eff"] {
            assert!(samples[0].get(key).is_some(), "joints sample missing {key}");
        }

        let imu = &by_stream["imu"];
        let samples = imu["samples"].as_array().cloned().unwrap_or_default();
        assert_eq!(samples.len(), 20, "imu batch");
        for key in ["ts_us", "accel", "gyro", "quat"] {
            assert!(samples[0].get(key).is_some(), "imu sample missing {key}");
        }

        let pose = &by_stream["pose"];
        let samples = pose["samples"].as_array().cloned().unwrap_or_default();
        assert_eq!(samples.len(), 5, "pose batch");
        assert_eq!(samples[0]["frame"], "map");
        for key in ["ts_us", "pos", "quat"] {
            assert!(samples[0].get(key).is_some(), "pose sample missing {key}");
        }

        let state = &by_stream["state"];
        for key in ["v", "seq", "ts_us", "mode", "cycle_id", "program"] {
            assert!(state.get(key).is_some(), "state missing {key}");
        }

        let events = &by_stream["events"];
        for key in [
            "v", "seq", "ts_us", "type", "level", "code", "message", "cycle_id",
        ] {
            assert!(events.get(key).is_some(), "events missing {key}");
        }
    }

    #[test]
    fn batch_sizes_and_rates_due_on_expected_ticks() {
        let config = cfg();
        let rates = config.rates;
        let mut robot = RobotGen::new(spec(0, "site-a", "robot-01"), &config);
        let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
        for tick in 0..1000 {
            for g in robot.tick(tick, 1_800_000_000_000_000 + tick * 100_000, &rates) {
                *counts.entry(g.stream.as_str().to_owned()).or_default() += 1;
            }
        }
        // 1000 ticks of 100 ms = 100 s; telemetry 10 msg/s, batched streams
        // 10 msg/s each.
        assert_eq!(counts["telemetry"], 1000);
        assert_eq!(counts["joints"], 1000);
        assert_eq!(counts["imu"], 1000);
        assert_eq!(counts["pose"], 1000);
        assert!(counts["state"] >= 2, "initial state plus heartbeat");
    }

    #[test]
    fn clock_skew_robot_offsets_every_timestamp() {
        let config = FleetConfig {
            chaos: ChaosConfig {
                clock_skew: Some(ClockSkewRule {
                    robot: "robot-04".to_owned(),
                    offset_seconds: 90.0,
                }),
                ..ChaosConfig::default()
            },
            ..FleetConfig::default()
        };
        let rates = config.rates;
        let mut skewed = RobotGen::new(spec(3, "site-b", "robot-04"), &config);
        let mut normal = RobotGen::new(spec(0, "site-a", "robot-01"), &config);
        let now = 1_800_000_000_000_000;
        let skewed_msgs = skewed.tick(0, now, &rates);
        let normal_msgs = normal.tick(0, now, &rates);
        let skewed_ts = skewed_msgs[0].samples[0].1.unwrap_or(0);
        let normal_ts = normal_msgs[0].samples[0].1.unwrap_or(0);
        assert_eq!(skewed_ts - normal_ts, 90_000_000, "+90 s in us");
        assert!(skewed_msgs[0].flags.contains(&flag::CLOCK_SKEW));
        // Samples inside batched messages are offset too.
        let joints = skewed_msgs
            .iter()
            .find(|g| g.stream == Stream::Imu)
            .unwrap_or_else(|| panic!("imu msg"));
        let ts = joints.payload["samples"][0]["ts_us"].as_u64().unwrap_or(0);
        assert!(ts > now + 89_000_000);
    }

    #[test]
    fn state_stream_includes_every_transition() {
        let config = cfg();
        let rates = config.rates;
        let mut robot = RobotGen::new(spec(0, "site-a", "robot-01"), &config);
        let mut triples = Vec::new();
        let mut event_types = Vec::new();
        for tick in 0..3_000 {
            for g in robot.tick(tick, tick * 100_000, &rates) {
                match (g.stream, g.state_fields) {
                    (Stream::State, Some(triple)) => triples.push(triple),
                    (Stream::Events, _) => {
                        if let Some(t) = g.payload["type"].as_str() {
                            event_types.push(t.to_owned());
                        }
                    }
                    _ => {}
                }
            }
        }
        assert!(triples.len() >= 2, "300 s must span cycles: {triples:?}");
        assert!(
            triples
                .windows(2)
                .any(|w| w[0].0 != w[1].0 || w[0].1 != w[1].1)
        );
        assert!(event_types.contains(&"cycle_start".to_owned()));
        assert!(event_types.contains(&"cycle_end".to_owned()));
        // Every cycle event also produced a state change.
        let cycle_starts = event_types
            .iter()
            .filter(|t| t.as_str() == "cycle_start")
            .count();
        let running_states = triples.iter().filter(|t| t.0 == "RUNNING").count();
        assert_eq!(cycle_starts, running_states);
    }
}
