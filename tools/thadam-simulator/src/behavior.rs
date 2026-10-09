//! The per-robot operating-mode model (plan section 3).
//!
//! Robots cycle `IDLE` -> `RUNNING` (numbered cycles) -> `IDLE`, with
//! occasional `PAUSED`, `FAULT`, and `ESTOP` excursions. Segment durations
//! are drawn at segment entry from the robot's `behavior` RNG stream, so the
//! whole trajectory is a pure function of the seed. Continuous model state
//! (trajectory phase, heading, state of charge) advances once per tick and
//! feeds every stream generator.

use crate::TICK;
use crate::config::Behavior;
use crate::rng::SimRng;

/// Ticks per second (the runner ticks at [`TICK`]).
pub const TICKS_PER_SECOND: u64 = 1000 / TICK.as_millis() as u64;

/// The operating mode, spelled the way the `state` dataset expects it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Between cycles.
    Idle,
    /// Executing a numbered cycle.
    Running,
    /// Cycle suspended, will resume.
    Paused,
    /// Faulted mid-cycle; the cycle is abandoned.
    Fault,
    /// Emergency stop; the cycle is abandoned.
    Estop,
}

impl Mode {
    /// The payload spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Mode::Idle => "IDLE",
            Mode::Running => "RUNNING",
            Mode::Paused => "PAUSED",
            Mode::Fault => "FAULT",
            Mode::Estop => "ESTOP",
        }
    }
}

/// A mode transition, emitted to the `events` stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelEvent {
    /// Entered RUNNING with a new `cycle_id`.
    CycleStart {
        /// The cycle that started.
        cycle: u64,
    },
    /// Finished a cycle cleanly.
    CycleEnd {
        /// The cycle that ended.
        cycle: u64,
    },
    /// A mid-cycle fault (abandons the cycle).
    FaultStart {
        /// The fault code, e.g. `E-TEMP-07`.
        code: &'static str,
        /// The interrupted cycle.
        cycle: u64,
    },
    /// Fault cleared.
    FaultEnd,
    /// Emergency stop asserted (abandons the cycle).
    EstopStart {
        /// The interrupted cycle.
        cycle: u64,
    },
    /// Emergency stop released.
    EstopEnd,
    /// Cycle paused.
    PauseStart,
    /// Cycle resumed.
    PauseEnd,
}

/// Fault codes the model raises.
pub const FAULT_CODES: [&str; 4] = ["E-TEMP-07", "E-CURR-03", "E-ENC-11", "E-COMM-02"];

/// Alarm codes for background `warn` events.
pub const ALARM_CODES: [&str; 3] = ["W-BATT-01", "W-TEMP-02", "W-VIB-04"];

/// The code published with an ESTOP event.
pub const ESTOP_CODE: &str = "E-ESTOP-00";

/// Which mid-cycle excursion this cycle rolls, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MidEvent {
    Fault,
    Pause,
    Estop,
}

/// Continuous model state the generators read.
#[derive(Clone, Copy, Debug)]
pub struct Kinematics {
    /// Trajectory phase for joints/IMU, advanced once per tick.
    pub phase: f64,
    /// Heading angle for the pose path, advanced once per tick.
    pub theta: f64,
    /// Pose path center and radius.
    pub path_center: (f64, f64),
    /// Pose path radius.
    pub path_radius: f64,
    /// Battery state of charge, 0..100.
    pub soc: f64,
}

/// One robot's behavior state machine.
#[derive(Debug)]
pub struct Model {
    cfg: Behavior,
    rng: SimRng,
    mode: Mode,
    /// Tick on which the current dwell ends.
    dwell_until: u64,
    /// Mid-cycle excursion: the tick it fires on.
    mid_event_at: Option<u64>,
    mid_event: Option<MidEvent>,
    /// Running time still owed when the cycle was paused.
    paused_remaining: u64,
    /// The current (or last completed) cycle number.
    pub cycle_id: u64,
    /// The robot's fixed program name.
    pub program: String,
    kin: Kinematics,
}

impl Model {
    /// Builds a model for robot `index`. Initial kinematic constants (path
    /// center/radius) come from the robot's `behavior` stream.
    pub fn new(seed: u64, robot_index: u32, cfg: &Behavior, program: String) -> Model {
        let mut rng = SimRng::new(seed, &format!("robot-{robot_index}/behavior"));
        let path_center = (rng.range(-20.0, 20.0), rng.range(-20.0, 20.0));
        let path_radius = rng.range(2.5, 4.0);
        let soc = rng.range(70.0, 95.0);
        let phase = rng.range(0.0, std::f64::consts::TAU);
        let theta = rng.range(0.0, std::f64::consts::TAU);
        let idle = seconds_to_ticks(rng.range(cfg.idle_seconds[0], cfg.idle_seconds[1]));
        Model {
            cfg: *cfg,
            rng,
            mode: Mode::Idle,
            dwell_until: idle,
            mid_event_at: None,
            mid_event: None,
            paused_remaining: 0,
            cycle_id: 0,
            program,
            kin: Kinematics {
                phase,
                theta,
                path_center,
                path_radius,
                soc,
            },
        }
    }

    /// The current mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Whether the robot is doing cycle work (drives load, motion, drain).
    pub fn is_running(&self) -> bool {
        self.mode == Mode::Running
    }

    /// A snapshot of the continuous state for the value generators.
    pub fn kinematics(&self) -> Kinematics {
        self.kin
    }

    /// A mutable snapshot of the continuous state; generators advance
    /// `phase`/`theta` sub-sample so consecutive samples move.
    pub fn kinematics_mut(&mut self) -> &mut Kinematics {
        &mut self.kin
    }

    /// Uniform within a configured `[min, max]` seconds range, from the
    /// behavior stream.
    fn dwell(&mut self, range: [f64; 2]) -> u64 {
        seconds_to_ticks(self.rng.range(range[0], range[1]))
    }

    /// Advances the state machine one tick; returns the transitions that
    /// happened this tick (usually empty, at most one mid-event plus one
    /// segment end).
    pub fn tick(&mut self, tick: u64, now_us: u64) -> Vec<ModelEvent> {
        let _ = now_us;
        let mut out = Vec::new();

        // A scheduled mid-cycle excursion fires first.
        if let (Mode::Running, Some(at), Some(kind)) =
            (self.mode, self.mid_event_at, self.mid_event)
            && tick >= at
        {
            self.mid_event_at = None;
            self.mid_event = None;
            match kind {
                MidEvent::Fault => {
                    let code = *self.rng.pick(&FAULT_CODES);
                    out.push(ModelEvent::FaultStart {
                        code,
                        cycle: self.cycle_id,
                    });
                    self.mode = Mode::Fault;
                    self.dwell_until = tick + self.dwell(self.cfg.fault_seconds);
                }
                MidEvent::Pause => {
                    out.push(ModelEvent::PauseStart);
                    self.paused_remaining = self.dwell_until.saturating_sub(tick);
                    self.mode = Mode::Paused;
                    self.dwell_until = tick + self.dwell(self.cfg.pause_seconds);
                }
                MidEvent::Estop => {
                    out.push(ModelEvent::EstopStart {
                        cycle: self.cycle_id,
                    });
                    self.mode = Mode::Estop;
                    self.dwell_until = tick + self.dwell(self.cfg.estop_seconds);
                }
            }
            return out;
        }

        if tick < self.dwell_until {
            return out;
        }

        match self.mode {
            Mode::Idle => {
                self.cycle_id += 1;
                self.mode = Mode::Running;
                let duration = self.dwell(self.cfg.cycle_seconds);
                self.dwell_until = tick + duration;
                out.push(ModelEvent::CycleStart {
                    cycle: self.cycle_id,
                });
                // Clear any excursion left over from the interrupted previous
                // cycle before rolling again for this one.
                self.mid_event = None;
                self.mid_event_at = None;
                self.roll_mid_event(tick, duration);
            }
            Mode::Running => {
                out.push(ModelEvent::CycleEnd {
                    cycle: self.cycle_id,
                });
                self.mode = Mode::Idle;
                self.dwell_until = tick + self.dwell(self.cfg.idle_seconds);
            }
            Mode::Paused => {
                out.push(ModelEvent::PauseEnd);
                self.mode = Mode::Running;
                self.dwell_until = tick + self.paused_remaining.max(1);
                self.paused_remaining = 0;
            }
            Mode::Fault => {
                out.push(ModelEvent::FaultEnd);
                self.mode = Mode::Idle;
                self.dwell_until = tick + self.dwell(self.cfg.idle_seconds);
            }
            Mode::Estop => {
                out.push(ModelEvent::EstopEnd);
                self.mode = Mode::Idle;
                self.dwell_until = tick + self.dwell(self.cfg.idle_seconds);
            }
        }
        out
    }

    /// Rolls the per-cycle excursion probabilities and schedules at most one.
    fn roll_mid_event(&mut self, tick: u64, duration: u64) {
        if duration < 2 {
            return;
        }
        let kind = if self.rng.bernoulli(self.cfg.fault_probability) {
            MidEvent::Fault
        } else if self.rng.bernoulli(self.cfg.pause_probability) {
            MidEvent::Pause
        } else if self.rng.bernoulli(self.cfg.estop_probability) {
            MidEvent::Estop
        } else {
            return;
        };
        self.mid_event = Some(kind);
        self.mid_event_at = Some(tick + self.rng.range_u64(1, duration - 1));
    }

    /// Advances continuous state for this tick; called once per robot tick
    /// before any message is generated.
    pub fn advance(&mut self, dt_us: u64) {
        let dt = dt_us as f64 / 1e6;
        let running = self.is_running();
        let omega = if running { 1.1 } else { 0.03 };
        let yaw_rate = if running { 0.25 } else { 0.015 };
        self.kin.phase += omega * dt;
        self.kin.theta += yaw_rate * dt;
        let dsoc = if running { -0.000_35 } else { 0.000_5 };
        self.kin.soc = (self.kin.soc + dsoc).clamp(4.0, 100.0);
    }
}

/// Seconds to ticks, rounding up so a dwell of 0 never busy-loops.
pub fn seconds_to_ticks(seconds: f64) -> u64 {
    (seconds * TICKS_PER_SECOND as f64).ceil().max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Behavior {
        Behavior::default()
    }

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Model::new(42, 0, &cfg(), "p".to_owned());
        let mut b = Model::new(42, 0, &cfg(), "p".to_owned());
        for tick in 0..10_000 {
            a.advance(100_000);
            b.advance(100_000);
            assert_eq!(a.tick(tick, tick * 100_000), b.tick(tick, tick * 100_000));
        }
        assert!(a.cycle_id > 3, "10k ticks must run several cycles");
    }

    #[test]
    fn different_robots_diverge() {
        let mut a = Model::new(42, 0, &cfg(), "p".to_owned());
        let mut b = Model::new(42, 1, &cfg(), "p".to_owned());
        let mut same = 0;
        for tick in 0..2000 {
            a.advance(100_000);
            b.advance(100_000);
            if a.tick(tick, 0) == b.tick(tick, 0) {
                same += 1;
            }
        }
        assert!(same < 2000);
    }

    #[test]
    fn paused_cycle_resumes_the_same_cycle() {
        let cfg = Behavior {
            idle_seconds: [0.1, 0.1],
            pause_probability: 1.0,
            fault_probability: 0.0,
            estop_probability: 0.0,
            cycle_seconds: [30.0, 30.0],
            pause_seconds: [0.2, 0.2],
            ..Behavior::default()
        };
        let mut m = Model::new(42, 0, &cfg, "p".to_owned());
        let mut cycle = 0;
        let mut saw_pause = false;
        for tick in 0..2000 {
            m.advance(100_000);
            for ev in m.tick(tick, 0) {
                match ev {
                    ModelEvent::CycleStart { cycle: c } => cycle = c,
                    ModelEvent::PauseStart => {
                        saw_pause = true;
                        assert_ne!(cycle, 0);
                    }
                    ModelEvent::PauseEnd => {
                        assert_eq!(m.mode(), Mode::Running);
                    }
                    ModelEvent::CycleEnd { cycle: c } => assert_eq!(c, cycle),
                    _ => {}
                }
            }
        }
        assert!(saw_pause, "forced pauses must fire");
    }
}
