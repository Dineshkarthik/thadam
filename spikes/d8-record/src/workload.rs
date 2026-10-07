//! Deterministic workloads. Events are generated with a fixed-seed xorshift so
//! runs are reproducible and no `rand` dependency is needed.

use crate::value::Ty;
use serde_json::{Value as Json, json};
use std::sync::Arc;

pub struct Field {
    pub name: &'static str,
    pub ty: Ty,
}

pub struct Steps {
    pub renames: Vec<(&'static str, &'static str)>,
    /// (post-rename field, scale, offset): x * scale + offset
    pub converts: Vec<(&'static str, f64, f64)>,
    /// (post-rename field, threshold): drop the event if the field (or any
    /// array element) exceeds the threshold.
    pub filter: (&'static str, f64),
}

pub struct Spec {
    pub name: &'static str,
    pub decl: Vec<Field>,
    pub steps: Steps,
}

pub struct Workload {
    pub spec: Arc<Spec>,
    pub events: Vec<Json>,
}

pub const DISTINCT_EVENTS: usize = 1024;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn round(x: f64, places: i32) -> f64 {
        let m = 10f64.powi(places);
        (x * m).round() / m
    }
}

/// W1: telemetry, 6 scalar fields + 1 string (7 fields).
/// Declared types let the extended value set use u32/u8/f32.
pub fn w1() -> Workload {
    let spec = Spec {
        name: "w1",
        decl: vec![
            Field { name: "ts", ty: Ty::I64 },
            Field { name: "seq", ty: Ty::U32 },
            Field { name: "temp_f", ty: Ty::F32 },
            Field { name: "volt_mv", ty: Ty::F32 },
            Field { name: "curr_ma", ty: Ty::F32 },
            Field { name: "soc", ty: Ty::U8 },
            Field { name: "robot", ty: Ty::Str },
        ],
        steps: Steps {
            renames: vec![
                ("temp_f", "temperature"),
                ("volt_mv", "voltage"),
                ("curr_ma", "current"),
                ("soc", "battery_pct"),
            ],
            converts: vec![
                // degF -> Cel
                ("temperature", 1.0 / 1.8, -32.0 / 1.8),
                // mV -> V, mA -> A
                ("voltage", 0.001, 0.0),
                ("current", 0.001, 0.0),
            ],
            // ~29% of events are dropped (temperature > 33 Cel)
            filter: ("temperature", 33.0),
        },
    };
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let events = (0..DISTINCT_EVENTS)
        .map(|i| {
            json!({
                "ts": 1_700_000_000_000i64 + i as i64 * 10,
                "seq": 100_000 + i as u64,
                "temp_f": Rng::round(70.0 + rng.unit() * 30.0, 1),
                "volt_mv": Rng::round(11_000.0 + rng.unit() * 1_800.0, 0),
                "curr_ma": Rng::round(500.0 + rng.unit() * 3_000.0, 0),
                "soc": (rng.next() % 101) as u64,
                "robot": format!("robot-{:02}", rng.next() % 40),
            })
        })
        .collect();
    Workload { spec: Arc::new(spec), events }
}

/// W2: joint state, 3 arrays of 7 f64.
pub fn w2() -> Workload {
    let spec = Spec {
        name: "w2",
        decl: vec![
            Field { name: "position", ty: Ty::ListF64 },
            Field { name: "velocity", ty: Ty::ListF64 },
            Field { name: "effort", ty: Ty::ListF64 },
        ],
        steps: Steps {
            renames: vec![
                ("position", "joint_pos"),
                ("velocity", "joint_vel"),
                ("effort", "joint_effort"),
            ],
            converts: vec![
                // rad -> deg
                ("joint_pos", 180.0 / std::f64::consts::PI, 0.0),
                ("joint_vel", 180.0 / std::f64::consts::PI, 0.0),
            ],
            // drop if any joint effort exceeds 36 (expected ~30% dropped for
            // uniform effort in [-40, 40]; actual ratio printed by alloc_report)
            filter: ("joint_effort", 36.0),
        },
    };
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let arr = |rng: &mut Rng, lo: f64, hi: f64| -> Vec<f64> {
        (0..7).map(|_| lo + rng.unit() * (hi - lo)).collect()
    };
    let events = (0..DISTINCT_EVENTS)
        .map(|_| {
            json!({
                "position": arr(&mut rng, -3.1, 3.1),
                "velocity": arr(&mut rng, -2.0, 2.0),
                "effort": arr(&mut rng, -40.0, 40.0),
            })
        })
        .collect();
    Workload { spec: Arc::new(spec), events }
}

/// Serialized form of the events, used only for the parse-only reference row.
pub fn serialized(w: &Workload) -> Vec<Vec<u8>> {
    w.events
        .iter()
        .map(|e| serde_json::to_vec(e).unwrap_or_default())
        .collect()
}
