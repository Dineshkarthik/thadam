//! A trimmed copy of the canonical model (ARCHITECTURE.md section 4, D8 representation B,
//! D9 extended value set): an `Arc<Shape>` plus a positional `Vec<Value>`, an envelope
//! with `Quality`, and event times. Only what the filter benchmark needs is kept.

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(Arc<str>),
    F32s(Arc<[f32]>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    Bool,
    I64,
    U64,
    F32,
    F64,
    Str,
    F32s,
}

pub struct Shape {
    pub names: Vec<String>,
    pub types: Vec<FieldType>,
    index: HashMap<String, usize>,
}

impl Shape {
    pub fn new(fields: &[(&str, FieldType)]) -> Arc<Shape> {
        let names: Vec<String> = fields.iter().map(|(n, _)| (*n).to_string()).collect();
        let types = fields.iter().map(|(_, t)| *t).collect();
        let index = names
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, n)| (n, i))
            .collect();
        Arc::new(Shape {
            names,
            types,
            index,
        })
    }
    pub fn idx(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QStatus {
    Good,
    Uncertain,
    Bad,
}
impl QStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            QStatus::Good => "good",
            QStatus::Uncertain => "uncertain",
            QStatus::Bad => "bad",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Quality {
    pub status: QStatus,
    pub flags: u32,
    pub reason: Option<Arc<str>>,
}

#[derive(Clone)]
pub struct Event {
    pub shape: Arc<Shape>,
    pub values: Vec<Value>,
    pub quality: Quality,
    pub ingest_time_us: i64,
    pub event_time_us: Option<i64>,
}

impl Event {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.shape.idx(name).map(|i| &self.values[i])
    }
}

/// Deterministic generator (SplitMix64), so every run sees the same events.
pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// The 13-field "demo" shape: identity, `events`-dataset fields, and `telemetry` fields in
/// one record, so each demo filter has something to read. Plus `Quality` and times.
pub fn demo_shape() -> Arc<Shape> {
    Shape::new(&[
        ("site", FieldType::Str),
        ("robot", FieldType::Str),
        ("event_type", FieldType::Str),
        ("level", FieldType::Str),
        ("code", FieldType::Str),
        ("message", FieldType::Str),
        ("cycle_id", FieldType::U64),
        ("seq", FieldType::U64),
        ("soc_pct", FieldType::F32),
        ("voltage_v", FieldType::F32),
        ("current_a", FieldType::F32),
        ("cpu_temp_c", FieldType::F32),
        ("motor_temp_c", FieldType::F32s),
    ])
}

/// 67 fields: identity plus 64 `f64` sensors (W2-like width), to show how cost scales with width.
pub fn wide_shape() -> Arc<Shape> {
    let mut f: Vec<(String, FieldType)> = vec![
        ("site".into(), FieldType::Str),
        ("robot".into(), FieldType::Str),
        ("level".into(), FieldType::Str),
    ];
    for i in 0..64 {
        f.push((format!("s{i:02}"), FieldType::F64));
    }
    let refs: Vec<(&str, FieldType)> = f.iter().map(|(n, t)| (n.as_str(), *t)).collect();
    Shape::new(&refs)
}

const LEVELS: [(&str, u64); 4] = [("debug", 25), ("info", 50), ("warn", 15), ("error", 10)];
const TYPES: [&str; 4] = ["fault", "info", "mode_change", "heartbeat"];

fn pick_level(r: &mut Rng) -> &'static str {
    let mut x = r.below(100);
    for (name, w) in LEVELS {
        if x < w {
            return name;
        }
        x -= w;
    }
    "info"
}

pub fn gen_demo(n: usize, seed: u64) -> Vec<Event> {
    let shape = demo_shape();
    let sites: Vec<Arc<str>> = ["site-a", "site-b"].iter().map(|s| Arc::from(*s)).collect();
    let robots: Vec<Arc<str>> = (1..=8)
        .map(|i| Arc::from(format!("robot-{i:02}")))
        .collect();
    let levels: Vec<Arc<str>> = LEVELS.iter().map(|(l, _)| Arc::from(*l)).collect();
    let types: Vec<Arc<str>> = TYPES.iter().map(|s| Arc::from(*s)).collect();
    let mut r = Rng::new(seed);
    (0..n)
        .map(|i| {
            let lvl = pick_level(&mut r);
            let lvl_arc = levels.iter().find(|l| &***l == lvl).unwrap().clone();
            let code = if r.below(100) < 30 {
                Value::Str(Arc::from(format!("E-TEMP-{:02}", r.below(20))))
            } else {
                Value::Null
            };
            let cycle = if r.below(100) < 60 {
                Value::U64(8000 + r.below(500))
            } else {
                Value::Null
            };
            let temps: Vec<f32> = (0..7).map(|_| (-60.0 + r.unit() * 230.0) as f32).collect();
            let q = r.below(100);
            let status = if q < 80 {
                QStatus::Good
            } else if q < 95 {
                QStatus::Uncertain
            } else {
                QStatus::Bad
            };
            Event {
                shape: shape.clone(),
                values: vec![
                    Value::Str(sites[r.below(2) as usize].clone()),
                    Value::Str(robots[r.below(8) as usize].clone()),
                    Value::Str(types[r.below(4) as usize].clone()),
                    Value::Str(lvl_arc),
                    code,
                    Value::Str(Arc::from(format!("joint {} over temperature", r.below(7)))),
                    cycle,
                    Value::U64(900_000 + i as u64),
                    Value::F32((r.unit() * 100.0) as f32),
                    Value::F32((44.0 + r.unit() * 8.0) as f32),
                    Value::F32((-5.0 + r.unit() * 10.0) as f32),
                    Value::F32((-50.0 + r.unit() * 200.0) as f32),
                    Value::F32s(Arc::from(temps)),
                ],
                quality: Quality {
                    status,
                    flags: if status == QStatus::Good {
                        0
                    } else {
                        1 << r.below(4)
                    },
                    reason: if status == QStatus::Good {
                        None
                    } else {
                        Some(Arc::from("sensor"))
                    },
                },
                ingest_time_us: 1_791_369_600_000_000 + (i as i64) * 100_000,
                event_time_us: Some(1_791_369_600_000_000 + (i as i64) * 100_000 - 500),
            }
        })
        .collect()
}

pub fn gen_wide(n: usize, seed: u64) -> Vec<Event> {
    let shape = wide_shape();
    let robots: Vec<Arc<str>> = (1..=8)
        .map(|i| Arc::from(format!("robot-{i:02}")))
        .collect();
    let levels: Vec<Arc<str>> = LEVELS.iter().map(|(l, _)| Arc::from(*l)).collect();
    let site: Arc<str> = Arc::from("site-a");
    let mut r = Rng::new(seed);
    (0..n)
        .map(|i| {
            let lvl = pick_level(&mut r);
            let mut values = vec![
                Value::Str(site.clone()),
                Value::Str(robots[r.below(8) as usize].clone()),
                Value::Str(levels.iter().find(|l| &***l == lvl).unwrap().clone()),
            ];
            for _ in 0..64 {
                values.push(Value::F64(-60.0 + r.unit() * 230.0));
            }
            Event {
                shape: shape.clone(),
                values,
                quality: Quality {
                    status: QStatus::Good,
                    flags: 0,
                    reason: None,
                },
                ingest_time_us: i as i64,
                event_time_us: Some(i as i64),
            }
        })
        .collect()
}
