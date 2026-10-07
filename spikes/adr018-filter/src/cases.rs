//! The filters under test: the demo's three plus a combination, each written three ways and
//! with a hand-written Rust reference used to check that all engines agree before timing.

use crate::model::{Event, QStatus, Value};

pub struct Case {
    pub name: &'static str,
    pub cel: &'static str,
    /// evalexpr source and the identifiers it needs. `None`: not expressible.
    pub evalexpr: Option<(&'static str, &'static [&'static str])>,
    pub yaml: &'static str,
    pub reference: fn(&Event) -> bool,
}

fn s<'a>(e: &'a Event, n: &str) -> &'a str {
    match e.get(n) {
        Some(Value::Str(s)) => s,
        _ => "",
    }
}
fn f(e: &Event, n: &str) -> f64 {
    match e.get(n) {
        Some(Value::F32(x)) => *x as f64,
        Some(Value::F64(x)) => *x,
        _ => f64::NAN,
    }
}

pub const CASES: &[Case] = &[
    Case {
        name: "level_ne_debug",
        cel: "level != 'debug'",
        evalexpr: Some(("level != \"debug\"", &["level"])),
        yaml: "{ field: level, ne: debug }",
        reference: |e| s(e, "level") != "debug",
    },
    Case {
        name: "quality_good",
        cel: "quality.status == 'good'",
        evalexpr: Some(("quality_status == \"good\"", &["quality_status"])),
        yaml: "{ field: quality.status, eq: good }",
        reference: |e| e.quality.status == QStatus::Good,
    },
    Case {
        name: "range_scalar",
        cel: "cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0",
        evalexpr: Some((
            "cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0",
            &["cpu_temp_c"],
        )),
        yaml: "{ field: cpu_temp_c, min: -40, max: 150 }",
        reference: |e| {
            // Compare in the f32 domain against f32-rounded bounds, exactly as the native
            // evaluator does; for these bounds (exactly representable) it equals the f64 test.
            let x = f(e, "cpu_temp_c");
            (-40.0..=150.0).contains(&x)
        },
    },
    Case {
        name: "range_list",
        cel: "motor_temp_c.all(t, t >= -40.0 && t <= 150.0)",
        evalexpr: None,
        yaml: "{ field: motor_temp_c, min: -40, max: 150 }",
        reference: |e| match e.get("motor_temp_c") {
            Some(Value::F32s(xs)) => xs.iter().all(|x| (-40.0..=150.0).contains(&(*x as f64))),
            _ => false,
        },
    },
    Case {
        name: "combo",
        cel: "level != 'debug' && (robot != 'robot-03' || quality.status == 'good') && soc_pct >= 10.0",
        evalexpr: Some((
            "level != \"debug\" && (robot != \"robot-03\" || quality_status == \"good\") && soc_pct >= 10.0",
            &["level", "robot", "quality_status", "soc_pct"],
        )),
        yaml: "all:\n  - { field: level, ne: debug }\n  - any:\n      - { field: robot, ne: robot-03 }\n      - { field: quality.status, eq: good }\n  - { field: soc_pct, ge: 10.0 }",
        reference: |e| {
            s(e, "level") != "debug"
                && (s(e, "robot") != "robot-03" || e.quality.status == QStatus::Good)
                && f(e, "soc_pct") >= 10.0
        },
    },
];

/// One filter over the wide (67 field) shape, to show how binding cost scales with width.
pub const WIDE_CASE: Case = Case {
    name: "wide_range_scalar",
    cel: "s17 >= -40.0 && s17 <= 150.0",
    evalexpr: Some(("s17 >= -40.0 && s17 <= 150.0", &["s17"])),
    yaml: "{ field: s17, min: -40, max: 150 }",
    reference: |e| (-40.0..=150.0).contains(&f(e, "s17")),
};
