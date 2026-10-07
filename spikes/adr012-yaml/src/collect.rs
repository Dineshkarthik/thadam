//! Proof of concept for multi-error collection (ADR-0012 "collect all errors"). One serde pass
//! stops at the first error in every candidate crate, so: parse once to `serde_json::Value`
//! (the D22 type), then deserialize each top-level section, and each item of the list sections,
//! separately with `serde_path_to_error`. Spans come from the low-level path index.

use crate::model::*;
use serde::de::DeserializeOwned;
use std::collections::HashMap;

pub struct Found {
    pub path: String,
    pub msg: String,
    pub pos: Option<(usize, usize)>,
}

type Index = HashMap<String, (usize, usize)>;

fn lookup(idx: &Index, path: &str) -> Option<(usize, usize)> {
    let mut p = path.to_string();
    loop {
        if let Some(pos) = idx.get(&p) {
            return Some(*pos);
        }
        match p.rfind(['.', '[']) {
            Some(i) => p.truncate(i),
            None => return None,
        }
    }
}

fn join(prefix: &str, inner: &str) -> String {
    match inner {
        "." => prefix.to_string(),
        s if s.starts_with('[') => format!("{prefix}{s}"),
        s => format!("{prefix}.{s}"),
    }
}

fn check<T: DeserializeOwned>(
    v: &serde_json::Value,
    path: &str,
    idx: &Index,
    out: &mut Vec<Found>,
) {
    if let Err(e) = serde_path_to_error::deserialize::<_, T>(v) {
        let full = join(path, &e.path().to_string());
        out.push(Found {
            pos: lookup(idx, &full),
            path: full,
            msg: e.inner().to_string(),
        });
    }
}

fn check_list<T: DeserializeOwned>(
    v: &serde_json::Value,
    path: &str,
    idx: &Index,
    out: &mut Vec<Found>,
) {
    match v.as_array() {
        Some(items) => {
            for (i, item) in items.iter().enumerate() {
                check::<T>(item, &format!("{path}[{i}]"), idx, out);
            }
        }
        None => out.push(Found {
            path: path.into(),
            msg: "expected a list".into(),
            pos: lookup(idx, path),
        }),
    }
}

const SECTIONS: [&str; 10] = [
    "version",
    "pipeline",
    "sources",
    "wal",
    "quarantine",
    "datasets",
    "transforms",
    "sinks",
    "runtime",
    "observability",
];

pub fn collect(root: &serde_json::Value, idx: &Index) -> Vec<Found> {
    let mut out = Vec::new();
    let Some(map) = root.as_object() else {
        out.push(Found {
            path: ".".into(),
            msg: "document root must be a mapping".into(),
            pos: None,
        });
        return out;
    };
    for k in map.keys() {
        if !SECTIONS.contains(&k.as_str()) {
            out.push(Found {
                path: k.clone(),
                msg: format!(
                    "unknown top-level key `{k}`, expected one of {}",
                    SECTIONS.join(", ")
                ),
                pos: lookup(idx, k),
            });
        }
    }
    for s in SECTIONS {
        let Some(v) = map.get(s) else {
            out.push(Found {
                path: s.into(),
                msg: format!("missing section `{s}`"),
                pos: None,
            });
            continue;
        };
        match s {
            "version" => check::<u32>(v, s, idx, &mut out),
            "pipeline" => check::<PipelineMeta>(v, s, idx, &mut out),
            "sources" | "sinks" => check_list::<Connector>(v, s, idx, &mut out),
            "wal" => check::<Wal>(v, s, idx, &mut out),
            "quarantine" => check::<Quarantine>(v, s, idx, &mut out),
            "datasets" => check_list::<Dataset>(v, s, idx, &mut out),
            "transforms" => check_list::<Transform>(v, s, idx, &mut out),
            "runtime" => check::<Runtime>(v, s, idx, &mut out),
            "observability" => check::<Observability>(v, s, idx, &mut out),
            _ => unreachable!(),
        }
    }
    out
}
