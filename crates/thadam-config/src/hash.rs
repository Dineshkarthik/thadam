//! The config hash.
//!
//! Scope: the typed [`PipelineSpec`], serialized in canonical form, with every
//! value that came from `${env:}` or `${file:}` replaced by the text as written
//! (`${file:/run/secrets/key}`), never by what it resolved to. Rotating a
//! secret therefore does not change the hash, and no secret-derived data
//! reaches the digest.
//!
//! Canonical form: compact JSON, object keys sorted, durations as normalized
//! text, sizes and counts as integers, identity levels in written order (their
//! order is meaningful, so it is hashed alongside the sorted map). A key that
//! holds its default is omitted, which is what lets new keys be added without
//! a new version: an `on_change` deadband of zero serializes as the bare field
//! name and an `enrich` `on_miss` of `null` is left out. The digest is
//! SHA-256 over `b"thadam-config-hash-v1\0"` followed by that JSON, truncated to
//! 16 bytes. Changing any of this requires a new [`CONFIG_HASH_VERSION`].

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::path::Seg;
use crate::spec::PipelineSpec;

/// The version of the canonical form and digest. It is part of the hashed
/// bytes, and is recorded next to the hash wherever it is stored.
pub const CONFIG_HASH_VERSION: u32 = 1;

const DOMAIN: &[u8] = b"thadam-config-hash-v1\0";

fn set_at(root: &mut Value, path: &[Seg], new: Value) {
    let mut cur = root;
    for seg in path {
        let next = match (seg, cur) {
            (Seg::Key(k), Value::Object(m)) => m.get_mut(k),
            (Seg::Idx(i), Value::Array(a)) => a.get_mut(*i),
            _ => None,
        };
        let Some(next) = next else { return };
        cur = next;
    }
    *cur = new;
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                if let Some(child) = map.get(k) {
                    write_canonical(child, out);
                }
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

impl PipelineSpec {
    /// The canonical text that [`PipelineSpec::config_hash`] digests.
    pub fn canonical_json(&self) -> String {
        // Serializing the spec to a value cannot fail: every field is a plain
        // string, number, list, or map. The fallback only keeps this infallible.
        let mut v = serde_json::to_value(self).unwrap_or_else(|e| Value::String(e.to_string()));
        for site in &self.interpolations {
            set_at(&mut v, &site.path, Value::String(site.template.clone()));
        }
        if let Some(Value::Array(datasets)) = v.get_mut("datasets") {
            for (ds, spec) in datasets.iter_mut().zip(&self.datasets) {
                if let Value::Object(map) = ds {
                    let order = spec
                        .identity
                        .iter()
                        .map(|l| Value::String(l.name.clone()))
                        .collect();
                    map.insert("$identity_order".to_owned(), Value::Array(order));
                }
            }
        }
        let mut out = String::new();
        write_canonical(&v, &mut out);
        out
    }

    /// The 16-byte config hash, stable across runs, key order, whitespace,
    /// comments, and secret rotation. See the module documentation for its scope.
    pub fn config_hash(&self) -> [u8; 16] {
        let digest = Sha256::new()
            .chain_update(DOMAIN)
            .chain_update(self.canonical_json().as_bytes())
            .finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest[..16]);
        out
    }

    /// [`PipelineSpec::config_hash`] as 32 lowercase hex digits.
    pub fn config_hash_hex(&self) -> String {
        self.config_hash()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}
