//! The estimate of Thadam-owned memory printed by `thadam validate`
//! (ARCHITECTURE.md section 10):
//!
//! `expected_broker_window x max_message_bytes + wal.max_pending_bytes
//! + per sink (read chunk + 2 x batch bytes)
//! + max_open_files x 2 x row_group_bytes + upload_concurrency x part_size
//! + dedup window`
//!
//! Connector settings stay opaque (D22), so the well-known keys are read
//! leniently from each `config` block and a documented default stands in when
//! a key is absent or not a plain value. The estimate covers memory Thadam
//! owns, not the allocator or third-party libraries.

use std::fmt;

use serde_json::Value;

use crate::scalars::{parse_count, parse_size};
use crate::spec::PipelineSpec;

/// The MQTT broker window assumed when `expected_broker_window` is not set.
pub const DEFAULT_BROKER_WINDOW: u64 = 100;
/// The largest message assumed when `max_message_bytes` is not set.
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1 << 20;
/// The WAL read chunk assumed when `read_batch_bytes` is not set.
pub const DEFAULT_READ_CHUNK_BYTES: u64 = 1 << 20;
/// The batch size assumed when `batch_bytes` is not set.
pub const DEFAULT_BATCH_BYTES: u64 = 1 << 20;
/// Open Parquet files assumed when `max_open_files` is not set (edge profile).
pub const DEFAULT_MAX_OPEN_FILES: u64 = 4;
/// The Parquet row-group size assumed when `row_group_bytes` is not set.
pub const DEFAULT_ROW_GROUP_BYTES: u64 = 8 << 20;
/// Concurrent uploads assumed when `upload_concurrency` is not set.
pub const DEFAULT_UPLOAD_CONCURRENCY: u64 = 2;
/// The multipart part size assumed when `part_size` is not set.
pub const DEFAULT_PART_BYTES: u64 = 8 << 20;
/// Memory per remembered dedup key: a 16-byte ID, a timestamp, and the hash
/// table and queue overhead.
pub const DEDUP_BYTES_PER_KEY: u64 = 48;

/// One term of the estimate.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MemoryComponent {
    /// What the term covers, such as ``source `fleet-mqtt` ``.
    pub label: String,
    /// The bytes.
    pub bytes: u64,
    /// The arithmetic, with `(default)` marking an assumed value.
    pub detail: String,
}

/// An estimate of Thadam-owned memory for a configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MemoryEstimate {
    /// The terms, in a stable order.
    pub components: Vec<MemoryComponent>,
    /// `runtime.memory_target`, when set.
    pub target_bytes: Option<u64>,
}

impl MemoryEstimate {
    /// The sum of all terms.
    pub fn total_bytes(&self) -> u64 {
        self.components
            .iter()
            .map(|c| c.bytes)
            .fold(0, u64::saturating_add)
    }

    /// Whether the total is above `runtime.memory_target`.
    pub fn exceeds_target(&self) -> bool {
        self.target_bytes.is_some_and(|t| self.total_bytes() > t)
    }
}

/// `12.5 MiB`.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (name, size) in UNITS {
        if bytes >= size {
            return format!("{:.1} {name}", bytes as f64 / size as f64);
        }
    }
    format!("{bytes} B")
}

impl fmt::Display for MemoryEstimate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "estimated Thadam-owned memory: {}",
            format_bytes(self.total_bytes())
        )?;
        match self.target_bytes {
            Some(t) => writeln!(f, " (runtime.memory_target: {})", format_bytes(t))?,
            None => writeln!(f, " (no runtime.memory_target set)")?,
        }
        let width = self
            .components
            .iter()
            .map(|c| c.label.len())
            .max()
            .unwrap_or(0);
        for c in &self.components {
            writeln!(
                f,
                "  {:<width$}  {:>10}  {}",
                c.label,
                format_bytes(c.bytes),
                c.detail
            )?;
        }
        Ok(())
    }
}

fn read_size(cfg: &Value, key: &str) -> Option<u64> {
    match cfg.get(key)? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => parse_size(s).ok(),
        _ => None,
    }
}

fn read_count(cfg: &Value, key: &str) -> Option<u64> {
    match cfg.get(key)? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => parse_count(s).ok(),
        _ => None,
    }
}

/// A value with a note saying whether it was configured or assumed.
struct Setting {
    value: u64,
    text: String,
}

fn setting(
    configured: Option<u64>,
    default: u64,
    render: fn(u64) -> String,
    name: &str,
) -> Setting {
    match configured {
        Some(value) => Setting {
            value,
            text: format!("{name} {}", render(value)),
        },
        None => Setting {
            value: default,
            text: format!("{name} {} (default)", render(default)),
        },
    }
}

fn count_text(n: u64) -> String {
    n.to_string()
}

impl PipelineSpec {
    /// Estimates Thadam-owned memory from the configuration.
    pub fn memory_estimate(&self) -> MemoryEstimate {
        let mut components = Vec::new();

        for source in self.sources.iter().filter(|s| s.kind == "mqtt") {
            let window = setting(
                read_count(&source.config, "expected_broker_window"),
                DEFAULT_BROKER_WINDOW,
                count_text,
                "window",
            );
            let size = setting(
                read_size(&source.config, "max_message_bytes"),
                DEFAULT_MAX_MESSAGE_BYTES,
                format_bytes,
                "message",
            );
            components.push(MemoryComponent {
                label: format!("source `{}` broker window", source.id),
                bytes: window.value.saturating_mul(size.value),
                detail: format!("{} x {}", window.text, size.text),
            });
        }

        components.push(MemoryComponent {
            label: "wal.max_pending_bytes".into(),
            bytes: self.wal.max_pending_bytes.get(),
            detail: format_bytes(self.wal.max_pending_bytes.get()),
        });

        for sink in &self.sinks {
            let cfg = &sink.config;
            let shipper = sink.kind == "object_store";
            if !shipper {
                let chunk = setting(
                    read_size(cfg, "read_batch_bytes"),
                    DEFAULT_READ_CHUNK_BYTES,
                    format_bytes,
                    "read chunk",
                );
                let batch = setting(
                    read_size(cfg, "batch_bytes"),
                    DEFAULT_BATCH_BYTES,
                    format_bytes,
                    "batch",
                );
                components.push(MemoryComponent {
                    label: format!("sink `{}` read and batch", sink.id),
                    bytes: chunk.value.saturating_add(batch.value.saturating_mul(2)),
                    detail: format!("{} + 2 x {}", chunk.text, batch.text),
                });
            }
            if sink.kind == "parquet" {
                let files = setting(
                    read_count(cfg, "max_open_files"),
                    DEFAULT_MAX_OPEN_FILES,
                    count_text,
                    "max_open_files",
                );
                let row_group = setting(
                    read_size(cfg, "row_group_bytes"),
                    DEFAULT_ROW_GROUP_BYTES,
                    format_bytes,
                    "row group",
                );
                components.push(MemoryComponent {
                    label: format!("sink `{}` open files", sink.id),
                    bytes: files
                        .value
                        .saturating_mul(2)
                        .saturating_mul(row_group.value),
                    detail: format!("{} x 2 x {}", files.text, row_group.text),
                });
            }
            if shipper {
                let uploads = setting(
                    read_count(cfg, "upload_concurrency"),
                    DEFAULT_UPLOAD_CONCURRENCY,
                    count_text,
                    "upload_concurrency",
                );
                let part = setting(
                    read_size(cfg, "part_size"),
                    DEFAULT_PART_BYTES,
                    format_bytes,
                    "part",
                );
                components.push(MemoryComponent {
                    label: format!("sink `{}` uploads", sink.id),
                    bytes: uploads.value.saturating_mul(part.value),
                    detail: format!("{} x {}", uploads.text, part.text),
                });
            }
        }

        let dedup_keys: u64 = self
            .datasets
            .iter()
            .filter_map(|d| d.dedup.as_ref())
            .map(|d| d.max_keys.get())
            .fold(0, u64::saturating_add);
        if dedup_keys > 0 {
            components.push(MemoryComponent {
                label: "dedup windows".into(),
                bytes: dedup_keys.saturating_mul(DEDUP_BYTES_PER_KEY),
                detail: format!("{dedup_keys} keys x {DEDUP_BYTES_PER_KEY} B (upper bound)"),
            });
        }

        MemoryEstimate {
            components,
            target_bytes: self.runtime.memory_target.map(|t| t.get()),
        }
    }
}
