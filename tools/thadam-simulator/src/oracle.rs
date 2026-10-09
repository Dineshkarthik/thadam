//! The oracle: an append-only NDJSON record of every sample the simulator
//! sends, with its broker-ack and expected-pipeline-outcome status.
//!
//! Rows are buffered per in-flight publish and appended exactly once, when
//! the publish is PUBACKed (`publish_acked: true`) or declared lost at drain
//! time (`publish_acked: false`). NDJSON keeps the writer trivial and lets
//! `oracle-stats`, DuckDB (`read_ndjson`), and text tools read the same file.
//!
//! ```json
//! {"site":"site-a","robot":"robot-01","stream":"telemetry","seq":41,"sample_idx":0,
//!  "ts_us":1791369600123456,"publish_acked":true,"expected":"deliver","flags":[]}
//! ```

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::SimError;
use crate::types::Stream;

/// What the configured pipeline should do with the sample (plan section 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expected {
    /// Written to the stream's Parquet dataset.
    Deliver,
    /// Dropped by the `filter` transform (a `level: debug` event).
    FilteredDebug,
    /// Rejected at decode time and quarantined (truncated JSON).
    Malformed,
    /// Dropped by message-level dedup: a simulator re-publish with the same
    /// `seq` inside the dedup window.
    DuplicateOf,
    /// Dropped by the `on_change` transform: the state triple did not change
    /// within its heartbeat window.
    StateRepeat,
}

impl Expected {
    /// The oracle spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Expected::Deliver => "deliver",
            Expected::FilteredDebug => "filtered_debug",
            Expected::Malformed => "malformed",
            Expected::DuplicateOf => "duplicate_of",
            Expected::StateRepeat => "state_repeat",
        }
    }
}

impl std::fmt::Display for Expected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One oracle line: one delivered (or not) pipeline row's worth of truth.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleRow {
    /// Site identity (topic level 2).
    pub site: String,
    /// Robot identity (topic level 3).
    pub robot: String,
    /// The stream (topic level 4, also the dataset id).
    pub stream: String,
    /// The per-(robot, stream) sequence number of the message.
    pub seq: u64,
    /// The index within the message's exploded samples (0 for single-sample
    /// streams).
    pub sample_idx: u32,
    /// The source timestamp as sent (`null` for missing-timestamp chaos).
    pub ts_us: Option<u64>,
    /// Whether the broker PUBACKed the publish.
    pub publish_acked: bool,
    /// What the configured pipeline should do with the sample.
    pub expected: Expected,
    /// Quality flags the pipeline assigns (the sample is still `deliver`):
    /// `out_of_range`, `clock_skew`, `missing_time`, `out_of_order`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<String>,
}

impl OracleRow {
    /// Sets the ack status (the writer appends the row after this call).
    pub fn acked(&mut self, acked: bool) {
        self.publish_acked = acked;
    }
}

/// Appends rows to the oracle file from a background thread; `write` is
/// cheap for callers (a channel send) and loses nothing on a clean
/// [`OracleWriter::close`].
#[derive(Debug)]
pub struct OracleWriter {
    tx: Option<std::sync::mpsc::SyncSender<Vec<OracleRow>>>,
    worker: Option<std::thread::JoinHandle<std::io::Result<u64>>>,
    path: PathBuf,
}

impl OracleWriter {
    /// Creates the file with `O_EXCL`: the oracle belongs to one run, and
    /// refusing to clobber is part of keeping it append-only. Rows are
    /// appended in batches as PUBACKs arrive; close fsyncs.
    pub fn create(path: &Path) -> Result<OracleWriter, SimError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| SimError::Oracle {
                path: path.to_path_buf(),
                source: e,
            })?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| SimError::Oracle {
                path: path.to_path_buf(),
                source: e,
            })?;
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<OracleRow>>(256);
        let writer_path = path.to_path_buf();
        let worker = std::thread::spawn(move || {
            let mut writer = BufWriter::new(file);
            let mut rows = 0u64;
            let mut result: std::io::Result<()> = Ok(());
            while let Ok(batch) = rx.recv() {
                for row in &batch {
                    let written = serde_json::to_writer(&mut writer, row)
                        .map_err(std::io::Error::from)
                        .and_then(|()| writer.write_all(b"\n"));
                    if let Err(e) = written {
                        result = Err(e);
                        break;
                    }
                    rows += 1;
                }
                if result.is_err() {
                    break;
                }
                if let Err(e) = writer.flush() {
                    result = Err(e);
                    break;
                }
            }
            result.and_then(|()| writer.flush()).and_then(|()| {
                writer.get_ref().sync_all()?;
                tracing::debug!(path = %writer_path.display(), rows, "oracle writer closed");
                Ok(rows)
            })
        });
        Ok(OracleWriter {
            tx: Some(tx),
            worker: Some(worker),
            path: path.to_path_buf(),
        })
    }

    /// Queues rows for appending. Silently no-ops after a write failure;
    /// the failure surfaces at [`OracleWriter::close`].
    pub fn write(&self, rows: Vec<OracleRow>) {
        if rows.is_empty() {
            return;
        }
        if let Some(tx) = &self.tx {
            let _ = tx.send(rows);
        }
    }

    /// The oracle path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flushes, fsyncs, and returns the number of rows written (or the write
    /// error that stopped the writer).
    pub fn close(mut self) -> std::io::Result<u64> {
        drop(self.tx.take());
        match self.worker.take() {
            Some(worker) => worker
                .join()
                .unwrap_or_else(|_| Err(std::io::Error::other("oracle writer thread panicked"))),
            None => Ok(0),
        }
    }
}

/// Reads every row of an oracle file.
pub fn read_all(path: &Path) -> Result<Vec<OracleRow>, SimError> {
    let file = File::open(path).map_err(|e| SimError::Oracle {
        path: path.to_path_buf(),
        source: e,
    })?;
    let mut rows = Vec::new();
    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|e| SimError::Oracle {
            path: path.to_path_buf(),
            source: e,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let row: OracleRow = serde_json::from_str(&line).map_err(|e| SimError::Oracle {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("line {}: {e}", line_no + 1),
            ),
        })?;
        rows.push(row);
    }
    Ok(rows)
}

/// Aggregated oracle contents, keyed by stream then expected outcome.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct OracleStats {
    /// `(stream, expected)` -> row count.
    pub by_stream_expected: BTreeMap<(String, Expected), u64>,
    /// Rows whose publish was PUBACKed.
    pub acked: u64,
    /// Rows whose publish never got a PUBACK before shutdown.
    pub unacked: u64,
    /// Flag -> row count (flags keep the row `deliver`-expected).
    pub flags: BTreeMap<String, u64>,
    /// `(site, robot)` pairs seen.
    pub robots: std::collections::BTreeSet<(String, String)>,
    /// First and last `ts_us` seen (source time), when present.
    pub ts_range: Option<(u64, u64)>,
}

impl OracleStats {
    /// Total rows.
    pub fn total(&self) -> u64 {
        self.acked + self.unacked
    }

    /// Rows for one stream and expected outcome.
    pub fn count(&self, stream: Stream, expected: Expected) -> u64 {
        self.by_stream_expected
            .get(&(stream.as_str().to_owned(), expected))
            .copied()
            .unwrap_or(0)
    }

    /// Total rows for one expected outcome across streams.
    pub fn count_expected(&self, expected: Expected) -> u64 {
        self.by_stream_expected
            .iter()
            .filter(|((_, e), _)| *e == expected)
            .map(|(_, n)| n)
            .sum()
    }

    /// Aggregates rows in memory (oracle files are demo-scale).
    pub fn from_rows(rows: impl IntoIterator<Item = OracleRow>) -> OracleStats {
        let mut stats = OracleStats::default();
        for row in rows {
            *stats
                .by_stream_expected
                .entry((row.stream, row.expected))
                .or_default() += 1;
            if row.publish_acked {
                stats.acked += 1;
            } else {
                stats.unacked += 1;
            }
            for flag in row.flags {
                *stats.flags.entry(flag).or_default() += 1;
            }
            stats.robots.insert((row.site, row.robot));
            if let Some(ts) = row.ts_us {
                stats.ts_range = Some(match stats.ts_range {
                    None => (ts, ts),
                    Some((lo, hi)) => (lo.min(ts), hi.max(ts)),
                });
            }
        }
        stats
    }

    /// Reads and aggregates an oracle file.
    pub fn load(path: &Path) -> Result<OracleStats, SimError> {
        read_all(path).map(OracleStats::from_rows)
    }

    /// A stable JSON summary for `--json`.
    pub fn to_json(&self) -> serde_json::Value {
        let by_stream_expected: serde_json::Map<String, serde_json::Value> = self
            .by_stream_expected
            .iter()
            .map(|((stream, expected), n)| {
                (
                    format!("{stream}/{}", expected.as_str()),
                    serde_json::Value::from(*n),
                )
            })
            .collect();
        serde_json::json!({
            "total_samples": self.total(),
            "publish_acked": self.acked,
            "publish_unacked": self.unacked,
            "robots": self.robots.len(),
            "by_stream_expected": by_stream_expected,
            "flags": self.flags,
            "ts_us_range": self.ts_range.map(|(lo, hi)| [lo, hi]),
        })
    }

    /// A human-readable table.
    pub fn render(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "samples: {} (acked {}, unacked {}) across {} robots",
            self.total(),
            self.acked,
            self.unacked,
            self.robots.len()
        );
        if let Some((lo, hi)) = self.ts_range {
            let span = hi.saturating_sub(lo) as f64 / 1e6;
            let _ = writeln!(out, "source time span: {span:.1}s");
        }
        let _ = writeln!(
            out,
            "{:<11} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "stream", "deliver", "debug", "malform", "dup", "state_rep", "total"
        );
        for stream in Stream::ALL {
            let name = stream.as_str();
            let deliver = self.count(stream, Expected::Deliver);
            let debug = self.count(stream, Expected::FilteredDebug);
            let malformed = self.count(stream, Expected::Malformed);
            let dup = self.count(stream, Expected::DuplicateOf);
            let repeat = self.count(stream, Expected::StateRepeat);
            let total = deliver + debug + malformed + dup + repeat;
            if total == 0 {
                continue;
            }
            let _ = writeln!(
                out,
                "{name:<11} {deliver:>9} {debug:>9} {malformed:>9} {dup:>9} {repeat:>9} {total:>9}"
            );
        }
        if !self.flags.is_empty() {
            let _ = writeln!(out, "flags:");
            for (flag, n) in &self.flags {
                let _ = writeln!(out, "  {flag:<13} {n:>9}");
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(stream: Stream, seq: u64, acked: bool, expected: Expected) -> OracleRow {
        OracleRow {
            site: "site-a".to_owned(),
            robot: "robot-01".to_owned(),
            stream: stream.as_str().to_owned(),
            seq,
            sample_idx: 0,
            ts_us: Some(1_000),
            publish_acked: acked,
            expected,
            flags: Vec::new(),
        }
    }

    #[test]
    fn write_and_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oracle.ndjson");
        let writer = OracleWriter::create(&path).unwrap();
        writer.write(vec![
            row(Stream::Telemetry, 1, true, Expected::Deliver),
            row(Stream::Events, 2, false, Expected::FilteredDebug),
        ]);
        writer.write(vec![row(Stream::Joints, 3, true, Expected::DuplicateOf)]);
        let written = writer.close().unwrap();
        assert_eq!(written, 3);

        let rows = read_all(&path).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].seq, 1);
        assert!(rows[0].publish_acked);
        assert!(!rows[1].publish_acked);
        assert_eq!(rows[2].expected, Expected::DuplicateOf);

        let err = OracleWriter::create(&path).err().map(|e| e.to_string());
        assert!(
            err.as_ref().is_some_and(|e| e.contains("oracle"),),
            "{err:?}"
        );
    }

    #[test]
    fn stats_aggregate_by_stream_and_outcome() {
        let rows = vec![
            row(Stream::Telemetry, 1, true, Expected::Deliver),
            row(Stream::Telemetry, 2, true, Expected::Deliver),
            row(Stream::Events, 1, true, Expected::FilteredDebug),
            row(Stream::Joints, 1, false, Expected::Malformed),
        ];
        let stats = OracleStats::from_rows(rows);
        assert_eq!(stats.total(), 4);
        assert_eq!(stats.acked, 3);
        assert_eq!(stats.unacked, 1);
        assert_eq!(stats.count(Stream::Telemetry, Expected::Deliver), 2);
        assert_eq!(stats.count_expected(Expected::Deliver), 2);
        assert_eq!(stats.count_expected(Expected::Malformed), 1);
        assert_eq!(stats.count(Stream::Joints, Expected::Malformed), 1);
        assert_eq!(stats.robots.len(), 1);
        let json = stats.to_json();
        assert_eq!(json["total_samples"], serde_json::json!(4));
        assert!(stats.render().contains("telemetry"));
    }

    #[test]
    fn flags_and_ts_range_are_tracked() {
        let mut a = row(Stream::Telemetry, 1, true, Expected::Deliver);
        a.flags = vec!["clock_skew".to_owned()];
        a.ts_us = Some(2_000);
        let mut b = row(Stream::Pose, 1, true, Expected::Deliver);
        b.flags = vec!["out_of_order".to_owned(), "clock_skew".to_owned()];
        b.ts_us = Some(500);
        let stats = OracleStats::from_rows(vec![a, b]);
        assert_eq!(stats.flags.get("clock_skew"), Some(&2));
        assert_eq!(stats.flags.get("out_of_order"), Some(&1));
        assert_eq!(stats.ts_range, Some((500, 2_000)));
    }
}
