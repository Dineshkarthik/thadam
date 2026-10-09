//! Read-only verification of delivered Parquet against the oracle.
//!
//! A sample is identified by `(dataset, site, robot, seq, sample_idx)`. That
//! is the oracle's own identity, and it is present in every delivered file as
//! the identity columns, `seq`, and `sample_idx` (absent, meaning 0, on
//! single-sample datasets). `_event_id` is an xxh3 digest over a tagged
//! encoding of the event key, so it is only checked for duplicates, not
//! recomputed.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Display;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, UInt16Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

use crate::SimError;
use crate::oracle::{Expected, OracleRow, read_all};

/// Samples listed by name per dataset in a report, before eliding the rest.
const SAMPLE_LIMIT: usize = 5;

/// The identity of one sample within a dataset.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SampleKey {
    /// Site identity.
    pub site: String,
    /// Robot identity.
    pub robot: String,
    /// Per-(robot, stream) message sequence number.
    pub seq: u64,
    /// Index within the exploded message (0 for single-sample streams).
    pub sample_idx: u32,
}

impl SampleKey {
    fn from_oracle(row: &OracleRow) -> SampleKey {
        SampleKey {
            site: row.site.clone(),
            robot: row.robot.clone(),
            seq: row.seq,
            sample_idx: row.sample_idx,
        }
    }

    fn describe(&self) -> String {
        format!(
            "{}/{} seq {} sample {}",
            self.site, self.robot, self.seq, self.sample_idx
        )
    }
}

/// The oracle, indexed for lookups by dataset and sample.
#[derive(Debug, Default)]
struct OracleIndex {
    /// Every oracle outcome recorded for a (dataset, sample), acked or not.
    outcomes: HashMap<(String, SampleKey), BTreeSet<Expected>>,
    /// Samples whose publish was acked and whose outcome is `deliver`.
    delivered: BTreeMap<String, BTreeSet<SampleKey>>,
    /// Distinct acked malformed messages: `(stream, site, robot, seq)`.
    malformed_messages: BTreeSet<(String, String, String, u64)>,
}

impl OracleIndex {
    fn build(rows: &[OracleRow]) -> OracleIndex {
        let mut index = OracleIndex::default();
        for row in rows {
            let key = SampleKey::from_oracle(row);
            index
                .outcomes
                .entry((row.stream.clone(), key.clone()))
                .or_default()
                .insert(row.expected);
            if !row.publish_acked {
                continue;
            }
            match row.expected {
                Expected::Deliver => {
                    index
                        .delivered
                        .entry(row.stream.clone())
                        .or_default()
                        .insert(key);
                }
                Expected::Malformed => {
                    index.malformed_messages.insert((
                        row.stream.clone(),
                        row.site.clone(),
                        row.robot.clone(),
                        row.seq,
                    ));
                }
                Expected::FilteredDebug | Expected::DuplicateOf | Expected::StateRepeat => {}
            }
        }
        index
    }

    fn dataset_names(&self) -> BTreeSet<String> {
        self.outcomes
            .keys()
            .map(|(dataset, _)| dataset.clone())
            .chain(self.delivered.keys().cloned())
            .collect()
    }
}

/// The contents of one delivered dataset, read from its Parquet files.
#[derive(Debug, Default)]
struct DatasetRead {
    files: usize,
    samples: BTreeMap<SampleKey, u64>,
    event_ids: HashMap<[u8; 16], u64>,
}

/// Findings for one dataset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DatasetReport {
    /// The dataset id, also its directory under `ready/`.
    pub dataset: String,
    /// Parquet files read for the dataset.
    pub files: usize,
    /// Rows read from the dataset's files.
    pub output_rows: u64,
    /// Acked `deliver` samples the oracle expects in the dataset.
    pub expected_deliver: u64,
    /// Expected samples absent from the output.
    pub missing: u64,
    /// Up to a few of the missing samples, for the report.
    pub missing_samples: Vec<String>,
    /// Output rows beyond the first for the same sample.
    pub duplicate_rows: u64,
    /// Output rows beyond the first for the same `_event_id`.
    pub duplicate_event_ids: u64,
    /// Output rows whose sample the oracle never recorded.
    pub unexpected_rows: u64,
    /// Up to a few of the unexpected samples, for the report.
    pub unexpected_samples: Vec<String>,
    /// Output rows whose sample the oracle says must not be delivered,
    /// keyed by the oracle outcome (`filtered_debug`, `state_repeat`, ...).
    pub forbidden_rows: BTreeMap<String, u64>,
}

impl DatasetReport {
    /// Whether every check passed for the dataset.
    pub fn passed(&self) -> bool {
        self.missing == 0
            && self.duplicate_rows == 0
            && self.duplicate_event_ids == 0
            && self.unexpected_rows == 0
            && self.forbidden_rows.is_empty()
    }
}

/// Findings for the quarantine dataset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineReport {
    /// Parquet files under the quarantine directory.
    pub files: usize,
    /// Rows across those files.
    pub rows: u64,
    /// Distinct acked malformed messages in the oracle; one quarantine row each.
    pub expected_malformed: u64,
}

impl QuarantineReport {
    /// Whether the quarantine row count matches the injected malformed count.
    pub fn passed(&self) -> bool {
        self.rows == self.expected_malformed
    }
}

/// The full verification result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyReport {
    /// One entry per dataset, in name order.
    pub datasets: Vec<DatasetReport>,
    /// Present when a quarantine directory was given.
    pub quarantine: Option<QuarantineReport>,
}

impl VerifyReport {
    /// Whether everything passed.
    pub fn passed(&self) -> bool {
        self.datasets.iter().all(DatasetReport::passed)
            && self
                .quarantine
                .as_ref()
                .is_none_or(QuarantineReport::passed)
    }

    /// A stable JSON form for `--json`.
    pub fn to_json(&self) -> serde_json::Value {
        let datasets: serde_json::Map<String, serde_json::Value> = self
            .datasets
            .iter()
            .map(|d| {
                (
                    d.dataset.clone(),
                    json!({
                        "passed": d.passed(),
                        "files": d.files,
                        "output_rows": d.output_rows,
                        "expected_deliver": d.expected_deliver,
                        "missing": d.missing,
                        "missing_samples": d.missing_samples,
                        "duplicate_rows": d.duplicate_rows,
                        "duplicate_event_ids": d.duplicate_event_ids,
                        "unexpected_rows": d.unexpected_rows,
                        "unexpected_samples": d.unexpected_samples,
                        "forbidden_rows": d.forbidden_rows,
                    }),
                )
            })
            .collect();
        let quarantine = self.quarantine.as_ref().map(|q| {
            json!({
                "passed": q.passed(),
                "files": q.files,
                "rows": q.rows,
                "expected_malformed": q.expected_malformed,
            })
        });
        json!({
            "passed": self.passed(),
            "datasets": datasets,
            "quarantine": quarantine,
        })
    }

    /// A human-readable table with details for failing datasets.
    pub fn render(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "{:<11} {:>9} {:>9} {:>8} {:>6} {:>9} {:>10} {:>10}  result",
            "dataset", "deliver", "rows", "missing", "dupes", "dup_evid", "unexpected", "forbidden"
        );
        for d in &self.datasets {
            let forbidden: u64 = d.forbidden_rows.values().sum();
            let _ = writeln!(
                out,
                "{:<11} {:>9} {:>9} {:>8} {:>6} {:>9} {:>10} {:>10}  {}",
                d.dataset,
                d.expected_deliver,
                d.output_rows,
                d.missing,
                d.duplicate_rows,
                d.duplicate_event_ids,
                d.unexpected_rows,
                forbidden,
                if d.passed() { "ok" } else { "FAIL" },
            );
        }
        if let Some(q) = &self.quarantine {
            let _ = writeln!(
                out,
                "quarantine: {} rows in {} files, expected {} malformed -> {}",
                q.rows,
                q.files,
                q.expected_malformed,
                if q.passed() { "ok" } else { "FAIL" },
            );
        }
        for d in self.datasets.iter().filter(|d| !d.passed()) {
            let _ = writeln!(out, "{}:", d.dataset);
            if d.missing > 0 {
                let _ = writeln!(out, "  {} expected deliver samples missing", d.missing);
                for s in &d.missing_samples {
                    let _ = writeln!(out, "    {s}");
                }
            }
            if d.duplicate_rows > 0 {
                let _ = writeln!(out, "  {} duplicate sample rows", d.duplicate_rows);
            }
            if d.duplicate_event_ids > 0 {
                let _ = writeln!(out, "  {} duplicate _event_id rows", d.duplicate_event_ids);
            }
            if d.unexpected_rows > 0 {
                let _ = writeln!(
                    out,
                    "  {} rows not present in the oracle",
                    d.unexpected_rows
                );
                for s in &d.unexpected_samples {
                    let _ = writeln!(out, "    {s}");
                }
            }
            for (outcome, n) in &d.forbidden_rows {
                let _ = writeln!(out, "  {n} rows the oracle marks {outcome}");
            }
        }
        out
    }
}

/// Verifies the Parquet under `ready` (the sink's `ready/` directory, one
/// subdirectory per dataset) and, when given, the Parquet under `quarantine`
/// against `oracle`. Read only.
pub fn verify(
    oracle: &Path,
    ready: &Path,
    quarantine: Option<&Path>,
) -> Result<VerifyReport, SimError> {
    let rows = read_all(oracle)?;
    let index = OracleIndex::build(&rows);

    let mut reads: BTreeMap<String, DatasetRead> = BTreeMap::new();
    for file in parquet_files(ready)? {
        let dataset = dataset_of(ready, &file)?;
        let read = reads.entry(dataset.clone()).or_default();
        read.files += 1;
        read_file(&file, read)?;
    }

    let mut datasets = Vec::new();
    let names: BTreeSet<String> = index
        .dataset_names()
        .into_iter()
        .chain(reads.keys().cloned())
        .collect();
    for name in names {
        datasets.push(check_dataset(&name, &index, reads.get(&name)));
    }

    let quarantine = match quarantine {
        Some(dir) => Some(check_quarantine(dir, &index)?),
        None => None,
    };

    Ok(VerifyReport {
        datasets,
        quarantine,
    })
}

fn check_dataset(name: &str, index: &OracleIndex, read: Option<&DatasetRead>) -> DatasetReport {
    let mut report = DatasetReport {
        dataset: name.to_owned(),
        ..DatasetReport::default()
    };
    let empty = DatasetRead::default();
    let read = read.unwrap_or(&empty);
    report.files = read.files;
    report.output_rows = read.samples.values().sum();

    if let Some(expected) = index.delivered.get(name) {
        report.expected_deliver = expected.len() as u64;
        for key in expected {
            if !read.samples.contains_key(key) {
                report.missing += 1;
                if report.missing_samples.len() < SAMPLE_LIMIT {
                    report.missing_samples.push(key.describe());
                }
            }
        }
    }

    for (key, &n) in &read.samples {
        report.duplicate_rows += n.saturating_sub(1);
        match index.outcomes.get(&(name.to_owned(), key.clone())) {
            None => {
                report.unexpected_rows += n;
                if report.unexpected_samples.len() < SAMPLE_LIMIT {
                    report.unexpected_samples.push(key.describe());
                }
            }
            Some(outcomes) if outcomes.contains(&Expected::Deliver) => {}
            Some(outcomes) => {
                if let Some(first) = outcomes.iter().next() {
                    *report
                        .forbidden_rows
                        .entry(first.as_str().to_owned())
                        .or_default() += n;
                }
            }
        }
    }

    report.duplicate_event_ids = read.event_ids.values().map(|n| n.saturating_sub(1)).sum();
    report
}

fn check_quarantine(dir: &Path, index: &OracleIndex) -> Result<QuarantineReport, SimError> {
    let mut files = 0usize;
    let mut rows = 0u64;
    for file in parquet_files(dir)? {
        files += 1;
        let handle = File::open(&file).map_err(|e| verify_io(&file, e))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(handle).map_err(|e| verify_io(&file, e))?;
        rows += builder.metadata().file_metadata().num_rows().max(0) as u64;
    }
    Ok(QuarantineReport {
        files,
        rows,
        expected_malformed: index.malformed_messages.len() as u64,
    })
}

fn parquet_files(root: &Path) -> Result<Vec<PathBuf>, SimError> {
    if !root.is_dir() {
        return Err(SimError::Verify(format!(
            "`{}` is not a directory",
            root.display()
        )));
    }
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).map_err(|e| verify_io(&dir, e))?;
        for entry in entries {
            let path = entry.map_err(|e| verify_io(&dir, e))?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn dataset_of(ready: &Path, file: &Path) -> Result<String, SimError> {
    let rel = file.strip_prefix(ready).map_err(|_| {
        SimError::Verify(format!("{} is outside {}", file.display(), ready.display()))
    })?;
    let mut parts = rel.components();
    let first = parts.next().and_then(|c| c.as_os_str().to_str());
    match (first, parts.next()) {
        (Some(name), Some(_)) => Ok(name.to_owned()),
        _ => Err(SimError::Verify(format!(
            "{} is not inside a dataset directory under {}",
            file.display(),
            ready.display()
        ))),
    }
}

fn read_file(file: &Path, read: &mut DatasetRead) -> Result<(), SimError> {
    let handle = File::open(file).map_err(|e| verify_io(file, e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(handle)
        .map_err(|e| verify_io(file, e))?
        .build()
        .map_err(|e| verify_io(file, e))?;
    for batch in reader {
        let batch = batch.map_err(|e| verify_io(file, e))?;
        ingest(&batch, file, read)?;
    }
    Ok(())
}

fn ingest(batch: &RecordBatch, file: &Path, read: &mut DatasetRead) -> Result<(), SimError> {
    let site = batch
        .column_by_name("site")
        .ok_or_else(|| missing_column(file, "site"))?;
    let robot = batch
        .column_by_name("robot")
        .ok_or_else(|| missing_column(file, "robot"))?;
    let seq = batch
        .column_by_name("seq")
        .and_then(|c| c.as_primitive_opt::<UInt64Type>())
        .ok_or_else(|| missing_column(file, "seq (u64)"))?;
    let sample_idx = batch
        .column_by_name("sample_idx")
        .map(|c| {
            c.as_primitive_opt::<UInt16Type>()
                .ok_or_else(|| missing_column(file, "sample_idx (u16)"))
        })
        .transpose()?;
    let event_id = batch
        .column_by_name("_event_id")
        .map(|c| {
            c.as_fixed_size_binary_opt()
                .ok_or_else(|| missing_column(file, "_event_id (fixed binary)"))
        })
        .transpose()?;

    for row in 0..batch.num_rows() {
        let key = SampleKey {
            site: identity_at(site.as_ref(), row, file, "site")?.to_owned(),
            robot: identity_at(robot.as_ref(), row, file, "robot")?.to_owned(),
            seq: seq.value(row),
            sample_idx: sample_idx
                .and_then(|c| (!c.is_null(row)).then(|| u32::from(c.value(row))))
                .unwrap_or(0),
        };
        *read.samples.entry(key).or_default() += 1;
        if let Some(ids) = event_id
            && !ids.is_null(row)
            && let Ok(bytes) = <[u8; 16]>::try_from(ids.value(row))
        {
            *read.event_ids.entry(bytes).or_default() += 1;
        }
    }
    Ok(())
}

/// The string at `row` of a `Utf8` or dictionary-encoded `Utf8` column.
fn identity_at<'a>(
    array: &'a dyn Array,
    row: usize,
    file: &Path,
    column: &str,
) -> Result<&'a str, SimError> {
    let null = || {
        SimError::Verify(format!(
            "{}: null or unreadable `{column}` at row {row}",
            file.display()
        ))
    };
    if array.is_null(row) {
        return Err(null());
    }
    if let Some(dict) = array.as_dictionary_opt::<Int32Type>() {
        let key = dict.keys().value(row) as usize;
        return dict
            .values()
            .as_string_opt::<i32>()
            .map(|values| values.value(key))
            .ok_or_else(null);
    }
    array
        .as_string_opt::<i32>()
        .map(|values| values.value(row))
        .ok_or_else(null)
}

fn missing_column(file: &Path, column: &str) -> SimError {
    SimError::Verify(format!(
        "{}: column `{column}` is missing or has the wrong type",
        file.display()
    ))
}

fn verify_io(path: &Path, what: impl Display) -> SimError {
    SimError::Verify(format!("{}: {what}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, FixedSizeBinaryArray, StringArray, UInt16Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    use super::*;

    const EXPLODED: &str = "joints";

    struct Row {
        seq: u64,
        sample_idx: u32,
        event_id: u128,
    }

    fn row(seq: u64, sample_idx: u32) -> Row {
        Row {
            seq,
            sample_idx,
            event_id: (u128::from(seq) << 16) | u128::from(sample_idx),
        }
    }

    fn oracle(
        stream: &str,
        seq: u64,
        sample_idx: u32,
        expected: Expected,
        acked: bool,
    ) -> OracleRow {
        OracleRow {
            site: "site-a".to_owned(),
            robot: "robot-01".to_owned(),
            stream: stream.to_owned(),
            seq,
            sample_idx,
            ts_us: Some(1_000),
            publish_acked: acked,
            expected,
            flags: Vec::new(),
        }
    }

    fn write_parquet(path: &Path, rows: &[Row], exploded: bool) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut fields = vec![
            Field::new("_event_id", DataType::FixedSizeBinary(16), false),
            Field::new("site", DataType::Utf8, false),
            Field::new("robot", DataType::Utf8, false),
            Field::new("seq", DataType::UInt64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.event_id.to_le_bytes()))
                    .unwrap(),
            ),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "site-a"))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|_| "robot-01"),
            )),
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.seq))),
        ];
        if exploded {
            fields.push(Field::new("sample_idx", DataType::UInt16, true));
            columns.push(Arc::new(UInt16Array::from_iter_values(
                rows.iter().map(|r| r.sample_idx as u16),
            )));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_quarantine(path: &Path, rows: usize) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("raw", DataType::Utf8, false)]));
        let raw = StringArray::from_iter_values((0..rows).map(|_| "{\"seq\":"));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(raw)]).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    struct Case {
        oracle: Vec<OracleRow>,
        sinks: Vec<(&'static str, Vec<Row>)>,
        quarantine_rows: usize,
    }

    impl Case {
        fn baseline() -> Case {
            Case {
                oracle: vec![
                    oracle("telemetry", 1, 0, Expected::Deliver, true),
                    oracle("telemetry", 2, 0, Expected::Deliver, true),
                    oracle("telemetry", 2, 0, Expected::DuplicateOf, true),
                    oracle("telemetry", 5, 0, Expected::Malformed, true),
                    oracle("telemetry", 6, 0, Expected::Deliver, false),
                    oracle(EXPLODED, 1, 0, Expected::Deliver, true),
                    oracle(EXPLODED, 1, 1, Expected::Deliver, true),
                    oracle("events", 1, 0, Expected::FilteredDebug, true),
                    oracle("events", 2, 0, Expected::Deliver, true),
                    oracle("state", 1, 0, Expected::StateRepeat, true),
                    oracle("state", 2, 0, Expected::Deliver, true),
                ],
                sinks: vec![
                    ("telemetry", vec![row(1, 0), row(2, 0)]),
                    (EXPLODED, vec![row(1, 0), row(1, 1)]),
                    ("events", vec![row(2, 0)]),
                    ("state", vec![row(2, 0)]),
                ],
                quarantine_rows: 1,
            }
        }

        fn run(&self) -> VerifyReport {
            let dir = tempfile::tempdir().unwrap();
            let oracle_path = dir.path().join("oracle.ndjson");
            let text: Vec<String> = self
                .oracle
                .iter()
                .map(|r| serde_json::to_string(r).unwrap())
                .collect();
            std::fs::write(&oracle_path, text.join("\n")).unwrap();
            let ready = dir.path().join("sinks/local/ready");
            for (dataset, rows) in &self.sinks {
                write_parquet(
                    &ready.join(dataset).join("part-0.parquet"),
                    rows,
                    *dataset == EXPLODED,
                );
            }
            let quarantine = dir.path().join("quarantine/local/ready/_quarantine");
            write_quarantine(&quarantine.join("part-0.parquet"), self.quarantine_rows);
            verify(
                &oracle_path,
                &ready,
                Some(&dir.path().join("quarantine/local/ready")),
            )
            .unwrap()
        }
    }

    fn dataset<'a>(report: &'a VerifyReport, name: &str) -> &'a DatasetReport {
        report.datasets.iter().find(|d| d.dataset == name).unwrap()
    }

    #[test]
    fn all_pass_on_a_faithful_output() {
        let report = Case::baseline().run();
        assert!(report.passed(), "{}", report.render());
        let telemetry = dataset(&report, "telemetry");
        assert_eq!(telemetry.expected_deliver, 2);
        assert_eq!(telemetry.output_rows, 2);
        assert_eq!(dataset(&report, EXPLODED).output_rows, 2);
        let quarantine = report.quarantine.as_ref().unwrap();
        assert_eq!((quarantine.rows, quarantine.expected_malformed), (1, 1));
        assert!(report.to_json()["passed"].as_bool().unwrap());
    }

    #[test]
    fn missing_sample_fails_the_dataset() {
        let mut case = Case::baseline();
        case.sinks[0].1 = vec![row(1, 0)];
        let report = case.run();
        assert!(!report.passed());
        let telemetry = dataset(&report, "telemetry");
        assert_eq!(telemetry.missing, 1);
        assert_eq!(
            telemetry.missing_samples,
            ["site-a/robot-01 seq 2 sample 0"]
        );
        assert!(!report.to_json()["passed"].as_bool().unwrap());
        assert!(
            report
                .render()
                .contains("1 expected deliver samples missing")
        );
    }

    #[test]
    fn duplicate_sample_and_event_id_fail_the_dataset() {
        let mut case = Case::baseline();
        case.sinks[0].1 = vec![row(1, 0), row(1, 0), row(2, 0)];
        let report = case.run();
        assert!(!report.passed());
        let telemetry = dataset(&report, "telemetry");
        assert_eq!(telemetry.duplicate_rows, 1);
        assert_eq!(telemetry.duplicate_event_ids, 1);
        assert_eq!(telemetry.missing, 0);
    }

    #[test]
    fn row_absent_from_the_oracle_is_unexpected() {
        let mut case = Case::baseline();
        case.sinks[0].1.push(row(99, 0));
        let report = case.run();
        assert!(!report.passed());
        let telemetry = dataset(&report, "telemetry");
        assert_eq!(telemetry.unexpected_rows, 1);
        assert_eq!(
            telemetry.unexpected_samples,
            ["site-a/robot-01 seq 99 sample 0"]
        );
    }

    #[test]
    fn quarantine_count_must_match_malformed_messages() {
        let mut case = Case::baseline();
        case.quarantine_rows = 2;
        let report = case.run();
        assert!(!report.passed());
        assert!(dataset(&report, "telemetry").passed());
        let quarantine = report.quarantine.as_ref().unwrap();
        assert_eq!((quarantine.rows, quarantine.expected_malformed), (2, 1));
        assert!(report.render().contains("quarantine: 2 rows"));
    }

    #[test]
    fn filtered_rows_present_fail_the_dataset() {
        let mut case = Case::baseline();
        case.sinks[2].1.push(row(1, 0));
        case.sinks[3].1.push(row(1, 0));
        let report = case.run();
        assert!(!report.passed());
        let events = dataset(&report, "events");
        assert_eq!(events.forbidden_rows.get("filtered_debug"), Some(&1));
        let state = dataset(&report, "state");
        assert_eq!(state.forbidden_rows.get("state_repeat"), Some(&1));
        assert_eq!(events.unexpected_rows + state.unexpected_rows, 0);
    }
}
