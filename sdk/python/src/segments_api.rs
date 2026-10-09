//! `thadam.read_segments`: read a Parquet sink's `ready/` files into a
//! `pyarrow.Table`.
//!
//! File selection follows the catalog when `catalog.log` exists (every
//! file-bearing record that is not deleted; `ready/` files the catalog has
//! not logged yet are not listed, matching startup recovery); without a
//! catalog, `ready/` is walked (the fixture path). Batches are sorted by WAL
//! range so the table is deterministic.

use std::path::{Path, PathBuf};

use arrow_schema59::SchemaRef;
use parquet59::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pyo3::prelude::*;
use thadam_core::{Classify, ErrorDomain, ThadamError, codes};
use thadam_segments::catalog::{SegmentFile, SegmentStatus, inspect};

use crate::error::make_err;

fn sink_err(code: &'static str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Sink, code, message)
}

/// Lists the Parquet files to read, in deterministic read order.
///
/// With a catalog: every non-deleted file record whose dataset matches,
/// oldest WAL range first. Without one: every `ready/<dataset>/**.parquet`,
/// path order.
fn segment_files(root: &Path, dataset: Option<&str>) -> Result<Vec<PathBuf>, ThadamError> {
    if root.join("catalog.log").is_file() {
        let summary = inspect(root).map_err(|e| e.to_thadam_error())?;
        let mut files: Vec<&SegmentFile> = summary
            .files
            .iter()
            .filter(|f| f.status != SegmentStatus::Deleted)
            .filter(|f| dataset.is_none_or(|d| f.dataset == d))
            .collect();
        files.sort_by(|a, b| {
            a.range
                .first()
                .cmp(&b.range.first())
                .then_with(|| a.dataset.cmp(&b.dataset))
                .then_with(|| a.id.cmp(&b.id))
        });
        return Ok(files
            .into_iter()
            .map(|f| root.join(&f.path))
            .filter(|p| p.is_file())
            .collect());
    }

    let ready = root.join("ready");
    let mut files = Vec::new();
    if ready.is_dir() {
        walk(&ready, 0, &mut files)?;
    }
    files.retain(|p| {
        dataset.is_none_or(|d| {
            p.strip_prefix(&ready)
                .ok()
                .and_then(|rel| rel.components().next())
                .and_then(|c| c.as_os_str().to_str())
                == Some(d)
        })
    });
    files.sort();
    Ok(files)
}

fn walk(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) -> Result<(), ThadamError> {
    if depth > 4 {
        return Ok(());
    }
    let entries = std::fs::read_dir(dir).map_err(|e| {
        sink_err(
            codes::SINK_IO,
            format!("read_dir {dir}: {e}", dir = dir.display()),
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            sink_err(
                codes::SINK_IO,
                format!("read_dir {dir}: {e}", dir = dir.display()),
            )
        })?;
        let path = entry.path();
        if path.is_dir() {
            walk(&path, depth + 1, out)?;
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

/// Reads the listed Parquet files and concatenates every record batch. All
/// files must share a schema (they do when one sink wrote them).
pub fn read_impl(
    root: &Path,
    dataset: Option<&str>,
) -> Result<(SchemaRef, Vec<arrow_array59::RecordBatch>), ThadamError> {
    let files = segment_files(root, dataset)?;
    let mut schema: Option<SchemaRef> = None;
    let mut batches = Vec::new();
    for path in files {
        let file = std::fs::File::open(&path)
            .map_err(|e| sink_err(codes::SINK_IO, format!("open {}: {e}", path.display())))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            sink_err(
                codes::SINK_IO,
                format!("{}: not a readable Parquet file: {e}", path.display()),
            )
        })?;
        let file_schema = builder.schema().clone();
        match &schema {
            None => schema = Some(file_schema),
            Some(existing) if existing.fields() != file_schema.fields() => {
                return Err(sink_err(
                    codes::SCHEMA_TYPE_MISMATCH,
                    format!(
                        "{} has a different Arrow schema than the earlier files",
                        path.display()
                    ),
                ));
            }
            Some(_) => {}
        }
        for batch in builder
            .build()
            .map_err(|e| sink_err(codes::SINK_IO, format!("{}: {e}", path.display())))?
        {
            batches.push(
                batch.map_err(|e| sink_err(codes::SINK_IO, format!("{}: {e}", path.display())))?,
            );
        }
    }
    let schema = match schema {
        Some(s) => s,
        None => SchemaRef::new(arrow_schema59::Schema::empty()),
    };
    Ok((schema, batches))
}

/// Reads a Parquet sink's ready segments into a `pyarrow.Table`.
///
/// `sink_dir` is the sink's directory (the one holding `catalog.log` and
/// `ready/`, e.g. `<data_dir>/sinks/local`). With `dataset`, only that
/// dataset's files are read. Rows arrive oldest WAL range first. The hand-off
/// is zero-copy over the Arrow PyCapsule interface. An empty table (with no
/// columns) is returned when nothing is ready to read.
#[pyfunction]
#[pyo3(signature = (sink_dir, dataset = None))]
fn read_segments(py: Python<'_>, sink_dir: &str, dataset: Option<String>) -> PyResult<Py<PyAny>> {
    let root = PathBuf::from(sink_dir);
    let result = py.detach(move || read_impl(&root, dataset.as_deref()));
    let (schema, batches) = result.map_err(|e| make_err(py, &e))?;
    let table = pyo3_arrow::PyTable::try_new(batches, schema)?;
    Ok(table.into_pyarrow(py)?.unbind())
}

/// Registers the read function on the extension module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_segments, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes one Parquet file with the workspace's writer settings minus
    /// Thadam's footer kvs.
    fn write_parquet(path: &Path, rows: u64) {
        use arrow_array59::{Float64Array, RecordBatch, StringArray};
        use arrow_schema59::{DataType, Field, Schema};
        use parquet59::arrow::ArrowWriter;
        use parquet59::file::properties::WriterProperties;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Float64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let props = WriterProperties::builder().build();
        let file = std::fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("writer");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from_iter_values((0..rows).map(|i| i as f64))),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("row{i}")),
                )),
            ],
        )
        .expect("batch");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    #[test]
    fn reads_files_from_a_catalog_ready_layout_without_catalog_log() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("ready/readings/date=2026-10-09/hour=10");
        std::fs::create_dir_all(&dir).expect("mkdir");
        write_parquet(
            &dir.join("n1_0000000000000007_00000000000000000001_00000000000000000003.parquet"),
            3,
        );
        write_parquet(
            &dir.join("n1_0000000000000007_00000000000000000004_00000000000000000006.parquet"),
            3,
        );

        let (schema, batches) = read_impl(tmp.path(), Some("readings")).expect("read");
        assert_eq!(schema.fields().len(), 2);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6);
    }

    #[test]
    fn dataset_filter_selects_one_dataset() {
        let tmp = tempfile::tempdir().expect("tempdir");
        for ds in ["a", "b"] {
            let dir = tmp.path().join(format!("ready/{ds}"));
            std::fs::create_dir_all(&dir).expect("mkdir");
            write_parquet(&dir.join("x.parquet"), 2);
        }
        let (_, batches) = read_impl(tmp.path(), Some("b")).expect("read");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        let (_, all) = read_impl(tmp.path(), None).expect("read");
        let rows: usize = all.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
    }

    #[test]
    fn missing_sink_dir_yields_an_empty_read_not_an_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (schema, batches) = read_impl(tmp.path(), None).expect("read");
        assert_eq!(schema.fields().len(), 0);
        assert!(batches.is_empty());
    }
}
