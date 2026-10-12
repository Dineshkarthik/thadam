//! Sink behavior: rotation by rows, bytes, and age; LRW eviction at
//! `max_open_files`; partition rotation (ingest-hour change and unsynced
//! clocks); read-back of written files (field IDs, values, footer metadata);
//! eligibility lifecycle; and shutdown policy.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use thadam_core::{
    DatasetEvents, Event, EventBatch, SegmentBatch, SegmentSink, WalEpoch, WalPos, WalRange,
};
use thadam_testkit::{test_event, test_segment_batch_across};

use crate::catalog::LocalRetention;
use crate::config::RotationConfig;
use crate::tests::fixture::{ParquetFixture, files_of};

const ONE: [&str; 1] = ["only"];
const ABC: [&str; 3] = ["alpha", "beta", "gamma"];

fn batch(datasets: &[&str], first: u64, last: u64) -> SegmentBatch {
    test_segment_batch_across(1, first, last, datasets)
}

async fn append_frames(
    sink: &mut crate::sink::ParquetSegmentSink,
    datasets: &[&str],
    first: u64,
    last: u64,
) {
    sink.append(batch(datasets, first, last)).await.unwrap();
}

// ---------------------------------------------------------------- rotation

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_by_rows() {
    let fixture = ParquetFixture::new(
        &ONE,
        RotationConfig {
            max_bytes: None,
            max_rows: Some(3),
            max_age: None,
        },
        4,
    );
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    for f in 1..=7u64 {
        append_frames(&mut sink, &ONE, f, f).await;
        sink.poll_commit().await.unwrap();
    }
    sink.finalize_all().await.unwrap();

    // Files at rows 3, 3, 1; every file's row count readable from footers.
    let files = fixture.ready_files();
    assert_eq!(files.len(), 3, "{files:?}");
    let mut row_counts: Vec<usize> = files
        .iter()
        .map(|(_, name, _)| {
            crate::tests::fixture::rows_of(
                &fixture
                    .dir
                    .path()
                    .join("ready/only")
                    .join("date=1970-01-01/hour=00")
                    .join(name),
            )
        })
        .collect();
    row_counts.sort_unstable();
    assert_eq!(row_counts, vec![1, 3, 3]);
    let merged = fixture.merged_ranges();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0],
        WalRange::new(WalEpoch::new(1), WalPos::new(1), WalPos::new(7)).unwrap()
    );
}

/// With durability deferred (the runtime's setting), a rotation the batch
/// triggers does not finalize inside `append`: the batch's rows only become
/// durable at the next `append` or `poll_commit`, after the runtime has
/// staged the rejects `append` returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_durability_postpones_rotation_to_the_next_call() {
    let rotation = RotationConfig {
        max_bytes: None,
        max_rows: Some(1),
        max_age: None,
    };
    let fixture = ParquetFixture::new(&ONE, rotation, 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    sink.defer_durability(true);

    append_frames(&mut sink, &ONE, 1, 1).await;
    assert!(
        fixture.ready_files().is_empty(),
        "frame 1 is not durable yet"
    );
    append_frames(&mut sink, &ONE, 2, 2).await;
    let files = fixture.ready_files();
    assert_eq!(files.len(), 1, "the next append closes frame 1: {files:?}");
    assert_eq!(files[0].2.last().get(), 1);
    assert_eq!(sink.poll_commit().await.unwrap(), Some(WalPos::new(2)));
    assert_eq!(fixture.ready_files().len(), 2);

    // Without the request, the rotation stays inside `append`.
    let fixture = ParquetFixture::new(&ONE, rotation, 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    append_frames(&mut sink, &ONE, 1, 1).await;
    assert_eq!(fixture.ready_files().len(), 1);
}

/// A segment evicted at `max_open_files` that already holds the current
/// batch's rows is closed later, not inside `append`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_durability_postpones_eviction_of_the_current_batch() {
    let two: [&str; 2] = ["alpha", "beta"];
    let rotation = RotationConfig {
        max_bytes: None,
        max_rows: None,
        max_age: None,
    };
    let fixture = ParquetFixture::new(&two, rotation, 1);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    sink.defer_durability(true);

    // Frame 2 goes to alpha, frame 1 to beta.
    append_frames(&mut sink, &two, 1, 2).await;
    assert!(
        fixture.ready_files().is_empty(),
        "alpha holds this batch's rows and must not become durable inside append"
    );
    sink.poll_commit().await.unwrap();
    assert_eq!(fixture.ready_files().len(), 1);
    sink.finalize_all().await.unwrap();
    let rows: usize = crate::tests::fixture::files_of(&fixture.dir.path().join("ready"))
        .iter()
        .map(|p| crate::tests::fixture::rows_of(p))
        .sum();
    assert_eq!(rows, 2, "both datasets' rows are written once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_by_bytes() {
    // 4 KiB file ceiling with 2 KiB row groups: byte estimates rotate the
    // file a handful of times over 400 frames.
    let fixture = ParquetFixture::new(
        &ONE,
        RotationConfig {
            max_bytes: Some(4 * 1024),
            max_rows: None,
            max_age: None,
        },
        4,
    );
    let ctx = fixture.ctx();
    let mut cfg = fixture.batch_config();
    cfg.row_group_bytes = 2 * 1024;
    let mut sink = crate::sink::ParquetSegmentSink::new(cfg).unwrap();
    sink.recover(&ctx).await.unwrap();
    for b in 0..40u64 {
        append_frames(&mut sink, &ONE, b * 10 + 1, b * 10 + 10).await;
        sink.poll_commit().await.unwrap();
    }
    sink.finalize_all().await.unwrap();

    let files = fixture.ready_files();
    assert!(
        files.len() >= 2,
        "byte rotation splits the stream: {files:?}"
    );
    let merged = fixture.merged_ranges();
    assert_eq!(merged.len(), 1, "no gaps across byte-rotated files");
    assert_eq!(merged[0].last().get(), 400);
    let rows: usize = crate::tests::fixture::files_of(&fixture.dir.path().join("ready/only"))
        .iter()
        .map(|p| crate::tests::fixture::rows_of(p))
        .sum();
    assert_eq!(rows, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_by_age_marks_empty_datasets_too() {
    let sparse: [&str; 3] = ["one", "empty", "three"];
    let fixture = ParquetFixture::new(
        &sparse,
        RotationConfig {
            max_bytes: None,
            max_rows: None,
            max_age: Some(Duration::from_secs(60)),
        },
        4,
    );
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();

    let batch = |f: u64, events: Vec<(&str, Vec<Event>)>| -> SegmentBatch {
        // Every dataset of the chain must be listed, events or not (the
        // SegmentBatch contract drives marker coverage for empty ones).
        let mut datasets: Vec<DatasetEvents> = sparse
            .iter()
            .map(|d| DatasetEvents::new(*d, Vec::new()))
            .collect();
        for (d, evs) in events {
            datasets
                .iter_mut()
                .find(|x| &*x.dataset == d)
                .unwrap()
                .events = evs;
        }
        SegmentBatch::new(
            WalRange::new(WalEpoch::new(1), WalPos::new(f), WalPos::new(f)).unwrap(),
            true,
            datasets,
        )
    };
    sink.append(batch(1, vec![("one", vec![test_event(1, 1)])]))
        .await
        .unwrap();
    assert!(sink.poll_commit().await.unwrap().is_none(), "not due yet");

    // Advance 61 s: the age rotation closes all three segments. `empty`
    // produces a coverage marker, so the watermark reaches 1.
    fixture.clock.advance(Duration::from_secs(61));
    let w = sink.poll_commit().await.unwrap();
    assert_eq!(
        w,
        Some(WalPos::new(1)),
        "age rotation + markers advance the watermark"
    );

    // A new file starts fresh after the age rotation.
    sink.append(batch(2, vec![("one", vec![test_event(2, 2)])]))
        .await
        .unwrap();
    sink.finalize_all().await.unwrap();
    let merged = fixture.merged_ranges();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].last().get(), 2);
}

// ----------------------------------------------------------- open-file cap

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_datasets_evict_least_recently_written() {
    let eight: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let fixture = ParquetFixture::new(&eight, RotationConfig::never(), 3);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();

    // 24 frames cycling datasets: with cap 3, segments are evicted
    // constantly; every dataset still ends fully covered.
    for f in 1..=24u64 {
        sink.append(batch(&eight, f, f)).await.unwrap();
        sink.poll_commit().await.unwrap();
    }
    assert_eq!(sink.finalize_all().await.unwrap(), Some(WalPos::new(24)));

    let merged = fixture.merged_ranges();
    assert_eq!(merged.len(), 1, "contiguous cover through eviction churn");
    assert_eq!(merged[0].last().get(), 24);
    let rows = fixture.dataset_rows();
    let expected: BTreeSet<usize> = rows.values().copied().collect();
    assert_eq!(
        expected,
        BTreeSet::from([3usize]),
        "each dataset got 3 of 24 rows: {rows:?}"
    );
}

// ------------------------------------------------------------- partitions

fn ingest_event(n: i64, wal_pos: u64, nanos: i64) -> Event {
    let mut ev = test_event(n, wal_pos);
    let times = thadam_core::Times::new(
        thadam_core::Timestamp::from_nanos(nanos),
        thadam_core::TimeDomain::Utc,
    );
    ev = Event::builder(ev.id(), WalPos::new(wal_pos), times)
        .payload(ev.payload().clone())
        .build()
        .unwrap();
    ev
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hour_change_rotates_and_unsynced_suffix_applies() {
    const HOUR: i64 = 3_600_000_000_000;
    let fixture = ParquetFixture::new(&ONE, RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    let mk = |first: u64, last: u64, nanos: i64, synced: bool| {
        let events = (first..=last)
            .map(|p| ingest_event(p as i64, p, nanos))
            .collect();
        SegmentBatch::single(
            "only",
            EventBatch::new(
                events,
                WalRange::new(WalEpoch::new(1), WalPos::new(first), WalPos::new(last)).unwrap(),
                synced,
            ),
        )
    };

    sink.append(mk(1, 3, HOUR + 5, true)).await.unwrap();
    sink.append(mk(4, 6, 2 * HOUR + 5, true)).await.unwrap(); // hour change: rotate
    sink.append(mk(7, 9, 2 * HOUR + 7, false)).await.unwrap(); // sync flag change: rotate
    sink.finalize_all().await.unwrap();

    let root = fixture.dir.path().join("ready/only");
    let paths: Vec<String> = files_of(&root)
        .iter()
        .map(|p| p.strip_prefix(&root).unwrap().to_str().unwrap().to_owned())
        .collect();
    assert_eq!(paths.len(), 3, "{paths:?}");
    assert!(
        paths
            .iter()
            .any(|p| p.starts_with("date=1970-01-01/hour=01/")),
        "{paths:?}"
    );
    assert!(
        paths
            .iter()
            .any(|p| p.starts_with("date=1970-01-01/hour=02/")),
        "{paths:?}"
    );
    assert!(
        paths.iter().any(|p| p.starts_with("unsynced/")),
        "{paths:?}"
    );

    // Footer clock_synced matches the partition.
    for p in files_of(&root) {
        let file = std::fs::File::open(&p).unwrap();
        let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let kv = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| kvs.iter().find(|kv| kv.key == "thadam.clock_synced"))
            .and_then(|kv| kv.value.clone());
        let unsynced = p.components().any(|c| c.as_os_str() == "unsynced");
        assert_eq!(
            kv.as_deref(),
            Some(if unsynced { "false" } else { "true" }),
            "{}: {kv:?}",
            p.display()
        );
    }

    let merged = fixture.merged_ranges();
    assert_eq!(
        merged.len(),
        1,
        "partitions still cover the stream contiguously"
    );
    assert_eq!(merged[0].last().get(), 9);
}

// --------------------------------------------------------------- read-back

/// The Parquet writer↔reader round trip: field IDs survive, values survive,
/// and every required footer key is present (ADR-0013 evidence for PR14).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_files_round_trip_field_ids_values_and_footer() {
    let fixture = ParquetFixture::new(&ONE, RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    sink.append(batch(&ONE, 1, 5)).await.unwrap();
    sink.finalize_all().await.unwrap();

    let root = fixture.dir.path().join("ready/only");
    let files = files_of(&root);
    assert_eq!(files.len(), 1);
    let path = &files[0];

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // Field IDs: every column carries the ID thadam-arrow assigned (PR14
    // evidence for ADR-0013's open O5 round trip).
    let descriptor = builder.parquet_schema();
    let names: Vec<&str> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let id_of = |name: &str| {
        let i = names
            .iter()
            .position(|n| *n == name)
            .unwrap_or_else(|| panic!("column {name} in {names:?}"));
        let info = descriptor.get_column_root(i).get_basic_info();
        assert!(info.has_id(), "column {name} has a field id");
        info.id()
    };
    assert_eq!(id_of("_ingest_time"), 2);
    assert_eq!(id_of("_wal_pos"), 11);
    assert_eq!(id_of("_wal_epoch"), 10);
    assert_eq!(id_of("_event_id"), 1);
    assert_eq!(id_of("n"), 1000, "declared field keeps its explicit ID");

    // Footer metadata: every ADR-0013 key, plus `created_by = "thadam <v>"`.
    let file2 = std::fs::File::open(path).unwrap();
    let meta = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file2)
        .unwrap();
    let empty_kvs = vec![];
    let kvs = meta
        .file_metadata()
        .key_value_metadata()
        .unwrap_or(&empty_kvs);
    let get = |key: &str| {
        kvs.iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.clone())
    };
    assert_eq!(get("thadam.dataset").as_deref(), Some("only"));
    assert_eq!(get("thadam.wal_range").as_deref(), Some("1-1-5"));
    assert_eq!(get("thadam.clock_synced").as_deref(), Some("true"));
    assert_eq!(get("thadam.config_hash").as_deref(), Some("c0ffee"));
    assert!(get("thadam.version").is_some());
    assert_eq!(get("thadam.enrichment_hash").as_deref(), Some(""));
    let fp = get("thadam.schema_fp").expect("schema fp");
    assert_eq!(fp.len(), 32, "fp is 16 bytes of hex");
    let created = meta
        .file_metadata()
        .created_by()
        .unwrap_or_default()
        .to_owned();
    assert!(created.starts_with("thadam "), "{created}");

    // Values: the n column holds the appended stream in order.
    let n_col = builder.schema().index_of("n").unwrap();
    let reader = builder.with_batch_size(1 << 16).build().unwrap();
    let mut vals = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let col = b.column(n_col);
        let arr = col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        vals.extend(arr.iter().flatten());
    }
    assert_eq!(vals, vec![1, 2, 3, 4, 5]);
}

// ---------------------------------------------------------------- lifecycle

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eligibility_lifecycle_and_upload_marks() {
    let fixture = ParquetFixture::new(&ONE, RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    sink.append(batch(&ONE, 1, 4)).await.unwrap();
    let w = sink.finalize_all().await.unwrap();
    assert_eq!(w, Some(WalPos::new(4)));

    let catalog = sink.catalog().unwrap().clone();
    assert!(catalog.eligible_for_upload().is_empty());

    // cursor_committed is synchronous; the Eligible records drain at the
    // next async entry point.
    sink.cursor_committed(WalPos::new(4));
    assert!(catalog.eligible_for_upload().is_empty());
    sink.poll_commit().await.unwrap();
    let eligible = catalog.eligible_for_upload();
    assert_eq!(eligible.len(), 1, "{eligible:?}");
    assert_eq!(eligible[0].range.last(), WalPos::new(4));
    assert!(eligible[0].has_file());

    // mark_uploaded only accepts eligible segments; a repeat is idempotent.
    let id = eligible[0].id.clone();
    catalog.mark_uploaded(&id, "obj/k1", None).await.unwrap();
    assert!(catalog.eligible_for_upload().is_empty());
    catalog.mark_uploaded(&id, "obj/k1", None).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uploaded_lists_key_before_and_after_local_release() {
    let fixture = ParquetFixture::new(&ONE, RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    sink.append(batch(&ONE, 1, 4)).await.unwrap();
    sink.finalize_all().await.unwrap();
    sink.cursor_committed(WalPos::new(4));
    sink.poll_commit().await.unwrap();
    let catalog = sink.catalog().unwrap().clone();
    assert!(catalog.uploaded().is_empty());

    let id = catalog.eligible_for_upload()[0].id.clone();
    catalog
        .mark_uploaded(&id, "lake/thadam/v1/p/d/k1", Some("e1"))
        .await
        .unwrap();
    let uploaded = catalog.uploaded();
    assert_eq!(uploaded.len(), 1);
    assert_eq!(uploaded[0].key, "lake/thadam/v1/p/d/k1");
    assert_eq!(uploaded[0].record.range.last(), WalPos::new(4));

    // Local retention turns `Uploaded` into `Deleted { uploaded: true }`;
    // the key must survive (the S3 object is still the registrar's target).
    let report = catalog
        .release_local(&LocalRetention::delete_after_upload())
        .await
        .unwrap();
    assert_eq!(report.deleted_files, 1);
    let uploaded = catalog.uploaded();
    assert_eq!(uploaded.len(), 1, "released uploads keep their key");
    assert_eq!(uploaded[0].key, "lake/thadam/v1/p/d/k1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finalize_on_shutdown_false_leaves_buffer_uncommitted() {
    let cfg_fixture = ParquetFixture::new(&ONE, RotationConfig::never(), 4);
    let ctx = cfg_fixture.ctx();
    let mut cfg = cfg_fixture.batch_config();
    cfg.finalize_on_shutdown = false;
    let mut sink = crate::sink::ParquetSegmentSink::new(cfg).unwrap();
    sink.recover(&ctx).await.unwrap();
    sink.append(batch(&ONE, 1, 3)).await.unwrap();
    assert_eq!(
        sink.finalize_all().await.unwrap(),
        None,
        "nothing closes at shutdown"
    );
    assert!(files_of(&cfg_fixture.dir.path().join("ready")).is_empty());

    drop(sink);
    let mut restarted = cfg_fixture.open_sink();
    assert_eq!(
        restarted.recover(&ctx).await.unwrap(),
        None,
        "nothing durable"
    );
    restarted.append(batch(&ONE, 1, 3)).await.unwrap();
    assert_eq!(
        restarted.finalize_all().await.unwrap(),
        Some(WalPos::new(3))
    );
}

// --------------------------------------------------------------- quarantine

/// Quarantine events in the runtime's exact quarantine shape drive the
/// quarantine sink end to end, on the quarantine budget category.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quarantine_sink_writes_the_fixed_schema() {
    use thadam_core::{
        BuildContext, FieldDef, FieldType, Record, Shape, ShapeRef, TimeDomain, Times, Timestamp,
        Value,
    };

    let dir = tempfile::tempdir().unwrap();
    let factory = crate::QuarantineSinkFactory::new();
    let build = BuildContext::new("quarantine.main", fixture_clock(), dir.path().to_path_buf());
    let mut sink = thadam_core::SegmentSinkFactory::build(&factory, serde_json::json!({}), &build)
        .unwrap_or_else(|e| panic!("quarantine sink builds: {e}"));

    let fixture = ParquetFixture::new(&ABC, RotationConfig::never(), 4);
    let _ctx = fixture.ctx();
    // The quarantine budget category was charged on the fixture's budget;
    // use a fresh one carrying the category limit instead.
    let budget = Arc::new(
        thadam_core::DiskBudget::new().with_limit(thadam_core::BudgetCategory::Quarantine, 1 << 20),
    );
    let qctx = thadam_core::SinkContext::new(
        thadam_core::SinkId::new("quarantine.main"),
        thadam_core::Shutdown::new(),
        fixture.clock.shared(),
        dir.path().to_path_buf(),
        budget,
    );
    sink.recover(&qctx).await.unwrap();

    let shape = ShapeRef::new(
        Shape::new(vec![
            FieldDef::new("_wal_epoch", FieldType::U64).required(),
            FieldDef::new("_wal_pos", FieldType::U64).required(),
            FieldDef::new("_ingest_time", FieldType::Ts),
            FieldDef::new("sink_id", FieldType::Str).required(),
            FieldDef::new("error_domain", FieldType::Str).required(),
            FieldDef::new("error_code", FieldType::Str).required(),
            FieldDef::new("reason", FieldType::Str).required(),
            FieldDef::new("raw_truncated", FieldType::Bool).required(),
        ])
        .unwrap(),
    );
    let mut events = Vec::new();
    for pos in 1..=4u64 {
        let rec = Record::new(
            shape.clone(),
            vec![
                Value::U64(1),
                Value::U64(pos),
                Value::Ts(Timestamp::from_nanos(1_500_000_000)),
                Value::str("main"),
                Value::str("decode"),
                Value::str("decode.bad_json"),
                Value::str("not json"),
                Value::Bool(false),
            ],
        )
        .unwrap();
        events.push(
            Event::builder(
                thadam_core::EventId::from_u128(u128::from(pos)),
                WalPos::new(pos),
                Times::new(Timestamp::from_nanos(1_500_000_000), TimeDomain::Unknown),
            )
            .payload(rec)
            .build()
            .unwrap(),
        );
    }
    let batch = SegmentBatch::single(
        thadam_core::QUARANTINE_DATASET,
        EventBatch::new(
            events,
            WalRange::new(WalEpoch::new(1), WalPos::new(1), WalPos::new(4)).unwrap(),
            true,
        ),
    );
    sink.append(batch).await.unwrap();
    let w = sink.finalize_all().await.unwrap();
    assert_eq!(w, Some(WalPos::new(4)));

    // Read the file back: all 13 fixed columns, four rows.
    let files = files_of(
        &dir.path()
            .join("ready")
            .join(thadam_core::QUARANTINE_DATASET),
    );
    assert_eq!(files.len(), 1, "one quarantine file");
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(&files[0]).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert_eq!(builder.schema().fields().len(), 13);
    let mut rows = 0;
    for b in builder.build().unwrap() {
        rows += b.unwrap().num_rows();
    }
    assert_eq!(rows, 4);
}

fn fixture_clock() -> Arc<dyn thadam_core::Clock> {
    thadam_testkit::FakeClock::default().shared()
}

#[test]
fn catalog_version_bumps_on_appends() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let fixture = ParquetFixture::new(&ONE, RotationConfig::never(), 4);
        let ctx = fixture.ctx();
        let mut sink = fixture.open_sink();
        sink.recover(&ctx).await.unwrap();
        let catalog = sink.catalog().unwrap().clone();
        let rx = catalog.subscribe();
        sink.append(batch(&ONE, 1, 2)).await.unwrap();
        sink.finalize_all().await.unwrap();
        assert!(rx.borrow().0 >= 1, "finalize bumped the version");
    });
}
