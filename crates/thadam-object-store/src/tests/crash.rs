//! Crash/restart recovery: no duplicate and no missing objects across a
//! crash between put and catalog mark (`ship::after_put_before_mark`) and a
//! crash mid-multipart (`ship::mid_multipart`).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use futures_util::StreamExt;

use super::behavior::sha_hex_of;
use super::fixture::TestSink;
use super::fixture::{
    FlakyStore, RecordingMetrics, catalog_segment, make_config, make_record, open_catalog,
    run_service_until,
};
use crate::config::RetentionMode;
use crate::failpoints::{Failpoint, Failpoints};
use crate::shipper::Shipper;

const TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_between_put_and_mark_uploaded_recovers_without_duplicates() {
    let sink = TestSink::new();
    let catalog = open_catalog(&sink).await;
    let record = make_record(&sink, 7, 0, 9, 512, true, "date=2026-10-08/hour=13");
    catalog_segment(&catalog, record.clone()).await;

    // First run: the object lands, the process dies before the catalog mark.
    let store = FlakyStore::new();
    let fps = Failpoints::default();
    fps.arm(Failpoint::AfterPutBeforeMark);
    let shipper1 = Shipper::new(
        "s3",
        make_config("demo", "", RetentionMode::KeepFor),
        store.store(),
    )
    .with_failpoints(fps.clone());
    let metrics1 = RecordingMetrics::new();
    let outcome1 = run_service_until(shipper1, &catalog, |_c, _m| false, &metrics1, TIMEOUT).await;
    let error1 = outcome1.expect_err("the armed shipper crashed");
    assert_eq!(error1.retry, thadam_core::Retry::Fatal, "{error1}");

    // Restart: a fresh catalog (log replay) and a fresh shipper.
    let catalog2 = open_catalog(&sink).await;
    assert_eq!(
        catalog2.eligible_for_upload().len(),
        1,
        "still eligible after restart"
    );
    let metrics2 = RecordingMetrics::new();
    let shipper2 = Shipper::new(
        "s3",
        make_config("demo", "", RetentionMode::KeepFor),
        store.store(),
    )
    .with_metrics(metrics2.clone());
    run_service_until(
        shipper2,
        &catalog2,
        |c, _m| c.eligible_for_upload().is_empty(),
        &metrics2,
        TIMEOUT,
    )
    .await
    .expect("second run");

    let mut objects = Vec::new();
    let mut stream = store.store().list(None);
    while let Some(m) = stream.next().await {
        objects.push(m.expect("list").location.to_string());
    }
    assert_eq!(
        objects.len(),
        1,
        "exactly one object survived the crash: {objects:?}"
    );
    assert_eq!(
        metrics2.conflicts.lock().unwrap().as_slice(),
        &[true],
        "settled by verification"
    );
    assert!(
        metrics1.retries.lock().unwrap().is_empty(),
        "crash is not a retry path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_mid_multipart_aborts_and_restarts_cleanly() {
    let sink = TestSink::new();
    let catalog = open_catalog(&sink).await;
    let record = make_record(&sink, 7, 0, 9, 512, true, "date=2026-10-08/hour=13");
    catalog_segment(&catalog, record.clone()).await;

    let store = FlakyStore::new();
    let mut config = make_config("demo", "", RetentionMode::KeepFor);
    config.multipart_threshold = 100;
    config.part_size = 64;

    let fps = Failpoints::default();
    fps.arm(Failpoint::MidMultipart);
    let shipper1 = Shipper::new("s3", config.clone(), store.store()).with_failpoints(fps.clone());
    let metrics1 = RecordingMetrics::new();
    let outcome1 = run_service_until(shipper1, &catalog, |_c, _m| false, &metrics1, TIMEOUT).await;
    assert!(outcome1.is_err(), "crash consumed the run");

    let catalog2 = open_catalog(&sink).await;
    let metrics2 = RecordingMetrics::new();
    let shipper2 = Shipper::new("s3", config, store.store()).with_metrics(metrics2.clone());
    run_service_until(
        shipper2,
        &catalog2,
        |c, _m| c.eligible_for_upload().is_empty(),
        &metrics2,
        TIMEOUT,
    )
    .await
    .expect("second run");

    let mut objects = Vec::new();
    let mut stream = store.store().list(None);
    while let Some(m) = stream.next().await {
        objects.push(m.expect("list").location.to_string());
    }
    assert_eq!(
        objects.len(),
        1,
        "no partial or duplicate objects: {objects:?}"
    );
    let get = store
        .store()
        .get_opts(
            &object_store::path::Path::from(objects[0].as_str()),
            Default::default(),
        )
        .await
        .expect("get");
    let sha = get
        .attributes
        .get(&object_store::Attribute::Metadata(
            std::borrow::Cow::Borrowed("thadam-sha256"),
        ))
        .map(|v| v.to_string())
        .expect("sha metadata");
    let local_body =
        std::fs::read(sink.root().join(record.path.as_ref().unwrap())).expect("local segment");
    assert_eq!(sha, sha_hex_of(&local_body));
}
