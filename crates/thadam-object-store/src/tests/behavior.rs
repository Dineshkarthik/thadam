//! Shipper behavior over the in-memory store: happy path, ordering,
//! concurrency bound, retry, conditional-put conflicts, multipart.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use object_store::ObjectStore;

use super::fixture::{
    FlakyStore, LaxCreateStore, RecordingConnectorMetrics, RecordingMetrics, TestSink,
    catalog_segment, make_config, make_record, open_catalog, run_service_until,
    run_service_until_hooked,
};
use crate::config::{ConditionalPut, RetentionMode};
use crate::shipper::Shipper;

const TIMEOUT: Duration = Duration::from_secs(30);

async fn fixture_one(
    sizes: &[(u64, u64)], // (first, last) WAL positions
) -> (
    TestSink,
    thadam_segments::SegmentCatalog,
    Vec<thadam_segments::SegmentRecord>,
) {
    let sink = TestSink::new();
    let catalog = open_catalog(&sink).await;
    let mut records = Vec::new();
    for (first, last) in sizes {
        let record = make_record(
            &sink,
            7,
            *first,
            *last,
            512,
            true,
            "date=2026-10-08/hour=13",
        );
        catalog_segment(&catalog, record.clone()).await;
        records.push(record);
    }
    (sink, catalog, records)
}

async fn object_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    store
        .list(None)
        .map(|m| m.map(|m| m.location.to_string()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("list")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uploads_marks_and_deletes_local_copies() {
    let (sink, catalog, records) = fixture_one(&[(0, 9)]).await;
    let store = FlakyStore::new();
    let metrics = RecordingMetrics::new();
    let config = make_config("demo", "", RetentionMode::DeleteAfterUpload);
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |_c, m| m.uploaded_count() >= 1,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    let keys = object_keys(&store.store()).await;
    let expected = format!(
        "thadam/v1/demo/telemetry/date=2026-10-08/hour=13/{}",
        records[0]
            .path
            .as_ref()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
    );
    assert_eq!(keys.len(), 1, "objects: {keys:?}");
    assert_eq!(keys[0], expected);

    let result = store
        .store()
        .get_opts(
            &object_store::path::Path::from(expected.as_str()),
            Default::default(),
        )
        .await
        .expect("get");
    let sha = result
        .attributes
        .get(&object_store::Attribute::Metadata(
            std::borrow::Cow::Borrowed("thadam-sha256"),
        ))
        .map(|v| v.to_string())
        .expect("sha metadata");
    assert_eq!(sha.len(), 64);
    let body: Vec<u8> = result
        .into_stream()
        .map(|b| b.map(|b| b.to_vec()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<u8>>, _>>()
        .expect("body")
        .concat();
    let abs = sink.root().join(records[0].path.as_ref().unwrap());
    assert_eq!(body.len(), 512);
    assert!(
        !abs.exists(),
        "delete_after_upload removed the local segment"
    );
    assert!(catalog.eligible_for_upload().is_empty(), "marked uploaded");
    assert_eq!(metrics.uploaded.lock().unwrap()[0].1, "put");
}

fn segment_firsts(order: &[String]) -> Vec<u64> {
    order
        .iter()
        .map(|k| {
            k.rsplit('_')
                .nth(1)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(u64::MAX)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uploads_go_oldest_first() {
    let (_sink, catalog, _records) = fixture_one(&[(0, 9), (10, 19), (20, 29), (30, 39)]).await;
    let store = FlakyStore::new().with_delay(Duration::from_millis(2));
    let metrics = RecordingMetrics::new();
    let config = make_config("demo", "", RetentionMode::KeepFor);
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |_c, m| m.uploaded_count() >= 4,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    let order: Vec<String> = store
        .order
        .lock()
        .unwrap()
        .iter()
        .filter(|k| k.starts_with("thadam/v1/"))
        .cloned()
        .collect();
    assert_eq!(order.len(), 4);
    let mut sorted = segment_firsts(&order);
    sorted.sort_unstable();
    assert_eq!(
        segment_firsts(&order),
        sorted,
        "uploads went oldest first: {order:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_uploads_are_bounded_by_upload_concurrency() {
    let (_sink, catalog, _records) = fixture_one(&[(0, 9), (10, 19), (20, 29), (30, 39)]).await;
    let store = FlakyStore::new().with_delay(Duration::from_millis(30));
    let metrics = RecordingMetrics::new();
    let mut config = make_config("demo", "", RetentionMode::KeepFor);
    config.upload_concurrency = 2;
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |_c, m| m.uploaded_count() >= 4,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    assert_eq!(
        store
            .max_concurrent
            .load(std::sync::atomic::Ordering::Relaxed),
        2,
        "the semaphore bound held"
    );
    assert_eq!(object_keys(&store.store()).await.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_failures_retry_until_success() {
    let (_sink, catalog, _records) = fixture_one(&[(0, 9)]).await;
    let store = FlakyStore::new();
    store.fail_puts(3);
    let metrics = RecordingMetrics::new();
    let config = make_config("demo", "", RetentionMode::KeepFor);
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |_c, m| m.uploaded_count() >= 1,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    assert_eq!(
        metrics.retries.lock().unwrap().len(),
        3,
        "three injected failures retried"
    );
    assert_eq!(
        object_keys(&store.store()).await.len(),
        1,
        "no duplicate objects"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_with_identical_content_is_verified_success() {
    let (sink, catalog, records) = fixture_one(&[(0, 9)]).await;
    let store = FlakyStore::new();
    // A previous run uploaded the object and died before marking the catalog.
    let key = format!(
        "thadam/v1/demo/telemetry/date=2026-10-08/hour=13/{}",
        records[0]
            .path
            .as_ref()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
    );
    let abs = sink.root().join(records[0].path.as_ref().unwrap());
    let body = std::fs::read(&abs).expect("segment bytes");
    let mut attrs = object_store::Attributes::new();
    attrs.insert(
        object_store::Attribute::Metadata(std::borrow::Cow::Borrowed("thadam-sha256")),
        sha_hex_of(&body).into(),
    );
    store
        .store()
        .put_opts(
            &object_store::path::Path::from(key.as_str()),
            body.clone().into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Overwrite,
                attributes: attrs,
                ..Default::default()
            },
        )
        .await
        .expect("out-of-band put");

    let metrics = RecordingMetrics::new();
    let config = make_config("demo", "", RetentionMode::KeepFor);
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |c, m| c.eligible_for_upload().is_empty() && m.uploaded_count() >= 1,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    assert_eq!(metrics.conflicts.lock().unwrap().as_slice(), &[true]);
    assert_eq!(
        object_keys(&store.store()).await.len(),
        1,
        "no duplicate object"
    );
    let after = store
        .store()
        .get_opts(
            &object_store::path::Path::from(key.as_str()),
            Default::default(),
        )
        .await
        .expect("get");
    let bytes: Vec<u8> = after
        .into_stream()
        .map(|b| b.map(|b| b.to_vec()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<u8>>, _>>()
        .expect("bytes")
        .concat();
    assert_eq!(bytes, body, "the original object was not overwritten");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_with_different_content_is_a_fatal_collision() {
    let (sink, catalog, records) = fixture_one(&[(0, 9)]).await;
    let store = FlakyStore::new();
    // Another writer (a misconfigured node) owns this key with a larger object.
    let key = format!(
        "thadam/v1/demo/telemetry/date=2026-10-08/hour=13/{}",
        records[0]
            .path
            .as_ref()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
    );
    store
        .store()
        .put_opts(
            &object_store::path::Path::from(key.as_str()),
            vec![0xffu8; 700].into(),
            object_store::PutMode::Overwrite.into(),
        )
        .await
        .expect("out-of-band put");

    let metrics = RecordingMetrics::new();
    let config = make_config("demo", "", RetentionMode::KeepFor);
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    let outcome = run_service_until(shipper, &catalog, |_c, _m| false, &metrics, TIMEOUT).await;
    let error = outcome.expect_err("fatal collision");
    assert_eq!(error.code, "sink.object_key_collision", "{error}");
    assert_eq!(error.retry, thadam_core::Retry::Fatal);
    assert!(!catalog.eligible_for_upload().is_empty(), "not marked");
    assert_eq!(metrics.conflicts.lock().unwrap().as_slice(), &[false]);
    assert!(sink.root().join(records[0].path.as_ref().unwrap()).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_above_the_threshold_go_multipart_with_metadata() {
    let (sink, catalog, records) = fixture_one(&[(0, 9)]).await;
    let store = FlakyStore::new();
    let metrics = RecordingMetrics::new();
    let mut config = make_config("demo", "", RetentionMode::KeepFor);
    config.multipart_threshold = 100;
    config.part_size = 64;
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |_c, m| m.uploaded_count() >= 1,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    let keys = object_keys(&store.store()).await;
    assert_eq!(keys.len(), 1);
    assert_eq!(metrics.uploaded.lock().unwrap()[0].1, "multipart");
    let result = store
        .store()
        .get_opts(
            &object_store::path::Path::from(keys[0].as_str()),
            Default::default(),
        )
        .await
        .expect("get");
    let sha = result
        .attributes
        .get(&object_store::Attribute::Metadata(
            std::borrow::Cow::Borrowed("thadam-sha256"),
        ))
        .map(|v| v.to_string())
        .expect("sha metadata present on multipart objects");
    let abs = sink.root().join(records[0].path.as_ref().unwrap());
    let local_body = std::fs::read(&abs).expect("local segment");
    assert_eq!(sha, sha_hex_of(&local_body));
    let body: Vec<u8> = result
        .into_stream()
        .map(|b| b.map(|b| b.to_vec()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<u8>>, _>>()
        .expect("bytes")
        .concat();
    assert_eq!(body, local_body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_falls_back_to_overwrite_when_the_store_ignores_conditions() {
    // The SeaweedFS scenario in memory: `conditional_put: auto` against a
    // store that ignores `If-None-Match` (a create-mode put overwrites).
    // The probe must catch that, the shipper must fall back to overwrite —
    // and, per the documented narrowing, a foreign object at the
    // deterministic key is overwritten by the segment payload.
    let (sink, catalog, records) = fixture_one(&[(0, 9)]).await;
    let store = LaxCreateStore::new();
    let key = format!(
        "thadam/v1/demo/telemetry/date=2026-10-08/hour=13/{}",
        records[0]
            .path
            .as_ref()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
    );
    store
        .store()
        .put_opts(
            &object_store::path::Path::from(key.as_str()),
            vec![0xffu8; 700].into(),
            object_store::PutMode::Overwrite.into(),
        )
        .await
        .expect("foreign put");

    let metrics = RecordingMetrics::new();
    let mut config = make_config("demo", "", RetentionMode::KeepFor);
    config.conditional_put = ConditionalPut::Auto;
    let shipper = Shipper::new("s3", config, store.store()).with_metrics(metrics.clone());

    run_service_until(
        shipper,
        &catalog,
        |c, m| c.eligible_for_upload().is_empty() && m.uploaded_count() >= 1,
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("probe must not turn a conditionless store into a fatal");

    assert_eq!(metrics.uploaded.lock().unwrap()[0].1, "put");
    assert!(
        metrics.conflicts.lock().unwrap().is_empty(),
        "no conflict verification happens in overwrite fallback"
    );
    assert_eq!(object_keys(&store.store()).await.len(), 1, "no duplicate");
    let abs = sink.root().join(records[0].path.as_ref().unwrap());
    let local = std::fs::read(&abs).expect("local segment");
    let after = store
        .store()
        .get_opts(
            &object_store::path::Path::from(key.as_str()),
            Default::default(),
        )
        .await
        .expect("get");
    let sha = after
        .attributes
        .get(&object_store::Attribute::Metadata(
            std::borrow::Cow::Borrowed("thadam-sha256"),
        ))
        .map(|v| v.to_string())
        .expect("sha metadata after fallback put");
    assert_eq!(sha, sha_hex_of(&local));
    let body: Vec<u8> = after
        .into_stream()
        .map(|b| b.map(|b| b.to_vec()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<u8>>, _>>()
        .expect("bytes")
        .concat();
    assert_eq!(
        body, local,
        "the segment payload replaced the foreign object"
    );
}

pub(crate) fn sha_hex_of(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest;
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Without an explicit [`crate::ShipMetrics`] receiver the shipper reports
/// through the service context's metrics hook: the upload lands as
/// `ShipUploaded` and the catalog transitions surface as `SegmentFiles`
/// snapshots labeled with the `from` sink.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_through_the_context_metrics_hook() {
    let (_sink, catalog, _records) = fixture_one(&[(0, 9)]).await;
    let store = FlakyStore::new();
    let metrics = RecordingConnectorMetrics::new();
    let config = make_config("demo", "", RetentionMode::DeleteAfterUpload);
    let shipper = Shipper::new("s3", config, store.store());

    run_service_until_hooked(
        shipper,
        &catalog,
        |_c, m| {
            !m.matching(r#"ShipUploaded { sink: "s3", outcome: "ok", bytes: 512"#)
                .is_empty()
                && !m
                    .matching(
                        r#"SegmentFiles { sink: "local", state: "uploaded", files: 1, bytes: 512"#,
                    )
                    .is_empty()
                && !m
                    .matching(r#"SegmentFiles { sink: "local", state: "deleted", files: 1"#)
                    .is_empty()
        },
        &metrics,
        TIMEOUT,
    )
    .await
    .expect("service");

    assert_eq!(metrics.matching("ShipUploaded").len(), 1);
    assert!(metrics.matching("ShipRetried").is_empty());
    // The eligible series dropped to zero when the record shipped.
    assert!(
        metrics
            .matching(r#"SegmentFiles { sink: "local", state: "eligible", files: 0"#)
            .len()
            >= 2,
        "eligibility scan and upload each publish absolute totals"
    );
}
