//! Integration tests for the MCAP source: fixtures are written with the
//! `mcap` crate's writer, then read back through `McapSource`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{JOINT_STATE_TOPIC, JSON_TOPIC, Msg, joint_state_schema, write_mcap, write_mcap_with};
use thadam_core::{
    AckOutcome, AckStatus, BatchLimits, ErrorDomain, RawMessage, Retry, Shutdown, Source,
    SourceContext, SourceId, SystemClock,
};
use thadam_mcap::{McapConfig, McapSource, attr};

fn source_for(cfg: serde_json::Value) -> (McapSource, Shutdown) {
    let config = McapConfig::from_value(&cfg).expect("valid config");
    let shutdown = Shutdown::new();
    (McapSource::new(config), shutdown)
}

async fn open(cfg: serde_json::Value) -> (McapSource, Shutdown) {
    let (mut source, shutdown) = source_for(cfg);
    let ctx = SourceContext::new(
        SourceId::new("test"),
        shutdown.clone(),
        Arc::new(SystemClock::new()),
    );
    source.open(&ctx).await.expect("open");
    (source, shutdown)
}

fn limits(max_messages: usize) -> BatchLimits {
    BatchLimits {
        max_messages,
        max_bytes: 1 << 24,
        linger: Duration::from_millis(50),
    }
}

/// Pulls everything, completing every ack. `Err` carries the first failure.
async fn collect_all(source: &mut McapSource) -> Result<Vec<RawMessage>, thadam_core::ThadamError> {
    let mut out = Vec::new();
    while let Some(batch) = source.next_batch(limits(4)).await? {
        out.extend(batch.batch.messages);
        batch.ack.complete(AckOutcome::Committed);
    }
    Ok(out)
}

/// (topic, log_time) per message, for order assertions.
fn trace(messages: &[RawMessage]) -> Vec<(String, u64)> {
    messages
        .iter()
        .map(|m| {
            (
                m.attrs.get(attr::MCAP_TOPIC).unwrap_or("").to_owned(),
                m.attrs
                    .get(attr::MCAP_LOG_TIME)
                    .and_then(|v| v.parse().ok())
                    .expect("log_time parses"),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_all_messages_in_log_time_order_across_files() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    let b = dir.path().join("b.mcap");
    // Both files are log-time ordered within themselves (the documented
    // contract; recorders write that way). File A is zstd-compressed and
    // spans several chunks.
    let mut msgs_a = vec![
        Msg::joint(100, 1),
        Msg::json(200, 1),
        Msg::joint(300, 2),
        Msg::json(400, 2),
        Msg::joint(500, 3),
    ];
    for i in 0..7 {
        msgs_a.push(Msg::joint(600 + i * 10, 10 + i as u32));
    }
    // File B: uncompressed chunks.
    let msgs_b = vec![
        Msg::joint(150, 1),
        Msg::json(250, 1),
        Msg::joint(350, 2),
        Msg::other("/tf/static", 450, 1),
    ];
    write_mcap(&a, &msgs_a, true);
    write_mcap(&b, &msgs_b, false);

    let cfg = serde_json::json!({ "paths": [b.to_str().unwrap(), a.to_str().unwrap()] });
    let (mut source, _shutdown) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");

    let mut want: Vec<u64> = vec![100, 150, 200, 250, 300, 350, 400, 450, 500];
    want.extend((600..=660).step_by(10));
    let got: Vec<u64> = trace(&messages).into_iter().map(|(_, t)| t).collect();
    assert_eq!(got, want);

    // Payloads came through byte-identical, on their own channels.
    let first = &messages[0];
    assert_eq!(first.payload.as_ref(), common::joint_state_payload());
    let json = messages
        .iter()
        .find(|m| m.attrs.get(attr::MCAP_TOPIC) == Some(JSON_TOPIC))
        .expect("json message");
    assert!(json.payload.starts_with(b"{\"note\":"));
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_filter_exact_and_slash_prefix() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    write_mcap(
        &a,
        &[
            Msg::joint(100, 1),
            Msg::other("/tf/static", 200, 2),
            Msg::other("/tf", 250, 3),
            Msg::json(300, 4),
            Msg::other("/joint_states_extra", 400, 5),
        ],
        true,
    );

    let cfg = serde_json::json!({
        "paths": [a.to_str().unwrap()],
        "topics": ["/joint_states", "/tf/"],
    });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");
    let got = trace(&messages);
    assert_eq!(
        got,
        vec![
            (JOINT_STATE_TOPIC.to_owned(), 100),
            ("/tf/static".to_owned(), 200)
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_time_bounds_are_inclusive() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    write_mcap(
        &a,
        &[
            Msg::joint(100, 1),
            Msg::joint(200, 2),
            Msg::joint(300, 3),
            Msg::joint(400, 4),
            Msg::joint(500, 5),
        ],
        false,
    );

    // Integer nanoseconds here; RFC 3339 is covered by the config tests.
    let cfg = serde_json::json!({
        "paths": [a.to_str().unwrap()],
        "start": 200,
        "end": 400,
    });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");
    let got: Vec<u64> = trace(&messages).into_iter().map(|(_, t)| t).collect();
    assert_eq!(got, vec![200, 300, 400]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attributes_and_deterministic_ingest_times_with_pace_max() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    write_mcap(
        &a,
        &[Msg::joint(1_758_000_000_000_000_042, 41), Msg::json(2, 1)],
        true,
    );

    let cfg = serde_json::json!({ "paths": [a.to_str().unwrap()], "pace": "max" });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");
    assert_eq!(messages.len(), 2);

    let joint = &messages[0];
    let log_time = 1_758_000_000_000_000_042u64;
    // Deterministic replay: with `pace: max` the ingest time IS the log time.
    assert_eq!(
        joint.ingest_time,
        thadam_core::Timestamp::from_nanos(log_time as i64)
    );
    let a = &joint.attrs;
    assert_eq!(a.get("topic"), Some(JOINT_STATE_TOPIC));
    assert_eq!(a.get(attr::MCAP_TOPIC), Some(JOINT_STATE_TOPIC));
    assert_eq!(
        a.get(attr::MCAP_SCHEMA_NAME),
        Some(common::JOINT_STATE_SCHEMA_NAME)
    );
    assert_eq!(a.get(attr::MCAP_SCHEMA_ENCODING), Some("ros2msg"));
    assert_eq!(a.get(attr::MCAP_MESSAGE_ENCODING), Some("cdr"));
    assert!(
        a.get(attr::MCAP_CHANNEL_ID)
            .is_some_and(|v| v.parse::<u16>().is_ok())
    );
    assert_eq!(a.get(attr::MCAP_SEQUENCE), Some("41"));
    assert_eq!(a.get(attr::MCAP_LOG_TIME), Some("1758000000000000042"));
    assert_eq!(a.get(attr::MCAP_PUBLISH_TIME), Some("1758000000000000049"));

    let json = &messages[1];
    assert_eq!(
        json.attrs.get(attr::MCAP_SCHEMA_ENCODING),
        Some("jsonschema")
    );
    assert_eq!(json.attrs.get(attr::MCAP_MESSAGE_ENCODING), Some("json"));
    assert!(json.attrs.get(attr::MCAP_ROS2_FIELDS).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_end_noop_acks_and_close_are_stable() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    write_mcap(&a, &[Msg::joint(10, 1)], false);

    let cfg = serde_json::json!({ "paths": [a.to_str().unwrap()] });
    let (mut source, _) = open(cfg).await;

    let batch = source
        .next_batch(limits(8))
        .await
        .expect("batch")
        .expect("some");
    assert_eq!(batch.batch.len(), 1);
    // Acks are no-ops and complete cleanly, in receive order.
    assert_eq!(
        batch.ack.complete(AckOutcome::Committed),
        AckStatus::Delivered
    );

    assert!(source.next_batch(limits(8)).await.expect("end").is_none());
    assert!(
        source
            .next_batch(limits(8))
            .await
            .expect("still end")
            .is_none()
    );
    source.close().await.expect("close");
    assert!(
        source
            .next_batch(limits(8))
            .await
            .expect("end after close")
            .is_none()
    );
    source.close().await.expect("close again");

    assert_eq!(source.capabilities().ack_mode, thadam_core::AckMode::None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_file_and_empty_directory_are_handled() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let empty = dir.path().join("empty.mcap");
    write_mcap(&empty, &[], false);
    let cfg = serde_json::json!({ "paths": [empty.to_str().unwrap()] });
    let (mut source, _) = open(cfg).await;
    assert!(source.next_batch(limits(8)).await.expect("end").is_none());

    // A directory with no matching files fails open with a config error.
    let bare = dir.path().join("bare");
    std::fs::create_dir(&bare).expect("mkdir");
    let (mut source, _) = source_for(serde_json::json!({ "paths": [bare.to_str().unwrap()] }));
    let ctx = SourceContext::new(
        SourceId::new("test"),
        Shutdown::new(),
        Arc::new(SystemClock::new()),
    );
    let err = source.open(&ctx).await.expect_err("open must fail");
    assert_eq!(err.domain, ErrorDomain::Config);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_input_expands_sorts_and_merges() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let bags = dir.path().join("bags");
    std::fs::create_dir(&bags).expect("mkdir");
    write_mcap(
        &bags.join("b.mcap"),
        &[Msg::json(10, 1), Msg::other("/z", 100, 2)],
        false,
    );
    write_mcap(
        &bags.join("a.mcap"),
        &[Msg::other("/a", 50, 1), Msg::other("/a", 100, 2)],
        false,
    );
    std::fs::write(bags.join("notes.txt"), b"not a bag").expect("txt");

    let cfg = serde_json::json!({ "paths": [bags.to_str().unwrap()] });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");
    let got = trace(&messages);
    assert_eq!(
        got,
        vec![
            (JSON_TOPIC.to_owned(), 10),
            ("/a".to_owned(), 50),
            // Tie on log_time breaks toward the sorted-first file (a.mcap).
            ("/a".to_owned(), 100),
            ("/z".to_owned(), 100),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn realtime_pacing_respects_recorded_gaps() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    let ms = 1_000_000u64;
    write_mcap(
        &a,
        &[
            Msg::joint(0, 1),
            Msg::joint(100 * ms, 2),
            Msg::joint(150 * ms, 3),
        ],
        false,
    );

    let cfg = serde_json::json!({ "paths": [a.to_str().unwrap()], "pace": "realtime" });
    let (mut source, _) = open(cfg).await;

    let started = tokio::time::Instant::now();
    // One call fills the batch across both gaps (linger is generous).
    let batch = tokio::time::timeout(
        Duration::from_secs(60),
        source.next_batch(BatchLimits {
            max_messages: 8,
            max_bytes: 1 << 24,
            linger: Duration::from_secs(10),
        }),
    )
    .await
    .expect("no real time may pass on a paused clock")
    .expect("batch")
    .expect("some");
    let elapsed = started.elapsed();
    assert_eq!(batch.batch.len(), 3);
    // The two gaps (100 ms, 50 ms) replay at 1x: virtual time advanced 150 ms.
    assert_eq!(elapsed, Duration::from_millis(150));

    // A 2x factor halves a 100 ms gap.
    let cfg = serde_json::json!({ "paths": [a.to_str().unwrap()], "pace": 2.0 });
    let (mut source, _) = open(cfg).await;
    let started = tokio::time::Instant::now();
    let batch = source
        .next_batch(BatchLimits {
            max_messages: 8,
            max_bytes: 1 << 24,
            linger: Duration::from_secs(10),
        })
        .await
        .expect("batch")
        .expect("some");
    let elapsed = started.elapsed();
    assert_eq!(batch.batch.len(), 3);
    assert_eq!(elapsed, Duration::from_millis(75));
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_a_pacing_wait() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    let hour = 3_600_000_000_000u64;
    write_mcap(&a, &[Msg::joint(0, 1), Msg::joint(hour, 2)], false);

    let cfg = serde_json::json!({ "paths": [a.to_str().unwrap()], "pace": "realtime" });
    let (mut source, shutdown) = open(cfg).await;

    let trigger = shutdown.clone();
    let canceller = async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        trigger.trigger();
    };
    let started = tokio::time::Instant::now();
    let (batch, ()) = tokio::join!(
        source.next_batch(BatchLimits {
            max_messages: 8,
            max_bytes: 1 << 24,
            linger: Duration::from_secs(3600),
        }),
        canceller
    );
    let elapsed = started.elapsed();
    // The first message was delivered immediately; the hour-long gap was
    // cancelled two virtual seconds in, and the batch holds what arrived.
    let batch = batch.expect("next_batch").expect("some");
    assert_eq!(batch.batch.len(), 1);
    assert_eq!(elapsed, Duration::from_secs(2));
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_files_fail_with_a_classified_error_never_a_panic() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let full = dir.path().join("full.mcap");
    let messages: Vec<Msg> = (0..12).map(|i| Msg::joint(i * 10, i as u32)).collect();
    write_mcap(&full, &messages, true);
    let original = std::fs::read(&full).expect("read");
    let len = original.len();

    let mut classified = 0usize;
    for cut in (24..len).step_by(13) {
        let p = dir.path().join("cut.mcap");
        std::fs::write(&p, &original[..cut]).expect("write");
        let (mut source, shutdown) =
            source_for(serde_json::json!({ "paths": [p.to_str().unwrap()] }));
        let ctx = SourceContext::new(SourceId::new("t"), shutdown, Arc::new(SystemClock::new()));
        let outcome = match source.open(&ctx).await {
            Ok(()) => collect_all(&mut source).await.map(|_| ()),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(()) => {}
            Err(e) => {
                classified += 1;
                assert!(
                    e.domain == ErrorDomain::Connector || e.domain == ErrorDomain::Schema,
                    "unexpected domain: {e}"
                );
                assert_ne!(
                    e.retry,
                    Retry::Transient,
                    "corruption is not transient: {e}"
                );
            }
        }
    }
    // A file cut anywhere before the DataEnd record MUST be an error, and
    // most of the sweep cut into the data section.
    assert!(
        classified > 10,
        "too few classified truncations: {classified}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_bytes_fail_with_a_classified_error_never_a_panic() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let full = dir.path().join("full.mcap");
    let messages: Vec<Msg> = (0..8).map(|i| Msg::joint(i * 10, i as u32)).collect();
    write_mcap(&full, &messages, false);
    let original = std::fs::read(&full).expect("read");
    let len = original.len();

    let mut classified = 0usize;
    // Flip one byte at a time across the data section (skip the first
    // magic and the trailing summary/magic region).
    for offset in (16..len.saturating_sub(16)).step_by(7) {
        let mut bytes = original.clone();
        bytes[offset] ^= 0xFF;
        let p = dir.path().join("corrupt.mcap");
        std::fs::write(&p, &bytes).expect("write");
        let (mut source, shutdown) =
            source_for(serde_json::json!({ "paths": [p.to_str().unwrap()] }));
        let ctx = SourceContext::new(SourceId::new("t"), shutdown, Arc::new(SystemClock::new()));
        let outcome = match source.open(&ctx).await {
            Ok(()) => collect_all(&mut source).await.map(|_| ()),
            Err(e) => Err(e),
        };
        if let Err(e) = outcome {
            classified += 1;
            assert!(
                e.domain == ErrorDomain::Connector || e.domain == ErrorDomain::Schema,
                "unexpected domain: {e}"
            );
        }
    }
    assert!(classified > 5, "corruption went undetected: {classified}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ros2_mode_preflights_schemas_and_attaches_fields() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let good = dir.path().join("good.mcap");
    write_mcap(&good, &[Msg::joint(10, 1), Msg::json(20, 2)], true);

    let cfg = serde_json::json!({ "paths": [good.to_str().unwrap()], "decode": "ros2" });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");
    let joint = &messages[0];
    let fields = joint
        .attrs
        .get(attr::MCAP_ROS2_FIELDS)
        .expect("ros2 fields attached");
    for want in ["header.stamp", "header.frame_id", "name", "position"] {
        assert!(fields.split(',').any(|f| f == want), "{fields}");
    }
    // The JSON channel stays untouched.
    assert!(messages[1].attrs.get(attr::MCAP_ROS2_FIELDS).is_none());

    // The same schema text decodes the carried payload end to end.
    let record = thadam_mcap::decode_ros2(
        common::JOINT_STATE_SCHEMA_NAME,
        &joint_state_schema(),
        &joint.payload,
    )
    .expect("decode");
    assert_eq!(
        record.get("header.frame_id").and_then(|v| v.as_str()),
        Some("arm")
    );

    // A schema that does not compile fails open with a Schema-domain error.
    let bad = dir.path().join("bad.mcap");
    write_mcap_with(
        &bad,
        &[Msg::joint(10, 1)],
        true,
        "this is not a msg definition",
    );
    let (mut source, shutdown) =
        source_for(serde_json::json!({ "paths": [bad.to_str().unwrap()], "decode": "ros2" }));
    let ctx = SourceContext::new(SourceId::new("t"), shutdown, Arc::new(SystemClock::new()));
    let err = source.open(&ctx).await.expect_err("open must fail");
    assert_eq!(err.domain, ErrorDomain::Schema, "{err}");

    // `decode: raw` (the default) never touches schema contents.
    let cfg = serde_json::json!({ "paths": [bad.to_str().unwrap()] });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("raw passes through");
    assert_eq!(messages.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ros2msg_schemas_are_stored_before_messages_and_hashed_on_every_message() {
    use thadam_core::{BuildContext, SchemaStore, SourceFactory};
    use thadam_mcap::McapSourceFactory;

    let dir = tempfile::tempdir().expect("tmpdir");
    let bag = dir.path().join("bag.mcap");
    // ros2msg + jsonschema + schemaless channels in one file.
    write_mcap(
        &bag,
        &[
            Msg::joint(10, 1),
            Msg::json(20, 2),
            Msg::other("/tf", 30, 3),
        ],
        false,
    );
    let schemas = dir.path().join("data").join("schemas");
    let expect_hash = SchemaStore::hash(
        common::JOINT_STATE_SCHEMA_NAME,
        "ros2msg",
        &joint_state_schema(),
    );

    // The factory path (BuildContext::schema_dir), in both decode modes.
    for decode in ["raw", "ros2"] {
        let ctx = BuildContext::new(
            "mcap-test",
            Arc::new(SystemClock::new()),
            dir.path().join("source"),
        )
        .with_schema_dir(&schemas);
        let mut source = McapSourceFactory::new()
            .build(
                serde_json::json!({ "paths": [bag.to_str().unwrap()], "decode": decode }),
                &ctx,
            )
            .expect("build");
        let source_ctx = SourceContext::new(
            SourceId::new("test"),
            Shutdown::new(),
            Arc::new(SystemClock::new()),
        );
        source.open(&source_ctx).await.expect("open");

        // BEFORE the first batch is ever returned the schema is durable.
        let store = SchemaStore::open(&schemas);
        let stored = store
            .get(&expect_hash)
            .expect("read")
            .unwrap_or_else(|| panic!("stored at open ({decode})"));
        assert_eq!(stored.text, joint_state_schema());
        assert_eq!(&*stored.name, common::JOINT_STATE_SCHEMA_NAME);
        assert_eq!(&*stored.encoding, "ros2msg");

        let mut messages = Vec::new();
        while let Some(batch) = source.next_batch(limits(4)).await.expect("batch") {
            messages.extend(batch.batch.messages);
        }
        // Every ros2msg-channel message carries the hash; the other
        // channels carry none and stored nothing.
        assert_eq!(messages.len(), 3);
        for m in &messages {
            let topic = m.attrs.get(attr::MCAP_TOPIC).unwrap_or("");
            let hash = m.attrs.get(attr::MCAP_SCHEMA_HASH);
            if topic == JOINT_STATE_TOPIC {
                assert_eq!(hash, Some(expect_hash.as_str()), "{decode}");
            } else {
                assert!(
                    hash.is_none(),
                    "{decode}: {topic} must not reference a schema"
                );
            }
        }
        // One schema for the file, idempotent across the second decode mode.
        assert_eq!(store.list().expect("list").len(), 1);
        source.close().await.expect("close");
    }

    // Without a store (a composition that manages no data directory) no
    // hash is attached; chain decoders then reject with a classified code.
    let cfg = serde_json::json!({ "paths": [bag.to_str().unwrap()], "decode": "ros2" });
    let (mut source, _) = open(cfg).await;
    let messages = collect_all(&mut source).await.expect("collect");
    assert!(
        messages
            .iter()
            .all(|m| m.attrs.get(attr::MCAP_SCHEMA_HASH).is_none())
    );
}

#[tokio::test(start_paused = true)]
async fn cancel_safety_no_loss_no_duplication_under_dropped_futures() {
    use thadam_testkit::{SplitMix64, poll_once, poll_up_to};

    // Every message sleeps a paced gap before staging, so a dropped future
    // always has something in flight to lose — if staging were broken.
    let dir = tempfile::tempdir().expect("tmpdir");
    let a = dir.path().join("a.mcap");
    let ms = 1_000_000u64;
    let messages: Vec<Msg> = (0..30).map(|i| Msg::joint(i * 10 * ms, i as u32)).collect();
    write_mcap(&a, &messages, true);

    let cfg = serde_json::json!({ "paths": [a.to_str().unwrap()], "pace": "realtime" });
    let (mut source, _) = open(cfg).await;

    let mut rng = SplitMix64::new(4242);
    let mut got: Vec<u64> = Vec::new();
    let mut dropped = 0u32;
    while (got.len() as u64) < 30 {
        let limits = BatchLimits {
            max_messages: rng.between(1, 9) as usize,
            max_bytes: 1 << 24,
            linger: Duration::from_secs(rng.between(1, 30)),
        };
        let fut = source.next_batch(limits);
        let outcome = match rng.below(3) {
            0 => poll_once(fut).await,
            1 => poll_up_to(fut, rng.between(1, 4) as u32).await,
            // Driven to completion: on the paused clock the runtime
            // auto-advances through each pacing sleep while idle.
            _ => Some(fut.await),
        };
        match outcome {
            // The future was dropped, mid-sleep, with staged messages inside
            // the run. The source must keep them.
            None => dropped += 1,
            Some(Ok(Some(batch))) => {
                got.extend(trace(&batch.batch.messages).into_iter().map(|(_, t)| t));
                batch.ack.complete(AckOutcome::Committed);
            }
            Some(Ok(None)) => break,
            Some(Err(e)) => panic!("next_batch failed: {e}"),
        }
    }
    assert!(dropped >= 5, "probe dropped only {dropped} futures");
    let want: Vec<u64> = (0..30).map(|i| i * 10 * ms).collect();
    assert_eq!(
        got, want,
        "messages lost or duplicated after {dropped} drops"
    );
}
