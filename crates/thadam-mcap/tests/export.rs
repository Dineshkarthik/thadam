//! Integration tests for the MCAP writer: messages shaped like the MCAP source
//! emits are exported to a file, then read back with the `mcap` crate and
//! through `McapSource`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{
    JOINT_STATE_SCHEMA_NAME, JOINT_STATE_TOPIC, JSON_TOPIC, joint_state_payload, joint_state_schema,
};
use thadam_core::{
    AckOutcome, BatchLimits, RawMessage, Shutdown, Source, SourceContext, SourceId, SystemClock,
    Timestamp,
};
use thadam_mcap::{
    DefaultEncoding, ExportOptions, GapKind, McapConfig, McapExporter, McapSource, Provenance,
    attr, decode_ros2,
};

const T0: u64 = 1_758_000_000_000_000_042;

fn joint_state_resolver(name: &str, encoding: &str) -> Option<Bytes> {
    (name == JOINT_STATE_SCHEMA_NAME && encoding == "ros2msg")
        .then(|| Bytes::from(joint_state_schema()))
}

fn raw(ingest: u64, attrs: &[(&str, &str)], payload: Vec<u8>) -> RawMessage {
    RawMessage {
        ingest_time: Timestamp::from_nanos(ingest as i64),
        attrs: attrs.iter().copied().collect(),
        payload: Bytes::from(payload),
    }
}

fn provenance() -> Provenance {
    Provenance {
        thadam_version: "0.0.0-test".into(),
        wal_epoch: 3,
        first_pos: 1,
        last_pos: 4,
        node_id: "edge-1".into(),
    }
}

fn metadata_records(path: &Path, name: &str) -> Vec<BTreeMap<String, String>> {
    let bytes = std::fs::read(path).expect("read export");
    let summary = mcap::read::Summary::read(&bytes)
        .expect("summary")
        .expect("present");
    summary
        .metadata_indexes
        .iter()
        .filter(|index| index.name == name)
        .map(|index| {
            mcap::read::metadata(&bytes, index)
                .expect("metadata")
                .metadata
        })
        .collect()
}

async fn replay(config: serde_json::Value) -> Vec<RawMessage> {
    let config = McapConfig::from_value(&config).expect("config");
    let mut source = McapSource::new(config);
    let ctx = SourceContext::new(
        SourceId::new("replay"),
        Shutdown::new(),
        Arc::new(SystemClock::new()),
    );
    source.open(&ctx).await.expect("open");
    let limits = BatchLimits {
        max_messages: 8,
        max_bytes: 1 << 24,
        linger: Duration::from_millis(50),
    };
    let mut out = Vec::new();
    while let Some(batch) = source.next_batch(limits).await.expect("batch") {
        out.extend(batch.batch.messages);
        batch.ack.complete(AckOutcome::Committed);
    }
    source.close().await.expect("close");
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exported_file_round_trips_through_the_mcap_source() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().join("export.mcap");

    let joint_attrs = [
        (attr::MCAP_TOPIC, JOINT_STATE_TOPIC),
        (attr::MCAP_MESSAGE_ENCODING, "cdr"),
        (attr::MCAP_SCHEMA_NAME, JOINT_STATE_SCHEMA_NAME),
        (attr::MCAP_SCHEMA_ENCODING, "ros2msg"),
    ];
    let mut first_attrs = joint_attrs.to_vec();
    first_attrs.push((attr::MCAP_SEQUENCE, "41"));
    first_attrs.push((attr::MCAP_PUBLISH_TIME, "1758000000000000049"));

    let messages = vec![
        ("robot", raw(T0, &first_attrs, joint_state_payload())),
        ("robot", raw(T0 + 10, &joint_attrs, joint_state_payload())),
        (
            "annotations",
            raw(
                T0 + 20,
                &[(attr::TOPIC, JSON_TOPIC)],
                br#"{"note":"n1"}"#.to_vec(),
            ),
        ),
        ("bin", raw(T0 + 30, &[], vec![0, 1, 2, 255])),
    ];

    let options = ExportOptions::new(DefaultEncoding::OctetStream, joint_state_resolver)
        .with_source_encoding("annotations", DefaultEncoding::Json);
    let file = File::create(&path).expect("create");
    let mut exporter = McapExporter::new(file, options).expect("exporter");
    for (source, msg) in &messages {
        exporter.write_raw(source, msg).expect("write");
    }
    exporter.add_gap(GapKind::Evicted, 3, 1, 4).expect("gap");
    let report = exporter.finish(provenance()).expect("finish");
    assert_eq!(report.messages, 4);
    assert_eq!(report.channels, 3);
    assert_eq!(report.gaps, 1);
    assert!(report.unresolved_schemas.is_empty());

    let bytes = std::fs::read(&path).expect("read export");
    let summary = mcap::read::Summary::read(&bytes)
        .expect("summary")
        .expect("present");
    let mut channel_sources: Vec<(String, String)> = summary
        .channels
        .values()
        .map(|c| (c.topic.clone(), c.metadata["thadam.source"].clone()))
        .collect();
    channel_sources.sort();
    assert_eq!(
        channel_sources,
        [
            ("/annotations".to_owned(), "annotations".to_owned()),
            ("/joint_states".to_owned(), "robot".to_owned()),
            ("bin".to_owned(), "bin".to_owned()),
        ]
    );

    let gaps = metadata_records(&path, "thadam.gap");
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["kind"], "evicted");
    assert_eq!(gaps[0]["epoch"], "3");
    assert_eq!(gaps[0]["first"], "1");
    assert_eq!(gaps[0]["last"], "4");

    let prov = metadata_records(&path, "thadam.provenance");
    assert_eq!(prov.len(), 1);
    assert_eq!(prov[0]["node_id"], "edge-1");
    assert_eq!(prov[0]["first_pos"], "1");
    assert_eq!(prov[0]["last_pos"], "4");

    let read = replay(serde_json::json!({
        "paths": [path.to_str().unwrap()],
        "decode": "ros2",
    }))
    .await;
    assert_eq!(read.len(), 4);

    let joint = &read[0];
    assert_eq!(joint.ingest_time, Timestamp::from_nanos(T0 as i64));
    assert_eq!(joint.attrs.get(attr::MCAP_TOPIC), Some(JOINT_STATE_TOPIC));
    assert_eq!(
        joint.attrs.get(attr::MCAP_SCHEMA_NAME),
        Some(JOINT_STATE_SCHEMA_NAME)
    );
    assert_eq!(joint.attrs.get(attr::MCAP_SCHEMA_ENCODING), Some("ros2msg"));
    assert_eq!(joint.attrs.get(attr::MCAP_MESSAGE_ENCODING), Some("cdr"));
    assert_eq!(joint.attrs.get(attr::MCAP_SEQUENCE), Some("41"));
    assert_eq!(
        joint.attrs.get(attr::MCAP_LOG_TIME),
        Some("1758000000000000042")
    );
    assert_eq!(
        joint.attrs.get(attr::MCAP_PUBLISH_TIME),
        Some("1758000000000000049")
    );
    assert!(joint.attrs.get(attr::MCAP_ROS2_FIELDS).is_some());
    assert_eq!(joint.payload.as_ref(), joint_state_payload().as_slice());
    let decoded = decode_ros2(
        JOINT_STATE_SCHEMA_NAME,
        &joint_state_schema(),
        &joint.payload,
    )
    .expect("ros2 decode");
    assert_eq!(
        decoded.get("header.frame_id").and_then(|v| v.as_str()),
        Some("arm")
    );

    let second = &read[1];
    assert_eq!(second.attrs.get(attr::MCAP_SEQUENCE), Some("1"));
    assert_eq!(
        second.attrs.get(attr::MCAP_PUBLISH_TIME),
        Some("1758000000000000052")
    );

    let json = &read[2];
    assert_eq!(json.attrs.get(attr::MCAP_TOPIC), Some(JSON_TOPIC));
    assert_eq!(json.attrs.get(attr::MCAP_MESSAGE_ENCODING), Some("json"));
    assert_eq!(json.attrs.get(attr::MCAP_SCHEMA_NAME), Some(""));
    assert_eq!(json.payload.as_ref(), br#"{"note":"n1"}"#);

    let bin = &read[3];
    assert_eq!(bin.attrs.get(attr::MCAP_TOPIC), Some("bin"));
    assert_eq!(
        bin.attrs.get(attr::MCAP_MESSAGE_ENCODING),
        Some("application/octet-stream")
    );
    assert_eq!(bin.payload.as_ref(), &[0, 1, 2, 255]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_schema_is_written_without_a_schema_and_reported() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().join("unresolved.mcap");

    let options = ExportOptions::new(DefaultEncoding::OctetStream, joint_state_resolver);
    let file = File::create(&path).expect("create");
    let mut exporter = McapExporter::new(file, options).expect("exporter");
    exporter
        .write_raw(
            "robot",
            &raw(
                T0,
                &[
                    (attr::MCAP_TOPIC, "/mystery"),
                    (attr::MCAP_MESSAGE_ENCODING, "cdr"),
                    (attr::MCAP_SCHEMA_NAME, "example/Mystery"),
                    (attr::MCAP_SCHEMA_ENCODING, "ros2msg"),
                ],
                vec![1, 2, 3],
            ),
        )
        .expect("write");
    let report = exporter.finish(provenance()).expect("finish");

    assert_eq!(report.unresolved_schemas.len(), 1);
    assert_eq!(report.unresolved_schemas[0].topic, "/mystery");
    assert_eq!(report.unresolved_schemas[0].schema_name, "example/Mystery");

    let records = metadata_records(&path, "thadam.unresolved_schemas");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["channel.0.topic"], "/mystery");
    assert_eq!(records[0]["channel.0.schema"], "example/Mystery");
    assert_eq!(records[0]["channel.0.source"], "robot");

    let read = replay(serde_json::json!({ "paths": [path.to_str().unwrap()] })).await;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].attrs.get(attr::MCAP_TOPIC), Some("/mystery"));
    assert_eq!(read[0].attrs.get(attr::MCAP_SCHEMA_NAME), Some(""));
    assert_eq!(read[0].payload.as_ref(), &[1, 2, 3]);
}
