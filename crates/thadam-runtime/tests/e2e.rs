//! End-to-end runs with the testkit memory connectors.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use bytes::Bytes;
use common::*;
use thadam_runtime::{ComponentState, ExitStatus, Pipeline, Registry, read_thadam_json};
use thadam_testkit::{MemoryBatchSinkConfig, TempDataDir};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivers_every_message_once_with_ordered_acks() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let up = kit.sources.scripted("src", (1..=500).map(json_n));
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: recseg, config: { frames_per_file: 3 } }",
        "",
    );
    let report = run_to_end(&kit, spec(&y), fast_opts()).await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    let out = kit.rec.store("out");
    assert_eq!(out.ns(), (1..=500).collect::<Vec<_>>());
    let wal_end = report.status.wal.as_ref().unwrap().durable_end;
    assert_contiguous(&out.ranges(), wal_end);
    assert_eq!(
        out.eligible(),
        out.ranges(),
        "every finalized file is ship-eligible"
    );
    assert_eq!(cursor(&report.status, "out"), wal_end);

    assert_eq!(up.acked_seqs(), (1..=500).collect::<Vec<_>>());
    let stats = up.stats();
    assert_eq!(stats.out_of_order_acks, 0);
    assert_eq!(stats.lost_acks, 0);
    assert_eq!(stats.outstanding_handles, 0);

    let src = component(&report.status, "source:src");
    assert_eq!(src.counters.messages, 500);
    assert_eq!(src.state, ComponentState::Stopped);
    assert!(report.recovery.wal_created);

    let meta = read_thadam_json(dir.path()).unwrap().unwrap();
    assert_eq!(meta.pipeline, "test");
    assert_eq!(meta.config_hash, report.status.config_hash);
    assert!(meta.wal.epoch.is_some());
    assert!(dir.join("wal/wal.json").exists());
    assert!(dir.join("wal/clean_shutdown").exists());
}

/// Collects the formatted log output a test subscriber writes.
#[derive(Clone, Default)]
struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;

    fn make_writer(&'a self) -> LogBuffer {
        self.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_record_names_the_node_build_and_configuration() {
    let logs = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=3).map(json_n));
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: recseg, config: { frames_per_file: 3 } }",
        "",
    );
    let expected_hash = spec(&y).config_hash_hex();
    let report = run_to_end(&kit, spec(&y), fast_opts()).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    let text = String::from_utf8(logs.0.lock().unwrap_or_else(|e| e.into_inner()).clone()).unwrap();
    let line = text
        .lines()
        .find(|l| l.contains("pipeline starting") && l.contains(&expected_hash))
        .unwrap_or_else(|| panic!("no startup record in:\n{text}"));
    let record: serde_json::Value = serde_json::from_str(line).unwrap();
    let fields = &record["fields"];
    assert_eq!(fields["pipeline"], "test", "{line}");
    assert_eq!(fields["node_id"], "node-1", "{line}");
    assert_eq!(fields["config_hash"], expected_hash, "{line}");
    assert_eq!(fields["config_hash_version"], 1, "{line}");
    assert_eq!(fields["previous_config_hash"], "", "{line}");
    assert_eq!(
        fields["thadam_version"],
        env!("CARGO_PKG_VERSION"),
        "{line}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_resumes_after_the_cursor_without_duplicates() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: recseg, config: { frames_per_file: 5 } }",
        "",
    );
    kit.sources.scripted("src", (1..=120).map(json_n));
    let first = run_to_end(&kit, spec(&y), fast_opts()).await;
    assert_eq!(first.exit, ExitStatus::Ok);

    kit.sources.scripted("src", (121..=240).map(json_n));
    let second = run_to_end(&kit, spec(&y), fast_opts()).await;
    assert_eq!(second.exit, ExitStatus::Ok);
    assert!(second.recovery.clean_shutdown);
    assert!(!second.recovery.wal_created);

    let out = kit.rec.store("out");
    assert_eq!(out.ns(), (1..=240).collect::<Vec<_>>());
    assert_contiguous(&out.ranges(), second.status.wal.unwrap().durable_end);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_are_quarantined_and_the_cursor_passes_them() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let mut payloads: Vec<Bytes> = (1..=50).map(json_n).collect();
    payloads.insert(10, Bytes::from_static(b"not json"));
    payloads.insert(30, Bytes::from_static(b"{\"x\": 1}"));
    kit.sources.scripted("src", payloads);
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: recseg, config: { frames_per_file: 2 } }",
        "",
    );
    let report = run_to_end(&kit, spec(&y), fast_opts()).await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(kit.rec.store("out").ns(), (1..=50).collect::<Vec<_>>());
    let q = kit.quarantine_rows("out");
    assert_eq!(q.len(), 2, "{q:#?}");
    assert!(q.iter().any(|r| r.values.contains("decode.")), "{q:#?}");
    assert!(
        q.iter().all(|r| r.values.contains("sink_id=Str(\"out\")")),
        "{q:#?}"
    );
    let sink = component(&report.status, "sink:out");
    assert_eq!(sink.counters.quarantined, 2);
    assert_eq!(
        cursor(&report.status, "out"),
        report.status.wal.unwrap().durable_end
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_sink_receives_every_event_once() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=300).map(json_n));
    let h = kit.batch.memory("ch", MemoryBatchSinkConfig::default());
    let y = yaml(dir.path(), "  - { id: ch, kind: membatch }", "");
    let mut o = fast_opts();
    o.batch_max_events = 64;
    let report = run_to_end(&kit, spec(&y), o).await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(h.applied_events(), 300);
    let ids = h.applied_batches();
    for w in ids.windows(2) {
        assert_eq!(
            w[0].0.range().last().get() + 1,
            w[1].0.range().first().get(),
            "{ids:?}"
        );
    }
    assert!(h.is_closed());
    let sink = component(&report.status, "sink:ch");
    assert_eq!(sink.delivery, Some("idempotent_batch"));
    let c = report
        .status
        .cursors
        .iter()
        .find(|c| c.sink == "ch")
        .unwrap();
    assert_eq!(c.inflight, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_dataset_lists_and_per_sink_transforms_shape_each_chain() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=40).map(json_n));
    let sinks = "  - { id: all, kind: recseg, config: { frames_per_file: 2 } }\n  \
                 - { id: odd, kind: recseg, config: { frames_per_file: 2, datasets: [d] } }";
    let transforms = "transforms:\n  - { kind: filter, dataset: d, sinks: [odd], where: { field: n, in: [1, 3, 5, 7, 9] } }";
    let report = run_to_end(
        &kit,
        spec(&yaml(dir.path(), sinks, transforms)),
        fast_opts(),
    )
    .await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(kit.rec.store("all").ns().len(), 40);
    assert_eq!(kit.rec.store("odd").ns(), vec![1, 3, 5, 7, 9]);
    let end = report.status.wal.as_ref().unwrap().durable_end;
    assert_eq!(
        cursor(&report.status, "odd"),
        end,
        "filtered frames still advance the cursor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sample_transform_keeps_the_first_and_every_nth_event_end_to_end() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=60).map(json_n));
    let sinks = "  - { id: out, kind: recseg, config: { frames_per_file: 2 } }";
    let transforms = "transforms:\n  - { kind: sample, dataset: d, every_n: 2 }";
    let report = run_to_end(
        &kit,
        spec(&yaml(dir.path(), sinks, transforms)),
        fast_opts(),
    )
    .await;

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    let out = kit.rec.store("out");
    assert_eq!(
        out.ns(),
        (1..=60).step_by(2).collect::<Vec<_>>(),
        "the engine keeps an asset's first event and every 2nd after it"
    );
    let end = report.status.wal.as_ref().unwrap().durable_end;
    assert_eq!(
        cursor(&report.status, "out"),
        end,
        "sampled-out frames still advance the cursor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_sinks_receive_events_grouped_by_dataset() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=60).map(json_n));
    let two_datasets = "  - id: e\n    match: { source: src }\n    decode: json\n    fields:\n      - { id: 1000, name: m, type: u64, from: /n, required: true }\n";
    let sinks = "  - { id: out, kind: recseg, config: { frames_per_file: 3 } }\n  \
                 - { id: only_e, kind: recseg, config: { frames_per_file: 3, datasets: [e] } }";
    let y = yaml(dir.path(), sinks, "").replace("\nsinks:\n", &format!("{two_datasets}sinks:\n"));
    let report = run_to_end(&kit, spec(&y), fast_opts()).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    let out = kit.rec.store("out");
    assert_eq!(
        out.dataset_events(),
        [("d".to_owned(), 60), ("e".to_owned(), 60)].into(),
        "each dataset keeps its own events"
    );
    assert!(
        out.dataset_lists()
            .iter()
            .all(|l| l == &["d".to_owned(), "e".to_owned()]),
        "every append lists every dataset in declaration order: {:?}",
        out.dataset_lists()
    );
    let only = kit.rec.store("only_e");
    assert_eq!(only.dataset_events(), [("e".to_owned(), 60)].into());
    assert!(only.dataset_lists().iter().all(|l| l == &["e".to_owned()]));
}

#[tokio::test]
async fn build_reports_every_config_problem_with_exit_code_two() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = format!(
        "{}  - {{ id: x, kind: nosuchkind }}\n  - {{ id: y, kind: recseg, config: {{ frames_per_file: nope }} }}\n",
        yaml(dir.path(), "  - { id: out, kind: recseg }", "").replace(
            "{ id: src, kind: memory }",
            "{ id: src, kind: memory, config: { bad: 1 } }"
        )
    );
    let errs = Pipeline::build(spec(&y), &kit.registry(), fast_opts()).unwrap_err();
    assert_eq!(errs.len(), 3, "{errs:#?}");
    assert!(
        errs.iter()
            .any(|e| e.code == thadam_core::codes::CONFIG_UNKNOWN_KIND)
    );
    assert!(
        errs.iter()
            .any(|e| e.context.get("component") == Some("sources[0]"))
    );
    assert_eq!(ExitStatus::for_errors(&errs), ExitStatus::InvalidConfig);

    let mut r = Registry::new();
    r.segment_sink(Box::new(RecFactory {
        kind: "dup",
        stores: RecStores::default(),
    }))
    .service(Box::new(LogServiceFactory(EventLog::default())));
    struct DupService;
    impl thadam_core::ServiceFactory for DupService {
        fn kind(&self) -> &'static str {
            "dup"
        }
        fn config_schema(&self) -> serde_json::Value {
            serde_json::json!({})
        }
        fn validate(&self, _: &serde_json::Value) -> Result<(), Vec<thadam_core::ThadamError>> {
            Ok(())
        }
        fn build(
            &self,
            _: serde_json::Value,
            _: &thadam_core::BuildContext,
        ) -> Result<Box<dyn thadam_core::Service>, thadam_core::ThadamError> {
            unreachable!()
        }
    }
    r.service(Box::new(DupService));
    let y = yaml(dir.path(), "  - { id: out, kind: dup }", "");
    let errs = Pipeline::build(spec(&y), &r, fast_opts()).unwrap_err();
    assert!(
        errs.iter().any(|e| e.message.contains("more than one")),
        "{errs:#?}"
    );
}
