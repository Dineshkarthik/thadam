//! The `thadam-testkit` batch-sink conformance suite against a real broker.
//!
//! `#[ignore]`d (needs Docker). Run with
//!
//! ```text
//! cargo nextest run -p thadam-kafka --run-ignored only -E 'binary(conformance)'
//! ```
//!
//! The suite's checks need a fresh target per sink, so the fixture hands out
//! one pre-created single-partition topic per case. The probe reads applied
//! events from the broker; attempt ids come from a wrapper that records each
//! `write`. The suite's fault injection needs a test double, which a real
//! broker is not, so those checks are skipped here; the docker_kafka tests
//! cover the transient, unknown-outcome, and fatal paths against the broker.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{Broker, Opts, create_topic};
use thadam_core::{
    BatchId, BatchSink, BatchSinkFactory, BuildContext, DatasetDecl, ThadamError, SinkBatch,
    SinkCapabilities, SinkContext, SystemClock, WriteOutcome,
};
use thadam_kafka::KafkaSinkFactory;
use thadam_testkit::conformance::batch_sink::{
    BatchSinkCase, BatchSinkFixture, BatchSinkProbe, InjectedFault, run_all,
};
use rskafka::client::Client;
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use tokio::runtime::Handle;

/// Pre-created topics, one per conformance case (the suite opens a fresh
/// sink for each check and counts what its topic holds).
const CASES: usize = 8;

struct KafkaFixture {
    addr: String,
    /// (topic, a partition client the probe reads offsets through).
    pool: Mutex<Vec<(String, PartitionClient)>>,
}

struct KafkaProbe {
    partition: PartitionClient,
    attempts: Arc<Mutex<Vec<BatchId>>>,
}

impl BatchSinkProbe for KafkaProbe {
    fn applied_events(&self) -> usize {
        // A single-partition topic: its high watermark counts every record
        // the broker stored, duplicates included.
        let latest = tokio::task::block_in_place(|| {
            Handle::current().block_on(self.partition.get_offset(OffsetAt::Latest))
        });
        usize::try_from(latest.expect("latest offset")).expect("non-negative offset")
    }

    fn attempt_ids(&self) -> Vec<BatchId> {
        self.attempts.lock().unwrap().clone()
    }

    fn inject(&self, _fault: InjectedFault) -> bool {
        false
    }
}

/// Records the id of every write attempt, then delegates.
struct Recording {
    inner: Box<dyn BatchSink>,
    attempts: Arc<Mutex<Vec<BatchId>>>,
}

#[async_trait]
impl BatchSink for Recording {
    async fn open(&mut self, ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        self.inner.open(ctx).await
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        self.attempts.lock().unwrap().push(batch.id());
        self.inner.write(batch).await
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.inner.close().await
    }
}

impl BatchSinkFixture for KafkaFixture {
    fn create(&self) -> BatchSinkCase {
        let (topic, partition) = self
            .pool
            .lock()
            .unwrap()
            .pop()
            .expect("more sinks than pre-created topics");
        let cfg = serde_json::json!({
            "bootstrap": [self.addr],
            "topic": topic,
            "request_timeout": "5s",
        });
        let ctx = BuildContext::new(
            "kafka-conformance",
            Arc::new(SystemClock::new()),
            PathBuf::from("/tmp/thadam-kafka-conformance"),
        )
        .with_datasets(Arc::from([suite_dataset()]));
        let sink = KafkaSinkFactory::new()
            .build(cfg, &ctx)
            .expect("build sink");
        let attempts = Arc::new(Mutex::new(Vec::new()));
        BatchSinkCase {
            sink: Box::new(Recording {
                inner: sink,
                attempts: Arc::clone(&attempts),
            }),
            probe: Box::new(KafkaProbe {
                partition,
                attempts,
            }),
        }
    }
}

/// The suite's events carry the `thadam-testkit` test dataset and no identity
/// levels; the literal topic needs none.
fn suite_dataset() -> DatasetDecl {
    DatasetDecl::new(thadam_testkit::TEST_DATASET, Vec::new(), Vec::new(), false)
}

async fn topic_pool(client: &Client) -> Vec<(String, PartitionClient)> {
    let mut pool = Vec::with_capacity(CASES);
    for i in 0..CASES {
        let name = format!("conformance-{i}");
        create_topic(client, &name, 1).await;
        let partition = client
            .partition_client(&name, 0, UnknownTopicHandling::Error)
            .await
            .expect("partition client");
        pool.push((name, partition));
    }
    pool
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (apache/kafka)"]
async fn kafka_passes_the_batch_sink_conformance_suite() {
    let broker = Broker::start(Opts::new("conformance")).await;
    let client = broker.client().await;
    let pool = topic_pool(&client).await;
    let fixture = KafkaFixture {
        addr: broker.addr.clone(),
        pool: Mutex::new(pool),
    };
    run_all(&fixture).await;
}
