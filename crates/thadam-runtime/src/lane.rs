//! The bounded channel from a chain task to its sink driver.

use std::sync::Arc;

use thadam_core::WalRange;
use thadam_transform::{DatasetBatch, RejectedRaw};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::status::{Component, add};

/// One chain read: per-dataset events over `origin`, plus the records the
/// chain rejected (empty unless the chain is primary).
pub(crate) struct ChainBatch {
    pub origin: WalRange,
    pub clock_synced: bool,
    pub datasets: Vec<DatasetBatch>,
    pub rejects: Vec<RejectedRaw>,
    /// The rewind generation the batch was read in; the driver drops batches
    /// from older generations.
    pub generation: u64,
}

/// Where the driver asks the chain to restart reading, after a sink lost its
/// buffered state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rewind {
    pub generation: u64,
    pub from: u64,
}

/// A received batch. Its bytes stay reserved until it is dropped, which the
/// driver does only once the sink has taken the batch.
pub(crate) struct Envelope {
    pub item: ChainBatch,
    bytes: u64,
    _permit: OwnedSemaphorePermit,
    gauge: Arc<Component>,
}

impl ChainBatch {
    pub fn empty(origin: WalRange) -> Self {
        ChainBatch {
            origin,
            clock_synced: true,
            datasets: Vec::new(),
            rejects: Vec::new(),
            generation: 0,
        }
    }
}

impl Envelope {
    /// Bytes reserved for this batch.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Takes the batch out; the bytes stay reserved until the envelope drops.
    pub fn take(&mut self) -> ChainBatch {
        let origin = self.item.origin;
        std::mem::replace(&mut self.item, ChainBatch::empty(origin))
    }
}

impl Drop for Envelope {
    fn drop(&mut self) {
        self.gauge
            .counters
            .queued_bytes
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(crate) struct LaneSender {
    tx: mpsc::Sender<Envelope>,
    bytes: Arc<Semaphore>,
    cap: u32,
    gauge: Arc<Component>,
}

pub(crate) type LaneReceiver = mpsc::Receiver<Envelope>;

/// A channel of at most `batches` items and `max_bytes` bytes. A batch larger
/// than `max_bytes` takes the whole budget instead of deadlocking.
pub(crate) fn lane(
    batches: usize,
    max_bytes: usize,
    gauge: Arc<Component>,
) -> (LaneSender, LaneReceiver) {
    let (tx, rx) = mpsc::channel(batches.max(1));
    let cap = u32::try_from(max_bytes.max(1)).unwrap_or(u32::MAX);
    (
        LaneSender {
            tx,
            bytes: Arc::new(Semaphore::new(cap as usize)),
            cap,
            gauge,
        },
        rx,
    )
}

impl LaneSender {
    /// Waits for room, then queues `item`. Cancel-safe: if dropped before it
    /// returns, nothing is queued. `Err` means the driver is gone.
    pub async fn send(&self, item: ChainBatch, bytes: usize) -> Result<(), ()> {
        let want = u32::try_from(bytes.max(1))
            .unwrap_or(u32::MAX)
            .min(self.cap);
        let permit = Arc::clone(&self.bytes)
            .acquire_many_owned(want)
            .await
            .map_err(|_| ())?;
        let reserved = u64::from(want);
        add(&self.gauge.counters.queued_bytes, reserved);
        let env = Envelope {
            item,
            bytes: reserved,
            _permit: permit,
            gauge: Arc::clone(&self.gauge),
        };
        self.tx.send(env).await.map_err(|_| ())
    }
}

pub(crate) fn rewind_channel() -> (watch::Sender<Rewind>, watch::Receiver<Rewind>) {
    watch::channel(Rewind {
        generation: 0,
        from: 0,
    })
}
