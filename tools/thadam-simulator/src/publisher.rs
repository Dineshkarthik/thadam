//! The publish pipeline: scheduled sends, PUBACK correlation, and the oracle
//! write-back path.
//!
//! ACK correlation relies on two `rumqttc` guarantees the S2 spike verified:
//! `Outgoing::Publish(pkid)` events fire in request order (the FIFO pairs
//! them with the publishes issued here, one at a time and in order), and
//! `Incoming::PubAck` carries the packet id. Retransmissions after a
//! reconnect repeat a packet id already seen, so they never consume a FIFO
//! entry.
//!
//! The poll loop runs in its own task and is never `select!`ed racing
//! anything: `EventLoop::poll` is not cancel-safe (ADR-0006 rule 1), so
//! nothing here ever drops an in-flight poll.

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::time::Instant;

use rumqttc::{AsyncClient, Event, EventLoop, Incoming, Outgoing, QoS};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::oracle::{OracleRow, OracleWriter};
use crate::stats::SimStats;
use crate::types::Stream;

/// One publish the simulator issued, waiting for its PUBACK handling.
#[derive(Debug)]
pub struct PubItem {
    /// The full MQTT topic.
    pub topic: String,
    /// The stream (stats only).
    pub stream: Stream,
    /// The exact payload bytes.
    pub bytes: Vec<u8>,
    /// Earliest send time (out-of-order and duplicate chaos push it out).
    pub send_at: Instant,
    /// This publish's oracle rows; appended once the publish resolves.
    pub rows: Vec<OracleRow>,
}

/// Events the poll task reports upward.
#[derive(Debug)]
pub enum LinkEvent {
    /// CONNACK received.
    Connected {
        /// MQTT session-present flag.
        session_present: bool,
    },
    /// A PUBLISH packet went out with this packet id.
    PublishSent(u16),
    /// A PUBACK came back for this packet id.
    PubAck(u16),
    /// The connection to the broker dropped; the next poll reconnects.
    ConnectionLost(String),
}

/// Drives the `rumqttc` event loop forever, reporting [`LinkEvent`]s. The
/// caller aborts the task at the very end of shutdown, after DISCONNECT.
pub async fn poll_eventloop(mut event_loop: EventLoop, tx: mpsc::UnboundedSender<LinkEvent>) {
    loop {
        match event_loop.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                let _ = tx.send(LinkEvent::Connected {
                    session_present: connack.session_present,
                });
            }
            Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                let _ = tx.send(LinkEvent::PubAck(ack.pkid));
            }
            Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                let _ = tx.send(LinkEvent::PublishSent(pkid));
            }
            Ok(_) => {}
            Err(error) => {
                if tx
                    .send(LinkEvent::ConnectionLost(error.to_string()))
                    .is_err()
                {
                    return;
                }
                // rumqttc reconnects on the next poll; spacing it here keeps a
                // dead broker from spinning the loop at full rate.
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// A heap entry: publish order breaks `send_at` ties deterministically.
struct Scheduled {
    send_at: Instant,
    order: u64,
    item: PubItem,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.send_at, self.order) == (other.send_at, other.order)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    // Min-heap on (send_at, order).
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (other.send_at, other.order).cmp(&(self.send_at, self.order))
    }
}

/// Owns the client, the send schedule, and the oracle correlation state.
pub struct Publisher {
    client: AsyncClient,
    rx: mpsc::Receiver<PubItem>,
    events: mpsc::UnboundedReceiver<LinkEvent>,
    oracle: OracleWriter,
    heap: BinaryHeap<Scheduled>,
    order: u64,
    sent_fifo: VecDeque<Vec<OracleRow>>,
    pending_ack: HashMap<u16, Vec<OracleRow>>,
    stats: SimStats,
    senders_open: bool,
    events_open: bool,
}

impl Publisher {
    /// Builds the publisher around an already-connected client handle.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: AsyncClient,
        rx: mpsc::Receiver<PubItem>,
        events: mpsc::UnboundedReceiver<LinkEvent>,
        oracle: OracleWriter,
    ) -> Publisher {
        Publisher {
            client,
            rx,
            events,
            oracle,
            heap: BinaryHeap::new(),
            order: 0,
            sent_fifo: VecDeque::new(),
            pending_ack: HashMap::new(),
            stats: SimStats::default(),
            senders_open: true,
            events_open: true,
        }
    }

    fn backlog(&self) -> usize {
        self.heap.len() + self.sent_fifo.len() + self.pending_ack.len()
    }

    /// Runs until every sender is dropped, every scheduled publish is out,
    /// and every PUBACK is back — or until `drain_deadline` fires, whichever
    /// first. Then flushes the rest of the oracle, disconnects, and returns
    /// the counters.
    pub async fn run(mut self, drain_deadline: CancellationToken) -> SimStats {
        loop {
            self.flush_due(Instant::now()).await;
            if self.drained() {
                break;
            }
            let next_send_at = self.heap.peek().map(|s| s.send_at);
            tokio::select! {
                () = drain_deadline.cancelled() => {
                    tracing::warn!(backlog = self.backlog(), "drain deadline hit; remaining publishes are oracle-marked unacked");
                    break;
                }
                item = self.rx.recv(), if self.senders_open => {
                    match item {
                        Some(item) => self.schedule(item),
                        None => self.senders_open = false,
                    }
                }
                event = self.events.recv(), if self.events_open => {
                    match event {
                        Some(event) => self.on_link_event(event),
                        None => self.events_open = false,
                    }
                }
                () = async {
                    let at = next_send_at.unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(3600));
                    tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
                }, if next_send_at.is_some() => {}
            }
        }
        self.finish().await
    }

    fn drained(&self) -> bool {
        !self.senders_open
            && self.heap.is_empty()
            && self.sent_fifo.is_empty()
            && self.pending_ack.is_empty()
    }

    fn schedule(&mut self, item: PubItem) {
        self.order += 1;
        self.heap.push(Scheduled {
            send_at: item.send_at,
            order: self.order,
            item,
        });
    }

    /// Publishes every scheduled item whose time has come. Once the senders
    /// are gone (draining), remaining items go out immediately: the delay
    /// chaos they carried only mattered for interleaving with live traffic.
    async fn flush_due(&mut self, now: Instant) {
        loop {
            let due = !self.senders_open || self.heap.peek().is_some_and(|s| s.send_at <= now);
            if !due {
                return;
            }
            let Some(scheduled) = self.heap.pop() else {
                return;
            };
            self.publish_now(scheduled.item).await;
        }
    }

    async fn publish_now(&mut self, item: PubItem) {
        let PubItem {
            topic,
            stream,
            bytes,
            rows,
            ..
        } = item;
        let nbytes = bytes.len();
        let result = self
            .client
            .publish(topic.clone(), QoS::AtLeastOnce, false, bytes)
            .await;
        match result {
            Ok(()) => {
                self.stats.published(stream, nbytes);
                self.sent_fifo.push_back(rows);
            }
            Err(error) => {
                self.stats.publish_errors += 1;
                tracing::warn!(topic = %topic, %error, "publish rejected by client");
                self.oracle.write(rows);
            }
        }
    }

    fn on_link_event(&mut self, event: LinkEvent) {
        match event {
            LinkEvent::Connected { session_present } => {
                self.stats.connects += 1;
                tracing::info!(session_present, "connected to broker");
            }
            LinkEvent::ConnectionLost(error) => {
                self.stats.disconnects += 1;
                tracing::warn!(%error, "broker connection lost; reconnecting");
            }
            LinkEvent::PublishSent(pkid) => {
                if self.pending_ack.contains_key(&pkid) {
                    // Retransmission after a reconnect: the FIFO entry was
                    // already paired on the first send.
                    return;
                }
                match self.sent_fifo.pop_front() {
                    Some(rows) => {
                        self.pending_ack.insert(pkid, rows);
                    }
                    None => {
                        self.stats.ack_mismatches += 1;
                        tracing::warn!(pkid, "publish sent with no pending oracle rows");
                    }
                }
            }
            LinkEvent::PubAck(pkid) => match self.pending_ack.remove(&pkid) {
                Some(mut rows) => {
                    self.stats.acked_messages += 1;
                    for row in &mut rows {
                        row.acked(true);
                    }
                    self.oracle.write(rows);
                }
                None => {
                    self.stats.ack_mismatches += 1;
                    tracing::warn!(pkid, "puback for an unknown packet id");
                }
            },
        }
    }

    /// End-of-run path: everything still unacknowledged is oracle-recorded
    /// as unacked, then the client DISCONNECTs and the writer fsyncs.
    async fn finish(mut self) -> SimStats {
        let mut lost = 0u64;
        if !self.sent_fifo.is_empty() || !self.pending_ack.is_empty() {
            tracing::warn!(
                sent = self.sent_fifo.len(),
                pending = self.pending_ack.len(),
                "publishes without PUBACK at shutdown"
            );
        }
        while let Some(rows) = self.sent_fifo.pop_front() {
            lost += rows.len() as u64;
            self.oracle.write(rows);
        }
        for (_, rows) in self.pending_ack.drain() {
            lost += rows.len() as u64;
            self.oracle.write(rows);
        }
        while let Some(scheduled) = self.heap.pop() {
            lost += scheduled.item.rows.len() as u64;
            self.oracle.write(scheduled.item.rows);
        }
        if lost > 0 {
            tracing::warn!(lost, "oracle rows recorded publish_acked=false");
        }
        let _ = self.client.disconnect().await;
        // Give the DISCONNECT packet a beat to leave before the poll task is
        // aborted by the runner.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        match self.oracle.close() {
            Ok(rows) => self.stats.oracle_rows = rows,
            Err(error) => tracing::error!(%error, "oracle writer failed"),
        }
        self.stats
    }
}
