//! The ack task: turns acknowledgement requests into client calls, in order.
//!
//! The client queues the request and the poll task writes it to the socket
//! (acknowledgements flush only while `poll` runs). This task may wait for
//! room in the client's request channel because the poll task never blocks,
//! so it always drains; the wait is sliced so the epoch is re-checked and an
//! acknowledgement that went stale while waiting is dropped instead of being
//! sent on a newer connection.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::link::ClientHandle;
use crate::state::{AckCmd, Shared};

const SLICE: Duration = Duration::from_millis(5);

pub(crate) async fn run(
    mut commands: mpsc::UnboundedReceiver<AckCmd>,
    client: ClientHandle,
    shared: Arc<Shared>,
    topics: Vec<(String, u8)>,
) {
    while let Some(command) = commands.recv().await {
        match command {
            AckCmd::Ack { epoch, pkid, qos } => loop {
                if shared.gate.current() != epoch {
                    shared.stats.acks_dropped.fetch_add(1, Ordering::Relaxed);
                    shared.record_stale_ack();
                    break;
                }
                match tokio::time::timeout(SLICE, client.ack(pkid, qos)).await {
                    Ok(Ok(())) => {
                        shared.stats.acks_sent.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Ok(Err(_closed)) => {
                        shared.stats.acks_dropped.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_elapsed) => {}
                }
            },
            AckCmd::Subscribe => loop {
                match tokio::time::timeout(SLICE, client.subscribe(&topics)).await {
                    Ok(_) => break,
                    Err(_elapsed) => {}
                }
            },
        }
    }
}
