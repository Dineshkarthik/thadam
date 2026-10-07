//! A reference ordered acknowledgement queue.
//!
//! The runtime's `SourceDriver` keeps a bounded FIFO of (commit ticket, ack
//! handle) pairs and completes handles strictly in receive order, whatever
//! order the WAL commits finish in. This is the minimal version of that rule,
//! used by the conformance suites and by benchmarks of ack bookkeeping.

use std::collections::VecDeque;

use thadam_core::{AckHandle, AckOutcome, AckStatus};

/// Identifies a pushed handle so its commit can be reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ticket(u64);

struct Entry {
    ticket: Ticket,
    committed: bool,
    handle: AckHandle,
}

/// Completes ack handles in receive order once their commits are done.
#[derive(Default)]
pub struct AckFifo {
    entries: VecDeque<Entry>,
    next: u64,
}

impl AckFifo {
    /// An empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a handle in receive order. The commit is still pending.
    pub fn push(&mut self, handle: AckHandle) -> Ticket {
        let ticket = Ticket(self.next);
        self.next += 1;
        self.entries.push_back(Entry {
            ticket,
            committed: false,
            handle,
        });
        ticket
    }

    /// Records that the commit for `ticket` finished. Nothing is acknowledged
    /// until [`AckFifo::release_ready`].
    pub fn commit(&mut self, ticket: Ticket) {
        if let Some(e) = self.entries.iter_mut().find(|e| e.ticket == ticket) {
            e.committed = true;
        }
    }

    /// Completes the longest prefix of committed handles, in order, and
    /// returns each completion's status.
    pub fn release_ready(&mut self) -> Vec<AckStatus> {
        let mut statuses = Vec::new();
        while self.entries.front().is_some_and(|e| e.committed) {
            if let Some(e) = self.entries.pop_front() {
                statuses.push(e.handle.complete(AckOutcome::Committed));
            }
        }
        statuses
    }

    /// Handles still queued.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is queued.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_complete_only_as_a_committed_prefix() {
        let mut fifo = AckFifo::new();
        let tickets: Vec<_> = (0..3).map(|_| fifo.push(AckHandle::noop())).collect();
        fifo.commit(tickets[2]);
        assert!(fifo.release_ready().is_empty(), "the head is not committed");
        fifo.commit(tickets[0]);
        assert_eq!(fifo.release_ready().len(), 1);
        assert_eq!(fifo.len(), 2);
        fifo.commit(tickets[1]);
        assert_eq!(fifo.release_ready().len(), 2);
        assert!(fifo.is_empty());
    }
}
