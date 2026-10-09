//! Live run counters, printed at shutdown.

use serde::Serialize;

use crate::types::Stream;

/// Per-stream message counters.
#[derive(Clone, Debug, Default, Serialize)]
pub struct StreamStats {
    /// Messages accepted by the client library.
    pub messages: u64,
    /// Payload bytes accepted by the client library.
    pub bytes: u64,
}

/// All counters a run collects. `oracle_rows` is filled in at the end.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SimStats {
    /// Messages handed to the MQTT client (excludes nothing; duplicates are
    /// separate messages).
    pub messages_published: u64,
    /// Payload bytes handed to the MQTT client.
    pub bytes_published: u64,
    /// Messages the broker PUBACKed.
    pub acked_messages: u64,
    /// Messages the client rejected (its request channel was closed).
    pub publish_errors: u64,
    /// ACK orderings that did not match the publish FIFO (expected: 0 on a
    /// healthy connection; rumqttc pairs PUBACKs by packet id).
    pub ack_mismatches: u64,
    /// Completed broker connections (1 on a quiet run).
    pub connects: u64,
    /// Lost broker connections.
    pub disconnects: u64,
    /// Rows written to the oracle file.
    pub oracle_rows: u64,
    /// Per-stream counters.
    pub per_stream: std::collections::BTreeMap<String, StreamStats>,
}

impl SimStats {
    /// Records one accepted publish.
    pub fn published(&mut self, stream: Stream, bytes: usize) {
        self.messages_published += 1;
        self.bytes_published += bytes as u64;
        let entry = self
            .per_stream
            .entry(stream.as_str().to_owned())
            .or_default();
        entry.messages += 1;
        entry.bytes += bytes as u64;
    }

    /// A one-line human summary.
    pub fn summary_line(&self) -> String {
        format!(
            "published {} messages ({} bytes), {} acked, {} publish errors, \
             {} connects/{} disconnects, {} oracle rows",
            self.messages_published,
            self.bytes_published,
            self.acked_messages,
            self.publish_errors,
            self.connects,
            self.disconnects,
            self.oracle_rows
        )
    }
}
