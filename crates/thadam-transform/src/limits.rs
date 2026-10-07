//! Bounds on everything an untrusted payload or a config can make a chain
//! allocate or compute.

/// Resource limits for one chain.
///
/// Overflow never grows memory without bound: it becomes a counted reject
/// (shapes, JSON depth, payload size, explode fan-out), an eviction of the
/// oldest entry (dedup keys, per-asset state), or a compile error (predicate
/// and enrichment size).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChainLimits {
    /// Most distinct shapes the chain may create. Datasets decode to declared
    /// shapes, so shapes are created at compile time; a chain whose steps
    /// derive more is refused.
    pub max_shapes: usize,
    /// Cap on a dataset's dedup window, whatever its `max_keys` says. At the
    /// cap the oldest key is evicted and counted.
    pub max_dedup_keys: usize,
    /// Deepest JSON nesting a payload may have; deeper is a reject.
    pub max_json_depth: usize,
    /// Largest payload a decoder will parse.
    pub max_payload_bytes: usize,
    /// Most samples `explode` produces from one message.
    pub max_explode_samples: usize,
    /// Most assets `on_change` and `sample` track per step. At the cap the
    /// oldest asset's state is evicted, so its next event is kept.
    pub max_state_keys: usize,
    /// Most datasets in a chain.
    pub max_datasets: usize,
    /// Most nodes (combinators and leaves) in one predicate.
    pub max_predicate_nodes: usize,
    /// Deepest nesting of `all`, `any`, and `not` in one predicate.
    pub max_predicate_depth: usize,
    /// Most literals in one `in` set.
    pub max_in_set: usize,
    /// Largest enrichment file, in bytes.
    pub max_enrichment_bytes: u64,
    /// Most rows in an enrichment table.
    pub max_enrichment_rows: usize,
}

impl ChainLimits {
    /// Sets `max_shapes`.
    #[must_use]
    pub fn with_max_shapes(mut self, max_shapes: usize) -> Self {
        self.max_shapes = max_shapes;
        self
    }

    /// Sets `max_dedup_keys`.
    #[must_use]
    pub fn with_max_dedup_keys(mut self, max_dedup_keys: usize) -> Self {
        self.max_dedup_keys = max_dedup_keys;
        self
    }

    /// Sets `max_json_depth`.
    #[must_use]
    pub fn with_max_json_depth(mut self, max_json_depth: usize) -> Self {
        self.max_json_depth = max_json_depth;
        self
    }

    /// Sets `max_payload_bytes`.
    #[must_use]
    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    /// Sets `max_explode_samples`.
    #[must_use]
    pub fn with_max_explode_samples(mut self, max_explode_samples: usize) -> Self {
        self.max_explode_samples = max_explode_samples;
        self
    }

    /// Sets `max_state_keys`.
    #[must_use]
    pub fn with_max_state_keys(mut self, max_state_keys: usize) -> Self {
        self.max_state_keys = max_state_keys;
        self
    }

    /// Sets `max_datasets`.
    #[must_use]
    pub fn with_max_datasets(mut self, max_datasets: usize) -> Self {
        self.max_datasets = max_datasets;
        self
    }

    /// Sets `max_predicate_nodes`.
    #[must_use]
    pub fn with_max_predicate_nodes(mut self, max_predicate_nodes: usize) -> Self {
        self.max_predicate_nodes = max_predicate_nodes;
        self
    }

    /// Sets `max_predicate_depth`.
    #[must_use]
    pub fn with_max_predicate_depth(mut self, max_predicate_depth: usize) -> Self {
        self.max_predicate_depth = max_predicate_depth;
        self
    }

    /// Sets `max_in_set`.
    #[must_use]
    pub fn with_max_in_set(mut self, max_in_set: usize) -> Self {
        self.max_in_set = max_in_set;
        self
    }

    /// Sets `max_enrichment_bytes`.
    #[must_use]
    pub fn with_max_enrichment_bytes(mut self, max_enrichment_bytes: u64) -> Self {
        self.max_enrichment_bytes = max_enrichment_bytes;
        self
    }

    /// Sets `max_enrichment_rows`.
    #[must_use]
    pub fn with_max_enrichment_rows(mut self, max_enrichment_rows: usize) -> Self {
        self.max_enrichment_rows = max_enrichment_rows;
        self
    }
}

impl Default for ChainLimits {
    fn default() -> Self {
        ChainLimits {
            max_shapes: 64,
            max_dedup_keys: 1_000_000,
            max_json_depth: 32,
            max_payload_bytes: 1 << 20,
            max_explode_samples: 65_536,
            max_state_keys: 100_000,
            max_datasets: 256,
            max_predicate_nodes: 128,
            max_predicate_depth: 16,
            max_in_set: 1024,
            max_enrichment_bytes: 16 << 20,
            max_enrichment_rows: 1_000_000,
        }
    }
}
