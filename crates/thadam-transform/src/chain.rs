//! The chain compiler and the per-batch driver.

use std::cell::Cell;
use std::collections::HashSet;
use std::marker::PhantomData;

use thadam_core::{
    ErrorDomain, EventBatch, Record, ThadamError, SchemaStore, ShapeRef, ShapeRegistry, Sym,
    TimeDomain, WalRange, codes,
};
use serde_json::Value as Json;

use crate::dataset::{Dataset, DatasetStats};
use crate::decode::parse_json;
use crate::input::{FrameInput, RejectedRaw};
use crate::limits::ChainLimits;
use crate::ros2::{Ros2Decode, record_to_json};
use crate::spec::{ChainSpec, DecodeSpec};
use crate::steps::{self, EnrichmentInfo, StepCtx, config_error};

/// What a compiled dataset produces and which files it was built from.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DatasetInfo {
    /// The dataset id.
    pub id: Sym,
    /// The shape of the events it emits, after every step.
    pub shape: ShapeRef,
    /// Identity levels in hierarchy order.
    pub identity_levels: Vec<Sym>,
    /// The sample-index column name, for an exploded dataset.
    pub index_field: Option<Sym>,
    /// The domain of the source time.
    pub time_domain: TimeDomain,
    /// Enrichment files loaded for this dataset, with their hashes.
    pub enrichment: Vec<EnrichmentInfo>,
}

/// One dataset's events from a [`Chain::process`] call.
#[derive(Clone, Debug)]
pub struct DatasetBatch {
    /// The dataset id.
    pub dataset: Sym,
    /// The events, in WAL order.
    pub batch: EventBatch,
}

/// The events one [`Chain::process`] call produced, per dataset.
///
/// `batches[i]` belongs to the chain's `i`th dataset, in declaration order.
/// Each batch carries the origin and clock state given to
/// [`Chain::new_output`], so a dataset with no events still reports the WAL
/// range the chain consumed.
#[derive(Clone, Debug)]
pub struct ChainOutput {
    /// One batch per dataset.
    pub batches: Vec<DatasetBatch>,
}

impl ChainOutput {
    /// The batch of dataset `id`.
    pub fn batch(&self, id: &str) -> Option<&EventBatch> {
        self.batches
            .iter()
            .find(|b| &*b.dataset == id)
            .map(|b| &b.batch)
    }

    /// Events across all datasets.
    pub fn total_events(&self) -> usize {
        self.batches.iter().map(|b| b.batch.len()).sum()
    }
}

/// Counters for a chain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChainStats {
    /// Records no dataset matched. They are not rejects.
    pub unmatched: u64,
    /// Per-dataset counters, in declaration order.
    pub datasets: Vec<(Sym, DatasetStats)>,
}

/// A compiled chain: router, decoders, and transform steps for each dataset.
///
/// A chain is synchronous and pure, apart from loading enrichment files at
/// compile time. It is `Send` but not `Sync`: one chain task owns it, with
/// its own shape registry and step state.
pub struct Chain {
    datasets: Vec<Dataset>,
    infos: Vec<DatasetInfo>,
    registry: ShapeRegistry,
    limits: ChainLimits,
    /// The ROS 2 payload decoder, built when any dataset is `decode: ros2`.
    ros2: Option<Ros2Decode>,
    unmatched: u64,
    _not_sync: PhantomData<Cell<()>>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chain")
            .field("datasets", &self.infos)
            .field("shapes", &self.registry.len())
            .finish()
    }
}

impl Chain {
    /// Compiles `spec` under `limits`, reporting every problem found.
    ///
    /// Errors carry the config path of the offending key in their `path`
    /// context, such as `transforms[2].where.all[1].field`.
    pub fn compile(spec: &ChainSpec, limits: ChainLimits) -> Result<Chain, Vec<ThadamError>> {
        let mut errs = Vec::new();
        if spec.datasets.len() > limits.max_datasets {
            return Err(vec![config_error(
                "datasets",
                format!("a chain has at most {} datasets", limits.max_datasets),
            )]);
        }
        let mut registry = ShapeRegistry::new(limits.max_shapes);

        let mut seen = HashSet::new();
        let mut slots: Vec<Option<Dataset>> = Vec::with_capacity(spec.datasets.len());
        for (i, d) in spec.datasets.iter().enumerate() {
            let path = format!("datasets[{i}]");
            if !seen.insert(d.id.as_str()) {
                errs.push(config_error(
                    &format!("{path}.id"),
                    format!("duplicate dataset id `{}`", d.id),
                ));
                slots.push(None);
                continue;
            }
            match Dataset::compile(d, &path, &limits, &mut registry) {
                Ok(ds) => slots.push(Some(ds)),
                Err(e) => {
                    errs.extend(e);
                    slots.push(None);
                }
            }
        }

        // A `decode: ros2` dataset needs the runtime-provided schema store;
        // without one there is nothing to resolve `mcap.schema_hash`
        // against, which only config can fix.
        let ros2 = if spec.datasets.iter().any(|d| d.decode == DecodeSpec::Ros2) {
            match &spec.schema_dir {
                Some(dir) => Some(Ros2Decode::new(SchemaStore::open(dir))),
                None => {
                    for (i, d) in spec.datasets.iter().enumerate() {
                        if d.decode == DecodeSpec::Ros2 {
                            errs.push(
                                ThadamError::permanent(
                                    ErrorDomain::Config,
                                    codes::CONFIG_MISSING,
                                    "`decode: ros2` needs the pipeline's schema store directory, which the runtime provides from the data directory",
                                )
                                .with_context("path", format!("datasets[{i}].decode")),
                            );
                        }
                    }
                    None
                }
            }
        } else {
            None
        };

        for (t, transform) in spec.transforms.iter().enumerate() {
            let path = format!("transforms[{t}]");
            let targets: Vec<usize> = match &transform.selector().0 {
                None => (0..slots.len()).collect(),
                Some(ids) if ids.is_empty() => {
                    errs.push(config_error(
                        &format!("{path}.dataset"),
                        "`dataset` lists no datasets",
                    ));
                    continue;
                }
                Some(ids) => {
                    let mut targets = Vec::new();
                    for id in ids {
                        match spec.datasets.iter().position(|d| d.id == *id) {
                            Some(i) => targets.push(i),
                            None => errs.push(config_error(
                                &format!("{path}.dataset"),
                                format!("unknown dataset `{id}`"),
                            )),
                        }
                    }
                    targets
                }
            };
            for i in targets {
                let Some(Some(ds)) = slots.get_mut(i) else {
                    continue;
                };
                let mut ctx = StepCtx {
                    dataset: &ds.id.clone(),
                    levels: &ds.levels.clone(),
                    limits: &limits,
                    registry: &mut registry,
                    base_dir: spec.base_dir.as_deref(),
                };
                match steps::compile(transform, &mut ctx, &ds.out_shape.clone(), &path) {
                    Ok(c) => {
                        ds.enrichment.extend(c.enrichment);
                        ds.push_step(c.step, c.out_shape);
                    }
                    Err(e) => errs.extend(e),
                }
            }
        }

        if !errs.is_empty() {
            return Err(errs);
        }
        let datasets: Vec<Dataset> = slots.into_iter().flatten().collect();
        let infos = datasets
            .iter()
            .map(|d| DatasetInfo {
                id: d.id.clone(),
                shape: d.out_shape.clone(),
                identity_levels: d.levels.clone(),
                index_field: d.index_field().cloned(),
                time_domain: d.time_domain(),
                enrichment: d.enrichment.clone(),
            })
            .collect();
        Ok(Chain {
            datasets,
            infos,
            registry,
            limits,
            ros2,
            unmatched: 0,
            _not_sync: PhantomData,
        })
    }

    /// The compiled datasets, in declaration order.
    pub fn datasets(&self) -> &[DatasetInfo] {
        &self.infos
    }

    /// Shapes the chain holds.
    pub fn shape_count(&self) -> usize {
        self.registry.len()
    }

    /// An empty output for records spanning `origin`.
    pub fn new_output(&self, origin: WalRange, clock_synced: bool) -> ChainOutput {
        ChainOutput {
            batches: self
                .infos
                .iter()
                .map(|d| DatasetBatch {
                    dataset: d.id.clone(),
                    batch: EventBatch::new(Vec::new(), origin, clock_synced),
                })
                .collect(),
        }
    }

    /// Routes, decodes, and transforms `input`, appending events to `out` and
    /// unprocessable records to `rejects`.
    ///
    /// Never panics on payload content and never aborts on a bad record: it
    /// becomes a [`RejectedRaw`]. `out` must come from [`Chain::new_output`]
    /// on this chain.
    pub fn process(
        &mut self,
        input: &FrameInput<'_>,
        out: &mut ChainOutput,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        let ros2 = &mut self.ros2;
        let limits = &self.limits;
        for view in input.records {
            // Decode failures are cached per record so every matching
            // dataset rejects the message with the same error.
            let mut json_root: Option<Result<Json, ThadamError>> = None;
            let mut ros2_root: Option<Result<Record, ThadamError>> = None;
            // The JSON spelling of the ROS 2 record, materialized only when
            // a matching dataset is not served by direct record reads.
            let mut ros2_bridge: Option<Json> = None;
            let mut matched = false;
            for (i, ds) in self.datasets.iter_mut().enumerate() {
                if !ds.matches(view) {
                    continue;
                }
                matched = true;
                let Some(slot) = out.batches.get_mut(i) else {
                    continue;
                };
                match ds.decode {
                    DecodeSpec::Json => {
                        match json_root.get_or_insert_with(|| parse_json(view.payload, limits)) {
                            Ok(root) => ds.process(view, root, &mut slot.batch.events, rejects),
                            Err(e) => {
                                ds.stats.messages += 1;
                                ds.stats.rejected += 1;
                                rejects.push(RejectedRaw::new(view, Some(&ds.id), None, e.clone()));
                            }
                        }
                    }
                    DecodeSpec::Ros2 => {
                        let root = ros2_root.get_or_insert_with(|| match ros2.as_mut() {
                            Some(decode) => decode.decode(view.attrs, view.payload),
                            None => Err(ThadamError::fatal(
                                ErrorDomain::Config,
                                codes::CONFIG_MISSING,
                                "`decode: ros2` has no schema store (compile-time check missed it)",
                            )),
                        });
                        match root {
                            Ok(record) => {
                                if ds.ros2_direct {
                                    ds.process_record(
                                        view,
                                        record,
                                        &mut ros2_bridge,
                                        &mut slot.batch.events,
                                        rejects,
                                    );
                                } else {
                                    let bridged =
                                        ros2_bridge.get_or_insert_with(|| record_to_json(record));
                                    ds.process(view, bridged, &mut slot.batch.events, rejects);
                                }
                            }
                            Err(e) => {
                                ds.stats.messages += 1;
                                ds.stats.rejected += 1;
                                rejects.push(RejectedRaw::new(view, Some(&ds.id), None, e.clone()));
                            }
                        }
                    }
                }
            }
            if !matched {
                self.unmatched += 1;
            }
        }
    }

    /// Rebuilds step state (dedup keys, last-kept values, sampling counters)
    /// from records already processed before the cursor, emitting nothing and
    /// leaving the counters as they were.
    ///
    /// Warm-then-process equals a continuous run over the same records
    /// whenever the warm records cover each state's reach: the dedup window,
    /// the heartbeat, and the history `on_change` and `sample` depend on.
    pub fn warm(&mut self, input: &FrameInput<'_>) {
        let saved_stats: Vec<DatasetStats> =
            self.datasets.iter().map(|d| d.stats.clone()).collect();
        let saved_unmatched = self.unmatched;
        let before: Vec<(u64, u64)> = self.datasets.iter().map(Dataset::evictions).collect();

        let mut scratch = ChainOutput {
            batches: self
                .infos
                .iter()
                .map(|d| DatasetBatch {
                    dataset: d.id.clone(),
                    batch: EventBatch::new(
                        Vec::new(),
                        input.origin().unwrap_or_else(|| {
                            WalRange::single(Default::default(), Default::default())
                        }),
                        input.clock_synced,
                    ),
                })
                .collect(),
        };
        let mut rejects = Vec::new();
        self.process(input, &mut scratch, &mut rejects);

        self.unmatched = saved_unmatched;
        for ((ds, stats), before) in self.datasets.iter_mut().zip(saved_stats).zip(before) {
            let after = ds.evictions();
            ds.warm_evictions.0 += after.0 - before.0;
            ds.warm_evictions.1 += after.1 - before.1;
            ds.stats = stats;
        }
    }

    /// A snapshot of the counters.
    pub fn stats(&self) -> ChainStats {
        ChainStats {
            unmatched: self.unmatched,
            datasets: self
                .datasets
                .iter()
                .map(|d| (d.id.clone(), d.snapshot_stats()))
                .collect(),
        }
    }
}
