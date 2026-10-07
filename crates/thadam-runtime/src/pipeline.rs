//! `Pipeline`: build-time validation, the startup recovery sequence, the
//! supervisor, and the shutdown order.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use thadam_config::PipelineSpec;
use thadam_core::{
    BatchSink, BuildContext, ErrorDomain, ThadamError, SegmentSink, Service, ServiceContext,
    Shutdown, SinkContext, SinkId, Source, codes,
};
use thadam_transform::Chain;
use thadam_wal::{RecoveryReport, Wal};
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::task::{Id, JoinError, JoinSet};

use crate::batch::{BatchDriver, BatchOpts};
use crate::chain::{ChainConfig, ChainTask, wal_error};
use crate::datadir::{self, Layout};
use crate::exit::ExitStatus;
use crate::lane::{lane, rewind_channel};
use crate::metrics::MetricEvent;
use crate::options::RuntimeOptions;
use crate::plan::{self, config_error, cursor_name};
use crate::quarantine::Quarantine;
use crate::registry::{Registry, SinkFactory};
use crate::segment::{DriverOpts, SegmentDriver};
use crate::source::SourceDriver;
use crate::status::{
    Component, ComponentKind, ErrorInfo, Phase, PipelineHandle, StatusSnapshot, add, lock,
};

/// What WAL recovery found at startup.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct RecoverySummary {
    /// The WAL directory was empty and a new WAL was created.
    pub wal_created: bool,
    /// The previous writer shut down cleanly.
    pub clean_shutdown: bool,
    /// Bytes removed from the end of the last segment.
    pub truncated_bytes: u64,
    /// The last segment ended in a torn frame.
    pub torn_tail: bool,
    /// Positions skipped because a cursor was beyond the recovered end.
    pub skipped: Option<(u64, u64)>,
    /// The position the next frame gets.
    pub next_seq: u64,
    /// Time spent in WAL recovery, milliseconds.
    pub elapsed_ms: f64,
}

impl From<&RecoveryReport> for RecoverySummary {
    fn from(r: &RecoveryReport) -> Self {
        RecoverySummary {
            wal_created: r.created,
            clean_shutdown: r.clean_shutdown,
            truncated_bytes: r.truncated_bytes,
            torn_tail: r.torn_tail,
            skipped: r.skipped.map(|s| (s.first().get(), s.last().get())),
            next_seq: r.next_seq.get(),
            elapsed_ms: r.elapsed.as_secs_f64() * 1e3,
        }
    }
}

/// How a run ended.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RunReport {
    /// The process exit status.
    pub exit: ExitStatus,
    /// The final status of every component, cursor, and budget.
    pub status: StatusSnapshot,
    /// What WAL recovery did at startup.
    pub recovery: RecoverySummary,
    /// `thadam.json` mismatches accepted because of [`RuntimeOptions::repair`].
    pub repairs: Vec<String>,
    /// The WAL writer's failure, if it had one.
    pub wal_error: Option<ErrorInfo>,
}

struct SourceSlot {
    id: String,
    source: Box<dyn Source>,
}

enum SinkConn {
    Segment(Box<dyn SegmentSink>),
    Batch(Box<dyn BatchSink>),
}

struct SinkSlot {
    id: String,
    conn: SinkConn,
    chain: Chain,
    chain_cfg: ChainConfig,
    quarantine: Option<Box<dyn SegmentSink>>,
}

struct ServiceSlot {
    id: String,
    service: Box<dyn Service>,
}

/// A built pipeline: connectors constructed and validated, nothing running.
pub struct Pipeline {
    spec: PipelineSpec,
    opts: RuntimeOptions,
    layout: Layout,
    handle: PipelineHandle,
    config_hash: String,
    sources: Vec<SourceSlot>,
    sinks: Vec<SinkSlot>,
    services: Vec<ServiceSlot>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("pipeline", &self.spec.pipeline.name)
            .field("data_dir", &self.layout.root)
            .field(
                "sources",
                &self.sources.iter().map(|s| &s.id).collect::<Vec<_>>(),
            )
            .field(
                "sinks",
                &self.sinks.iter().map(|s| &s.id).collect::<Vec<_>>(),
            )
            .field(
                "services",
                &self.services.iter().map(|s| &s.id).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Ids become WAL cursor names and directory names.
fn check_id(kind: &str, index: usize, id: &str, errs: &mut Vec<ThadamError>) {
    let ok = !id.is_empty()
        && id.len() <= 100
        && !id.starts_with('.')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if !ok {
        errs.push(config_error(
            &format!("{kind}[{index}].id"),
            format!(
                "id `{id}` must be 1 to 100 characters of [A-Za-z0-9_.-] and not start with '.'"
            ),
        ));
    }
}

fn locate(errs: Vec<ThadamError>, component: &str) -> impl Iterator<Item = ThadamError> + '_ {
    errs.into_iter()
        .map(move |e| e.with_context("component", component))
}

enum Planned<'r> {
    Segment(&'r dyn thadam_core::SegmentSinkFactory, ChainPlan),
    Batch(&'r dyn thadam_core::BatchSinkFactory, ChainPlan),
    Service(&'r dyn thadam_core::ServiceFactory),
}

struct ChainPlan {
    chain: Chain,
    cfg: ChainConfig,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Group {
    Sources = 0,
    Lanes = 1,
    Services = 2,
    Maintenance = 3,
}

struct Supervisor {
    set: JoinSet<()>,
    tasks: HashMap<Id, (Arc<Component>, Group)>,
    outstanding: [usize; 4],
}

fn panic_code(kind: ComponentKind) -> (ErrorDomain, &'static str) {
    match kind {
        ComponentKind::Source | ComponentKind::Service | ComponentKind::Maintenance => {
            (ErrorDomain::Connector, codes::CONNECTOR_PANIC)
        }
        ComponentKind::Chain => (ErrorDomain::Transform, codes::TRANSFORM_PANIC),
        _ => (ErrorDomain::Sink, codes::SINK_PANIC),
    }
}

impl Supervisor {
    fn new() -> Self {
        Supervisor {
            set: JoinSet::new(),
            tasks: HashMap::new(),
            outstanding: [0; 4],
        }
    }

    fn spawn<F>(&mut self, group: Group, component: Arc<Component>, fut: F)
    where
        F: Future<Output = Result<(), ThadamError>> + Send + 'static,
    {
        let c = Arc::clone(&component);
        let h = self.set.spawn(async move {
            if let Err(e) = fut.await {
                c.failed(&e);
            }
        });
        self.tasks.insert(h.id(), (component, group));
        self.outstanding[group as usize] += 1;
    }

    fn reap(&mut self, r: Option<Result<(Id, ()), JoinError>>) {
        let id = match r {
            None => return,
            Some(Ok((id, ()))) => id,
            Some(Err(e)) => {
                let id = e.id();
                if e.is_panic()
                    && let Some((c, _)) = self.tasks.get(&id)
                {
                    let detail = e
                        .try_into_panic()
                        .ok()
                        .and_then(|p| {
                            p.downcast_ref::<&str>()
                                .map(|s| (*s).to_owned())
                                .or_else(|| p.downcast_ref::<String>().cloned())
                        })
                        .unwrap_or_else(|| "panic".to_owned());
                    let (domain, code) = panic_code(c.kind);
                    add(&c.counters.panics, 1);
                    c.failed(&ThadamError::fatal(
                        domain,
                        code,
                        format!("{} panicked: {detail}", c.id),
                    ));
                }
                id
            }
        };
        if let Some((_, g)) = self.tasks.remove(&id) {
            self.outstanding[g as usize] -= 1;
        }
    }

    async fn next(&mut self) {
        let r = self.set.join_next_with_id().await;
        self.reap(r);
    }

    async fn wait_group(&mut self, g: Group) {
        while self.outstanding[g as usize] > 0 && !self.set.is_empty() {
            self.next().await;
        }
    }
}

impl Pipeline {
    /// Validates every connector's configuration through its factory, compiles
    /// each sink's chain, and builds the connectors. Returns every problem
    /// found; map them to an exit code with [`ExitStatus::for_errors`].
    pub fn build(
        spec: PipelineSpec,
        registry: &Registry,
        opts: RuntimeOptions,
    ) -> Result<Pipeline, Vec<ThadamError>> {
        let mut errs = Vec::new();
        let root = opts
            .data_dir
            .clone()
            .unwrap_or_else(|| spec.pipeline.data_dir.clone());
        let layout = Layout { root };

        let mut seen = HashMap::new();
        for (kind, list) in [("sources", &spec.sources), ("sinks", &spec.sinks)] {
            for (i, c) in list.iter().enumerate() {
                check_id(kind, i, &c.id, &mut errs);
                if let Some(prev) = seen.insert(c.id.clone(), format!("{kind}[{i}]")) {
                    errs.push(config_error(
                        &format!("{kind}[{i}].id"),
                        format!("id `{}` is also used by {prev}", c.id),
                    ));
                }
            }
        }

        let mut source_factories = Vec::new();
        for (i, s) in spec.sources.iter().enumerate() {
            let at = format!("sources[{i}]");
            match registry.source_factory(&s.kind) {
                None => errs.push(
                    ThadamError::permanent(
                        ErrorDomain::Config,
                        codes::CONFIG_UNKNOWN_KIND,
                        format!("no source is registered for kind `{}`", s.kind),
                    )
                    .with_context("path", format!("{at}.kind")),
                ),
                Some(f) => {
                    if let Err(es) = f.validate(&s.config) {
                        errs.extend(locate(es, &at));
                    }
                    source_factories.push(f);
                }
            }
        }

        let limits = opts.chain_limits.clone();
        let mut planned = Vec::new();
        let mut first_chain = true;
        for (i, s) in spec.sinks.iter().enumerate() {
            let at = format!("sinks[{i}]");
            let found = registry.sink_factories(&s.kind);
            let factory = match found.len() {
                0 => {
                    errs.push(
                        ThadamError::permanent(
                            ErrorDomain::Config,
                            codes::CONFIG_UNKNOWN_KIND,
                            format!("no sink or service is registered for kind `{}`", s.kind),
                        )
                        .with_context("path", format!("{at}.kind")),
                    );
                    continue;
                }
                1 => found.into_iter().next(),
                _ => {
                    errs.push(config_error(
                        &format!("{at}.kind"),
                        format!("kind `{}` is registered as more than one of segment sink, batch sink, and service", s.kind),
                    ));
                    continue;
                }
            };
            let Some(factory) = factory else { continue };
            let valid = match &factory {
                SinkFactory::Segment(f) => f.validate(&s.config),
                SinkFactory::Batch(f) => f.validate(&s.config),
                SinkFactory::Service(f) => f.validate(&s.config),
            };
            if let Err(es) = valid {
                errs.extend(locate(es, &at));
            }
            let chain_plan = if matches!(factory, SinkFactory::Service(_)) {
                None
            } else {
                let only = match plan::sink_datasets(&spec, i, &s.config) {
                    Ok(o) => o,
                    Err(e) => {
                        errs.push(e);
                        continue;
                    }
                };
                let cs = match plan::chain_spec(
                    &spec,
                    &s.id,
                    only.as_deref(),
                    opts.config_dir.as_deref(),
                ) {
                    Ok(cs) => cs,
                    Err(e) => {
                        errs.push(e.with_context("component", at.clone()));
                        continue;
                    }
                };
                match Chain::compile(&cs, limits.clone()) {
                    Ok(chain) => {
                        let primary = std::mem::replace(&mut first_chain, false);
                        Some(ChainPlan {
                            chain,
                            cfg: ChainConfig {
                                sink_id: s.id.clone(),
                                cursor: cursor_name(&s.id),
                                spec: cs,
                                limits: limits.clone(),
                                primary,
                                has_state: plan::chain_has_state(&spec, &s.id, only.as_deref()),
                                warm_frames: opts.warm_start_frames,
                                read_bytes: opts.read_batch_bytes,
                                poison_retries: opts.poison_retries,
                                failpoints: opts.failpoints.clone(),
                            },
                        })
                    }
                    Err(es) => {
                        errs.extend(locate(es, &at));
                        continue;
                    }
                }
            };
            planned.push((
                s,
                match (factory, chain_plan) {
                    (SinkFactory::Segment(f), Some(p)) => Planned::Segment(f, p),
                    (SinkFactory::Batch(f), Some(p)) => Planned::Batch(f, p),
                    (SinkFactory::Service(f), _) => Planned::Service(f),
                    _ => continue,
                },
            ));
        }

        let quarantine_cfg = plan::quarantine_config(&spec);
        if let Some(q) = registry.quarantine_factory()
            && let Err(es) = q.validate(&quarantine_cfg)
        {
            errs.extend(locate(es, "quarantine"));
        }
        if !errs.is_empty() {
            return Err(errs);
        }

        let clock = Arc::clone(&opts.clock);
        let mut sources = Vec::new();
        for (s, f) in spec.sources.iter().zip(source_factories) {
            let ctx = BuildContext::new(s.id.as_str(), Arc::clone(&clock), layout.source(&s.id));
            match f.build(s.config.clone(), &ctx) {
                Ok(source) => sources.push(SourceSlot {
                    id: s.id.clone(),
                    source,
                }),
                Err(e) => errs.push(e.with_context("component", s.id.clone())),
            }
        }
        let mut sinks = Vec::new();
        let mut services = Vec::new();
        for (s, p) in planned {
            let ctx = BuildContext::new(s.id.as_str(), Arc::clone(&clock), layout.sink(&s.id));
            let quarantine = |errs: &mut Vec<ThadamError>| {
                let q = registry.quarantine_factory()?;
                let id = format!("quarantine.{}", s.id);
                let qctx =
                    BuildContext::new(id.as_str(), Arc::clone(&clock), layout.quarantine(&s.id));
                match q.build(quarantine_cfg.clone(), &qctx) {
                    Ok(q) => Some(q),
                    Err(e) => {
                        errs.push(e.with_context("component", id));
                        None
                    }
                }
            };
            match p {
                Planned::Segment(f, cp) => match f.build(s.config.clone(), &ctx) {
                    Ok(sink) => sinks.push(SinkSlot {
                        id: s.id.clone(),
                        conn: SinkConn::Segment(sink),
                        chain: cp.chain,
                        chain_cfg: cp.cfg,
                        quarantine: quarantine(&mut errs),
                    }),
                    Err(e) => errs.push(e.with_context("component", s.id.clone())),
                },
                Planned::Batch(f, cp) => match f.build(s.config.clone(), &ctx) {
                    Ok(sink) => sinks.push(SinkSlot {
                        id: s.id.clone(),
                        conn: SinkConn::Batch(sink),
                        chain: cp.chain,
                        chain_cfg: cp.cfg,
                        quarantine: quarantine(&mut errs),
                    }),
                    Err(e) => errs.push(e.with_context("component", s.id.clone())),
                },
                Planned::Service(f) => {
                    let ctx =
                        BuildContext::new(s.id.as_str(), Arc::clone(&clock), layout.service(&s.id));
                    match f.build(s.config.clone(), &ctx) {
                        Ok(service) => services.push(ServiceSlot {
                            id: s.id.clone(),
                            service,
                        }),
                        Err(e) => errs.push(e.with_context("component", s.id.clone())),
                    }
                }
            }
        }
        if !errs.is_empty() {
            return Err(errs);
        }
        let config_hash = spec.config_hash_hex();
        let handle = PipelineHandle::new(
            spec.pipeline.name.clone(),
            config_hash.clone(),
            opts.sync_flag.clone(),
        );
        Ok(Pipeline {
            spec,
            opts,
            layout,
            handle,
            config_hash,
            sources,
            sinks,
            services,
        })
    }

    /// Status snapshots, readiness, and forced shutdown for this pipeline.
    pub fn handle(&self) -> PipelineHandle {
        self.handle.clone()
    }

    /// Runs until `shutdown` triggers (or, with
    /// [`RuntimeOptions::stop_when_sources_finish`], until every source ends),
    /// then shuts down in order. Requires a Tokio runtime with time enabled.
    ///
    /// `Err` means the run could not start (data directory locked or
    /// incompatible, WAL recovery failed); map it with
    /// [`ExitStatus::for_error`]. Once started, the outcome is in
    /// [`RunReport::exit`].
    pub async fn run(self, shutdown: Shutdown) -> Result<RunReport, ThadamError> {
        let handle = self.handle.clone();
        handle.set_phase(Phase::Starting);
        let result = self.run_inner(shutdown).await;
        let result = result.map(|mut report| {
            report.status = handle.status();
            report.status.phase = Phase::Stopped;
            report.status.ready = false;
            report
        });
        *lock(&handle.0.wal) = None;
        handle.set_phase(Phase::Stopped);
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner(self, external: Shutdown) -> Result<RunReport, ThadamError> {
        let Pipeline {
            spec,
            opts,
            layout,
            handle,
            config_hash,
            sources,
            sinks,
            services,
        } = self;

        // 1. Data directory lock and thadam.json.
        let _lock = datadir::lock(&layout.root)?;
        let existing = datadir::read_thadam_json(&layout.root)?;
        let mut repairs =
            datadir::check_before_wal(existing.as_ref(), &spec.pipeline.name, opts.repair)?;

        // 2. WAL recovery on the blocking pool.
        let budget = Arc::new(plan::budget(&spec));
        *lock(&handle.0.budget) = Arc::clone(&budget);
        let chain_ids: Vec<String> = sinks.iter().map(|s| s.id.clone()).collect();
        let wal_opts = plan::wal_options(&spec, &chain_ids, opts.machine_id.clone())?;
        let mut limits = opts.batch_limits;
        limits.max_bytes = limits.max_bytes.min(wal_opts.frame_max_bytes / 2).max(1);
        let (wal_dir, b, clock) = (layout.wal(), Arc::clone(&budget), Arc::clone(&opts.clock));
        let (wal, recovery) =
            tokio::task::spawn_blocking(move || thadam_wal::open(&wal_dir, wal_opts, b, clock))
                .await
                .map_err(|_| {
                    ThadamError::fatal(
                        ErrorDomain::Buffer,
                        codes::BUFFER_IO,
                        "wal open task panicked",
                    )
                })?
                .map_err(|e| wal_error(&e))?;
        repairs.extend(datadir::check_wal_binding(
            existing.as_ref(),
            wal.epoch(),
            opts.repair,
        )?);
        for r in &repairs {
            tracing::warn!(repair = %r, "thadam.json mismatch accepted (--repair)");
        }
        let meta = datadir::next_thadam_json(
            existing.as_ref(),
            &spec.pipeline.name,
            &config_hash,
            wal.epoch(),
        );
        datadir::write_thadam_json(&layout.root, &meta)?;
        if meta.previous_config_hash.is_some()
            && existing
                .as_ref()
                .is_some_and(|e| e.config_hash != config_hash)
        {
            tracing::info!(previous = ?meta.previous_config_hash, current = %config_hash, "configuration changed since the last run");
        }
        *lock(&handle.0.wal) = Some(wal.clone());
        tracing::info!(
            epoch = %wal.epoch(), created = recovery.created, clean = recovery.clean_shutdown,
            truncated = recovery.truncated_bytes, next = %recovery.next_seq,
            elapsed_ms = recovery.elapsed.as_secs_f64() * 1e3, "wal recovered"
        );
        let writer = wal.start_writer().map_err(|e| wal_error(&e))?;

        let stopping = Shutdown::new();
        let intake = Shutdown::new();
        let drain = Shutdown::new();
        let services_stop = Shutdown::new();
        let maint_stop = Shutdown::new();
        let wal_failed = Shutdown::new();
        let force = handle.0.force.clone();
        let metrics = Arc::clone(&opts.metrics);
        let clock = Arc::clone(&opts.clock);
        let mut sup = Supervisor::new();

        // 3. Maintenance first.
        let maint = Component::new(
            "maintenance".into(),
            ComponentKind::Maintenance,
            Arc::clone(&metrics),
            Arc::clone(&clock),
        );
        handle.register(Arc::clone(&maint));
        sup.spawn(
            Group::Maintenance,
            Arc::clone(&maint),
            maintenance(maint, wal.clone(), maint_stop.clone(), opts.clone()),
        );

        // 4. Sinks and chains: each driver recovers its sink, then releases
        //    its chain, which warms up and starts reading.
        let mut ready = Vec::new();
        for slot in sinks {
            let SinkSlot {
                id,
                conn,
                chain,
                chain_cfg,
                quarantine,
            } = slot;
            let kind = match conn {
                SinkConn::Segment(_) => ComponentKind::SegmentSink,
                SinkConn::Batch(_) => ComponentKind::BatchSink,
            };
            let sink_c = Component::new(
                format!("sink:{id}"),
                kind,
                Arc::clone(&metrics),
                Arc::clone(&clock),
            );
            let chain_c = Component::new(
                format!("chain:{id}"),
                ComponentKind::Chain,
                Arc::clone(&metrics),
                Arc::clone(&clock),
            );
            handle.register(Arc::clone(&sink_c));
            handle.register(Arc::clone(&chain_c));
            let (tx, rx) = lane(
                opts.chain_channel_batches,
                opts.read_batch_bytes.saturating_mul(2),
                Arc::clone(&sink_c),
            );
            let (rewind_tx, rewind_rx) = rewind_channel();
            let (start_tx, start_rx) = oneshot::channel();
            let (ready_tx, ready_rx) = oneshot::channel();
            ready.push(ready_rx);
            let ctx = SinkContext::new(
                SinkId::new(id.as_str()),
                force.clone(),
                Arc::clone(&clock),
                layout.sink(&id),
                Arc::clone(&budget),
            );
            let quarantine = quarantine.map(|q| {
                let qctx = SinkContext::new(
                    SinkId::new(format!("quarantine.{id}")),
                    force.clone(),
                    Arc::clone(&clock),
                    layout.quarantine(&id),
                    Arc::clone(&budget),
                );
                Quarantine::new(q, qctx, id.clone(), Arc::clone(&sink_c))
            });
            let cursor = cursor_name(&id);
            match conn {
                SinkConn::Segment(sink) => {
                    let d = SegmentDriver::new(
                        id.clone(),
                        cursor,
                        Arc::clone(&sink_c),
                        sink,
                        ctx,
                        quarantine,
                        wal.clone(),
                        handle.clone(),
                        rx,
                        rewind_tx,
                        start_tx,
                        DriverOpts {
                            poll_interval: opts.poll_commit_interval,
                            commit_interval: opts.cursor_commit_interval,
                            retry: opts.sink_retry,
                            failpoints: opts.failpoints.clone(),
                        },
                        stopping.clone(),
                        force.clone(),
                    );
                    sup.spawn(Group::Lanes, Arc::clone(&sink_c), d.run());
                }
                SinkConn::Batch(sink) => {
                    let d = BatchDriver::new(
                        id.clone(),
                        cursor,
                        Arc::clone(&sink_c),
                        sink,
                        ctx,
                        quarantine,
                        wal.clone(),
                        handle.clone(),
                        rx,
                        rewind_tx,
                        start_tx,
                        BatchOpts {
                            max_events: opts.batch_max_events,
                            max_bytes: opts.batch_max_bytes,
                            max_age: opts.batch_max_age,
                            max_attempts_before_bisect: opts.max_attempts_before_bisect,
                            retry: opts.sink_retry,
                            failpoints: opts.failpoints.clone(),
                        },
                        stopping.clone(),
                        force.clone(),
                    );
                    sup.spawn(Group::Lanes, Arc::clone(&sink_c), d.run());
                }
            }
            let task = ChainTask {
                cfg: chain_cfg,
                component: Arc::clone(&chain_c),
                chain,
                wal: wal.clone(),
                tx,
                rewind: rewind_rx,
                drain: drain.clone(),
                start: start_rx,
                ready: Some(ready_tx),
            };
            sup.spawn(Group::Lanes, chain_c, task.run());
        }

        let mut wal_close_error = None;
        let orchestrate = async {
            // 5. Wait until every chain is reading (or gave up), then
            //    services, then sources last.
            for rx in ready {
                tokio::pin!(rx);
                loop {
                    tokio::select! {
                        _ = &mut rx => break,
                        () = external.triggered() => break,
                        () = sup.next(), if !sup.set.is_empty() => {}
                    }
                }
            }
            if !external.is_triggered() {
                for s in services {
                    let c = Component::new(
                        format!("service:{}", s.id),
                        ComponentKind::Service,
                        Arc::clone(&metrics),
                        Arc::clone(&clock),
                    );
                    handle.register(Arc::clone(&c));
                    let ctx = ServiceContext::new(
                        s.id.as_str(),
                        services_stop.clone(),
                        Arc::clone(&clock),
                        layout.service(&s.id),
                        Arc::clone(&budget),
                    );
                    sup.spawn(
                        Group::Services,
                        Arc::clone(&c),
                        run_service(c, s.service, ctx),
                    );
                }
                for s in sources {
                    let c = Component::new(
                        format!("source:{}", s.id),
                        ComponentKind::Source,
                        Arc::clone(&metrics),
                        Arc::clone(&clock),
                    );
                    handle.register(Arc::clone(&c));
                    let d = SourceDriver {
                        id: s.id,
                        component: Arc::clone(&c),
                        source: s.source,
                        submit: writer.clone(),
                        intake: intake.clone(),
                        clock: Arc::clone(&clock),
                        limits,
                        max_uncommitted: opts.max_uncommitted_batches,
                        restart: opts.source_restart,
                        drain_timeout: opts.drain_timeout,
                        wal_failed: wal_failed.clone(),
                    };
                    sup.spawn(Group::Sources, c, d.run());
                }
                handle.set_phase(Phase::Running);
                tracing::info!(pipeline = %spec.pipeline.name, "pipeline running");
            }

            // 6. Run until told to stop.
            loop {
                if external.is_triggered() || wal_failed.is_triggered() {
                    break;
                }
                if opts.stop_when_sources_finish && sup.outstanding[Group::Sources as usize] == 0 {
                    break;
                }
                tokio::select! {
                    () = external.triggered() => {}
                    () = wal_failed.triggered() => {}
                    () = sup.next(), if !sup.set.is_empty() => {}
                }
            }

            // 7. Shutdown, in order.
            stopping.trigger();
            handle.set_phase(Phase::Draining);
            tracing::info!("shutdown: stopping intake");
            intake.trigger();
            sup.wait_group(Group::Sources).await;
            drop(writer);
            tracing::info!("shutdown: sources stopped; closing the wal writer");
            if let Err(e) = wal.close().await {
                wal_close_error = Some(wal_error(&e));
            }
            let deadline = tokio::time::sleep(opts.drain_timeout);
            tokio::pin!(deadline);
            while sup.outstanding[Group::Lanes as usize] > 0 && !sup.set.is_empty() {
                tokio::select! {
                    () = sup.next() => {}
                    () = &mut deadline, if !drain.is_triggered() => {
                        tracing::warn!("shutdown: drain timeout; stopping chains");
                        drain.trigger();
                    }
                }
            }
            tracing::info!("shutdown: sinks finished; stopping services");
            services_stop.trigger();
            sup.wait_group(Group::Services).await;
            maint_stop.trigger();
            sup.wait_group(Group::Maintenance).await;
        };
        let abort = async {
            tokio::select! {
                () = force.triggered() => {}
                () = async {
                    stopping.triggered().await;
                    tokio::time::sleep(opts.shutdown_timeout).await;
                } => {}
                () = async {
                    external.triggered().await;
                    tokio::time::sleep(opts.shutdown_timeout).await;
                } => {}
            }
        };
        let aborted = tokio::select! {
            () = orchestrate => false,
            () = abort => true,
        };
        if aborted {
            tracing::error!("shutdown deadline passed or forced; aborting every task");
            sup.set.abort_all();
            while sup.set.join_next().await.is_some() {}
            if let Err(e) = wal.close().await {
                wal_close_error = Some(wal_error(&e));
            }
        }

        let failed = handle.components().iter().any(|c| c.is_failed());
        let exit = if aborted {
            ExitStatus::Interrupted
        } else if wal_failed.is_triggered() || wal_close_error.is_some() || failed {
            ExitStatus::RuntimeFailure
        } else {
            ExitStatus::Ok
        };
        tracing::info!(exit = exit.code(), "pipeline stopped");
        Ok(RunReport {
            exit,
            status: handle.status(),
            recovery: RecoverySummary::from(&recovery),
            repairs,
            wal_error: wal_close_error.as_ref().map(ErrorInfo::from),
        })
    }
}

async fn run_service(
    component: Arc<Component>,
    mut service: Box<dyn Service>,
    ctx: ServiceContext,
) -> Result<(), ThadamError> {
    component.running();
    service.run(&ctx).await?;
    component.stopped();
    Ok(())
}

async fn maintenance(
    component: Arc<Component>,
    wal: Wal,
    stop: Shutdown,
    opts: RuntimeOptions,
) -> Result<(), ThadamError> {
    component.running();
    let mut tick = tokio::time::interval(opts.maintenance_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.triggered() => break,
            _ = tick.tick() => {
                match wal.maintain().await {
                    Ok(r) => {
                        if r.deleted_bytes > 0 || r.evicted_bytes > 0 {
                            tracing::debug!(deleted = r.deleted_bytes, evicted = r.evicted_bytes, "wal retention");
                        }
                        component.metrics().record(MetricEvent::WalMaintained {
                            deleted_bytes: r.deleted_bytes,
                            evicted_bytes: r.evicted_bytes,
                        });
                        if !r.lagging_consumers.is_empty() {
                            tracing::warn!(consumers = ?r.lagging_consumers, "wal consumers exceeded max_lag");
                        }
                        if matches!(component.state(), crate::ComponentState::Degraded { .. }) {
                            component.running();
                        }
                    }
                    Err(e) => component.degraded(&wal_error(&e)),
                }
                if let Some(probe) = &opts.clock_sync_probe
                    && let Some(synced) = probe()
                {
                    opts.sync_flag.set(synced);
                    component.metrics().record(MetricEvent::ClockSynced { synced });
                }
            }
        }
    }
    component.stopped();
    Ok(())
}
