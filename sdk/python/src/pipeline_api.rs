//! `thadam.Pipeline`: build once, run on a Rust-owned Tokio runtime, stop from
//! another thread.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use thadam_config::ProcessEnv;
use thadam_core::Shutdown;
use thadam_runtime::{
    ExitStatus, LogFormat, Pipeline, PrometheusMetrics, RuntimeOptions, init_logging,
};
use tokio::runtime::Builder;

use crate::error::{classify_to_py_err, combine_to_py_err};
use crate::registry::build_registry;

struct PipelineState {
    pipeline: Option<Pipeline>,
    shutdown: Shutdown,
    worker_threads: usize,
    ran: bool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A poisoned mutex here means a panicked `run`; the state is whole either
    // way (`pipeline` is taken before the run starts).
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A built Thadam pipeline, ready to run in-process.
///
/// Construction loads and validates the configuration and builds every
/// connector (raising `ThadamError` on failure). `run(timeout)` blocks the
/// calling thread with the GIL released; `stop()` from another thread
/// triggers the same graceful shutdown the CLI performs on SIGINT. A
/// Pipeline runs once: construct a new one to run again.
#[pyclass(name = "Pipeline", module = "thadam._native")]
pub struct PyPipeline {
    state: Mutex<PipelineState>,
}

#[pymethods]
impl PyPipeline {
    /// Loads `path` and builds the pipeline. Raises `ThadamError` with a
    /// `config` domain for configuration problems, or the component's domain
    /// for connector build failures (all messages joined).
    #[new]
    fn new(py: Python<'_>, path: &str) -> PyResult<Self> {
        let path = PathBuf::from(path);
        let spec = thadam_config::load_file(&path, &ProcessEnv)
            .map_err(|report| classify_to_py_err(py, &report))?;
        let worker_threads = spec
            .runtime
            .worker_threads
            .map(|n| n.get() as usize)
            .unwrap_or_else(|| {
                // Default: min(cores, 4), like the CLI.
                std::thread::available_parallelism()
                    .map(|n| n.get().min(4))
                    .unwrap_or(4)
            });
        let registry = build_registry();
        let mut opts = RuntimeOptions::default();
        opts.metrics = Arc::new(PrometheusMetrics::new());
        let pipeline =
            Pipeline::build(spec, &registry, opts).map_err(|errs| combine_to_py_err(py, &errs))?;
        Ok(PyPipeline {
            state: Mutex::new(PipelineState {
                pipeline: Some(pipeline),
                shutdown: Shutdown::new(),
                worker_threads,
                ran: false,
            }),
        })
    }

    /// Runs the pipeline to completion and returns the process-style exit
    /// code (0, 1, 2, 3, 130; see `website/docs/src/operations/exit-codes.md`).
    ///
    /// The pipeline runs on a fresh Rust-owned Tokio multi-thread runtime
    /// with the GIL released, so other Python threads keep working and may
    /// call `stop()`. With `timeout` (seconds), a graceful shutdown is
    /// triggered when it elapses; the code reflects the outcome (`0` means
    /// the pipeline drained cleanly, `130` that the shutdown deadline
    /// passed). The timeout is not a hard kill.
    ///
    /// Raises `ThadamError` when the configuration is unusable and
    /// `RuntimeError` when called on a pipeline that already ran.
    #[pyo3(signature = (timeout = None))]
    fn run(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<i32> {
        let (pipeline, shutdown, worker_threads) = {
            let mut state = lock(&self.state);
            if state.ran {
                return Err(PyRuntimeError::new_err(
                    "this Pipeline already ran; construct a new thadam.Pipeline to run again",
                ));
            }
            let pipeline = state
                .pipeline
                .take()
                .ok_or_else(|| PyRuntimeError::new_err("pipeline build state is missing"))?;
            state.ran = true;
            (pipeline, state.shutdown.clone(), state.worker_threads)
        };

        if let Some(secs) = timeout
            && (!secs.is_finite() || secs < 0.0)
        {
            return Err(PyValueError::new_err(
                "timeout must be a non-negative number of seconds or None",
            ));
        }

        py.detach(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .max_blocking_threads(8)
                .enable_all()
                .build()
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("failed to build Tokio runtime: {e}"))
                })?;

            // Logging is process-global: the first pipeline to run wins; later
            // runs keep working without re-initializing the subscriber.
            let _logging_guard = init_logging(LogFormat::Pretty, "info").ok();

            runtime.block_on(async move {
                let fut = pipeline.run(shutdown.clone());
                tokio::pin!(fut);
                let result = match timeout {
                    Some(secs) => {
                        tokio::select! {
                            res = &mut fut => res,
                            () = tokio::time::sleep(std::time::Duration::from_secs_f64(secs)) => {
                                shutdown.trigger();
                                fut.await
                            }
                        }
                    }
                    None => fut.await,
                };
                let status = match result {
                    Ok(report) => report.exit,
                    Err(e) => {
                        eprintln!("error: pipeline run failed: {e}");
                        ExitStatus::for_error(&e)
                    }
                };
                Ok(i32::from(status.code()))
            })
        })
    }

    /// Triggers graceful shutdown from any thread. Idempotent: safe to call
    /// before, during, or after `run`. Returns immediately; `run` returns
    /// when the pipeline has drained (bounded by the runtime's shutdown
    /// timeout).
    fn stop(&self) {
        let state = lock(&self.state);
        state.shutdown.trigger();
    }

    /// Whether `stop` has been triggered.
    #[getter]
    fn stopping(&self) -> bool {
        let state = lock(&self.state);
        state.shutdown.is_triggered()
    }
}

/// Registers the pipeline class on the extension module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPipeline>()?;
    Ok(())
}
