//! `thadam run`: build and run a pipeline.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use thadam_config::{ProcessEnv, load_file};
use thadam_core::Shutdown;
use thadam_runtime::{
    AdminConfig, ExitStatus, Pipeline, PrometheusMetrics, RuntimeOptions, init_logging,
};
use tokio::runtime::Builder;

use crate::exit::Exit;
use crate::registry;

/// Run a pipeline configuration.
///
/// This function:
/// 1. Builds the connector registry
/// 2. Loads and validates the configuration file
/// 3. Initializes logging
/// 4. Builds a Tokio multi-thread runtime
/// 5. Builds and runs the pipeline with signal handling
pub fn run(
    path: &Path,
    repair: bool,
    log_format: Option<thadam_runtime::LogFormat>,
    log_level: &str,
    admin_bind: Option<&str>,
    admin_token_file: Option<&Path>,
    err: &mut dyn Write,
) -> Exit {
    // 1. Build the registry
    let registry = registry::build_registry();

    // 2. Load the configuration file
    let spec = match load_file(path, &ProcessEnv) {
        Ok(spec) => spec,
        Err(report) => {
            let _ = write!(err, "{}", report.render(&path.display().to_string()));
            return Exit::InvalidConfig;
        }
    };

    // 3. Initialize logging. The `--log-format` flag wins; without it the
    // config's `observability.logs.format` applies.
    let log_format = log_format.unwrap_or(match spec.observability.logs.format {
        thadam_config::LogFormat::Json => thadam_runtime::LogFormat::Json,
        _ => thadam_runtime::LogFormat::Pretty,
    });
    // The guard must stay alive until the run ends: dropping it closes the
    // non-blocking writer's channel and every later record is lost.
    let _logging_guard = match init_logging(log_format, log_level) {
        Ok(guard) => guard,
        Err(e) => {
            let _ = writeln!(err, "failed to initialize logging: {e}");
            return Exit::RuntimeFailure;
        }
    };

    // 4. Build the Tokio runtime
    let worker_threads = spec
        .runtime
        .worker_threads
        .map(|n| n.get() as usize)
        .unwrap_or_else(|| {
            // Default: min(cores, 4) as per ARCHITECTURE.md
            std::thread::available_parallelism()
                .map(|n| n.get().min(4))
                .unwrap_or(4)
        });

    let runtime = match Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(8)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = writeln!(err, "failed to build Tokio runtime: {e}");
            return Exit::RuntimeFailure;
        }
    };

    // 5. Run the pipeline inside the Tokio runtime
    runtime.block_on(async {
        run_pipeline(&registry, spec, repair, admin_bind, admin_token_file).await
    })
}

async fn run_pipeline(
    registry: &thadam_runtime::Registry,
    spec: thadam_config::PipelineSpec,
    repair: bool,
    admin_bind: Option<&str>,
    admin_token_file: Option<&Path>,
) -> Exit {
    // Build the pipeline - use a mutable options object
    let mut opts = RuntimeOptions::default();
    opts.repair = repair;
    opts.metrics = Arc::new(PrometheusMetrics::new());

    let pipeline = match Pipeline::build(spec, registry, opts) {
        Ok(pipeline) => pipeline,
        Err(errs) => {
            for e in &errs {
                tracing::error!(error = %e, "pipeline build error");
            }
            return ExitStatus::for_errors(&errs).into();
        }
    };

    let handle = pipeline.handle();
    let shutdown = Shutdown::new();

    // Set up signal handling
    let shutdown_clone = shutdown.clone();
    let handle_clone = handle.clone();
    let signal_task = tokio::spawn(async move {
        // Handle first signal
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(sig) => sig,
                Err(e) => {
                    tracing::error!(error = %e, "failed to create SIGTERM listener");
                    return;
                }
            };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received SIGINT");
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM");
            }
        }
        shutdown_clone.trigger();

        // Handle second signal
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("received second SIGINT, forcing shutdown");
            }
            _ = sigterm.recv() => {
                tracing::warn!("received second SIGTERM, forcing shutdown");
            }
        }
        handle_clone.force_shutdown();
    });

    // Spawn admin server if requested
    let admin_task = if let Some(bind) = admin_bind {
        let bind_addr: std::net::SocketAddr = match bind.parse() {
            Ok(addr) => addr,
            Err(e) => {
                let _ = writeln!(
                    std::io::stderr(),
                    "invalid admin bind address `{bind}`: {e}"
                );
                return Exit::InvalidConfig;
            }
        };

        let token = if let Some(token_path) = admin_token_file {
            match tokio::fs::read_to_string(token_path).await {
                Ok(content) => Some(content.trim().to_string()),
                Err(e) => {
                    let _ = writeln!(
                        std::io::stderr(),
                        "cannot read admin token file `{}`: {e}",
                        token_path.display()
                    );
                    return Exit::RuntimeFailure;
                }
            }
        } else {
            None
        };

        let admin_config = AdminConfig {
            bind: bind_addr,
            token,
        };

        let metrics = Arc::new(PrometheusMetrics::new());
        let admin_handle = handle.clone();
        let admin_shutdown = shutdown.clone();

        Some(tokio::spawn(async move {
            if let Err(e) =
                thadam_runtime::serve(admin_config, admin_handle, metrics, admin_shutdown).await
            {
                tracing::error!(error = %e, "admin server error");
            }
        }))
    } else {
        None
    };

    // Run the pipeline
    let run_result = pipeline.run(shutdown.clone()).await;

    // Cancel signal handler
    signal_task.abort();

    // Cancel admin server if running
    if let Some(task) = admin_task {
        task.abort();
    }

    // Map the result to an exit code
    match run_result {
        Ok(report) => report.exit.into(),
        Err(e) => {
            tracing::error!(error = %e, "pipeline run failed");
            ExitStatus::for_error(&e).into()
        }
    }
}

impl From<ExitStatus> for Exit {
    fn from(status: ExitStatus) -> Self {
        match status {
            ExitStatus::Ok => Exit::Ok,
            ExitStatus::RuntimeFailure => Exit::RuntimeFailure,
            ExitStatus::InvalidConfig => Exit::InvalidConfig,
            ExitStatus::DataDir => Exit::DataDir,
            ExitStatus::Interrupted => Exit::Interrupted,
            _ => Exit::RuntimeFailure,
        }
    }
}
