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
use crate::{LogFormatArg, RunArgs, registry};

/// Run a pipeline configuration.
///
/// This function:
/// 1. Builds the connector registry
/// 2. Loads and validates the configuration file
/// 3. Initializes logging
/// 4. Builds a Tokio multi-thread runtime
/// 5. Builds and runs the pipeline with signal handling
pub fn run(args: &RunArgs, err: &mut dyn Write) -> Exit {
    // 1. Build the registry
    let registry = registry::build_registry();

    // 2. Load the configuration file
    let path = args.path.as_path();
    let spec = match load_file(path, &ProcessEnv) {
        Ok(spec) => spec,
        Err(report) => {
            let _ = write!(err, "{}", report.render(&path.display().to_string()));
            return Exit::InvalidConfig;
        }
    };

    // 3. Initialize logging. The `--log-format` flag wins; without it the
    // config's `observability.logs.format` applies.
    let log_format = args
        .log_format
        .map(LogFormatArg::into_runtime_format)
        .unwrap_or(match spec.observability.logs.format {
            thadam_config::LogFormat::Json => thadam_runtime::LogFormat::Json,
            _ => thadam_runtime::LogFormat::Pretty,
        });
    // The guard must stay alive until the run ends: dropping it closes the
    // non-blocking writer's channel and every later record is lost.
    let _logging_guard = match init_logging(log_format, &args.log_level) {
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

    // The admin API settings come from `observability.admin`; `--admin-bind`
    // and `--admin-token-file` override them for this run. The section's
    // schema default serves the admin API on loopback.
    let admin_bind = args
        .admin_bind
        .clone()
        .unwrap_or_else(|| spec.observability.admin.bind.clone());
    let admin_token_file = args
        .admin_token_file
        .clone()
        .or_else(|| spec.observability.admin.token_file.clone());

    // 5. Run the pipeline inside the Tokio runtime
    runtime.block_on(async {
        run_pipeline(
            &registry,
            spec,
            args.repair,
            &admin_bind,
            admin_token_file.as_deref(),
            args.exit_when_sources_finish,
        )
        .await
    })
}

async fn run_pipeline(
    registry: &thadam_runtime::Registry,
    spec: thadam_config::PipelineSpec,
    repair: bool,
    admin_bind: &str,
    admin_token_file: Option<&Path>,
    exit_when_sources_finish: bool,
) -> Exit {
    // Build the pipeline - use a mutable options object
    let mut opts = RuntimeOptions::default();
    opts.repair = repair;
    opts.stop_when_sources_finish = exit_when_sources_finish;
    let metrics = Arc::new(PrometheusMetrics::new());
    opts.metrics = metrics.clone();

    let pipeline = match Pipeline::build(spec, registry, opts) {
        Ok(pipeline) => pipeline,
        Err(errs) => {
            for e in &errs {
                tracing::error!(error = %e, "pipeline build error");
                eprintln!("error: pipeline build failed: {e}");
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

    // Serve the admin API for the whole run; the bind and token were resolved
    // from `observability.admin` with any CLI overrides applied by the caller.
    let bind_addr: std::net::SocketAddr = match admin_bind.parse() {
        Ok(addr) => addr,
        Err(e) => {
            let _ = writeln!(
                std::io::stderr(),
                "invalid admin bind address `{admin_bind}`: {e}"
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

    let admin_metrics = metrics.clone();
    let admin_handle = handle.clone();
    let admin_shutdown = shutdown.clone();
    let admin_task = tokio::spawn(async move {
        if let Err(e) =
            thadam_runtime::serve(admin_config, admin_handle, admin_metrics, admin_shutdown).await
        {
            tracing::error!(error = %e, "admin server error");
        }
    });

    // Run the pipeline
    let run_result = pipeline.run(shutdown.clone()).await;

    // Cancel signal handler
    signal_task.abort();

    // Cancel the admin server
    admin_task.abort();

    // Map the result to an exit code
    match run_result {
        Ok(report) => report.exit.into(),
        Err(e) => {
            tracing::error!(error = %e, "pipeline run failed");
            eprintln!("error: pipeline run failed: {e}");
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
