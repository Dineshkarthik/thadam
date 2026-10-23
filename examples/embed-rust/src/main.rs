use std::path::PathBuf;
use std::process::ExitCode;

use thadam_config::{ProcessEnv, load_file};
use thadam_core::Shutdown;
use thadam_mqtt::MqttSourceFactory;
use thadam_runtime::{ExitStatus, LogFormat, Pipeline, Registry, RuntimeOptions, init_logging};
use thadam_segments::{ParquetSegmentSinkFactory, QuarantineSinkFactory};

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: thadam-embed-example <pipeline.yaml>");
        return ExitCode::from(ExitStatus::InvalidConfig.code());
    };
    let spec = match load_file(&path, &ProcessEnv) {
        Ok(spec) => spec,
        Err(report) => {
            eprint!("{}", report.render(&path.display().to_string()));
            return ExitCode::from(ExitStatus::InvalidConfig.code());
        }
    };
    // Keep the guard alive for the whole run so the non-blocking log writer stays open.
    let _logging = init_logging(LogFormat::Pretty, "info").ok();

    let mut registry = Registry::new();
    registry.source(Box::new(MqttSourceFactory::new()));
    registry.segment_sink(Box::new(ParquetSegmentSinkFactory::new()));
    registry.quarantine_sink(Box::new(QuarantineSinkFactory::new()));

    let pipeline = match Pipeline::build(spec, &registry, RuntimeOptions::default()) {
        Ok(pipeline) => pipeline,
        Err(errs) => {
            for e in &errs {
                eprintln!("error: pipeline build failed: {e}");
            }
            return ExitCode::from(ExitStatus::for_errors(&errs).code());
        }
    };

    let shutdown = Shutdown::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop.trigger();
        }
    });

    let status = match pipeline.run(shutdown).await {
        Ok(report) => report.exit,
        Err(e) => {
            eprintln!("error: pipeline run failed: {e}");
            ExitStatus::for_error(&e)
        }
    };
    ExitCode::from(status.code())
}
