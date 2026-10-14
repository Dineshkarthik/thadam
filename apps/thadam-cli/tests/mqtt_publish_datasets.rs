//! An `mqtt_publish` sink names its one dataset in `config.datasets`, the same
//! selector the other sinks use, in a pipeline that declares two datasets.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::process::Command;

use thadam_config::MapEnv;
use thadam_mqtt::MqttSourceFactory;
use thadam_mqtt::sink::MqttPublishSinkFactory;
use thadam_runtime::{Pipeline, Registry, RuntimeOptions};
use thadam_segments::QuarantineSinkFactory;
use thadam_testkit::TempDataDir;

fn fleet_config(data: &Path) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: fleet, node_id: n1, data_dir: "{dir}" }}
sources:
  - id: src
    kind: mqtt
    config: {{ url: "mqtt://broker:1883", client_id: "edge-01", topics: [{{ filter: "t/+/+", qos: 1 }}] }}
wal: {{ max_disk: 256MiB, on_full: block }}
datasets:
  - id: telemetry
    match: {{ source: src, topic: "t/+/telemetry" }}
    decode: json
    identity: {{ robot: {{ id: 100, template: "{{topic.1}}" }} }}
    fields:
      - {{ id: 1000, name: temp, type: f32, from: /temp }}
  - id: status
    match: {{ source: src, topic: "t/+/status" }}
    decode: json
    identity: {{ robot: {{ id: 100, template: "{{topic.1}}" }} }}
    fields:
      - {{ id: 1000, name: state, type: str, from: /state }}
sinks:
  - id: mqtt-out
    kind: mqtt_publish
    config:
      datasets: [telemetry]
      url: "mqtt://broker:1883"
      client_id: "thadam-out-01"
      topic: "thadam/{{dataset}}/{{robot}}"
"#,
        dir = data.display()
    )
}

#[test]
fn an_mqtt_publish_sink_selects_one_of_two_datasets_through_config() {
    let base = TempDataDir::new().unwrap();
    let yaml = fleet_config(&base.join("data"));
    let path = base.join("pipeline.yaml");
    std::fs::write(&path, &yaml).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_thadam"))
        .args(["validate", path.to_str().unwrap()])
        .output()
        .expect("run thadam");
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let spec = thadam_config::load_str(&yaml, &MapEnv::new())
        .unwrap_or_else(|r| panic!("{}", r.render(&yaml)));
    let mut registry = Registry::new();
    registry
        .source(Box::new(MqttSourceFactory::new()))
        .quarantine_sink(Box::new(QuarantineSinkFactory::new()))
        .batch_sink(Box::new(MqttPublishSinkFactory::new()));
    let mut opts = RuntimeOptions::default();
    opts.clock_sync_probe = None;
    Pipeline::build(spec, &registry, opts).expect("the mqtt_publish sink builds");
}
