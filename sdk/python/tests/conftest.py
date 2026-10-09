import json
import socket
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "crates" / "thadam-config" / "tests" / "fixtures"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def post(url: str, payload: dict, timeout: float = 15.0) -> int:
    """POST one JSON message, retrying connection errors until the pipeline's
    HTTP source is listening. Returns the response status."""
    body = json.dumps(payload).encode()
    deadline = time.monotonic() + timeout
    while True:
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=2.0) as response:
                return response.status
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, ConnectionError, OSError):
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


class RunResult:
    def __init__(self):
        self.code = None
        self.error = None


class PipelineThread:
    """Run `pipeline.run(timeout)` on a daemon thread, capturing the result."""

    def __init__(self, pipeline, timeout=None):
        self.result = RunResult()

        def target():
            try:
                self.result.code = pipeline.run(timeout=timeout)
            except BaseException as exc:  # noqa: BLE001 - surfaced in the test
                self.result.error = exc

        self._thread = threading.Thread(target=target, daemon=True)

    def start(self):
        self._thread.start()

    def join(self, timeout: float = 60.0) -> RunResult:
        self._thread.join(timeout)
        assert not self._thread.is_alive(), "pipeline.run did not return in time"
        return self.result


@pytest.fixture
def e2e_config(tmp_path):
    """Write an HTTP-source -> Parquet-sink config; returns (path, ingest_url,
    data_dir)."""

    def make() -> tuple[Path, str, Path]:
        port = free_port()
        data_dir = tmp_path / f"data-{port}"
        config = tmp_path / f"pipeline-{port}.yaml"
        config.write_text(
            f"""\
version: 1
pipeline: {{ name: sdke2e, node_id: n1, data_dir: "{data_dir}" }}
sources:
  - id: src
    kind: http
    config:
      listen:
        bind: "127.0.0.1:{port}"
        path: "/ingest"
wal: {{ max_disk: 256MiB, on_full: block }}
datasets:
  - id: readings
    match: {{ source: src }}
    decode: json
    identity: {{ device: {{ id: 100, template: "sdk" }} }}
    time: {{ field: /ts, unit: ms, domain: utc }}
    fields:
      - {{ id: 1000, name: temp, type: f64, from: /temp, required: true }}
      - {{ id: 1001, name: level, type: str, from: /level }}
sinks:
  - id: local
    kind: parquet
    config: {{ datasets: [readings] }}
"""
        )
        return config, f"http://127.0.0.1:{port}/ingest", data_dir

    return make
