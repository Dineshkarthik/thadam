import time

import pytest
import thadam

from conftest import PipelineThread, e2e_config, post  # noqa: F401


@pytest.mark.usefixtures("e2e_config")
def test_pipeline_runs_and_stops(e2e_config):  # noqa: F811
    config, _url, _data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    assert pipeline.stopping is False

    handle = PipelineThread(pipeline)
    handle.start()
    time.sleep(0.5)
    pipeline.stop()
    assert pipeline.stopping is True

    result = handle.join()
    assert result.error is None
    assert result.code == 0


def test_stop_before_run_shuts_down_immediately(e2e_config):  # noqa: F811
    config, _url, _data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    pipeline.stop()
    handle = PipelineThread(pipeline)
    handle.start()
    result = handle.join()
    assert result.error is None
    assert result.code == 0


def test_run_twice_raises(e2e_config):  # noqa: F811
    config, _url, _data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    pipeline.stop()
    handle = PipelineThread(pipeline)
    handle.start()
    result = handle.join()
    assert result.error is None

    with pytest.raises(RuntimeError, match="already ran"):
        pipeline.run()


def test_timeout_triggers_graceful_shutdown(e2e_config):  # noqa: F811
    config, _url, data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    started = time.monotonic()
    code = pipeline.run(timeout=0.3)
    elapsed = time.monotonic() - started
    assert code == 0
    assert elapsed < 30
    assert data_dir.exists()


def test_invalid_timeout_raises(e2e_config):  # noqa: F811
    config, _url, _data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    with pytest.raises(ValueError):
        pipeline.run(timeout=-1.0)


def test_invalid_config_raises_at_construction(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("version: 1\npipeline: { name: x }\n")
    with pytest.raises(thadam.ThadamError):
        thadam.Pipeline(str(bad))


def test_gil_released_during_run(e2e_config):  # noqa: F811
    """While one thread blocks in run(), another reaches stop()."""
    config, url, _data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    handle = PipelineThread(pipeline)
    handle.start()

    # If run() held the GIL, this POST could never be sent.
    assert post(url, {"ts": 1759968000000, "temp": 21.5, "level": "ok"}) == 204
    pipeline.stop()
    result = handle.join()
    assert result.error is None
    assert result.code == 0
