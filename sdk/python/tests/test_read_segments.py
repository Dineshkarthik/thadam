import pyarrow as pa
import pytest
import thadam

from conftest import PipelineThread, e2e_config, post  # noqa: F401

EVENTS = [
    {"ts": 1759968000000, "temp": 21.5, "level": "ok"},
    {"ts": 1759968001000, "temp": 22.0, "level": "ok"},
    {"ts": 1759968002000, "temp": 22.7, "level": "high"},
    {"ts": 1759968003000, "temp": 21.9, "level": "ok"},
    {"ts": 1759968004000, "temp": 23.1, "level": "high"},
]


@pytest.fixture
def written_sink(e2e_config, tmp_path):  # noqa: F811
    """Run a pipeline that captures EVENTS over HTTP, then stop it; returns
    (sink_dir, events). The Parquet sink finalizes its open segments during
    graceful shutdown."""
    config, url, data_dir = e2e_config()
    pipeline = thadam.Pipeline(str(config))
    handle = PipelineThread(pipeline)
    handle.start()
    for event in EVENTS:
        assert post(url, event) == 204
    pipeline.stop()
    result = handle.join()
    assert result.error is None
    assert result.code == 0
    return data_dir / "sinks" / "local"


def test_read_all_rows(written_sink):
    table = thadam.read_segments(str(written_sink))
    assert isinstance(table, pa.Table)
    assert table.num_rows == len(EVENTS)
    assert sorted(table.column("temp").to_pylist()) == sorted(
        e["temp"] for e in EVENTS
    )
    assert sorted(table.column("level").to_pylist()) == ["high", "high", "ok", "ok", "ok"]


def test_dataset_filter(written_sink):
    table = thadam.read_segments(str(written_sink), dataset="readings")
    assert table.num_rows == len(EVENTS)

    empty = thadam.read_segments(str(written_sink), dataset="nope")
    assert isinstance(empty, pa.Table)
    assert empty.num_rows == 0


def test_table_is_an_arrow_c_stream_table(written_sink):
    # The hand-off is the Arrow PyCapsule interface; re-importing the table
    # through the same interface round-trips it.
    table = thadam.read_segments(str(written_sink), dataset="readings")
    capsule = table.__arrow_c_stream__()
    again = pa.table(thadam.read_segments(str(written_sink), dataset="readings"))
    assert again.equals(table)
    assert capsule is not None


def test_missing_sink_dir_returns_empty_table(tmp_path):
    table = thadam.read_segments(str(tmp_path / "nothing"))
    assert isinstance(table, pa.Table)
    assert table.num_rows == 0
    assert table.num_columns == 0


def test_schema_envelope_columns_present(written_sink):
    table = thadam.read_segments(str(written_sink), dataset="readings")
    names = table.schema.names
    # Declared fields always present; envelope columns carry identity etc.
    assert "temp" in names
    assert "level" in names
