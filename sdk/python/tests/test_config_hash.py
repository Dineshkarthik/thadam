import shutil

import pytest
import thadam

from conftest import FIXTURES


def test_hash_is_32_lowercase_hex():
    digest = thadam.config_hash(str(FIXTURES / "valid" / "minimal.yaml"))
    assert len(digest) == 32
    assert digest == digest.lower()
    int(digest, 16)


def test_hash_is_stable_across_calls_and_file_locations(tmp_path):
    source = FIXTURES / "valid" / "minimal.yaml"
    copy = tmp_path / "renamed.yaml"
    shutil.copy(source, copy)

    first = thadam.config_hash(str(source))
    assert thadam.config_hash(str(source)) == first
    assert thadam.config_hash(str(copy)) == first


def test_different_configurations_hash_differently():
    a = thadam.config_hash(str(FIXTURES / "valid" / "minimal.yaml"))
    b = thadam.config_hash(str(FIXTURES / "valid" / "sink_policies.yaml"))
    assert a != b


def test_invalid_config_raises_thadam_error_with_code_and_domain():
    with pytest.raises(thadam.ThadamError) as err:
        thadam.config_hash(str(FIXTURES / "invalid" / "version_missing.yaml"))
    assert err.value.code.startswith("config.")
    assert err.value.domain == "config"
    assert err.value.message


def test_missing_file_raises():
    with pytest.raises(thadam.ThadamError):
        thadam.config_hash("/definitely/not/here.yaml")
