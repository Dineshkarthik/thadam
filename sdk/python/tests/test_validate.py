import thadam

from conftest import FIXTURES


def test_valid_minimal_has_no_errors():
    diagnostics = thadam.validate(str(FIXTURES / "valid" / "minimal.yaml"))
    assert all(d.severity == "warning" for d in diagnostics), diagnostics


def test_invalid_fixtures_report_errors():
    for fixture in sorted((FIXTURES / "invalid").glob("*.yaml")):
        diagnostics = thadam.validate(str(fixture))
        errors = [d for d in diagnostics if d.severity == "error"]
        assert errors, f"{fixture.name}: expected errors, got {diagnostics}"
        for d in errors:
            assert isinstance(d.code, str) and "." in d.code
            assert isinstance(d.path, str) and d.path
            assert isinstance(d.message, str) and d.message
            if d.line is not None:
                assert isinstance(d.line, int) and d.line >= 1
                assert isinstance(d.column, int) and d.column >= 1


def test_unknown_connector_kind_named_by_registry():
    diagnostics = thadam.validate(
        str(FIXTURES / "invalid" / "unknown_connector_kind.yaml")
    )
    assert any(d.code == "config.unknown_kind" for d in diagnostics)


def test_positions_and_hints_surface():
    diagnostics = thadam.validate(str(FIXTURES / "invalid" / "bad_duration.yaml"))
    assert diagnostics[0].line is not None
    assert any(d.path for d in diagnostics)


def test_warning_fixture_reports_a_warning_and_no_error():
    diagnostics = thadam.validate(str(FIXTURES / "warnings" / "log_format_text.yaml"))
    assert any(d.severity == "warning" for d in diagnostics)
    assert all(d.severity != "error" for d in diagnostics)


def test_unreadable_file_is_a_diagnostic_not_an_exception():
    diagnostics = thadam.validate("/definitely/not/here.yaml")
    assert len(diagnostics) == 1
    assert diagnostics[0].severity == "error"
    assert "cannot read" in diagnostics[0].message


def test_repr_is_useful():
    (diagnostic,) = thadam.validate("/definitely/not/here.yaml")
    text = repr(diagnostic)
    assert diagnostic.code in text
    assert diagnostic.path in text
