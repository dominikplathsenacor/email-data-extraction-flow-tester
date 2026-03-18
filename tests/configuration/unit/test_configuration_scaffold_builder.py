"""Configuration scaffold builder tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from simple_e2e_tester.configuration.config_scaffold_builder import (
    build_placeholder_configuration,
    write_placeholder_configuration,
)


def test_build_placeholder_configuration_contains_all_supported_sections() -> None:
    scaffold = build_placeholder_configuration()

    assert "Test configuration template" in scaffold
    assert "schema:" in scaffold
    assert "transport:" in scaffold
    assert "rest:" in scaffold
    assert "wait_between_calls_seconds:" in scaffold
    assert "avsc:" in scaffold
    assert "json_schema:" in scaffold
    assert "kafka_event:" in scaffold
    assert "rest_response:" in scaffold
    assert "matching:" in scaffold
    assert "validation:" in scaffold
    assert "field_names:" in scaffold
    assert "all schema-derived fields are validated" in scaffold
    assert "smtp:" in scaffold
    assert "mail:" in scaffold
    assert "kafka:" in scaffold
    assert "<REQUIRED>" in scaffold
    assert "<OPTIONAL>" in scaffold
    assert "# REST response schema" in scaffold


def test_write_placeholder_configuration_writes_file(tmp_path: Path) -> None:
    output_path = tmp_path / "config.yaml"

    written_path = write_placeholder_configuration(output_path)

    assert written_path == output_path.resolve()
    assert output_path.exists()
    assert "<REQUIRED>" in output_path.read_text(encoding="utf-8")


def test_write_placeholder_configuration_fails_when_file_exists(tmp_path: Path) -> None:
    output_path = tmp_path / "config.yaml"
    output_path.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError):
        write_placeholder_configuration(output_path)
