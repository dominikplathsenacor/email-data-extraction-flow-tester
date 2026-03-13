"""Tests for run execution domain entities."""

from __future__ import annotations

from pathlib import Path

from simple_e2e_tester.configuration.runtime_settings import Configuration
from simple_e2e_tester.run_execution.run_contracts import (
    RunArtifacts,
    RunOutcome,
    RunRequest,
)
from simple_e2e_tester.schema_management.schema_models import FlattenedField
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase


def test_run_request_defaults_to_live_mode() -> None:
    request = RunRequest(
        config_path="config.json",
        input_path="input.xlsx",
        output_dir=None,
    )

    assert request.dry_run is False


def test_run_outcome_contains_resolved_output_path_and_sent_count() -> None:
    outcome = RunOutcome(
        output_path=Path("/tmp/results.xlsx"),
        sent_ok=2,
        dry_run=False,
    )

    assert outcome.output_path.name == "results.xlsx"
    assert outcome.sent_ok == 2
    assert outcome.dry_run is False


def test_run_artifacts_groups_configuration_fields_and_cases() -> None:
    artifacts = RunArtifacts(
        configuration=Configuration.__new__(Configuration),
        fields=(FlattenedField(path="sender", definition={"type": "string"}),),
        testcases=(TemplateTestCase.__new__(TemplateTestCase),),
        attachments_base=Path("/tmp"),
    )

    assert len(artifacts.fields) == 1
    assert len(artifacts.testcases) == 1
    assert artifacts.attachments_base == Path("/tmp")
