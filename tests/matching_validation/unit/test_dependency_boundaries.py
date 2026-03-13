"""Boundary tests for matching_validation internal dependencies."""

from __future__ import annotations

from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def test_matching_core_does_not_import_template_or_kafka_entities() -> None:
    matching_dir = _project_root() / "src" / "simple_e2e_tester" / "matching_validation"
    core_modules = (
        matching_dir / "matching_outcomes.py",
        matching_dir / "case_evaluator.py",
    )
    forbidden_import_fragments = (
        "simple_e2e_tester.template_ingestion.testcase_models",
        "simple_e2e_tester.kafka_consumption.actual_event_messages",
    )

    for module_path in core_modules:
        text = module_path.read_text(encoding="utf-8")
        for fragment in forbidden_import_fragments:
            assert (
                fragment not in text
            ), f"Forbidden core dependency in {module_path}: {fragment}"
