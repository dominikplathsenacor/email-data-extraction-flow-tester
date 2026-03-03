"""Run execution entities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from simple_e2e_tester.configuration.runtime_settings import Configuration
from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.kafka_consumption.actual_event_messages import ActualEventMessage
from simple_e2e_tester.schema_management.schema_models import FlattenedField
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase


@dataclass(frozen=True)
class RunRequest:
    """Input contract for executing one run."""

    config_path: str
    input_path: str
    output_dir: str | None
    dry_run: bool = False


@dataclass(frozen=True)
class RunOutcome:
    """Output contract for one completed run."""

    output_path: Path
    sent_ok: int
    dry_run: bool


@dataclass(frozen=True)
class RunArtifacts:
    """Loaded domain artifacts required during run execution."""

    configuration: Configuration
    fields: tuple[FlattenedField, ...]
    testcases: tuple[TemplateTestCase, ...]
    attachments_base: Path


@dataclass(frozen=True)
class TransportExecutionResult:
    """Boundary output returned by an execution transport."""

    send_status_by_test_id: dict[str, SendStatus]
    sent_ok: int
    actual_messages: tuple[ActualEventMessage, ...]
