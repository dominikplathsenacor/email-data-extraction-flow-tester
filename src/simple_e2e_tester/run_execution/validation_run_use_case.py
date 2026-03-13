"""Run execution use-case service."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from simple_e2e_tester.configuration import ConfigurationError, load_configuration
from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.email_sending.email_dispatch import (
    EmailCompositionError,
    ExpectedEventDispatcher,
    SynchronousSMTPClient,
    validate_attachments_for_testcases,
)
from simple_e2e_tester.kafka_consumption.actual_event_reader import (
    ActualEventDecodeError,
    ActualEventReader,
)
from simple_e2e_tester.matching_validation import match_and_validate
from simple_e2e_tester.matching_validation.event_boundary_mappers import (
    to_actual_events,
    to_expected_events,
)
from simple_e2e_tester.matching_validation.matching_outcomes import (
    MatchValidationResult,
)
from simple_e2e_tester.rest_execution.rest_transport import (
    RequestsRestRequestClient,
    RestExecutionTransport,
)
from simple_e2e_tester.results_writing import RunMetadata, write_results_workbook
from simple_e2e_tester.schema_management import (
    SchemaError,
    flatten_schema,
    load_schema_document,
)
from simple_e2e_tester.template_ingestion.workbook_reader import (
    TemplateValidationError,
    read_template,
)

from .run_contracts import (
    RunArtifacts,
    RunOutcome,
    RunRequest,
    TransportExecutionResult,
)


class RunExecutionError(Exception):
    """Raised when a run use case cannot be completed."""


@dataclass(frozen=True)
class _RunExecution:
    """Execution details used to write run outputs."""

    send_status_by_test_id: dict[str, SendStatus]
    sent_ok: int
    match_result: MatchValidationResult


class ExecutionTransport(Protocol):  # pylint: disable=too-few-public-methods
    """Transport boundary for collecting actual events for a run."""

    def execute(
        self, *, artifacts: RunArtifacts, run_start: datetime
    ) -> TransportExecutionResult:
        """Execute one run through a concrete transport."""


class EmailKafkaExecutionTransport:  # pylint: disable=too-few-public-methods
    """Default execution transport for the email->kafka flow."""

    def __init__(
        self,
        *,
        email_sender_cls,
        kafka_service_cls,
        smtp_client_factory: Callable[[], SynchronousSMTPClient],
    ) -> None:
        self._email_sender_cls = email_sender_cls
        self._kafka_service_cls = kafka_service_cls
        self._smtp_client_factory = smtp_client_factory

    def execute(
        self, *, artifacts: RunArtifacts, run_start: datetime
    ) -> TransportExecutionResult:
        kafka_service = _build_kafka_service_for_email_kafka(
            artifacts=artifacts,
            kafka_service_cls=self._kafka_service_cls,
        )
        sender = self._email_sender_cls(
            smtp_client=self._smtp_client_factory(),
            smtp_settings=artifacts.configuration.smtp,
            mail_settings=artifacts.configuration.mail,
            attachments_base=artifacts.attachments_base,
        )
        kafka_future: Future[list] | None = None
        enabled_testcases = [
            testcase for testcase in artifacts.testcases if testcase.enabled
        ]
        expected_events_for_stop = to_expected_events(enabled_testcases)
        with ThreadPoolExecutor(max_workers=1) as executor:
            if kafka_service and enabled_testcases:
                kafka_future = executor.submit(
                    _read_actual_event_messages,
                    kafka_service,
                    run_start,
                    expected_events_for_stop,
                    artifacts.configuration.matching,
                    artifacts.fields,
                )
            send_results = sender.send_all(artifacts.testcases)
            kafka_messages = kafka_future.result() if kafka_future else []
        send_status_by_test_id = {
            result.test_id: result.status for result in send_results
        }
        sent_ok = sum(1 for result in send_results if result.status == SendStatus.SENT)
        return TransportExecutionResult(
            send_status_by_test_id=send_status_by_test_id,
            sent_ok=sent_ok,
            actual_messages=tuple(kafka_messages),
        )


# pylint: disable=too-many-arguments,too-many-locals
def execute_email_kafka_validation_run(
    request: RunRequest,
    *,
    email_sender_cls=None,
    kafka_service_cls=None,
    smtp_client_factory: Callable[[], SynchronousSMTPClient] | None = None,
    execution_transport: ExecutionTransport | None = None,
    rest_transport: RestExecutionTransport | None = None,
) -> RunOutcome:
    """Execute one full email-kafka validation run and return run outcome."""
    resolved_email_sender_cls = email_sender_cls or ExpectedEventDispatcher
    resolved_kafka_service_cls = kafka_service_cls or ActualEventReader
    resolved_smtp_client_factory = smtp_client_factory or SynchronousSMTPClient

    artifacts = _load_run_artifacts(request.config_path, request.input_path)
    default_execution_transport = _build_default_execution_transport(
        configuration=artifacts.configuration,
        email_sender_cls=resolved_email_sender_cls,
        kafka_service_cls=resolved_kafka_service_cls,
        smtp_client_factory=resolved_smtp_client_factory,
        rest_transport=rest_transport,
    )
    selected_execution_transport = execution_transport or default_execution_transport
    run_start = datetime.now(UTC)
    execution = (
        _execute_dry_run(artifacts.testcases)
        if request.dry_run
        else _execute_live_run(
            artifacts=artifacts,
            run_start=run_start,
            execution_transport=selected_execution_transport,
        )
    )

    output_path = _resolve_output_path(request.input_path, request.output_dir)
    run_metadata = RunMetadata(
        run_start=run_start,
        input_path=Path(request.input_path).resolve(),
        output_path=output_path.resolve(),
        kafka_topic=artifacts.configuration.kafka.topic,
        timeout_seconds=artifacts.configuration.kafka.timeout_seconds,
        sent_ok=execution.sent_ok,
    )
    write_results_workbook(
        template_path=request.input_path,
        output_path=output_path,
        schema_config=artifacts.configuration.schema,
        schema_fields=artifacts.fields,
        testcases=artifacts.testcases,
        match_result=execution.match_result,
        run_metadata=run_metadata,
        send_status_by_test_id=execution.send_status_by_test_id,
    )
    return RunOutcome(
        output_path=output_path.resolve(),
        sent_ok=execution.sent_ok,
        dry_run=request.dry_run,
    )


# pylint: enable=too-many-arguments,too-many-locals


def _build_default_execution_transport(
    *,
    configuration,
    email_sender_cls,
    kafka_service_cls,
    smtp_client_factory: Callable[[], SynchronousSMTPClient],
    rest_transport: RestExecutionTransport | None,
) -> ExecutionTransport:
    if configuration.transport.mode == "rest":
        if rest_transport is not None:
            return rest_transport
        return RestExecutionTransport(RequestsRestRequestClient())
    return EmailKafkaExecutionTransport(
        email_sender_cls=email_sender_cls,
        kafka_service_cls=kafka_service_cls,
        smtp_client_factory=smtp_client_factory,
    )


def _build_kafka_service_for_email_kafka(
    *,
    artifacts: RunArtifacts,
    kafka_service_cls,
):
    try:
        validate_attachments_for_testcases(
            artifacts.testcases,
            attachments_base=artifacts.attachments_base,
        )
        return kafka_service_cls(
            kafka_settings=artifacts.configuration.kafka,
            schema_fields=artifacts.fields,
            schema_config=artifacts.configuration.schema,
        )
    except (EmailCompositionError, ActualEventDecodeError) as exc:
        raise RunExecutionError(str(exc)) from exc


def _resolve_output_path(input_path: str, output_dir: str | None) -> Path:
    input_file = Path(input_path)
    destination = Path(output_dir) if output_dir else input_file.parent
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return destination / f"{input_file.stem}-results-{timestamp}.xlsx"


def _load_run_artifacts(config_path: str, input_path: str) -> RunArtifacts:
    try:
        configuration = load_configuration(config_path)
        fields = flatten_schema(load_schema_document(configuration.schema))
        testcases = read_template(
            input_path, [field.path for field in fields]
        ).testcases
    except (
        ConfigurationError,
        SchemaError,
        TemplateValidationError,
        OSError,
        ValueError,
    ) as exc:
        raise RunExecutionError(str(exc)) from exc
    return RunArtifacts(
        configuration=configuration,
        fields=tuple(fields),
        testcases=testcases,
        attachments_base=Path(input_path).resolve().parent,
    )


def _execute_dry_run(testcases) -> _RunExecution:
    send_status_by_test_id = {
        testcase.test_id: SendStatus.SKIPPED
        for testcase in testcases
        if testcase.enabled
    }
    expected_events = to_expected_events(testcases)
    match_result = MatchValidationResult(
        matches=(),
        conflicts=(),
        unmatched_actual_events=(),
        unmatched_expected_event_ids=tuple(
            expected_event.expected_event_id
            for expected_event in expected_events
            if expected_event.enabled
        ),
    )
    return _RunExecution(
        send_status_by_test_id=send_status_by_test_id,
        sent_ok=0,
        match_result=match_result,
    )


def _execute_live_run(
    *,
    artifacts: RunArtifacts,
    run_start: datetime,
    execution_transport: ExecutionTransport,
) -> _RunExecution:
    transport_result = execution_transport.execute(
        artifacts=artifacts, run_start=run_start
    )
    send_status_by_test_id = transport_result.send_status_by_test_id
    sent_ok = transport_result.sent_ok
    kafka_messages = list(transport_result.actual_messages)
    sent_testcases = [
        testcase
        for testcase in artifacts.testcases
        if send_status_by_test_id.get(testcase.test_id) == SendStatus.SENT
    ]
    expected_events = to_expected_events(sent_testcases)
    actual_events = to_actual_events(kafka_messages)
    match_result = match_and_validate(
        expected_events,
        actual_events,
        artifacts.configuration.matching,
        artifacts.fields,
    )
    return _RunExecution(
        send_status_by_test_id=send_status_by_test_id,
        sent_ok=sent_ok,
        match_result=match_result,
    )


def _read_actual_event_messages(
    kafka_service,
    run_start: datetime,
    expected_events,
    matching_config,
    schema_fields,
) -> list:
    consumed_messages = []
    for message in kafka_service.consume_from(run_start):
        consumed_messages.append(message)
        if _all_enabled_expected_events_matched(
            expected_events,
            consumed_messages,
            matching_config,
            schema_fields,
        ):
            break
    return consumed_messages


def _all_enabled_expected_events_matched(
    expected_events,
    kafka_messages,
    matching_config,
    schema_fields,
) -> bool:
    if not any(expected_event.enabled for expected_event in expected_events):
        return True
    current_result = match_and_validate(
        expected_events,
        to_actual_events(kafka_messages),
        matching_config,
        schema_fields,
    )
    return len(current_result.unmatched_expected_event_ids) == 0
