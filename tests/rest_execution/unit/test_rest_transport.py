"""REST execution transport tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from simple_e2e_tester.configuration.runtime_settings import (
    Configuration,
    KafkaSettings,
    MailSettings,
    MatchingConfig,
    RestSettings,
    SchemaConfig,
    SMTPSettings,
    TransportSettings,
)
from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.rest_execution.rest_transport import (
    RestExecutionTransport,
    RestRequestError,
)
from simple_e2e_tester.run_execution.run_contracts import RunArtifacts
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase


class _FakeRestClient:
    def __init__(self, response_payload: dict[str, object] | None = None) -> None:
        self._response_payload = response_payload
        self.calls: list[tuple[str, str, dict[str, object], int]] = []
        self.error: Exception | None = None

    def request(
        self,
        *,
        method: str,
        url: str,
        json_payload: dict[str, object],
        timeout_seconds: int,
    ) -> dict[str, object]:
        self.calls.append((method, url, json_payload, timeout_seconds))
        if self.error is not None:
            raise self.error
        return self._response_payload or {}


def test_given_enabled_testcase_when_rest_transport_executes_then_body_is_mapped_to_dok_text(
) -> None:
    testcase = TemplateTestCase(
        row_number=3,
        test_id="TC-REST-1",
        tags=(),
        enabled=True,
        notes="",
        from_address="sender@example.com",
        subject="Subject-1",
        body="Body value",
        attachment="ignored for now",
        expected_values={},
    )
    artifacts = _build_run_artifacts((testcase,))
    client = _FakeRestClient({"sender": "sender@example.com", "subject": "Subject-1"})

    result = RestExecutionTransport(client).execute(
        artifacts=artifacts,
        run_start=datetime.now(UTC),
    )

    assert result.sent_ok == 1
    assert result.send_status_by_test_id == {"TC-REST-1": SendStatus.SENT}
    assert len(result.actual_messages) == 1
    assert result.actual_messages[0].key == "TC-REST-1"
    assert client.calls == [
        (
            "POST",
            "http://localhost:8080/extract",
            {
                "ag": "AG-1",
                "dokart": "DOKART-1",
                "dokrefuid": "DOKREF-1",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "emailabsender": "sender@example.com",
                "emailbetreff": "Subject-1",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
                "dok_text": "Body value",
            },
            30,
        )
    ]


def test_given_disabled_testcase_when_rest_transport_executes_then_request_is_skipped() -> None:
    testcase = TemplateTestCase(
        row_number=3,
        test_id="TC-REST-SKIP",
        tags=(),
        enabled=False,
        notes="",
        from_address="sender@example.com",
        subject="Subject-1",
        body="Body value",
        attachment="",
        expected_values={},
    )
    artifacts = _build_run_artifacts((testcase,))
    client = _FakeRestClient({"sender": "sender@example.com", "subject": "Subject-1"})

    result = RestExecutionTransport(client).execute(
        artifacts=artifacts,
        run_start=datetime.now(UTC),
    )

    assert result.sent_ok == 0
    assert result.send_status_by_test_id == {"TC-REST-SKIP": SendStatus.SKIPPED}
    assert result.actual_messages == ()
    assert client.calls == []


def test_given_request_error_when_rest_transport_executes_then_testcase_marked_failed() -> None:
    testcase = TemplateTestCase(
        row_number=3,
        test_id="TC-REST-FAIL",
        tags=(),
        enabled=True,
        notes="",
        from_address="sender@example.com",
        subject="Subject-1",
        body="Body value",
        attachment="",
        expected_values={},
    )
    artifacts = _build_run_artifacts((testcase,))
    client = _FakeRestClient()
    client.error = RestRequestError("request failed")

    result = RestExecutionTransport(client).execute(
        artifacts=artifacts,
        run_start=datetime.now(UTC),
    )

    assert result.sent_ok == 0
    assert result.send_status_by_test_id == {"TC-REST-FAIL": SendStatus.FAILED}
    assert result.actual_messages == ()


def test_given_unexpected_client_exception_when_rest_transport_executes_then_error_is_propagated(
) -> None:
    testcase = TemplateTestCase(
        row_number=3,
        test_id="TC-REST-ERR",
        tags=(),
        enabled=True,
        notes="",
        from_address="sender@example.com",
        subject="Subject-1",
        body="Body value",
        attachment="",
        expected_values={},
    )
    artifacts = _build_run_artifacts((testcase,))
    client = _FakeRestClient()
    client.error = ValueError("unexpected")

    try:
        RestExecutionTransport(client).execute(
            artifacts=artifacts,
            run_start=datetime.now(UTC),
        )
    except ValueError as exc:
        assert str(exc) == "unexpected"
    else:
        raise AssertionError("expected ValueError to be propagated")


def test_given_three_consecutive_request_failures_when_rest_transport_executes_then_it_aborts_early(
) -> None:
    testcases = (
        TemplateTestCase(
            row_number=3,
            test_id="TC-1",
            tags=(),
            enabled=True,
            notes="",
            from_address="sender@example.com",
            subject="Subject-1",
            body="Body value",
            attachment="",
            expected_values={},
        ),
        TemplateTestCase(
            row_number=4,
            test_id="TC-2",
            tags=(),
            enabled=True,
            notes="",
            from_address="sender@example.com",
            subject="Subject-2",
            body="Body value",
            attachment="",
            expected_values={},
        ),
        TemplateTestCase(
            row_number=5,
            test_id="TC-3",
            tags=(),
            enabled=True,
            notes="",
            from_address="sender@example.com",
            subject="Subject-3",
            body="Body value",
            attachment="",
            expected_values={},
        ),
        TemplateTestCase(
            row_number=6,
            test_id="TC-4",
            tags=(),
            enabled=True,
            notes="",
            from_address="sender@example.com",
            subject="Subject-4",
            body="Body value",
            attachment="",
            expected_values={},
        ),
    )
    artifacts = _build_run_artifacts(testcases)
    client = _FakeRestClient()
    client.error = RestRequestError("request failed")

    result = RestExecutionTransport(client).execute(
        artifacts=artifacts,
        run_start=datetime.now(UTC),
    )

    assert result.sent_ok == 0
    assert len(client.calls) == 3
    assert result.send_status_by_test_id == {
        "TC-1": SendStatus.FAILED,
        "TC-2": SendStatus.FAILED,
        "TC-3": SendStatus.FAILED,
        "TC-4": SendStatus.SKIPPED,
    }


def _build_run_artifacts(testcases: tuple[TemplateTestCase, ...]) -> RunArtifacts:
    schema = SchemaConfig(schema_type="json_schema", text='{"type":"object"}', source_path=None)
    configuration = Configuration(
        path=Path("/tmp/config.yaml"),
        schema=schema,
        response_schema=schema,
        kafka_event_schema=None,
        transport=TransportSettings(mode="rest"),
        matching=MatchingConfig(from_field="sender", subject_field="subject"),
        smtp=SMTPSettings(
            host="smtp.example.com",
            port=25,
            username=None,
            password=None,
            use_starttls=True,
            use_ssl=False,
            timeout_seconds=30,
            parallelism=4,
        ),
        mail=MailSettings(to_address="qa@example.com", cc=(), bcc=()),
        kafka=KafkaSettings(
            bootstrap_servers=("localhost:9092",),
            topic="result-topic",
            group_id=None,
            security={},
            timeout_seconds=600,
            poll_interval_ms=500,
            auto_offset_reset="latest",
        ),
        rest=RestSettings(
            base_url="http://localhost:8080",
            path="/extract",
            method="POST",
            timeout_seconds=30,
            retry_count=2,
            retry_backoff_ms=250,
            defaults={
                "ag": "AG-1",
                "dokart": "DOKART-1",
                "dokrefuid": "DOKREF-1",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
            },
        ),
    )
    return RunArtifacts(
        configuration=configuration,
        fields=(),
        testcases=testcases,
        attachments_base=Path("/tmp"),
    )
