"""Matching and validation service tests."""

from __future__ import annotations

from datetime import UTC, datetime

from simple_e2e_tester.configuration.runtime_settings import MatchingConfig
from simple_e2e_tester.kafka_consumption.actual_event_messages import ActualEventMessage
from simple_e2e_tester.matching_validation.case_evaluator import match_and_validate
from simple_e2e_tester.matching_validation.event_boundary_mappers import (
    to_actual_events,
    to_expected_events,
)
from simple_e2e_tester.schema_management.schema_models import FlattenedField
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase


def _testcase(
    *,
    test_id: str,
    from_address: str = "sender@example.com",
    subject: str = "Subject",
    enabled: bool = True,
    expected_values: dict[str, object] | None = None,
) -> TemplateTestCase:
    return TemplateTestCase(
        row_number=3,
        test_id=test_id,
        tags=(),
        enabled=enabled,
        notes="",
        from_address=from_address,
        subject=subject,
        body="",
        attachment="",
        expected_values=expected_values or {},
    )


def _message(flattened: dict[str, object]) -> ActualEventMessage:
    return ActualEventMessage(
        key=None,
        value=flattened,
        timestamp=datetime.now(UTC),
        flattened=flattened,
    )


def _matching_config(
    from_field: str = "sender", subject_field: str = "subject"
) -> MatchingConfig:
    return MatchingConfig(from_field=from_field, subject_field=subject_field)


def _fields(*entries: tuple[str, object]) -> list[FlattenedField]:
    return [
        FlattenedField(path=path, definition=definition) for path, definition in entries
    ]


def _evaluate(
    testcases: list[TemplateTestCase],
    messages: list[ActualEventMessage],
    schema_fields: list[FlattenedField],
    validation_field_names: tuple[str, ...] | None = None,
):
    return match_and_validate(
        to_expected_events(testcases),
        to_actual_events(messages),
        _matching_config(),
        schema_fields,
        validation_field_names,
    )


def test_matches_unique_sender_candidate() -> None:
    testcases = [_testcase(test_id="TC-1", expected_values={"result": "OK"})]
    messages = [
        _message({"sender": "sender@example.com", "subject": "Other", "result": "OK"})
    ]
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("result", {"type": "string"}),
    )

    result = _evaluate(testcases, messages, schema_fields)

    assert len(result.matches) == 1
    assert result.matches[0].expected_event.expected_event_id == "TC-1"
    assert result.matches[0].mismatches == ()
    assert result.conflicts == ()
    assert result.unmatched_actual_events == ()
    assert result.unmatched_expected_event_ids == ()


def test_uses_subject_to_disambiguate_sender_candidates() -> None:
    testcases = [
        _testcase(test_id="TC-1", subject="Subject A"),
        _testcase(test_id="TC-2", subject="Subject B"),
    ]
    messages = [_message({"sender": "sender@example.com", "subject": "Subject B"})]
    schema_fields = _fields(
        ("sender", {"type": "string"}), ("subject", {"type": "string"})
    )

    result = _evaluate(testcases, messages, schema_fields)

    assert len(result.matches) == 1
    assert result.matches[0].expected_event.expected_event_id == "TC-2"
    assert result.conflicts == ()


def test_records_conflict_when_sender_candidates_do_not_resolve() -> None:
    testcases = [
        _testcase(test_id="TC-1", subject="Subject A"),
        _testcase(test_id="TC-2", subject="Subject B"),
    ]
    messages = [_message({"sender": "sender@example.com", "subject": "Subject Z"})]
    schema_fields = _fields(
        ("sender", {"type": "string"}), ("subject", {"type": "string"})
    )

    result = _evaluate(testcases, messages, schema_fields)

    assert result.matches == ()
    assert len(result.conflicts) == 1
    assert result.conflicts[0].candidate_expected_event_ids == ("TC-1", "TC-2")
    assert result.unmatched_actual_events == ()
    assert result.unmatched_expected_event_ids == ("TC-1", "TC-2")


def test_keeps_unmatched_actual_events() -> None:
    testcases = [_testcase(test_id="TC-1")]
    messages = [_message({"sender": "other@example.com", "subject": "Subject"})]
    schema_fields = _fields(
        ("sender", {"type": "string"}), ("subject", {"type": "string"})
    )

    result = _evaluate(testcases, messages, schema_fields)

    assert result.matches == ()
    assert result.conflicts == ()
    assert len(result.unmatched_actual_events) == 1
    assert result.unmatched_expected_event_ids == ("TC-1",)


def test_allows_multiple_messages_for_same_testcase() -> None:
    testcases = [_testcase(test_id="TC-1")]
    messages = [
        _message({"sender": "sender@example.com", "subject": "one"}),
        _message({"sender": "sender@example.com", "subject": "two"}),
    ]
    schema_fields = _fields(
        ("sender", {"type": "string"}), ("subject", {"type": "string"})
    )

    result = _evaluate(testcases, messages, schema_fields)

    assert len(result.matches) == 2
    assert [match.expected_event.expected_event_id for match in result.matches] == [
        "TC-1",
        "TC-1",
    ]
    assert result.unmatched_expected_event_ids == ()


def test_ignores_empty_expected_values_for_validation() -> None:
    testcase = _testcase(
        test_id="TC-1", expected_values={"score": None, "comment": "  "}
    )
    message = _message(
        {
            "sender": "sender@example.com",
            "subject": "Subject",
            "score": 1.23,
            "comment": "x",
        }
    )
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("score", {"type": "number"}),
        ("comment", {"type": "string"}),
    )

    result = _evaluate([testcase], [message], schema_fields)

    assert result.matches[0].mismatches == ()


def test_enforces_muss_leer_sein() -> None:
    testcase = _testcase(test_id="TC-1", expected_values={"comment": "MUSS_LEER_SEIN"})
    message = _message(
        {"sender": "sender@example.com", "subject": "Subject", "comment": "not empty"}
    )
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("comment", {"type": "string"}),
    )

    result = _evaluate([testcase], [message], schema_fields)

    assert len(result.matches[0].mismatches) == 1
    mismatch = result.matches[0].mismatches[0]
    assert mismatch.field == "comment"
    assert mismatch.expected == "MUSS_LEER_SEIN"
    assert mismatch.actual == "not empty"


def test_float_tolerance_supports_german_decimal_comma() -> None:
    testcase = _testcase(test_id="TC-1", expected_values={"score": "3,14+-0,2"})
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("score", {"type": "number"}),
    )
    pass_message = _message(
        {"sender": "sender@example.com", "subject": "Subject", "score": 3.30}
    )
    fail_message = _message(
        {"sender": "sender@example.com", "subject": "Subject", "score": 3.50}
    )

    pass_result = _evaluate([testcase], [pass_message], schema_fields)
    fail_result = _evaluate([testcase], [fail_message], schema_fields)

    assert pass_result.matches[0].mismatches == ()
    assert len(fail_result.matches[0].mismatches) == 1


def test_float_tolerance_supports_upper_and_lower_bounds() -> None:
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("score", {"type": "number"}),
    )
    upper = _testcase(test_id="TC-UP", expected_values={"score": "3,14+0,1"})
    lower = _testcase(test_id="TC-LOW", expected_values={"score": "3,14-0,1"})

    upper_ok = _message(
        {"sender": "sender@example.com", "subject": "Subject", "score": 3.24}
    )
    upper_fail = _message(
        {"sender": "sender@example.com", "subject": "Subject", "score": 3.25}
    )
    lower_ok = _message(
        {"sender": "sender@example.com", "subject": "Subject", "score": 3.05}
    )
    lower_fail = _message(
        {"sender": "sender@example.com", "subject": "Subject", "score": 3.03}
    )

    assert _evaluate([upper], [upper_ok], schema_fields).matches[0].mismatches == ()
    assert (
        len(_evaluate([upper], [upper_fail], schema_fields).matches[0].mismatches) == 1
    )
    assert _evaluate([lower], [lower_ok], schema_fields).matches[0].mismatches == ()
    assert (
        len(_evaluate([lower], [lower_fail], schema_fields).matches[0].mismatches) == 1
    )


def test_validates_only_configured_field_subset() -> None:
    testcase = _testcase(
        test_id="TC-1",
        expected_values={
            "score": 1.58,
            "comment": "expected comment",
        },
    )
    message = _message(
        {
            "sender": "sender@example.com",
            "subject": "Subject",
            "score": 1.58,
            "comment": "different actual comment",
        }
    )
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("score", {"type": "number"}),
        ("comment", {"type": "string"}),
    )

    result = _evaluate(
        [testcase],
        [message],
        schema_fields,
        validation_field_names=("score",),
    )

    assert result.matches[0].mismatches == ()


def test_exact_validation_trims_whitespace_and_compares_integers_numerically() -> None:
    testcase = _testcase(
        test_id="TC-1",
        expected_values={
            "comment": "  hello  ",
            "attempts": 5,
        },
    )
    message = _message(
        {
            "sender": "sender@example.com",
            "subject": "Subject",
            "comment": "hello",
            "attempts": "5",
        }
    )
    schema_fields = _fields(
        ("sender", {"type": "string"}),
        ("subject", {"type": "string"}),
        ("comment", {"type": "string"}),
        ("attempts", {"type": "integer"}),
    )

    result = _evaluate([testcase], [message], schema_fields)

    assert result.matches[0].mismatches == ()
