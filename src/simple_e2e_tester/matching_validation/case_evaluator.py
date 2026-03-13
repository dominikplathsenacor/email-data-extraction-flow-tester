"""Matching and expected-vs-actual validation service."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum

from simple_e2e_tester.configuration.runtime_settings import MatchingConfig
from simple_e2e_tester.schema_management.schema_models import FlattenedField

from .expectation_rules import ExpectationRuleKind, parse_expectation_rule
from .matching_outcomes import (
    ActualEvent,
    ExpectedEvent,
    FieldMismatch,
    MatchingConflict,
    MatchValidationResult,
    ValidatedMatch,
)

_NUMBER_PATTERN = r"[+-]?\d+(?:[.,]\d+)?"
_PLUS_MINUS_PATTERN = re.compile(
    rf"^\s*({_NUMBER_PATTERN})\s*\+\-\s*({_NUMBER_PATTERN})\s*$"
)
_PLUS_PATTERN = re.compile(rf"^\s*({_NUMBER_PATTERN})\s*\+\s*({_NUMBER_PATTERN})\s*$")
_MINUS_PATTERN = re.compile(rf"^\s*({_NUMBER_PATTERN})\s*-\s*({_NUMBER_PATTERN})\s*$")


class _FieldKind(str, Enum):
    FLOAT = "float"
    INTEGER = "integer"
    OTHER = "other"


@dataclass(frozen=True)
class _MatchingContext:
    """Read-only context needed while processing each actual event."""

    from_field: str
    subject_field: str
    field_kinds: Mapping[str, _FieldKind]
    expected_events_by_sender: Mapping[str, Sequence[ExpectedEvent]]


@dataclass
class _MatchingState:
    """Mutable collector for match processing outcomes."""

    matches: list[ValidatedMatch]
    conflicts: list[MatchingConflict]
    unmatched_actual_events: list[ActualEvent]
    matched_expected_event_ids: set[str]


def match_and_validate(
    expected_events: Sequence[ExpectedEvent],
    actual_events: Sequence[ActualEvent],
    matching_config: MatchingConfig,
    schema_fields: Sequence[FlattenedField],
) -> MatchValidationResult:
    """Match actual events to expected events and validate expected field values."""
    enabled_expected_events = [event for event in expected_events if event.enabled]
    context = _MatchingContext(
        from_field=matching_config.from_field,
        subject_field=matching_config.subject_field,
        field_kinds=_infer_field_kinds(schema_fields),
        expected_events_by_sender=_group_expected_events_by_sender(
            enabled_expected_events
        ),
    )
    state = _MatchingState(
        matches=[],
        conflicts=[],
        unmatched_actual_events=[],
        matched_expected_event_ids=set(),
    )

    for actual_event in actual_events:
        _process_actual_event(actual_event=actual_event, context=context, state=state)

    unmatched_expected_event_ids = tuple(
        event.expected_event_id
        for event in enabled_expected_events
        if event.expected_event_id not in state.matched_expected_event_ids
    )

    return MatchValidationResult(
        matches=tuple(state.matches),
        conflicts=tuple(state.conflicts),
        unmatched_actual_events=tuple(state.unmatched_actual_events),
        unmatched_expected_event_ids=unmatched_expected_event_ids,
    )


def _process_actual_event(
    *,
    actual_event: ActualEvent,
    context: _MatchingContext,
    state: _MatchingState,
) -> None:
    sender_candidates = context.expected_events_by_sender.get(
        _normalize_sender(actual_event.flattened.get(context.from_field)),
        [],
    )
    if not sender_candidates:
        state.unmatched_actual_events.append(actual_event)
        return

    selected = _select_expected_event(
        sender_candidates,
        actual_event,
        context.subject_field,
    )
    if selected is None:
        candidate_ids = tuple(
            expected_event.expected_event_id for expected_event in sender_candidates
        )
        state.conflicts.append(
            MatchingConflict(
                actual_event=actual_event,
                candidate_expected_event_ids=candidate_ids,
            )
        )
        return

    mismatches = _validate_expected_values(
        selected.expected_values,
        actual_event.flattened,
        context.field_kinds,
    )
    state.matches.append(
        ValidatedMatch(
            expected_event=selected,
            actual_event=actual_event,
            mismatches=tuple(mismatches),
        )
    )
    state.matched_expected_event_ids.add(selected.expected_event_id)


def _select_expected_event(
    sender_candidates: Sequence[ExpectedEvent],
    actual_event: ActualEvent,
    subject_field: str,
) -> ExpectedEvent | None:
    if len(sender_candidates) == 1:
        return sender_candidates[0]
    return _disambiguate_by_subject(sender_candidates, actual_event, subject_field)


def _group_expected_events_by_sender(
    expected_events: Sequence[ExpectedEvent],
) -> dict[str, list[ExpectedEvent]]:
    grouped: dict[str, list[ExpectedEvent]] = {}
    for expected_event in expected_events:
        grouped.setdefault(_normalize_sender(expected_event.sender), []).append(
            expected_event
        )
    return grouped


def _disambiguate_by_subject(
    candidates: Sequence[ExpectedEvent],
    actual_event: ActualEvent,
    subject_field: str,
) -> ExpectedEvent | None:
    actual_subject = _normalize_subject(actual_event.flattened.get(subject_field))
    subject_matches = [
        candidate
        for candidate in candidates
        if _normalize_subject(candidate.subject) == actual_subject
    ]
    if len(subject_matches) == 1:
        return subject_matches[0]
    return None


def _validate_expected_values(
    expected_values: Mapping[str, object],
    actual_values: Mapping[str, object],
    field_kinds: Mapping[str, _FieldKind],
) -> list[FieldMismatch]:
    mismatches: list[FieldMismatch] = []
    for field, expected_value in expected_values.items():
        expectation = parse_expectation_rule(expected_value)
        if expectation.kind == ExpectationRuleKind.IGNORE:
            continue

        actual_value = actual_values.get(field)
        if expectation.kind == ExpectationRuleKind.MUST_BE_EMPTY:
            if not _is_empty_actual(actual_value):
                mismatches.append(
                    FieldMismatch(
                        field=field,
                        expected=_display_value(expectation.expected_value),
                        actual=_display_value(actual_value),
                    )
                )
            continue

        kind = field_kinds.get(field, _FieldKind.OTHER)
        if _values_match(expectation.expected_value, actual_value, kind):
            continue
        mismatches.append(
            FieldMismatch(
                field=field,
                expected=_display_value(expectation.expected_value),
                actual=_display_value(actual_value),
            )
        )
    return mismatches


def _values_match(
    expected_value: object, actual_value: object, kind: _FieldKind
) -> bool:
    if kind == _FieldKind.FLOAT:
        tolerance_match = _match_float_tolerance_expression(
            expected_value, actual_value
        )
        if tolerance_match is not None:
            return tolerance_match
        expected_number = _parse_decimal(expected_value)
        actual_number = _parse_decimal(actual_value)
        if expected_number is not None and actual_number is not None:
            return expected_number == actual_number

    if kind == _FieldKind.INTEGER:
        expected_number = _parse_decimal(expected_value)
        actual_number = _parse_decimal(actual_value)
        if expected_number is not None and actual_number is not None:
            return expected_number == actual_number

    return _normalize_comparison_value(expected_value) == _normalize_comparison_value(
        actual_value
    )


def _match_float_tolerance_expression(
    expected_value: object, actual_value: object
) -> bool | None:
    if not isinstance(expected_value, str):
        return None
    actual_number = _parse_decimal(actual_value)
    if actual_number is None:
        return None
    text = expected_value.strip()
    for pattern, evaluator in (
        (_PLUS_MINUS_PATTERN, _evaluate_plus_minus),
        (_PLUS_PATTERN, _evaluate_plus),
        (_MINUS_PATTERN, _evaluate_minus),
    ):
        match = pattern.fullmatch(text)
        if not match:
            continue
        return evaluator(actual_number, match.group(1), match.group(2))
    return None


def _evaluate_plus_minus(actual: Decimal, center_raw: str, tolerance_raw: str) -> bool:
    center, tolerance = _parse_tolerance_parts(center_raw, tolerance_raw)
    if center is None or tolerance is None:
        return False
    return abs(actual - center) <= tolerance


def _evaluate_plus(actual: Decimal, center_raw: str, tolerance_raw: str) -> bool:
    center, tolerance = _parse_tolerance_parts(center_raw, tolerance_raw)
    if center is None or tolerance is None:
        return False
    return actual <= center + tolerance


def _evaluate_minus(actual: Decimal, center_raw: str, tolerance_raw: str) -> bool:
    center, tolerance = _parse_tolerance_parts(center_raw, tolerance_raw)
    if center is None or tolerance is None:
        return False
    return actual >= center - tolerance


def _parse_tolerance_parts(
    center_raw: str, tolerance_raw: str
) -> tuple[Decimal | None, Decimal | None]:
    return _parse_decimal(center_raw), _parse_decimal(tolerance_raw)


def _infer_field_kinds(
    schema_fields: Sequence[FlattenedField],
) -> dict[str, _FieldKind]:
    return {field.path: _infer_field_kind(field.definition) for field in schema_fields}


def _infer_field_kind(definition: object) -> _FieldKind:
    type_names = _extract_type_names(definition)
    if "number" in type_names or "float" in type_names or "double" in type_names:
        return _FieldKind.FLOAT
    if "integer" in type_names or "int" in type_names or "long" in type_names:
        return _FieldKind.INTEGER
    return _FieldKind.OTHER


def _extract_type_names(definition: object) -> set[str]:
    if isinstance(definition, str):
        return {definition}
    if isinstance(definition, Mapping):
        inner = definition.get("type")
        if inner is None:
            return set()
        return _extract_type_names(inner)
    if isinstance(definition, Sequence) and not isinstance(definition, str | bytes):
        names: set[str] = set()
        for item in definition:
            names.update(_extract_type_names(item))
        return names
    return set()


def _is_empty_actual(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _parse_decimal(value: object) -> Decimal | None:
    normalized = _normalize_decimal_input(value)
    if normalized is None:
        return None
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def _normalize_decimal_input(value: object) -> str | Decimal | None:
    coerced_text = _coerce_decimal_text(value)
    if coerced_text is None:
        return None
    return _normalize_decimal_separators(coerced_text)


def _coerce_decimal_text(value: object) -> str | Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float):
        return str(value)
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _normalize_decimal_separators(value: str | Decimal) -> str | Decimal:
    if isinstance(value, Decimal):
        return value
    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            return value.replace(".", "").replace(",", ".")
        return value.replace(",", "")
    if "," in value:
        return value.replace(".", "").replace(",", ".")
    return value


def _normalize_comparison_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Mapping):
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _display_value(value: object) -> str:
    return _normalize_comparison_value(value)


def _normalize_sender(value: object) -> str:
    return _normalize_comparison_value(value).lower()


def _normalize_subject(value: object) -> str:
    return _normalize_comparison_value(value)
