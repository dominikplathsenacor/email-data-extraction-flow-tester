"""Expectation rule modeling for expected-vs-actual validation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

_NUMBER_PATTERN = r"[+-]?\d+(?:[.,]\d+)?"
_PLUS_MINUS_PATTERN = re.compile(
    rf"^\s*{_NUMBER_PATTERN}\s*\+\-\s*{_NUMBER_PATTERN}\s*$"
)
_PLUS_PATTERN = re.compile(rf"^\s*{_NUMBER_PATTERN}\s*\+\s*{_NUMBER_PATTERN}\s*$")
_MINUS_PATTERN = re.compile(rf"^\s*{_NUMBER_PATTERN}\s*-\s*{_NUMBER_PATTERN}\s*$")


class ExpectationRuleKind(str, Enum):
    """Supported expectation rule kinds."""

    IGNORE = "ignore"
    MUST_BE_EMPTY = "must_be_empty"
    EXACT = "exact"
    TOLERANCE = "tolerance"


@dataclass(frozen=True)
class ExpectationRule:
    """Parsed expectation rule for one expected field value."""

    kind: ExpectationRuleKind
    expected_value: object | None = None


def parse_expectation_rule(expected_value: object) -> ExpectationRule:
    """Parse raw expected cell value into an explicit rule."""
    if expected_value is None:
        return ExpectationRule(kind=ExpectationRuleKind.IGNORE)
    if isinstance(expected_value, str):
        stripped = expected_value.strip()
        if not stripped:
            return ExpectationRule(kind=ExpectationRuleKind.IGNORE)
        if stripped == "MUSS_LEER_SEIN":
            return ExpectationRule(
                kind=ExpectationRuleKind.MUST_BE_EMPTY,
                expected_value="MUSS_LEER_SEIN",
            )
        if _is_tolerance_expression(stripped):
            return ExpectationRule(
                kind=ExpectationRuleKind.TOLERANCE,
                expected_value=stripped,
            )
        return ExpectationRule(kind=ExpectationRuleKind.EXACT, expected_value=stripped)
    return ExpectationRule(
        kind=ExpectationRuleKind.EXACT, expected_value=expected_value
    )


def _is_tolerance_expression(value: str) -> bool:
    return any(
        pattern.fullmatch(value)
        for pattern in (_PLUS_MINUS_PATTERN, _PLUS_PATTERN, _MINUS_PATTERN)
    )
