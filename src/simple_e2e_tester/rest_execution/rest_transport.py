"""REST transport for synchronous testcase execution."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol

import requests

from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.kafka_consumption.actual_event_messages import ActualEventMessage
from simple_e2e_tester.run_execution.run_contracts import RunArtifacts, TransportExecutionResult


class RestRequestClient(Protocol):  # pylint: disable=too-few-public-methods
    """Protocol for HTTP request adapter used by the transport."""

    def request(
        self,
        *,
        method: str,
        url: str,
        json_payload: dict[str, object],
        timeout_seconds: int,
    ) -> Mapping[str, object]:
        """Send one synchronous request and return decoded JSON object."""
        raise NotImplementedError


class RestRequestError(Exception):
    """Raised when a REST request fails at HTTP/transport/protocol level."""


class RequestsRestRequestClient:  # pylint: disable=too-few-public-methods
    """HTTP client implementation backed by requests."""

    def request(
        self,
        *,
        method: str,
        url: str,
        json_payload: dict[str, object],
        timeout_seconds: int,
    ) -> Mapping[str, object]:
        try:
            response = requests.request(
                method=method,
                url=url,
                json=json_payload,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RestRequestError(str(exc)) from exc

        try:
            decoded = response.json()
        except ValueError as exc:
            raise RestRequestError("REST response is not valid JSON.") from exc
        if not isinstance(decoded, Mapping):
            raise RestRequestError("REST response root must be a JSON object.")
        return decoded


class RestExecutionTransport:  # pylint: disable=too-few-public-methods
    """Execute enabled testcases against a REST interface."""

    _ABORT_AFTER_CONSECUTIVE_FAILURES = 3

    def __init__(self, client: RestRequestClient) -> None:
        self._client = client

    def execute(
        self, *, artifacts: RunArtifacts, run_start: datetime
    ) -> TransportExecutionResult:
        del run_start
        rest_settings = artifacts.configuration.rest
        if rest_settings is None:
            raise ValueError("REST settings are required for RestExecutionTransport.")

        send_status_by_test_id: dict[str, SendStatus] = {}
        actual_messages: list[ActualEventMessage] = []
        sent_ok = 0
        consecutive_failures = 0
        endpoint = _build_endpoint(rest_settings.base_url, rest_settings.path)

        for testcase in artifacts.testcases:
            if not testcase.enabled:
                send_status_by_test_id[testcase.test_id] = SendStatus.SKIPPED
                continue
            if consecutive_failures >= self._ABORT_AFTER_CONSECUTIVE_FAILURES:
                send_status_by_test_id[testcase.test_id] = SendStatus.SKIPPED
                continue
            payload = _build_request_payload(testcase=testcase, defaults=rest_settings.defaults)
            try:
                response_payload = self._client.request(
                    method=rest_settings.method,
                    url=endpoint,
                    json_payload=payload,
                    timeout_seconds=rest_settings.timeout_seconds,
                )
            except RestRequestError:
                send_status_by_test_id[testcase.test_id] = SendStatus.FAILED
                consecutive_failures += 1
                continue
            consecutive_failures = 0
            send_status_by_test_id[testcase.test_id] = SendStatus.SENT
            sent_ok += 1
            actual_messages.append(
                ActualEventMessage(
                    key=testcase.test_id,
                    value=dict(response_payload),
                    timestamp=datetime.now(UTC),
                    flattened=dict(response_payload),
                )
            )
        return TransportExecutionResult(
            send_status_by_test_id=send_status_by_test_id,
            sent_ok=sent_ok,
            actual_messages=tuple(actual_messages),
        )


def _build_endpoint(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _build_request_payload(*, testcase, defaults: Mapping[str, str]) -> dict[str, object]:
    return {
        "ag": defaults["ag"],
        "dokart": defaults["dokart"],
        "dokrefuid": defaults["dokrefuid"],
        "eingangsdatum": defaults["eingangsdatum"],
        "emailabsender": testcase.from_address,
        "emailbetreff": testcase.subject,
        "flowid": defaults["flowid"],
        "ordnungsbegriff": defaults["ordnungsbegriff"],
        "referenztyp": defaults["referenztyp"],
        "dok_text": testcase.body,
    }
