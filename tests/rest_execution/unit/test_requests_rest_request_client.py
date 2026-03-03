"""Tests for requests-based REST request client."""

from __future__ import annotations

import pytest
import requests
from simple_e2e_tester.rest_execution.rest_transport import (
    RequestsRestRequestClient,
    RestRequestError,
)


class _FakeResponse:
    def __init__(self, payload: object, *, status_error: Exception | None = None) -> None:
        self._payload = payload
        self._status_error = status_error

    def raise_for_status(self) -> None:
        if self._status_error is not None:
            raise self._status_error

    def json(self) -> object:
        return self._payload


def test_given_valid_json_object_when_request_client_calls_rest_then_mapping_returned(
    monkeypatch,
) -> None:
    def _fake_request(**kwargs):
        assert kwargs["method"] == "POST"
        assert kwargs["url"] == "http://localhost:8080/extract"
        assert kwargs["json"] == {"dok_text": "body"}
        assert kwargs["timeout"] == 30
        return _FakeResponse({"sender": "x", "subject": "y"})

    monkeypatch.setattr("requests.request", _fake_request)

    result = RequestsRestRequestClient().request(
        method="POST",
        url="http://localhost:8080/extract",
        json_payload={"dok_text": "body"},
        timeout_seconds=30,
    )

    assert result == {"sender": "x", "subject": "y"}


def test_given_non_object_json_when_request_client_calls_rest_then_error_raised(
    monkeypatch,
) -> None:
    monkeypatch.setattr("requests.request", lambda **kwargs: _FakeResponse(["not-an-object"]))

    with pytest.raises(RestRequestError, match="root must be a JSON object"):
        RequestsRestRequestClient().request(
            method="POST",
            url="http://localhost:8080/extract",
            json_payload={"dok_text": "body"},
            timeout_seconds=30,
        )


def test_given_request_exception_when_request_client_calls_rest_then_error_raised(
    monkeypatch,
) -> None:
    def _raise_request_exception(**kwargs):
        raise requests.RequestException("x")

    monkeypatch.setattr("requests.request", _raise_request_exception)

    with pytest.raises(RestRequestError, match="x"):
        RequestsRestRequestClient().request(
            method="POST",
            url="http://localhost:8080/extract",
            json_payload={"dok_text": "body"},
            timeout_seconds=30,
        )
