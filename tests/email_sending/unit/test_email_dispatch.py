"""Email composition and sending tests."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from email.message import EmailMessage, Message
from pathlib import Path
from typing import Any, cast

import pytest
from simple_e2e_tester.configuration.runtime_settings import MailSettings, SMTPSettings
from simple_e2e_tester.email_sending.delivery_outcomes import SendStatus
from simple_e2e_tester.email_sending.email_dispatch import (
    EmailCompositionError,
    ExpectedEventDispatcher,
    SynchronousSMTPClient,
    compose_email,
)
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase


def _testcase(**overrides) -> TemplateTestCase:
    defaults: dict[str, Any] = {
        "row_number": 3,
        "test_id": "TC-1",
        "tags": ("smoke",),
        "enabled": True,
        "notes": "",
        "from_address": "sender@example.com",
        "subject": "Subject",
        "body": "Hello world",
        "attachment": "",
        "expected_values": {},
    }
    defaults.update(overrides)
    return TemplateTestCase(**defaults)


def _smtp_settings(
    parallelism: int = 2, use_ssl: bool = False, use_starttls: bool = True
) -> SMTPSettings:
    return SMTPSettings(
        host="smtp.example.com",
        port=587,
        username="user",
        password="secret",
        use_starttls=use_starttls,
        use_ssl=use_ssl,
        timeout_seconds=30,
        parallelism=parallelism,
    )


def _mail_settings() -> MailSettings:
    return MailSettings(to_address="qa@example.com", cc=("cc@example.com",), bcc=())


def test_compose_email_creates_plain_and_html_parts(tmp_path: Path) -> None:
    mail_settings = _mail_settings()
    testcase = _testcase(body="Plain text body", attachment="")

    message = compose_email(testcase, mail_settings, attachments_base=tmp_path)

    assert message["To"] == mail_settings.to_address
    assert message["From"] == testcase.from_address
    assert message["Subject"] == testcase.subject
    payloads = {
        part.get_content_type(): _part_text(part) for part in message.iter_parts()
    }
    assert payloads["text/plain"].strip() == "Plain text body"
    assert "Plain text body" in payloads["text/html"]


def test_compose_email_attaches_files(tmp_path: Path) -> None:
    attachment_path = tmp_path / "sample.txt"
    attachment_path.write_text("content", encoding="utf-8")
    testcase = _testcase(attachment=str(attachment_path))

    message = compose_email(testcase, _mail_settings(), attachments_base=tmp_path)

    attachments = [part for part in message.iter_attachments()]
    assert len(attachments) == 1
    attachment = attachments[0]
    assert attachment.get_filename() == "sample.txt"
    assert attachment.get_content_type() == "text/plain"
    assert _part_text(attachment) == "content"


def test_compose_email_generates_pdf_from_text(tmp_path: Path) -> None:
    testcase = _testcase(attachment="Line one\nLine two")

    message = compose_email(testcase, _mail_settings(), attachments_base=tmp_path)

    attachments = [part for part in message.iter_attachments()]
    assert len(attachments) == 1
    attachment = attachments[0]
    assert attachment.get_filename() == "attachment.pdf"
    assert attachment.get_content_type() == "application/pdf"
    assert _part_bytes(attachment)[:4] == b"%PDF"


def _part_bytes(part: Message) -> bytes:
    payload = part.get_payload(decode=True)
    if payload is None:
        return b""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode(part.get_content_charset() or "utf-8")
    return b""


def _part_text(part: Message) -> str:
    return _part_bytes(part).decode(part.get_content_charset() or "utf-8")


def test_compose_email_errors_on_missing_file(tmp_path: Path) -> None:
    testcase = _testcase(attachment=str(tmp_path / "missing.txt"))

    with pytest.raises(EmailCompositionError):
        compose_email(testcase, _mail_settings(), attachments_base=tmp_path)


def test_compose_email_resolves_relative_attachment_paths(tmp_path: Path) -> None:
    attachment_path = tmp_path / "fixtures" / "sample.txt"
    attachment_path.parent.mkdir(parents=True, exist_ok=True)
    attachment_path.write_text("relative-content", encoding="utf-8")
    testcase = _testcase(attachment="./fixtures/sample.txt")

    message = compose_email(testcase, _mail_settings(), attachments_base=tmp_path)

    attachments = [part for part in message.iter_attachments()]
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "sample.txt"


def test_compose_email_treats_extensionless_attachment_value_as_text_payload(
    tmp_path: Path,
) -> None:
    testcase = _testcase(attachment="README")

    message = compose_email(testcase, _mail_settings(), attachments_base=tmp_path)

    attachments = [part for part in message.iter_attachments()]
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "attachment.pdf"
    assert attachments[0].get_content_type() == "application/pdf"


def test_compose_email_treats_dot_suffix_text_as_text_payload_without_path_prefix(
    tmp_path: Path,
) -> None:
    testcase = _testcase(attachment="hello.world")

    message = compose_email(testcase, _mail_settings(), attachments_base=tmp_path)

    attachments = [part for part in message.iter_attachments()]
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "attachment.pdf"
    assert attachments[0].get_content_type() == "application/pdf"


class FakeSMTPClient:
    def __init__(
        self, fail_for: Sequence[str] | None = None, delay: float = 0.0
    ) -> None:
        self.fail_for = set(fail_for or [])
        self.delay = delay
        self.sent_messages: list[str] = []
        self.max_concurrent = 0
        self._lock = threading.Lock()
        self._active = 0

    def send_message(self, settings: SMTPSettings, message: EmailMessage) -> None:
        with self._lock:
            self._active += 1
            self.max_concurrent = max(self.max_concurrent, self._active)
        try:
            time.sleep(self.delay)
            subject = cast(str, message["Subject"])
            if subject in self.fail_for:
                raise RuntimeError(f"Failure for {subject}")
            self.sent_messages.append(subject)
        finally:
            with self._lock:
                self._active -= 1


def test_email_sender_sends_enabled_cases_in_parallel(tmp_path: Path) -> None:
    mail_settings = _mail_settings()
    smtp_settings = _smtp_settings(parallelism=2)
    testcases = [
        _testcase(test_id="TC-1", subject="One"),
        _testcase(test_id="TC-2", subject="Two"),
        _testcase(test_id="TC-3", subject="Three", enabled=False),
    ]
    fake_client = FakeSMTPClient(delay=0.1)
    sender = ExpectedEventDispatcher(
        smtp_client=fake_client,
        smtp_settings=smtp_settings,
        mail_settings=mail_settings,
        attachments_base=tmp_path,
    )

    results = sender.send_all(testcases)

    assert {result.test_id: result.status for result in results} == {
        "TC-1": SendStatus.SENT,
        "TC-2": SendStatus.SENT,
        "TC-3": SendStatus.SKIPPED,
    }

    assert fake_client.max_concurrent >= 2


def test_email_sender_parallelism_follows_configured_value(tmp_path: Path) -> None:
    mail_settings = _mail_settings()
    configured_parallelism = 3
    smtp_settings = _smtp_settings(parallelism=configured_parallelism)
    testcases = [
        _testcase(test_id=f"TC-{index}", subject=f"Subject-{index}")
        for index in range(1, 25)
    ]
    fake_client = FakeSMTPClient(delay=0.05)
    sender = ExpectedEventDispatcher(
        smtp_client=fake_client,
        smtp_settings=smtp_settings,
        mail_settings=mail_settings,
        attachments_base=tmp_path,
    )

    results = sender.send_all(testcases)

    assert all(result.status == SendStatus.SENT for result in results)
    assert fake_client.max_concurrent <= configured_parallelism
    assert fake_client.max_concurrent >= 2


def test_email_sender_records_failures(tmp_path: Path) -> None:
    mail_settings = _mail_settings()
    smtp_settings = _smtp_settings(parallelism=2)
    testcases = [
        _testcase(test_id="TC-1", subject="One"),
        _testcase(test_id="TC-2", subject="Two"),
    ]
    fake_client = FakeSMTPClient(fail_for=["Two"])
    sender = ExpectedEventDispatcher(
        fake_client,
        smtp_settings,
        mail_settings,
        attachments_base=tmp_path,
    )

    results = sender.send_all(testcases)

    failure = next(result for result in results if result.test_id == "TC-2")
    assert failure.status == SendStatus.FAILED
    assert failure.error_message is not None


class _FakeSmtpSession:
    def __init__(self, host: str, port: int, timeout: int) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.actions: list[object] = []
        self.sent_message: EmailMessage | None = None
        self.sent_to_addrs: list[str] | None = None

    def ehlo(self) -> None:
        self.actions.append("ehlo")

    def starttls(self) -> None:
        self.actions.append("starttls")

    def login(self, username: str, password: str) -> None:
        self.actions.append(("login", username, password))

    def send_message(self, message: EmailMessage, to_addrs: list[str]) -> None:
        self.actions.append("send_message")
        self.sent_message = message
        self.sent_to_addrs = to_addrs

    def quit(self) -> None:
        self.actions.append("quit")


def test_synchronous_smtp_client_uses_starttls_login_and_all_recipients(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions: list[_FakeSmtpSession] = []

    def smtp_factory(host: str, port: int, timeout: int) -> _FakeSmtpSession:
        session = _FakeSmtpSession(host, port, timeout)
        sessions.append(session)
        return session

    monkeypatch.setattr(
        "simple_e2e_tester.email_sending.email_dispatch.smtplib.SMTP", smtp_factory
    )
    mail_settings = MailSettings(
        to_address="qa@example.com",
        cc=("copy@example.com",),
        bcc=("hidden@example.com",),
    )
    message = compose_email(_testcase(), mail_settings, attachments_base=tmp_path)

    SynchronousSMTPClient().send_message(
        _smtp_settings(use_ssl=False, use_starttls=True), message
    )

    assert len(sessions) == 1
    session = sessions[0]
    assert session.host == "smtp.example.com"
    assert session.port == 587
    assert session.timeout == 30
    assert session.actions == [
        "ehlo",
        "starttls",
        "ehlo",
        ("login", "user", "secret"),
        "send_message",
        "quit",
    ]
    assert session.sent_to_addrs == [
        "qa@example.com",
        "copy@example.com",
        "hidden@example.com",
    ]
    assert session.sent_message is not None
    assert session.sent_message["X-Test-Id"] == "TC-1"


def test_synchronous_smtp_client_uses_ssl_without_starttls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions: list[_FakeSmtpSession] = []

    def smtp_ssl_factory(host: str, port: int, timeout: int) -> _FakeSmtpSession:
        session = _FakeSmtpSession(host, port, timeout)
        sessions.append(session)
        return session

    monkeypatch.setattr(
        "simple_e2e_tester.email_sending.email_dispatch.smtplib.SMTP_SSL",
        smtp_ssl_factory,
    )
    message = compose_email(_testcase(), _mail_settings(), attachments_base=tmp_path)

    SynchronousSMTPClient().send_message(
        _smtp_settings(use_ssl=True, use_starttls=True), message
    )

    assert len(sessions) == 1
    session = sessions[0]
    assert session.actions == [
        "ehlo",
        ("login", "user", "secret"),
        "send_message",
        "quit",
    ]
