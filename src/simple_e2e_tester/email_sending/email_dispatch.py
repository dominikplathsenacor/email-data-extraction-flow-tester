"""Email composition and SMTP sending service."""

from __future__ import annotations

import mimetypes
import re
import smtplib
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, wait
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Protocol

from simple_e2e_tester.configuration.runtime_settings import MailSettings, SMTPSettings
from simple_e2e_tester.template_ingestion.testcase_models import TemplateTestCase

from .delivery_outcomes import EmailSendResult


class EmailCompositionError(Exception):
    """Raised when email composition fails (attachments, etc.)."""


WINDOWS_PATH_PREFIX = re.compile(r"^[A-Za-z]:\\")


class SMTPClient(Protocol):  # pylint: disable=too-few-public-methods
    """Protocol for SMTP clients used by the sender."""

    def send_message(self, settings: SMTPSettings, message: EmailMessage) -> None: ...


class SynchronousSMTPClient:  # pylint: disable=too-few-public-methods
    """Real SMTP client implementation using smtplib."""

    def send_message(self, settings: SMTPSettings, message: EmailMessage) -> None:
        recipients = _collect_recipients(message)
        smtp: smtplib.SMTP
        if settings.use_ssl:
            smtp = smtplib.SMTP_SSL(
                settings.host, settings.port, timeout=settings.timeout_seconds
            )
        else:
            smtp = smtplib.SMTP(
                settings.host, settings.port, timeout=settings.timeout_seconds
            )
        try:
            smtp.ehlo()
            if settings.use_starttls and not settings.use_ssl:
                smtp.starttls()
                smtp.ehlo()
            if settings.username and settings.password:
                smtp.login(settings.username, settings.password)
            smtp.send_message(message, to_addrs=recipients)
        finally:
            smtp.quit()


def compose_email(
    testcase: TemplateTestCase,
    mail_settings: MailSettings,
    *,
    attachments_base: Path,
) -> EmailMessage:
    """Build an EmailMessage for the test case."""
    message = EmailMessage()
    message["Message-ID"] = make_msgid()
    message["From"] = testcase.from_address
    message["To"] = mail_settings.to_address
    if mail_settings.cc:
        message["Cc"] = ", ".join(mail_settings.cc)
    if mail_settings.bcc:
        message["Bcc"] = ", ".join(mail_settings.bcc)
    message["Subject"] = testcase.subject
    message["X-Test-Id"] = testcase.test_id

    body_text = testcase.body or ""
    html_body = _render_html_body(body_text)
    message.set_content(body_text)
    message.add_alternative(html_body, subtype="html")

    for attachment in _parse_attachments(testcase.attachment or "", attachments_base):
        message.add_attachment(
            attachment.data,
            maintype=attachment.content_type.split("/")[0],
            subtype=attachment.content_type.split("/")[1],
            filename=attachment.filename,
        )

    return message


class ExpectedEventDispatcher:  # pylint: disable=too-few-public-methods
    """Service responsible for sending composed emails using an SMTP client."""

    def __init__(
        self,
        smtp_client: SMTPClient,
        smtp_settings: SMTPSettings,
        mail_settings: MailSettings,
        *,
        attachments_base: Path,
    ) -> None:
        self._smtp_client = smtp_client
        self._smtp_settings = smtp_settings
        self._mail_settings = mail_settings
        self._attachments_base = attachments_base

    def send_all(self, testcases: Sequence[TemplateTestCase]) -> list[EmailSendResult]:
        results: list[EmailSendResult] = []
        enabled_cases = [tc for tc in testcases if tc.enabled]
        disabled_cases = [tc for tc in testcases if not tc.enabled]

        for testcase in disabled_cases:
            results.append(EmailSendResult.skipped(testcase.test_id))

        max_workers = max(1, self._smtp_settings.parallelism)
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for testcase in enabled_cases:
                future = executor.submit(self._send_single, testcase)
                futures[future] = testcase.test_id
            wait(futures.keys())

        result_map = {result.test_id: result for result in results}
        for future, test_id in futures.items():
            result_map[test_id] = future.result()

        ordered = [result_map[tc.test_id] for tc in testcases]
        return ordered

    def _send_single(self, testcase: TemplateTestCase) -> EmailSendResult:
        try:
            message = compose_email(
                testcase, self._mail_settings, attachments_base=self._attachments_base
            )
            self._smtp_client.send_message(self._smtp_settings, message)
            return EmailSendResult.sent(testcase.test_id)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return EmailSendResult.failed(testcase.test_id, exc)


def validate_attachments_for_testcases(
    testcases: Sequence[TemplateTestCase], *, attachments_base: Path
) -> None:
    """Fail fast when an enabled test case references invalid attachment paths."""
    for testcase in testcases:
        if not testcase.enabled:
            continue
        _parse_attachments(testcase.attachment or "", attachments_base)


def _render_html_body(plain_text: str) -> str:
    escaped = (
        plain_text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br>")
    )
    content = escaped or "No content provided."
    return f"<html><body><h2>Test Execution</h2><p>{content}</p></body></html>"


class Attachment:  # pylint: disable=too-few-public-methods
    """Attachment representation for composed emails."""

    def __init__(self, filename: str, content_type: str, data: bytes) -> None:
        self.filename = filename
        self.content_type = content_type
        self.data = data


def _parse_attachments(raw_value: str, attachments_base: Path) -> list[Attachment]:
    value = (raw_value or "").strip()
    if not value:
        return []
    lines = [
        line.strip()
        for line in value.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    payload_lines = [line for line in lines if line]
    if not payload_lines:
        return []
    if _looks_like_path(payload_lines[0]):
        return [_attachment_from_path(line, attachments_base) for line in payload_lines]
    text = "\n".join(payload_lines)
    pdf_bytes = _text_to_pdf(text)
    return [Attachment("attachment.pdf", "application/pdf", pdf_bytes)]


def _looks_like_path(value: str) -> bool:
    if value.startswith(("./", "/", ".\\")):
        return True
    return WINDOWS_PATH_PREFIX.match(value) is not None


def _attachment_from_path(value: str, attachments_base: Path) -> Attachment:
    if not value:
        raise EmailCompositionError("Attachment path is empty.")
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = (attachments_base / candidate).resolve()
    if not candidate.exists():
        raise EmailCompositionError(f"Attachment file not found: {candidate}")
    data = candidate.read_bytes()
    content_type, _ = mimetypes.guess_type(candidate.name)
    content_type = content_type or "application/octet-stream"
    return Attachment(candidate.name, content_type, data)


def _text_to_pdf(text: str) -> bytes:
    encoded = text.encode("utf-8")
    hex_stream = encoded.hex()
    content_stream = f"BT /F1 12 Tf 72 720 Td <{hex_stream}> Tj ET"
    objects = _build_pdf_objects(content_stream)
    return _serialize_pdf(objects)


def _build_pdf_objects(content_stream: str) -> list[str]:
    objects: list[str] = []

    def add(obj_id: int, body: str) -> None:
        objects.append(f"{obj_id} 0 obj\n{body}\nendobj\n")

    add(1, "<< /Type /Catalog /Pages 2 0 R >>")
    add(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add(
        3,
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        "/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
    )
    add(4, f"<< /Length {len(content_stream)} >>\nstream\n{content_stream}\nendstream")
    add(5, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    return objects


def _serialize_pdf(objects: Sequence[str]) -> bytes:
    pdf_header = "%PDF-1.4\n"
    body_chunks, offsets, xref_offset = _collect_pdf_offsets(objects, pdf_header)
    xref_entries = ["0000000000 65535 f \n"]
    for offset in offsets:
        xref_entries.append(f"{offset:010d} 00000 n \n")
    xref_section = "xref\n0 6\n" + "".join(xref_entries)
    trailer = f"trailer<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF"
    pdf_body = pdf_header + "".join(body_chunks) + xref_section + trailer
    return pdf_body.encode("latin-1")


def _collect_pdf_offsets(
    objects: Sequence[str], pdf_header: str
) -> tuple[list[str], list[int], int]:
    body_chunks: list[str] = []
    offsets: list[int] = []
    current_offset = len(pdf_header.encode("latin-1"))
    for obj in objects:
        offsets.append(current_offset)
        body_chunks.append(obj)
        current_offset += len(obj.encode("latin-1"))
    return body_chunks, offsets, current_offset


def _collect_recipients(message: EmailMessage) -> list[str]:
    recipients = []
    for header in ("To", "Cc", "Bcc"):
        if header in message:
            recipients.extend(
                [
                    address.strip()
                    for address in message[header].split(",")
                    if address.strip()
                ]
            )
    return recipients
