"""Configuration domain entities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SchemaConfig:
    """Normalized schema settings."""

    schema_type: str
    text: str
    source_path: Path | None


@dataclass(frozen=True)
class MatchingConfig:
    """Field names used for matching Kafka records with test cases."""

    from_field: str
    subject_field: str


@dataclass(frozen=True)
class ValidationSettings:
    """Subset of flattened field names to validate against actual values."""

    field_names: tuple[str, ...] | None


@dataclass(frozen=True)
class SMTPSettings:  # pylint: disable=too-many-instance-attributes
    """SMTP server connectivity configuration."""

    host: str
    port: int
    username: str | None
    password: str | None
    use_starttls: bool
    use_ssl: bool
    timeout_seconds: int
    parallelism: int


@dataclass(frozen=True)
class MailSettings:
    """Destination mailbox configuration."""

    to_address: str
    cc: tuple[str, ...]
    bcc: tuple[str, ...]


@dataclass(frozen=True)
class KafkaSettings:
    """Kafka consumer configuration."""

    bootstrap_servers: tuple[str, ...]
    topic: str
    group_id: str | None
    security: Mapping[str, object]
    timeout_seconds: int
    poll_interval_ms: int
    auto_offset_reset: str


@dataclass(frozen=True)
class Configuration:  # pylint: disable=too-many-instance-attributes
    """Top-level configuration aggregate."""

    path: Path
    schema: SchemaConfig
    response_schema: SchemaConfig
    kafka_event_schema: SchemaConfig | None
    transport: TransportSettings
    matching: MatchingConfig
    validation: ValidationSettings
    smtp: SMTPSettings
    mail: MailSettings
    kafka: KafkaSettings
    rest: RestSettings | None


@dataclass(frozen=True)
class TransportSettings:
    """Execution transport selection."""

    mode: str


@dataclass(frozen=True)
class RestSettings:
    """REST transport settings."""

    base_url: str
    path: str
    method: str
    timeout_seconds: int
    wait_between_calls_seconds: int | None
    retry_count: int
    retry_backoff_ms: int
    defaults: Mapping[str, str]
    basic_auth_username: str | None
    basic_auth_password: str | None
