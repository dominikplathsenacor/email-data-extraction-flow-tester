"""Kafka consumption service tests."""

from __future__ import annotations

import json
import struct
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import simple_e2e_tester.kafka_consumption.actual_event_reader as kafka_service_module
from confluent_kafka import KafkaError
from simple_e2e_tester.configuration.runtime_settings import KafkaSettings, SchemaConfig
from simple_e2e_tester.kafka_consumption.actual_event_reader import (
    ActualEventDecodeError,
    ActualEventReader,
)
from simple_e2e_tester.schema_management import flatten_schema, load_schema_document


def _schema_config() -> SchemaConfig:
    return SchemaConfig(
        schema_type="avsc",
        text="""
{
  "type": "record",
  "name": "Root",
  "fields": [
    {"name": "emailabsender", "type": "string"},
    {"name": "emailbetreff", "type": "string"},
    {
      "name": "ki_ergebnis",
      "type": {
        "type": "record",
        "name": "KiResult",
        "fields": [
          {"name": "klasse", "type": {"type": "array", "items": "string"}}
        ]
      }
    }
  ]
}
""",
        source_path=None,
    )


def _kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers=("localhost:9092",),
        topic="test-topic",
        group_id="group",
        security={},
        timeout_seconds=10,
        poll_interval_ms=100,
        auto_offset_reset="latest",
    )


class FakeRecord:
    def __init__(
        self,
        payload: bytes,
        timestamp: datetime | None,
        *,
        key: bytes | None = None,
        error_obj: object | None = None,
    ) -> None:
        self._payload = payload
        self._timestamp = timestamp
        self._key = key
        self._error = error_obj

    def error(self) -> object | None:
        return self._error

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes:
        return self._payload

    def timestamp(self) -> tuple[int, int | None]:
        if self._timestamp is None:
            return (0, None)
        return (0, int(self._timestamp.timestamp() * 1000))


class FakeConsumer:
    def __init__(self, records: Iterable[FakeRecord]) -> None:
        self.records = list(records)
        self._index = 0

    def subscribe(
        self,
        topics: list[str],
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
    ) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> FakeRecord | None:
        if self._index >= len(self.records):
            return None
        record = self.records[self._index]
        self._index += 1
        return record

    def close(self) -> None:
        self.closed = True


class FakeError:
    def __init__(self, code_value: int, text: str = "boom") -> None:
        self._code_value = code_value
        self._text = text

    def code(self) -> int:
        return self._code_value

    def __str__(self) -> str:
        return self._text


def _flattened_fields():
    schema_doc = load_schema_document(_schema_config())
    return flatten_schema(schema_doc)


def _encode_long(value: int) -> bytes:
    unsigned = (value << 1) ^ (value >> 63)
    encoded = bytearray()
    while unsigned & ~0x7F:
        encoded.append((unsigned & 0x7F) | 0x80)
        unsigned >>= 7
    encoded.append(unsigned)
    return bytes(encoded)


def _encode_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return _encode_long(len(encoded)) + encoded


def _encode_string_array(values: list[str]) -> bytes:
    if not values:
        return b"\x00"
    payload = _encode_long(len(values))
    for value in values:
        payload += _encode_string(value)
    payload += b"\x00"
    return payload


def _encode_avro_payload(sender: str, subject: str, klasse_values: list[str]) -> bytes:
    return (
        _encode_string(sender)
        + _encode_string(subject)
        + _encode_string_array(klasse_values)
    )


def test_kafka_consumer_yields_messages_after_timestamp() -> None:
    now = datetime.now(UTC)
    records = [
        FakeRecord(
            _encode_avro_payload("late", "subject", ["A"]),
            timestamp=now - timedelta(seconds=10),
        ),
        FakeRecord(
            _encode_avro_payload("on-time", "subject", ["A"]),
            timestamp=now + timedelta(seconds=1),
        ),
    ]
    consumer = FakeConsumer(records)
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=consumer,
    )

    messages = list(service.consume_from(now))

    assert len(messages) == 1
    assert messages[0].flattened["emailabsender"] == "on-time"


def test_kafka_consumer_stops_after_timeout() -> None:
    now = datetime.now(UTC)
    consumer = FakeConsumer([])
    settings = _kafka_settings()
    settings = KafkaSettings(**{**settings.__dict__, "timeout_seconds": 0})
    service = ActualEventReader(
        kafka_settings=settings,
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=consumer,
    )

    messages = list(service.consume_from(now))

    assert messages == []


def test_kafka_consumer_raises_on_decode_error() -> None:
    now = datetime.now(UTC)
    records = [
        FakeRecord(
            _encode_string("only-one-field"), timestamp=now + timedelta(seconds=1)
        ),
    ]
    consumer = FakeConsumer(records)
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=consumer,
    )

    with pytest.raises(ActualEventDecodeError):
        list(service.consume_from(now))


def test_kafka_consumer_accepts_confluent_wire_header() -> None:
    now = datetime.now(UTC)
    payload = _encode_avro_payload("sender", "subject", ["A"])
    schema_registry_header = b"\x00" + struct.pack(">I", 7)
    records = [
        FakeRecord(
            schema_registry_header + payload, timestamp=now + timedelta(seconds=1)
        )
    ]
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=FakeConsumer(records),
    )

    messages = list(service.consume_from(now))

    assert len(messages) == 1
    assert messages[0].flattened["emailabsender"] == "sender"
    assert messages[0].flattened["ki_ergebnis.klasse"] == ["A"]


def test_json_schema_decode_reads_json_object_payload() -> None:
    now = datetime.now(UTC)
    json_schema_config = SchemaConfig(
        schema_type="json_schema",
        text="""
{
  "type": "object",
  "properties": {
    "emailabsender": {"type": "string"},
    "emailbetreff": {"type": "string"},
    "ki_ergebnis": {
      "type": "object",
      "properties": {
        "klasse": {"type": "array", "items": {"type": "string"}}
      }
    }
  }
}
""",
        source_path=None,
    )
    fields = flatten_schema(load_schema_document(json_schema_config))
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=fields,
        schema_config=json_schema_config,
        consumer=FakeConsumer(
            [
                FakeRecord(
                    json.dumps(
                        {
                            "emailabsender": "sender@example.com",
                            "emailbetreff": "Subject-1",
                            "ki_ergebnis": {"klasse": ["A"]},
                        }
                    ).encode("utf-8"),
                    timestamp=now + timedelta(seconds=1),
                )
            ]
        ),
    )

    messages = list(service.consume_from(now))

    assert len(messages) == 1
    assert messages[0].flattened["emailabsender"] == "sender@example.com"
    assert messages[0].flattened["emailbetreff"] == "Subject-1"
    assert messages[0].flattened["ki_ergebnis.klasse"] == ["A"]


def test_kafka_consumer_skips_partition_eof_errors() -> None:
    now = datetime.now(UTC)
    partition_eof_code = getattr(KafkaError, "_PARTITION_EOF", -1)
    records = [
        FakeRecord(
            b"",
            timestamp=now + timedelta(seconds=1),
            error_obj=FakeError(partition_eof_code, "eof"),
        ),
        FakeRecord(
            _encode_avro_payload("sender", "subject", ["A"]),
            timestamp=now + timedelta(seconds=1),
        ),
    ]
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=FakeConsumer(records),
    )

    messages = list(service.consume_from(now))

    assert len(messages) == 1
    assert messages[0].flattened["emailabsender"] == "sender"


def test_kafka_consumer_raises_for_non_eof_kafka_error() -> None:
    now = datetime.now(UTC)
    records = [
        FakeRecord(
            b"",
            timestamp=now + timedelta(seconds=1),
            error_obj=FakeError(-1, "fatal"),
        )
    ]
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=FakeConsumer(records),
    )

    with pytest.raises(ActualEventDecodeError, match="Kafka error: fatal"):
        list(service.consume_from(now))


def test_kafka_consumer_skips_records_without_timestamp() -> None:
    now = datetime.now(UTC)
    records = [
        FakeRecord(
            _encode_avro_payload("skip", "subject", ["A"]),
            timestamp=None,
        ),
        FakeRecord(
            _encode_avro_payload("keep", "subject", ["A"]),
            timestamp=now + timedelta(seconds=1),
        ),
    ]
    settings = KafkaSettings(**{**_kafka_settings().__dict__, "timeout_seconds": 1})
    service = ActualEventReader(
        kafka_settings=settings,
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=FakeConsumer(records),
    )

    messages = list(service.consume_from(now))

    assert len(messages) == 1
    assert messages[0].flattened["emailabsender"] == "keep"


def test_consumer_creation_uses_silent_logger(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeCreatedConsumer:
        def subscribe(
            self,
            topics: list[str],
            on_assign: Any = None,
            on_revoke: Any = None,
            on_lost: Any = None,
        ) -> None:
            return None

        def poll(self, timeout: float) -> None:
            return None

        def close(self) -> None:
            return None

    def _consumer_factory(
        config: dict[str, object],
        logger: object | None = None,
    ) -> _FakeCreatedConsumer:
        captured["config"] = config
        captured["logger"] = logger
        return _FakeCreatedConsumer()

    monkeypatch.setattr(kafka_service_module, "Consumer", _consumer_factory)

    ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=None,
    )

    assert captured["logger"] is kafka_service_module._KAFKA_CLIENT_LOGGER


def test_kafka_consumer_decodes_invalid_utf8_key_as_none() -> None:
    now = datetime.now(UTC)
    records = [
        FakeRecord(
            _encode_avro_payload("sender", "subject", ["A"]),
            timestamp=now + timedelta(seconds=1),
            key=b"\xff\xfe",
        ),
    ]
    service = ActualEventReader(
        kafka_settings=_kafka_settings(),
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
        consumer=FakeConsumer(records),
    )

    messages = list(service.consume_from(now))

    assert len(messages) == 1
    assert messages[0].key is None


def test_create_consumer_uses_security_and_default_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeConfluentConsumer:
        def __init__(self, config: dict[str, object]) -> None:
            captured["config"] = config

        def subscribe(
            self,
            topics: list[str],
            on_assign: Any = None,
            on_revoke: Any = None,
            on_lost: Any = None,
        ) -> None:
            pass

        def poll(self, timeout: float) -> None:
            return None

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "simple_e2e_tester.kafka_consumption.actual_event_reader.Consumer",
        FakeConfluentConsumer,
    )
    settings = KafkaSettings(
        bootstrap_servers=("kafka-1:9092", "kafka-2:9092"),
        topic="topic-a",
        group_id=None,
        security={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "alice",
            "sasl.password": "secret",
        },
        timeout_seconds=10,
        poll_interval_ms=100,
        auto_offset_reset="latest",
    )

    ActualEventReader(
        kafka_settings=settings,
        schema_fields=_flattened_fields(),
        schema_config=_schema_config(),
    )

    assert "config" in captured
    config = captured["config"]
    assert isinstance(config, dict)
    assert config["bootstrap.servers"] == "kafka-1:9092,kafka-2:9092"
    assert config["group.id"] == "simple-e2e-tester"
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.username"] == "alice"
