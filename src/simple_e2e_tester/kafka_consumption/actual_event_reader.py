"""Kafka consumer wrapper service."""

from __future__ import annotations

import json
import logging
import struct
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from confluent_kafka import Consumer, KafkaError
from simple_e2e_tester.configuration.runtime_settings import KafkaSettings, SchemaConfig
from simple_e2e_tester.schema_management.schema_models import FlattenedField

from .actual_event_messages import ActualEventMessage

_KAFKA_CLIENT_LOGGER = logging.getLogger("simple_e2e_tester.kafka.client")
_KAFKA_CLIENT_LOGGER.addHandler(logging.NullHandler())
_KAFKA_CLIENT_LOGGER.propagate = False
_KAFKA_CLIENT_LOGGER.setLevel(logging.CRITICAL + 1)


class ActualEventDecodeError(Exception):
    """Raised when a Kafka message cannot be decoded according to the schema."""


class KafkaConsumerProtocol(Protocol):
    """Protocol implemented by both real and fake consumers."""

    def subscribe(
        self,
        topics: list[str],
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
    ) -> None: ...

    def poll(self, timeout: float) -> _KafkaRawMessage | None: ...

    def close(self) -> None: ...


class _KafkaRawMessage(Protocol):
    """Subset of Kafka message API required by the service."""

    def error(self) -> Any: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...

    def timestamp(self) -> tuple[int, int | None]: ...


class ActualEventReader:
    """Service that consumes Kafka messages from a topic and yields flattened payloads."""

    def __init__(
        self,
        kafka_settings: KafkaSettings,
        schema_fields: Sequence[FlattenedField],
        schema_config: SchemaConfig,
        consumer: KafkaConsumerProtocol | None = None,
    ) -> None:
        self._settings = kafka_settings
        self._schema_fields = schema_fields
        self._schema_config = schema_config
        self._consumer = consumer or self._create_consumer()
        self._avro_schema: Mapping[str, Any] | None = None
        self._named_types: dict[str, Mapping[str, Any]] = {}
        if self._schema_config.schema_type == "avsc":
            self._avro_schema = self._load_avro_schema(self._schema_config.text)

    def consume_from(self, start_time: datetime) -> Iterator[ActualEventMessage]:
        """Yield Kafka messages whose timestamps are >= start_time."""
        self._consumer.subscribe([self._settings.topic])
        end_time = start_time + timedelta(seconds=self._settings.timeout_seconds)
        try:
            while datetime.now(UTC) < end_time:
                message = self._consumer.poll(
                    timeout=self._settings.poll_interval_ms / 1000.0
                )
                if message is None:
                    continue
                if message.error():
                    partition_eof_code = getattr(KafkaError, "_PARTITION_EOF", None)
                    if (
                        partition_eof_code is not None
                        and message.error().code() == partition_eof_code
                    ):
                        continue
                    raise ActualEventDecodeError(f"Kafka error: {message.error()}")
                timestamp_type, timestamp_value = message.timestamp()
                if timestamp_value is None:
                    continue
                message_time = datetime.fromtimestamp(timestamp_value / 1000, tz=UTC)
                if message_time < start_time:
                    continue
                decoded_value = self._decode_message(message)
                flattened = self._flatten(decoded_value)
                yield ActualEventMessage(
                    key=self._decode_key(message.key()),
                    value=decoded_value,
                    timestamp=message_time,
                    flattened=flattened,
                )
        finally:
            self._consumer.close()

    def _decode_message(self, message: _KafkaRawMessage) -> Mapping[str, Any]:
        payload = message.value()
        if payload is None:
            raise ActualEventDecodeError("Received empty message payload.")
        payload_bytes = bytes(payload)

        if self._schema_config.schema_type == "avsc":
            decoded = self._decode_avro_payload(payload_bytes)
        elif self._schema_config.schema_type == "json_schema":
            decoded = self._decode_json_payload(payload_bytes)
        else:
            raise ActualEventDecodeError(
                f"Unsupported schema type for Kafka decoding: {self._schema_config.schema_type}"
            )
        if not isinstance(decoded, Mapping):
            raise ActualEventDecodeError("Decoded payload must be an object.")
        return decoded

    def _decode_json_payload(self, payload: bytes) -> Mapping[str, Any]:
        for candidate_payload in _json_payload_candidates(payload):
            try:
                raw_json = candidate_payload.decode("utf-8")
            except UnicodeDecodeError:
                continue
            try:
                decoded = json.loads(raw_json)
            except json.JSONDecodeError:
                continue
            if not isinstance(decoded, Mapping):
                raise ActualEventDecodeError(
                    "Decoded JSON payload root must be an object."
                )
            return decoded
        raise ActualEventDecodeError(
            "JSON payload decoding failed. Expected UTF-8 encoded JSON object payload."
        )

    def _decode_avro_payload(self, payload: bytes) -> Mapping[str, Any]:
        if self._avro_schema is None:
            raise ActualEventDecodeError("AVSC schema is not initialized.")
        # Support Confluent Schema Registry wire format:
        # magic byte 0 + 4-byte schema id + avro binary payload.
        if len(payload) >= 5 and payload[0] == 0:
            payload = payload[5:]
        reader = _AvroBinaryReader(payload)
        decoded = self._decode_avro_node(self._avro_schema, reader)
        if reader.remaining > 0:
            raise ActualEventDecodeError("Avro payload contains trailing bytes.")
        if not isinstance(decoded, Mapping):
            raise ActualEventDecodeError("Decoded Avro root must be a record object.")
        return decoded

    def _decode_avro_node(self, schema: Any, reader: _AvroBinaryReader) -> Any:
        if isinstance(schema, list):
            index = reader.read_long()
            if index < 0 or index >= len(schema):
                raise ActualEventDecodeError(f"Avro union index out of range: {index}")
            return self._decode_avro_node(schema[index], reader)

        if isinstance(schema, str):
            return self._decode_avro_type(schema, None, reader)

        if not isinstance(schema, Mapping):
            raise ActualEventDecodeError("Invalid AVSC node encountered during decode.")

        node_type = schema.get("type")
        if isinstance(node_type, list):
            return self._decode_avro_node(node_type, reader)
        if isinstance(node_type, Mapping):
            return self._decode_avro_node(node_type, reader)
        if isinstance(node_type, str):
            return self._decode_avro_type(node_type, schema, reader)

        raise ActualEventDecodeError("AVSC node is missing a valid 'type'.")

    def _decode_avro_type(
        self,
        type_name: str,
        schema_node: Mapping[str, Any] | None,
        reader: _AvroBinaryReader,
    ) -> Any:
        if type_name == "null":
            return None
        if type_name == "boolean":
            return reader.read_boolean()
        if type_name in {"int", "long"}:
            return reader.read_long()
        if type_name == "float":
            return reader.read_float()
        if type_name == "double":
            return reader.read_double()
        if type_name == "bytes":
            return reader.read_bytes()
        if type_name == "string":
            return reader.read_string()

        if type_name == "record":
            if schema_node is None:
                raise ActualEventDecodeError(
                    "Record definition is missing from AVSC schema."
                )
            fields = schema_node.get("fields")
            if not isinstance(fields, Sequence):
                raise ActualEventDecodeError("Record schema requires a fields array.")
            record_output: dict[str, Any] = {}
            for field in fields:
                if not isinstance(field, Mapping) or "name" not in field:
                    raise ActualEventDecodeError("Record field definition is invalid.")
                record_output[str(field["name"])] = self._decode_avro_node(
                    field.get("type"), reader
                )
            return record_output

        if type_name == "enum":
            if schema_node is None:
                raise ActualEventDecodeError(
                    "Enum definition is missing from AVSC schema."
                )
            symbols = schema_node.get("symbols")
            if not isinstance(symbols, Sequence):
                raise ActualEventDecodeError("Enum schema requires a symbols array.")
            index = reader.read_long()
            if index < 0 or index >= len(symbols):
                raise ActualEventDecodeError(f"Avro enum index out of range: {index}")
            return symbols[index]

        if type_name == "array":
            if schema_node is None:
                raise ActualEventDecodeError(
                    "Array definition is missing from AVSC schema."
                )
            items_schema = schema_node.get("items")
            items: list[Any] = []
            while True:
                count = reader.read_long()
                if count == 0:
                    break
                if count < 0:
                    _block_size = reader.read_long()
                    count = -count
                for _ in range(count):
                    items.append(self._decode_avro_node(items_schema, reader))
            return items

        if type_name == "map":
            if schema_node is None:
                raise ActualEventDecodeError(
                    "Map definition is missing from AVSC schema."
                )
            values_schema = schema_node.get("values")
            map_output: dict[str, Any] = {}
            while True:
                count = reader.read_long()
                if count == 0:
                    break
                if count < 0:
                    _block_size = reader.read_long()
                    count = -count
                for _ in range(count):
                    key = reader.read_string()
                    map_output[key] = self._decode_avro_node(values_schema, reader)
            return map_output

        if type_name == "fixed":
            if schema_node is None:
                raise ActualEventDecodeError(
                    "Fixed definition is missing from AVSC schema."
                )
            size = schema_node.get("size")
            if not isinstance(size, int) or size < 0:
                raise ActualEventDecodeError(
                    "Fixed schema requires a non-negative integer size."
                )
            return reader.read_exact(size)

        named_type = self._named_types.get(type_name)
        if named_type is None:
            raise ActualEventDecodeError(
                f"Unsupported or unknown Avro type reference: {type_name}"
            )
        return self._decode_avro_node(named_type, reader)

    def _load_avro_schema(self, schema_text: str) -> Mapping[str, Any]:
        try:
            root = json.loads(schema_text)
        except json.JSONDecodeError as exc:
            raise ActualEventDecodeError(f"Invalid avsc schema JSON: {exc}") from exc
        if not isinstance(root, Mapping):
            raise ActualEventDecodeError("AVSC schema root must be a JSON object.")
        self._named_types.clear()
        self._register_named_types(root)
        return root

    def _register_named_types(self, schema: Any) -> None:
        if isinstance(schema, list):
            for node in schema:
                self._register_named_types(node)
            return

        if not isinstance(schema, Mapping):
            return

        node_type = schema.get("type")
        if isinstance(node_type, Mapping):
            self._register_named_types(node_type)
            return
        if isinstance(node_type, list):
            self._register_named_types(node_type)
            return
        if not isinstance(node_type, str):
            return

        if node_type in {"record", "enum", "fixed"}:
            name = schema.get("name")
            if isinstance(name, str) and name:
                self._named_types.setdefault(name, schema)

        if node_type == "record":
            fields = schema.get("fields")
            if isinstance(fields, Sequence):
                for field in fields:
                    if isinstance(field, Mapping):
                        self._register_named_types(field.get("type"))
        elif node_type == "array":
            self._register_named_types(schema.get("items"))
        elif node_type == "map":
            self._register_named_types(schema.get("values"))

    def _flatten(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        flattened = {}
        for field in self._schema_fields:
            value: Any = payload
            parts = field.path.split(".")
            try:
                for part in parts:
                    if isinstance(value, Mapping):
                        value = value.get(part)
                    else:
                        raise KeyError(part)
            except KeyError as exc:
                raise ActualEventDecodeError(
                    f"Missing schema field {field.path}"
                ) from exc
            flattened[field.path] = value
        return flattened

    @staticmethod
    def _decode_key(key: bytes | None) -> str | None:
        if key is None:
            return None
        try:
            return key.decode("utf-8")
        except UnicodeDecodeError:
            return None

    def _create_consumer(self) -> KafkaConsumerProtocol:
        config: dict[str, str | int | float | bool | None] = {
            "bootstrap.servers": ",".join(self._settings.bootstrap_servers),
            "group.id": self._settings.group_id or "simple-e2e-tester",
            "enable.auto.commit": False,
            "auto.offset.reset": self._settings.auto_offset_reset,
        }
        for key, value in self._settings.security.items():
            if isinstance(value, str | int | float | bool) or value is None:
                config[key] = value
        try:
            return cast(
                KafkaConsumerProtocol,
                Consumer(config, logger=_KAFKA_CLIENT_LOGGER),  # type: ignore[call-arg]
            )
        except TypeError:
            # Older/mock Consumer implementations may not support the logger kwarg.
            return cast(KafkaConsumerProtocol, Consumer(config))


def _json_payload_candidates(payload: bytes) -> tuple[bytes, ...]:
    candidates: list[bytes] = [payload]
    if len(payload) >= 5 and payload[0] == 0:
        # Confluent wire format: magic byte + 4-byte schema id + serialized payload.
        candidates.append(payload[5:])
    return tuple(candidates)


class _AvroBinaryReader:
    """Small Avro binary reader supporting the schema types used by this project."""

    def __init__(self, payload: bytes) -> None:
        self._data = payload
        self._offset = 0

    @property
    def remaining(self) -> int:
        """Number of unread bytes."""
        return len(self._data) - self._offset

    def read_exact(self, size: int) -> bytes:
        """Read exactly `size` bytes or raise ActualEventDecodeError."""
        if size < 0:
            raise ActualEventDecodeError("Negative read size is invalid.")
        end = self._offset + size
        if end > len(self._data):
            raise ActualEventDecodeError("Unexpected end of Avro payload.")
        chunk = self._data[self._offset : end]
        self._offset = end
        return chunk

    def read_boolean(self) -> bool:
        """Read a boolean value."""
        return self.read_exact(1) != b"\x00"

    def read_float(self) -> float:
        """Read an Avro float (32-bit little-endian)."""
        return struct.unpack("<f", self.read_exact(4))[0]

    def read_double(self) -> float:
        """Read an Avro double (64-bit little-endian)."""
        return struct.unpack("<d", self.read_exact(8))[0]

    def read_bytes(self) -> bytes:
        """Read Avro bytes."""
        length = self.read_long()
        if length < 0:
            raise ActualEventDecodeError("Negative bytes length in Avro payload.")
        return self.read_exact(length)

    def read_string(self) -> str:
        """Read Avro UTF-8 string."""
        raw = self.read_bytes()
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ActualEventDecodeError(
                "Invalid UTF-8 string in Avro payload."
            ) from exc

    def read_long(self) -> int:
        """Read Avro zigzag-encoded long."""
        shift = 0
        raw_value = 0
        while True:
            byte = self.read_exact(1)[0]
            raw_value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
            if shift > 63:
                raise ActualEventDecodeError("Avro varint is too long.")
        return (raw_value >> 1) ^ -(raw_value & 1)
