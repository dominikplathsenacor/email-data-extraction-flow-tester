"""Configuration loader service."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from simple_e2e_tester.schema_management.schema_projection import (
    SchemaError,
    flatten_schema,
    load_schema_document,
)

from .runtime_settings import (
    Configuration,
    KafkaSettings,
    MailSettings,
    MatchingConfig,
    RestSettings,
    SchemaConfig,
    SMTPSettings,
    TransportSettings,
)


class ConfigurationError(Exception):
    """Raised when the configuration file is invalid."""


def load_configuration(config_path: Path | str) -> Configuration:
    """Load and validate the configuration file."""
    path = Path(config_path)
    if not path.exists():
        raise ConfigurationError(f"Configuration file not found: {path}")

    text = path.read_text(encoding="utf-8")
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as exc:  # pragma: no cover - exercised indirectly
        raise ConfigurationError(f"Failed to parse configuration file: {exc}") from exc

    if parsed is None:
        parsed = {}

    if not isinstance(parsed, Mapping):
        raise ConfigurationError("Configuration root must be a mapping.")

    schema_raw = parsed.get("schema")
    transport = _parse_transport_section(parsed.get("transport"), schema_raw)
    schema, response_schema, kafka_event_schema = _parse_schema_section(
        schema_raw, path.parent, transport
    )
    try:
        schema_document = load_schema_document(response_schema)
        flattened_fields = flatten_schema(schema_document)
    except SchemaError as exc:
        raise ConfigurationError(str(exc)) from exc
    field_names = {field.path for field in flattened_fields}

    matching = _parse_matching_section(parsed.get("matching"), available_fields=field_names)
    smtp = _parse_smtp_section(parsed.get("smtp"))
    mail = _parse_mail_section(parsed.get("mail"))
    kafka = _parse_kafka_section(parsed.get("kafka"), transport)
    rest = _parse_rest_section(parsed.get("rest"), transport)

    return Configuration(
        path=path,
        schema=schema,
        response_schema=response_schema,
        kafka_event_schema=kafka_event_schema,
        transport=transport,
        matching=matching,
        smtp=smtp,
        mail=mail,
        kafka=kafka,
        rest=rest,
    )


def _parse_schema_section(
    value: Any, base_path: Path, transport: TransportSettings
) -> tuple[SchemaConfig, SchemaConfig, SchemaConfig | None]:
    """Parse configured schemas and return `(schema, response_schema, kafka_event_schema)`.

    Tuple elements:
      1. `schema`: legacy compatibility alias used by existing call sites.
      2. `response_schema`: schema used to flatten fields for matching/validation.
      3. `kafka_event_schema`: optional explicit Kafka-event schema from
         `schema.kafka_event` for mode-specific execution wiring.

    Notes:
      - Legacy single-schema configs (`schema.avsc` / `schema.json_schema`) map to all
        active schema slots to preserve current behavior.
      - In `rest` mode, `response_schema` resolves from `schema.rest_response`.
      - In `email_kafka` mode, `response_schema` resolves from `schema.kafka_event`.
    """
    section = _require_mapping(value, "schema")

    has_legacy_schema = any(section.get(key) for key in ("avsc", "json_schema"))
    if has_legacy_schema:
        schema = _parse_single_schema_config(section, base_path)
        return schema, schema, schema

    rest_raw = section.get("rest_response")
    kafka_raw = section.get("kafka_event")
    rest_schema = (
        _parse_single_schema_config(_require_mapping(rest_raw, "schema.rest_response"), base_path)
        if rest_raw is not None
        else None
    )
    kafka_schema = (
        _parse_single_schema_config(_require_mapping(kafka_raw, "schema.kafka_event"), base_path)
        if kafka_raw is not None
        else None
    )

    if transport.mode == "rest":
        if rest_schema is None:
            raise ConfigurationError(
                "schema.rest_response must be provided when transport.mode is 'rest'."
            )
        return rest_schema, rest_schema, kafka_schema

    if kafka_schema is None:
        raise ConfigurationError(
            "schema.kafka_event must be provided when transport.mode is 'email_kafka'."
        )
    return kafka_schema, kafka_schema, kafka_schema


def _parse_single_schema_config(value: Any, base_path: Path) -> SchemaConfig:
    section = _require_mapping(value, "schema")
    type_candidates = [key for key in ("avsc", "json_schema") if section.get(key)]
    if len(type_candidates) != 1:
        raise ConfigurationError(
            "Exactly one event schema type (avsc or json_schema) must be provided."
        )

    schema_type = type_candidates[0]
    definition = section[schema_type]
    text, source_path = _load_schema_definition(definition, base_path)
    if not text.strip():
        raise ConfigurationError("Schema text cannot be empty.")

    return SchemaConfig(schema_type=schema_type, text=text, source_path=source_path)


def _parse_transport_section(value: Any, schema_value: Any) -> TransportSettings:
    if value is None:
        schema_mapping = schema_value if isinstance(schema_value, Mapping) else {}
        has_legacy_schema = any(schema_mapping.get(key) for key in ("avsc", "json_schema"))
        if has_legacy_schema:
            return TransportSettings(mode="email_kafka")
        return TransportSettings(mode="rest")
    section = _require_mapping(value, "transport")
    mode = _require_non_empty_string(section.get("mode"), "transport.mode").lower()
    if mode not in {"rest", "email_kafka"}:
        raise ConfigurationError("transport.mode must be either 'rest' or 'email_kafka'.")
    return TransportSettings(mode=mode)


def _load_schema_definition(definition: Any, base_path: Path) -> tuple[str, Path | None]:
    if isinstance(definition, str):
        return definition, None
    mapping = _require_mapping(definition, "schema definition")
    inline = mapping.get("inline")
    path_value = mapping.get("path")
    if inline and path_value:
        raise ConfigurationError("Schema definition must not set both inline and path.")
    if inline:
        if not isinstance(inline, str):
            raise ConfigurationError("Schema inline value must be a string.")
        return inline, None
    if path_value:
        if not isinstance(path_value, str):
            raise ConfigurationError("Schema path must be a string.")
        schema_path = _resolve_path(base_path, path_value)
        if not schema_path.exists():
            raise ConfigurationError(f"Schema file not found: {schema_path}")
        text = schema_path.read_text(encoding="utf-8")
        return text, schema_path
    raise ConfigurationError("Schema definition requires either inline or path.")


def _parse_matching_section(value: Any, *, available_fields: set[str]) -> MatchingConfig:
    section = _require_mapping(value, "matching")
    from_field = _require_non_empty_string(section.get("from_field"), "matching.from_field")
    subject_field = _require_non_empty_string(
        section.get("subject_field"), "matching.subject_field"
    )
    for field_name, label in (
        (from_field, "matching.from_field"),
        (subject_field, "matching.subject_field"),
    ):
        if field_name not in available_fields:
            raise ConfigurationError(f"{label} '{field_name}' does not exist in schema.")
    return MatchingConfig(from_field=from_field, subject_field=subject_field)


def _parse_smtp_section(value: Any) -> SMTPSettings:
    section = _require_mapping(value, "smtp")
    host = _require_non_empty_string(section.get("host"), "smtp.host")
    port = _require_positive_int(section.get("port"), "smtp.port")
    username = _optional_string(section.get("username"), "smtp.username")
    password = _optional_string(section.get("password"), "smtp.password")
    use_ssl = bool(section.get("use_ssl", False))
    use_starttls = section.get("use_starttls")
    use_starttls_bool = not use_ssl if use_starttls is None else bool(use_starttls)
    timeout_seconds = _require_positive_int(
        section.get("timeout_seconds", 30), "smtp.timeout_seconds"
    )
    parallelism = _require_positive_int(section.get("parallelism", 4), "smtp.parallelism")
    return SMTPSettings(
        host=host,
        port=port,
        username=username,
        password=password,
        use_starttls=use_starttls_bool,
        use_ssl=use_ssl,
        timeout_seconds=timeout_seconds,
        parallelism=parallelism,
    )


def _parse_mail_section(value: Any) -> MailSettings:
    section = _require_mapping(value, "mail")
    to_address = _require_non_empty_string(section.get("to_address"), "mail.to_address")
    cc = _normalize_string_sequence(section.get("cc"))
    bcc = _normalize_string_sequence(section.get("bcc"))
    return MailSettings(to_address=to_address, cc=cc, bcc=bcc)


def _parse_kafka_section(value: Any, transport: TransportSettings) -> KafkaSettings:
    if value is None:
        if transport.mode == "rest":
            return KafkaSettings(
                bootstrap_servers=("REST_DIRECT",),
                topic="REST_DIRECT",
                group_id=None,
                security={},
                timeout_seconds=600,
                poll_interval_ms=500,
                auto_offset_reset="latest",
            )
        raise ConfigurationError("Configuration section 'kafka' is required.")
    section = _require_mapping(value, "kafka")
    bootstrap_servers = _normalize_bootstrap_servers(section.get("bootstrap_servers"))
    topic = _require_non_empty_string(section.get("topic"), "kafka.topic")
    group_id = _optional_string(section.get("group_id"), "kafka.group_id")
    security = section.get("security") or {}
    if not isinstance(security, Mapping):
        raise ConfigurationError("kafka.security must be a mapping.")
    timeout_seconds = _require_positive_int(
        section.get("timeout_seconds", 600), "kafka.timeout_seconds"
    )
    poll_interval_ms = _require_positive_int(
        section.get("poll_interval_ms", 500), "kafka.poll_interval_ms"
    )
    auto_offset_reset_raw = section.get("auto_offset_reset", "latest")
    auto_offset_reset = _require_non_empty_string(
        auto_offset_reset_raw, "kafka.auto_offset_reset"
    ).lower()
    return KafkaSettings(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        security=dict(security),
        timeout_seconds=timeout_seconds,
        poll_interval_ms=poll_interval_ms,
        auto_offset_reset=auto_offset_reset,
    )


def _parse_rest_section(value: Any, transport: TransportSettings) -> RestSettings | None:
    if value is None:
        if transport.mode == "rest":
            raise ConfigurationError("Configuration section 'rest' is required.")
        return None

    section = _require_mapping(value, "rest")
    defaults = _require_mapping(section.get("defaults"), "rest.defaults")
    defaults_required = (
        "ag",
        "dokart",
        "dokrefuid",
        "eingangsdatum",
        "flowid",
        "ordnungsbegriff",
        "referenztyp",
    )
    parsed_defaults = {
        key: _require_non_empty_string(defaults.get(key), f"rest.defaults.{key}")
        for key in defaults_required
    }
    method = _require_non_empty_string(section.get("method", "POST"), "rest.method").upper()
    return RestSettings(
        base_url=_require_non_empty_string(section.get("base_url"), "rest.base_url"),
        path=_require_non_empty_string(section.get("path"), "rest.path"),
        method=method,
        timeout_seconds=_require_positive_int(
            section.get("timeout_seconds", 30), "rest.timeout_seconds"
        ),
        retry_count=_require_non_negative_int(section.get("retry_count", 2), "rest.retry_count"),
        retry_backoff_ms=_require_non_negative_int(
            section.get("retry_backoff_ms", 250), "rest.retry_backoff_ms"
        ),
        defaults=parsed_defaults,
    )


def _normalize_bootstrap_servers(value: Any) -> tuple[str, ...]:
    if value is None:
        raise ConfigurationError("kafka.bootstrap_servers is required.")
    servers: list[str] = []
    if isinstance(value, str):
        servers = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, Sequence):
        for item in value:
            if not isinstance(item, str):
                raise ConfigurationError("kafka.bootstrap_servers entries must be strings.")
            stripped = item.strip()
            if stripped:
                servers.append(stripped)
    else:
        raise ConfigurationError("kafka.bootstrap_servers must be a string or list of strings.")
    if not servers:
        raise ConfigurationError("kafka.bootstrap_servers must contain at least one server.")
    return tuple(servers)


def _normalize_string_sequence(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        stripped = value.strip()
        return (stripped,) if stripped else ()
    if isinstance(value, Sequence):
        normalized = []
        for item in value:
            if not isinstance(item, str):
                raise ConfigurationError("mail.cc/bcc entries must be strings.")
            stripped = item.strip()
            if stripped:
                normalized.append(stripped)
        return tuple(normalized)
    raise ConfigurationError("mail.cc/bcc must be a string or list of strings.")


def _resolve_path(base_path: Path, raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        return (base_path / candidate).resolve()
    return candidate


def _require_mapping(value: Any, section_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"Configuration section '{section_name}' is required.")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ConfigurationError(f"{field_name} must be a string.")
    stripped = value.strip()
    if not stripped:
        raise ConfigurationError(f"{field_name} must not be empty.")
    return stripped


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigurationError(f"{field_name} must be a string.")
    stripped = value.strip()
    return stripped or None


def _require_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ConfigurationError(f"{field_name} must be an integer.")
    if not isinstance(value, int):
        raise ConfigurationError(f"{field_name} must be an integer.")
    if value <= 0:
        raise ConfigurationError(f"{field_name} must be greater than zero.")
    return value


def _require_non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ConfigurationError(f"{field_name} must be an integer.")
    if not isinstance(value, int):
        raise ConfigurationError(f"{field_name} must be an integer.")
    if value < 0:
        raise ConfigurationError(f"{field_name} must be greater than or equal to zero.")
    return value
