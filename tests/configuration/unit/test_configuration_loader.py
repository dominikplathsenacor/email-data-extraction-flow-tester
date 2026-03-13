"""Configuration loader tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from simple_e2e_tester.configuration.loader import (
    ConfigurationError,
    load_configuration,
)


def _write_file(path: Path, contents: str) -> Path:
    path.write_text(contents, encoding="utf-8")
    return path


def test_loads_yaml_configuration_with_defaults(tmp_path: Path) -> None:
    config_path = _write_file(
        tmp_path / "config.yaml",
        """
schema:
  avsc:
    inline: |
      {
        "type": "record",
        "name": "Root",
        "fields": [
          {
            "name": "envelope",
            "type": {
              "type": "record",
              "name": "Envelope",
              "fields": [
                {"name": "from", "type": "string"},
                {"name": "subject", "type": "string"}
              ]
            }
          }
        ]
      }
matching:
  from_field: "envelope.from"
  subject_field: "envelope.subject"
smtp:
  host: smtp.example.com
  port: 25
mail:
  to_address: "qa@example.com"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "email-results"
""",
    )

    configuration = load_configuration(config_path)

    assert configuration.schema.schema_type == "avsc"
    assert configuration.schema.text.startswith("{")
    assert configuration.smtp.use_starttls is True
    assert configuration.smtp.use_ssl is False
    assert configuration.smtp.timeout_seconds == 30
    assert configuration.smtp.parallelism == 4
    assert configuration.kafka.timeout_seconds == 600
    assert configuration.kafka.poll_interval_ms == 500
    assert configuration.kafka.auto_offset_reset == "latest"
    assert configuration.kafka.bootstrap_servers == ("localhost:9092",)
    assert configuration.mail.cc == ()
    assert configuration.mail.bcc == ()


def test_loads_json_configuration_with_schema_path(tmp_path: Path) -> None:
    schema_path = _write_file(
        tmp_path / "schema.json",
        json.dumps(
            {
                "type": "object",
                "properties": {
                    "sender": {"type": "string"},
                    "subject": {"type": "string"},
                },
            }
        ),
    )
    config_path = _write_file(
        tmp_path / "config.json",
        json.dumps(
            {
                "schema": {
                    "json_schema": {
                        "path": str(schema_path.name),
                    }
                },
                "matching": {
                    "from_field": "sender",
                    "subject_field": "subject",
                },
                "smtp": {
                    "host": "smtp.example.com",
                    "port": 465,
                    "use_ssl": True,
                },
                "mail": {
                    "to_address": "qa@example.com",
                    "cc": ["cc@example.com"],
                },
                "kafka": {
                    "bootstrap_servers": ["broker-1:9092", "broker-2:9092"],
                    "topic": "email-results",
                    "group_id": "qa-suite",
                },
            }
        ),
    )

    configuration = load_configuration(config_path)

    assert configuration.schema.schema_type == "json_schema"
    assert configuration.schema.text == schema_path.read_text(encoding="utf-8")
    assert configuration.schema.source_path == schema_path
    assert configuration.smtp.use_ssl is True
    assert configuration.smtp.use_starttls is False
    assert configuration.smtp.port == 465
    assert configuration.smtp.timeout_seconds == 30
    assert configuration.kafka.bootstrap_servers == ("broker-1:9092", "broker-2:9092")
    assert configuration.kafka.group_id == "qa-suite"
    assert configuration.mail.cc == ("cc@example.com",)
    assert configuration.mail.bcc == ()


@pytest.mark.parametrize(
    "schema_section",
    [
        {},
        {"avsc": None, "json_schema": None},
        {
            "avsc": {"inline": "{}"},
            "json_schema": {"inline": "{}"},
        },
    ],
)
def test_errors_when_schema_definition_invalid(
    tmp_path: Path, schema_section: dict
) -> None:
    config = {
        "schema": schema_section,
        "matching": {
            "from_field": "sender",
            "subject_field": "subject",
        },
        "smtp": {
            "host": "smtp.example.com",
            "port": 25,
        },
        "mail": {
            "to_address": "qa@example.com",
        },
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topic": "email-results",
        },
    }
    config_path = _write_file(tmp_path / "config.yaml", yaml_dump(config))

    with pytest.raises(ConfigurationError):
        load_configuration(config_path)


def test_errors_when_inline_and_path_are_both_defined(tmp_path: Path) -> None:
    config_path = _write_file(
        tmp_path / "config.yaml",
        """
schema:
  avsc:
    inline: "{}"
    path: "./schema.avsc"
matching:
  from_field: "sender"
  subject_field: "subject"
smtp:
  host: smtp.example.com
  port: 25
mail:
  to_address: "qa@example.com"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "email-results"
""",
    )

    with pytest.raises(ConfigurationError):
        load_configuration(config_path)


@pytest.mark.parametrize(
    "matching_section",
    [
        {},
        {"from_field": "sender"},
        {"subject_field": "subject"},
        {"from_field": " ", "subject_field": "subject"},
    ],
)
def test_errors_when_matching_fields_missing(
    tmp_path: Path, matching_section: dict
) -> None:
    config = {
        "schema": {"avsc": {"inline": "{}"}},
        "matching": matching_section,
        "smtp": {
            "host": "smtp.example.com",
            "port": 25,
        },
        "mail": {"to_address": "qa@example.com"},
        "kafka": {"bootstrap_servers": "localhost:9092", "topic": "email-results"},
    }
    config_path = _write_file(tmp_path / "config.yaml", yaml_dump(config))

    with pytest.raises(ConfigurationError):
        load_configuration(config_path)


def test_errors_when_matching_fields_not_in_schema(tmp_path: Path) -> None:
    config_path = _write_file(
        tmp_path / "config.yaml",
        """
schema:
  json_schema:
    inline: |
      {
        "type": "object",
        "properties": {
          "sender": {"type": "string"}
        }
      }
matching:
  from_field: "sender"
  subject_field: "subject"
smtp:
  host: smtp.example.com
  port: 25
mail:
  to_address: "qa@example.com"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "email-results"
""",
    )

    with pytest.raises(ConfigurationError):
        load_configuration(config_path)


def test_errors_when_configuration_root_is_not_mapping(tmp_path: Path) -> None:
    config_path = _write_file(tmp_path / "config.yaml", "[]")

    with pytest.raises(
        ConfigurationError, match="Configuration root must be a mapping"
    ):
        load_configuration(config_path)


def test_errors_when_schema_path_does_not_exist(tmp_path: Path) -> None:
    config_path = _write_file(
        tmp_path / "config.yaml",
        """
schema:
  avsc:
    path: ./missing.avsc
matching:
  from_field: "sender"
  subject_field: "subject"
smtp:
  host: smtp.example.com
  port: 25
mail:
  to_address: "qa@example.com"
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "email-results"
""",
    )

    with pytest.raises(ConfigurationError, match="Schema file not found"):
        load_configuration(config_path)


def test_errors_when_mail_cc_contains_non_string(tmp_path: Path) -> None:
    config = {
        "schema": {
            "json_schema": {
                "inline": json.dumps(
                    {
                        "type": "object",
                        "properties": {
                            "sender": {"type": "string"},
                            "subject": {"type": "string"},
                        },
                    }
                )
            }
        },
        "matching": {"from_field": "sender", "subject_field": "subject"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com", "cc": ["cc@example.com", 7]},
        "kafka": {"bootstrap_servers": "localhost:9092", "topic": "email-results"},
    }
    config_path = _write_file(tmp_path / "config.yaml", yaml_dump(config))

    with pytest.raises(ConfigurationError, match="mail.cc/bcc entries must be strings"):
        load_configuration(config_path)


def test_errors_when_kafka_security_is_not_mapping(tmp_path: Path) -> None:
    config = {
        "schema": {
            "json_schema": {
                "inline": json.dumps(
                    {
                        "type": "object",
                        "properties": {
                            "sender": {"type": "string"},
                            "subject": {"type": "string"},
                        },
                    }
                )
            }
        },
        "matching": {"from_field": "sender", "subject_field": "subject"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com"},
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topic": "email-results",
            "security": "sasl",
        },
    }
    config_path = _write_file(tmp_path / "config.yaml", yaml_dump(config))

    with pytest.raises(ConfigurationError, match="kafka.security must be a mapping"):
        load_configuration(config_path)


def yaml_dump(value: object) -> str:
    """Local helper to avoid importing yaml in tests."""
    return json.dumps(value)


def test_loads_rest_transport_configuration_with_defaults(tmp_path: Path) -> None:
    config = {
        "transport": {"mode": "rest"},
        "schema": {
            "rest_response": {
                "json_schema": {
                    "inline": json.dumps(
                        {
                            "type": "object",
                            "properties": {
                                "sender": {"type": "string"},
                                "subject": {"type": "string"},
                            },
                        }
                    )
                }
            },
        },
        "matching": {"from_field": "sender", "subject_field": "subject"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com"},
        "kafka": {"bootstrap_servers": "localhost:9092", "topic": "email-results"},
        "rest": {
            "base_url": "http://localhost:8080",
            "path": "/extract",
            "defaults": {
                "ag": "SAMPLE_AG",
                "dokart": "SAMPLE_DOKART",
                "dokrefuid": "SAMPLE_REF",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
            },
        },
    }
    config_path = _write_file(tmp_path / "config.json", json.dumps(config))

    configuration = load_configuration(config_path)

    assert configuration.transport.mode == "rest"
    assert configuration.rest is not None
    assert configuration.rest.method == "POST"
    assert configuration.rest.timeout_seconds == 30
    assert configuration.rest.retry_count == 2
    assert configuration.rest.retry_backoff_ms == 250
    assert configuration.rest.path == "/extract"
    assert configuration.rest.basic_auth_username is None
    assert configuration.rest.basic_auth_password is None
    assert configuration.response_schema.schema_type == "json_schema"


def test_loads_rest_transport_basic_auth_when_configured(tmp_path: Path) -> None:
    config = {
        "transport": {"mode": "rest"},
        "schema": {
            "rest_response": {
                "json_schema": {
                    "inline": json.dumps(
                        {
                            "type": "object",
                            "properties": {
                                "emailabsender": {"type": "string"},
                                "emailbetreff": {"type": "string"},
                            },
                        }
                    )
                }
            },
        },
        "matching": {"from_field": "emailabsender", "subject_field": "emailbetreff"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com"},
        "rest": {
            "base_url": "http://localhost:10888",
            "path": "/prediction",
            "auth": {"basic": {"username": "Example_User", "password": "Example_PW"}},
            "defaults": {
                "ag": "KV",
                "dokart": "BRIEF",
                "dokrefuid": "REF-1",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
            },
        },
    }
    config_path = _write_file(tmp_path / "config.json", json.dumps(config))

    configuration = load_configuration(config_path)

    assert configuration.rest is not None
    assert configuration.rest.basic_auth_username == "Example_User"
    assert configuration.rest.basic_auth_password == "Example_PW"


def test_errors_when_rest_transport_selected_without_rest_response_schema(
    tmp_path: Path,
) -> None:
    config = {
        "transport": {"mode": "rest"},
        "schema": {
            "kafka_event": {
                "json_schema": {
                    "inline": json.dumps(
                        {
                            "type": "object",
                            "properties": {
                                "sender": {"type": "string"},
                                "subject": {"type": "string"},
                            },
                        }
                    )
                }
            }
        },
        "matching": {"from_field": "sender", "subject_field": "subject"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com"},
        "kafka": {"bootstrap_servers": "localhost:9092", "topic": "email-results"},
        "rest": {
            "base_url": "http://localhost:8080",
            "path": "/extract",
            "defaults": {
                "ag": "SAMPLE_AG",
                "dokart": "SAMPLE_DOKART",
                "dokrefuid": "SAMPLE_REF",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
            },
        },
    }
    config_path = _write_file(tmp_path / "config.json", json.dumps(config))

    with pytest.raises(ConfigurationError, match="rest_response"):
        load_configuration(config_path)


def test_defaults_transport_mode_to_rest_when_transport_section_missing(
    tmp_path: Path,
) -> None:
    config = {
        "schema": {
            "rest_response": {
                "json_schema": {
                    "inline": json.dumps(
                        {
                            "type": "object",
                            "properties": {
                                "sender": {"type": "string"},
                                "subject": {"type": "string"},
                            },
                        }
                    )
                }
            }
        },
        "matching": {"from_field": "sender", "subject_field": "subject"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com"},
        "rest": {
            "base_url": "http://localhost:8080",
            "path": "/extract",
            "defaults": {
                "ag": "SAMPLE_AG",
                "dokart": "SAMPLE_DOKART",
                "dokrefuid": "SAMPLE_REF",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
            },
        },
    }
    config_path = _write_file(tmp_path / "config.json", json.dumps(config))

    configuration = load_configuration(config_path)

    assert configuration.transport.mode == "rest"


def test_given_rest_mode_when_kafka_section_missing_then_configuration_loads(
    tmp_path: Path,
) -> None:
    config = {
        "transport": {"mode": "rest"},
        "schema": {
            "rest_response": {
                "json_schema": {
                    "inline": json.dumps(
                        {
                            "type": "object",
                            "properties": {
                                "sender": {"type": "string"},
                                "subject": {"type": "string"},
                            },
                        }
                    )
                }
            }
        },
        "matching": {"from_field": "sender", "subject_field": "subject"},
        "smtp": {"host": "smtp.example.com", "port": 25},
        "mail": {"to_address": "qa@example.com"},
        "rest": {
            "base_url": "http://localhost:8080",
            "path": "/extract",
            "defaults": {
                "ag": "SAMPLE_AG",
                "dokart": "SAMPLE_DOKART",
                "dokrefuid": "SAMPLE_REF",
                "eingangsdatum": "2026-01-01-00.00.00.000000",
                "flowid": "FLOW-1",
                "ordnungsbegriff": "ORD-1",
                "referenztyp": "EM",
            },
        },
    }
    config_path = _write_file(tmp_path / "config.json", json.dumps(config))

    configuration = load_configuration(config_path)

    assert configuration.transport.mode == "rest"
    assert configuration.kafka.topic == "REST_DIRECT"
