"""Schema management service tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from simple_e2e_tester.configuration.runtime_settings import SchemaConfig
from simple_e2e_tester.schema_management.schema_projection import (
    SchemaError,
    flatten_schema,
    load_schema_document,
)


def _schema_config(
    schema_type: str, text: str, source_path: Path | None = None
) -> SchemaConfig:
    return SchemaConfig(schema_type=schema_type, text=text, source_path=source_path)


def test_json_schema_flattening_uses_sample_schema() -> None:
    sample_path = (
        Path(__file__).resolve().parents[3] / "samples" / "sample-json-schema.json"
    )
    schema_text = sample_path.read_text(encoding="utf-8")

    document = load_schema_document(
        _schema_config("json_schema", schema_text, sample_path)
    )
    fields = flatten_schema(document)
    names = [field.path for field in fields]

    assert names[:5] == [
        "event_id",
        "record_id",
        "document_type",
        "channel_type",
        "sender_address",
    ]
    assert "model_output.classifications" in names
    assert "model_output.attributes.reason.value" in names
    assert any(name.endswith(".score") for name in names)
    assert not any(name.startswith("model_output.classifications.") for name in names)


def test_json_and_avsc_sample_schemas_flatten_to_same_structure() -> None:
    project_root = Path(__file__).resolve().parents[3]
    json_path = project_root / "samples" / "sample-json-schema.json"
    avsc_path = project_root / "samples" / "sample-avsc-schema.json"

    json_document = load_schema_document(
        _schema_config("json_schema", json_path.read_text(encoding="utf-8"), json_path)
    )
    avsc_document = load_schema_document(
        _schema_config("avsc", avsc_path.read_text(encoding="utf-8"), avsc_path)
    )

    json_fields = [field.path for field in flatten_schema(json_document)]
    avsc_fields = [field.path for field in flatten_schema(avsc_document)]

    assert len(json_fields) == len(avsc_fields)
    assert json_fields == avsc_fields


def test_avro_schema_flattening_supports_nested_records() -> None:
    avro_text = """
{
  "type": "record",
  "name": "Root",
  "fields": [
    {"name": "id", "type": "string"},
    {
      "name": "details",
      "type": {
        "type": "record",
        "name": "Details",
        "fields": [
          {"name": "value", "type": ["null", "int"]}
        ]
      }
    }
  ]
}
"""

    document = load_schema_document(_schema_config("avsc", avro_text))
    fields = flatten_schema(document)

    assert [field.path for field in fields] == ["id", "details.value"]


def test_detects_duplicate_field_names_after_flattening() -> None:
    schema_text = """
{
  "type": "object",
  "properties": {
    "customer": {
      "type": "object",
      "properties": {
        "address": {
          "type": "object",
          "properties": {
            "zip": {"type": "string"}
          }
        }
      }
    },
    "customer.address.zip": {"type": "string"}
  }
}
"""

    document = load_schema_document(_schema_config("json_schema", schema_text))

    with pytest.raises(SchemaError, match="Duplicate flattened field"):
        flatten_schema(document)


def test_invalid_schema_text_raises_schema_error() -> None:
    bad_text = "{not-valid-json}"

    with pytest.raises(SchemaError):
        load_schema_document(_schema_config("json_schema", bad_text))
