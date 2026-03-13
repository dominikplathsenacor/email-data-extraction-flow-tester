"""Schema loading and flattening service."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from simple_e2e_tester.configuration.runtime_settings import SchemaConfig

from .schema_models import FlattenedField, SchemaDocument


class SchemaError(Exception):
    """Raised for schema parsing or flattening failures."""


def load_schema_document(config: SchemaConfig) -> SchemaDocument:
    """Parse schema text into a structured document."""
    try:
        root = json.loads(config.text)
    except json.JSONDecodeError as exc:
        raise SchemaError(f"Invalid {config.schema_type} schema: {exc}") from exc

    return SchemaDocument(schema_type=config.schema_type, root=root)


def flatten_schema(document: SchemaDocument) -> list[FlattenedField]:
    """Return deterministic flattened fields."""
    fields: list[FlattenedField] = []
    seen_paths: set[str] = set()

    if document.schema_type == "json_schema":
        _flatten_json_schema(
            document.root, prefix="", fields=fields, seen_paths=seen_paths
        )
    elif document.schema_type == "avsc":
        _flatten_avro_schema(
            document.root, prefix="", fields=fields, seen_paths=seen_paths
        )
    else:
        raise SchemaError(f"Unsupported schema type: {document.schema_type}")

    return fields


def _flatten_json_schema(
    node: Any, *, prefix: str, fields: list[FlattenedField], seen_paths: set[str]
) -> None:
    if not isinstance(node, Mapping):
        raise SchemaError("JSON schema nodes must be objects.")

    node_types = _json_schema_types(node)
    if "object" in node_types or ("object" not in node_types and "properties" in node):
        properties = node.get("properties")
        if isinstance(properties, Mapping):
            for key, child in properties.items():
                child_path = key if not prefix else f"{prefix}.{key}"
                _flatten_json_schema(
                    child, prefix=child_path, fields=fields, seen_paths=seen_paths
                )
            return
        if "object" in node_types:
            _register_field(prefix, node, fields, seen_paths)
            return

    if "array" in node_types:
        _register_field(prefix, node, fields, seen_paths)
        return

    if prefix:
        _register_field(prefix, node, fields, seen_paths)
        return

    raise SchemaError("JSON schema root must define object properties.")


def _json_schema_types(node: Mapping[str, Any]) -> tuple[str, ...]:
    node_type = node.get("type")
    if isinstance(node_type, list):
        filtered = [
            value for value in node_type if isinstance(value, str) and value != "null"
        ]
        return tuple(filtered) if filtered else ("null",)
    if isinstance(node_type, str):
        return (node_type,)
    return ()


def _flatten_avro_schema(
    node: Any, *, prefix: str, fields: list[FlattenedField], seen_paths: set[str]
) -> None:
    avro_type, definition = _resolve_avro_type(node)
    if avro_type == "record":
        record_fields = definition.get("fields")
        if not isinstance(record_fields, Sequence):
            raise SchemaError("Avro record requires fields.")
        for field in record_fields:
            if not isinstance(field, Mapping) or "name" not in field:
                raise SchemaError("Avro field definitions must include a name.")
            child_path = field["name"] if not prefix else f"{prefix}.{field['name']}"
            _flatten_avro_schema(
                field.get("type"),
                prefix=child_path,
                fields=fields,
                seen_paths=seen_paths,
            )
        return
    if prefix:
        _register_field(
            prefix, definition if definition else avro_type, fields, seen_paths
        )
        return
    raise SchemaError("Avro root must be a record with named fields.")


def _resolve_avro_type(schema: Any) -> tuple[str, Mapping[str, Any] | Any]:
    if isinstance(schema, list):
        non_null = [item for item in schema if item != "null"]
        if not non_null:
            return "null", schema
        return _resolve_avro_type(non_null[0])
    if isinstance(schema, str):
        return schema, {}
    if isinstance(schema, Mapping):
        inner = schema.get("type")
        if isinstance(inner, list):
            return _resolve_avro_type(inner)
        if isinstance(inner, Mapping):
            return _resolve_avro_type(inner)
        if isinstance(inner, str):
            return inner, schema
    raise SchemaError("Unsupported Avro schema segment.")


def _register_field(
    path: str, definition: Any, fields: list[FlattenedField], seen_paths: set[str]
) -> None:
    if not path:
        raise SchemaError("Cannot register a field without a path.")
    if path in seen_paths:
        raise SchemaError(f"Duplicate flattened field detected: {path}")
    seen_paths.add(path)
    fields.append(FlattenedField(path=path, definition=definition))
