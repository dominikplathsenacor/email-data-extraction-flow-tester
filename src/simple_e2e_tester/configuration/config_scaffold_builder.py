"""Configuration scaffold generation helpers."""

from __future__ import annotations

from pathlib import Path

DEFAULT_CONFIG_FILENAME = "config.yaml"

_CONFIG_SCAFFOLD_TEMPLATE = """# Test configuration template for simple-e2e-tester.
# Replace every <REQUIRED> placeholder before running generate-template or run.
# Replace <OPTIONAL> placeholders only when your setup needs them.

transport:
  # Supported: rest (default), email_kafka
  mode: "rest"

schema:
  # REST response schema (used in transport.mode=rest)
  rest_response:
    json_schema:
      # Provide either inline event schema JSON text or an event schema path.
      inline: "<REQUIRED>"
      # path: "<OPTIONAL>"
  # Kafka event schema (used in transport.mode=email_kafka)
  kafka_event:
    avsc:
      inline: "<OPTIONAL>"
      # path: "<OPTIONAL>"
    # json_schema:
    #   inline: "<OPTIONAL>"
    #   path: "<OPTIONAL>"

matching:
  # matching.from_field and matching.subject_field must be flattened event schema paths.
  from_field: "<REQUIRED>"
  subject_field: "<REQUIRED>"

validation:
  # Optional subset of flattened schema field paths to validate.
  # If omitted, all schema-derived fields are validated.
  field_names:
    - "<OPTIONAL>"

smtp:
  host: "<REQUIRED>"
  port: "<REQUIRED>"
  username: "<OPTIONAL>"
  password: "<OPTIONAL>"
  use_ssl: "<OPTIONAL>"
  use_starttls: "<OPTIONAL>"
  timeout_seconds: "<OPTIONAL>"
  parallelism: "<OPTIONAL>"

mail:
  to_address: "<REQUIRED>"
  cc:
    - "<OPTIONAL>"
  bcc:
    - "<OPTIONAL>"

kafka:
  bootstrap_servers:
    - "<REQUIRED>"
  topic: "<REQUIRED>"
  group_id: "<OPTIONAL>"
  security:
    sasl.username: "<OPTIONAL>"
    sasl.password: "<OPTIONAL>"
    security.protocol: "<OPTIONAL>"
    sasl.mechanisms: "<OPTIONAL>"
  timeout_seconds: "<OPTIONAL>"
  poll_interval_ms: "<OPTIONAL>"
  auto_offset_reset: "<OPTIONAL>"

rest:
  base_url: "<REQUIRED>"
  path: "<REQUIRED>"
  method: "POST"
  timeout_seconds: "<OPTIONAL>"
  wait_between_calls_seconds: "<OPTIONAL>"
  retry_count: "<OPTIONAL>"
  retry_backoff_ms: "<OPTIONAL>"
  auth:
    basic:
      username: "<OPTIONAL>"
      password: "<OPTIONAL>"
  # Optional static request fields included with every REST call.
  default_request_params:
    "<OPTIONAL_KEY>": "<OPTIONAL_VALUE>"
    # "<OPTIONAL_KEY_2>": "<OPTIONAL_VALUE_2>"
"""


def build_placeholder_configuration() -> str:
    """Build a YAML test configuration template with placeholders and inline guidance."""
    return _CONFIG_SCAFFOLD_TEMPLATE


def write_placeholder_configuration(output_path: Path | str) -> Path:
    """Write the placeholder test configuration template to the requested output path.

    Args:
      output_path: Destination file path for the scaffold.

    Returns:
      The resolved destination path.

    Raises:
      FileExistsError: If the destination file already exists.
      OSError: If writing the scaffold fails.
    """
    destination = Path(output_path)
    if destination.exists():
        raise FileExistsError(
            f"Test configuration file already exists: {destination.resolve()}"
        )
    destination.write_text(build_placeholder_configuration(), encoding="utf-8")
    return destination.resolve()
