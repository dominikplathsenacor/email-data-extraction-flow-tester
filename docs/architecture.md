# Architecture

## Design style

The project is organized by business domain (DDD-style), aligned with `AGENTS.md`.
Each domain owns its entities, services, and tests.

## Domain map

| Domain | Responsibility | Key entry points |
| --- | --- | --- |
| `configuration` | Parse and validate YAML/JSON test configuration files. | `load_configuration` |
| `schema_management` | Parse AVSC/JSON event schema and flatten field paths. | `load_schema_document`, `flatten_schema` |
| `template_generation` | Generate test template workbooks from schema fields. | `generate_template_workbook` |
| `template_ingestion` | Read and validate filled test templates into test case objects. | `read_template` |
| `email_sending` | Compose and send emails via SMTP (parallelized). | `compose_email`, `EmailSender` |
| `kafka_consumption` | Consume Kafka records from run start and decode Avro payloads. | `KafkaConsumerService.consume_from` |
| `rest_execution` | Call REST extraction API per testcase and normalize responses. | `RestExecutionTransport`, `RequestsRestRequestClient` |
| `matching_validation` | Match actual events to expected events and validate expected values. | `match_and_validate` |
| `results_writing` | Produce output workbook with actuals, match result, and run metadata. | `write_results_workbook` |
| `run_execution` | Execute the run use case from request to output workbook. | `execute_email_kafka_validation_run` |
| `cli` | Expose command-line adapters for `generate-template` and `run`. | `cli` group commands |

## Runtime flows

## `generate-template`

1. Load test configuration.
2. Parse and flatten event schema.
3. Generate test template workbook with:
   - `TestCases` sheet: grouped headers `Metadata | Input | Expected`
   - `Schema` sheet: `schema_type`, `schema_hash`, `schema_text`

## `run`

1. Load test configuration and flatten event schema.
2. Read/validate input workbook.
3. Record `run_start` timestamp.
4. Resolve execution transport:
   - `rest` (default for modern transport-aware configs)
   - `email_kafka` (default for legacy schema-only configs)
5. Execute transport:
   - `rest`: synchronous request per enabled row
   - `email_kafka`: SMTP send + Kafka consume window from `run_start`
6. Match/validate records against test cases.
7. Write output workbook:
   - Original columns + `Actual` + `Match`
   - `Schema` and `RunInfo` sheets

## External boundaries

- HTTP REST service (`requests`) for REST extraction flow.
- SMTP server (`smtplib`) for email transport.
- Kafka broker (`confluent-kafka`) for Kafka transport.
- Excel IO (`openpyxl`) for template/result workbooks.

## Architectural invariants currently enforced

- Exactly one schema type per configured schema slot must be configured (`avsc` or `json_schema`).
- Transport defaults are schema-shape aware (`rest` for modern config shape, `email_kafka` for legacy shape).
- In REST mode, `kafka` config is optional and run metadata uses sentinel topic `REST_DIRECT`.
- Matching fields (`matching.from_field`, `matching.subject_field`) must exist in flattened event schema.
- Template column order must match generated schema-derived fields.
- IDs must be unique; enabled rows must have unique `(FROM, SUBJECT)` pairs.
