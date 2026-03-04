# Spec Alignment

This document summarizes current implementation behavior, gaps, and extensions.

Status legend:
- `Implemented`: behavior exists and is covered by tests.
- `Partial`: some but not all required behavior is implemented.
- `Gap`: expected behavior is not implemented.
- `Extension`: additional behavior beyond spec.

## Summary matrix

| Area | Status | Notes |
| --- | --- | --- |
| CLI commands (`generate-template`, `run`) | Implemented | Transport-aware run mode exists and is test-covered. |
| Config file requirement and parsing | Implemented | YAML/JSON supported; schema/matching/transport validation enforced. |
| Schema flattening and collision checks | Implemented | AVSC + JSON Schema flattening with duplicate-path errors. |
| Template generation (grouped headers + schema sheet) | Implemented | `Metadata/Input/Expected` + `Schema` sheet generated. |
| Template ingestion sanity checks | Partial | IDs, FROM/SUBJECT uniqueness, header/schema alignment are validated; merged-cell shape is not strictly validated. |
| SMTP send concurrency and composition | Implemented | Parallel send and attachment support are implemented. |
| REST transport execution | Implemented | Per-row synchronous REST requests with JSON-object response checks. |
| Kafka consume window (>= run start) | Implemented | Timestamp filter exists in consumer service. |
| Kafka decode in run mode for JSON Schema config | Implemented | Run mode decodes JSON object payloads when `schema.json_schema` is configured. |
| Stop condition `all matched OR timeout` | Implemented | Kafka consumption now stops as soon as all enabled expected events are matched; timeout remains the fallback stop condition. |
| Output workbook structure (`Actual` + `Match` + RunInfo/Schema) | Implemented | Includes duplicate rows for multiple matches. |
| Validation semantics (empty expected cell ignored, `MUSS_LEER_SEIN`, float tolerance) | Implemented | Includes German decimal parsing for tolerance expressions; `IGNORE` is not treated as a special token. |
| Logging requirements | Gap | Structured summary/conflict logging is minimal currently. |
| Strict CLI contract from spec | Extension | `generate-config` exists although it is outside the strict mode contract. |

## Key implementation-specific notes

- REST mode is supported and defaults for modern transport-aware configs.
- In REST mode, run metadata writes `RunInfo.kafka_topic = REST_DIRECT`.
- In REST mode, execution aborts outbound requests after 3 consecutive REST request failures.
- If `schema.json_schema` is configured for Kafka flow, run mode expects UTF-8 JSON object payloads (including Confluent wire header support).
- If `kafka.group_id` is omitted in Kafka flow, current code falls back to a fixed consumer group ID.
- Attachment path detection includes a permissive suffix heuristic not explicitly required by the spec.

## Documentation intent

This file is descriptive and intended as an implementation-status reference.
