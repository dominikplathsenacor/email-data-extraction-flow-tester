# simple-e2e-tester

Schema-driven E2E test runner for validating `Email -> Kafka` and `REST` extraction flows.

It provides CLI commands for setup and execution:
- `init`: create `.venv`, install `uv` in that virtual environment, and sync dependency groups.
- `generate-config`: create a commented YAML test configuration scaffold with placeholders.
- `generate-template`: build a test template workbook from an AVSC or JSON event schema.
- `run`: execute test rows from a filled test template using configured transport (`rest` or `email_kafka`), validate expected vs actual values, and write a result workbook.

The implementation follows a domain-first structure and TDD workflow as defined in `AGENTS.md`.

## Current implementation status

Implemented and verified by tests:
- Configuration loading and validation (YAML/JSON).
- Event schema flattening for AVSC and JSON Schema.
- Template generation (`Metadata`, `Input`, `Expected`) plus `Schema` sheet.
- Template ingestion and sanity checks (IDs, sender/subject uniqueness, email format, schema column alignment).
- SMTP email composition/sending with concurrency and attachment handling.
- Kafka consumption with AVSC binary decoding and JSON payload decoding for JSON Schema (including Confluent wire header handling).
- Matching and validation rules (empty expected cell -> ignored, `MUSS_LEER_SEIN`, float tolerance syntax like `3,14+-0,2`) over expected/actual events.
- Result workbook writing (`Actual`, `Match`, `RunInfo`, `Schema`).

Known behavior to be aware of:
- `run --dry-run` is supported for smoke-style runs that skip SMTP/Kafka interactions while still producing a results workbook.

See `/docs/spec-alignment.md` for a precise spec-vs-implementation map.

## Quick start

1. Initialize the local environment:

```bash
python e2k-tester init
```

Manual dependency-sync alternative:

```bash
uv sync
```

For development tooling (tests/lint/type-check), install all groups:

```bash
uv sync --all-groups
```

2. Generate a test template:
```bash
python e2k-tester generate-template \
  --config /path/to/config.yaml \
  --output /path/to/template.xlsx
```

Optional first step to scaffold a test configuration:
```bash
python e2k-tester generate-config --output /path/to/config.yaml
```

3. Fill test rows in the generated test template workbook (`TestCases` sheet).

4. Run the test execution:
```bash
python e2k-tester run \
  --config /path/to/config.yaml \
  --input /path/to/template.xlsx \
  --output-dir /path/to/results
```

Optional smoke mode:
```bash
python e2k-tester run \
  --config /path/to/config.yaml \
  --input /path/to/template.xlsx \
  --dry-run
```

## Documentation

- Overview and navigation: `docs/docs-overview.md`
- Architecture and domain boundaries: `docs/architecture.md`
- Configuration reference: `docs/configuration.md`
- CLI workflow and workbook semantics: `docs/workflows.md`
- Spec alignment and known gaps: `docs/spec-alignment.md`
- Testing and quality gates: `docs/testing-and-quality.md`
- Development process and constraints: `AGENTS.md`

## Repository layout

```text
src/simple_e2e_tester/
  configuration/
  schema_management/
  template_generation/
  template_ingestion/
  email_sending/
  kafka_consumption/
  run_execution/
  matching_validation/
  results_writing/
  cli.py
tests/
  <domain>/{unit,integration}
samples/
docs/
```
