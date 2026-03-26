# Testing and Quality

The repository follows a TDD-oriented workflow and domain-focused test layout.

## Test layout

```text
tests/
  <domain>/
    unit/
    integration/
```

Current domains with tests include:
- `cli`
- `configuration`
- `schema_management`
- `template_generation`
- `template_ingestion`
- `email_sending`
- `kafka_consumption`
- `matching_validation`
- `results_writing`

## Standard commands

Run all tests:

```bash
uv run pytest -q
```

Run domain-focused suites:

```bash
uv run pytest tests/<domain>/unit -q
uv run pytest tests/<domain>/integration -q
```

## Recommended quality gates

```bash
uv run ruff check .
uv run mypy src tests
uv run pylint src/simple_e2e_tester
uv run pytest -q
```

## Latest local verification during this documentation pass

- `uv run pytest -q` passed.
