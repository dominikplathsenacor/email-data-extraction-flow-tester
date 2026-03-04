# CLI Workflows

## Commands

## `init`

```bash
python e2k-tester init
```

Behavior:
- Creates `.venv` if it does not exist.
- Installs `uv` into `.venv`.
- Runs `uv sync --all-groups` using the `.venv`-local `uv` binary.

## `generate-config`

```bash
python e2k-tester generate-config [--output <config.yaml>]
```

Behavior:
- Writes a placeholder YAML test configuration with inline comments.
- Uses `<REQUIRED>` and `<OPTIONAL>` placeholders across all supported sections.
- Defaults to `./config.yaml` when `--output` is not provided.
- Fails if the target output file already exists.

## `generate-template`

```bash
python e2k-tester generate-template --config <test-configuration> --output <template.xlsx>
```

Behavior:
- Loads and validates the test configuration.
- Flattens event schema fields.
- Writes a test template workbook with sheets:
  - `TestCases`
  - `Schema`

## `run`

```bash
python e2k-tester run --config <test-configuration> --input <filled-template.xlsx> [--output-dir <dir>]
```

Optional dry run:

```bash
python e2k-tester run --config <test-configuration> --input <filled-template.xlsx> --dry-run
```

Behavior:
- Reads and validates input workbook.
- Chooses transport from config (`rest` default for modern schema configs).
- `rest` mode: sends one synchronous REST request per enabled test case.
- `email_kafka` mode: sends emails and consumes Kafka records.
- Matches actual events to expected events and validates values.
- Writes results workbook named:
  - `<input_stem>-results-<YYYYMMDD-HHMMSS>.xlsx`

## Workbook contract

## Input test template (`TestCases` sheet)

Row 1 group headers:
- `Metadata`
- `Input`
- `Expected`

Row 2 column headers:
- Metadata: `ID`, `Tags`, `Enabled`, `Notes`
- Input: `FROM`, `SUBJECT`, `BODY`, `ATTACHMENT`
- Expected: flattened event schema field names

## Result workbook (`TestCases` sheet)

Adds:
- `Actual` group with same schema-derived field list as `Expected`
- `Match` group with one `Match` column

Also includes:
- `Schema` sheet: event schema type/hash/text
- `RunInfo` sheet: run metadata and counts

## Attachment modes

`ATTACHMENT` cell is parsed line-by-line:
- file-path mode: first non-empty line starts with a path prefix (`./`, `/`, `.\\`, `C:\`).
- text-to-pdf mode: otherwise, all text is rendered as a generated PDF attachment.

## Matching and validation rules

- Candidate selection:
  - First by sender (`matching.from_field`).
  - If multiple candidates, disambiguate by subject (`matching.subject_field`).
  - If unresolved, mark conflict.
- Expected cell semantics:
  - Empty -> ignored.
  - `IGNORE` is treated as a normal exact string value (not a special token).
  - `MUSS_LEER_SEIN` -> actual must be empty/null.
  - Float tolerance expressions supported:
    - `v+-t`
    - `v+t`
    - `v-t`
  - Decimal comma is supported (for example `3,14+-0,2`).
- Match output:
  - `OK` if no mismatches.
  - Otherwise newline-separated blocks:
    - `expected: <value>`
    - `actual: <value>`

## Row status values in `Match`

- `OK`: matched and validation passed.
- mismatch details: matched with validation differences.
- `NOT_FOUND`: enabled test case not matched.
- `CONFLICT`: ambiguous message-to-test-case matching.
- `SEND_FAILED`: SMTP send failed.
- `SKIPPED`: disabled row or dry-run skipped send.

## Transport notes

- In `rest` mode, the `kafka` section may be omitted.
- `RunInfo.kafka_topic` is populated with sentinel value `REST_DIRECT` for `rest` runs.
- In `rest` mode, the run aborts further REST requests after 3 consecutive request failures and marks remaining rows as `SKIPPED`.
