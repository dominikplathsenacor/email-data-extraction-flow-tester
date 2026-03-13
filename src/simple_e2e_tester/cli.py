"""Command line interface entry point."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from simple_e2e_tester.bootstrap import BootstrapError, bootstrap_project_environment
from simple_e2e_tester.configuration import (
    DEFAULT_CONFIG_FILENAME,
    ConfigurationError,
    load_configuration,
    write_placeholder_configuration,
)
from simple_e2e_tester.email_sending.email_dispatch import (
    ExpectedEventDispatcher,
    SynchronousSMTPClient,
)
from simple_e2e_tester.kafka_consumption.actual_event_reader import ActualEventReader
from simple_e2e_tester.run_execution.run_contracts import RunRequest
from simple_e2e_tester.run_execution.validation_run_use_case import (
    RunExecutionError,
    execute_email_kafka_validation_run,
)
from simple_e2e_tester.schema_management import (
    SchemaError,
    flatten_schema,
    load_schema_document,
)
from simple_e2e_tester.template_generation import generate_template_workbook


class CliError(Exception):
    """Custom CLI error."""


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="simple-e2e-tester")
def cli() -> None:
    """Schema-driven E2E tester utility."""


@cli.command(name="generate-config")
@click.option(
    "--output",
    "output_path",
    required=False,
    default=DEFAULT_CONFIG_FILENAME,
    show_default=True,
    type=click.Path(path_type=str),
    help="Path to the YAML test configuration template to write",
)
def generate_config(output_path: str) -> None:
    """Generate a placeholder YAML test configuration with guidance comments."""
    try:
        resolved_output = write_placeholder_configuration(output_path)
    except (FileExistsError, OSError) as exc:
        raise CliError(str(exc)) from exc
    click.echo(str(resolved_output))


@cli.command(name="init")
def init() -> None:
    """Prepare local virtual environment and install project dependencies."""
    repo_root = Path(__file__).resolve().parents[2]
    try:
        bootstrap_project_environment(repo_root=repo_root)
    except BootstrapError as exc:
        raise CliError(str(exc)) from exc
    click.echo(f"local virtual environment ready: {repo_root / '.venv'}")


@cli.command(name="generate-template")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(path_type=str),
    help="Path to YAML/JSON test configuration file",
)
@click.option(
    "--output",
    "output_path",
    required=True,
    type=click.Path(path_type=str),
    help="Path to the test template workbook to write",
)
def generate_template(config_path: str, output_path: str) -> None:
    """Generate a test template workbook from the configured event schema."""
    try:
        configuration = load_configuration(config_path)
        fields = flatten_schema(load_schema_document(configuration.schema))
        generate_template_workbook(configuration.schema, fields, output_path)
    except (ConfigurationError, SchemaError, OSError, ValueError) as exc:
        raise CliError(str(exc)) from exc
    click.echo(str(Path(output_path).resolve()))


@cli.command(name="run")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(path_type=str),
    help="Path to YAML/JSON test configuration file",
)
@click.option(
    "--input",
    "input_path",
    required=True,
    type=click.Path(path_type=str),
    help="Path to the filled test template workbook",
)
@click.option(
    "--output-dir",
    "output_dir",
    required=False,
    type=click.Path(path_type=str),
    help="Optional directory for storing result workbooks",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Skip SMTP/Kafka interactions and write a skipped-results workbook.",
)
def run_tests(
    config_path: str, input_path: str, output_dir: str | None, dry_run: bool
) -> None:
    """Execute tests defined in the test template workbook."""
    try:
        outcome = execute_email_kafka_validation_run(
            RunRequest(
                config_path=config_path,
                input_path=input_path,
                output_dir=output_dir,
                dry_run=dry_run,
            ),
            email_sender_cls=ExpectedEventDispatcher,
            kafka_service_cls=ActualEventReader,
            smtp_client_factory=SynchronousSMTPClient,
        )
    except RunExecutionError as exc:
        raise CliError(str(exc)) from exc
    click.echo(str(outcome.output_path))


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for console_scripts wiring."""
    argv = argv if argv is not None else sys.argv[1:]
    try:
        cli.main(args=list(argv), standalone_mode=False)
    except CliError as exc:
        click.echo(str(exc), err=True)
        return 1
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.Abort:
        click.echo("Aborted.", err=True)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
