"""Run execution domain exports."""

from .run_contracts import RunArtifacts, RunOutcome, RunRequest

__all__ = [
    "RunRequest",
    "RunOutcome",
    "RunArtifacts",
    "RunExecutionError",
    "execute_email_kafka_validation_run",
]


def __getattr__(name: str):
    """Lazily expose use-case symbols to avoid import cycles."""
    if name in {"RunExecutionError", "execute_email_kafka_validation_run"}:
        from .validation_run_use_case import (
            RunExecutionError,
            execute_email_kafka_validation_run,
        )

        exports = {
            "RunExecutionError": RunExecutionError,
            "execute_email_kafka_validation_run": execute_email_kafka_validation_run,
        }
        return exports[name]
    raise AttributeError(name)
