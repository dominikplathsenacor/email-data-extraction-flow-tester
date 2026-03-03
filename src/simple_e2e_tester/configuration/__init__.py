"""Configuration domain exports."""

from .config_scaffold_builder import (
    DEFAULT_CONFIG_FILENAME,
    build_placeholder_configuration,
    write_placeholder_configuration,
)
from .loader import ConfigurationError, load_configuration
from .runtime_settings import (
    Configuration,
    KafkaSettings,
    MailSettings,
    MatchingConfig,
    RestSettings,
    SchemaConfig,
    SMTPSettings,
    TransportSettings,
)

__all__ = [
    "Configuration",
    "KafkaSettings",
    "MailSettings",
    "MatchingConfig",
    "RestSettings",
    "SchemaConfig",
    "SMTPSettings",
    "TransportSettings",
    "ConfigurationError",
    "load_configuration",
    "DEFAULT_CONFIG_FILENAME",
    "build_placeholder_configuration",
    "write_placeholder_configuration",
]
