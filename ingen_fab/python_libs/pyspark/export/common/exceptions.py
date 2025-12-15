"""Custom exceptions for the export framework."""


class ExportError(Exception):
    """Base exception for export operations."""

    pass


class SourceReadError(ExportError):
    """Error reading from source table/query."""

    pass


class FileWriteError(ExportError):
    """Error writing to output file."""

    pass


class ConfigurationError(ExportError):
    """Error in export configuration."""

    pass


class ConfigValidationError(ConfigurationError):
    """Raised when export config validation fails during loading."""

    pass
