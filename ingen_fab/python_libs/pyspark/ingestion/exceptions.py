"""Custom exceptions for the ingestion framework"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ErrorContext:
    """Structured context for errors"""

    resource_name: Optional[str] = None
    source_name: Optional[str] = None
    batch_id: Optional[str] = None
    file_path: Optional[str] = None
    operation: Optional[str] = None
    additional_info: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        result = {}
        if self.resource_name:
            result["resource_name"] = self.resource_name
        if self.source_name:
            result["source_name"] = self.source_name
        if self.batch_id:
            result["batch_id"] = self.batch_id
        if self.file_path:
            result["file_path"] = self.file_path
        if self.operation:
            result["operation"] = self.operation
        if self.additional_info:
            result.update(self.additional_info)
        return result

    def __str__(self) -> str:
        """Format context for error messages"""
        parts = []
        if self.resource_name:
            parts.append(f"resource={self.resource_name}")
        if self.source_name:
            parts.append(f"source={self.source_name}")
        if self.batch_id:
            parts.append(f"batch={self.batch_id}")
        if self.file_path:
            parts.append(f"file={self.file_path}")
        if self.operation:
            parts.append(f"operation={self.operation}")
        return ", ".join(parts) if parts else "no context"


class IngestionError(Exception):
    """Base exception for all ingestion framework errors"""

    def __init__(
        self,
        message: str,
        context: Optional[ErrorContext] = None,
        error_code: Optional[str] = None,
    ):
        self.message = message
        self.context = context or ErrorContext()
        self.error_code = error_code
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with context"""
        parts = [self.message]
        if self.context and str(self.context) != "no context":
            parts.append(f"[{self.context}]")
        if self.error_code:
            parts.append(f"(code: {self.error_code})")
        return " ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for structured logging"""
        result = {
            "error_type": self.__class__.__name__,
            "message": self.message,
        }
        if self.error_code:
            result["error_code"] = self.error_code
        if self.context:
            result["context"] = self.context.to_dict()
        return result


# =============================================================================
# Configuration Errors
# =============================================================================


class ConfigurationError(IngestionError):
    """Raised when configuration is invalid or missing"""

    pass


class ValidationError(IngestionError):
    """Raised when data validation fails"""

    pass


# =============================================================================
# Extraction Errors
# =============================================================================


class ExtractionError(IngestionError):
    """Raised when extraction from external source fails"""

    pass


# =============================================================================
# File Discovery Errors
# =============================================================================


class FileDiscoveryError(IngestionError):
    """Raised when file discovery fails"""

    pass


class FileNotFoundError(FileDiscoveryError):
    """Raised when expected file is not found"""

    pass


class DuplicateFilesError(IngestionError):
    """Raised when duplicate files are detected and duplicate_handling='fail'"""

    pass


# =============================================================================
# File Reading Errors
# =============================================================================


class FileReadError(IngestionError):
    """Raised when file reading fails"""

    pass


class SchemaValidationError(FileReadError):
    """Raised when schema validation fails"""

    pass


# =============================================================================
# Writing Errors
# =============================================================================


class WriteError(IngestionError):
    """Raised when writing to target fails"""

    pass


class MergeError(WriteError):
    """Raised when merge operation fails"""

    pass


# =============================================================================
# Archive Errors
# =============================================================================


@dataclass
class ArchiveFailure:
    """Details of a single archive failure"""

    file_path: str
    error_message: str
    error_type: str
    is_transient: bool = False


@dataclass
class ArchiveResult:
    """Result of archiving operation with partial failure tracking"""

    total_files: int = 0
    succeeded: int = 0
    failed: int = 0
    failures: List[ArchiveFailure] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate success rate (0.0 to 1.0)"""
        if self.total_files == 0:
            return 0.0
        return self.succeeded / self.total_files

    @property
    def has_failures(self) -> bool:
        """Check if any failures occurred"""
        return self.failed > 0

    @property
    def all_successful(self) -> bool:
        """Check if all archives succeeded"""
        return self.total_files > 0 and self.failed == 0

    def add_success(self) -> None:
        """Record a successful archive"""
        self.total_files += 1
        self.succeeded += 1

    def add_failure(
        self,
        file_path: str,
        error_message: str,
        error_type: str = "ArchiveError",
        is_transient: bool = False,
    ) -> None:
        """Record a failed archive"""
        self.total_files += 1
        self.failed += 1
        self.failures.append(
            ArchiveFailure(
                file_path=file_path,
                error_message=error_message,
                error_type=error_type,
                is_transient=is_transient,
            )
        )

    def summary(self) -> str:
        """Get human-readable summary"""
        if self.total_files == 0:
            return "No files to archive"
        return (
            f"Archived {self.succeeded}/{self.total_files} file(s) successfully"
            + (f", {self.failed} failed" if self.failed > 0 else "")
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            "total_files": self.total_files,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "success_rate": self.success_rate,
            "failures": [
                {
                    "file_path": f.file_path,
                    "error_message": f.error_message,
                    "error_type": f.error_type,
                    "is_transient": f.is_transient,
                }
                for f in self.failures
            ],
        }


class ArchiveError(IngestionError):
    """Base class for archive-related errors"""

    def __init__(
        self,
        message: str,
        context: Optional[ErrorContext] = None,
        error_code: Optional[str] = None,
        archive_result: Optional[ArchiveResult] = None,
    ):
        super().__init__(message, context, error_code)
        self.archive_result = archive_result


class ArchivePermissionError(ArchiveError):
    """Raised when archive fails due to permission issues"""

    pass


class ArchiveNetworkError(ArchiveError):
    """Raised when archive fails due to network issues (transient)"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_transient = True


class ArchiveStorageError(ArchiveError):
    """Raised when archive fails due to storage issues (disk full, etc.)"""

    pass


class PartialArchiveFailure(ArchiveError):
    """Raised when some but not all files failed to archive"""

    def __init__(
        self,
        archive_result: ArchiveResult,
        context: Optional[ErrorContext] = None,
    ):
        message = archive_result.summary()
        super().__init__(
            message=message,
            context=context,
            error_code="PARTIAL_ARCHIVE_FAILURE",
            archive_result=archive_result,
        )
