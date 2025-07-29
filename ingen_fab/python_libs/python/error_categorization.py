"""
Error categorization module for Synapse extraction operations.

This module provides comprehensive error categorization and handling patterns
that match the original synapse_sync_logic.py file complexity.
"""

import logging
from enum import Enum
from typing import Optional, Tuple, Dict, Any
import re

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """Error categories for different types of failures."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    NETWORK = "network"
    RESOURCE = "resource"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    """Error severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategorizer:
    """
    Categorizes errors based on patterns and characteristics.

    This class implements the sophisticated error categorization logic
    from the original file, providing proper classification of errors
    for retry and handling decisions.
    """

    # HTTP status codes that indicate transient errors
    TRANSIENT_HTTP_CODES = {429, 500, 503, 504, 408}

    # Network-related error patterns
    NETWORK_ERROR_PATTERNS = [
        r"timeout",
        r"connection",
        r"network",
        r"unreachable",
        r"dns",
        r"timed out",
        r"connection reset",
        r"connection refused",
        r"connection aborted",
    ]

    # Resource constraint error patterns
    RESOURCE_ERROR_PATTERNS = [
        r"quota",
        r"capacity",
        r"throttl",
        r"rate limit",
        r"resource limit",
        r"too many requests",
        r"service busy",
        r"temporarily unavailable",
    ]

    # Transient operation error patterns
    TRANSIENT_OPERATION_PATTERNS = [
        r"operationtimedout",
        r"temporarily unavailable",
        r"service busy",
        r"retry",
        r"temporary failure",
        r"transient",
        r"not found.*retry",
        r"requestexecutionfailed.*notfound",
    ]

    # Configuration error patterns
    CONFIG_ERROR_PATTERNS = [
        r"not valid for guid",
        r"invalid.*format",
        r"malformed",
        r"invalid.*id",
        r"bad request",
        r"unauthorized",
        r"forbidden",
        r"permission denied",
    ]

    # Permanent failure patterns
    PERMANENT_ERROR_PATTERNS = [
        r"not found",
        r"does not exist",
        r"invalid.*parameter",
        r"schema.*mismatch",
        r"syntax error",
        r"compilation error",
        r"access denied",
    ]

    def __init__(self):
        """Initialize the error categorizer with compiled patterns."""
        self.network_regex = re.compile(
            "|".join(self.NETWORK_ERROR_PATTERNS), re.IGNORECASE
        )
        self.resource_regex = re.compile(
            "|".join(self.RESOURCE_ERROR_PATTERNS), re.IGNORECASE
        )
        self.transient_regex = re.compile(
            "|".join(self.TRANSIENT_OPERATION_PATTERNS), re.IGNORECASE
        )
        self.config_regex = re.compile(
            "|".join(self.CONFIG_ERROR_PATTERNS), re.IGNORECASE
        )
        self.permanent_regex = re.compile(
            "|".join(self.PERMANENT_ERROR_PATTERNS), re.IGNORECASE
        )

    def categorize_http_error(
        self, status_code: int, response_text: str = ""
    ) -> Tuple[ErrorCategory, ErrorSeverity, bool]:
        """
        Categorize HTTP errors based on status code and response text.

        Args:
            status_code: HTTP status code
            response_text: Response body text

        Returns:
            Tuple of (category, severity, is_retryable)
        """
        if status_code == 404:
            # 404 can be transient for jobs that are still initializing
            if "job" in response_text.lower() or "instance" in response_text.lower():
                return ErrorCategory.TRANSIENT, ErrorSeverity.LOW, True
            else:
                return ErrorCategory.PERMANENT, ErrorSeverity.HIGH, False

        elif status_code == 429:
            # Rate limiting - always retryable
            return ErrorCategory.RESOURCE, ErrorSeverity.MEDIUM, True

        elif status_code == 408:
            # Request timeout - retryable
            return ErrorCategory.NETWORK, ErrorSeverity.MEDIUM, True

        elif status_code >= 500:
            # Server errors - generally retryable
            return ErrorCategory.TRANSIENT, ErrorSeverity.HIGH, True

        elif status_code in [400, 401, 403]:
            # Client errors - check response text for more context
            if self.resource_regex.search(response_text):
                return ErrorCategory.RESOURCE, ErrorSeverity.MEDIUM, True
            elif self.config_regex.search(response_text):
                return ErrorCategory.CONFIGURATION, ErrorSeverity.HIGH, False
            else:
                return ErrorCategory.PERMANENT, ErrorSeverity.HIGH, False

        else:
            # Other client errors (4xx)
            return ErrorCategory.PERMANENT, ErrorSeverity.MEDIUM, False

    def categorize_exception(
        self, exception: Exception, context: Optional[Dict[str, Any]] = None
    ) -> Tuple[ErrorCategory, ErrorSeverity, bool]:
        """
        Categorize exceptions based on type and message.

        Args:
            exception: The exception to categorize
            context: Optional context information

        Returns:
            Tuple of (category, severity, is_retryable)
        """
        error_msg = str(exception).lower()
        exception_type = type(exception).__name__

        # Network-related exceptions
        if self.network_regex.search(error_msg):
            return ErrorCategory.NETWORK, ErrorSeverity.MEDIUM, True

        # Resource constraint exceptions
        if self.resource_regex.search(error_msg):
            return ErrorCategory.RESOURCE, ErrorSeverity.MEDIUM, True

        # Transient operation exceptions
        if self.transient_regex.search(error_msg):
            return ErrorCategory.TRANSIENT, ErrorSeverity.LOW, True

        # Configuration exceptions
        if self.config_regex.search(error_msg):
            return ErrorCategory.CONFIGURATION, ErrorSeverity.HIGH, False

        # Permanent failure exceptions
        if self.permanent_regex.search(error_msg):
            return ErrorCategory.PERMANENT, ErrorSeverity.HIGH, False

        # Exception type-based categorization
        if exception_type in [
            "ConnectionError",
            "ConnectTimeout",
            "ReadTimeout",
            "Timeout",
        ]:
            return ErrorCategory.NETWORK, ErrorSeverity.MEDIUM, True

        elif exception_type in ["ValueError", "TypeError", "AttributeError"]:
            return ErrorCategory.CONFIGURATION, ErrorSeverity.HIGH, False

        elif exception_type in ["OSError", "IOError"]:
            return ErrorCategory.TRANSIENT, ErrorSeverity.MEDIUM, True

        else:
            # Unknown exception type
            return ErrorCategory.UNKNOWN, ErrorSeverity.MEDIUM, True

    def categorize_pipeline_failure(
        self, failure_reason: Dict[str, Any]
    ) -> Tuple[ErrorCategory, ErrorSeverity, bool]:
        """
        Categorize pipeline failure reasons.

        Args:
            failure_reason: Pipeline failure reason dictionary

        Returns:
            Tuple of (category, severity, is_retryable)
        """
        error_code = failure_reason.get("errorCode", "")
        message = failure_reason.get("message", "")

        # Specific error code patterns
        if error_code == "RequestExecutionFailed" and "NotFound" in message:
            return ErrorCategory.TRANSIENT, ErrorSeverity.LOW, True

        # Check message patterns
        if self.resource_regex.search(message):
            return ErrorCategory.RESOURCE, ErrorSeverity.MEDIUM, True

        elif self.transient_regex.search(message):
            return ErrorCategory.TRANSIENT, ErrorSeverity.LOW, True

        elif self.config_regex.search(message):
            return ErrorCategory.CONFIGURATION, ErrorSeverity.HIGH, False

        elif self.permanent_regex.search(message):
            return ErrorCategory.PERMANENT, ErrorSeverity.HIGH, False

        else:
            # Unknown pipeline failure
            return ErrorCategory.UNKNOWN, ErrorSeverity.HIGH, False

    def should_retry(
        self, category: ErrorCategory, attempt: int, max_retries: int
    ) -> bool:
        """
        Determine if an error should be retried based on category and attempt count.

        Args:
            category: Error category
            attempt: Current attempt number
            max_retries: Maximum number of retries

        Returns:
            True if should retry, False otherwise
        """
        if attempt >= max_retries:
            return False

        # Retry policies by category
        if category in [
            ErrorCategory.NETWORK,
            ErrorCategory.TRANSIENT,
            ErrorCategory.RESOURCE,
        ]:
            return True
        elif category == ErrorCategory.UNKNOWN:
            # Be conservative with unknown errors
            return attempt < (max_retries // 2)
        else:
            # Don't retry permanent or configuration errors
            return False

    def get_retry_delay(
        self, category: ErrorCategory, attempt: int, base_delay: float = 2.0
    ) -> float:
        """
        Calculate retry delay based on error category and attempt.

        Args:
            category: Error category
            attempt: Current attempt number
            base_delay: Base delay in seconds

        Returns:
            Delay in seconds
        """
        # Different backoff strategies by category
        if category == ErrorCategory.RESOURCE:
            # Longer delays for resource constraints
            return base_delay * (2**attempt) * 1.5
        elif category == ErrorCategory.NETWORK:
            # Standard exponential backoff for network errors
            return base_delay * (1.5**attempt)
        elif category == ErrorCategory.TRANSIENT:
            # Shorter delays for transient errors
            return base_delay * (1.2**attempt)
        else:
            # Default exponential backoff
            return base_delay * (1.5**attempt)

    def log_error(
        self,
        error: Exception,
        category: ErrorCategory,
        severity: ErrorSeverity,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log error with appropriate level and context.

        Args:
            error: The error to log
            category: Error category
            severity: Error severity
            context: Optional context information
        """
        context_str = f" | Context: {context}" if context else ""
        error_msg = f"[{category.value.upper()}] {str(error)}{context_str}"

        if severity == ErrorSeverity.CRITICAL:
            logger.critical(error_msg, exc_info=True)
        elif severity == ErrorSeverity.HIGH:
            logger.error(error_msg, exc_info=True)
        elif severity == ErrorSeverity.MEDIUM:
            logger.warning(error_msg)
        else:
            logger.info(error_msg)


# Global error categorizer instance
error_categorizer = ErrorCategorizer()
