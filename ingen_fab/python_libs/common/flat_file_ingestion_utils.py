# Common utilities for flat file ingestion
# Shared logic between PySpark and Python implementations

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import (
    FlatFileIngestionConfig,
    ProcessingMetrics,
)


class DatePartitionUtils:
    """Utilities for working with date partitions in file paths and names"""

    @staticmethod
    def extract_date_from_folder_name(
        folder_name: str, date_format: str
    ) -> Optional[str]:
        """Extract date from folder name based on date_partition_format"""
        try:
            # Handle different date formats in folder names
            if date_format == "YYYYMMDD":
                # Look for patterns like "orders_20240101.parquet", "customers_20240101.parquet"
                date_match = re.search(r"(\d{8})", folder_name)
                if date_match:
                    date_str = date_match.group(1)
                    # Convert YYYYMMDD to YYYY-MM-DD
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            elif date_format == "YYYY-MM-DD":
                # Look for YYYY-MM-DD pattern
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", folder_name)
                if date_match:
                    return date_match.group(1)

            elif date_format == "YYYY/MM/DD":
                # Look for YYYY/MM/DD pattern (though unlikely in folder names)
                date_match = re.search(r"(\d{4})/(\d{2})/(\d{2})", folder_name)
                if date_match:
                    return f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

            # Fallback: try to extract any 8-digit pattern and assume YYYYMMDD
            date_match = re.search(r"(\d{8})", folder_name)
            if date_match:
                date_str = date_match.group(1)
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        except Exception as e:
            print(f"⚠️ Could not extract date from folder name {folder_name}: {e}")

        return None

    @staticmethod
    def extract_date_from_path(
        file_path: str, base_path: str, date_format: str
    ) -> Optional[str]:
        """Extract date partition from file path based on format"""
        try:
            # First try to match YYYY/MM/DD pattern
            date_match = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", file_path)
            if date_match:
                return (
                    f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                )

            # If that fails, try to extract from path parts considering the base path
            # Handle both absolute and relative paths
            if base_path in file_path:
                # Find the part after base_path
                base_index = file_path.find(base_path)
                if base_index >= 0:
                    after_base = file_path[base_index + len(base_path) :].strip("/")
                    path_parts = after_base.split("/")

                    # For YYYY/MM/DD format, look for the first 3 numeric parts
                    if date_format == "YYYY/MM/DD" and len(path_parts) >= 3:
                        # Check if first 3 parts look like a date
                        year, month, day = path_parts[0], path_parts[1], path_parts[2]
                        if (
                            len(year) == 4
                            and year.isdigit()
                            and len(month) <= 2
                            and month.isdigit()
                            and len(day) <= 2
                            and day.isdigit()
                        ):
                            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        except Exception as e:
            print(f"⚠️ Could not extract date from path {file_path}: {e}")

        return None

    @staticmethod
    def is_date_in_range(date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        if not date_str:
            return True  # Include files without date partitions

        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            if start_date:
                start = datetime.strptime(start_date, "%Y-%m-%d").date()
                if file_date < start:
                    return False

            if end_date:
                end = datetime.strptime(end_date, "%Y-%m-%d").date()
                if file_date > end:
                    return False

            return True
        except Exception as e:
            print(f"⚠️ Date range check failed for {date_str}: {e}")
            return True

    @staticmethod
    def discover_nested_date_table_paths(
        base_path: str, date_format: str, table_subfolder: Optional[str] = None
    ) -> List[Tuple[str, str, str]]:
        """
        Discover nested date/table paths in hierarchical structure

        Args:
            base_path: Base directory to search
            date_format: Date format pattern (e.g., "YYYY/MM/DD")
            table_subfolder: Specific table name to filter for (optional)

        Returns:
            List of tuples: (full_path, date_partition, table_name)
        """
        results = []

        try:
            import os

            if not os.path.exists(base_path):
                print(f"⚠️ Base path does not exist: {base_path}")
                return results

            # For YYYY/MM/DD structure, look for year folders first
            if date_format == "YYYY/MM/DD":
                for year_item in os.listdir(base_path):
                    year_path = os.path.join(base_path, year_item)
                    if (
                        not os.path.isdir(year_path)
                        or not year_item.isdigit()
                        or len(year_item) != 4
                    ):
                        continue

                    # Look for month folders
                    for month_item in os.listdir(year_path):
                        month_path = os.path.join(year_path, month_item)
                        if (
                            not os.path.isdir(month_path)
                            or not month_item.isdigit()
                            or len(month_item) > 2
                        ):
                            continue

                        # Look for day folders
                        for day_item in os.listdir(month_path):
                            day_path = os.path.join(month_path, day_item)
                            if (
                                not os.path.isdir(day_path)
                                or not day_item.isdigit()
                                or len(day_item) > 2
                            ):
                                continue

                            # Construct date partition
                            date_partition = (
                                f"{year_item}-{month_item.zfill(2)}-{day_item.zfill(2)}"
                            )

                            # Look for table folders within the date folder
                            for table_item in os.listdir(day_path):
                                table_path = os.path.join(day_path, table_item)
                                if not os.path.isdir(table_path):
                                    continue

                                # Filter by table_subfolder if specified
                                if table_subfolder and table_item != table_subfolder:
                                    continue

                                results.append((table_path, date_partition, table_item))

            else:
                print(f"⚠️ Unsupported date format for nested discovery: {date_format}")

        except Exception as e:
            print(f"⚠️ Error during nested path discovery: {e}")

        return results


class FilePatternUtils:
    """Utilities for working with file patterns and discovery"""

    @staticmethod
    def glob_to_regex(pattern: str) -> str:
        """Convert glob pattern to regex pattern"""
        # Escape special regex characters except * and ?
        escaped = re.escape(pattern)
        # Convert glob wildcards to regex
        regex_pattern = escaped.replace(r"\*", ".*").replace(r"\?", ".")
        return f"^{regex_pattern}$"

    @staticmethod
    def matches_pattern(filename: str, pattern: str) -> bool:
        """Check if filename matches glob pattern"""
        if not pattern:
            return True

        try:
            regex_pattern = FilePatternUtils.glob_to_regex(pattern)
            return bool(re.match(regex_pattern, filename))
        except Exception as e:
            print(
                f"⚠️ Pattern matching failed for {filename} with pattern {pattern}: {e}"
            )
            return False

    @staticmethod
    def extract_folder_name_from_path(path: str) -> str:
        """Extract folder name from path"""
        return os.path.basename(path.rstrip("/"))


class ConfigurationUtils:
    """Utilities for working with flat file ingestion configuration"""

    @staticmethod
    def validate_config(config: FlatFileIngestionConfig) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []

        # Required fields validation
        if not config.config_id:
            errors.append("config_id is required")
        if not config.source_file_path:
            errors.append("source_file_path is required")
        if not config.source_file_format:
            errors.append("source_file_format is required")
        if not config.target_table_name:
            errors.append("target_table_name is required")

        # Format validation
        supported_formats = ["csv", "json", "parquet", "avro", "xml"]
        if config.source_file_format.lower() not in supported_formats:
            errors.append(f"source_file_format must be one of: {supported_formats}")

        # Write mode validation
        supported_write_modes = ["overwrite", "append", "merge"]
        if config.write_mode.lower() not in supported_write_modes:
            errors.append(f"write_mode must be one of: {supported_write_modes}")

        # Date range validation
        if config.date_range_start and config.date_range_end:
            try:
                start_date = datetime.strptime(config.date_range_start, "%Y-%m-%d")
                end_date = datetime.strptime(config.date_range_end, "%Y-%m-%d")
                if start_date > end_date:
                    errors.append("date_range_start must be before date_range_end")
            except ValueError as e:
                errors.append(f"Invalid date format in date range: {e}")

        return errors

    @staticmethod
    def get_file_read_options(config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Get file reading options based on configuration"""
        options = {}

        if config.source_file_format.lower() == "csv":
            options.update(
                {
                    "header": config.has_header,
                    "sep": config.file_delimiter,
                    "encoding": config.encoding,
                    "quote": config.quote_character,
                    "escape": config.escape_character,
                    "multiLine": config.multiline_values,
                    "ignoreLeadingWhiteSpace": config.ignore_leading_whitespace,
                    "ignoreTrailingWhiteSpace": config.ignore_trailing_whitespace,
                    "nullValue": config.null_value,
                    "emptyValue": config.empty_value,
                    "dateFormat": config.date_format,
                    "timestampFormat": config.timestamp_format,
                    "inferSchema": config.schema_inference,
                    "maxColumns": config.max_columns,
                    "maxCharsPerColumn": config.max_chars_per_column,
                }
            )

            if config.comment_character:
                options["comment"] = config.comment_character

        elif config.source_file_format.lower() == "json":
            options.update(
                {
                    "multiLine": config.multiline_values,
                    "dateFormat": config.date_format,
                    "timestampFormat": config.timestamp_format,
                    "encoding": config.encoding,
                }
            )

        elif config.source_file_format.lower() == "parquet":
            # Parquet files don't need many options as they are self-describing
            options.update(
                {
                    "dateFormat": config.date_format,
                    "timestampFormat": config.timestamp_format,
                }
            )

        return options


class ProcessingMetricsUtils:
    """Utilities for working with processing metrics"""

    @staticmethod
    def calculate_performance_metrics(
        metrics: ProcessingMetrics, write_mode: str = "append"
    ) -> ProcessingMetrics:
        """Calculate derived performance metrics"""
        # Calculate total duration if not set
        if metrics.total_duration_ms == 0:
            metrics.total_duration_ms = (
                metrics.read_duration_ms + metrics.write_duration_ms
            )

        # Calculate rows per second
        if metrics.total_duration_ms > 0 and metrics.records_processed > 0:
            metrics.avg_rows_per_second = (
                metrics.records_processed * 1000
            ) / metrics.total_duration_ms

        # Calculate throughput if data size is available
        if metrics.total_duration_ms > 0 and metrics.data_size_mb > 0:
            metrics.throughput_mb_per_second = (
                metrics.data_size_mb * 1000
            ) / metrics.total_duration_ms

        # Set row count reconciliation status based on write mode
        if metrics.source_row_count > 0 and metrics.target_row_count_after >= 0:
            if write_mode.lower() == "overwrite":
                # For overwrite mode, source rows should equal final target rows
                if metrics.source_row_count == metrics.target_row_count_after:
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            elif write_mode.lower() == "append":
                # For append mode, source rows should equal the difference
                if metrics.source_row_count == (
                    metrics.target_row_count_after - metrics.target_row_count_before
                ):
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            elif write_mode.lower() == "merge":
                # For merge mode, we can't easily determine reconciliation without more info
                # So we'll mark as verified if target count increased or stayed same
                if metrics.target_row_count_after >= metrics.target_row_count_before:
                    metrics.row_count_reconciliation_status = "verified"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            else:
                # Unknown write mode
                metrics.row_count_reconciliation_status = "not_verified"
        else:
            metrics.row_count_reconciliation_status = "not_verified"

        return metrics

    @staticmethod
    def merge_metrics(
        metrics_list: List[ProcessingMetrics], write_mode: str = "append"
    ) -> ProcessingMetrics:
        """Merge multiple metrics into a single aggregated metric"""
        if not metrics_list:
            return ProcessingMetrics()

        merged = ProcessingMetrics()

        # Sum up numerical values
        merged.read_duration_ms = sum(m.read_duration_ms for m in metrics_list)
        merged.write_duration_ms = sum(m.write_duration_ms for m in metrics_list)
        merged.total_duration_ms = sum(m.total_duration_ms for m in metrics_list)
        merged.records_processed = sum(m.records_processed for m in metrics_list)
        merged.records_inserted = sum(m.records_inserted for m in metrics_list)
        merged.records_updated = sum(m.records_updated for m in metrics_list)
        merged.records_deleted = sum(m.records_deleted for m in metrics_list)
        merged.records_failed = sum(m.records_failed for m in metrics_list)
        merged.source_row_count = sum(m.source_row_count for m in metrics_list)
        merged.target_row_count_before = sum(
            m.target_row_count_before for m in metrics_list
        )
        merged.target_row_count_after = sum(
            m.target_row_count_after for m in metrics_list
        )
        merged.data_size_mb = sum(m.data_size_mb for m in metrics_list)

        # Calculate performance metrics
        return ProcessingMetricsUtils.calculate_performance_metrics(merged, write_mode)


class ErrorHandlingUtils:
    """Utilities for error handling in flat file ingestion"""

    @staticmethod
    def categorize_error(error: Exception) -> str:
        """Categorize error type for reporting"""
        error_type = type(error).__name__  # noqa: F841
        error_message = str(error).lower()

        if "file not found" in error_message or "no such file" in error_message:
            return "file_not_found"
        elif "permission" in error_message or "access denied" in error_message:
            return "permission_denied"
        elif "schema" in error_message or "column" in error_message:
            return "schema_mismatch"
        elif "format" in error_message or "parse" in error_message:
            return "format_error"
        elif "connection" in error_message or "network" in error_message:
            return "connection_error"
        elif "timeout" in error_message:
            return "timeout_error"
        else:
            return "unknown_error"

    @staticmethod
    def should_retry_error(
        error: Exception, retry_count: int, max_retries: int = 3
    ) -> bool:
        """Determine if an error should be retried"""
        if retry_count >= max_retries:
            return False

        error_category = ErrorHandlingUtils.categorize_error(error)

        # Retry on connection, timeout, and unknown errors
        retryable_categories = ["connection_error", "timeout_error", "unknown_error"]
        return error_category in retryable_categories

    @staticmethod
    def format_error_details(
        error: Exception, context: Dict[str, Any] = None
    ) -> Dict[str, str]:
        """Format error details for logging"""
        details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_category": ErrorHandlingUtils.categorize_error(error),
        }

        if context:
            details.update({f"context_{k}": str(v) for k, v in context.items()})

        return details
