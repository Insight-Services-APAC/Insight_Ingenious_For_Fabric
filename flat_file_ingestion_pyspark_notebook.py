# Standalone Microsoft Fabric Notebook Version
# Flat File Ingestion - PySpark Implementation with Incremental Wildcard Pattern Support
#
# This file contains all necessary code for testing in a Fabric notebook
# Copy and paste into a notebook cell to test

import logging
import os
import time
import uuid
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from fnmatch import fnmatch

from pyspark.sql import DataFrame, SparkSession


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Configure logging with timestamp and level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Create module logger
logger = logging.getLogger(__name__)


class ConfigLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that prepends execution group and config name to messages"""
    def process(self, msg, kwargs):
        execution_group = self.extra.get('execution_group', '')
        config_name = self.extra.get('config_name', '')

        prefix_parts = []
        if execution_group:
            prefix_parts.append(f'Group {execution_group}')
        if config_name:
            prefix_parts.append(config_name)

        if prefix_parts:
            prefix = '[' + '] ['.join(prefix_parts) + ']'
            return f'{prefix} {msg}', kwargs
        return msg, kwargs


@dataclass
class ConfigExecutionResult:
    """Result of configuration execution for summary reporting"""
    config_id: str
    config_name: str
    status: str  # 'completed', 'failed', 'no_data'
    rows_processed: int = 0
    duration_sec: float = 0.0
    error_message: Optional[str] = None


# ============================================================================
# DATACLASSES (from interfaces)
# ============================================================================

@dataclass
class FlatFileIngestionConfig:
    """Configuration class for flat file ingestion"""

    # Core configuration
    config_id: str
    config_name: str
    source_file_path: str
    source_file_format: str

    # Source location fields (optional)
    source_workspace_id: Optional[str] = None
    source_datastore_id: Optional[str] = None
    source_datastore_type: Optional[str] = None
    source_file_root_path: Optional[str] = None

    # Target location fields
    target_workspace_id: str = ""
    target_datastore_id: str = ""
    target_datastore_type: str = ""
    target_schema_name: str = ""
    target_table_name: str = ""

    # Optional configuration
    staging_table_name: Optional[str] = None
    file_delimiter: str = ","
    has_header: bool = True
    encoding: str = "utf-8"
    date_format: str = "yyyy-MM-dd"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    schema_inference: bool = True

    # Advanced CSV handling options
    quote_character: str = '"'
    escape_character: str = "\\"
    multiline_values: bool = True
    ignore_leading_whitespace: bool = False
    ignore_trailing_whitespace: bool = False
    null_value: str = ""
    empty_value: str = ""
    comment_character: Optional[str] = None
    max_columns: int = 20480
    max_chars_per_column: int = -1

    # Schema and processing options
    custom_schema_json: Optional[str] = None
    partition_columns: List[str] = None
    sort_columns: List[str] = None
    write_mode: str = "overwrite"
    merge_keys: List[str] = None
    enable_schema_evolution: bool = True
    execution_group: int = 1
    active_yn: str = "Y"

    # Date partitioning and batch processing
    import_pattern: str = "single_file"
    date_partition_format: str = "YYYY/MM/DD"
    table_relationship_group: Optional[str] = None
    batch_import_enabled: bool = False
    file_discovery_pattern: Optional[str] = None
    import_sequence_order: int = 1
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    skip_existing_dates: bool = True
    source_is_folder: bool = False

    # Hierarchical nested structure support
    table_subfolder: Optional[str] = None
    hierarchical_date_structure: bool = False
    nested_path_separator: str = "/"

    # File discovery validation
    require_files: bool = False  # If True, fail config when no files are found

    # Filename attribute extraction
    filename_attribute_pattern: Optional[str] = None  # Regex pattern with named groups to extract attributes from filename
    filename_sort_attribute: Optional[str] = None  # Which extracted attribute to use for sorting and incremental loading

    # Duplicate file handling
    duplicate_file_handling: str = "skip"  # 'allow', 'skip', 'fail'

    def __post_init__(self):
        """Post-initialization processing"""
        if self.partition_columns is None:
            self.partition_columns = []
        if self.sort_columns is None:
            self.sort_columns = []
        if self.merge_keys is None:
            self.merge_keys = []


@dataclass
class FileDiscoveryResult:
    """Result of file discovery operation"""

    file_path: str
    date_partition: Optional[str] = None
    folder_name: Optional[str] = None
    size: int = 0
    modified_time: Optional[datetime] = None
    nested_structure: bool = False
    filename_attributes: Optional[Dict[str, str]] = None


@dataclass
class ProcessingMetrics:
    """Metrics for file processing performance"""

    read_duration_ms: int = 0
    write_duration_ms: int = 0
    total_duration_ms: int = 0
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    source_row_count: int = 0
    target_row_count_before: int = 0
    target_row_count_after: int = 0
    row_count_reconciliation_status: str = "not_verified"


# ============================================================================
# EXCEPTIONS
# ============================================================================

class DuplicateFilesError(Exception):
    """Raised when duplicate files are found and duplicate_file_handling='fail'"""
    pass


# ============================================================================
# UTILITY CLASSES (from common/flat_file_ingestion_utils.py)
# ============================================================================

class DatePartitionUtils:
    """Utilities for working with date partitions in file paths and names"""

    @staticmethod
    def extract_date_from_folder_name(folder_name: str, date_format: str) -> Optional[str]:
        """Extract date from folder name based on date_partition_format"""
        try:
            if date_format == "YYYYMMDD":
                date_match = re.search(r"(\d{8})", folder_name)
                if date_match:
                    date_str = date_match.group(1)
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            elif date_format == "YYYY-MM-DD":
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", folder_name)
                if date_match:
                    return date_match.group(1)

            elif date_format == "YYYY/MM/DD":
                date_match = re.search(r"(\d{4})/(\d{2})/(\d{2})", folder_name)
                if date_match:
                    return f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

            # Fallback: try to extract any 8-digit pattern
            date_match = re.search(r"(\d{8})", folder_name)
            if date_match:
                date_str = date_match.group(1)
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        except Exception as e:
            logger.warning(f"Could not extract date from folder name {folder_name}: {e}")

        return None

    @staticmethod
    def is_date_in_range(date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        if not date_str:
            return True

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
            logger.warning(f"Date range check failed for {date_str}: {e}")
            return True


class FilePatternUtils:
    """Utilities for working with file patterns and discovery"""

    @staticmethod
    def glob_to_regex(pattern: str) -> str:
        """Convert glob pattern to regex pattern"""
        escaped = re.escape(pattern)
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
            logger.warning(f"Pattern matching failed for {filename} with pattern {pattern}: {e}")
            return False

    @staticmethod
    def extract_folder_name_from_path(path: str) -> str:
        """Extract folder name from path"""
        return os.path.basename(path.rstrip("/"))


class SchemaUtils:
    """Utilities for working with PySpark schemas"""

    @staticmethod
    def parse_custom_schema(schema_json: str) -> Optional[Any]:
        """
        Parse custom schema JSON string into PySpark StructType.

        Args:
            schema_json: JSON string representing a PySpark schema

        Returns:
            PySpark StructType if valid, None if parsing fails

        Expected JSON format:
            {
                "type": "struct",
                "fields": [
                    {"name": "col1", "type": "string", "nullable": true, "metadata": {}},
                    {"name": "col2", "type": "integer", "nullable": false, "metadata": {}}
                ]
            }
        """
        if not schema_json or not isinstance(schema_json, str):
            return None

        try:
            import json
            from pyspark.sql.types import StructType

            # Parse JSON string to dict
            schema_dict = json.loads(schema_json)

            # Validate basic structure
            if not isinstance(schema_dict, dict):
                logger.warning("Custom schema must be a JSON object")
                return None

            # Convert to PySpark StructType using built-in fromJson method
            schema = StructType.fromJson(schema_dict)

            logger.info(f"Successfully parsed custom schema with {len(schema.fields)} fields")
            return schema

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in custom schema: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to parse custom schema: {e}")
            return None


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

        # Merge mode specific validation
        if config.write_mode.lower() == "merge":
            # Check if merge_keys is provided
            if not config.merge_keys or len(config.merge_keys) == 0:
                errors.append(
                    "merge_keys must be provided when write_mode is 'merge'. "
                    "Please specify one or more column names to use as merge keys."
                )

            # Check if target is warehouse (merge only supported for lakehouse)
            if config.target_datastore_type and config.target_datastore_type.lower() == "warehouse":
                errors.append(
                    "Merge write mode is only supported for lakehouse targets, not warehouse. "
                    "Please use 'overwrite' or 'append' for warehouse targets."
                )
        else:
            # For non-merge modes, merge_keys should not be populated
            if config.merge_keys and len(config.merge_keys) > 0:
                errors.append(
                    f"merge_keys should not be specified for write_mode '{config.write_mode}'. "
                    "merge_keys are only applicable when write_mode is 'merge'. "
                    "Please remove merge_keys or change write_mode to 'merge'."
                )

        # Date range validation
        if config.date_range_start and config.date_range_end:
            try:
                start_date = datetime.strptime(config.date_range_start, "%Y-%m-%d")
                end_date = datetime.strptime(config.date_range_end, "%Y-%m-%d")
                if start_date > end_date:
                    errors.append("date_range_start must be before date_range_end")
            except ValueError as e:
                errors.append(f"Invalid date format in date range: {e}")

        # Duplicate file handling validation
        valid_duplicate_handling = ["allow", "skip", "fail"]
        if config.duplicate_file_handling not in valid_duplicate_handling:
            errors.append(
                f"duplicate_file_handling must be one of: {valid_duplicate_handling}, "
                f"got '{config.duplicate_file_handling}'"
            )

        return errors

    @staticmethod
    def get_file_read_options(config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Get file reading options based on configuration"""
        options = {}

        if config.source_file_format.lower() == "csv":
            options.update({
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
            })

        elif config.source_file_format.lower() == "json":
            options.update({
                "multiLine": config.multiline_values,
                "dateFormat": config.date_format,
                "timestampFormat": config.timestamp_format,
                "encoding": config.encoding,
            })

        elif config.source_file_format.lower() == "parquet":
            options.update({
                "dateFormat": config.date_format,
                "timestampFormat": config.timestamp_format,
            })

        # Handle custom schema if provided
        if config.custom_schema_json:
            custom_schema = SchemaUtils.parse_custom_schema(config.custom_schema_json)
            if custom_schema:
                options["schema"] = custom_schema
                # When custom schema is provided, disable schema inference for CSV
                if config.source_file_format.lower() == "csv":
                    options["inferSchema"] = False
                logger.info(f"Using custom schema for {config.source_file_format} file read")
            else:
                logger.warning("Custom schema provided but failed to parse, falling back to schema inference")

        return options


class ProcessingMetricsUtils:
    """Utilities for working with processing metrics"""

    @staticmethod
    def calculate_performance_metrics(metrics: ProcessingMetrics, write_mode: str = "append") -> ProcessingMetrics:
        """Calculate derived performance metrics"""
        if metrics.total_duration_ms == 0:
            metrics.total_duration_ms = metrics.read_duration_ms + metrics.write_duration_ms

        # Row count reconciliation
        if metrics.source_row_count > 0 and metrics.target_row_count_after >= 0:
            if write_mode.lower() == "overwrite":
                if metrics.source_row_count == metrics.target_row_count_after:
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            elif write_mode.lower() == "append":
                if metrics.source_row_count == (metrics.target_row_count_after - metrics.target_row_count_before):
                    metrics.row_count_reconciliation_status = "matched"
                else:
                    metrics.row_count_reconciliation_status = "mismatched"
            else:
                metrics.row_count_reconciliation_status = "not_verified"

        return metrics

    @staticmethod
    def merge_metrics(metrics_list: List[ProcessingMetrics], write_mode: str = "append") -> ProcessingMetrics:
        """Merge multiple metrics into a single aggregated metric"""
        if not metrics_list:
            return ProcessingMetrics()

        merged = ProcessingMetrics()
        merged.read_duration_ms = sum(m.read_duration_ms for m in metrics_list)
        merged.write_duration_ms = sum(m.write_duration_ms for m in metrics_list)
        merged.total_duration_ms = sum(m.total_duration_ms for m in metrics_list)
        merged.records_processed = sum(m.records_processed for m in metrics_list)
        merged.records_inserted = sum(m.records_inserted for m in metrics_list)
        merged.records_updated = sum(m.records_updated for m in metrics_list)
        merged.records_deleted = sum(m.records_deleted for m in metrics_list)
        merged.source_row_count = sum(m.source_row_count for m in metrics_list)
        merged.target_row_count_before = sum(m.target_row_count_before for m in metrics_list)
        merged.target_row_count_after = sum(m.target_row_count_after for m in metrics_list)

        return ProcessingMetricsUtils.calculate_performance_metrics(merged, write_mode)


class ErrorHandlingUtils:
    """Utilities for error handling in flat file ingestion"""

    @staticmethod
    def format_error_details(error: Exception, context: Dict[str, Any] = None) -> Dict[str, str]:
        """Format error details for logging"""
        details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
        }

        if context:
            details.update({f"context_{k}": str(v) for k, v in context.items()})

        return details


# ============================================================================
# LAKEHOUSE UTILITIES AND LOCATION RESOLVER
# ============================================================================

@dataclass
class FileInfo:
    """Custom FileInfo dataclass for file metadata."""
    path: str
    name: str
    size: int
    modified_ms: int  # Unix timestamp in milliseconds
    is_file: bool
    is_dir: bool

    @property
    def size_mb(self) -> float:
        """Return size in megabytes."""
        return self.size / (1024 * 1024)

    @property
    def extension(self) -> str:
        """Return file extension."""
        return self.name.split('.')[-1] if '.' in self.name else ''

    @property
    def modified(self) -> datetime:
        """Return modified time as datetime object."""
        return datetime.fromtimestamp(self.modified_ms / 1000)


class SimpleLakehouseUtils:
    """Simplified lakehouse utils for Fabric notebooks"""

    def __init__(self, target_workspace_id: str, target_lakehouse_id: str, spark: SparkSession = None):
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id
        self.spark = spark if spark else SparkSession.getActiveSession()

    def lakehouse_tables_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Tables directory."""
        return f"abfss://{self.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self.target_lakehouse_id}/Tables/"

    def lakehouse_files_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Files directory."""
        return f"abfss://{self.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self.target_lakehouse_id}/Files/"

    def write_to_table(self, df: DataFrame, table_name: str, mode: str = "overwrite", partition_by: List[str] = None, schema_name: str = None, options: Dict[str, str] = None) -> None:
        """Write a DataFrame to a lakehouse table."""
        writer = df.write.format("delta").mode(mode)

        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(f"{self.lakehouse_tables_uri()}{table_name}")

    def merge_to_table(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        schema_name: str = None,
        immutable_columns: List[str] = None,
        enable_schema_evolution: bool = False,
        partition_by: List[str] = None,
    ) -> Dict[str, Any]:
        """Merge DataFrame into table using Delta Lake merge operation.

        Args:
            df: Source DataFrame to merge
            table_name: Name of the target table
            merge_keys: Columns to use as join condition for merge
            schema_name: Optional schema name (not used in lakehouse)
            immutable_columns: Columns to preserve from target during updates
                              (default: ["_raw_created_at"])
            enable_schema_evolution: Enable schema auto-merge for new columns
            partition_by: Optional list of columns to partition by (used on initial create)

        Returns:
            Dictionary with merge metrics:
                - records_inserted: Number of new records inserted
                - records_updated: Number of existing records updated
                - records_deleted: Number of records deleted (always 0 for this operation)
                - target_row_count_before: Row count before merge
                - target_row_count_after: Row count after merge
        """
        from delta.tables import DeltaTable

        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        immutable_cols = set(immutable_columns or ["_raw_created_at"])

        # Check if table exists
        try:
            self.read_table(table_name)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            # Initial load - create table with data
            write_options = {}
            if enable_schema_evolution:
                write_options["mergeSchema"] = "true"

            self.write_to_table(
                df=df,
                table_name=table_name,
                mode="overwrite",
                options=write_options,
                partition_by=partition_by,
            )

            # Get row count after initial load
            result_df = self.read_table(table_name)
            row_count = result_df.count()

            return {
                "records_inserted": row_count,
                "records_updated": 0,
                "records_deleted": 0,
                "target_row_count_before": 0,
                "target_row_count_after": row_count,
            }

        # Table exists - perform merge
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Get row count before merge
        target_row_count_before = delta_table.toDF().count()

        # Build merge condition from merge_keys
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )

        # Enable schema evolution if configured
        if enable_schema_evolution:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", "true"
            )

        # Build merge operation
        merge_builder = delta_table.alias("target").merge(
            df.alias("source"), merge_condition
        )

        # When matched: update all columns except immutable ones
        update_columns = {
            col: f"source.{col}" for col in df.columns if col not in immutable_cols
        }
        merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)

        # When not matched: insert all columns
        merge_builder = merge_builder.whenNotMatchedInsertAll()

        # Execute the merge
        merge_builder.execute()

        # Get metrics from Delta table history
        history = delta_table.history(1).select("operationMetrics").collect()[0]
        operation_metrics = history["operationMetrics"]

        # Extract merge metrics
        records_inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
        records_updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
        records_deleted = int(operation_metrics.get("numTargetRowsDeleted", 0))

        # Get row count after merge
        target_row_count_after = delta_table.toDF().count()

        return {
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "target_row_count_before": target_row_count_before,
            "target_row_count_after": target_row_count_after,
        }

    def read_table(self, table_name: str, schema_name: str = None) -> DataFrame:
        """Read data from a table."""
        return self.spark.read.format("delta").load(f"{self.lakehouse_tables_uri()}{table_name}")

    def read_file(self, file_path: str, file_format: str, options: Dict[str, Any] = None) -> DataFrame:
        """Read a file from the file system."""
        if options is None:
            options = {}

        reader = self.spark.read.format(file_format.lower())

        # Apply custom schema if provided
        if "schema" in options:
            reader = reader.schema(options["schema"])

        # Apply CSV options
        if file_format.lower() == "csv":
            if "header" in options:
                reader = reader.option("header", str(options["header"]).lower())
            if "sep" in options:
                reader = reader.option("sep", options["sep"])
            if "inferSchema" in options:
                reader = reader.option("inferSchema", str(options["inferSchema"]).lower())
            for key, value in options.items():
                if key not in ["header", "sep", "inferSchema", "schema"]:
                    reader = reader.option(key, value)

        # Build full file path
        if not file_path.startswith(("file://", "abfss://")):
            full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        else:
            full_file_path = file_path

        return reader.load(full_file_path)

    def list_files_with_metadata(self, directory_path: str, pattern: str = "*", recursive: bool = False) -> List[FileInfo]:
        """List files matching a pattern with metadata using notebookutils."""
        import sys
        matching_files = []

        # Build full directory path
        if not directory_path.startswith(("file://", "abfss://")):
            # Strip leading slash if present to avoid double slashes
            clean_path = directory_path.lstrip("/")
            full_directory_path = f"{self.lakehouse_files_uri()}{clean_path}"
        else:
            full_directory_path = directory_path

        def search(current_path):
            try:
                if "notebookutils" in sys.modules:
                    import notebookutils  # type: ignore

                    all_items = notebookutils.fs.ls(current_path)  # type: ignore

                    for item in all_items:
                        if item.isFile and fnmatch(item.name, pattern):
                            file_info = FileInfo(
                                path=item.path,
                                name=item.name,
                                size=item.size,
                                modified_ms=item.modifyTime,
                                is_file=item.isFile,
                                is_dir=item.isDir,
                            )
                            matching_files.append(file_info)
                        elif item.isDir and recursive:
                            search(item.path)
            except Exception as e:
                # Catch specific errors related to directory not found
                error_msg = str(e).lower()
                if "not found" in error_msg or "does not exist" in error_msg or "pathnotfound" in error_msg:
                    logger.info(f"Directory not found: {current_path}")
                else:
                    logger.warning(f"Error accessing {current_path}: {e}")

        try:
            search(full_directory_path)
        except Exception as e:
            # Catch any top-level errors
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg or "pathnotfound" in error_msg:
                logger.info(f"Directory not found: {full_directory_path}")
            else:
                logger.warning(f"Error listing files in {full_directory_path}: {e}")

        return matching_files

    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get information about a file (size, modification time, etc.)."""
        import sys
        try:
            # Build full file path
            if not file_path.startswith(("file://", "abfss://")):
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
            else:
                full_file_path = file_path

            if "notebookutils" in sys.modules:
                import notebookutils  # type: ignore
                info = notebookutils.fs.ls(full_file_path)[0]  # type: ignore
                return {
                    "path": info.path,
                    "size": info.size,
                    "modified_time": datetime.fromtimestamp(info.modifyTime / 1000),
                    "is_file": info.isFile,
                    "is_directory": info.isDir,
                }
        except Exception as e:
            logger.error(f"Error getting file info for {file_path}: {e}")
            return {}


@dataclass
class LocationConfig:
    """Resolved location configuration"""
    workspace_id: str
    datastore_id: str
    datastore_type: str  # 'lakehouse' or 'warehouse'
    file_root_path: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    location_type: Optional[str] = None


class LocationResolver:
    """Unified location resolver for both source and target scenarios"""

    def __init__(
        self,
        default_workspace_id: Optional[str] = None,
        default_datastore_id: Optional[str] = None,
        default_datastore_type: str = "lakehouse",
        default_file_root_path: str = "Files",
        default_schema_name: str = "default",
    ):
        self.default_workspace_id = default_workspace_id
        self.default_datastore_id = default_datastore_id
        self.default_datastore_type = default_datastore_type
        self.default_file_root_path = default_file_root_path
        self.default_schema_name = default_schema_name
        self._utils_cache: Dict[str, Any] = {}

    def _resolve_source_location(self, config: FlatFileIngestionConfig) -> LocationConfig:
        """Resolve source location with intelligent defaults"""
        workspace_id = (
            config.source_workspace_id
            or config.target_workspace_id
            or self.default_workspace_id
        )

        datastore_id = (
            config.source_datastore_id
            or self.default_datastore_id
            or config.target_datastore_id
        )

        datastore_type = config.source_datastore_type or self.default_datastore_type
        file_root_path = config.source_file_root_path or self.default_file_root_path

        if not workspace_id or not datastore_id:
            raise ValueError(
                f"Could not resolve source location for config {config.config_id}: "
                f"workspace_id={workspace_id}, datastore_id={datastore_id}"
            )

        return LocationConfig(
            workspace_id=workspace_id,
            datastore_id=datastore_id,
            datastore_type=datastore_type.lower(),
            file_root_path=file_root_path,
            location_type="source",
        )

    def _resolve_target_location(self, config: FlatFileIngestionConfig) -> LocationConfig:
        """Resolve target location with intelligent defaults"""
        workspace_id = config.target_workspace_id or self.default_workspace_id
        datastore_id = config.target_datastore_id or self.default_datastore_id
        datastore_type = config.target_datastore_type or self.default_datastore_type
        schema_name = config.target_schema_name or self.default_schema_name

        if not workspace_id or not datastore_id:
            raise ValueError(
                f"Could not resolve target location for config {config.config_id}: "
                f"workspace_id={workspace_id}, datastore_id={datastore_id}"
            )

        return LocationConfig(
            workspace_id=workspace_id,
            datastore_id=datastore_id,
            datastore_type=datastore_type.lower(),
            schema_name=schema_name,
            table_name=config.target_table_name,
            location_type="target",
        )

    def get_source_utils(self, config: FlatFileIngestionConfig, spark=None, connection=None):
        """Get utils instance for source location"""
        location_config = self._resolve_source_location(config)
        cache_key = f"{location_config.workspace_id}_{location_config.datastore_id}_source"

        if cache_key not in self._utils_cache:
            if location_config.datastore_type == "lakehouse":
                self._utils_cache[cache_key] = SimpleLakehouseUtils(
                    target_workspace_id=location_config.workspace_id,
                    target_lakehouse_id=location_config.datastore_id,
                    spark=spark,
                )
            else:
                raise ValueError(f"Unsupported datastore type: {location_config.datastore_type}")

        return self._utils_cache[cache_key]

    def get_target_utils(self, config: FlatFileIngestionConfig, spark=None, connection=None):
        """Get utils instance for target location"""
        location_config = self._resolve_target_location(config)
        cache_key = f"{location_config.workspace_id}_{location_config.datastore_id}_target"

        if cache_key not in self._utils_cache:
            if location_config.datastore_type == "lakehouse":
                self._utils_cache[cache_key] = SimpleLakehouseUtils(
                    target_workspace_id=location_config.workspace_id,
                    target_lakehouse_id=location_config.datastore_id,
                    spark=spark,
                )
            else:
                raise ValueError(f"Unsupported datastore type: {location_config.datastore_type}")

        return self._utils_cache[cache_key]

    def resolve_full_source_path(self, config: FlatFileIngestionConfig) -> str:
        """Resolve the full source file path including root path"""
        source_config = self._resolve_source_location(config)

        # For lakehouse, paths are relative to Files/ (which lakehouse_files_uri already includes)
        if source_config.datastore_type == "lakehouse":
            # Strip leading slash if present to ensure relative path
            if config.source_file_path.startswith("/"):
                return config.source_file_path.lstrip("/")
            return config.source_file_path

        # For non-lakehouse datastores, prepend file_root_path
        # Handle absolute paths
        if config.source_file_path.startswith("/"):
            return config.source_file_path

        # Handle paths that already include the root
        if config.source_file_path.startswith(source_config.file_root_path):
            return config.source_file_path

        # Combine root path with relative path
        return f"{source_config.file_root_path}/{config.source_file_path}".replace("//", "/")


# ============================================================================
# PYSPARK IMPLEMENTATION CLASSES
# ============================================================================

class PySparkFlatFileDiscovery:
    """PySpark implementation of file discovery using dynamic location resolution"""

    def __init__(self, location_resolver, spark=None):
        self.location_resolver = location_resolver
        self.spark = spark
        self.discovery_strategies = {
            "single_file": self._discover_single_file,
            "wildcard_pattern": self._discover_wildcard_files,
            "date_partitioned": self._discover_date_partitioned_files,
        }
        # Track duplicate files found during last discovery
        self.last_duplicate_files = []

    def discover_files(self, config: FlatFileIngestionConfig) -> List[FileDiscoveryResult]:
        """Discover files based on configuration using dynamic source resolution"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })
        full_source_path = self.location_resolver.resolve_full_source_path(config)

        strategy = self.discovery_strategies.get(config.import_pattern)
        if strategy:
            return strategy(config, full_source_path)
        else:
            config_logger.warning(f"Unknown import pattern: {config.import_pattern}")
            return []

    def _discover_single_file(self, config: FlatFileIngestionConfig, file_path: str) -> List[FileDiscoveryResult]:
        """Discover a single file with metadata"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })

        try:
            source_utils = self.location_resolver.get_source_utils(config, spark=self.spark)

            file_info = source_utils.get_file_info(file_path)

            full_file_path = file_info.get("path", file_path)

            # Extract filename for attribute extraction
            filename = os.path.basename(full_file_path)

            # Extract filename attributes if pattern is configured
            filename_attributes = self._extract_filename_attributes(
                filename, config.filename_attribute_pattern
            )

            result = FileDiscoveryResult(
                file_path=full_file_path,
                size=file_info.get("size", 0),
                modified_time=file_info.get("modified_time"),
                filename_attributes=filename_attributes if filename_attributes else None,
            )

            if filename_attributes:
                config_logger.info(f"Extracted attributes from filename: {filename_attributes}")

            config_logger.info(f"Single file: {file_path} ({result.size / (1024*1024):.2f} MB)")
            return [result]

        except Exception as e:
            config_logger.error(f"Could not get metadata for {file_path}: {e}")
            raise

    def _discover_wildcard_files(self, config: FlatFileIngestionConfig, base_path: str) -> List[FileDiscoveryResult]:
        """Discover files using wildcard pattern with incremental filtering"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })

        try:
            source_utils = self.location_resolver.get_source_utils(config, spark=self.spark)

            config_logger.info(f"Discovering wildcard files at: {base_path}")
            config_logger.info(f"Pattern: {config.file_discovery_pattern or '*'}")

            file_infos = source_utils.list_files_with_metadata(
                directory_path=base_path,
                pattern=config.file_discovery_pattern or "*",
                recursive=False,
            )

            # Filter to unprocessed files and detect duplicates (always for wildcard pattern)
            unprocessed_files, duplicate_files = self._get_unprocessed_files(file_infos, config)

            # Store duplicates for orchestrator to log
            self.last_duplicate_files = duplicate_files

            discovered_files = []
            for file_info in unprocessed_files:
                # Extract filename for attribute extraction
                filename = os.path.basename(file_info.path)

                # Extract filename attributes if pattern is configured
                filename_attributes = self._extract_filename_attributes(
                    filename, config.filename_attribute_pattern
                )

                file_result = FileDiscoveryResult(
                    file_path=file_info.path,
                    size=file_info.size,
                    modified_time=file_info.modified,
                    filename_attributes=filename_attributes if filename_attributes else None,
                )

                discovered_files.append(file_result)

            # Sort by filename attribute if configured
            if config.filename_sort_attribute and discovered_files:
                config_logger.info(f"Sorting files by attribute: {config.filename_sort_attribute}")
                discovered_files.sort(
                    key=lambda x: x.filename_attributes.get(config.filename_sort_attribute, "")
                    if x.filename_attributes
                    else ""
                )

            config_logger.info(f"Discovered {len(discovered_files)} unprocessed files to ingest")
            return discovered_files

        except Exception as e:
            config_logger.error(f"Wildcard file discovery failed: {e}")
            raise

    def _get_unprocessed_files(self, file_infos: list, config: FlatFileIngestionConfig) -> Tuple[list, list]:
        """Return unprocessed files and duplicate files based on filename

        Two-step filtering process:
        1. Filter by modified time: Only consider files newer than last run
        2. Check for duplicate filenames: Among newer files, identify duplicates

        Args:
            file_infos: List of discovered file info objects
            config: Configuration object

        Returns:
            Tuple[list, list]: (unprocessed_files, duplicate_files)

        Raises:
            DuplicateFilesError: If duplicate_file_handling='fail' and duplicates found
        """
        config_logger = ConfigLoggerAdapter(
            logger, {"config_name": config.config_name, "execution_group": config.execution_group}
        )

        if not file_infos:
            return [], []

        try:
            import os

            # STEP 1: Filter by modified time - only consider newer files
            # Include 'running' status to handle orphaned writes (logging failure after successful write)
            latest_time_result = self.spark.sql(
                f"""
                SELECT MAX(source_file_modified_time) as latest_time
                FROM log_file_load
                WHERE config_id = '{config.config_id}'
                AND status IN ('completed', 'duplicate', 'running')
                AND source_file_modified_time IS NOT NULL
                """
            ).collect()

            if latest_time_result and latest_time_result[0].latest_time:
                latest_processed_ms = int(latest_time_result[0].latest_time.timestamp() * 1000)
                newer_files = [f for f in file_infos if f.modified_ms > latest_processed_ms]
                config_logger.info(f"Found {len(newer_files)} file(s) newer than last run")
            else:
                config_logger.info("No previous runs found, processing all files")
                newer_files = file_infos

            if not newer_files:
                config_logger.info("No new files to process")
                return [], []

            # STEP 2: Among newer files, check for duplicate filenames
            discovered_data = [
                (os.path.basename(f.path), f.path, f.size, f.modified_ms) for f in newer_files
            ]

            discovered_df = self.spark.createDataFrame(
                discovered_data, ["filename", "file_path", "size", "modified_ms"]
            )

            # STEP 3: Query log table to get most recent status per file
            processed_df = self.spark.sql(
                f"""
                WITH latest_logs AS (
                    SELECT substring_index(source_file_path, '/', -1) as filename,
                           status,
                           ROW_NUMBER() OVER (PARTITION BY source_file_path ORDER BY created_date DESC) as rn
                    FROM log_file_load
                    WHERE config_id = '{config.config_id}'
                )
                SELECT DISTINCT filename
                FROM latest_logs
                WHERE rn = 1
                AND status = 'completed'
                """
            )

            # INNER JOIN: Find duplicates (newer files with matching filename)
            duplicates_df = discovered_df.join(processed_df, on="filename", how="inner")
            duplicate_filenames = {row.filename for row in duplicates_df.collect()}

            # LEFT ANTI JOIN: Find unprocessed (newer files without matching filename)
            unprocessed_df = discovered_df.join(processed_df, on="filename", how="left_anti")
            unprocessed_filenames = {row.filename for row in unprocessed_df.collect()}

            # Separate the newer_files into duplicates and unprocessed
            duplicate_files = [
                f for f in newer_files if os.path.basename(f.path) in duplicate_filenames
            ]
            unprocessed_files = [
                f for f in newer_files if os.path.basename(f.path) in unprocessed_filenames
            ]

            # Handle based on duplicate_file_handling setting
            if config.duplicate_file_handling == "allow":
                # Process all files including duplicates as normal
                all_files = unprocessed_files + duplicate_files
                if duplicate_files:
                    config_logger.info(
                        f"Found {len(all_files)} file(s) to ingest (including {len(duplicate_files)} duplicates)"
                    )
                else:
                    config_logger.info(f"Found {len(all_files)} unprocessed file(s) to ingest")
                return all_files, []
            elif config.duplicate_file_handling == "fail":
                if duplicate_files:
                    duplicate_names = [os.path.basename(f.path) for f in duplicate_files]
                    error_msg = f"Duplicate files detected: {', '.join(duplicate_names)}"
                    config_logger.error(error_msg)
                    raise DuplicateFilesError(error_msg)
                config_logger.info(f"Found {len(unprocessed_files)} unprocessed file(s) to ingest")
                return unprocessed_files, []
            else:  # "skip" mode (validated, so this is the only remaining option)
                if duplicate_files:
                    config_logger.info(f"Skipping {len(duplicate_files)} duplicate file(s)")
                config_logger.info(f"Found {len(unprocessed_files)} unprocessed file(s) to ingest")
                return unprocessed_files, duplicate_files

        except DuplicateFilesError:
            # Re-raise DuplicateFilesError without catching
            raise
        except Exception as e:
            config_logger.warning(f"Could not check for duplicates, processing all files: {e}")
            return file_infos, []

    def _discover_date_partitioned_files(self, config: FlatFileIngestionConfig, full_source_path: str) -> List[FileDiscoveryResult]:
        """Discover files in date-partitioned structure"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })
        # Simplified version - implement full logic as needed
        config_logger.info(f"Detected date-partitioned import")
        return []

    def date_already_processed(self, date_partition: str, config: FlatFileIngestionConfig) -> bool:
        """Check if a date has already been processed"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })

        try:
            log_df = self.spark.sql(
                f"""
                SELECT DISTINCT date_partition, status
                FROM log_file_load
                WHERE config_id = '{config.config_id}'
                AND date_partition = '{date_partition}'
                AND status = 'completed'
                LIMIT 1
            """
            )

            return log_df.count() > 0

        except Exception as e:
            config_logger.warning(f"Could not check if date {date_partition} already processed: {e}")
            return False

    def _extract_filename_attributes(
        self, filename: str, pattern: str
    ) -> Dict[str, str]:
        """Extract attributes from filename using regex named groups

        Args:
            filename: The filename to extract attributes from
            pattern: Regex pattern with named groups (e.g., r"orders_(?P<batch_date>\d{8})\.csv")

        Returns:
            Dictionary of extracted attributes {attribute_name: value}
            Returns empty dict if pattern is None or doesn't match
        """
        import re

        if not pattern:
            return {}

        try:
            match = re.search(pattern, filename)
            if match:
                return match.groupdict()
            return {}
        except Exception as e:
            logger.warning(f"Error extracting filename attributes from '{filename}' with pattern '{pattern}': {e}")
            return {}


class PySparkFlatFileProcessor:
    """PySpark implementation of flat file processing"""

    def __init__(self, spark_session: SparkSession, location_resolver):
        self.spark = spark_session
        self.location_resolver = location_resolver

    def read_file(self, config: FlatFileIngestionConfig, file_result: FileDiscoveryResult) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read a file based on configuration and return DataFrame with metrics"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            source_utils = self.location_resolver.get_source_utils(config, spark=self.spark)
            options = ConfigurationUtils.get_file_read_options(config)

            df = source_utils.read_file(
                file_path=file_result.file_path,
                file_format=config.source_file_format,
                options=options,
            )

            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            config_logger.info(f"Read {metrics.source_row_count} records from source file in {metrics.read_duration_ms}ms")

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            config_logger.error(f"Error reading file {file_result.file_path}: {e}")
            raise

    def batch_read_files(
        self, config: FlatFileIngestionConfig, file_results: List[FileDiscoveryResult]
    ) -> Tuple[DataFrame, ProcessingMetrics]:
        """Batch read multiple files at once using Spark's native multi-file reading

        This is much more efficient than looping through files individually.
        Spark can read multiple files in parallel and optimize the read operation.
        """
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get file reading options
            options = ConfigurationUtils.get_file_read_options(config)

            # Extract file paths - these should already be full ABFSS URIs
            file_paths = [f.file_path for f in file_results]

            config_logger.info(f"Batch reading {len(file_paths)} files in a single operation")

            # Use Spark reader directly with list of paths for batch reading
            reader = self.spark.read.format(config.source_file_format.lower())

            # Apply custom schema if provided
            if "schema" in options:
                reader = reader.schema(options["schema"])

            # Apply options based on file format
            if config.source_file_format.lower() == "csv":
                if "header" in options:
                    reader = reader.option("header", str(options["header"]).lower())
                if "sep" in options:
                    reader = reader.option("sep", options["sep"])
                if "inferSchema" in options:
                    reader = reader.option("inferSchema", str(options["inferSchema"]).lower())
                for key, value in options.items():
                    if key not in ["header", "sep", "inferSchema", "schema"]:
                        reader = reader.option(key, value)

            # Load multiple files at once - Spark handles this efficiently
            # Pass list of paths directly to load()
            df = reader.load(file_paths)

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            config_logger.info(
                f"Batch read {metrics.source_row_count} records from {len(file_paths)} files in {metrics.read_duration_ms}ms"
            )

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            config_logger.error(f"Error in batch read: {e}")
            raise

    def _add_ingestion_metadata(
        self,
        df: DataFrame,
        load_id: str,
        file_result: FileDiscoveryResult,
        config: FlatFileIngestionConfig,
    ) -> DataFrame:
        """Add ingestion metadata columns to DataFrame including source file attributes"""
        from pyspark.sql.functions import current_timestamp, lit
        import os

        # Start with the original dataframe
        result_df = df

        # Add extracted filename attributes as columns (before _raw_filename)
        if file_result.filename_attributes:
            for attr_name, attr_value in file_result.filename_attributes.items():
                result_df = result_df.withColumn(attr_name, lit(attr_value))

        # Add load_id for linking to log table
        result_df = result_df.withColumn("_raw_load_id", lit(load_id))

        # Add filename for lineage
        filename = os.path.basename(file_result.file_path)
        result_df = result_df.withColumn("_raw_filename", lit(filename))

        # Add standard ingestion timestamps (last)
        result_df = result_df.withColumn("_raw_created_at", current_timestamp()) \
                             .withColumn("_raw_updated_at", current_timestamp())

        return result_df

    def _execute_delta_merge(self, source_df: DataFrame, target_utils, config: FlatFileIngestionConfig, write_start: float) -> ProcessingMetrics:
        """Execute Delta Lake merge operation with full metrics tracking

        Merge Behavior:
        - NOT matched: Insert all columns (new record)
        - Matched: Update all columns EXCEPT _raw_created_at (preserved from target)

        This ensures _raw_created_at is immutable while _raw_updated_at,
        _raw_filename, and business columns reflect the latest source data.
        """
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })

        metrics = ProcessingMetrics()

        try:
            # Delegate to lakehouse_utils merge_to_table method
            merge_result = target_utils.merge_to_table(
                df=source_df,
                table_name=config.target_table_name,
                merge_keys=config.merge_keys,
                schema_name=config.target_schema_name,
                immutable_columns=["_raw_created_at"],
                enable_schema_evolution=config.enable_schema_evolution,
                partition_by=config.partition_columns,
            )

            # Populate metrics from returned dictionary
            metrics.records_inserted = merge_result["records_inserted"]
            metrics.records_updated = merge_result["records_updated"]
            metrics.records_deleted = merge_result["records_deleted"]
            metrics.target_row_count_before = merge_result["target_row_count_before"]
            metrics.target_row_count_after = merge_result["target_row_count_after"]

            # Calculate timing
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)

            return ProcessingMetricsUtils.calculate_performance_metrics(
                metrics, config.write_mode
            )

        except ValueError as e:
            # Handle merge key validation errors
            error_msg = str(e)
            if "merge_keys" in error_msg.lower():
                config_logger.error(f"Merge failed: One or more merge keys don't exist in source or target")
                config_logger.error(f"   Merge keys specified: {config.merge_keys}")
                config_logger.error(f"   Check that these column names exist in both your source file and target table")
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            raise
        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            raise

    def write_data(self, data: DataFrame, config: FlatFileIngestionConfig) -> ProcessingMetrics:
        """Write data to target destination"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })
        write_start = time.time()
        metrics = ProcessingMetrics()

        try:
            target_utils = self.location_resolver.get_target_utils(config, spark=self.spark)

            # Handle merge mode separately using Delta Lake merge
            if config.write_mode.lower() == "merge":
                config_logger.info(f"Executing merge operation with keys: {config.merge_keys}")
                return self._execute_delta_merge(
                    source_df=data,
                    target_utils=target_utils,
                    config=config,
                    write_start=write_start,
                )

            # Standard write modes (overwrite, append)
            # Get target row count before write
            try:
                existing_df = target_utils.read_table(config.target_table_name)
                metrics.target_row_count_before = existing_df.count()
            except Exception:
                metrics.target_row_count_before = 0

            # Prepare write options for schema evolution
            write_options = {}
            if config.enable_schema_evolution:
                write_options["mergeSchema"] = "true"

            # Write data
            target_utils.write_to_table(
                df=data,
                table_name=config.target_table_name,
                mode=config.write_mode,
                options=write_options,
                partition_by=config.partition_columns,
            )

            # Get target row count after write
            try:
                result_df = target_utils.read_table(config.target_table_name)
                metrics.target_row_count_after = result_df.count()
            except Exception:
                metrics.target_row_count_after = 0

            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)

            if config.write_mode == "overwrite":
                metrics.records_inserted = metrics.target_row_count_after
                metrics.records_deleted = metrics.target_row_count_before
            elif config.write_mode == "append":
                metrics.records_inserted = metrics.target_row_count_after - metrics.target_row_count_before

            config_logger.info(f"Wrote data to {config.target_table_name} in {metrics.write_duration_ms}ms")

            return ProcessingMetricsUtils.calculate_performance_metrics(metrics, config.write_mode)

        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            config_logger.error(f"Error writing data to {config.target_table_name}: {e}")
            raise

    def validate_data(self, data: DataFrame, config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Validate data based on configuration rules"""
        validation_results = {
            "is_valid": True,
            "row_count": 0,
            "column_count": 0,
            "errors": [],
        }

        try:
            validation_results["row_count"] = data.count()
            validation_results["column_count"] = len(data.columns)

            if validation_results["row_count"] == 0:
                validation_results["errors"].append("No data found in source")
                validation_results["is_valid"] = False

        except Exception as e:
            validation_results["errors"].append(f"Validation failed: {e}")
            validation_results["is_valid"] = False

        return validation_results


# ============================================================================
# SCHEMA DEFINITIONS FOR LOGGING
# ============================================================================

def get_config_execution_log_schema():
    """Returns the standardized schema for the log_config_execution table."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("execution_id", StringType(), nullable=False),
        StructField("config_id", StringType(), nullable=False),
        StructField("config_name", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("files_discovered", LongType(), nullable=True),
        StructField("files_processed", LongType(), nullable=True),
        StructField("files_failed", LongType(), nullable=True),
        StructField("files_skipped", LongType(), nullable=True),
        StructField("records_processed", LongType(), nullable=True),
        StructField("records_inserted", LongType(), nullable=True),
        StructField("records_updated", LongType(), nullable=True),
        StructField("records_deleted", LongType(), nullable=True),
        StructField("total_duration_ms", LongType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        StructField("error_details", StringType(), nullable=True),
        StructField("created_date", TimestampType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
    ])


def get_file_load_log_schema():
    """Returns the standardized schema for the log_file_load table."""
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

    return StructType([
        StructField("log_id", StringType(), nullable=False),
        StructField("load_id", StringType(), nullable=False),
        StructField("execution_id", StringType(), nullable=False),
        StructField("config_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField("source_file_size_bytes", LongType(), nullable=True),
        StructField("source_file_modified_time", TimestampType(), nullable=True),
        StructField("target_table_name", StringType(), nullable=False),
        StructField("records_processed", LongType(), nullable=True),
        StructField("records_inserted", LongType(), nullable=True),
        StructField("records_updated", LongType(), nullable=True),
        StructField("records_deleted", LongType(), nullable=True),
        StructField("source_row_count", LongType(), nullable=True),
        StructField("target_row_count_before", LongType(), nullable=True),
        StructField("target_row_count_after", LongType(), nullable=True),
        StructField("row_count_reconciliation_status", StringType(), nullable=True),
        StructField("data_read_duration_ms", LongType(), nullable=True),
        StructField("total_duration_ms", LongType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        StructField("error_details", StringType(), nullable=True),
        StructField("execution_duration_seconds", IntegerType(), nullable=True),
        StructField("source_file_partition_cols", StringType(), nullable=True),
        StructField("source_file_partition_values", StringType(), nullable=True),
        StructField("date_partition", StringType(), nullable=True),
        StructField("filename_attributes_json", StringType(), nullable=True),
        StructField("created_date", TimestampType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
    ])


class PySparkFlatFileLogging:
    """PySpark implementation of flat file ingestion logging"""

    def __init__(self, lakehouse_utils):
        self.lakehouse_utils = lakehouse_utils

    def log_config_execution_start(self, config: FlatFileIngestionConfig, execution_id: str) -> None:
        """Log the start of a config execution run"""
        try:
            schema = get_config_execution_log_schema()

            log_data = [(
                execution_id,
                config.config_id,
                config.config_name,
                "running",
                None, None, None, None,  # files_discovered, files_processed, files_failed, files_skipped
                None, None, None, None,  # records_processed, records_inserted, records_updated, records_deleted
                None,  # total_duration_ms
                None, None,  # error_message, error_details
                datetime.now(),
                "system",
            )]

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_config_execution", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log config execution start: {e}")

    def log_config_execution_completion(self, config: FlatFileIngestionConfig, execution_id: str, files_discovered: int, files_processed: int, files_failed: int, files_skipped: int, metrics: ProcessingMetrics, status: str) -> None:
        """Log the completion of a config execution run"""
        try:
            schema = get_config_execution_log_schema()

            log_data = [(
                execution_id,
                config.config_id,
                config.config_name,
                status,
                files_discovered,
                files_processed,
                files_failed,
                files_skipped,
                metrics.records_processed,
                metrics.records_inserted,
                metrics.records_updated,
                metrics.records_deleted,
                metrics.total_duration_ms,
                None, None,  # error_message, error_details
                datetime.now(),
                "system",
            )]

            schema = get_config_execution_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_config_execution", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log config execution completion: {e}")

    def log_config_execution_error(self, config: FlatFileIngestionConfig, execution_id: str, error_message: str, error_details: str, files_discovered: int = 0, files_processed: int = 0, files_failed: int = 0) -> None:
        """Log a config execution error"""
        try:
            schema = get_config_execution_log_schema()

            log_data = [(
                execution_id,
                config.config_id,
                config.config_name,
                "failed",
                files_discovered,
                files_processed,
                files_failed,
                0,  # files_skipped
                0, 0, 0, 0,  # records_processed, records_inserted, records_updated, records_deleted
                None,  # total_duration_ms
                error_message,
                error_details,
                datetime.now(),
                "system",
            )]

            schema = get_config_execution_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_config_execution", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log config execution error: {e}")

    def log_file_load_start(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, file_result: Optional[FileDiscoveryResult] = None) -> None:
        """Log the start of a file load"""
        try:
            import json
            schema = get_file_load_log_schema()

            # Extract file metadata from file_result
            source_file_path = config.source_file_path
            source_file_size_bytes = None
            source_file_modified_time = None
            date_partition = None
            partition_cols = None
            partition_values = None
            filename_attributes_json = None

            if file_result:
                source_file_path = file_result.file_path
                source_file_size_bytes = file_result.size
                source_file_modified_time = file_result.modified_time
                date_partition = file_result.date_partition

                if file_result.date_partition:
                    partition_cols = json.dumps(["date"])
                    partition_values = json.dumps([file_result.date_partition])

                # Store filename attributes as JSON
                if file_result.filename_attributes:
                    filename_attributes_json = json.dumps(file_result.filename_attributes)

            log_data = [(
                str(uuid.uuid4()),
                load_id,
                execution_id,
                config.config_id,
                "running",
                source_file_path,
                source_file_size_bytes,
                source_file_modified_time,
                config.target_table_name,
                None, None, None, None, None, None, None, None, None, None, None,
                None, None,
                partition_cols,
                partition_values,
                date_partition,
                filename_attributes_json,
                datetime.now(),
                "system",
            )]

            schema = get_file_load_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_file_load", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log file load start: {e}")

    def log_execution_start(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, file_result: Optional[FileDiscoveryResult] = None) -> None:
        """DEPRECATED: Use log_file_load_start instead. Kept for backwards compatibility."""
        self.log_file_load_start(config, execution_id, load_id, file_result)

    def log_file_load_completion(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, metrics: ProcessingMetrics, status: str, file_result: Optional[FileDiscoveryResult] = None) -> None:
        """Log the completion of a file load"""
        try:
            import json
            schema = get_file_load_log_schema()

            # Extract file metadata from file_result
            source_file_path = config.source_file_path
            source_file_size_bytes = None
            source_file_modified_time = None
            date_partition = None
            partition_cols = None
            partition_values = None
            filename_attributes_json = None

            if file_result:
                source_file_path = file_result.file_path
                if file_result.size:
                    source_file_size_bytes = file_result.size
                source_file_modified_time = file_result.modified_time
                date_partition = file_result.date_partition

                if file_result.date_partition:
                    partition_cols = json.dumps(["date"])
                    partition_values = json.dumps([file_result.date_partition])

                # Store filename attributes as JSON
                if file_result.filename_attributes:
                    filename_attributes_json = json.dumps(file_result.filename_attributes)

            log_data = [(
                str(uuid.uuid4()),
                load_id,
                execution_id,
                config.config_id,
                status,
                source_file_path,
                source_file_size_bytes,
                source_file_modified_time,
                config.target_table_name,
                metrics.records_processed,
                metrics.records_inserted,
                metrics.records_updated,
                metrics.records_deleted,
                metrics.source_row_count,
                metrics.target_row_count_before,
                metrics.target_row_count_after,
                metrics.row_count_reconciliation_status,
                metrics.read_duration_ms,
                metrics.total_duration_ms,
                None,
                None,
                int(metrics.total_duration_ms / 1000) if metrics.total_duration_ms else None,
                partition_cols,
                partition_values,
                date_partition,
                filename_attributes_json,
                datetime.now(),
                "system",
            )]

            schema = get_file_load_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_file_load", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log file load completion: {e}")

    def log_execution_completion(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, metrics: ProcessingMetrics, status: str, file_result: Optional[FileDiscoveryResult] = None) -> None:
        """DEPRECATED: Use log_file_load_completion instead. Kept for backwards compatibility."""
        self.log_file_load_completion(config, execution_id, load_id, metrics, status, file_result)

    def log_file_load_error(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, error_message: str, error_details: str, file_result: Optional[FileDiscoveryResult] = None) -> None:
        """Log a file load error"""
        try:
            import json
            schema = get_file_load_log_schema()

            # Extract file metadata from file_result
            source_file_path = config.source_file_path
            source_file_size_bytes = None
            source_file_modified_time = None
            date_partition = None
            partition_cols = None
            partition_values = None
            filename_attributes_json = None

            if file_result:
                source_file_path = file_result.file_path
                source_file_size_bytes = file_result.size
                source_file_modified_time = file_result.modified_time
                date_partition = file_result.date_partition

                if file_result.date_partition:
                    partition_cols = json.dumps(["date"])
                    partition_values = json.dumps([file_result.date_partition])

                # Store filename attributes as JSON
                if file_result.filename_attributes:
                    filename_attributes_json = json.dumps(file_result.filename_attributes)

            log_data = [(
                str(uuid.uuid4()),
                load_id,
                execution_id,
                config.config_id,
                "failed",
                source_file_path,
                source_file_size_bytes,
                source_file_modified_time,
                config.target_table_name,
                0, 0, 0, 0, 0, 0, 0,
                "not_verified",
                None, None,
                error_message,
                error_details,
                None,
                partition_cols,
                partition_values,
                date_partition,
                filename_attributes_json,
                datetime.now(),
                "system",
            )]

            schema = get_file_load_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_file_load", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log file load error: {e}")

    def log_execution_error(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, error_message: str, error_details: str, file_result: Optional[FileDiscoveryResult] = None) -> None:
        """DEPRECATED: Use log_file_load_error instead. Kept for backwards compatibility."""
        self.log_file_load_error(config, execution_id, load_id, error_message, error_details, file_result)

    def log_duplicate_file(self, config: FlatFileIngestionConfig, execution_id: str, load_id: str, file_result: FileDiscoveryResult) -> None:
        """Log a duplicate file that was skipped"""
        try:
            import json
            schema = get_file_load_log_schema()

            # Extract file metadata from file_result
            source_file_path = file_result.file_path
            source_file_size_bytes = file_result.size
            source_file_modified_time = file_result.modified_time
            date_partition = file_result.date_partition
            partition_cols = None
            partition_values = None
            filename_attributes_json = None

            if file_result.date_partition:
                partition_cols = json.dumps(["date"])
                partition_values = json.dumps([file_result.date_partition])

            # Store filename attributes as JSON
            if file_result.filename_attributes:
                filename_attributes_json = json.dumps(file_result.filename_attributes)

            log_data = [(
                str(uuid.uuid4()),
                load_id,
                execution_id,
                config.config_id,
                "duplicate",
                source_file_path,
                source_file_size_bytes,
                source_file_modified_time,
                config.target_table_name,
                0, 0, 0, 0, 0, 0, 0,
                "not_verified",
                None, None, None, None, None,
                partition_cols,
                partition_values,
                date_partition,
                filename_attributes_json,
                datetime.now(),
                "system",
            )]

            schema = get_file_load_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)
            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_file_load", mode="append")

        except Exception as e:
            logger.warning(f"Failed to log duplicate file: {e}")


class PySparkFlatFileIngestionOrchestrator:
    """PySpark implementation of flat file ingestion orchestrator"""

    def __init__(
        self,
        discovery_service,
        processor_service,
        logging_service,
        max_concurrency: int = 4,
    ):
        """Initialize orchestrator with configurable concurrency

        Args:
            discovery_service: File discovery service implementation
            processor_service: File processor service implementation
            logging_service: Logging service implementation
            max_concurrency: Maximum number of concurrent threads for parallel processing (default: 4)
        """
        self.discovery_service = discovery_service
        self.processor_service = processor_service
        self.logging_service = logging_service
        self.max_concurrency = max_concurrency

    def process_configuration(self, config: FlatFileIngestionConfig, execution_id: str) -> Dict[str, Any]:
        """Process a single configuration"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })
        start_time = time.time()
        result = {
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "pending",
            "metrics": ProcessingMetrics(),
            "errors": [],
            "files_discovered": 0,
            "files_processed": 0,
            "files_failed": 0,
            "files_skipped": 0,
        }

        # Log config execution start
        self.logging_service.log_config_execution_start(config, execution_id)

        try:
            config_logger.info(f"Source: {config.source_file_path}")
            # Format target with schema only if not empty
            target_display = f"{config.target_schema_name}.{config.target_table_name}" if config.target_schema_name else config.target_table_name
            config_logger.info(f"Target: {target_display}")
            config_logger.info(f"Format: {config.source_file_format}")
            config_logger.info(f"Write Mode: {config.write_mode}")

            # Validate configuration
            validation_errors = ConfigurationUtils.validate_config(config)
            if validation_errors:
                result["errors"] = validation_errors
                result["status"] = "failed"
                config_logger.error(f"Configuration validation failed: {', '.join(validation_errors)}")

                # Log config execution error
                error_details = ErrorHandlingUtils.format_error_details(
                    Exception(', '.join(validation_errors)), {"config_id": config.config_id}
                )
                self.logging_service.log_config_execution_error(
                    config, execution_id, ', '.join(validation_errors), str(error_details)
                )
                return result

            # Discover files
            discovered_files = self.discovery_service.discover_files(config)
            files_discovered = len(discovered_files)
            result["files_discovered"] = files_discovered

            # Log duplicate files if any were found during discovery
            if hasattr(self.discovery_service, 'last_duplicate_files'):
                duplicate_files = self.discovery_service.last_duplicate_files
                if duplicate_files:
                    config_logger.info(f"Logging {len(duplicate_files)} duplicate file(s)")
                    result["files_skipped"] = len(duplicate_files)
                    for file_info in duplicate_files:
                        # Generate unique load_id for duplicate file log entry
                        duplicate_load_id = str(uuid.uuid4())
                        # Convert file_info to FileDiscoveryResult for logging
                        duplicate_file_result = FileDiscoveryResult(
                            file_path=file_info.path,
                            size=file_info.size,
                            modified_time=file_info.modified,
                            filename_attributes=None,
                        )
                        self.logging_service.log_duplicate_file(
                            config, execution_id, duplicate_load_id, duplicate_file_result
                        )

            if not discovered_files:
                if config.require_files:
                    error_msg = "No files found and require_files is set to True"
                    config_logger.error(error_msg)
                    raise Exception(error_msg)

                result["status"] = "no_data_found"
                result["files_processed"] = 0
                config_logger.warning(f"No source data found. Skipping write and reconciliation operations.")

                # Log config execution completion for no_data case
                self.logging_service.log_config_execution_completion(
                    config, execution_id, files_discovered, 0, 0, result.get("files_skipped", 0), ProcessingMetrics(), "no_data_found"
                )
                return result

            # Sequential processing: read and write files one-by-one
            config_logger.info(f"Processing {len(discovered_files)} file(s) sequentially")

            all_metrics = []

            for file_result in discovered_files:
                # Generate ONE load_id per file (per write transaction)
                file_load_id = str(uuid.uuid4())

                try:
                    # Log start for each discovered file
                    self.logging_service.log_execution_start(config, execution_id, file_load_id, file_result)

                    # Read file
                    df, read_metrics = self.processor_service.read_file(config, file_result)

                    if read_metrics.source_row_count == 0:
                        config_logger.warning(f"No data found in file: {file_result.file_path}")
                        continue

                    # Add ingestion metadata (including load_id)
                    df = self.processor_service._add_ingestion_metadata(df, file_load_id, file_result, config)

                    # Validate data
                    validation_result = self.processor_service.validate_data(df, config)
                    if not validation_result["is_valid"]:
                        result["errors"].extend(validation_result["errors"])
                        config_logger.error(f"Validation failed for file {file_result.file_path}: {validation_result['errors']}")
                        continue

                    # Write file
                    write_metrics = self.processor_service.write_data(df, config)

                    # Combine metrics
                    combined_metrics = ProcessingMetrics(
                        read_duration_ms=read_metrics.read_duration_ms,
                        write_duration_ms=write_metrics.write_duration_ms,
                        records_processed=read_metrics.records_processed,
                        records_inserted=write_metrics.records_inserted,
                        records_updated=write_metrics.records_updated,
                        records_deleted=write_metrics.records_deleted,
                        source_row_count=read_metrics.source_row_count,
                        target_row_count_before=write_metrics.target_row_count_before,
                        target_row_count_after=write_metrics.target_row_count_after,
                    )

                    all_metrics.append(combined_metrics)

                    # Log per-file completion for wildcard/single_file patterns
                    if not file_result.date_partition:
                        self.logging_service.log_execution_completion(
                            config, execution_id, file_load_id, combined_metrics, "completed", file_result
                        )

                except Exception as file_error:
                    error_details = ErrorHandlingUtils.format_error_details(
                        file_error,
                        {"file_path": file_result.file_path, "config_id": config.config_id},
                    )
                    result["errors"].append(f"Error processing file {file_result.file_path}: {file_error}")
                    result["files_failed"] = result.get("files_failed", 0) + 1
                    config_logger.error(f"Error processing file {file_result.file_path}: {file_error}")

                    # Log error for this file
                    self.logging_service.log_execution_error(
                        config, execution_id, file_load_id, str(file_error), str(error_details), file_result
                    )

            # Aggregate metrics
            if all_metrics:
                result["metrics"] = ProcessingMetricsUtils.merge_metrics(all_metrics, config.write_mode)
                result["status"] = "completed"

                # Track files processed: one metrics object per file
                result["files_processed"] = len(all_metrics)

                end_time = time.time()
                processing_duration = end_time - start_time
                result["metrics"].total_duration_ms = int(processing_duration * 1000)

                config_logger.info(f"Processing completed in {processing_duration:.2f} seconds")
                config_logger.info(f"Records processed: {result['metrics'].records_processed}")
                config_logger.info(f"Records inserted: {result['metrics'].records_inserted}")

                # Log aggregated completion for date_partitioned only
                if config.import_pattern == "date_partitioned":
                    aggregated_load_id = str(uuid.uuid4())
                    self.logging_service.log_execution_completion(config, execution_id, aggregated_load_id, result["metrics"], "completed")

                # Log config execution completion
                self.logging_service.log_config_execution_completion(
                    config, execution_id,
                    result["files_discovered"],
                    result["files_processed"],
                    result.get("files_failed", 0),
                    result.get("files_skipped", 0),
                    result["metrics"],
                    "completed"
                )
            else:
                result["status"] = "no_data_processed"
                result["files_processed"] = 0
                config_logger.info(f"Processing completed in {time.time() - start_time:.2f} seconds (no data processed)")

                # Log config execution completion for no_data_processed case
                self.logging_service.log_config_execution_completion(
                    config, execution_id,
                    result["files_discovered"],
                    0,
                    result.get("files_failed", 0),
                    result.get("files_skipped", 0),
                    ProcessingMetrics(),
                    "no_data_processed"
                )

        except DuplicateFilesError as e:
            # Handle duplicate files error specifically
            result["status"] = "failed"
            result["errors"].append(str(e))
            error_details = ErrorHandlingUtils.format_error_details(e, {"config_id": config.config_id, "config_name": config.config_name})
            config_logger.error(f"Configuration failed due to duplicate files: {e}")

            # Log config execution error
            self.logging_service.log_config_execution_error(
                config, execution_id, str(e), str(error_details),
                result.get("files_discovered", 0), result.get("files_processed", 0), result.get("files_failed", 0)
            )

        except Exception as e:
            result["status"] = "failed"
            result["errors"].append(str(e))
            # Ensure file counts are set even in exception case
            if "files_discovered" not in result:
                result["files_discovered"] = 0
            if "files_processed" not in result:
                result["files_processed"] = 0
            error_details = ErrorHandlingUtils.format_error_details(e, {"config_id": config.config_id, "config_name": config.config_name})
            config_logger.error(f"Error processing: {e}")

            # Log config execution error
            self.logging_service.log_config_execution_error(
                config, execution_id, str(e), str(error_details),
                result.get("files_discovered", 0), result.get("files_processed", 0), result.get("files_failed", 0)
            )

        return result

    def process_configurations(
        self, configs: List[FlatFileIngestionConfig], execution_id: str
    ) -> Dict[str, Any]:
        """Process multiple configurations with parallel execution within groups"""
        from collections import defaultdict

        results = {
            "execution_id": execution_id,
            "total_configurations": len(configs),
            "successful": 0,
            "failed": 0,
            "no_data_found": 0,
            "configurations": [],
            "execution_groups_processed": [],
        }

        if not configs:
            return results

        # Group configurations by execution_group
        grouped_configs = defaultdict(list)
        for config in configs:
            grouped_configs[config.execution_group].append(config)

        # Process execution groups in ascending order
        execution_groups = sorted(grouped_configs.keys())
        logger.info(
            f"Processing {len(execution_groups)} execution groups: {execution_groups}"
        )

        for group_num in execution_groups:
            group_configs = grouped_configs[group_num]
            logger.info(f"Processing {len(group_configs)} configurations in parallel (Group {group_num})")

            # Process configurations in this group in parallel
            group_results = self._process_execution_group_parallel(
                group_configs, execution_id, group_num
            )

            # Aggregate results
            results["configurations"].extend(group_results)
            results["execution_groups_processed"].append(group_num)

            # Count statuses
            for config_result in group_results:
                if config_result["status"] == "completed":
                    results["successful"] += 1
                elif config_result["status"] == "failed":
                    results["failed"] += 1
                elif config_result["status"] in ["no_data_found", "no_data_processed"]:
                    results["no_data_found"] += 1

        return results

    def _process_execution_group_parallel(
        self, configs: List[FlatFileIngestionConfig], execution_id: str, group_num: int
    ) -> List[Dict[str, Any]]:
        """Process configurations within an execution group in parallel"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = []
        start_time = time.time()

        if len(configs) == 1:
            # Single configuration - no need for parallel processing
            logger.info(f"Processing single configuration: {configs[0].config_name}")
            result = self.process_configuration(configs[0], execution_id)
            results = [result]
        else:
            # Multiple configurations - process in parallel
            max_workers = min(len(configs), self.max_concurrency)
            logger.info(
                f"Using {max_workers} parallel workers for {len(configs)} configurations (max_concurrency={self.max_concurrency})"
            )

            with ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=f"FlatFileGroup{group_num}"
            ) as executor:
                # Submit all configurations for processing
                future_to_config = {
                    executor.submit(
                        self._process_configuration_with_thread_info, config, execution_id
                    ): config
                    for config in configs
                }

                # Collect results as they complete
                for future in as_completed(future_to_config):
                    config = future_to_config[future]
                    try:
                        result = future.result()
                        results.append(result)
                        logger.info(f"Completed: {config.config_name} ({result['status']})")
                    except Exception as e:
                        error_result = {
                            "config_id": config.config_id,
                            "config_name": config.config_name,
                            "status": "failed",
                            "metrics": ProcessingMetrics(),
                            "errors": [f"Thread execution error: {str(e)}"],
                        }
                        results.append(error_result)
                        logger.error(f"Failed: {config.config_name} - {str(e)}")

        # Generate summary
        end_time = time.time()
        total_duration = end_time - start_time

        # Count statuses
        completed = sum(1 for r in results if r['status'] == 'completed')
        failed = sum(1 for r in results if r['status'] == 'failed')
        no_data = sum(1 for r in results if r['status'] in ['no_data_found', 'no_data_processed'])

        logger.info("")
        logger.info(f"Execution group {group_num} completed in {total_duration:.1f}s")
        logger.info(f"Processed {len(results)} configuration{'s' if len(results) != 1 else ''} - {completed} completed, {failed} failed, {no_data} no data")
        logger.info("")

        for r in results:
            status = r['status']
            config_name = r['config_name']
            files_discovered = r.get('files_discovered', 0)
            files_processed = r.get('files_processed', 0)

            if status == 'completed':
                icon = '✓'
                rows = r['metrics'].records_processed if r.get('metrics') and r['metrics'].records_processed else 0
                duration_ms = r['metrics'].total_duration_ms if r.get('metrics') and r['metrics'].total_duration_ms else 0
                duration = f"{duration_ms / 1000:.1f}s" if duration_ms else "0.0s"

                # Format file count
                file_text = f"{files_processed} file{'s' if files_processed != 1 else ''}"

                logger.info(f"{icon} {config_name} ({status}) - {file_text}, {rows} rows in {duration}")
            elif status == 'failed':
                icon = '✗'
                error = r['errors'][0] if r.get('errors') and len(r['errors']) > 0 else "Unknown error"

                # Format file count for partial failure
                if files_discovered > 0:
                    file_text = f"{files_processed}/{files_discovered} file{'s' if files_discovered != 1 else ''}"
                    logger.info(f"{icon} {config_name} ({status}) - {file_text} - Error: {error}")
                else:
                    logger.info(f"{icon} {config_name} ({status}) - Error: {error}")
            else:  # no_data_found or no_data_processed
                icon = '○'

                # Format file count for no data cases
                if files_discovered > 0:
                    file_text = f"{files_discovered} file{'s' if files_discovered != 1 else ''} found"
                    logger.info(f"{icon} {config_name} ({status}) - {file_text}")
                else:
                    logger.info(f"{icon} {config_name} ({status})")

        return results

    def _process_configuration_with_thread_info(
        self, config: FlatFileIngestionConfig, execution_id: str
    ) -> Dict[str, Any]:
        """Process a single configuration with thread information"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_name,
            'execution_group': config.execution_group
        })

        config_logger.info(f"Starting processing")

        try:
            result = self.process_configuration(config, execution_id)
            config_logger.info(f"Finished processing ({result['status']})")
            return result
        except Exception as e:
            config_logger.error(f"Error processing: {str(e)}")
            raise
