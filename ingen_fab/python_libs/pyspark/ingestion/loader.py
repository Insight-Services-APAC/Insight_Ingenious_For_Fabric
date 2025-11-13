# File Loading Framework - FileLoader
# Manages all file operations for batch processing

import json
import logging
import time
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    ErrorContext,
    FileReadError,
    SchemaValidationError,
)
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    BatchReadResult,
    ProcessingMetrics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# Configure logging
logger = logging.getLogger(__name__)


class FileLoader:
    """
    Manages all file operations for batch processing.

    This class handles:
    - Reading batch files into DataFrames (CSV, JSON, Parquet, etc.)
    - Schema handling (custom or inferred)
    - File quarantine operations
    - Read metrics tracking

    NOTE: Discovery and logging are handled by LoadingOrchestrator and LoadingLogger.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ResourceConfig,
        lakehouse_utils_instance: Optional[lakehouse_utils] = None,
    ):
        """
        Initialize FileLoader with configuration.

        Args:
            spark: Spark session for reading files
            config: ResourceConfig with file loading settings
            lakehouse_utils_instance: Optional pre-configured lakehouse_utils instance
                                     (if not provided, will create from config)
        """
        self.spark = spark
        self.config = config

        # Get or create lakehouse_utils for file operations
        if lakehouse_utils_instance:
            self.lakehouse = lakehouse_utils_instance
        else:
            # Create lakehouse_utils from config
            source_workspace = config.source_config.connection_params.get("workspace_name")
            source_lakehouse = config.source_config.connection_params.get("lakehouse_name")

            if not source_workspace or not source_lakehouse:
                raise ValueError(
                    "source_config.connection_params must have workspace_name and lakehouse_name"
                )

            self.lakehouse = lakehouse_utils(
                target_workspace_name=source_workspace,
                target_lakehouse_name=source_lakehouse,
                spark=spark,
            )

        # Configure logging if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s - %(message)s',
                force=True
            )

        # Create logger adapter with resource context
        self.logger = ConfigLoggerAdapter(logger, {
            'source_name': self.config.source_name,
            'config_name': self.config.resource_name,
        })

    def _count_corrupt_records(self, df) -> int:
        """Count corrupt records in DataFrame"""
        return df.filter(col("_corrupt_record").isNotNull()).count()

    def _count_duplicates(self, df) -> int:
        """Count duplicate records on merge keys"""
        if not self.config.merge_keys:
            return 0

        return (
            df.groupBy(*self.config.merge_keys)
            .count()
            .filter(col("count") > 1)
            .count()
        )

    def _read_dataframe(self, batch_info: BatchInfo, file_format: str, options: dict):
        """Read batch files into DataFrame and cache for validation"""
        if len(batch_info.file_paths) == 1:
            # Single file or folder
            df = self.lakehouse.read_file(
                file_path=batch_info.file_paths[0],
                file_format=file_format,
                options=options,
            )
        else:
            # Multiple files - use Spark batch read
            reader = self.spark.read.format(file_format.lower())

            if "schema" in options:
                reader = reader.schema(options["schema"])

            # Apply corrupt record tracking options (must be applied for all formats)
            if "columnNameOfCorruptRecord" in options:
                reader = reader.option("columnNameOfCorruptRecord", options["columnNameOfCorruptRecord"])
            if "mode" in options:
                reader = reader.option("mode", options["mode"])

            # Apply CSV options - translate human-readable names to Spark option names
            if file_format.lower() == "csv":
                if "has_header" in options:
                    reader = reader.option("header", str(options["has_header"]).lower())
                if "file_delimiter" in options:
                    reader = reader.option("sep", options["file_delimiter"])
                if "encoding" in options:
                    reader = reader.option("encoding", options["encoding"])
                if "quote_character" in options:
                    reader = reader.option("quote", options["quote_character"])
                if "escape_character" in options:
                    reader = reader.option("escape", options["escape_character"])
                if "multiline_values" in options:
                    reader = reader.option("multiLine", str(options["multiline_values"]).lower())
                if "inferSchema" in options:
                    reader = reader.option("inferSchema", str(options["inferSchema"]).lower())

            df = reader.load(batch_info.file_paths)

        # Cache DataFrame before querying _corrupt_record (required by Spark 2.3+)
        df.cache()
        return df

    def read_batch(self, batch_info: BatchInfo) -> BatchReadResult:
        """
        Read batch files into DataFrame with quality checks.

        Args:
            batch_info: BatchInfo describing what to read

        Returns:
            BatchReadResult with status ("success" | "rejected"), df, metrics, and optional rejection reason
        """
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Validate file_format
            if not self.config.file_format:
                raise ValueError("file_format is required")

            file_format = self.config.file_format

            # Get file reading options and read DataFrame
            options = self._get_file_read_options()
            df = self._read_dataframe(batch_info, file_format, options)

            # Validate corrupt records
            corrupt_count = self._count_corrupt_records(df)
            if corrupt_count > self.config.corrupt_record_tolerance:
                # Exceed tolerance - reject batch
                read_end = time.time()
                metrics.read_duration_ms = int((read_end - read_start) * 1000)
                metrics.corrupt_records_count = corrupt_count
                df.unpersist()

                return BatchReadResult(
                    status="rejected",
                    rejection_reason=f"Corrupt record count ({corrupt_count}) exceeds tolerance ({self.config.corrupt_record_tolerance})",
                    corrupt_count=corrupt_count,
                    metrics=metrics,
                )

            # Clean corrupt records (if within tolerance)
            if corrupt_count > 0:
                self.logger.warning(
                    f"Found {corrupt_count} corrupt record(s) (within tolerance {self.config.corrupt_record_tolerance}), dropping them"
                )
                df = df.filter(col("_corrupt_record").isNull())

            # Drop the corrupt record column (not needed in target)
            if "_corrupt_record" in df.columns:
                df = df.drop("_corrupt_record")

            # Remove full duplicate rows
            df = df.distinct()

            # Validate duplicates on merge keys
            duplicate_count = self._count_duplicates(df)
            if duplicate_count > 0:
                # Reject - no tolerance for duplicates
                read_end = time.time()
                metrics.read_duration_ms = int((read_end - read_start) * 1000)
                metrics.source_row_count = df.count()
                df.unpersist()

                return BatchReadResult(
                    status="rejected",
                    rejection_reason=f"Found {duplicate_count} duplicate record(s) on merge keys {self.config.merge_keys}",
                    metrics=metrics,
                )

            # Track validation metrics
            metrics.corrupt_records_count = corrupt_count

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            # Unpersist cached DataFrame before returning (free memory)
            df.unpersist()

            return BatchReadResult(
                status="success",
                df=df,
                metrics=metrics,
                corrupt_count=corrupt_count,
            )

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)

            # Get file path for context
            file_path = batch_info.file_paths[0] if batch_info.file_paths else "unknown"

            # Check if this is a schema validation error (user config issue)
            error_str = str(e).lower()
            is_schema_error = (
                "schema" in error_str
                or "decimal scale" in error_str
                or "decimal precision" in error_str
                or "parsedatatype" in error_str
                or ("cannot be greater than precision" in error_str)
            )

            if is_schema_error:
                # Schema errors are user config issues - log cleanly without stack trace
                self.logger.error(f"Schema validation failed: {e}")

                raise SchemaValidationError(
                    message=f"Schema validation failed for {self.config.resource_name}",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        batch_id=batch_info.batch_id,
                        file_path=file_path,
                        operation="read_batch",
                        additional_info={
                            "file_format": self.config.file_format,
                            "schema_error": str(e),
                        },
                    ),
                ) from e
            else:
                # System errors - log with full stack trace
                self.logger.exception(f"Failed to read batch: {e}")

                raise FileReadError(
                    message=f"Failed to read batch {batch_info.batch_id}",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        batch_id=batch_info.batch_id,
                        file_path=file_path,
                        operation="read_batch",
                        additional_info={
                            "file_format": self.config.file_format,
                            "import_mode": self.config.import_mode,
                        },
                    ),
                ) from e

    def quarantine_batch(self, batch_info: BatchInfo, error_message: str) -> None:
        """
        Move failed batch files to quarantine directory.

        This is a file management operation (not logging). Call this after logging
        the batch failure via LoadingLogger.log_batch_error().

        Args:
            batch_info: Batch that failed
            error_message: Error message describing why the batch failed
        """

        # Move files from landing to quarantine
        landing_base = self.config.raw_landing_path.rstrip('/')
        first_file_relative_path = None

        for file_path in batch_info.file_paths:
            # Extract relative path from landing
            if file_path.startswith(landing_base):
                relative_path = file_path[len(landing_base):].lstrip('/')
            else:
                # File path might be full URI, extract after landing folder name
                landing_folder = landing_base.split('/')[-1]
                if landing_folder in file_path:
                    relative_path = file_path.split(landing_folder + '/', 1)[1]
                else:
                    relative_path = file_path.split('/')[-1]

            # Track first file's relative path for cleanup
            if first_file_relative_path is None:
                first_file_relative_path = f"{landing_base}/{relative_path}"

            # Build quarantine path
            quarantine_path = f"{self.config.raw_quarantined_path.rstrip('/')}/{relative_path}"

            # Move file
            success = self.lakehouse.move_file(file_path, quarantine_path)
            if success:
                logger.info(f"Moved file to quarantine: {relative_path}")

                # Save error metadata as JSON
                error_metadata = {
                    "batch_id": batch_info.batch_id,
                    "error_message": error_message,
                    "timestamp": datetime.now().isoformat(),
                    "config_id": self.config.resource_name,
                }

                metadata_path = f"{quarantine_path}.error.json"
                metadata_json = json.dumps(error_metadata, indent=2)

                # Write error metadata (would need to implement this via spark)
                # For now, just log it at debug level
                logger.debug(f"Error metadata: {metadata_json}")
            else:
                logger.warning(f"Failed to move file to quarantine: {file_path}")

        # Clean up empty directories in landing ONCE after all files moved
        if first_file_relative_path:
            self.lakehouse.cleanup_empty_directories_recursive(
                first_file_relative_path,
                self.config.raw_landing_path
            )

        logger.info(f"Quarantined batch {batch_info.batch_id}: {error_message}")

    def _get_file_read_options(self) -> dict:
        """Build file reading options from configuration"""
        options = {}

        if not self.config.file_format:
            return options

        if self.config.file_format.lower() == "csv":
            # For filesystem sources, get CSV params from extraction_params
            # For other sources (already in raw), they may be in loading_params
            if self.config.source_config.source_type == "filesystem":
                params = self.config.extraction_params
            else:
                params = self.config.loading_params

            # Pass through human-readable names (translation to Spark names happens in lakehouse_utils)
            options["has_header"] = params.get("has_header", True)
            options["file_delimiter"] = params.get("file_delimiter", ",")
            options["encoding"] = params.get("encoding", "utf-8")
            options["quote_character"] = params.get("quote_character", '"')
            options["escape_character"] = params.get("escape_character", "\\")
            options["multiline_values"] = params.get("multiline_values", True)
            options["null_value"] = params.get("null_value", "")

            # Schema handling
            if self.config.custom_schema_json:
                schema_dict = json.loads(self.config.custom_schema_json)
                schema = StructType.fromJson(schema_dict)

                # Append corrupt record column if not already present
                # (required when using columnNameOfCorruptRecord with explicit schema)
                if "_corrupt_record" not in [field.name for field in schema.fields]:
                    schema = schema.add(StructField("_corrupt_record", StringType(), True))

                options["schema"] = schema
            else:
                options["inferSchema"] = True

        # Enable corrupt record tracking (Spark PERMISSIVE mode - works for all formats)
        options["columnNameOfCorruptRecord"] = "_corrupt_record"
        options["mode"] = "PERMISSIVE"

        return options
