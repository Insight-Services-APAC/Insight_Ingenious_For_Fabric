# File Loading Framework - FileLoader
# Manages all file operations for batch processing

import json
import logging
import time
from typing import Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, concat_ws, when, expr
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
        if not self.config.target_merge_keys:
            return 0

        return (
            df.groupBy(*self.config.target_merge_keys)
            .count()
            .filter(col("count") > 1)
            .count()
        )

    def _parse_partition_values(self, path: str) -> Dict[str, str]:
        """
        Parse Hive partition values from path.

        Extracts key=value pairs from paths like:
        - "abfss://.../raw/pe/buyername/ds=2025-11-14/file.csv" → {"ds": "2025-11-14"}
        - "abfss://.../year=2025/month=11/day=14/file.parquet" → {"year": "2025", "month": "11", "day": "14"}

        Args:
            path: Full ABFSS path with Hive partitions

        Returns:
            Dict mapping partition column names to values
        """
        import re

        partition_values = {}

        # Extract all key=value pairs from path
        # Pattern: word characters followed by = followed by word/digit/dash characters
        pattern = r'(\w+)=([\w\-]+)'
        matches = re.findall(pattern, path)

        for col_name, col_value in matches:
            partition_values[col_name] = col_value

        return partition_values

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
                    rejection_reason=f"Found {duplicate_count} duplicate record(s) on merge keys {self.config.target_merge_keys}",
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

    def load_files_to_raw_table(self, batch_info: BatchInfo) -> BatchReadResult:
        """
        Load files to raw table with structural validation only (Step 1 of two-step loading).

        Strategy:
        - Infer schema to get column names
        - Cast ALL columns to STRING (no type validation)
        - Use PERMISSIVE mode to capture structurally corrupt rows in _raw_corrupt_record
        - Add raw metadata columns (_raw_load_id, _raw_file_path, _raw_loaded_at, _raw_corrupt_record)
        - Add partition columns from batch destination path
        - Write to raw_table with partition alignment

        Args:
            batch_info: Batch info with file_paths and destination_path

        Returns:
            BatchReadResult with success/failed status (no rejection possible in Step 1)
        """
        start_time = time.time()
        metrics = ProcessingMetrics()

        # Save original ANSI setting and disable for PERMISSIVE mode
        original_ansi = self.spark.conf.get("spark.sql.ansi.enabled", "false")
        self.spark.conf.set("spark.sql.ansi.enabled", "false")

        try:
            if not self.config.file_format:
                raise ValueError("file_format is required")

            file_format = self.config.file_format

            # Get file reading options (delimiter, encoding, etc.) but with inference
            options = self._get_file_read_options()
            # Remove schema if provided (we need to infer first)
            options.pop("schema", None)
            options["inferSchema"] = True

            # Read and infer schema
            df_inferred = self._read_dataframe(batch_info, file_format, options)
            inferred_schema = df_inferred.schema

            # Build all-string schema WITH _raw_corrupt_record column
            string_schema = StructType([
                StructField(field.name, StringType(), True)
                for field in inferred_schema.fields
                if field.name != "_raw_corrupt_record"  # Exclude if present from inference
            ])
            # Add _raw_corrupt_record column for structural corruption tracking
            string_schema = string_schema.add(StructField("_raw_corrupt_record", StringType(), True))

            # Re-read with string schema + corruption tracking
            options["schema"] = string_schema
            options["columnNameOfCorruptRecord"] = "_raw_corrupt_record"
            options["mode"] = "PERMISSIVE"
            options.pop("inferSchema", None)

            df = self._read_dataframe(batch_info, file_format, options)

            # Add raw metadata columns
            df = df.withColumn("_raw_load_id", lit(batch_info.batch_id))
            df = df.withColumn("_raw_file_path", lit(batch_info.file_paths[0] if batch_info.file_paths else "unknown"))
            df = df.withColumn("_raw_loaded_at", current_timestamp())

            # Parse and add partition columns from destination path
            partition_cols = []
            if batch_info.destination_path:
                partition_values = self._parse_partition_values(batch_info.destination_path)
                for col_name, col_value in partition_values.items():
                    df = df.withColumn(col_name, lit(col_value))
                    partition_cols.append(col_name)

            # Reorder columns: business columns, then metadata in specific order
            business_cols = [c for c in df.columns if not c.startswith("_raw") and c not in partition_cols]
            ordered_cols = (
                business_cols +
                ["_raw_load_id", "_raw_file_path", "_raw_corrupt_record", "_raw_loaded_at"] +
                partition_cols
            )
            df = df.select(ordered_cols)

            # Get raw table connection
            raw_table_utils = lakehouse_utils(
                target_workspace_name=self.config.raw_table_workspace,
                target_lakehouse_name=self.config.raw_table_lakehouse,
                spark=self.spark,
            )

            # Write to raw table using lakehouse_utils
            raw_table_utils.write_to_table(
                df=df,
                table_name=self.config.raw_table_name,
                mode=self.config.raw_table_write_mode,
                schema_name=self.config.raw_table_schema,
                partition_by=self.config.raw_table_partition_columns,
            )

            # Calculate metrics
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            return BatchReadResult(
                status="success",
                metrics=metrics,
            )

        except Exception as e:
            # Step 1 failures are system errors (file read, table write)
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)

            self.logger.exception(f"Failed to load files to raw table: {e}")

            return BatchReadResult(
                status="failed",
                metrics=metrics,
                rejection_reason=f"Raw table load failed: {str(e)}",
            )

        finally:
            # Restore original ANSI setting
            self.spark.conf.set("spark.sql.ansi.enabled", original_ansi)

    def load_raw_table_to_target(self, batch_info: BatchInfo) -> BatchReadResult:
        """
        Load batch from raw table and apply validation (Step 2 of two-step loading).

        Strategy:
        - Read from raw_table filtered by partition + _raw_load_id
        - Apply schema validation (cast strings to proper types)
        - Apply corrupt record checks
        - Apply duplicate checks on merge keys
        - Return validated DataFrame for target write

        Args:
            batch_info: Batch info with destination_path for partition filtering

        Returns:
            BatchReadResult with validated DataFrame (same as current read_batch)
        """
        start_time = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get raw table connection
            raw_table_utils = lakehouse_utils(
                target_workspace_name=self.config.raw_table_workspace,
                target_lakehouse_name=self.config.raw_table_lakehouse,
                spark=self.spark,
            )

            # Read from raw table using lakehouse_utils
            df_raw = raw_table_utils.read_table(
                table_name=self.config.raw_table_name,
                schema_name=self.config.raw_table_schema,
            )

            # Parse partition values from destination path
            if batch_info.destination_path:
                partition_values = self._parse_partition_values(batch_info.destination_path)

                # Apply partition filters (partition pruning)
                for col_name, col_value in partition_values.items():
                    df_raw = df_raw.filter(col(col_name) == col_value)

            # Filter to specific batch
            df_raw = df_raw.filter(col("_raw_load_id") == batch_info.batch_id)

            # Drop raw metadata columns (not needed in target)
            df_raw = df_raw.drop("_raw_load_id", "_raw_file_path", "_raw_loaded_at")

            # Drop partition columns (will be re-added in target if needed)
            for col_name in self.config.raw_table_partition_columns:
                if col_name in df_raw.columns:
                    df_raw = df_raw.drop(col_name)

            # Check _raw_corrupt_record from Step 1 (structural corruption from CSV)
            corrupt_count = 0
            if "_raw_corrupt_record" in df_raw.columns:
                corrupt_count = df_raw.filter(col("_raw_corrupt_record").isNotNull()).count()

                if corrupt_count > self.config.corrupt_record_tolerance:
                    end_time = time.time()
                    metrics.read_duration_ms = int((end_time - start_time) * 1000)
                    metrics.corrupt_records_count = corrupt_count

                    return BatchReadResult(
                        status="rejected",
                        rejection_reason=f"Structural corruption: {corrupt_count} corrupt rows exceeds tolerance ({self.config.corrupt_record_tolerance})",
                        corrupt_count=corrupt_count,
                        metrics=metrics,
                    )

                # Filter out corrupt rows (if within tolerance)
                if corrupt_count > 0:
                    self.logger.warning(f"Found {corrupt_count} structurally corrupt rows, dropping them")
                    df_raw = df_raw.filter(col("_raw_corrupt_record").isNull())

                # Drop _raw_corrupt_record column
                df_raw = df_raw.drop("_raw_corrupt_record")

            # Apply schema validation with try_cast (identifies problematic rows)
            if self.config.custom_schema_json:
                schema_dict = json.loads(self.config.custom_schema_json)
                target_schema = StructType.fromJson(schema_dict)

                # Build casted columns using try_cast (returns null on failure)
                casted_columns = []
                for field in target_schema.fields:
                    if field.name in df_raw.columns:
                        # Use try_cast - returns null on cast failure instead of throwing
                        casted_columns.append(
                            expr(f"try_cast({field.name} as {field.dataType.simpleString()})").alias(field.name)
                        )
                    else:
                        # Add missing column as null
                        casted_columns.append(lit(None).cast(field.dataType).alias(field.name))

                # Build error tracking column - concatenates names of columns that failed to cast
                error_tracking_exprs = []
                for field in target_schema.fields:
                    if field.name in df_raw.columns:
                        # If try_cast returns null but original value is not null, cast failed
                        error_tracking_exprs.append(
                            when(
                                expr(f"try_cast({field.name} as {field.dataType.simpleString()})").isNull()
                                & col(field.name).isNotNull(),
                                lit(field.name),
                            )
                        )

                error_tracking_column = concat_ws(", ", *error_tracking_exprs).alias("_type_cast_error")

                # Select all casted columns + error tracking
                df_with_casts = df_raw.select(*casted_columns, error_tracking_column)

                # Count rows with cast errors
                cast_error_count = df_with_casts.filter(col("_type_cast_error") != "").count()

                # Check if cast errors exceed tolerance
                if cast_error_count > self.config.corrupt_record_tolerance:
                    end_time = time.time()
                    metrics.read_duration_ms = int((end_time - start_time) * 1000)
                    metrics.corrupt_records_count = corrupt_count + cast_error_count

                    # Get sample of failed columns for error message
                    error_samples = (
                        df_with_casts.filter(col("_type_cast_error") != "")
                        .select("_type_cast_error")
                        .limit(5)
                        .collect()
                    )
                    failed_columns = set()
                    for row in error_samples:
                        for col_name in row._type_cast_error.split(", "):
                            if col_name:
                                failed_columns.add(col_name)

                    return BatchReadResult(
                        status="rejected",
                        rejection_reason=f"Type casting errors: {cast_error_count} rows failed to cast (tolerance: {self.config.corrupt_record_tolerance}). Failed columns: {', '.join(sorted(failed_columns))}",
                        corrupt_count=corrupt_count + cast_error_count,
                        metrics=metrics,
                    )

                # Filter out rows with cast errors (if within tolerance)
                if cast_error_count > 0:
                    self.logger.warning(
                        f"Found {cast_error_count} rows with type cast errors, filtering them out (within tolerance)"
                    )
                    df_typed = df_with_casts.filter(col("_type_cast_error") == "").drop("_type_cast_error")
                else:
                    df_typed = df_with_casts.drop("_type_cast_error")

            else:
                # No schema validation - use strings as-is
                df_typed = df_raw
                cast_error_count = 0

            # Remove full duplicates
            df_typed = df_typed.distinct()

            # Validate duplicates on merge keys
            duplicate_count = self._count_duplicates(df_typed)
            if duplicate_count > 0:
                end_time = time.time()
                metrics.read_duration_ms = int((end_time - start_time) * 1000)
                metrics.source_row_count = df_typed.count()

                return BatchReadResult(
                    status="rejected",
                    rejection_reason=f"Found {duplicate_count} duplicate records on merge keys {self.config.target_merge_keys}",
                    metrics=metrics,
                )

            # Calculate metrics
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)
            metrics.source_row_count = df_typed.count()
            metrics.records_processed = metrics.source_row_count
            metrics.corrupt_records_count = corrupt_count + cast_error_count

            return BatchReadResult(
                status="success",
                df=df_typed,
                metrics=metrics,
                corrupt_count=corrupt_count + cast_error_count,
            )

        except Exception as e:
            # Schema validation or read errors
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)

            # Get file path for context
            file_path = batch_info.file_paths[0] if batch_info.file_paths else "unknown"

            # Check if schema error
            error_str = str(e).lower()
            is_schema_error = (
                "schema" in error_str
                or "decimal scale" in error_str
                or "decimal precision" in error_str
                or "parsedatatype" in error_str
                or ("cannot be greater than precision" in error_str)
            )

            if is_schema_error:
                self.logger.error(f"Schema validation failed: {e}")

                raise SchemaValidationError(
                    message=f"Schema validation failed for {self.config.resource_name}",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        batch_id=batch_info.batch_id,
                        file_path=file_path,
                        operation="load_raw_table_to_target",
                        additional_info={
                            "file_format": self.config.file_format,
                            "schema_error": str(e),
                        },
                    ),
                ) from e
            else:
                self.logger.exception(f"Failed to read from raw table: {e}")

                raise FileReadError(
                    message=f"Failed to read from raw table for batch {batch_info.batch_id}",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        batch_id=batch_info.batch_id,
                        file_path=file_path,
                        operation="load_raw_table_to_target",
                        additional_info={
                            "raw_table": f"{self.config.raw_table_schema}.{self.config.raw_table_name}",
                        },
                    ),
                ) from e

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
