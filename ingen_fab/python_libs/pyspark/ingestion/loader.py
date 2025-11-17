# File Loading Framework - FileLoader
# Manages all file operations for batch processing

import logging
import os
import re
import time
from dataclasses import fields
from datetime import datetime
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, concat_ws, when, expr, to_date, to_timestamp, trim
from pyspark.sql.types import StructField, StringType

from ingen_fab.python_libs.common.flat_file_ingestion_utils import ProcessingMetricsUtils
from ingen_fab.python_libs.pyspark.ingestion.config import MetadataColumns, ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import WriteMode, LoadType
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    ErrorContext,
    FileReadError,
    SchemaValidationError,
    WriteError,
)
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    BatchResult,
    ProcessingMetrics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)

class FileLoader:
    """
    Manages all file operations for batch processing.

    This class handles:
    - Reading batch files into DataFrames (CSV, JSON, Parquet, etc.)
    - Schema handling
    - Read metrics tracking

    NOTE: Discovery and logging are handled by LoadingOrchestrator and LoadingLogger.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ResourceConfig,
        metadata_columns: MetadataColumns,
    ):
        """
        Initialize FileLoader with configuration.

        Args:
            spark: Spark session for reading files
            config: ResourceConfig with file loading settings
            metadata_columns: Metadata column names
        """
        self.spark = spark
        self.config = config
        self.metadata_cols = metadata_columns

        # Create all lakehouse connections (fail fast if config invalid)
        self.source_lakehouse_utils = self._create_source_lakehouse_utils()
        self.stg_table_utils = self._create_stg_table_utils()
        self.target_table_utils = self._create_target_utils()

        # Create logger adapter with resource context
        self.logger = ConfigLoggerAdapter(logger, {
            'source_name': self.config.source_name,
            'config_name': self.config.resource_name,
        })

    def load_files_to_stg_table(self, batch_info: BatchInfo) -> BatchResult:
        """
        Read files and write to staging table with structural validation only (Step 1 of two-step loading).

        Strategy:
        - Use custom_schema_json from config (if provided) OR infer schema from file
        - Cast ALL columns to STRING (no type validation)
        - Use PERMISSIVE mode to capture structurally corrupt rows in _stg_corrupt_record
        - Add staging metadata columns (_stg_corrupt_record, _stg_file_path, _stg_created_load_id, _stg_created_at)
        - Add partition columns from batch destination path
        - Write to stg_table with partition alignment

        Args:
            batch_info: Batch info with file_paths and destination_path

        Returns:
            BatchResult with success/failed status and metrics
        """
        start_time = time.time()
        metrics = ProcessingMetrics()

        try:
            if not self.config.raw_file_format:
                raise ValueError("raw_file_format is required")

            # Get complete file read options (includes schema, corruption tracking, format params)
            options = self._get_file_read_options()

            # Read files from source
            df = self.source_lakehouse_utils.read_file(
                file_path=batch_info.file_paths[0],
                file_format=self.config.raw_file_format,
                options=options,
            )

            # Add staging metadata columns
            df = df.withColumn(self.metadata_cols.stg_created_load_id, lit(batch_info.batch_id))
            df = df.withColumn(self.metadata_cols.stg_file_path, lit(batch_info.file_paths[0] if batch_info.file_paths else "unknown"))
            df = df.withColumn(self.metadata_cols.stg_created_at, current_timestamp())

            # Parse and add partition columns from destination path
            partition_cols = []
            if batch_info.destination_path:
                partition_values = self._parse_partition_values(batch_info.destination_path)
                for col_name, col_value in partition_values.items():
                    df = df.withColumn(col_name, lit(col_value))
                    partition_cols.append(col_name)

            # Reorder columns: source columns, then metadata
            metadata_col_names = {getattr(self.metadata_cols, f.name) for f in fields(self.metadata_cols)}

            # Exclude metadata and partition columns to get business columns
            excluded_cols = metadata_col_names | set(partition_cols)
            business_cols = [c for c in df.columns if c not in excluded_cols]

            ordered_cols = (
                business_cols +
                [self.metadata_cols.stg_corrupt_record, self.metadata_cols.stg_file_path,
                 self.metadata_cols.stg_created_load_id, self.metadata_cols.stg_created_at] +
                partition_cols
            )
            df = df.select(ordered_cols)

            self.stg_table_utils.write_to_table(
                df=df,
                table_name=self.config.stg_table_name,
                mode=self.config.stg_table_write_mode,
                schema_name=self.config.stg_table_schema,
                partition_by=self.config.stg_table_partition_columns,
            )

            # Calculate metrics
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            return BatchResult(
                status="success",
                metrics=metrics,
            )

        except Exception as e:
            # Step 1 failures are system errors (file read, table write)
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)

            return BatchResult(
                status="failed",
                metrics=metrics,
                rejection_reason=f"Staging table load failed: {str(e)}",
            )

    def load_stg_table_to_target(self, batch_info: BatchInfo) -> BatchResult:
        """
        Read from staging table, apply validation, and write to target table (Step 2 of two-step loading).

        Strategy:
        - Read from stg_table filtered by partition + _stg_created_load_id
        - Apply schema validation (cast strings to proper types using try_cast)
        - Apply corrupt record checks
        - Apply duplicate checks on merge keys
        - Add target metadata columns (_raw_created_load_id, _raw_updated_load_id, timestamps)
        - Write to target table (merge/append/overwrite)

        Args:
            batch_info: Batch info with destination_path for partition filtering

        Returns:
            BatchResult with success/rejected status and combined metrics (read + write)
        """
        start_time = time.time()
        metrics = ProcessingMetrics()

        try:
            # Read from staging table using lakehouse_utils
            df_stg = self.stg_table_utils.read_table(
                table_name=self.config.stg_table_name,
                schema_name=self.config.stg_table_schema,
            )

            # Parse partition values from destination path
            if batch_info.destination_path:
                partition_values = self._parse_partition_values(batch_info.destination_path)

                # Apply partition filters (partition pruning)
                for col_name, col_value in partition_values.items():
                    df_stg = df_stg.filter(col(col_name) == col_value)

            # Filter to specific batch
            df_stg = df_stg.filter(col(self.metadata_cols.stg_created_load_id) == batch_info.batch_id)

            # Drop staging-specific columns (will add target metadata later)
            df_stg = df_stg.drop(self.metadata_cols.stg_created_load_id, self.metadata_cols.stg_file_path, self.metadata_cols.stg_created_at)

            # Drop partition columns (will be re-added in target if needed)
            for col_name in self.config.stg_table_partition_columns:
                if col_name in df_stg.columns:
                    df_stg = df_stg.drop(col_name)

            # Check corrupt record from Step 1 (structural corruption from CSV)
            corrupt_count = 0
            if self.metadata_cols.stg_corrupt_record in df_stg.columns:
                corrupt_count = df_stg.filter(col(self.metadata_cols.stg_corrupt_record).isNotNull()).count()

                if corrupt_count > self.config.target_max_corrupt_records:
                    if self.config.target_fail_on_rejection:
                        # Fail on rejection (current behavior)
                        end_time = time.time()
                        metrics.read_duration_ms = int((end_time - start_time) * 1000)
                        metrics.corrupt_records_count = corrupt_count

                        return BatchResult(
                            status="rejected",
                            rejection_reason=f"Structural corruption: {corrupt_count} corrupt rows exceeds tolerance ({self.config.target_max_corrupt_records})",
                            corrupt_count=corrupt_count,
                            metrics=metrics,
                        )
                    else:
                        # Skip rejection, filter corrupt records and continue
                        self.logger.warning(f"Structural corruption: {corrupt_count} corrupt rows exceeds tolerance ({self.config.target_max_corrupt_records}), but target_fail_on_rejection=False, filtering and continuing")

                # Filter out corrupt rows (if within tolerance OR if fail_on_rejection=False)
                if corrupt_count > 0:
                    if corrupt_count <= self.config.target_max_corrupt_records:
                        self.logger.warning(f"Found {corrupt_count} structurally corrupt rows, dropping them")
                    df_stg = df_stg.filter(col(self.metadata_cols.stg_corrupt_record).isNull())

                # Drop corrupt record column
                df_stg = df_stg.drop(self.metadata_cols.stg_corrupt_record)

            # Trim all string columns (improves merge key matching and removes source whitespace issues)
            for column in df_stg.columns:
                df_stg = df_stg.withColumn(column, trim(col(column)))

            # Apply schema validation with try_cast (identifies problematic rows)
            target_schema = self.config.target_schema_columns.to_target_schema()

            # Build casted columns using try_cast
            casted_columns = []
            for field in target_schema.fields:
                if field.name in df_stg.columns:
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
                if field.name in df_stg.columns:
                    # If try_cast returns null but original value is not null, cast failed
                    error_tracking_exprs.append(
                        when(
                            expr(f"try_cast({field.name} as {field.dataType.simpleString()})").isNull()
                            & col(field.name).isNotNull(),
                            lit(field.name),
                        )
                    )

            error_tracking_column = concat_ws(", ", *error_tracking_exprs).alias("_type_cast_error")

            # Select all casted columns + error tracking (staging columns already dropped)
            df_with_casts = df_stg.select(
                *casted_columns,
                error_tracking_column
            )

            # Count rows with cast errors
            cast_error_count = df_with_casts.filter(col("_type_cast_error") != "").count()

            # Check if cast errors exceed tolerance
            if cast_error_count > self.config.target_max_corrupt_records:
                if self.config.target_fail_on_rejection:
                    # Fail on rejection (current behavior)
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

                    return BatchResult(
                        status="rejected",
                        rejection_reason=f"Type casting errors: {cast_error_count} rows failed to cast (tolerance: {self.config.target_max_corrupt_records}). Failed columns: {', '.join(sorted(failed_columns))}",
                        corrupt_count=corrupt_count + cast_error_count,
                        metrics=metrics,
                    )
                else:
                    # Skip rejection, filter corrupt records and continue
                    self.logger.warning(f"Type casting errors: {cast_error_count} rows failed to cast (tolerance: {self.config.target_max_corrupt_records}), but target_fail_on_rejection=False, filtering and continuing")

            # Filter out rows with cast errors (if within tolerance)
            if cast_error_count > 0:
                self.logger.warning(
                    f"Found {cast_error_count} rows with type cast errors, filtering them out (within tolerance)"
                )
                df_typed = df_with_casts.filter(col("_type_cast_error") == "").drop("_type_cast_error")
            else:
                df_typed = df_with_casts.drop("_type_cast_error")

            # Remove full duplicates
            df_typed = df_typed.distinct()

            # Validate duplicates on merge keys
            duplicate_count = self._count_duplicates(df_typed)
            if duplicate_count > 0:
                if self.config.target_fail_on_rejection:
                    # Fail on rejection (current behavior)
                    end_time = time.time()
                    metrics.read_duration_ms = int((end_time - start_time) * 1000)
                    metrics.source_row_count = df_typed.count()

                    return BatchResult(
                        status="rejected",
                        rejection_reason=f"Found {duplicate_count} duplicate records on merge keys {self.config.target_merge_keys}",
                        metrics=metrics,
                    )
                else:
                    # Skip rejection, log warning and continue
                    self.logger.warning(f"Found {duplicate_count} duplicate records on merge keys {self.config.target_merge_keys}, but target_fail_on_rejection=False, continuing (duplicates will remain in data)")

            # Add target metadata columns to DataFrame
            df_typed = self._add_target_metadata(df_typed, batch_info)

            # Calculate read metrics
            end_time = time.time()
            metrics.read_duration_ms = int((end_time - start_time) * 1000)
            metrics.source_row_count = df_typed.count()
            metrics.records_processed = metrics.source_row_count
            metrics.corrupt_records_count = corrupt_count + cast_error_count

            # Write to target table
            write_metrics = self._write_to_target(df_typed, self.target_table_utils)

            # Combine read and write metrics
            combined_metrics = self._combine_metrics(metrics, write_metrics)

            return BatchResult(
                status="success",
                df=None,  # No longer returning DataFrame
                metrics=combined_metrics,
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
                raise SchemaValidationError(
                    message=f"Schema validation failed for {self.config.resource_name}",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        batch_id=batch_info.batch_id,
                        file_path=file_path,
                        operation="load_stg_table_to_target",
                        additional_info={
                            "file_format": self.config.raw_file_format,
                            "schema_error": str(e),
                        },
                    ),
                ) from e
            else:
                raise FileReadError(
                    message=f"Failed to read from staging table for batch {batch_info.batch_id}",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        batch_id=batch_info.batch_id,
                        file_path=file_path,
                        operation="load_stg_table_to_target",
                        additional_info={
                            "stg_table": f"{self.config.stg_table_schema}.{self.config.stg_table_name}",
                        },
                    ),
                ) from e

    # Private methods

    def _create_source_lakehouse_utils(self) -> lakehouse_utils:
        """Create lakehouse utils for reading source files"""
        workspace = self.config.source_config.source_connection_params.get("workspace_name")
        lakehouse = self.config.source_config.source_connection_params.get("lakehouse_name")

        if not workspace or not lakehouse:
            raise ValueError(
                "source_config.source_connection_params must have workspace_name and lakehouse_name"
            )

        return lakehouse_utils(
            target_workspace_name=workspace,
            target_lakehouse_name=lakehouse,
            spark=self.spark,
        )

    def _create_stg_table_utils(self) -> lakehouse_utils:
        """Create lakehouse utils for staging table operations"""
        if not self.config.stg_table_workspace or not self.config.stg_table_lakehouse:
            raise ValueError(
                "stg_table_workspace and stg_table_lakehouse must be configured"
            )

        return lakehouse_utils(
            target_workspace_name=self.config.stg_table_workspace,
            target_lakehouse_name=self.config.stg_table_lakehouse,
            spark=self.spark,
        )

    def _create_target_utils(self) -> lakehouse_utils:
        """Create lakehouse utils for target table operations"""
        if not self.config.target_workspace or not self.config.target_lakehouse:
            raise ValueError(
                "target_workspace and target_lakehouse must be configured"
            )

        return lakehouse_utils(
            target_workspace_name=self.config.target_workspace,
            target_lakehouse_name=self.config.target_lakehouse,
            spark=self.spark,
        )

    def _get_file_read_options(self) -> dict:
        """Build file reading options from configuration"""
        options = {}

        if not self.config.raw_file_format:
            return options

        if self.config.raw_file_format.lower() == "csv":
            # Get CSV format params from source_extraction_params (all source types)
            params = self.config.source_extraction_params

            options["has_header"] = params.get("has_header", True)
            options["file_delimiter"] = params.get("file_delimiter", ",")
            options["encoding"] = params.get("encoding", "utf-8")
            options["quote_character"] = params.get("quote_character", '"')
            options["escape_character"] = params.get("escape_character", "\\")
            options["multiline_values"] = params.get("multiline_values", True)
            options["null_value"] = params.get("null_value", "")

            schema = self.config.target_schema_columns.to_raw_schema()

            schema = schema.add(StructField(self.metadata_cols.stg_corrupt_record, StringType(), True))

            options["schema"] = schema

        # Enable corrupt record tracking (Spark PERMISSIVE mode - works for all formats)
        options["columnNameOfCorruptRecord"] = self.metadata_cols.stg_corrupt_record
        options["mode"] = "PERMISSIVE"

        return options

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

    def _add_filename_metadata_columns(self, df, file_path: str):
        """
        Extract metadata from file path and add as DataFrame columns.

        Uses regex patterns from config.source_extraction_params['filename_metadata'] to extract
        values from the file path and add them as typed columns (date, int, string, etc.).

        Example config:
        {
            "filename_metadata": [
                {"name": "file_date", "regex": r"(\\d{8})", "type": "date", "format": "yyyyMMdd"}
            ]
        }

        Args:
            df: Source DataFrame
            file_path: Full file path to extract metadata from

        Returns:
            DataFrame with added metadata columns
        """
        # Get metadata patterns from source_extraction_params
        if not self.config.source_extraction_params or not isinstance(self.config.source_extraction_params, dict):
            return df

        metadata_patterns = self.config.source_extraction_params.get("filename_metadata", [])
        if not metadata_patterns:
            return df

        # Extract each metadata field and add as column
        for pattern in metadata_patterns:
            field_name = pattern["name"]
            regex = pattern["regex"]
            field_type = pattern.get("type", "string")
            date_format = pattern.get("format", "yyyyMMdd")

            try:
                # Extract value using Python regex
                match = re.search(regex, file_path)

                if match:
                    groups = match.groups()
                    if groups:
                        # Single group or concatenate multiple groups
                        if len(groups) == 1:
                            value = groups[0]
                        else:
                            value = "".join(groups)

                        # Add column with appropriate type conversion
                        if field_type == "date":
                            df = df.withColumn(field_name, to_date(lit(value), date_format))
                        elif field_type == "timestamp":
                            df = df.withColumn(field_name, to_timestamp(lit(value), date_format))
                        elif field_type == "int":
                            df = df.withColumn(field_name, lit(int(value)))
                        elif field_type == "long":
                            df = df.withColumn(field_name, lit(int(value)).cast("long"))
                        elif field_type == "double":
                            df = df.withColumn(field_name, lit(float(value)))
                        elif field_type == "boolean":
                            df = df.withColumn(field_name, lit(value.lower() in ["true", "1", "yes"]))
                        else:  # string (default)
                            df = df.withColumn(field_name, lit(value))

                        self.logger.debug(f"Added metadata column: {field_name}='{value}' ({field_type})")
                    else:
                        # Regex matched but no capture groups - add NULL
                        df = df.withColumn(field_name, lit(None))
                else:
                    # Regex didn't match - add NULL
                    df = df.withColumn(field_name, lit(None))
                    self.logger.debug(f"Metadata field {field_name} not found in path: {file_path}")

            except Exception as e:
                # Error during extraction/conversion - add NULL and log warning
                df = df.withColumn(field_name, lit(None))
                self.logger.warning(f"Failed to extract metadata field {field_name}: {e}")

        return df

    def _add_target_metadata(self, df, batch_info: BatchInfo):
        """
        Add all target table metadata columns to DataFrame.

        Adds in order:
        1. Filename metadata columns (file_date, etc.) - business columns extracted from path
        2. _raw_filename - file tracking
        3. _raw_is_deleted (if soft_delete_enabled) - file tracking
        4. _raw_created_load_id (current batch_id - for inserts, immutable on updates) - load tracking
        5. _raw_updated_load_id (current batch_id - updated on every load) - load tracking
        6. _raw_created_at, _raw_updated_at (timestamps)

        Note: Staging columns (_stg_*) have already been dropped

        Args:
            df: Source DataFrame (staging columns already removed)
            batch_info: Batch info with file_paths and batch_id

        Returns:
            DataFrame with all target metadata columns
        """
        result_df = df

        # Add filename metadata columns FIRST (extracted from path - business columns like file_date)
        if batch_info.file_paths:
            file_path = batch_info.file_paths[0]
            result_df = self._add_filename_metadata_columns(result_df, file_path)

        # Add filename (file tracking)
        if batch_info.file_paths:
            filename = os.path.basename(batch_info.file_paths[0])
            result_df = result_df.withColumn(self.metadata_cols.raw_filename, lit(filename))

        # Add soft delete column (file tracking - if enabled)
        if self.config.target_soft_delete_enabled:
            result_df = result_df.withColumn(self.metadata_cols.raw_is_deleted, lit(False))

        # Add created load_id (load tracking - current batch, immutable on merge)
        result_df = result_df.withColumn(self.metadata_cols.raw_created_load_id, lit(batch_info.batch_id))

        # Add updated load_id (load tracking - current batch, updates on every load)
        result_df = result_df.withColumn(self.metadata_cols.raw_updated_load_id, lit(batch_info.batch_id))

        # Add timestamps (last)
        result_df = result_df.withColumn(self.metadata_cols.raw_created_at, current_timestamp()) \
                             .withColumn(self.metadata_cols.raw_updated_at, current_timestamp())

        return result_df

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

    def _write_to_target(
        self,
        df: DataFrame,
        target_utils,
    ) -> ProcessingMetrics:
        """
        Write DataFrame to target table using configured write mode.

        Delegates to specialized methods based on write mode:
        - Merge: _write_merge()
        - Overwrite/Append: _write_overwrite_or_append()

        Args:
            df: Source DataFrame to write
            target_utils: Lakehouse utilities

        Returns:
            ProcessingMetrics with write results and timing
        """
        write_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Delegate to specialized write method based on mode
            if self.config.target_write_mode.lower() == WriteMode.MERGE:
                metrics = self._write_merge(df, target_utils)
            else:
                metrics = self._write_overwrite_or_append(df, target_utils)

            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)

            return ProcessingMetricsUtils.calculate_performance_metrics(
                metrics, self.config.target_write_mode
            )

        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            self.logger.exception(f"Write failed: {e}")
            raise WriteError(
                message=f"Failed to write to target table {self.config.target_table}",
                context=ErrorContext(
                    resource_name=self.config.resource_name,
                    operation="write_to_target",
                    additional_info={
                        "table": f"{self.config.target_schema}.{self.config.target_table}",
                        "write_mode": self.config.target_write_mode,
                    },
                ),
            ) from e

    def _write_merge(
        self,
        df: DataFrame,
        target_utils,
    ) -> ProcessingMetrics:
        """
        Write DataFrame using merge (upsert) mode.

        Args:
            df: Source DataFrame to merge
            target_utils: Lakehouse utilities

        Returns:
            ProcessingMetrics with merge results
        """
        metrics = ProcessingMetrics()

        self.logger.info(f"Executing merge with keys: {self.config.target_merge_keys}")

        # Handle full load deletes (mark records missing from batch as deleted)
        if self.config.target_load_type == LoadType.FULL:
            df = self._handle_full_load_deletes(df, target_utils)

        merge_result = target_utils.merge_to_table(
            df=df,
            table_name=self.config.target_table,
            merge_keys=self.config.target_merge_keys,
            schema_name=self.config.target_schema,
            immutable_columns=[self.metadata_cols.raw_created_at, self.metadata_cols.raw_created_load_id],
            enable_schema_evolution=False,
            partition_by=self.config.target_partition_columns,
            soft_delete_enabled=self.config.target_soft_delete_enabled,
            cdc_config=self.config.target_cdc_config,
        )

        metrics.records_inserted = merge_result["records_inserted"]
        metrics.records_updated = merge_result["records_updated"]
        metrics.records_deleted = merge_result["records_deleted"]
        metrics.target_row_count_before = merge_result["target_row_count_before"]
        metrics.target_row_count_after = merge_result["target_row_count_after"]

        return metrics

    def _handle_full_load_deletes(
        self,
        df: DataFrame,
        target_utils,
    ) -> DataFrame:
        """
        Handle full load deletes by marking records missing from the batch as deleted.

        For full load type, the incoming batch represents a complete snapshot.
        Any records that exist in the target table but are NOT in the batch
        should be marked as deleted.

        Args:
            df: Incoming batch DataFrame
            target_utils: Lakehouse utilities

        Returns:
            DataFrame with deleted records unioned (if soft_delete_enabled)
            or original DataFrame if table doesn't exist or hard deletes are used
        """
        # Try to read existing target table
        try:
            target_df = target_utils.read_table(
                table_name=self.config.target_table,
                schema_name=self.config.target_schema,
            )
        except Exception as e:
            # Table doesn't exist yet - no records to mark as deleted
            self.logger.debug(f"Target table does not exist yet, skipping full load delete logic: {e}")
            return df

        # If soft delete is NOT enabled, we rely on the merge logic to hard delete
        # (the merge will only keep records from the incoming batch)
        if not self.config.target_soft_delete_enabled:
            self.logger.info("Full load mode with hard deletes - merge will remove records not in batch")
            return df

        # Soft delete enabled - find records to mark as deleted
        if not self.config.target_merge_keys:
            self.logger.warning("Full load mode requires merge_keys - skipping delete logic")
            return df

        # Find records in target that are NOT in incoming batch (anti-join)
        # Filter out already deleted records (don't need to delete them again)
        target_active_df = target_df.filter(col(self.metadata_cols.raw_is_deleted) == False)

        # Perform anti-join: records in target but NOT in batch
        records_to_delete = target_active_df.join(
            df.select(*self.config.target_merge_keys).distinct(),
            on=self.config.target_merge_keys,
            how="left_anti"
        )

        delete_count = records_to_delete.count()

        if delete_count == 0:
            self.logger.info("Full load mode: No records to mark as deleted")
            return df

        self.logger.info(f"Full load mode: Marking {delete_count} records as deleted")

        # Mark records as deleted
        records_to_delete = records_to_delete.withColumn(
            self.metadata_cols.raw_is_deleted, lit(True)
        ).withColumn(
            self.metadata_cols.raw_updated_load_id, lit(df.select(self.metadata_cols.raw_updated_load_id).first()[0])
        ).withColumn(
            self.metadata_cols.raw_updated_at, current_timestamp()
        )

        # Union deleted records with incoming batch
        # Note: Column order must match for union
        combined_df = df.unionByName(records_to_delete, allowMissingColumns=True)

        return combined_df

    def _write_overwrite_or_append(
        self,
        df: DataFrame,
        target_utils,
    ) -> ProcessingMetrics:
        """
        Write DataFrame using overwrite or append mode.

        Args:
            df: Source DataFrame to write
            target_utils: Lakehouse utilities

        Returns:
            ProcessingMetrics with write results
        """
        metrics = ProcessingMetrics()

        # Get before count
        metrics.target_row_count_before = self._get_table_row_count(
            target_utils, self.config.target_table, self.config.target_schema
        )
        self.logger.debug(f"Target table row count before write: {metrics.target_row_count_before}")

        # Write data (schema evolution disabled)
        target_utils.write_to_table(
            df=df,
            table_name=self.config.target_table,
            schema_name=self.config.target_schema,
            mode=self.config.target_write_mode,
            partition_by=self.config.target_partition_columns,
        )

        # Get after count
        metrics.target_row_count_after = self._get_table_row_count(
            target_utils, self.config.target_table, self.config.target_schema
        )
        self.logger.debug(f"Target table row count after write: {metrics.target_row_count_after}")

        # Calculate inserted/deleted
        if self.config.target_write_mode == WriteMode.OVERWRITE:
            metrics.records_inserted = metrics.target_row_count_after
            metrics.records_deleted = metrics.target_row_count_before
        elif self.config.target_write_mode == WriteMode.APPEND:
            metrics.records_inserted = (
                metrics.target_row_count_after - metrics.target_row_count_before
            )

        return metrics

    def _get_table_row_count(
        self,
        target_utils,
        table_name: str,
        schema_name: Optional[str],
    ) -> int:
        """
        Get row count for a table, returning 0 if table doesn't exist.

        Args:
            target_utils: Lakehouse utilities
            table_name: Name of the table
            schema_name: Optional schema name

        Returns:
            Row count (0 if table doesn't exist)
        """
        try:
            table_df = target_utils.read_table(table_name, schema_name=schema_name)
            return table_df.count()
        except Exception as e:
            self.logger.debug(f"Table does not exist or cannot be read: {e}")
            return 0

    def _combine_metrics(
        self,
        read_metrics: ProcessingMetrics,
        write_metrics: ProcessingMetrics,
    ) -> ProcessingMetrics:
        """
        Combine read and write metrics into a single ProcessingMetrics object.

        Args:
            read_metrics: Metrics from file reading
            write_metrics: Metrics from writing to target

        Returns:
            Combined ProcessingMetrics with completion timestamp
        """
        return ProcessingMetrics(
            read_duration_ms=read_metrics.read_duration_ms,
            write_duration_ms=write_metrics.write_duration_ms,
            total_duration_ms=read_metrics.read_duration_ms + write_metrics.write_duration_ms,
            records_processed=read_metrics.records_processed,
            records_inserted=write_metrics.records_inserted,
            records_updated=write_metrics.records_updated,
            records_deleted=write_metrics.records_deleted,
            source_row_count=read_metrics.source_row_count,
            target_row_count_before=write_metrics.target_row_count_before,
            target_row_count_after=write_metrics.target_row_count_after,
            completed_at=datetime.now(),
        )
