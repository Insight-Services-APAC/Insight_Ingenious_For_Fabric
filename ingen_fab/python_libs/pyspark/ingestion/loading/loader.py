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
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    expr,
    lit,
    to_date,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType

from ingen_fab.python_libs.pyspark.ingestion.common.metrics_utils import (
    ProcessingMetricsUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    MetadataColumns,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.common.constants import LoadType, WriteMode
from ingen_fab.python_libs.pyspark.ingestion.common.exceptions import (
    ErrorContext,
    FileReadError,
    SchemaValidationError,
    WriteError,
)
from ingen_fab.python_libs.pyspark.ingestion.common.results import (
    BatchInfo,
    BatchResult,
    ProcessingMetrics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)

# Snake_case → Spark camelCase option translations
# Users configure with snake_case, we translate to Spark's expected names
OPTION_TRANSLATIONS = {
    # Common options (work across multiple formats)
    "multi_line": "multiLine",
    "null_value": "nullValue",
    "date_format": "dateFormat",
    "timestamp_format": "timestampFormat",
    "infer_schema": "inferSchema",
    "column_name_of_corrupt_record": "columnNameOfCorruptRecord",

    # CSV options
    "has_header": "header",
    "file_delimiter": "sep",
    "quote_character": "quote",
    "escape_character": "escape",
    "empty_value": "emptyValue",
    "enforce_schema": "enforceSchema",
    "ignore_leading_white_space": "ignoreLeadingWhiteSpace",
    "ignore_trailing_white_space": "ignoreTrailingWhiteSpace",
    "max_columns": "maxColumns",
    "max_chars_per_column": "maxCharsPerColumn",
    "unescaped_quote_handling": "unescapedQuoteHandling",

    # JSON options
    "primitives_as_string": "primitivesAsString",
    "allow_comments": "allowComments",
    "allow_unquoted_field_names": "allowUnquotedFieldNames",
    "allow_single_quotes": "allowSingleQuotes",
    "allow_numeric_leading_zero": "allowNumericLeadingZero",
    "allow_backslash_escaping_any_character": "allowBackslashEscapingAnyCharacter",
    "line_sep": "lineSep",
    "drop_field_if_all_null": "dropFieldIfAllNull",

    # XML options
    "row_tag": "rowTag",
    "root_tag": "rootTag",
    "attribute_prefix": "attributePrefix",
    "value_tag": "valueTag",
    "ignore_surrounding_spaces": "ignoreSurroundingSpaces",
    "exclude_attribute": "excludeAttribute",
}


def _translate_options(options: dict) -> dict:
    """Translate snake_case options to Spark camelCase names."""
    return {OPTION_TRANSLATIONS.get(k, k): v for k, v in options.items()}


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

        # Use module logger (context added via filter)
        self.logger = logger

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
            if not self.config.extract_file_format_params.file_format:
                raise ValueError("file_format is required")

            # Get base file read options (excludes schema)
            options = self._get_file_read_options()
            file_format = self.config.extract_file_format_params.file_format.lower()

            # Resolve schema (handles explicit, table-based, or inferred)
            schema = self._resolve_schema(file_format, batch_info.file_paths)
            if schema:
                options["schema"] = schema

            # Read files from source
            df = self.source_lakehouse_utils.read_file(
                file_path=batch_info.file_paths,
                file_format=file_format,
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

            # Build metadata columns list (only include corrupt_record if it exists)
            metadata_cols_list = []
            if self.metadata_cols.stg_corrupt_record in df.columns:
                metadata_cols_list.append(self.metadata_cols.stg_corrupt_record)
            metadata_cols_list.extend([
                self.metadata_cols.stg_file_path,
                self.metadata_cols.stg_created_load_id,
                self.metadata_cols.stg_created_at
            ])

            ordered_cols = business_cols + metadata_cols_list + partition_cols
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

        Args:
            batch_info: Batch info with destination_path for partition filtering

        Returns:
            BatchResult with success/rejected status and combined metrics (read + write)
        """
        start_time = time.time()
        metrics = ProcessingMetrics()

        try:
            # Read and filter staging data
            df = self._read_and_filter_staging(batch_info)

            # Handle corrupt records
            df, corrupt_count = self._handle_corrupt_records(df)
            if corrupt_count > self.config.target_max_corrupt_records:
                if self.config.target_fail_on_rejection:
                    metrics.read_duration_ms = int((time.time() - start_time) * 1000)
                    metrics.corrupt_records_count = corrupt_count
                    return BatchResult(
                        status="rejected",
                        rejection_reason=f"Structural corruption: {corrupt_count} corrupt rows exceeds tolerance ({self.config.target_max_corrupt_records})",
                        corrupt_count=corrupt_count,
                        metrics=metrics,
                    )
                else:
                    self.logger.warning(f"Structural corruption: {corrupt_count} corrupt rows exceeds tolerance, but target_fail_on_rejection=False")

            # Trim string columns
            for field in df.schema.fields:
                if isinstance(field.dataType, StringType):
                    df = df.withColumn(field.name, trim(col(field.name)))

            # Apply schema validation
            df, cast_error_count = self._apply_schema_validation(df)
            if cast_error_count > self.config.target_max_corrupt_records:
                if self.config.target_fail_on_rejection:
                    metrics.read_duration_ms = int((time.time() - start_time) * 1000)
                    metrics.corrupt_records_count = corrupt_count + cast_error_count
                    return BatchResult(
                        status="rejected",
                        rejection_reason=f"Type casting errors: {cast_error_count} rows failed to cast (tolerance: {self.config.target_max_corrupt_records})",
                        corrupt_count=corrupt_count + cast_error_count,
                        metrics=metrics,
                    )
                else:
                    self.logger.warning(f"Type casting errors: {cast_error_count} rows failed, but target_fail_on_rejection=False")

            # Remove duplicates and validate
            df = df.distinct()
            duplicate_count = self._count_duplicates(df)
            if duplicate_count > 0:
                if self.config.target_fail_on_rejection:
                    metrics.read_duration_ms = int((time.time() - start_time) * 1000)
                    metrics.source_row_count = df.count()
                    return BatchResult(
                        status="rejected",
                        rejection_reason=f"Found {duplicate_count} duplicate records on merge keys {self.config.target_merge_keys}",
                        metrics=metrics,
                    )
                else:
                    self.logger.warning(f"Found {duplicate_count} duplicates on merge keys, but target_fail_on_rejection=False")

            # Add metadata and write
            df = self._add_target_metadata(df, batch_info)

            metrics.read_duration_ms = int((time.time() - start_time) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count
            metrics.corrupt_records_count = corrupt_count + cast_error_count

            write_metrics = self._write_to_target(df, self.target_table_utils)

            return BatchResult(
                status="success",
                metrics=self._combine_metrics(metrics, write_metrics),
                corrupt_count=corrupt_count + cast_error_count,
            )

        except Exception as e:
            metrics.read_duration_ms = int((time.time() - start_time) * 1000)
            file_path = batch_info.file_paths[0] if batch_info.file_paths else "unknown"

            error_str = str(e).lower()
            is_schema_error = (
                "schema" in error_str
                or "decimal scale" in error_str
                or "decimal precision" in error_str
                or "parsedatatype" in error_str
                or "cannot be greater than precision" in error_str
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
                            "file_format": self.config.extract_file_format_params.file_format,
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
        """
        Create lakehouse utils for reading extract layer files.

        Loader reads from extract layer (raw files), not the original source.
        For filesystem sources, extract layer = raw lakehouse.
        For database sources, extract layer = where JDBC extraction wrote data.
        """
        workspace = self.config.extract_storage_workspace
        lakehouse = self.config.extract_storage_lakehouse

        if not workspace or not lakehouse:
            raise ValueError(
                "extract_storage_workspace and extract_storage_lakehouse must be configured"
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
        """Build file reading options, translated to Spark names."""
        if not self.config.extract_file_format_params.file_format:
            return {}

        file_format = self.config.extract_file_format_params.file_format.lower()

        # Start with user-configured options (snake_case)
        options = dict(self.config.extract_file_format_params.format_options)

        # Enable corrupt record tracking only for text-based formats
        # Binary formats (parquet, avro, orc) have strong schemas and can't have corrupt records
        if file_format in ["csv", "json", "xml"]:
            options["column_name_of_corrupt_record"] = self.metadata_cols.stg_corrupt_record
            options["mode"] = "PERMISSIVE"

        # Translate snake_case to Spark camelCase
        return _translate_options(options)

    def _resolve_schema(self, file_format: str, file_paths: list) -> Optional[StructType]:
        """
        Resolve schema for reading files into staging.

        Always infers schema from the file. Target schema handling (try_cast)
        is done in load_stg_table_to_target() when target_schema_columns is provided.

        Args:
            file_format: File format (csv, json, parquet, etc.)
            file_paths: List of file paths to read

        Returns:
            StructType schema or None (for formats that auto-infer)
        """
        return self._infer_schema_from_file(file_format, file_paths)

    def _infer_schema_from_file(self, file_format: str, file_paths: list) -> Optional[StructType]:
        """
        Infer schema from file.

        CSV requires two-pass (infer then add corrupt record column).
        JSON/XML auto-infer with columnNameOfCorruptRecord.
        Parquet/Avro/ORC use embedded schema.

        Args:
            file_format: File format
            file_paths: List of file paths

        Returns:
            Inferred schema with corrupt record column for text formats
        """
        if file_format == "csv":
            # Two-pass for CSV: infer schema without corrupt record options
            options = self._get_file_read_options()
            infer_options = {k: v for k, v in options.items() if k not in ["columnNameOfCorruptRecord", "mode"]}
            infer_options["inferSchema"] = True
            inferred = self.source_lakehouse_utils.read_file(
                file_path=file_paths,
                file_format=file_format,
                options=infer_options,
            ).schema

            # For headerless CSVs with target_schema_columns: rename _c0, _c1, etc. to target names
            has_header = self.config.extract_file_format_params.format_options.get("has_header", True)
            if self.config.target_schema_columns and not has_header:
                target_names = [f.name for f in self.config.target_schema_columns.to_spark_schema().fields]
                renamed_fields = []
                for i, field in enumerate(inferred.fields):
                    if i < len(target_names):
                        renamed_fields.append(StructField(target_names[i], field.dataType, field.nullable))
                    else:
                        renamed_fields.append(field)  # Extra columns keep inferred names
                inferred = StructType(renamed_fields)

            # Add corrupt record column
            return inferred.add(StructField(self.metadata_cols.stg_corrupt_record, StringType(), True))
        else:
            # JSON/XML auto-infer via columnNameOfCorruptRecord option
            # Parquet/Avro/ORC use embedded schema
            return None  # Let Spark handle it

    def _filter_metadata_columns(self, schema: StructType) -> StructType:
        """
        Remove framework metadata columns from schema.

        Filters out columns starting with _stg_ or _raw_ prefixes.

        Args:
            schema: Source schema (typically from target table)

        Returns:
            Schema with only business columns
        """
        metadata_prefixes = ("_stg_", "_raw_")
        return StructType([
            f for f in schema.fields
            if not f.name.startswith(metadata_prefixes)
        ])

    def _merge_schemas(self, inferred: StructType, existing: StructType) -> StructType:
        """
        Merge inferred schema with existing table schema.

        - Existing columns: use existing types (for consistency)
        - New columns: use inferred types (schema drift)

        Args:
            inferred: Schema inferred from file
            existing: Schema from existing target table

        Returns:
            Merged schema with consistent types for existing columns
        """
        existing_fields = {f.name: f for f in existing.fields if not f.name.startswith(("_stg_", "_raw_"))}

        merged_fields = []
        for field in inferred.fields:
            if field.name in existing_fields:
                # Use existing table's type for known columns
                merged_fields.append(existing_fields[field.name])
            else:
                # New column - use inferred type
                merged_fields.append(field)

        return StructType(merged_fields)

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
        partition_values = {}

        # Extract all key=value pairs from path
        # Pattern: word characters followed by = followed by word/digit/dash characters
        pattern = r'(\w+)=([\w\-]+)'
        matches = re.findall(pattern, path)

        for col_name, col_value in matches:
            partition_values[col_name] = col_value

        return partition_values

    def _read_and_filter_staging(self, batch_info: BatchInfo) -> DataFrame:
        """
        Read from staging table filtered by partition and batch_id.

        Args:
            batch_info: Batch info with destination_path and batch_id

        Returns:
            DataFrame with staging and partition columns dropped
        """
        df = self.stg_table_utils.read_table(
            table_name=self.config.stg_table_name,
            schema_name=self.config.stg_table_schema,
        )

        # Apply partition filters
        if batch_info.destination_path:
            for col_name, col_value in self._parse_partition_values(batch_info.destination_path).items():
                df = df.filter(col(col_name) == col_value)

        # Filter to batch
        df = df.filter(col(self.metadata_cols.stg_created_load_id) == batch_info.batch_id)

        # Drop staging columns
        df = df.drop(
            self.metadata_cols.stg_created_load_id,
            self.metadata_cols.stg_file_path,
            self.metadata_cols.stg_created_at
        )

        # Drop partition columns
        for col_name in self.config.stg_table_partition_columns:
            if col_name in df.columns:
                df = df.drop(col_name)

        return df

    def _handle_corrupt_records(self, df: DataFrame) -> tuple:
        """
        Handle corrupt records from Step 1 - filter and return count.

        Args:
            df: DataFrame potentially containing corrupt records

        Returns:
            Tuple of (filtered_df, corrupt_count)
        """
        if self.metadata_cols.stg_corrupt_record not in df.columns:
            return df, 0

        corrupt_count = df.filter(col(self.metadata_cols.stg_corrupt_record).isNotNull()).count()

        if corrupt_count > 0:
            if corrupt_count <= self.config.target_max_corrupt_records:
                self.logger.warning(f"Found {corrupt_count} structurally corrupt rows, dropping them")
            df = df.filter(col(self.metadata_cols.stg_corrupt_record).isNull())

        return df.drop(self.metadata_cols.stg_corrupt_record), corrupt_count

    def _apply_schema_validation(self, df: DataFrame) -> tuple:
        """
        Apply schema validation with try_cast.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (typed_df, cast_error_count)
        """
        if not self.config.target_schema_columns:
            return df, 0

        target_schema = self.config.target_schema_columns.to_target_schema()

        # Build casted columns
        casted_columns = []
        for field in target_schema.fields:
            if field.name in df.columns:
                casted_columns.append(
                    expr(f"try_cast({field.name} as {field.dataType.simpleString()})").alias(field.name)
                )
            else:
                casted_columns.append(lit(None).cast(field.dataType).alias(field.name))

        # Build error tracking
        error_exprs = []
        for field in target_schema.fields:
            if field.name in df.columns:
                error_exprs.append(
                    when(
                        expr(f"try_cast({field.name} as {field.dataType.simpleString()})").isNull()
                        & col(field.name).isNotNull(),
                        lit(field.name)
                    )
                )

        error_col = concat_ws(", ", *error_exprs).alias("_type_cast_error")
        df_casted = df.select(*casted_columns, error_col)

        cast_error_count = df_casted.filter(col("_type_cast_error") != "").count()

        if cast_error_count > 0:
            self.logger.warning(f"Found {cast_error_count} rows with type cast errors, filtering")
            df_casted = df_casted.filter(col("_type_cast_error") == "")

        return df_casted.drop("_type_cast_error"), cast_error_count

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
        # Get metadata patterns from source_extraction_params (handle both dict and typed params)
        if isinstance(self.config.source_extraction_params, dict):
            metadata_patterns = self.config.source_extraction_params.get("filename_metadata", [])
        elif self.config.source_extraction_params:
            metadata_patterns = getattr(self.config.source_extraction_params, "filename_metadata", [])
        else:
            return df

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
            enable_schema_evolution=self.config.target_schema_drift_enabled,
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

        # Write data
        write_options = {}
        if self.config.target_schema_drift_enabled:
            write_options["mergeSchema"] = "true"

        target_utils.write_to_table(
            df=df,
            table_name=self.config.target_table,
            schema_name=self.config.target_schema,
            mode=self.config.target_write_mode,
            partition_by=self.config.target_partition_columns,
            options=write_options if write_options else None,
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
            corrupt_records_count=read_metrics.corrupt_records_count,
            completed_at=datetime.now(),
        )
