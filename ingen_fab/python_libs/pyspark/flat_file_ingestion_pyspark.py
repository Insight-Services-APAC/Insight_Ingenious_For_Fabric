# PySpark-specific implementations for flat file ingestion
# Uses PySpark DataFrame API and lakehouse_utils

import time
import uuid
from typing import Dict, List, Optional, Any, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, when, coalesce

from ..interfaces.flat_file_ingestion_interface import (
    FlatFileIngestionConfig,
    FileDiscoveryResult,
    ProcessingMetrics,
    FlatFileDiscoveryInterface,
    FlatFileProcessorInterface,
    FlatFileLoggingInterface,
    FlatFileIngestionOrchestrator,
)
from ..common.flat_file_ingestion_utils import (
    DatePartitionUtils,
    FilePatternUtils,
    ConfigurationUtils,
    ProcessingMetricsUtils,
    ErrorHandlingUtils,
)
from ..common.flat_file_logging_schema import get_flat_file_ingestion_log_schema


class PySparkFlatFileDiscovery(FlatFileDiscoveryInterface):
    """PySpark implementation of file discovery using lakehouse_utils"""

    def __init__(self, lakehouse_utils):
        self.lakehouse_utils = lakehouse_utils

    def discover_files(
        self, config: FlatFileIngestionConfig
    ) -> List[FileDiscoveryResult]:
        """Discover files based on configuration using lakehouse_utils"""

        if config.import_pattern != "date_partitioned":
            # For non-date-partitioned imports, return single file path
            return [FileDiscoveryResult(file_path=config.source_file_path)]

        discovered_files = []
        base_path = config.source_file_path.rstrip("/")

        try:
            print(f"🔍 Detected date-partitioned import for {config.config_name}")
            print(f"🔍 Discovering files with pattern: {config.file_discovery_pattern}")
            print(f"🔍 Base path: {base_path}")

            # List all directories in the source directory to find folders matching the pattern
            try:
                directory_items = self.lakehouse_utils.list_directories(
                    base_path, recursive=False
                )
                print(f"📂 Found {len(directory_items)} directories in base directory")
                print(f"🔍 First few directories: {directory_items[:3]}")
            except Exception as e:
                print(f"⚠️ Could not list directory {base_path}: {e}")
                import traceback

                traceback.print_exc()
                return [FileDiscoveryResult(file_path=config.source_file_path)]

            # Process each directory item to find date-partitioned folders
            for item_path in directory_items:
                try:
                    # Extract the folder name from the path
                    folder_name = FilePatternUtils.extract_folder_name_from_path(
                        item_path
                    )

                    # Check if folder matches the file discovery pattern
                    if not FilePatternUtils.matches_pattern(
                        folder_name, config.file_discovery_pattern
                    ):
                        continue

                    # Extract date from folder name based on date_partition_format
                    date_partition = self.extract_date_from_folder_name(
                        folder_name, config.date_partition_format
                    )

                    if date_partition:
                        # Check if date is within the specified range
                        if self.is_date_in_range(
                            date_partition,
                            config.date_range_start,
                            config.date_range_end,
                        ):
                            # Check if we should skip existing dates
                            if (
                                config.skip_existing_dates
                                and self.date_already_processed(date_partition, config)
                            ):
                                print(
                                    f"⏭️ Skipping already processed date: {date_partition}"
                                )
                                continue

                            # Add this folder as a target for processing
                            folder_path = f"{base_path}/{folder_name}"
                            discovered_files.append(
                                FileDiscoveryResult(
                                    file_path=folder_path,
                                    date_partition=date_partition,
                                    folder_name=folder_name,
                                )
                            )
                            print(
                                f"📁 Added date folder: {folder_name} (date: {date_partition})"
                            )
                        else:
                            print(
                                f"📅 Date {date_partition} from folder {folder_name} is outside range ({config.date_range_start} to {config.date_range_end})"
                            )
                    else:
                        print(
                            f"⚠️ Could not extract date from folder name: {folder_name}"
                        )

                except Exception as e:
                    print(f"⚠️ Error processing directory item {item_path}: {e}")
                    continue

            # Sort by date partition for sequential processing
            discovered_files.sort(key=lambda x: x.date_partition or "")

            print(f"📁 Discovered {len(discovered_files)} date folders for processing")

            if not discovered_files:
                print(
                    f"⚠️ No files discovered for date-partitioned import {config.config_name}"
                )

        except Exception as e:
            print(f"⚠️ Warning: File discovery failed: {e}")
            # Fallback to single file processing
            discovered_files = [FileDiscoveryResult(file_path=config.source_file_path)]

        return discovered_files

    def extract_date_from_folder_name(
        self, folder_name: str, date_format: str
    ) -> Optional[str]:
        """Extract date from folder name based on format"""
        return DatePartitionUtils.extract_date_from_folder_name(
            folder_name, date_format
        )

    def extract_date_from_path(
        self, file_path: str, base_path: str, date_format: str
    ) -> Optional[str]:
        """Extract date from file path based on format"""
        return DatePartitionUtils.extract_date_from_path(
            file_path, base_path, date_format
        )

    def is_date_in_range(self, date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        return DatePartitionUtils.is_date_in_range(date_str, start_date, end_date)

    def date_already_processed(
        self, date_partition: str, config: FlatFileIngestionConfig
    ) -> bool:
        """Check if a date has already been processed (for skip_existing_dates feature)"""
        try:
            # Query the log table to see if this date has been successfully processed
            log_df = self.lakehouse_utils.get_connection.sql(f"""
                SELECT DISTINCT date_partition, status 
                FROM log_flat_file_ingestion 
                WHERE config_id = '{config.config_id}' 
                AND date_partition = '{date_partition}'
                AND status = 'completed'
                LIMIT 1
            """)

            return log_df.count() > 0

        except Exception as e:
            print(f"⚠️ Could not check if date {date_partition} already processed: {e}")
            # If we can't check, assume not processed to be safe
            return False


class PySparkFlatFileProcessor(FlatFileProcessorInterface):
    """PySpark implementation of flat file processing using lakehouse_utils"""

    def __init__(self, spark_session: SparkSession, lakehouse_utils):
        self.spark = spark_session
        self.lakehouse_utils = lakehouse_utils

    def read_file(
        self, config: FlatFileIngestionConfig, file_path: str
    ) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read a file based on configuration and return DataFrame with metrics"""
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get file reading options based on configuration
            options = ConfigurationUtils.get_file_read_options(config)

            # Read file using lakehouse_utils abstraction
            if config.source_is_folder:
                df = self._read_folder_contents(config, file_path, options)
            else:
                df = self.lakehouse_utils.read_file(
                    file_path=file_path,
                    file_format=config.source_file_format,
                    options=options,
                )

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            print(
                f"Read {metrics.source_row_count} records from source file in {metrics.read_duration_ms}ms"
            )

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            print(f"❌ Error reading file {file_path}: {e}")
            raise

    def read_folder(
        self, config: FlatFileIngestionConfig, folder_path: str
    ) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read all files in a folder based on configuration"""
        return self.read_file(config, folder_path)

    def _read_folder_contents(
        self, config: FlatFileIngestionConfig, folder_path: str, options: Dict[str, Any]
    ) -> DataFrame:
        """Read all files in a folder using wildcard pattern first, with fallback"""
        print(f"📁 Reading folder: {folder_path}")

        try:
            # Try wildcard pattern first
            wildcard_path = f"{folder_path}/part-*"
            print(f"Reading file from: {wildcard_path}")

            df = self.lakehouse_utils.read_file(
                file_path=wildcard_path,
                file_format=config.source_file_format,
                options=options,
            )

            print(f"✓ Successfully read folder using wildcard pattern: {wildcard_path}")
            return df

        except Exception as wildcard_error:
            print(f"⚠️ Wildcard pattern failed: {wildcard_error}")

            try:
                # Fallback: list files and read individually, then union
                file_list = self.lakehouse_utils.list_files(
                    folder_path, recursive=False
                )

                if not file_list:
                    raise Exception(f"No data files found in folder: {folder_path}")

                # Filter for relevant files (e.g., part-* files)
                data_files = [
                    f for f in file_list if "part-" in f and not f.endswith("_SUCCESS")
                ]

                if not data_files:
                    raise Exception(f"No data files found in folder: {folder_path}")

                print(f"📂 Found {len(data_files)} data files in folder")

                # Read first file to get schema
                first_df = self.lakehouse_utils.read_file(
                    file_path=data_files[0],
                    file_format=config.source_file_format,
                    options=options,
                )

                # If only one file, return it
                if len(data_files) == 1:
                    return first_df

                # Read remaining files and union them
                all_dfs = [first_df]
                for file_path in data_files[1:]:
                    df = self.lakehouse_utils.read_file(
                        file_path=file_path,
                        file_format=config.source_file_format,
                        options=options,
                    )
                    all_dfs.append(df)

                # Union all dataframes
                result_df = all_dfs[0]
                for df in all_dfs[1:]:
                    result_df = result_df.union(df)

                print(f"✓ Successfully read folder by unioning {len(data_files)} files")
                return result_df

            except Exception as fallback_error:
                print(f"❌ Error reading folder {folder_path}: {fallback_error}")
                raise Exception(f"No data files found in folder: {folder_path}")

    def write_data(
        self, data: DataFrame, config: FlatFileIngestionConfig
    ) -> ProcessingMetrics:
        """Write data to target destination using lakehouse_utils"""
        write_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get target row count before write (for reconciliation)
            try:
                existing_df = self.lakehouse_utils.read_table(config.target_table_name)
                metrics.target_row_count_before = existing_df.count()
            except Exception:
                metrics.target_row_count_before = 0

            # Write data using lakehouse_utils abstraction
            self.lakehouse_utils.write_to_table(
                df=data,
                table_name=config.target_table_name,
                mode=config.write_mode,
                partition_by=config.partition_columns,
            )

            # Get target row count after write
            try:
                result_df = self.lakehouse_utils.read_table(config.target_table_name)
                metrics.target_row_count_after = result_df.count()
            except Exception:
                metrics.target_row_count_after = 0

            # Calculate metrics
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)

            if config.write_mode == "overwrite":
                metrics.records_inserted = metrics.target_row_count_after
                metrics.records_deleted = metrics.target_row_count_before
            elif config.write_mode == "append":
                metrics.records_inserted = (
                    metrics.target_row_count_after - metrics.target_row_count_before
                )

            print(
                f"Wrote data to {config.target_table_name} in {metrics.write_duration_ms}ms"
            )

            return ProcessingMetricsUtils.calculate_performance_metrics(metrics)

        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            print(f"❌ Error writing data to {config.target_table_name}: {e}")
            raise

    def validate_data(
        self, data: DataFrame, config: FlatFileIngestionConfig
    ) -> Dict[str, Any]:
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

            # Basic validation
            if validation_results["row_count"] == 0:
                validation_results["errors"].append("No data found in source")
                validation_results["is_valid"] = False

            # Additional validation rules can be implemented here
            # based on config.data_validation_rules

        except Exception as e:
            validation_results["errors"].append(f"Validation failed: {e}")
            validation_results["is_valid"] = False

        return validation_results


class PySparkFlatFileLogging(FlatFileLoggingInterface):
    """PySpark implementation of flat file ingestion logging"""

    def __init__(self, lakehouse_utils):
        self.lakehouse_utils = lakehouse_utils

    def log_execution_start(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        partition_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Log the start of a file processing execution"""
        try:
            from datetime import datetime
            import json
            from pyspark.sql.types import (
                FloatType,
                IntegerType,
                LongType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            # Extract partition information
            partition_cols = None
            partition_values = None
            date_partition = None

            if partition_info:
                if "columns" in partition_info:
                    partition_cols = json.dumps(partition_info["columns"])
                if "values" in partition_info:
                    partition_values = json.dumps(partition_info["values"])
                if "date_partition" in partition_info:
                    date_partition = partition_info["date_partition"]

            log_data = [
                (
                    str(uuid.uuid4()),  # log_id
                    config.config_id,
                    execution_id,
                    datetime.now(),  # job_start_time
                    None,  # job_end_time
                    "running",  # status
                    config.source_file_path,
                    None,  # source_file_size_bytes
                    None,  # source_file_modified_time
                    config.target_table_name,
                    None,  # records_processed
                    None,  # records_inserted
                    None,  # records_updated
                    None,  # records_deleted
                    None,  # records_failed
                    None,  # source_row_count
                    None,  # staging_row_count
                    None,  # target_row_count_before
                    None,  # target_row_count_after
                    None,  # row_count_reconciliation_status
                    None,  # row_count_difference
                    None,  # data_read_duration_ms
                    None,  # staging_write_duration_ms
                    None,  # merge_duration_ms
                    None,  # total_duration_ms
                    None,  # avg_rows_per_second
                    None,  # data_size_mb
                    None,  # throughput_mb_per_second
                    None,  # error_message
                    None,  # error_details
                    None,  # execution_duration_seconds
                    None,  # spark_application_id
                    partition_cols,  # source_file_partition_cols
                    partition_values,  # source_file_partition_values
                    date_partition,  # date_partition
                    datetime.now(),  # created_date
                    "system",  # created_by
                )
            ]

            # Define proper schema with types
            schema = StructType(
                [
                    StructField("log_id", StringType(), nullable=False),
                    StructField("config_id", StringType(), nullable=False),
                    StructField("execution_id", StringType(), nullable=False),
                    StructField("job_start_time", TimestampType(), nullable=False),
                    StructField("job_end_time", TimestampType(), nullable=True),
                    StructField("status", StringType(), nullable=False),
                    StructField("source_file_path", StringType(), nullable=False),
                    StructField("source_file_size_bytes", LongType(), nullable=True),
                    StructField(
                        "source_file_modified_time", TimestampType(), nullable=True
                    ),
                    StructField("target_table_name", StringType(), nullable=False),
                    StructField("records_processed", LongType(), nullable=True),
                    StructField("records_inserted", LongType(), nullable=True),
                    StructField("records_updated", LongType(), nullable=True),
                    StructField("records_deleted", LongType(), nullable=True),
                    StructField("records_failed", LongType(), nullable=True),
                    StructField("source_row_count", LongType(), nullable=True),
                    StructField("staging_row_count", LongType(), nullable=True),
                    StructField("target_row_count_before", LongType(), nullable=True),
                    StructField("target_row_count_after", LongType(), nullable=True),
                    StructField(
                        "row_count_reconciliation_status", StringType(), nullable=True
                    ),
                    StructField("row_count_difference", LongType(), nullable=True),
                    StructField("data_read_duration_ms", LongType(), nullable=True),
                    StructField("staging_write_duration_ms", LongType(), nullable=True),
                    StructField("merge_duration_ms", LongType(), nullable=True),
                    StructField("total_duration_ms", LongType(), nullable=True),
                    StructField("avg_rows_per_second", FloatType(), nullable=True),
                    StructField("data_size_mb", FloatType(), nullable=True),
                    StructField("throughput_mb_per_second", FloatType(), nullable=True),
                    StructField("error_message", StringType(), nullable=True),
                    StructField("error_details", StringType(), nullable=True),
                    StructField(
                        "execution_duration_seconds", IntegerType(), nullable=True
                    ),
                    StructField("spark_application_id", StringType(), nullable=True),
                    StructField(
                        "source_file_partition_cols", StringType(), nullable=True
                    ),
                    StructField(
                        "source_file_partition_values", StringType(), nullable=True
                    ),
                    StructField("date_partition", StringType(), nullable=True),
                    StructField("created_date", TimestampType(), nullable=False),
                    StructField("created_by", StringType(), nullable=False),
                ]
            )

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            self.lakehouse_utils.write_to_table(
                df=log_df, table_name="log_flat_file_ingestion", mode="append"
            )

        except Exception as e:
            print(f"⚠️ Failed to log execution start: {e}")

    def log_execution_completion(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        metrics: ProcessingMetrics,
        status: str,
        partition_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Log the completion of a file processing execution"""
        try:
            from datetime import datetime
            import json
            from pyspark.sql.types import (
                FloatType,
                IntegerType,
                LongType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            # Extract partition information
            partition_cols = None
            partition_values = None
            date_partition = None

            if partition_info:
                if "columns" in partition_info:
                    partition_cols = json.dumps(partition_info["columns"])
                if "values" in partition_info:
                    partition_values = json.dumps(partition_info["values"])
                if "date_partition" in partition_info:
                    date_partition = partition_info["date_partition"]

            log_data = [
                (
                    str(uuid.uuid4()),  # log_id
                    config.config_id,
                    execution_id,
                    datetime.now(),  # job_start_time
                    datetime.now(),  # job_end_time
                    status,
                    config.source_file_path,
                    int(metrics.data_size_mb * 1024 * 1024)
                    if metrics.data_size_mb and metrics.data_size_mb > 0
                    else None,
                    None,  # source_file_modified_time
                    config.target_table_name,
                    metrics.records_processed,
                    metrics.records_inserted,
                    metrics.records_updated,
                    metrics.records_deleted,
                    metrics.records_failed,
                    metrics.source_row_count,
                    None,  # staging_row_count
                    metrics.target_row_count_before,
                    metrics.target_row_count_after,
                    metrics.row_count_reconciliation_status,
                    None,  # row_count_difference
                    metrics.read_duration_ms,
                    None,  # staging_write_duration_ms
                    None,  # merge_duration_ms
                    metrics.total_duration_ms,
                    metrics.avg_rows_per_second,
                    metrics.data_size_mb,
                    metrics.throughput_mb_per_second,
                    None,  # error_message
                    None,  # error_details
                    int(metrics.total_duration_ms / 1000)
                    if metrics.total_duration_ms
                    else None,
                    None,  # spark_application_id
                    partition_cols,  # source_file_partition_cols
                    partition_values,  # source_file_partition_values
                    date_partition,  # date_partition
                    datetime.now(),  # created_date
                    "system",  # created_by
                )
            ]

            # Define proper schema with types
            schema = StructType(
                [
                    StructField("log_id", StringType(), nullable=False),
                    StructField("config_id", StringType(), nullable=False),
                    StructField("execution_id", StringType(), nullable=False),
                    StructField("job_start_time", TimestampType(), nullable=False),
                    StructField("job_end_time", TimestampType(), nullable=True),
                    StructField("status", StringType(), nullable=False),
                    StructField("source_file_path", StringType(), nullable=False),
                    StructField("source_file_size_bytes", LongType(), nullable=True),
                    StructField(
                        "source_file_modified_time", TimestampType(), nullable=True
                    ),
                    StructField("target_table_name", StringType(), nullable=False),
                    StructField("records_processed", LongType(), nullable=True),
                    StructField("records_inserted", LongType(), nullable=True),
                    StructField("records_updated", LongType(), nullable=True),
                    StructField("records_deleted", LongType(), nullable=True),
                    StructField("records_failed", LongType(), nullable=True),
                    StructField("source_row_count", LongType(), nullable=True),
                    StructField("staging_row_count", LongType(), nullable=True),
                    StructField("target_row_count_before", LongType(), nullable=True),
                    StructField("target_row_count_after", LongType(), nullable=True),
                    StructField(
                        "row_count_reconciliation_status", StringType(), nullable=True
                    ),
                    StructField("row_count_difference", LongType(), nullable=True),
                    StructField("data_read_duration_ms", LongType(), nullable=True),
                    StructField("staging_write_duration_ms", LongType(), nullable=True),
                    StructField("merge_duration_ms", LongType(), nullable=True),
                    StructField("total_duration_ms", LongType(), nullable=True),
                    StructField("avg_rows_per_second", FloatType(), nullable=True),
                    StructField("data_size_mb", FloatType(), nullable=True),
                    StructField("throughput_mb_per_second", FloatType(), nullable=True),
                    StructField("error_message", StringType(), nullable=True),
                    StructField("error_details", StringType(), nullable=True),
                    StructField(
                        "execution_duration_seconds", IntegerType(), nullable=True
                    ),
                    StructField("spark_application_id", StringType(), nullable=True),
                    StructField(
                        "source_file_partition_cols", StringType(), nullable=True
                    ),
                    StructField(
                        "source_file_partition_values", StringType(), nullable=True
                    ),
                    StructField("date_partition", StringType(), nullable=True),
                    StructField("created_date", TimestampType(), nullable=False),
                    StructField("created_by", StringType(), nullable=False),
                ]
            )

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            self.lakehouse_utils.write_to_table(
                df=log_df, table_name="log_flat_file_ingestion", mode="append"
            )

        except Exception as e:
            print(f"⚠️ Failed to log execution completion: {e}")

    def log_execution_error(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        error_message: str,
        error_details: str,
        partition_info: Optional[Dict[str, str]] = None,
    ) -> None:
        """Log an execution error"""
        try:
            from datetime import datetime
            import json
            from pyspark.sql.types import (
                FloatType,
                IntegerType,
                LongType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            # Extract partition information
            partition_cols = None
            partition_values = None
            date_partition = None

            if partition_info:
                if "columns" in partition_info:
                    partition_cols = json.dumps(partition_info["columns"])
                if "values" in partition_info:
                    partition_values = json.dumps(partition_info["values"])
                if "date_partition" in partition_info:
                    date_partition = partition_info["date_partition"]

            log_data = [
                (
                    str(uuid.uuid4()),  # log_id
                    config.config_id,
                    execution_id,
                    datetime.now(),  # job_start_time
                    datetime.now(),  # job_end_time
                    "failed",
                    config.source_file_path,
                    None,  # source_file_size_bytes
                    None,  # source_file_modified_time
                    config.target_table_name,
                    0,  # records_processed
                    0,  # records_inserted
                    0,  # records_updated
                    0,  # records_deleted
                    0,  # records_failed
                    0,  # source_row_count
                    0,  # staging_row_count
                    0,  # target_row_count_before
                    0,  # target_row_count_after
                    "not_verified",  # row_count_reconciliation_status
                    None,  # row_count_difference
                    None,  # data_read_duration_ms
                    None,  # staging_write_duration_ms
                    None,  # merge_duration_ms
                    None,  # total_duration_ms
                    None,  # avg_rows_per_second
                    None,  # data_size_mb
                    None,  # throughput_mb_per_second
                    error_message,  # error_message
                    error_details,  # error_details
                    None,  # execution_duration_seconds
                    None,  # spark_application_id
                    partition_cols,  # source_file_partition_cols
                    partition_values,  # source_file_partition_values
                    date_partition,  # date_partition
                    datetime.now(),  # created_date
                    "system",  # created_by
                )
            ]

            # Define proper schema with types
            schema = StructType(
                [
                    StructField("log_id", StringType(), nullable=False),
                    StructField("config_id", StringType(), nullable=False),
                    StructField("execution_id", StringType(), nullable=False),
                    StructField("job_start_time", TimestampType(), nullable=False),
                    StructField("job_end_time", TimestampType(), nullable=True),
                    StructField("status", StringType(), nullable=False),
                    StructField("source_file_path", StringType(), nullable=False),
                    StructField("source_file_size_bytes", LongType(), nullable=True),
                    StructField(
                        "source_file_modified_time", TimestampType(), nullable=True
                    ),
                    StructField("target_table_name", StringType(), nullable=False),
                    StructField("records_processed", LongType(), nullable=True),
                    StructField("records_inserted", LongType(), nullable=True),
                    StructField("records_updated", LongType(), nullable=True),
                    StructField("records_deleted", LongType(), nullable=True),
                    StructField("records_failed", LongType(), nullable=True),
                    StructField("source_row_count", LongType(), nullable=True),
                    StructField("staging_row_count", LongType(), nullable=True),
                    StructField("target_row_count_before", LongType(), nullable=True),
                    StructField("target_row_count_after", LongType(), nullable=True),
                    StructField(
                        "row_count_reconciliation_status", StringType(), nullable=True
                    ),
                    StructField("row_count_difference", LongType(), nullable=True),
                    StructField("data_read_duration_ms", LongType(), nullable=True),
                    StructField("staging_write_duration_ms", LongType(), nullable=True),
                    StructField("merge_duration_ms", LongType(), nullable=True),
                    StructField("total_duration_ms", LongType(), nullable=True),
                    StructField("avg_rows_per_second", FloatType(), nullable=True),
                    StructField("data_size_mb", FloatType(), nullable=True),
                    StructField("throughput_mb_per_second", FloatType(), nullable=True),
                    StructField("error_message", StringType(), nullable=True),
                    StructField("error_details", StringType(), nullable=True),
                    StructField(
                        "execution_duration_seconds", IntegerType(), nullable=True
                    ),
                    StructField("spark_application_id", StringType(), nullable=True),
                    StructField(
                        "source_file_partition_cols", StringType(), nullable=True
                    ),
                    StructField(
                        "source_file_partition_values", StringType(), nullable=True
                    ),
                    StructField("date_partition", StringType(), nullable=True),
                    StructField("created_date", TimestampType(), nullable=False),
                    StructField("created_by", StringType(), nullable=False),
                ]
            )

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            self.lakehouse_utils.write_to_table(
                df=log_df, table_name="log_flat_file_ingestion", mode="append"
            )

        except Exception as e:
            print(f"⚠️ Failed to log execution error: {e}")


class PySparkFlatFileIngestionOrchestrator(FlatFileIngestionOrchestrator):
    """PySpark implementation of flat file ingestion orchestrator"""

    def process_configuration(
        self, config: FlatFileIngestionConfig, execution_id: str
    ) -> Dict[str, Any]:
        """Process a single configuration"""
        start_time = time.time()
        result = {
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "pending",
            "metrics": ProcessingMetrics(),
            "errors": [],
        }

        try:
            print(f"\n=== Processing {config.config_name} ===")
            print(f"Source: {config.source_file_path}")
            print(f"Target: {config.target_schema_name}.{config.target_table_name}")
            print(f"Format: {config.source_file_format}")
            print(f"Write Mode: {config.write_mode}")

            # Validate configuration
            validation_errors = ConfigurationUtils.validate_config(config)
            if validation_errors:
                result["errors"] = validation_errors
                result["status"] = "failed"
                return result

            # Log execution start
            self.logging_service.log_execution_start(config, execution_id)

            # Discover files
            discovered_files = self.discovery_service.discover_files(config)

            if not discovered_files:
                result["status"] = "no_data_found"
                print(
                    f"⚠️ No source data found for {config.config_name}. Skipping write and reconciliation operations."
                )
                return result

            # Process discovered files sequentially
            all_metrics = []

            for file_result in discovered_files:
                try:
                    # Build partition info if this is a date-partitioned file
                    partition_info = None
                    if file_result.date_partition:
                        partition_info = {
                            "columns": ["date"],
                            "values": [file_result.date_partition],
                            "date_partition": file_result.date_partition,
                        }
                        # Log start for this specific partition
                        self.logging_service.log_execution_start(
                            config, execution_id, partition_info
                        )

                    # Read file
                    df, read_metrics = self.processor_service.read_file(
                        config, file_result.file_path
                    )

                    if read_metrics.source_row_count == 0:
                        print(f"⚠️ No data found in file: {file_result.file_path}")
                        continue

                    # Validate data
                    validation_result = self.processor_service.validate_data(df, config)
                    if not validation_result["is_valid"]:
                        result["errors"].extend(validation_result["errors"])
                        continue

                    # Write data
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

                except Exception as file_error:
                    error_details = ErrorHandlingUtils.format_error_details(
                        file_error,
                        {
                            "file_path": file_result.file_path,
                            "config_id": config.config_id,
                        },
                    )
                    result["errors"].append(
                        f"Error processing file {file_result.file_path}: {file_error}"
                    )
                    print(
                        f"❌ Error processing file {file_result.file_path}: {file_error}"
                    )

                    # Log error with partition info if available
                    if file_result.date_partition:
                        partition_info = {
                            "columns": ["date"],
                            "values": [file_result.date_partition],
                            "date_partition": file_result.date_partition,
                        }
                        self.logging_service.log_execution_error(
                            config,
                            execution_id,
                            str(file_error),
                            str(error_details),
                            partition_info,
                        )

            # Aggregate metrics
            if all_metrics:
                result["metrics"] = ProcessingMetricsUtils.merge_metrics(all_metrics)
                result["status"] = "completed"

                # Calculate processing time
                end_time = time.time()
                processing_duration = end_time - start_time
                result["metrics"].total_duration_ms = int(processing_duration * 1000)

                print(f"\nProcessing completed in {processing_duration:.2f} seconds")
                print(f"Records processed: {result['metrics'].records_processed}")
                print(f"Records inserted: {result['metrics'].records_inserted}")
                print(f"Records updated: {result['metrics'].records_updated}")
                print(f"Records deleted: {result['metrics'].records_deleted}")
                print(f"Source rows: {result['metrics'].source_row_count}")
                print(
                    f"Target rows before: {result['metrics'].target_row_count_before}"
                )
                print(f"Target rows after: {result['metrics'].target_row_count_after}")
                print(
                    f"Row count reconciliation: {result['metrics'].row_count_reconciliation_status}"
                )

                # Log completion
                self.logging_service.log_execution_completion(
                    config, execution_id, result["metrics"], "completed"
                )
            else:
                result["status"] = "no_data_processed"
                print(
                    f"Processing completed in {time.time() - start_time:.2f} seconds (no data processed)"
                )
                print("Row count reconciliation: skipped (no source data)")

        except Exception as e:
            result["status"] = "failed"
            result["errors"].append(str(e))
            error_details = ErrorHandlingUtils.format_error_details(
                e, {"config_id": config.config_id, "config_name": config.config_name}
            )
            self.logging_service.log_execution_error(
                config, execution_id, str(e), str(error_details)
            )
            print(f"Error processing {config.config_name}: {e}")

        return result

    def process_configurations(
        self, configs: List[FlatFileIngestionConfig], execution_id: str
    ) -> Dict[str, Any]:
        """Process multiple configurations"""
        results = {
            "execution_id": execution_id,
            "total_configurations": len(configs),
            "successful": 0,
            "failed": 0,
            "no_data_found": 0,
            "configurations": [],
        }

        for config in configs:
            config_result = self.process_configuration(config, execution_id)
            results["configurations"].append(config_result)

            if config_result["status"] == "completed":
                results["successful"] += 1
            elif config_result["status"] == "failed":
                results["failed"] += 1
            elif config_result["status"] in ["no_data_found", "no_data_processed"]:
                results["no_data_found"] += 1

        return results
