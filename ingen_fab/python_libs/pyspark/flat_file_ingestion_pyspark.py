# PySpark-specific implementations for flat file ingestion
# Uses PySpark DataFrame API and lakehouse_utils

import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
    ConfigurationUtils,
    DatePartitionUtils,
    ErrorHandlingUtils,
    FilePatternUtils,
    ProcessingMetricsUtils,
)
from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import (
    FileDiscoveryResult,
    FlatFileDiscoveryInterface,
    FlatFileIngestionConfig,
    FlatFileIngestionOrchestrator,
    FlatFileLoggingInterface,
    FlatFileProcessorInterface,
    ProcessingMetrics,
)


class PySparkFlatFileDiscovery(FlatFileDiscoveryInterface):
    """PySpark implementation of file discovery using dynamic location resolution"""

    def __init__(self, location_resolver, spark=None):
        self.location_resolver = location_resolver
        self.spark = spark

    def discover_files(self, config: FlatFileIngestionConfig) -> List[FileDiscoveryResult]:
        """Discover files based on configuration using dynamic source resolution"""

        # Resolve full source path
        full_source_path = self.location_resolver.resolve_full_source_path(config)

        if config.import_pattern != "date_partitioned":
            # For non-date-partitioned imports, return single file path
            return [FileDiscoveryResult(file_path=full_source_path)]

        discovered_files = []
        base_path = full_source_path.rstrip("/")

        try:
            print(f"🔍 Detected date-partitioned import for {config.config_name}")
            print(f"🔍 Discovering files with pattern: {config.file_discovery_pattern}")
            print(f"🔍 Base path: {base_path}")

            # Check if hierarchical nested structure is enabled
            if config.hierarchical_date_structure and config.table_subfolder:
                print(f"🏗️ Using hierarchical structure with table: {config.table_subfolder}")
                return self._discover_nested_hierarchical_files(config, base_path)

            # Original flat structure discovery
            return self._discover_flat_date_partitioned_files(config, base_path)

        except Exception as e:
            print(f"⚠️ Warning: File discovery failed: {e}")
            # Fallback to single file processing
            discovered_files = [FileDiscoveryResult(file_path=config.source_file_path)]

        return discovered_files

    def _discover_nested_hierarchical_files(
        self, config: FlatFileIngestionConfig, base_path: str
    ) -> List[FileDiscoveryResult]:
        """Discover files in hierarchical nested structure (YYYY/MM/DD/table_name/)"""
        discovered_files = []

        try:
            # Get source utilities for this configuration
            source_utils = self.location_resolver.get_source_utils(config, spark=self.spark)

            # Convert base_path to absolute local path for file system operations
            if hasattr(source_utils, "lakehouse_files_uri"):
                files_uri = source_utils.lakehouse_files_uri()
                if files_uri.startswith("file:///"):
                    # Extract the local path
                    local_base_path = files_uri.replace("file:///", "/")
                    if not base_path.startswith("/"):
                        full_local_path = os.path.join(local_base_path, base_path)
                    else:
                        full_local_path = base_path
                else:
                    full_local_path = base_path
            else:
                full_local_path = base_path

            print(f"🔍 Searching hierarchical structure at: {full_local_path}")

            # Use the new utility to discover nested paths
            nested_paths = DatePartitionUtils.discover_nested_date_table_paths(
                base_path=full_local_path,
                date_format=config.date_partition_format,
                table_subfolder=config.table_subfolder,
            )

            print(f"📁 Found {len(nested_paths)} nested date/table combinations")

            for table_path, date_partition, table_name in nested_paths:
                # Check if date is within the specified range
                if self.is_date_in_range(
                    date_partition,
                    config.date_range_start,
                    config.date_range_end,
                ):
                    # Check if we should skip existing dates
                    if config.skip_existing_dates and self.date_already_processed(date_partition, config):
                        print(f"⏭️ Skipping already processed date: {date_partition}")
                        continue

                    # Convert back to relative path for processing
                    if full_local_path in table_path:
                        relative_table_path = table_path.replace(full_local_path, "").lstrip("/")
                        if not relative_table_path.startswith(config.source_file_path):
                            relative_table_path = f"{config.source_file_path.rstrip('/')}/{relative_table_path}"
                    else:
                        relative_table_path = table_path

                    discovered_files.append(
                        FileDiscoveryResult(
                            file_path=relative_table_path,
                            date_partition=date_partition,
                            folder_name=f"{date_partition}_{table_name}",
                            nested_structure=True,
                        )
                    )
                    print(f"📁 Added nested path: {relative_table_path} (date: {date_partition}, table: {table_name})")
                else:
                    print(
                        f"📅 Date {date_partition} from table {table_name} is outside range ({config.date_range_start} to {config.date_range_end})"
                    )

        except Exception as e:
            print(f"⚠️ Error in nested hierarchical discovery: {e}")
            import traceback

            traceback.print_exc()

        # Sort by date partition for sequential processing (same as flat discovery)
        discovered_files.sort(key=lambda x: x.date_partition or "")

        print(f"📅 Sorted {len(discovered_files)} nested files chronologically")

        return discovered_files

    def _discover_flat_date_partitioned_files(
        self, config: FlatFileIngestionConfig, base_path: str
    ) -> List[FileDiscoveryResult]:
        """Discover files in flat date-partitioned structure (original logic)"""
        discovered_files = []

        try:
            # List all directories in the source directory to find folders matching the pattern
            try:
                directory_items = self.location_resolver.get_source_utils(config, spark=self.spark).list_directories(
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
                    folder_name = FilePatternUtils.extract_folder_name_from_path(item_path)

                    # Check if folder matches the file discovery pattern
                    if not FilePatternUtils.matches_pattern(folder_name, config.file_discovery_pattern):
                        continue

                    # Extract date from folder name based on date_partition_format
                    date_partition = self.extract_date_from_folder_name(folder_name, config.date_partition_format)

                    if date_partition:
                        # Check if date is within the specified range
                        if self.is_date_in_range(
                            date_partition,
                            config.date_range_start,
                            config.date_range_end,
                        ):
                            # Check if we should skip existing dates
                            if config.skip_existing_dates and self.date_already_processed(date_partition, config):
                                print(f"⏭️ Skipping already processed date: {date_partition}")
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
                            print(f"📁 Added date folder: {folder_name} (date: {date_partition})")
                        else:
                            print(
                                f"📅 Date {date_partition} from folder {folder_name} is outside range ({config.date_range_start} to {config.date_range_end})"
                            )
                    else:
                        print(f"⚠️ Could not extract date from folder name: {folder_name}")

                except Exception as e:
                    print(f"⚠️ Error processing directory item {item_path}: {e}")
                    continue

            # Sort by date partition for sequential processing
            discovered_files.sort(key=lambda x: x.date_partition or "")

            print(f"📁 Discovered {len(discovered_files)} date folders for processing")

            if not discovered_files:
                print(f"⚠️ No files discovered for date-partitioned import {config.config_name}")

        except Exception as e:
            print(f"⚠️ Warning: File discovery failed: {e}")
            # Fallback to single file processing
            discovered_files = [FileDiscoveryResult(file_path=config.source_file_path)]

        return discovered_files

    def extract_date_from_folder_name(self, folder_name: str, date_format: str) -> Optional[str]:
        """Extract date from folder name based on format"""
        return DatePartitionUtils.extract_date_from_folder_name(folder_name, date_format)

    def extract_date_from_path(self, file_path: str, base_path: str, date_format: str) -> Optional[str]:
        """Extract date from file path based on format"""
        return DatePartitionUtils.extract_date_from_path(file_path, base_path, date_format)

    def is_date_in_range(self, date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        return DatePartitionUtils.is_date_in_range(date_str, start_date, end_date)

    def date_already_processed(self, date_partition: str, config: FlatFileIngestionConfig) -> bool:
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
    """PySpark implementation of flat file processing using dynamic location resolution"""

    def __init__(self, spark_session: SparkSession, location_resolver):
        self.spark = spark_session
        self.location_resolver = location_resolver

    def read_file(
        self, config: FlatFileIngestionConfig, file_path: FileDiscoveryResult
    ) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read a file based on configuration and return DataFrame with metrics"""
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get source utilities for this configuration
            source_utils = self.location_resolver.get_source_utils(config, spark=self.spark)

            # Get file reading options based on configuration
            options = ConfigurationUtils.get_file_read_options(config)

            # Read file using dynamic source utilities
            if config.source_is_folder:
                df = self._read_folder_contents(config, file_path, options, source_utils)
            else:
                df = source_utils.read_file(
                    file_path=file_path.file_path,
                    file_format=config.source_file_format,
                    options=options,
                )

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            print(f"Read {metrics.source_row_count} records from source file in {metrics.read_duration_ms}ms")

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            print(f"❌ Error reading file {file_path}: {e}")
            raise

    def read_folder(self, config: FlatFileIngestionConfig, folder_path: str) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read all files in a folder based on configuration"""
        return self.read_file(config, folder_path)

    def _analyze_path_structure(
        self, file: FileDiscoveryResult, config: FlatFileIngestionConfig, source_utils
    ) -> Dict[str, Any]:
        """Systematically analyze path structure to optimize processing decisions"""
        analysis = {
            "path_exists": False,
            "is_file": False,
            "is_folder": False,
            "contains_files": False,
            "contains_folders": False,
            "total_files": 0,
            "total_folders": 0,
            "data_files": [],
            "metadata_files": [],
            "other_files": [],
            "subfolders": [],
            "file_extensions": set(),
            "has_spark_structure": False,
            "recommended_approach": "spark_fallback",
        }

        try:
            # Check if path exists
            if not source_utils.file_exists(file.file_path):
                analysis["recommended_approach"] = "error_file_not_found"
                return analysis

            analysis["path_exists"] = True

            # Try to get file info to determine if it's a file or folder
            try:
                file_info = source_utils.get_file_info(file.file_path)
                analysis["is_file"] = not file_info.get("is_directory", True)
                analysis["is_folder"] = file_info.get("is_directory", True)
            except:  # noqa: E722
                # Fallback: assume it's a folder if we can't determine
                analysis["is_folder"] = True

            # If it's a single file, recommend direct read
            if analysis["is_file"]:
                analysis["recommended_approach"] = "direct_read"
                return analysis

            # Analyze folder contents
            try:
                all_files = source_utils.list_files(file.file_path, recursive=False)
                all_directories = source_utils.list_directories(directory_path=file.file_path, recursive=False)
                analysis["total_files"] = len([item for item in all_files])
                analysis["total_folders"] = len([item for item in all_directories])
                analysis["contains_files"] = analysis["total_files"] > 0
                analysis["contains_folders"] = analysis["total_folders"] > 0

                # Categorize files
                for item in all_files:
                    # Extract file extension
                    if "." in item:
                        ext = item.split(".")[-1].lower()
                        analysis["file_extensions"].add(ext)

                    # Categorize by type
                    if item.endswith("_SUCCESS") or item.endswith(".crc") or item.startswith("."):
                        analysis["metadata_files"].append(item)
                    elif "part-" in item or config.source_file_format.lower() in item.lower():
                        analysis["data_files"].append(item)
                    else:
                        analysis["other_files"].append(item)

                for dir_item in all_directories:
                    analysis["subfolders"].append(dir_item)

                # Check for Spark-style output structure
                if any("part-" in f for f in analysis["data_files"]) and "_SUCCESS" in str(analysis["metadata_files"]):
                    analysis["has_spark_structure"] = True

                # Determine recommended approach based on analysis
                if len(analysis["data_files"]) == 0 and len(analysis["subfolders"]) >= 0:
                    analysis["recommended_approach"] = "subfolder_processing"  # Process subfolders individually
                if len(analysis["data_files"]) == 0:
                    if len(analysis["other_files"]) > 0:
                        analysis["recommended_approach"] = "wildcard_pattern"  # Let Spark try to read other files
                    else:
                        analysis["recommended_approach"] = "spark_fallback"  # No obvious data files
                elif len(analysis["data_files"]) == 1:
                    analysis["recommended_approach"] = "direct_read"
                elif config.source_file_format.lower() == "parquet" and len(analysis["data_files"]) > 1:
                    analysis["recommended_approach"] = "individual_union"  # Avoid schema warnings
                elif analysis["has_spark_structure"]:
                    analysis["recommended_approach"] = "individual_union"  # Handle Spark output properly
                elif len(analysis["data_files"]) <= 5:
                    analysis["recommended_approach"] = "individual_union"  # Small number, union is efficient
                else:
                    analysis["recommended_approach"] = "wildcard_pattern"  # Large number, let Spark handle

            except Exception as list_error:
                print(f"ℹ️ Cannot analyze folder contents: {list_error}")
                analysis["recommended_approach"] = "spark_fallback"

        except Exception as analysis_error:
            print(f"ℹ️ Path analysis failed: {analysis_error}")
            analysis["recommended_approach"] = "spark_fallback"

        return analysis

    def _read_folder_contents(
        self,
        config: FlatFileIngestionConfig,
        file: FileDiscoveryResult,
        options: Dict[str, Any],
        source_utils,
    ) -> DataFrame:
        """Read all files in a folder with intelligent path analysis and optimization"""
        print(f"📁 Reading folder: {file.file_path}")

        # Systematic path analysis
        print("🔍 Analyzing path structure...")
        path_analysis = self._analyze_path_structure(file, config, source_utils)

        # Report analysis findings
        if path_analysis["path_exists"]:
            print(f"✓ Path exists ({'file' if path_analysis['is_file'] else 'folder'})")
            if path_analysis["is_folder"]:
                print(f"📊 Contains: {path_analysis['total_files']} files, {path_analysis['total_folders']} folders")
                if path_analysis["data_files"]:
                    print(f"📄 Data files: {len(path_analysis['data_files'])} matching format")
                if path_analysis["metadata_files"]:
                    print(f"🏷️ Metadata files: {len(path_analysis['metadata_files'])} (_SUCCESS, .crc, etc.)")
                if path_analysis["has_spark_structure"]:
                    print("⚡ Detected Spark output structure")
                if path_analysis["file_extensions"]:
                    print(f"📝 File types: {', '.join(path_analysis['file_extensions'])}")
        else:
            print(f"❌ Error reading folder {file.file_path}")
            raise Exception(f"Unable to read data from folder: {file.file_path}")

        print(f"💡 Recommended approach: {path_analysis['recommended_approach']}")

        if config.source_is_folder:
            if path_analysis["total_folders"] > 0:
                print(
                    "🔄 Folder only contains subfolders. Need to determine if this is a nested series or just single level folder containing part files."
                )
                path_analysis["recommended_approach"] = "subfolder_processing"
                # Implement subfolder processing logic here
            elif path_analysis["total_folders"] == 0 and len(path_analysis["data_files"]) > 0:
                print("🔄 Using individual file union approach...")
                # Use the individual files and union them
            else:
                print(f"❌ Configuration source_is_folder is True, but no subfolders exist at {file.file_path}.")
                raise Exception(f"No subfolders found in folder: {file.file_path}")

        # Apply intelligent optimization based on analysis
        if path_analysis["recommended_approach"] == "individual_union" and path_analysis["data_files"]:
            print("🎯 Using optimized individual file reading approach")
            try:
                return self._read_individual_files_and_union(path_analysis["data_files"], config, options, source_utils)
            except Exception as opt_error:
                print(f"ℹ️ Optimization failed, falling back to standard Spark processing: {opt_error}")
                # Fall through to Spark fallback

        elif path_analysis["recommended_approach"] == "direct_read":
            print("🎯 Using direct file read approach")
            try:
                df = source_utils.read_file(
                    file_path=file.file_path,
                    file_format=config.source_file_format,
                    options=options,
                )
                print("✓ Successfully read file directly")
                return df
            except Exception as direct_error:
                print(f"ℹ️ Direct read failed, trying Spark approaches: {direct_error}")
                # Fall through to Spark fallback

        elif path_analysis["recommended_approach"] == "wildcard_pattern":
            print("🎯 Skipping direct read, using wildcard approach")
            wildcard_path = f"{file.file_path}/*"
            try:
                df = source_utils.read_file(
                    file_path=wildcard_path,
                    file_format=config.source_file_format,
                    options=options,
                )
                print(f"✓ Successfully read using wildcard pattern: {wildcard_path}")
                return df
            except Exception as wildcard_error:
                print(f"ℹ️ Wildcard read failed, trying individual files: {wildcard_error}")
                # Fall through to individual files approach

        elif path_analysis["recommended_approach"] == "subfolder_processing":
            print("🎯 Skipping direct read, using subfolder processing")
            try:
                if path_analysis["data_files"]:
                    data_files = path_analysis["data_files"]
                    print(f"📂 Using pre-analyzed data files ({len(data_files)} files)")
                else:
                    # Re-list files as last resort
                    file_list = source_utils.list_files(file.file_path, recursive=False)
                    if not file_list:
                        raise Exception(f"No data files found in folder: {file.file_path}")

                    data_files = [
                        f
                        for f in file_list
                        if not f.endswith("_SUCCESS")
                        and not f.endswith(".crc")
                        and not f.startswith(".")
                        and ("part-" in f or config.source_file_format.lower() in f.lower())
                    ]

                    if not data_files:
                        raise Exception(f"No data files found in folder: {file.file_path}")

                    print(f"📂 Found {len(data_files)} data files to process")

                # Read individual files and union them
                return self._read_individual_files_and_union(data_files, config, options, source_utils)
            except Exception as subfolder_error:
                print(f"ℹ️ Subfolder read failed, trying individual files: {subfolder_error}")
                # Fall through to individual files approach

        # Standard Spark processing with graceful fallbacks
        print("🔄 Using standard Spark processing chain...")
        try:
            # Try reading the folder directly first
            print(f"Attempting direct folder read: {file.file_path}")
            df = source_utils.read_file(
                file_path=file.file_path,
                file_format=config.source_file_format,
                options=options,
            )

            print("✓ Successfully read folder directly")
            return df

        except Exception:
            print("ℹ️ Direct folder read not supported, trying alternatives...")

            try:
                # Fallback 1: Try wildcard pattern
                wildcard_path = f"{file.file_path}/*"
                print(f"Attempting wildcard pattern: {wildcard_path}")
                df = source_utils.read_file(
                    file_path=wildcard_path,
                    file_format=config.source_file_format,
                    options=options,
                )
                print(f"✓ Successfully read using wildcard pattern: {wildcard_path}")
                return df

            except Exception:
                print("ℹ️ Wildcard pattern not supported, using individual file approach...")

                try:
                    # Fallback 2: Use analysis data or list files
                    if path_analysis["data_files"]:
                        data_files = path_analysis["data_files"]
                        print(f"📂 Using pre-analyzed data files ({len(data_files)} files)")
                    else:
                        # Re-list files as last resort
                        file_list = source_utils.list_files(file.file_path, recursive=False)
                        if not file_list:
                            raise Exception(f"No data files found in folder: {file.file_path}")

                        data_files = [
                            f
                            for f in file_list
                            if not f.endswith("_SUCCESS")
                            and not f.endswith(".crc")
                            and not f.startswith(".")
                            and ("part-" in f or config.source_file_format.lower() in f.lower())
                        ]

                        if not data_files:
                            raise Exception(f"No data files found in folder: {file.file_path}")

                        print(f"📂 Found {len(data_files)} data files to process")

                    # Read individual files and union them
                    return self._read_individual_files_and_union(data_files, config, options, source_utils)

                except Exception as fallback_error:
                    print(f"❌ Error reading folder {file.file_path}: {fallback_error}")
                    raise Exception(f"Unable to read data from folder: {file.file_path}. Error: {str(fallback_error)}")

    def _read_individual_files_and_union(
        self,
        data_files: list,
        config: FlatFileIngestionConfig,
        options: Dict[str, Any],
        source_utils,
    ) -> DataFrame:
        """Read individual files and union them (optimized approach for parquet folders)"""
        try:
            print(f"📊 Reading {len(data_files)} individual files...")

            # Read first file to get schema
            logging.debug(f"   Reading file 1/{len(data_files)}: {data_files[0].split('/')[-1]}")
            first_df = source_utils.read_file(
                file_path=data_files[0],
                file_format=config.source_file_format,
                options=options,
            )

            # If only one file, return it
            if len(data_files) == 1:
                print("✓ Successfully read single file")
                return first_df

            # Read remaining files and union them
            print(f"📊 Combining {len(data_files)} files...")
            all_dfs = [first_df]
            for i, file_path in enumerate(data_files[1:], 2):
                logging.debug(f"   Reading file {i}/{len(data_files)}: {file_path.split('/')[-1]}")
                df = source_utils.read_file(
                    file_path=file_path,
                    file_format=config.source_file_format,
                    options=options,
                )
                all_dfs.append(df)

            # Union all dataframes
            result_df = all_dfs[0]
            for df in all_dfs[1:]:
                result_df = result_df.union(df)

            print(f"✓ Successfully combined {len(data_files)} files into single DataFrame")
            return result_df

        except Exception as e:
            print(f"❌ Error reading individual files: {e}")
            raise Exception(f"Unable to read individual files. Error: {str(e)}")

    def write_data(self, data: DataFrame, config: FlatFileIngestionConfig) -> ProcessingMetrics:
        """Write data to target destination using dynamic target resolution"""
        write_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get target utilities for this configuration
            target_utils = self.location_resolver.get_target_utils(config, spark=self.spark)

            # Get target row count before write (for reconciliation)
            try:
                existing_df = target_utils.read_table(config.target_table_name)
                metrics.target_row_count_before = existing_df.count()
            except Exception:
                metrics.target_row_count_before = 0

            # Write data using dynamic target utilities
            target_utils.write_to_table(
                df=data,
                table_name=config.target_table_name,
                mode=config.write_mode,
                partition_by=config.partition_columns,
            )

            # Get target row count after write
            try:
                result_df = target_utils.read_table(config.target_table_name)
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
                metrics.records_inserted = metrics.target_row_count_after - metrics.target_row_count_before

            print(f"Wrote data to {config.target_table_name} in {metrics.write_duration_ms}ms")

            return ProcessingMetricsUtils.calculate_performance_metrics(metrics, config.write_mode)

        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            print(f"❌ Error writing data to {config.target_table_name}: {e}")
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
            import json
            from datetime import datetime

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
                    StructField("source_file_modified_time", TimestampType(), nullable=True),
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
                    StructField("row_count_reconciliation_status", StringType(), nullable=True),
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
                    StructField("execution_duration_seconds", IntegerType(), nullable=True),
                    StructField("spark_application_id", StringType(), nullable=True),
                    StructField("source_file_partition_cols", StringType(), nullable=True),
                    StructField("source_file_partition_values", StringType(), nullable=True),
                    StructField("date_partition", StringType(), nullable=True),
                    StructField("created_date", TimestampType(), nullable=False),
                    StructField("created_by", StringType(), nullable=False),
                ]
            )

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_flat_file_ingestion", mode="append")

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
            import json
            from datetime import datetime

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
                    int(metrics.total_duration_ms / 1000) if metrics.total_duration_ms else None,
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
                    StructField("source_file_modified_time", TimestampType(), nullable=True),
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
                    StructField("row_count_reconciliation_status", StringType(), nullable=True),
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
                    StructField("execution_duration_seconds", IntegerType(), nullable=True),
                    StructField("spark_application_id", StringType(), nullable=True),
                    StructField("source_file_partition_cols", StringType(), nullable=True),
                    StructField("source_file_partition_values", StringType(), nullable=True),
                    StructField("date_partition", StringType(), nullable=True),
                    StructField("created_date", TimestampType(), nullable=False),
                    StructField("created_by", StringType(), nullable=False),
                ]
            )

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_flat_file_ingestion", mode="append")

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
            import json
            from datetime import datetime

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
                    StructField("source_file_modified_time", TimestampType(), nullable=True),
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
                    StructField("row_count_reconciliation_status", StringType(), nullable=True),
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
                    StructField("execution_duration_seconds", IntegerType(), nullable=True),
                    StructField("spark_application_id", StringType(), nullable=True),
                    StructField("source_file_partition_cols", StringType(), nullable=True),
                    StructField("source_file_partition_values", StringType(), nullable=True),
                    StructField("date_partition", StringType(), nullable=True),
                    StructField("created_date", TimestampType(), nullable=False),
                    StructField("created_by", StringType(), nullable=False),
                ]
            )

            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            self.lakehouse_utils.write_to_table(df=log_df, table_name="log_flat_file_ingestion", mode="append")

        except Exception as e:
            print(f"⚠️ Failed to log execution error: {e}")


class PySparkFlatFileIngestionOrchestrator(FlatFileIngestionOrchestrator):
    """PySpark implementation of flat file ingestion orchestrator"""

    def process_configuration(self, config: FlatFileIngestionConfig, execution_id: str) -> Dict[str, Any]:
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
                print(f"⚠️ No source data found for {config.config_name}. Skipping write and reconciliation operations.")
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
                        self.logging_service.log_execution_start(config, execution_id, partition_info)

                    # Read file
                    df, read_metrics = self.processor_service.read_file(config, file_result)

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
                    result["errors"].append(f"Error processing file {file_result.file_path}: {file_error}")
                    print(f"❌ Error processing file {file_result.file_path}: {file_error}")

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
                result["metrics"] = ProcessingMetricsUtils.merge_metrics(all_metrics, config.write_mode)
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
                print(f"Target rows before: {result['metrics'].target_row_count_before}")
                print(f"Target rows after: {result['metrics'].target_row_count_after}")
                print(f"Row count reconciliation: {result['metrics'].row_count_reconciliation_status}")

                # Log completion
                self.logging_service.log_execution_completion(config, execution_id, result["metrics"], "completed")
            else:
                result["status"] = "no_data_processed"
                print(f"Processing completed in {time.time() - start_time:.2f} seconds (no data processed)")
                print("Row count reconciliation: skipped (no source data)")

        except Exception as e:
            result["status"] = "failed"
            result["errors"].append(str(e))
            error_details = ErrorHandlingUtils.format_error_details(
                e, {"config_id": config.config_id, "config_name": config.config_name}
            )
            self.logging_service.log_execution_error(config, execution_id, str(e), str(error_details))
            print(f"Error processing {config.config_name}: {e}")

        return result

    def process_configurations(self, configs: List[FlatFileIngestionConfig], execution_id: str) -> Dict[str, Any]:
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
        print(f"\n🔄 Processing {len(execution_groups)} execution groups: {execution_groups}")

        for group_num in execution_groups:
            group_configs = grouped_configs[group_num]
            print(f"\n=== Execution Group {group_num} ===")
            print(f"Processing {len(group_configs)} configurations in parallel")

            # Process configurations in this group in parallel
            group_results = self._process_execution_group_parallel(group_configs, execution_id, group_num)

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

            print(f"✓ Execution Group {group_num} completed")

        return results

    def _process_execution_group_parallel(
        self, configs: List[FlatFileIngestionConfig], execution_id: str, group_num: int
    ) -> List[Dict[str, Any]]:
        """Process configurations within an execution group in parallel"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = []

        if len(configs) == 1:
            # Single configuration - no need for parallel processing
            print(f"  📄 Processing single configuration: {configs[0].config_name}")
            result = self.process_configuration(configs[0], execution_id)
            return [result]

        # Multiple configurations - process in parallel
        max_workers = min(len(configs), 4)  # Limit concurrent threads
        print(f"  🔀 Using {max_workers} parallel workers for {len(configs)} configurations")

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"FlatFileGroup{group_num}") as executor:
            # Submit all configurations for processing
            future_to_config = {
                executor.submit(self._process_configuration_with_thread_info, config, execution_id): config
                for config in configs
            }

            # Collect results as they complete
            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"  ✓ Completed: {config.config_name} ({result['status']})")
                except Exception as e:
                    error_result = {
                        "config_id": config.config_id,
                        "config_name": config.config_name,
                        "status": "failed",
                        "metrics": ProcessingMetrics(),
                        "errors": [f"Thread execution error: {str(e)}"],
                    }
                    results.append(error_result)
                    print(f"  ❌ Failed: {config.config_name} - {str(e)}")

        return results

    def _process_configuration_with_thread_info(
        self, config: FlatFileIngestionConfig, execution_id: str
    ) -> Dict[str, Any]:
        """Process a single configuration with thread information"""
        import threading

        thread_name = threading.current_thread().name
        print(f"  🧵 [{thread_name}] Starting: {config.config_name}")

        try:
            result = self.process_configuration(config, execution_id)
            print(f"  🧵 [{thread_name}] Finished: {config.config_name} ({result['status']})")
            return result
        except Exception as e:
            print(f"  🧵 [{thread_name}] Error: {config.config_name} - {str(e)}")
            raise
