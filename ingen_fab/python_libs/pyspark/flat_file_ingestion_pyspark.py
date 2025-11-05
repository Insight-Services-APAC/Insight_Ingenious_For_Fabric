# PySpark-specific implementations for flat file ingestion
# Uses PySpark DataFrame API and lakehouse_utils

import logging
import os
import time
import uuid
from dataclasses import dataclass
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
    ConfigExecutionResult,
    DuplicateFilesError,
    FileDiscoveryResult,
    FlatFileDiscoveryInterface,
    FlatFileIngestionConfig,
    FlatFileIngestionOrchestrator,
    FlatFileLoggingInterface,
    FlatFileProcessorInterface,
    ProcessingMetrics,
)

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
    """Logger adapter that prepends config group and config name to messages"""
    def process(self, msg, kwargs):
        config_group = self.extra.get('config_group_id', '')
        config_name = self.extra.get('config_name', '')

        prefix_parts = []
        if config_group:
            prefix_parts.append(f'Group: {config_group}')
        if config_name:
            prefix_parts.append(config_name)

        if prefix_parts:
            prefix = '[' + '] ['.join(prefix_parts) + ']'
            return f'{prefix} {msg}', kwargs
        return msg, kwargs


class PySparkFlatFileDiscovery(FlatFileDiscoveryInterface):
    """PySpark implementation of file discovery using dynamic location resolution"""

    def __init__(self, spark, location_resolver, config: FlatFileIngestionConfig):
        self.spark = spark
        self.location_resolver = location_resolver
        self.config = config
        self.logger = ConfigLoggerAdapter(logger, {
            'config_name': self.config.config_id,
            'config_group_id': self.config.config_group_id
        })
        self.import_pattern_discovery_methods = {
            "single_file": self._discover_single_file,
            "incremental_files": self._discover_incremental_files,
            "incremental_folders": self._discover_incremental_folders,
        }
        self.last_duplicate_items = []

    def discover_files(self) -> List[FileDiscoveryResult]:
        """Discover files based on configuration using dynamic source resolution"""
        full_source_path = self.location_resolver.resolve_full_source_path(self.config)
        strategy = self.import_pattern_discovery_methods.get(self.config.import_pattern)

        if strategy:
            return strategy(full_source_path)
        else:
            self.logger.warning(f"Unknown import pattern: {self.config.import_pattern}")
            return []

    def _discover_single_file(
        self, file_path: str
    ) -> List[FileDiscoveryResult]:
        """Discover a single file with metadata

        Args:
            file_path: Relative path from resolve_full_source_path()
        """
        try:
            source_utils = self.location_resolver.get_source_utils(
                self.config, spark=self.spark
            )

            file_info = source_utils.get_file_info(file_path)

            if hasattr(source_utils, 'lakehouse_files_uri'):
                abfss_prefix = source_utils.lakehouse_files_uri()
                full_file_path = f"{abfss_prefix}{file_path.lstrip('/')}"
            else:
                # Fallback for non-lakehouse sources (e.g., warehouse)
                full_file_path = file_path

            result = FileDiscoveryResult(
                file_path=full_file_path,
                size=file_info.get("size", 0),
                modified_time=file_info.get("modified_time"),
            )

            self.logger.info(f"Single file: {file_path} ({result.size / (1024*1024):.2f} MB)")
            return [result]

        except Exception as e:
            self.logger.error(f"Could not get metadata for {file_path}: {e}")
            raise

    def _discover_incremental_files(
        self, base_path: str
    ) -> List[FileDiscoveryResult]:
        """Discover files incrementally with optional pattern filtering

        This is the incremental_files import pattern. It:
        1. Lists all files in the source directory
        2. Applies discovery_pattern filter if provided (default: *)
        3. Filters to unprocessed files based on log table
        4. Handles duplicate detection based on duplicate_handling setting
        5. Optional date extraction and filtering if date_pattern provided
        """

        try:
            source_utils = self.location_resolver.get_source_utils(
                self.config, spark=self.spark
            )

            file_infos = source_utils.list_files_with_metadata(
                directory_path=base_path,
                pattern=self.config.discovery_pattern or "*",
                recursive=False,
            )

            # Filter to unprocessed files and detect duplicates
            unprocessed_files, duplicate_files = self._get_unprocessed_items(file_infos)

            # Store duplicates for orchestrator to log
            self.last_duplicate_items = duplicate_files

            discovered_files = []
            for file_info in unprocessed_files:
                # Extract filename for date extraction
                filename = os.path.basename(file_info.path)

                # Extract date from filename if date_pattern configured
                date_partition = None
                if self.config.date_pattern:
                    date_partition = self._extract_date_from_name(filename, self.config.date_pattern)

                file_result = FileDiscoveryResult(
                    file_path=file_info.path,
                    size=file_info.size,
                    modified_time=file_info.modified,
                    date_partition=date_partition,
                )

                discovered_files.append(file_result)

            # Sort by date if date_pattern configured, otherwise by modified time
            if self.config.date_pattern and discovered_files:
                self.logger.info("Sorting files chronologically by extracted date")
                discovered_files.sort(key=lambda x: x.date_partition or "")
            else:
                # Sort by modified time to process oldest files first
                discovered_files.sort(key=lambda x: x.modified_time or "")

            # Apply control file filter if required
            if self.config.require_control_file and self.config.control_file_pattern:
                discovered_files, missing_control = self._filter_by_control_files(discovered_files)
                if missing_control:
                    self.logger.info(f"Skipped {len(missing_control)} file(s) without control files")

            return discovered_files

        except Exception as e:
            self.logger.error(f"File discovery failed: {e}")
            raise

    def _get_unprocessed_items(
        self,
        item_infos: list,
    ) -> Tuple[list, list]:
        """Return unprocessed items (files/folders) and duplicates

        Two-step filtering process:
        1. Filter by modified time: Only consider items newer than last run
        2. Check for duplicates: Among newer items, identify duplicates
        3. Optional: Filter by date range if date_pattern configured

        Args:
            item_infos: List of discovered item info objects (files or folders)

        Returns:
            Tuple[list, list]: (unprocessed_items, duplicate_items)

        Raises:
            DuplicateFilesError: If duplicate_handling='fail' and duplicates found
        """
        if not item_infos:
            return [], []

        import os

        # STEP 1: Filter by modified time - only consider newer items
        latest_time_result = self.spark.sql(
            f"""
            SELECT MAX(source_file_modified_time) as latest_time
            FROM log_file_load
            WHERE config_id = '{self.config.config_id}'
            AND status = 'completed'
            AND source_file_modified_time IS NOT NULL
            """
        ).collect()

        if latest_time_result and latest_time_result[0].latest_time:
            latest_processed_ms = int(latest_time_result[0].latest_time.timestamp() * 1000)
            newer_items = [f for f in item_infos if f.modified_ms > latest_processed_ms]
            self.logger.info(f"Found {len(newer_items)} item(s) newer than last run")
        else:
            self.logger.info("No previous runs found, processing all items")
            newer_items = item_infos

        if not newer_items:
            return [], []

        # STEP 2: Among newer items, check for duplicate names
        discovered_data = [
            (os.path.basename(f.path), f.path, f.size, f.modified_ms) for f in newer_items
        ]

        discovered_df = self.spark.createDataFrame(
            discovered_data, ["item_name", "item_path", "size", "modified_ms"]
        )

        # STEP 3: Query log table to get most recent status per item
        # Note: With merge pattern, there's only one row per load_id, but we still use
        # ROW_NUMBER to handle edge cases and maintain query compatibility
        processed_df = self.spark.sql(
            f"""
            WITH latest_logs AS (
                SELECT element_at(split(source_file_path, '/'), -1) as item_name,
                        status,
                        ROW_NUMBER() OVER (PARTITION BY source_file_path
                                            ORDER BY started_at DESC) as rn
                FROM log_file_load
                WHERE config_id = '{self.config.config_id}'
            )
            SELECT DISTINCT item_name
            FROM latest_logs
            WHERE rn = 1
            AND status = 'completed'
            """
        )

        # INNER JOIN: Find duplicates (newer items with matching name)
        duplicates_df = discovered_df.join(processed_df, on="item_name", how="inner")
        duplicate_names = {row.item_name for row in duplicates_df.collect()}

        # LEFT ANTI JOIN: Find unprocessed (newer items without matching name)
        unprocessed_df = discovered_df.join(processed_df, on="item_name", how="left_anti")
        unprocessed_names = {row.item_name for row in unprocessed_df.collect()}

        # Separate the newer_items into duplicates and unprocessed
        duplicate_items = [
            f for f in newer_items if os.path.basename(f.path) in duplicate_names
        ]
        unprocessed_items = [
            f for f in newer_items if os.path.basename(f.path) in unprocessed_names
        ]

        # STEP 4: Apply date filtering if date_pattern configured
        if self.config.date_pattern and self.config.date_range_start:
            unprocessed_items = self._filter_by_date_range(unprocessed_items)
            self.logger.info(
                f"After date filtering: {len(unprocessed_items)} item(s) in range"
            )

        # Handle based on duplicate_handling setting
        if self.config.duplicate_handling == "allow":
            # Process all items including duplicates as normal
            all_items = unprocessed_items + duplicate_items
            if duplicate_items:
                self.logger.info(
                    f"Found {len(all_items)} item(s) to ingest "
                    f"(including {len(duplicate_items)} duplicates)"
                )
            else:
                self.logger.info(f"Found {len(all_items)} unprocessed item(s) to ingest")
            return all_items, []
        elif self.config.duplicate_handling == "fail":
            if duplicate_items:
                dup_names = [os.path.basename(f.path) for f in duplicate_items]
                error_msg = f"Duplicate items detected: {', '.join(dup_names)}"
                self.logger.error(error_msg)
                raise DuplicateFilesError(error_msg)
            self.logger.info(f"Found {len(unprocessed_items)} unprocessed item(s) to ingest")
            return unprocessed_items, []
        else:  # "skip" mode
            if duplicate_items:
                self.logger.info(f"Skipping {len(duplicate_items)} duplicate item(s)")
            self.logger.info(f"Found {len(unprocessed_items)} unprocessed item(s) to ingest")
            return unprocessed_items, duplicate_items

    def _discover_incremental_folders(
        self, base_path: str
    ) -> List[FileDiscoveryResult]:
        """Discover folders incrementally with optional pattern filtering

        This is the incremental_folders import pattern. It:
        1. Lists all folders in the source directory
        2. Applies discovery_pattern filter if provided (default: *)
        3. Filters to unprocessed folders based on log table
        4. Handles duplicate detection based on duplicate_handling setting
        5. Optional date extraction and filtering if date_pattern provided
        6. Each discovered folder represents a batch of files to be processed together
        """

        try:
            source_utils = self.location_resolver.get_source_utils(
                self.config, spark=self.spark
            )

            self.logger.info(f"Pattern: {self.config.discovery_pattern or '*'}")

            # Detect hierarchical patterns (e.g., "*/*/*" for 2025/07/01)
            is_hierarchical = self.config.discovery_pattern and "/" in self.config.discovery_pattern

            if is_hierarchical:
                # For hierarchical patterns, list recursively and match full relative paths
                # Calculate expected depth from pattern
                expected_depth = self.config.discovery_pattern.count("/") + 1

                # List all directories recursively
                all_folder_paths = source_utils.list_directories(
                    base_path,
                    recursive=True
                )

                # Filter to folders at the right depth and matching pattern
                folder_paths = []
                for folder_path in all_folder_paths:
                    # Get relative path from base_path
                    # folder_path is full ABFSS URI, base_path is relative path
                    # We need to extract the relative portion after base_path in the URI

                    normalized_base = base_path.rstrip("/")
                    normalized_folder = folder_path.rstrip("/")

                    # Find where base_path appears in the full URI
                    if f"/{normalized_base}/" in normalized_folder:
                        # Extract everything after base_path
                        relative_path = normalized_folder.split(f"/{normalized_base}/", 1)[1]
                    elif normalized_folder.endswith(f"/{normalized_base}"):
                        # folder_path is exactly the base path
                        continue
                    else:
                        # Fallback for edge cases
                        self.logger.debug(f"Could not extract relative path for {folder_path}")
                        continue

                    if not relative_path:
                        continue

                    # Check depth matches
                    folder_depth = relative_path.count("/") + 1 if relative_path else 0

                    self.logger.debug(
                        f"Folder: {folder_path}, Relative: {relative_path}, Depth: {folder_depth}"
                    )

                    if folder_depth != expected_depth:
                        continue

                    # Match full relative path against pattern
                    if FilePatternUtils.matches_pattern(relative_path, self.config.discovery_pattern):
                        folder_paths.append(folder_path)
                        self.logger.debug(f"Matched: {relative_path}")
            else:
                # For simple patterns, list immediate subdirectories only
                folder_paths = source_utils.list_directories(
                    base_path,
                    recursive=False
                )

                # Apply discovery pattern if specified
                if self.config.discovery_pattern and self.config.discovery_pattern != "*":
                    filtered_folders = []
                    for folder_path in folder_paths:
                        folder_name = os.path.basename(folder_path.rstrip("/"))
                        if FilePatternUtils.matches_pattern(folder_name, self.config.discovery_pattern):
                            filtered_folders.append(folder_path)
                    folder_paths = filtered_folders
                    self.logger.info(f"After pattern filtering: {len(folder_paths)} folder(s)")

            # Get metadata for each folder (list files to get modified times)
            folder_infos = []
            for folder_path in folder_paths:
                try:
                    # Get files in folder to determine latest modified time
                    files_in_folder = source_utils.list_files_with_metadata(
                        directory_path=folder_path,
                        pattern="*",
                        recursive=True,
                    )

                    if files_in_folder:
                        # Use latest file modified time as folder modified time
                        latest_modified_ms = max(f.modified_ms for f in files_in_folder)
                        folder_size = sum(f.size for f in files_in_folder)

                        # Create a folder info object with path and metadata
                        from types import SimpleNamespace
                        folder_info = SimpleNamespace(
                            path=folder_path,
                            modified_ms=latest_modified_ms,
                            size=folder_size
                        )
                        folder_infos.append(folder_info)
                    else:
                        self.logger.warning(f"Folder {folder_path} is empty, skipping")

                except Exception as e:
                    self.logger.warning(f"Could not get metadata for folder {folder_path}: {e}")
                    continue

            # Filter to unprocessed folders and detect duplicates
            unprocessed_folders, duplicate_folders = self._get_unprocessed_items(folder_infos)

            # Store duplicates for orchestrator to log
            self.last_duplicate_items = duplicate_folders

            # Build FileDiscoveryResult objects
            discovered_folders = []
            for folder_info in unprocessed_folders:
                from datetime import datetime
                # For hierarchical patterns, use relative path; for simple patterns, use folder name
                if is_hierarchical:
                    # Get relative path for hierarchical date extraction (e.g., "2025/07/01")
                    relative_path = folder_info.path.replace(base_path, "").strip("/")
                    extraction_key = relative_path
                    folder_name = relative_path.replace("/", "_")  # Use full path as folder identifier
                else:
                    # Extract just the folder name for simple patterns (e.g., "batch_001")
                    folder_name = os.path.basename(folder_info.path.rstrip("/"))
                    extraction_key = folder_name

                # Extract date from folder name/path if date_pattern configured
                date_partition = None
                if self.config.date_pattern:
                    date_partition = self._extract_date_from_name(extraction_key, self.config.date_pattern)

                # Convert modified_ms to datetime for proper duplicate detection
                modified_time = None
                if hasattr(folder_info, 'modified_ms') and folder_info.modified_ms:
                    modified_time = datetime.fromtimestamp(folder_info.modified_ms / 1000)

                folder_result = FileDiscoveryResult(
                    file_path=folder_info.path,
                    size=folder_info.size,
                    modified_time=modified_time,
                    date_partition=date_partition,
                    folder_name=folder_name,
                )

                discovered_folders.append(folder_result)

            # Sort by date if date_pattern configured, otherwise by modified time
            if self.config.date_pattern and discovered_folders:
                self.logger.info("Sorting folders chronologically by extracted date")
                discovered_folders.sort(key=lambda x: x.date_partition or "")
            else:
                # Sort by modified time to process oldest folders first
                discovered_folders.sort(key=lambda x: x.modified_time or "")

            # Apply control file filter if required
            if self.config.require_control_file and self.config.control_file_pattern:
                discovered_folders, missing_control = self._filter_by_control_files(discovered_folders)
                if missing_control:
                    self.logger.info(f"Skipped {len(missing_control)} folder(s) without control files")

            return discovered_folders

        except Exception as e:
            self.logger.error(f"Folder discovery failed: {e}")
            raise

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

    def date_already_processed(self, date_partition: str) -> bool:
        """Check if a date has already been processed (for skip_existing_dates feature)"""
        try:
            # Query the log table to see if this date has been successfully processed
            log_df = self.spark.sql(
                f"""
                SELECT DISTINCT date_partition, status
                FROM log_file_load
                WHERE config_id = '{self.config.config_id}'
                AND date_partition = '{date_partition}'
                AND status = 'completed'
                LIMIT 1
            """
            )

            return log_df.count() > 0

        except Exception as e:
            self.logger.warning(f"Could not check if date {date_partition} already processed: {e}")
            # If we can't check, assume not processed to be safe
            return False

    def extract_date_from_folder_name(
        self, folder_name: str, date_format: str
    ) -> Optional[str]:
        """Extract date from folder name based on format (required by interface)"""
        return DatePartitionUtils.extract_date_from_folder_name(folder_name, date_format)

    def _extract_date_from_name(self, name: str, date_pattern: str) -> Optional[str]:
        """Extract date from filename or folder name based on date_pattern

        Args:
            name: The filename or folder name to extract date from
            date_pattern: Date format pattern (e.g., 'YYYYMMDD', 'YYYY-MM-DD', 'YYYY/MM/DD')

        Returns:
            Extracted date in YYYY-MM-DD format, or None if not found
        """
        return DatePartitionUtils.extract_date_from_folder_name(name, date_pattern)

    def _filter_by_date_range(
        self, items: list
    ) -> list:
        """Filter items by date range if date_pattern and date_range_start configured

        Args:
            items: List of items (files or folders with .path attribute)

        Returns:
            Filtered list of items within the date range
        """
        if not self.config.date_pattern:
            return items


        # Extract dates and filter
        filtered_items = []
        for item in items:
            item_name = os.path.basename(item.path)
            date_str = self._extract_date_from_name(item_name, self.config.date_pattern)

            # Check if date is in range
            if date_str:
                # Use empty string for missing date range values to let is_date_in_range handle them
                start_date = self.config.date_range_start or ""
                end_date = self.config.date_range_end or ""

                if DatePartitionUtils.is_date_in_range(date_str, start_date, end_date):
                    filtered_items.append(item)
                else:
                    self.logger.debug(
                        f"Skipping {item_name}: date {date_str} outside range "
                        f"[{start_date or 'unbounded'} to {end_date or 'unbounded'}]"
                    )
            else:
                # If we can't extract a date but date filtering is configured, skip the item
                self.logger.debug(
                    f"Skipping {item_name}: no date found matching pattern {self.config.date_pattern}"
                )

        return filtered_items

    def _resolve_control_file_pattern(
        self, item_path: str, pattern: str
    ) -> str:
        """Resolve control file pattern with {basename} substitution

        Args:
            item_path: Full path to data file/folder
            pattern: Pattern string (e.g., "{basename}.CTL", "_SUCCESS")

        Returns:
            Expected control file name

        Examples:
            - data.csv + "{basename}.CTL" → data.CTL
            - sales.xlsx + "{basename}.SUCCESS" → sales.SUCCESS
            - any file + "_SUCCESS" → _SUCCESS (fixed name)
        """
        import os

        # If pattern doesn't contain {basename}, return as-is (fixed name like "_SUCCESS")
        if "{basename}" not in pattern:
            return pattern

        # Extract filename from path
        filename = os.path.basename(item_path.rstrip("/"))

        # Get basename (filename without extension)
        basename = os.path.splitext(filename)[0]

        # Replace {basename} with actual basename
        return pattern.replace("{basename}", basename)

    def _filter_by_control_files(
        self,
        items: list,
    ) -> Tuple[list, list]:
        """Filter items to only those with corresponding control files

        Args:
            items: List of file/folder info objects

        Returns:
            Tuple[list, list]: (items_with_control, items_without_control)

        Logic:
        - Auto-detects mode from pattern:
          - Pattern contains {basename} → per-file mode (each item needs own control file)
          - Pattern is fixed (e.g., "_SUCCESS") → per-folder mode (shared control file)
        """

        if not items:
            return [], []

        import os

        # Get source utilities for checking file existence
        source_utils = self.location_resolver.get_source_utils(
            self.config, spark=self.spark
        )

        items_with_control = []
        items_without_control = []

        # Auto-detect mode: if pattern contains {basename}, use per-file mode
        is_per_file_mode = "{basename}" in self.config.control_file_pattern

        if not is_per_file_mode:
            # Per-folder mode: check for single control file in parent directory
            # Group items by directory
            from collections import defaultdict
            items_by_directory = defaultdict(list)

            for item in items:
                directory = os.path.dirname(item.path.rstrip("/"))
                items_by_directory[directory].append(item)

            # Check each directory for control file
            for directory, dir_items in items_by_directory.items():
                control_file_name = self._resolve_control_file_pattern(
                    directory, self.config.control_file_pattern
                )
                control_file_path = f"{directory}/{control_file_name}".replace("//", "/")

                # Check if control file exists
                try:
                    if source_utils.file_exists(control_file_path):
                        # All items in this directory have control file
                        for item in dir_items:
                            items_with_control.append(item)
                        self.logger.debug(
                            f"Found control file {control_file_name} for {len(dir_items)} item(s) in {directory}"
                        )
                    else:
                        items_without_control.extend(dir_items)
                        self.logger.debug(
                            f"No control file {control_file_name} found in {directory}"
                        )
                except Exception as e:
                    self.logger.debug(f"Error checking control file {control_file_path}: {e}")
                    items_without_control.extend(dir_items)
        else:
            # Per-file mode: each item needs its own control file
            for item in items:
                # Get directory containing the item
                directory = os.path.dirname(item.path.rstrip("/"))

                # Resolve control file name based on item
                control_file_name = self._resolve_control_file_pattern(
                    item.path, self.config.control_file_pattern
                )
                control_file_path = f"{directory}/{control_file_name}".replace("//", "/")

                # Check if control file exists
                try:
                    if source_utils.file_exists(control_file_path):
                        items_with_control.append(item)
                        self.logger.debug(f"Found control file {control_file_name} for {os.path.basename(item.path)}")
                    else:
                        items_without_control.append(item)
                        self.logger.debug(f"No control file {control_file_name} for {os.path.basename(item.path)}")
                except Exception as e:
                    self.logger.debug(f"Error checking control file {control_file_path}: {e}")
                    items_without_control.append(item)

        return items_with_control, items_without_control


class PySparkFlatFileProcessor(FlatFileProcessorInterface):
    """PySpark implementation of flat file processing using dynamic location resolution"""

    def __init__(self, spark_session: SparkSession, location_resolver, config: FlatFileIngestionConfig):
        self.spark = spark_session
        self.location_resolver = location_resolver
        self.config = config
        self.logger = ConfigLoggerAdapter(logger, {
            'config_name': self.config.config_id,
            'config_group_id': self.config.config_group_id
        })

    def read_file(self, file: FileDiscoveryResult) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read a file based on configuration and return DataFrame with metrics"""
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get source utilities for this configuration
            source_utils = self.location_resolver.get_source_utils(
                self.config, spark=self.spark
            )

            # Get file reading options based on configuration
            options = ConfigurationUtils.get_file_read_options(self.config)

            # Add column mismatch detection for CSV files with validation rules
            validation_rules = self._parse_validation_rules()
            enable_column_mismatch_check = (
                self.config.source_file_format.lower() == "csv"
                and "column_mismatch" in validation_rules
            )

            if enable_column_mismatch_check:
                # Column mismatch detection requires explicit schema
                if self.config.custom_schema_json:
                    # Parse custom schema and add corrupt record column
                    base_schema = self._parse_custom_schema_json(self.config.custom_schema_json)
                    schema_with_corrupt = self._add_corrupt_record_column(base_schema)

                    # Configure for PERMISSIVE mode with explicit schema
                    options["schema"] = schema_with_corrupt
                    options["columnNameOfCorruptRecord"] = "_raw_corrupt_record"
                    options["mode"] = "PERMISSIVE"
                    options.pop("inferSchema", None)  # Ensure inferSchema is disabled

                    self.logger.info("Column mismatch detection enabled with provided schema")
                else:
                    # Schema inference is enabled - cannot support corrupt record detection
                    self.logger.warning(
                        "Column mismatch validation configured but custom_schema_json not provided. "
                        "Skipping column mismatch detection (requires explicit schema). "
                        "To enable, provide custom_schema_json in config."
                    )
                    enable_column_mismatch_check = False  # Disable the check

            # Read file using dynamic source utilities
            # For incremental_folders pattern, read entire folder contents
            if self.config.import_pattern == "incremental_folders":
                df = self._read_folder_contents(file, options, source_utils)
            else:
                # For single_file and incremental_files, read individual file
                df = source_utils.read_file(
                    file_path=file.file_path,
                    file_format=self.config.source_file_format,
                    options=options,
                )

            # Check for column mismatches if configured
            if enable_column_mismatch_check:
                # Cache DataFrame before querying _raw_corrupt_record column
                # Required by Spark 2.3+ when querying corrupt record column
                df = df.cache()
                df, corrupt_count = self._check_column_mismatch_thresholds(df)
                metrics.corrupt_records_count = corrupt_count

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            self.logger.info(
                f"Read {metrics.source_row_count} records from source file in {metrics.read_duration_ms}ms"
            )

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            raise

    def batch_read_files(
        self, file_results: List[FileDiscoveryResult]
    ) -> Tuple[DataFrame, ProcessingMetrics]:
        """Batch read multiple files at once using Spark's native multi-file reading

        This is much more efficient than looping through files individually.
        Spark can read multiple files in parallel and optimize the read operation.
        """
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get file reading options
            options = ConfigurationUtils.get_file_read_options(self.config)

            # Extract file paths - these should already be full ABFSS URIs
            file_paths = [f.file_path for f in file_results]

            self.logger.info(f"Batch reading {len(file_paths)} files in a single operation")

            # Use Spark reader directly with list of paths for batch reading
            reader = self.spark.read.format(self.config.source_file_format.lower())

            # Apply custom schema if provided
            if "schema" in options:
                reader = reader.schema(options["schema"])

            # Apply options based on file format
            if self.config.source_file_format.lower() == "csv":
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

            self.logger.info(
                f"Batch read {metrics.source_row_count} records from {len(file_paths)} files in {metrics.read_duration_ms}ms"
            )

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            raise

    def read_folder(self, folder_path: str) -> Tuple[DataFrame, ProcessingMetrics]:
        """Read all files in a folder based on configuration"""
        # Create a FileDiscoveryResult for the folder
        folder_result = FileDiscoveryResult(
            file_path=folder_path,
            folder_name=os.path.basename(folder_path.rstrip("/"))
        )
        return self.read_file(folder_result)

    def _parse_validation_rules(self) -> dict:
        """Parse data_validation_rules JSON from config"""
        import json
        if not self.config.data_validation_rules:
            return {}
        try:
            return json.loads(self.config.data_validation_rules)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in data_validation_rules: {self.config.data_validation_rules}")
            return {}

    def _parse_custom_schema_json(self, schema_json: str):
        """Parse custom schema JSON to StructType"""
        import json
        from pyspark.sql.types import StructType

        try:
            schema_dict = json.loads(schema_json)
            return StructType.fromJson(schema_dict)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in custom_schema_json: {e}")
        except Exception as e:
            raise ValueError(f"Failed to parse custom schema: {e}")

    def _add_corrupt_record_column(self, schema):
        """Add _raw_corrupt_record column to schema for PERMISSIVE mode"""
        from pyspark.sql.types import StructType, StructField, StringType

        return StructType(
            schema.fields + [StructField("_raw_corrupt_record", StringType(), True)]
        )

    def _check_column_mismatch_thresholds(
        self,
        df: DataFrame,
    ) -> Tuple[DataFrame, int]:
        """Check for column mismatches and apply thresholds

        Returns: (filtered_df, corrupt_count)
        """
        from pyspark.sql.functions import col


        # Safety check: column should exist if this method is called
        if "_raw_corrupt_record" not in df.columns:
            self.logger.warning("_raw_corrupt_record column not found, skipping validation")
            return df, 0

        # Calculate statistics
        total_rows = df.count()
        corrupt_count = df.filter(col("_raw_corrupt_record").isNotNull()).count()

        if corrupt_count == 0:
            # No corrupt records - drop the column and return
            return df.drop("_raw_corrupt_record"), 0

        corrupt_pct = (corrupt_count / total_rows * 100) if total_rows > 0 else 0

        # Parse validation rules
        validation_rules = self._parse_validation_rules()
        column_mismatch_rules = validation_rules.get("column_mismatch", {})
        max_rows = column_mismatch_rules.get("max_rows")
        max_percentage = column_mismatch_rules.get("max_percentage")

        # Check thresholds (OR logic)
        threshold_exceeded = False
        failure_reason = None

        if max_rows is not None and corrupt_count > max_rows:
            threshold_exceeded = True
            failure_reason = f"Column mismatch threshold exceeded: {corrupt_count} rows > {max_rows} max_rows"

        if max_percentage is not None and corrupt_pct > max_percentage:
            threshold_exceeded = True
            if failure_reason:
                failure_reason += f" AND {corrupt_pct:.2f}% > {max_percentage}% max_percentage"
            else:
                failure_reason = f"Column mismatch threshold exceeded: {corrupt_pct:.2f}% > {max_percentage}% max_percentage"

        # Log and handle based on threshold check
        if threshold_exceeded:
            self.logger.error(failure_reason)
            raise Exception(failure_reason)
        elif max_rows is not None or max_percentage is not None:
            # Build threshold description for logging
            threshold_parts = []
            if max_rows is not None:
                threshold_parts.append(f"max_rows={max_rows}")
            if max_percentage is not None:
                threshold_parts.append(f"max_percentage={max_percentage}%")
            threshold_desc = ", ".join(threshold_parts)

            self.logger.info(
                f"Found {corrupt_count} corrupt rows ({corrupt_pct:.2f}%), within thresholds ({threshold_desc}) - processing {total_rows - corrupt_count} valid rows"
            )
        else:
            self.logger.warning(
                f"Found {corrupt_count} corrupt rows ({corrupt_pct:.2f}%), no thresholds configured - processing {total_rows - corrupt_count} valid rows"
            )

        # Filter to valid rows only
        valid_df = df.filter(col("_raw_corrupt_record").isNull()).drop("_raw_corrupt_record")
        return valid_df, corrupt_count

    def _analyze_path_structure(
        self, file: FileDiscoveryResult, source_utils
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
                all_directories = source_utils.list_directories(
                    directory_path=file.file_path, recursive=False
                )
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
                    if (
                        item.endswith("_SUCCESS")
                        or item.endswith(".crc")
                        or item.startswith(".")
                    ):
                        analysis["metadata_files"].append(item)
                    elif (
                        "part-" in item
                        or self.config.source_file_format.lower() in item.lower()
                    ):
                        analysis["data_files"].append(item)
                    else:
                        analysis["other_files"].append(item)

                for dir_item in all_directories:
                    analysis["subfolders"].append(dir_item)

                # Check for Spark-style output structure
                if any(
                    "part-" in f for f in analysis["data_files"]
                ) and "_SUCCESS" in str(analysis["metadata_files"]):
                    analysis["has_spark_structure"] = True

                # Determine recommended approach based on analysis
                if (
                    len(analysis["data_files"]) == 0
                    and len(analysis["subfolders"]) >= 0
                ):
                    analysis["recommended_approach"] = (
                        "subfolder_processing"  # Process subfolders individually
                    )
                if len(analysis["data_files"]) == 0:
                    if len(analysis["other_files"]) > 0:
                        analysis["recommended_approach"] = (
                            "wildcard_pattern"  # Let Spark try to read other files
                        )
                    else:
                        analysis["recommended_approach"] = (
                            "spark_fallback"  # No obvious data files
                        )
                elif len(analysis["data_files"]) == 1:
                    analysis["recommended_approach"] = "direct_read"
                elif (
                    self.config.source_file_format.lower() == "parquet"
                    and len(analysis["data_files"]) > 1
                ):
                    analysis["recommended_approach"] = (
                        "individual_union"  # Avoid schema warnings
                    )
                elif analysis["has_spark_structure"]:
                    analysis["recommended_approach"] = (
                        "individual_union"  # Handle Spark output properly
                    )
                elif len(analysis["data_files"]) <= 5:
                    analysis["recommended_approach"] = (
                        "individual_union"  # Small number, union is efficient
                    )
                else:
                    analysis["recommended_approach"] = (
                        "wildcard_pattern"  # Large number, let Spark handle
                    )

            except Exception as list_error:
                logger.info(f"Cannot analyze folder contents: {list_error}")
                analysis["recommended_approach"] = "spark_fallback"

        except Exception as analysis_error:
            logger.info(f"Path analysis failed: {analysis_error}")
            analysis["recommended_approach"] = "spark_fallback"

        return analysis

    def _read_folder_contents(
        self,
        file: FileDiscoveryResult,
        options: Dict[str, Any],
        source_utils,
    ) -> DataFrame:
        """Read all files in a folder - Spark reads all files from folder path"""

        # For folders, just read directly - Spark will handle all files in the folder
        # This works for both flat folders and hierarchical structures
        try:
            df = source_utils.read_file(
                file_path=file.file_path,
                file_format=self.config.source_file_format,
                options=options,
            )
            return df
        except Exception as e:
            self.logger.error(f"Failed to read folder: {e}")
            raise Exception(f"Unable to read data from folder: {file.file_path}")

    def _read_individual_files_and_union(
        self,
        data_files: list,
        options: Dict[str, Any],
        source_utils,
    ) -> DataFrame:
        """Read individual files and union them (optimized approach for parquet folders)"""

        try:
            self.logger.info(f"Reading {len(data_files)} individual files...")

            # Read first file to get schema
            logging.debug(
                f"   Reading file 1/{len(data_files)}: {data_files[0].split('/')[-1]}"
            )
            first_df = source_utils.read_file(
                file_path=data_files[0],
                file_format=self.config.source_file_format,
                options=options,
            )

            # If only one file, return it
            if len(data_files) == 1:
                self.logger.info("Successfully read single file")
                return first_df

            # Read remaining files and union them
            self.logger.info(f"Combining {len(data_files)} files...")
            all_dfs = [first_df]
            for i, file_path in enumerate(data_files[1:], 2):
                logging.debug(
                    f"   Reading file {i}/{len(data_files)}: {file_path.split('/')[-1]}"
                )
                df = source_utils.read_file(
                    file_path=file_path,
                    file_format=self.config.source_file_format,
                    options=options,
                )
                all_dfs.append(df)

            # Union all dataframes
            result_df = all_dfs[0]
            for df in all_dfs[1:]:
                result_df = result_df.union(df)

            self.logger.info(
                f"Successfully combined {len(data_files)} files into single DataFrame"
            )
            return result_df

        except Exception as e:
            self.logger.error(f"Error reading individual files: {e}")
            raise Exception(f"Unable to read individual files. Error: {str(e)}")

    def _add_ingestion_metadata(
        self,
        df: DataFrame,
        load_id: str,
        file_result: FileDiscoveryResult,
    ) -> DataFrame:
        """Add ingestion metadata columns to DataFrame including source file attributes and load_id"""
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

    def _execute_delta_merge(
        self,
        source_df: DataFrame,
        target_utils,
        write_start: float,
    ) -> ProcessingMetrics:
        """Execute Delta Lake merge operation with full metrics tracking

        Merge Behavior:
        - NOT matched: Insert all columns (new record)
        - Matched: Update all columns EXCEPT _raw_created_at (preserved from target)

        This ensures _raw_created_at is immutable while _raw_updated_at,
        _raw_filename, and business columns reflect the latest source data.
        """

        metrics = ProcessingMetrics()

        try:
            # Delegate to lakehouse_utils merge_to_table method
            merge_result = target_utils.merge_to_table(
                df=source_df,
                table_name=self.config.target_table_name,
                merge_keys=self.config.merge_keys,
                schema_name=self.config.target_schema_name,
                immutable_columns=["_raw_created_at"],
                enable_schema_evolution=self.config.enable_schema_evolution,
                partition_by=self.config.partition_columns,
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
                metrics, self.config.write_mode
            )

        except ValueError as e:
            # Handle merge key validation errors
            error_msg = str(e)
            if "merge_keys" in error_msg.lower():
                self.logger.error(f"Merge failed: One or more merge keys don't exist in source or target")
                self.logger.error(f"   Merge keys specified: {self.config.merge_keys}")
                self.logger.error(f"   Check that these column names exist in both your source file and target table")
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            raise
        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            raise

    def write_data(self, data: DataFrame) -> ProcessingMetrics:
        """Write data to target destination using dynamic target resolution"""
        write_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Get target utilities for this configuration
            target_utils = self.location_resolver.get_target_utils(
                self.config, spark=self.spark
            )

            # Handle merge mode separately using Delta Lake merge
            if self.config.write_mode.lower() == "merge":
                self.logger.info(f"Executing merge operation with keys: {self.config.merge_keys}")
                return self._execute_delta_merge(
                    source_df=data,
                    target_utils=target_utils,
                    write_start=write_start,
                )

            # Standard write modes (overwrite, append)
            # Get target row count before write (for reconciliation)
            try:
                existing_df = target_utils.read_table(self.config.target_table_name)
                metrics.target_row_count_before = existing_df.count()
            except Exception:
                metrics.target_row_count_before = 0

            # Prepare write options for schema evolution
            write_options = {}
            if self.config.enable_schema_evolution:
                write_options["mergeSchema"] = "true"

            # Write data using dynamic target utilities
            target_utils.write_to_table(
                df=data,
                table_name=self.config.target_table_name,
                mode=self.config.write_mode,
                options=write_options,
                partition_by=self.config.partition_columns,
            )

            # Get target row count after write
            try:
                result_df = target_utils.read_table(self.config.target_table_name)
                metrics.target_row_count_after = result_df.count()
            except Exception:
                metrics.target_row_count_after = 0

            # Calculate metrics
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)

            if self.config.write_mode == "overwrite":
                metrics.records_inserted = metrics.target_row_count_after
                metrics.records_deleted = metrics.target_row_count_before
            elif self.config.write_mode == "append":
                metrics.records_inserted = (
                    metrics.target_row_count_after - metrics.target_row_count_before
                )

            self.logger.info(
                f"Wrote data to {self.config.target_table_name} in {metrics.write_duration_ms}ms"
            )

            return ProcessingMetricsUtils.calculate_performance_metrics(
                metrics, self.config.write_mode
            )

        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            self.logger.error(f"Error writing data to {self.config.target_table_name}: {e}")
            raise

    def validate_data(self, data: DataFrame) -> Dict[str, Any]:
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

        except Exception as e:
            validation_results["errors"].append(f"Validation failed: {e}")
            validation_results["is_valid"] = False

        return validation_results


class PySparkFlatFileLogging(FlatFileLoggingInterface):
    """PySpark implementation of flat file ingestion logging"""

    def __init__(self, lakehouse_utils):
        self.lakehouse_utils = lakehouse_utils

    def _extract_file_metadata(
        self, config: FlatFileIngestionConfig, file_result: Optional[FileDiscoveryResult]
    ) -> dict:
        """Extract file metadata from file_result for logging

        Returns a dict with all file metadata fields needed for logging.
        """
        import json

        # Default to config source_file_path if no file_result
        source_file_path = config.source_file_path
        source_file_size_bytes = None
        source_file_modified_time = None
        date_partition = None
        partition_cols = None
        partition_values = None
        filename_attributes_json = None
        control_file_path = None

        if file_result:
            source_file_path = file_result.file_path
            if file_result.size:
                source_file_size_bytes = file_result.size
            source_file_modified_time = file_result.modified_time
            date_partition = file_result.date_partition
            control_file_path = file_result.control_file_path

            if file_result.date_partition:
                partition_cols = json.dumps(["date"])
                partition_values = json.dumps([file_result.date_partition])

            # Store filename attributes as JSON
            if file_result.filename_attributes:
                filename_attributes_json = json.dumps(file_result.filename_attributes)

        return {
            "source_file_path": source_file_path,
            "source_file_size_bytes": source_file_size_bytes,
            "source_file_modified_time": source_file_modified_time,
            "date_partition": date_partition,
            "partition_cols": partition_cols,
            "partition_values": partition_values,
            "filename_attributes_json": filename_attributes_json,
            "control_file_path": control_file_path,
        }

    def _log_file_load_event(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        status: str,
        file_result: Optional[FileDiscoveryResult] = None,
        metrics: Optional[ProcessingMetrics] = None,
        error_message: Optional[str] = None,
        error_details: Optional[str] = None,
    ) -> None:
        """Unified method to log file load events to log_file_load table using merge/upsert pattern

        Uses Airflow-style single-row-per-execution pattern where:
        - First call creates a row with status='running'
        - Subsequent calls update the same row with latest status/metrics
        - Timestamps: started_at (immutable), updated_at (always updated), completed_at (set on completion)
        - attempt_count incremented on retries
        """
        try:
            from datetime import datetime

            from ingen_fab.python_libs.common.file_load_logging_schema import (
                get_file_load_log_schema,
            )

            # Extract file metadata
            file_metadata = self._extract_file_metadata(config, file_result)

            # Populate metrics fields
            records_processed = metrics.records_processed if metrics else (0 if status != "running" else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != "running" else None)
            records_updated = metrics.records_updated if metrics else (0 if status != "running" else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != "running" else None)
            source_row_count = metrics.source_row_count if metrics else (0 if status != "running" else None)
            target_row_count_before = metrics.target_row_count_before if metrics else (0 if status != "running" else None)
            target_row_count_after = metrics.target_row_count_after if metrics else (0 if status != "running" else None)
            row_count_reconciliation_status = (
                metrics.row_count_reconciliation_status if metrics
                else ("not_verified" if status != "running" else None)
            )
            corrupt_records_count = metrics.corrupt_records_count if metrics else (0 if status != "running" else None)
            data_read_duration_ms = metrics.read_duration_ms if metrics else None
            total_duration_ms = metrics.total_duration_ms if metrics else None
            execution_duration_seconds = (
                int(metrics.total_duration_ms / 1000) if metrics and metrics.total_duration_ms
                else None
            )

            # Timestamp logic: Airflow-style
            now = datetime.now()
            started_at = now  # Will be preserved on updates (immutable)
            updated_at = now  # Always updated
            completed_at = now if status in ["completed", "failed", "duplicate", "skipped"] else None
            attempt_count = 1  # Default for inserts, will be incremented on updates

            log_data = [
                (
                    load_id,  # PK - load_id (no more log_id)
                    execution_id,  # execution_id
                    config.config_id,  # config_id
                    status,  # status
                    file_metadata["source_file_path"],  # source_file_path
                    file_metadata["source_file_size_bytes"],  # source_file_size_bytes
                    file_metadata["source_file_modified_time"],  # source_file_modified_time
                    config.target_table_name,  # target_table_name
                    records_processed,  # records_processed
                    records_inserted,  # records_inserted
                    records_updated,  # records_updated
                    records_deleted,  # records_deleted
                    source_row_count,  # source_row_count
                    target_row_count_before,  # target_row_count_before
                    target_row_count_after,  # target_row_count_after
                    row_count_reconciliation_status,  # row_count_reconciliation_status
                    corrupt_records_count,  # corrupt_records_count
                    data_read_duration_ms,  # data_read_duration_ms
                    total_duration_ms,  # total_duration_ms
                    error_message,  # error_message
                    error_details,  # error_details
                    execution_duration_seconds,  # execution_duration_seconds
                    file_metadata["partition_cols"],  # source_file_partition_cols
                    file_metadata["partition_values"],  # source_file_partition_values
                    file_metadata["date_partition"],  # date_partition
                    file_metadata["filename_attributes_json"],  # filename_attributes_json
                    file_metadata["control_file_path"],  # control_file_path
                    started_at,  # started_at (immutable)
                    updated_at,  # updated_at (always updated)
                    completed_at,  # completed_at (set when final)
                    attempt_count,  # attempt_count (default 1, incremented on updates)
                )
            ]

            schema = get_file_load_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            # Use merge instead of append
            self.lakehouse_utils.merge_to_table(
                df=log_df,
                table_name="log_file_load",
                merge_keys=["load_id"],  # PK for upsert
                immutable_columns=[
                    "load_id",
                    "execution_id",
                    "config_id",
                    "source_file_path",
                    "started_at",
                ],
                custom_update_expressions={
                    "attempt_count": "TARGET.attempt_count + 1",  # Increment on retries
                    "updated_at": "SOURCE.updated_at",  # Always update timestamp
                },
            )

        except Exception as e:
            logger.warning(f"Failed to log file load event ({status}): {e}")

    def _log_config_execution_event(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        status: str,
        files_discovered: int = 0,
        files_processed: int = 0,
        files_failed: int = 0,
        files_skipped: int = 0,
        metrics: Optional[ProcessingMetrics] = None,
        error_message: Optional[str] = None,
        error_details: Optional[str] = None,
    ) -> None:
        """Unified method to log config execution events to log_config_execution table using merge/upsert pattern

        Uses Airflow-style single-row-per-execution pattern where:
        - First call creates a row with status='running'
        - Subsequent calls update the same row with latest status/metrics
        - Timestamps: started_at (immutable), updated_at (always updated), completed_at (set on completion)

        Args:
            config: Configuration object
            execution_id: Unique execution identifier
            status: Status of execution (running, completed, failed, no_data)
            files_discovered: Number of files discovered
            files_processed: Number of files successfully processed
            files_failed: Number of files that failed
            files_skipped: Number of files skipped (duplicates, etc.)
            metrics: Aggregated processing metrics across all files
            error_message: Error message if status is 'failed'
            error_details: Detailed error information if status is 'failed'
        """
        try:
            from datetime import datetime

            from ingen_fab.python_libs.common.config_execution_logging_schema import (
                get_config_execution_log_schema,
            )

            # Populate metrics fields (aggregated across all files in execution)
            records_processed = metrics.records_processed if metrics else (0 if status != "running" else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != "running" else None)
            records_updated = metrics.records_updated if metrics else (0 if status != "running" else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != "running" else None)
            total_duration_ms = metrics.total_duration_ms if metrics else None

            # Timestamp logic: Airflow-style
            now = datetime.now()
            started_at = now  # Will be preserved on updates (immutable)
            updated_at = now  # Always updated
            completed_at = now if status in ["completed", "failed", "no_data"] else None

            log_data = [
                (
                    execution_id,  # PK part 1
                    config.config_id,  # PK part 2
                    status,  # status
                    files_discovered,  # files_discovered
                    files_processed,  # files_processed
                    files_failed,  # files_failed
                    files_skipped,  # files_skipped
                    records_processed,  # records_processed
                    records_inserted,  # records_inserted
                    records_updated,  # records_updated
                    records_deleted,  # records_deleted
                    total_duration_ms,  # total_duration_ms
                    error_message,  # error_message
                    error_details,  # error_details
                    started_at,  # started_at (immutable)
                    updated_at,  # updated_at (always updated)
                    completed_at,  # completed_at (set when final)
                )
            ]

            schema = get_config_execution_log_schema()
            log_df = self.lakehouse_utils.spark.createDataFrame(log_data, schema)

            # Use merge instead of append
            self.lakehouse_utils.merge_to_table(
                df=log_df,
                table_name="log_config_execution",
                merge_keys=["execution_id", "config_id"],  # Composite PK for upsert
                immutable_columns=[
                    "execution_id",
                    "config_id",
                    "started_at",
                ],
                custom_update_expressions={
                    "updated_at": "SOURCE.updated_at",  # Always update timestamp
                },
            )

        except Exception as e:
            logger.warning(f"Failed to log config execution event ({status}): {e}")

    def log_config_execution_start(
        self, config: FlatFileIngestionConfig, execution_id: str
    ) -> None:
        """Log the start of a config execution run"""
        self._log_config_execution_event(config, execution_id, "running")

    def log_config_execution_completion(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        files_discovered: int,
        files_processed: int,
        files_failed: int,
        files_skipped: int,
        metrics: ProcessingMetrics,
        status: str,
    ) -> None:
        """Log the completion of a config execution run"""
        self._log_config_execution_event(
            config,
            execution_id,
            status,
            files_discovered,
            files_processed,
            files_failed,
            files_skipped,
            metrics,
        )

    def log_config_execution_error(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        error_message: str,
        error_details: str,
        files_discovered: int = 0,
        files_processed: int = 0,
        files_failed: int = 0,
    ) -> None:
        """Log a config execution error"""
        self._log_config_execution_event(
            config,
            execution_id,
            "failed",
            files_discovered,
            files_processed,
            files_failed,
            0,  # files_skipped
            None,  # metrics
            error_message,
            error_details,
        )

    def log_file_load_start(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Log the start of a file load"""
        self._log_file_load_event(config, execution_id, load_id, "running", file_result)

    def log_execution_start(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """DEPRECATED: Use log_file_load_start instead. Kept for backwards compatibility."""
        self.log_file_load_start(config, execution_id, load_id, file_result)

    def log_file_load_completion(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        metrics: ProcessingMetrics,
        status: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Log the completion of a file load"""
        self._log_file_load_event(config, execution_id, load_id, status, file_result, metrics)

    def log_execution_completion(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        metrics: ProcessingMetrics,
        status: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """DEPRECATED: Use log_file_load_completion instead. Kept for backwards compatibility."""
        self.log_file_load_completion(
            config, execution_id, load_id, metrics, status, file_result
        )

    def log_file_load_error(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        error_message: str,
        error_details: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Log a file load error to the log_file_load table"""
        self._log_file_load_event(
            config, execution_id, load_id, "failed", file_result,
            error_message=error_message, error_details=error_details
        )

    # Deprecated: Use log_file_load_error instead
    def log_execution_error(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        error_message: str,
        error_details: str,
        file_result: Optional[FileDiscoveryResult] = None,
    ) -> None:
        """Deprecated: Use log_file_load_error instead"""
        self.log_file_load_error(
            config, execution_id, load_id, error_message, error_details, file_result
        )

    def log_duplicate_file(
        self,
        config: FlatFileIngestionConfig,
        execution_id: str,
        load_id: str,
        file_result: FileDiscoveryResult,
    ) -> None:
        """Log a duplicate file that was skipped to the log_file_load table"""
        self._log_file_load_event(config, execution_id, load_id, "duplicate", file_result)


class PySparkFlatFileIngestionOrchestrator(FlatFileIngestionOrchestrator):
    """PySpark implementation of flat file ingestion orchestrator"""

    def __init__(
        self,
        spark,
        location_resolver,
        logging_service,
        max_concurrency: int = 4,
    ):
        """Initialize orchestrator with dependencies for creating config-scoped services

        Args:
            spark: Spark session
            location_resolver: Location resolver for source/target resolution
            logging_service: Logging service implementation (stateless, shared across configs)
            max_concurrency: Maximum number of concurrent threads for parallel processing (default: 4)
        """
        # Don't call super().__init__() since we're not using injected services anymore
        self.spark = spark
        self.location_resolver = location_resolver
        self.logging_service = logging_service
        self.max_concurrency = max_concurrency

    def process_configuration(
        self, config: FlatFileIngestionConfig, execution_id: str
    ) -> ConfigExecutionResult:
        """Process a single configuration"""
        # Instantiate config-scoped services
        discovery_service = PySparkFlatFileDiscovery(self.spark, self.location_resolver, config)
        processor_service = PySparkFlatFileProcessor(self.spark, self.location_resolver, config)

        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_id,
            'config_group_id': config.config_group_id
        })
        start_time = time.time()
        result = ConfigExecutionResult(
            config_id=config.config_id,
            config_name=config.config_id,
            status="pending",
            metrics=ProcessingMetrics(),
            errors=[],
            files_discovered=0,
            files_processed=0,
            files_failed=0,
            files_skipped=0,
        )

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
                result.errors = validation_errors
                result.status = "failed"
                config_logger.error(f"Configuration validation failed: {', '.join(validation_errors)}")
                return result

            # Discover files
            discovered_files = discovery_service.discover_files()
            files_discovered = len(discovered_files)
            result.files_discovered = files_discovered

            # Log duplicate items if any were found during discovery
            if hasattr(discovery_service, 'last_duplicate_items'):
                duplicate_items = discovery_service.last_duplicate_items
                if duplicate_items:
                    config_logger.info(f"Logging {len(duplicate_items)} duplicate item(s)")
                    result.files_skipped = len(duplicate_items)
                    for item_info in duplicate_items:
                        from datetime import datetime
                        # Generate unique load_id for duplicate item log entry
                        duplicate_load_id = str(uuid.uuid4())
                        # Convert item_info to FileDiscoveryResult for logging
                        # item_info is a SimpleNamespace with modified_ms (not modified)
                        modified_time = None
                        if hasattr(item_info, 'modified_ms') and item_info.modified_ms:
                            modified_time = datetime.fromtimestamp(item_info.modified_ms / 1000)
                        elif hasattr(item_info, 'modified') and item_info.modified:
                            modified_time = item_info.modified

                        duplicate_item_result = FileDiscoveryResult(
                            file_path=item_info.path,
                            size=item_info.size,
                            modified_time=modified_time,
                        )
                        self.logging_service.log_duplicate_file(
                            config, execution_id, duplicate_load_id, duplicate_item_result
                        )

            if not discovered_files:
                if config.require_files:
                    error_msg = "No files found and require_files is set to True"
                    config_logger.error(error_msg)
                    raise Exception(error_msg)

                result.status = "completed"
                result.files_processed = 0
                config_logger.info(
                    f"No source data found. Skipping write and reconciliation operations."
                )

                # Log config execution completion
                self.logging_service.log_config_execution_completion(
                    config,
                    execution_id,
                    result.files_discovered,
                    result.files_processed,
                    result.files_failed,
                    result.files_skipped,
                    result.metrics,
                    "completed",
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
                    self.logging_service.log_execution_start(
                        config, execution_id, file_load_id, file_result
                    )

                    # Read file
                    df, read_metrics = processor_service.read_file(file_result)

                    if read_metrics.source_row_count == 0:
                        config_logger.warning(f"No data found in file: {file_result.file_path}")
                        continue

                    # Add ingestion metadata (including load_id)
                    df = processor_service._add_ingestion_metadata(df, file_load_id, file_result)

                    # Validate data
                    validation_result = processor_service.validate_data(df)
                    if not validation_result["is_valid"]:
                        result.files_failed = result.files_failed + 1
                        result.errors.extend(validation_result["errors"])
                        error_msg = f"Validation failed: {', '.join(validation_result['errors'])}"
                        config_logger.error(f"Validation failed for file {file_result.file_path}: {validation_result['errors']}")

                        self.logging_service.log_execution_error(
                            config,
                            execution_id,
                            file_load_id,
                            error_msg,
                            str(validation_result),
                            file_result,
                        )
                        break

                    # Write file
                    write_metrics = processor_service.write_data(df)

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

                    # Log per-file completion for all new import patterns
                    if config.import_pattern in ["single_file", "incremental_files", "incremental_folders"]:
                        self.logging_service.log_execution_completion(
                            config, execution_id, file_load_id, combined_metrics, "completed", file_result
                        )

                except Exception as file_error:
                    result.files_failed = result.files_failed + 1
                    error_details = ErrorHandlingUtils.format_error_details(
                        file_error,
                        {
                            "file_path": file_result.file_path,
                            "config_id": config.config_id,
                        },
                    )
                    result.errors.append(
                        f"Error processing file {file_result.file_path}: {file_error}"
                    )
                    config_logger.error(
                        f"Error processing file {file_result.file_path}: {file_error}"
                    )

                    self.logging_service.log_execution_error(
                            config,
                            execution_id,
                            file_load_id,
                            str(file_error),
                            str(error_details),
                            file_result,
                        )
                    break

            # Aggregate metrics
            if all_metrics:
                result.metrics = ProcessingMetricsUtils.merge_metrics(
                    all_metrics, config.write_mode
                )

                if result.files_failed > 0:
                    result.status = "failed"
                else:
                    result.status = "completed"

                result.files_processed = len(all_metrics)

                end_time = time.time()
                processing_duration = end_time - start_time
                result.metrics.total_duration_ms = int(processing_duration * 1000)

                config_logger.info(f"Processing completed in {processing_duration:.2f} seconds")
                config_logger.info(f"Records processed: {result.metrics.records_processed}")
                config_logger.info(f"Records inserted: {result.metrics.records_inserted}")
                config_logger.info(f"Records updated: {result.metrics.records_updated}")
                config_logger.info(f"Records deleted: {result.metrics.records_deleted}")
                config_logger.info(f"Source rows: {result.metrics.source_row_count}")
                config_logger.info(
                    f"Target rows before: {result.metrics.target_row_count_before}"
                )
                config_logger.info(f"Target rows after: {result.metrics.target_row_count_after}")
                config_logger.info(
                    f"Row count reconciliation: {result.metrics.row_count_reconciliation_status}"
                )

                # Log aggregated completion for date_partitioned only
                if config.import_pattern == "date_partitioned":
                    aggregated_load_id = str(uuid.uuid4())
                    self.logging_service.log_execution_completion(
                        config, execution_id, aggregated_load_id, result.metrics, result.status
                    )

                # Log config execution completion
                self.logging_service.log_config_execution_completion(
                    config,
                    execution_id,
                    result.files_discovered,
                    result.files_processed,
                    result.files_failed,
                    result.files_skipped,
                    result.metrics,
                    result.status,
                )
            else:
                if result.files_failed > 0:
                    result.status = "failed"
                else:
                    result.status = "no_data_processed"

                result.files_processed = 0
                config_logger.info(
                    f"Processing completed in {time.time() - start_time:.2f} seconds ({result.status})"
                )

                if result.status == "no_data_processed":
                    config_logger.info("Row count reconciliation: skipped (no source data)")

                # Log config execution completion
                self.logging_service.log_config_execution_completion(
                    config,
                    execution_id,
                    result.files_discovered,
                    result.files_processed,
                    result.files_failed,
                    result.files_skipped,
                    result.metrics,
                    result.status,
                )

        except DuplicateFilesError as e:
            # Handle duplicate files error specifically
            result.status = "failed"
            result.errors.append(str(e))
            error_details = ErrorHandlingUtils.format_error_details(
                e, {"config_id": config.config_id, "config_name": config.config_id}
            )
            config_logger.error(f"Configuration failed due to duplicate files: {e}")

            # Log config execution error
            self.logging_service.log_config_execution_error(
                config,
                execution_id,
                str(e),
                str(error_details),
                result.files_discovered,
                result.files_processed,
                result.files_failed + 1,  # Count this as a failed file
            )

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            # File counts already initialized in dataclass, no need to check
            error_details = ErrorHandlingUtils.format_error_details(
                e, {"config_id": config.config_id, "config_name": config.config_id}
            )
            config_logger.error(str(e))

            # Log config execution error
            self.logging_service.log_config_execution_error(
                config,
                execution_id,
                str(e),
                str(error_details),
                result.files_discovered,
                result.files_processed,
                result.files_failed,
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
        execution_groups = defaultdict(list)
        for config in configs:
            execution_groups[config.execution_group].append(config)

        # Sort by execution_group (lower = earlier)
        group_numbers = sorted(execution_groups.keys())

        logger.info(
            f"Processing {len(configs)} configurations across {len(group_numbers)} execution group(s)"
        )

        for group_num in group_numbers:
            group_configs = execution_groups[group_num]

            # Log which config_group_ids are in this execution group
            config_group_names = sorted({c.config_group_id or "ungrouped" for c in group_configs})

            # Process configurations in this group in parallel
            group_results = self._process_execution_group_parallel(
                group_configs, execution_id, f"exec_group_{group_num}"
            )

            # Aggregate results
            results["configurations"].extend(group_results)
            results["execution_groups_processed"].append(group_num)

            # Count statuses
            for config_result in group_results:
                if config_result.status == "completed":
                    results["successful"] += 1
                elif config_result.status == "failed":
                    results["failed"] += 1
                elif config_result.status in ["no_data_found", "no_data_processed"]:
                    results["no_data_found"] += 1

        return results

    def _process_execution_group_parallel(
        self, configs: List[FlatFileIngestionConfig], execution_id: str, group_id: str
    ) -> List[ConfigExecutionResult]:
        """Process configurations within a config group in parallel"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = []
        start_time = time.time()

        if len(configs) == 1:
            # Single configuration - no need for parallel processing
            result = self.process_configuration(configs[0], execution_id)
            results = [result]
        else:
            # Multiple configurations - process in parallel
            max_workers = min(len(configs), self.max_concurrency)
            logger.info(
                f"Using {max_workers} parallel workers for {len(configs)} configurations (max_concurrency={self.max_concurrency})"
            )

            with ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix=f"FlatFileGroup_{group_id}"
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
                    except Exception as e:
                        error_result = ConfigExecutionResult(
                            config_id=config.config_id,
                            config_name=config.config_id,
                            status="failed",
                            metrics=ProcessingMetrics(),
                            errors=[f"Thread execution error: {str(e)}"],
                            files_discovered=0,
                            files_processed=0,
                            files_failed=0,
                            files_skipped=0,
                        )
                        results.append(error_result)
                        logger.error(f"Failed: {config.config_id} - {str(e)}")

        # Generate summary
        end_time = time.time()
        total_duration = end_time - start_time

        # Count statuses
        completed = sum(1 for r in results if r.status == 'completed')
        failed = sum(1 for r in results if r.status == 'failed')
        no_data = sum(1 for r in results if r.status in ['no_data_found', 'no_data_processed'])

        logger.info("")
        logger.info(f"Config group '{group_id}' completed in {total_duration:.1f}s")
        logger.info(f"Processed {len(results)} configuration{'s' if len(results) != 1 else ''} - {completed} completed, {failed} failed, {no_data} no data")
        logger.info("")

        for r in results:
            status = r.status
            config_name = r.config_name
            files_discovered = r.files_discovered
            files_processed = r.files_processed

            if status == 'completed':
                icon = '✓'
                rows = r.metrics.records_processed if r.metrics and r.metrics.records_processed else 0
                duration_ms = r.metrics.total_duration_ms if r.metrics and r.metrics.total_duration_ms else 0
                duration = f"{duration_ms / 1000:.1f}s" if duration_ms else "0.0s"

                # Format file count
                file_text = f"{files_processed} file{'s' if files_processed != 1 else ''}"

                logger.info(f"{icon} {config_name} ({status}) - {file_text}, {rows} rows in {duration}")
            elif status == 'failed':
                icon = '✗'
                errors_list = r.errors
                error_count = len(errors_list)

                # Format file count for partial failure
                if files_discovered > 0:
                    file_text = f"{files_processed}/{files_discovered} file{'s' if files_discovered != 1 else ''}"
                else:
                    file_text = "0 files"

                # Show error summary
                if error_count == 0:
                    logger.info(f"{icon} {config_name} ({status}) - {file_text} - Unknown error")
                elif error_count == 1:
                    logger.info(f"{icon} {config_name} ({status}) - {file_text} - Error: {errors_list[0]}")
                else:
                    # Multiple errors: show count and first error
                    logger.info(f"{icon} {config_name} ({status}) - {file_text} - {error_count} errors:")
                    # Log errors with indentation
                    if error_count <= 10:
                        # Show all errors if 10 or fewer
                        for error in errors_list:
                            logger.info(f"    └─ {error}")
                    else:
                        # Too many errors, show first 5 and summarize
                        for error in errors_list[:5]:
                            logger.info(f"    └─ {error}")
                        logger.info(f"    └─ ... and {error_count - 5} more errors")
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
    ) -> ConfigExecutionResult:
        """Process a single configuration with thread information"""
        config_logger = ConfigLoggerAdapter(logger, {
            'config_name': config.config_id,
            'config_group_id': config.config_group_id
        })

        config_logger.info(f"Starting processing")

        try:
            result = self.process_configuration(config, execution_id)
            config_logger.info(f"Finished processing ({result.status})")
            return result
        except Exception as e:
            raise
