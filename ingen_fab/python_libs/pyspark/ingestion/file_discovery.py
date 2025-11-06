# File Discovery Framework
# Discovers files/folders from Microsoft Fabric lakehouse Files storage
# Returns BatchInfo objects ready for processing

import logging
import os
from datetime import datetime
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, max as spark_max, row_number, split
from pyspark.sql.window import Window

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
    DatePartitionUtils,
    FilePatternUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.config import (
    FileSystemLoadingParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.constants import (
    BatchBy,
    DuplicateHandling,
    ExecutionStatus,
    ImportPattern,
)
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    DuplicateFilesError,
    ErrorContext,
    FileDiscoveryError,
    FileNotFoundError as IngestionFileNotFoundError,
    IngestionError,
)
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import BatchInfo
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo, lakehouse_utils

# Configure logging
logger = logging.getLogger(__name__)


class FileDiscovery:
    """
    Discovers files/folders from lakehouse Files storage.

    This class handles:
    - File discovery based on import_pattern (single_file, incremental_files, incremental_folders)
    - Duplicate detection and handling
    - Date extraction and filtering
    - Control file validation
    - Batch creation and sorting

    Returns list of BatchInfo objects ready for reading/processing.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ResourceConfig,
        lakehouse_utils_instance: Optional[lakehouse_utils] = None,
    ):
        """
        Initialize FileDiscovery with configuration.

        Args:
            spark: Spark session for querying log tables
            config: ResourceConfig with file loading settings
            lakehouse_utils_instance: Optional pre-configured lakehouse_utils instance
                                     (if not provided, will create from config)
        """
        self.spark = spark
        self.config = config

        # Extract FileSystemLoadingParams from loading_params dict
        if isinstance(config.loading_params, dict):
            self.params = FileSystemLoadingParams.from_dict(config.loading_params)
        elif isinstance(config.loading_params, FileSystemLoadingParams):
            self.params = config.loading_params
        else:
            raise ValueError("loading_params must be dict or FileSystemLoadingParams")

        # Get or create lakehouse_utils for source lakehouse
        if lakehouse_utils_instance:
            self.lakehouse = lakehouse_utils_instance
        else:
            # Extract workspace/lakehouse from source_config
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

        self._last_duplicate_items: List[FileInfo] = []

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

    @property
    def last_duplicate_items(self) -> List[FileInfo]:
        """Get list of duplicate items from last discovery operation"""
        return self._last_duplicate_items

    def discover(self) -> List[BatchInfo]:
        """
        Discover files based on configuration.

        Returns:
            List of BatchInfo objects representing discovered batches
        """
        base_path = self.config.source_file_path

        if not base_path:
            raise ValueError("source_file_path is required")

        try:
            # Step 1: List items based on batch_by
            batch_by = BatchBy(self.params.batch_by)
            item_infos = self._list_items_by_batch_strategy(batch_by, base_path)

            if not item_infos:
                self.logger.info(f"No items found in {base_path}")
                return []

            # Step 2: Filter based on import_pattern (incremental vs full)
            import_pattern = ImportPattern(self.params.import_pattern)
            if import_pattern == ImportPattern.INCREMENTAL:
                unprocessed_items, duplicate_items = self._get_unprocessed_items(item_infos)
                self._last_duplicate_items = duplicate_items
            else:  # FULL
                unprocessed_items = item_infos
                self._last_duplicate_items = []

            if not unprocessed_items:
                self.logger.info("No unprocessed items found")
                return []

            # Step 3: Build batches based on batch_by strategy
            batches = self._build_batches_by_strategy(batch_by, unprocessed_items, base_path)

            # Step 4: Sort batches
            batches = self._sort_batches(batches)

            # Step 5: Apply filters
            batches = self._apply_filters(batches)

            self.logger.info(f"Discovered {len(batches)} batch(es) to process")

            return batches

        except Exception as e:
            # Don't wrap our custom exceptions - they already have context
            if isinstance(e, IngestionError):
                raise

            self.logger.exception(f"Discovery failed: {e}")
            raise FileDiscoveryError(
                message=f"Failed to discover items in: {base_path}",
                context=ErrorContext(
                    resource_name=self.config.resource_name,
                    source_name=self.config.source_name,
                    file_path=base_path,
                    operation="discover",
                ),
            ) from e

    def _list_items_by_batch_strategy(self, batch_by: BatchBy, base_path: str) -> List[FileInfo]:
        """
        List items based on batching strategy.

        Args:
            batch_by: Batching strategy (FILE, FOLDER, or ALL)
            base_path: Base path to list from

        Returns:
            List of FileInfo objects
        """
        if batch_by == BatchBy.ALL:
            # Read everything in path as one batch - list all files recursively
            all_files = self.lakehouse.list_files_with_metadata(
                directory_path=base_path,
                pattern=self.params.discovery_pattern or "*",
                recursive=True,
            )

            if not all_files:
                return []

            # Combine all files into a single "virtual" item representing the entire directory
            total_size = sum(f.size for f in all_files)
            latest_modified_ms = max(f.modified_ms for f in all_files)

            # Create a single FileInfo representing the entire directory
            return [FileInfo(
                path=base_path,
                name=os.path.basename(base_path.rstrip("/")),
                size=total_size,
                modified_ms=latest_modified_ms,
            )]

        elif batch_by == BatchBy.FILE:
            # List individual files
            pattern = self.params.discovery_pattern or "*"
            return self.lakehouse.list_files_with_metadata(
                directory_path=base_path,
                pattern=pattern,
                recursive=self.params.recursive,
            )

        else:  # BatchBy.FOLDER
            # List subfolders and treat each as a batch
            is_hierarchical = (
                self.params.discovery_pattern and "/" in self.params.discovery_pattern
            )

            if is_hierarchical:
                # List recursively and match full relative paths
                pattern = self.params.discovery_pattern or "*"
                expected_depth = pattern.count("/") + 1
                all_folders = self.lakehouse.list_directories(base_path, recursive=True)

                # Filter to correct depth and pattern
                folder_paths = []
                for folder_path in all_folders:
                    # Extract relative path
                    normalized_base = base_path.rstrip("/")
                    normalized_folder = folder_path.rstrip("/")

                    if f"/{normalized_base}/" in normalized_folder:
                        relative_path = normalized_folder.split(f"/{normalized_base}/", 1)[1]
                    elif normalized_folder.endswith(f"/{normalized_base}"):
                        continue
                    else:
                        continue

                    if not relative_path:
                        continue

                    folder_depth = relative_path.count("/") + 1

                    if folder_depth == expected_depth:
                        if FilePatternUtils.matches_pattern(relative_path, pattern):
                            folder_paths.append(folder_path)
            else:
                # Simple pattern - list immediate subdirectories
                folder_paths = self.lakehouse.list_directories(base_path, recursive=False)

                # Apply discovery pattern
                if self.params.discovery_pattern and self.params.discovery_pattern != "*":
                    filtered = []
                    for folder_path in folder_paths:
                        folder_name = os.path.basename(folder_path.rstrip("/"))
                        if FilePatternUtils.matches_pattern(
                            folder_name, self.params.discovery_pattern
                        ):
                            filtered.append(folder_path)
                    folder_paths = filtered

            # Get metadata for each folder
            folder_infos = []
            for folder_path in folder_paths:
                try:
                    # Get files in folder to determine metadata
                    files_in_folder = self.lakehouse.list_files_with_metadata(
                        directory_path=folder_path,
                        pattern="*",
                        recursive=True,
                    )

                    if files_in_folder:
                        self.logger.debug(
                            f"  Folder {os.path.basename(folder_path.rstrip('/'))} "
                            f"contains {len(files_in_folder)} file(s)"
                        )
                        latest_modified_ms = max(f.modified_ms for f in files_in_folder)
                        folder_size = sum(f.size for f in files_in_folder)

                        folder_info = FileInfo(
                            path=folder_path,
                            name=os.path.basename(folder_path.rstrip("/")),
                            size=folder_size,
                            modified_ms=latest_modified_ms,
                        )
                        folder_infos.append(folder_info)
                    else:
                        self.logger.warning(f"Folder {folder_path} is empty, skipping")
                except Exception as e:
                    self.logger.warning(f"Could not get metadata for folder {folder_path}: {e}")
                    continue

            return folder_infos

    def _build_batches_by_strategy(
        self,
        batch_by: BatchBy,
        item_infos: List[FileInfo],
        base_path: str
    ) -> List[BatchInfo]:
        """
        Build BatchInfo objects based on batching strategy.

        Args:
            batch_by: Batching strategy (FILE, FOLDER, or ALL)
            item_infos: List of FileInfo objects
            base_path: Base path for relative path calculation

        Returns:
            List of BatchInfo objects
        """
        batches = []

        if batch_by == BatchBy.ALL:
            # Single batch with all files in the directory
            if not item_infos:
                return []

            item_info = item_infos[0]  # Should only be one "virtual" item
            full_path = f"{self.lakehouse.lakehouse_files_uri()}{item_info.path.lstrip('/')}"

            batch_info = BatchInfo(
                batch_id=self._generate_batch_id(),
                file_paths=[full_path],
                size_bytes=item_info.size,
                modified_time=datetime.fromtimestamp(item_info.modified_ms / 1000),
            )

            batches.append(batch_info)

        elif batch_by == BatchBy.FILE:
            # Build file batches
            for file_info in item_infos:
                # Build full ABFSS path
                full_path = f"{self.lakehouse.lakehouse_files_uri()}{file_info.path.lstrip('/')}"

                # Extract date if configured
                date_partition = None
                if self.params.date_pattern:
                    date_partition = DatePartitionUtils.extract_date_from_folder_name(
                        file_info.name, self.params.date_pattern
                    )

                batch_info = BatchInfo(
                    batch_id=self._generate_batch_id(),
                    file_paths=[full_path],
                    size_bytes=file_info.size,
                    modified_time=datetime.fromtimestamp(file_info.modified_ms / 1000),
                    date_partition=date_partition,
                )

                batches.append(batch_info)

        else:  # BatchBy.FOLDER
            # Detect hierarchical patterns
            is_hierarchical = (
                self.params.discovery_pattern and "/" in self.params.discovery_pattern
            )

            # Build folder batches
            for folder_info in item_infos:
                if is_hierarchical:
                    relative_path = folder_info.path.replace(base_path, "").strip("/")
                    folder_name = relative_path.replace("/", "_")
                    extraction_key = relative_path
                else:
                    folder_name = os.path.basename(folder_info.path.rstrip("/"))
                    extraction_key = folder_name

                # Extract date if configured
                date_partition = None
                if self.params.date_pattern:
                    date_partition = DatePartitionUtils.extract_date_from_folder_name(
                        extraction_key, self.params.date_pattern
                    )

                batch_info = BatchInfo(
                    batch_id=self._generate_batch_id(),
                    file_paths=[folder_info.path],  # Folder path as single item
                    size_bytes=folder_info.size,
                    modified_time=datetime.fromtimestamp(folder_info.modified_ms / 1000),
                    date_partition=date_partition,
                    folder_name=folder_name,
                )

                batches.append(batch_info)

        return batches

    def _sort_batches(self, batches: List[BatchInfo]) -> List[BatchInfo]:
        """
        Sort batches chronologically by date partition or modified time.

        Args:
            batches: List of BatchInfo objects

        Returns:
            Sorted list of BatchInfo objects
        """
        if not batches:
            return batches

        # Sort by date or modified time
        if self.params.date_pattern:
            self.logger.info("Sorting batches chronologically by extracted date")
            batches.sort(key=lambda x: x.date_partition or "")
        else:
            batches.sort(key=lambda x: x.modified_time or datetime.min)

        return batches

    def _apply_filters(self, batches: List[BatchInfo]) -> List[BatchInfo]:
        """
        Apply control file and date range filters to batches.

        Args:
            batches: List of BatchInfo objects

        Returns:
            Filtered list of BatchInfo objects
        """
        # Apply control file filter if required
        if self.params.require_control_file and self.params.control_file_pattern:
            batches = self._filter_by_control_files(batches)

        # Apply date range filter if configured
        if self.params.date_pattern and self.params.date_range_start:
            batches = self._filter_by_date_range(batches)

        return batches

    def _get_unprocessed_items(
        self, item_infos: List[FileInfo]
    ) -> Tuple[List[FileInfo], List[FileInfo]]:
        """
        Filter to unprocessed items and detect duplicates.

        Two-step process:
        1. Filter by modified time: only consider items newer than last run
        2. Check for duplicates: identify items already processed

        Args:
            item_infos: List of FileInfo objects

        Returns:
            (unprocessed_items, duplicate_items)
        """
        if not item_infos:
            return [], []

        # Check if log table exists
        try:
            self.spark.sql("DESCRIBE TABLE log_file_load")
        except Exception:
            # Log table doesn't exist yet - treat all items as new
            self.logger.info("Log table 'log_file_load' not found, treating all items as new")
            return item_infos, []

        # STEP 1: Filter by modified time
        try:
            # Use DataFrame API to avoid SQL injection
            latest_time_result = (
                self.spark.table("log_file_load")
                .filter(col("config_id") == self.config.resource_name)
                .filter(col("status") == ExecutionStatus.COMPLETED)
                .filter(col("source_file_modified_time").isNotNull())
                .agg(spark_max("source_file_modified_time").alias("latest_time"))
                .collect()
            )
        except Exception as e:
            self.logger.warning(f"Could not query log table: {e}, treating all items as new")
            return item_infos, []

        if latest_time_result and latest_time_result[0].latest_time:
            latest_processed_ms = int(latest_time_result[0].latest_time.timestamp() * 1000)
            newer_items = [f for f in item_infos if f.modified_ms > latest_processed_ms]
            self.logger.info(f"Found {len(newer_items)} item(s) newer than last run")
        else:
            self.logger.info("No previous runs found, processing all items")
            newer_items = item_infos

        if not newer_items:
            return [], []

        # STEP 2: Check for duplicates among newer items
        discovered_data = [
            (os.path.basename(f.path), f.path, f.size, f.modified_ms)
            for f in newer_items
        ]

        discovered_df = self.spark.createDataFrame(
            discovered_data, ["item_name", "item_path", "size", "modified_ms"]
        )

        # Query log table for processed items
        try:
            # Use DataFrame API to avoid SQL injection
            window_spec = Window.partitionBy("source_file_path").orderBy(col("started_at").desc())

            processed_df = (
                self.spark.table("log_file_load")
                .filter(col("config_id") == self.config.resource_name)
                .withColumn("item_name", element_at(split(col("source_file_path"), "/"), -1))
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .filter(col("status").isin([ExecutionStatus.COMPLETED, ExecutionStatus.NO_DATA]))
                .select("item_name")
                .distinct()
            )
        except Exception as e:
            self.logger.warning(f"Could not query processed items: {e}, treating all items as new")
            return newer_items, []

        # Find duplicates and unprocessed
        duplicates_df = discovered_df.join(processed_df, on="item_name", how="inner")
        duplicate_names = {row.item_name for row in duplicates_df.collect()}

        unprocessed_df = discovered_df.join(processed_df, on="item_name", how="left_anti")
        unprocessed_names = {row.item_name for row in unprocessed_df.collect()}

        duplicate_items = [
            f for f in newer_items if os.path.basename(f.path) in duplicate_names
        ]
        unprocessed_items = [
            f for f in newer_items if os.path.basename(f.path) in unprocessed_names
        ]

        # Handle based on duplicate_handling setting
        if self.params.duplicate_handling == DuplicateHandling.ALLOW:
            all_items = unprocessed_items + duplicate_items
            if duplicate_items:
                self.logger.info(
                    f"Found {len(all_items)} item(s) including {len(duplicate_items)} duplicates"
                )
            else:
                self.logger.info(f"Found {len(all_items)} unprocessed item(s)")
            return all_items, []
        elif self.params.duplicate_handling == DuplicateHandling.FAIL:
            if duplicate_items:
                dup_names = [os.path.basename(f.path) for f in duplicate_items]
                error_msg = f"Duplicate items detected: {', '.join(dup_names)}"
                self.logger.error(error_msg)
                raise DuplicateFilesError(error_msg)
            self.logger.info(f"Found {len(unprocessed_items)} unprocessed item(s)")
            return unprocessed_items, []
        else:  # SKIP mode
            if duplicate_items:
                self.logger.info(f"Skipping {len(duplicate_items)} duplicate item(s)")
            self.logger.info(f"Found {len(unprocessed_items)} unprocessed item(s)")
            return unprocessed_items, duplicate_items

    def _filter_by_control_files(self, batches: List[BatchInfo]) -> List[BatchInfo]:
        """Filter batches to only those with corresponding control files"""
        if not batches or not self.params.control_file_pattern:
            return batches

        # Auto-detect mode
        control_pattern = self.params.control_file_pattern
        is_per_file_mode = "{basename}" in control_pattern

        batches_with_control = []
        batches_without_control = []

        for batch in batches:
            # Get first file path (for folders, this is the folder path)
            item_path = batch.file_paths[0]
            directory = os.path.dirname(item_path.rstrip("/"))

            # Resolve control file pattern
            if is_per_file_mode:
                # Per-file mode
                filename = os.path.basename(item_path.rstrip("/"))
                basename = os.path.splitext(filename)[0]
                control_file_name = control_pattern.replace("{basename}", basename)
            else:
                # Per-folder mode (fixed name)
                control_file_name = control_pattern

            control_file_path = f"{directory}/{control_file_name}".replace("//", "/")

            # Check if control file exists
            if self.lakehouse.file_exists(control_file_path):
                batches_with_control.append(batch)
            else:
                batches_without_control.append(batch)

        if batches_without_control:
            self.logger.info(f"Skipped {len(batches_without_control)} item(s) without control files")

        return batches_with_control

    def _filter_by_date_range(self, batches: List[BatchInfo]) -> List[BatchInfo]:
        """Filter batches to those within configured date range"""
        if not self.params.date_pattern:
            return batches

        filtered_batches = []
        for batch in batches:
            if batch.date_partition:
                start_date = self.params.date_range_start or ""
                end_date = self.params.date_range_end or ""

                if DatePartitionUtils.is_date_in_range(
                    batch.date_partition, start_date, end_date
                ):
                    filtered_batches.append(batch)
                else:
                    self.logger.debug(
                        f"Skipping {batch.batch_id}: date {batch.date_partition} "
                        f"outside range [{start_date or 'unbounded'} to {end_date or 'unbounded'}]"
                    )
            else:
                self.logger.debug(
                    f"Skipping {batch.batch_id}: no date found matching pattern"
                )

        return filtered_batches

    def _generate_batch_id(self) -> str:
        """Generate unique batch ID"""
        import uuid
        return str(uuid.uuid4())
