# File Discovery Framework (SIMPLIFIED)
# Discovers files from raw layer for ingestion into bronze tables
# Assumes files are already validated (by FileSystemExtractor for external files)

import logging
import os
from datetime import datetime
from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, element_at, max as spark_max, row_number, split
from pyspark.sql.window import Window

from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    ErrorContext,
    FileDiscoveryError,
    FileNotFoundError as IngestionFileNotFoundError,
)
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import BatchInfo
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo, lakehouse_utils

logger = logging.getLogger(__name__)


class FileDiscovery:
    """
    Discovers files from raw layer for ingestion.

    SIMPLIFIED VERSION - assumes files are already validated.

    This class handles:
    - File discovery in raw layer
    - Checking log_file_load for already processed files (incremental mode)
    - Batch creation

    REMOVED (moved to FileSystemExtractor):
    - Control file validation
    - Duplicate detection for inbound files
    - Complex discovery patterns
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ResourceConfig,
        lakehouse_utils_instance: lakehouse_utils = None,
    ):
        """
        Initialize FileDiscovery with configuration.

        Args:
            spark: Spark session for querying log tables
            config: ResourceConfig with file loading settings
            lakehouse_utils_instance: Optional pre-configured lakehouse_utils instance
        """
        self.spark = spark
        self.config = config

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

        # Configure logging
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s - %(message)s',
                force=True
            )

        self.logger = ConfigLoggerAdapter(logger, {
            'source_name': self.config.source_name,
            'config_name': self.config.resource_name,
        })

    def discover(self) -> List[BatchInfo]:
        """
        Discover files from raw layer.

        Returns:
            List of BatchInfo objects representing discovered batches
        """
        raw_path = self.config.raw_landing_path

        if not raw_path:
            raise ValueError("raw_landing_path is required")

        try:
            # Step 1: List items based on batch_by strategy
            batch_by = self.config.batch_by
            item_infos = self._list_items_by_batch_strategy(batch_by, raw_path)

            if not item_infos:
                self.logger.info(f"No items found in {raw_path}")
                return []

            # Step 2: Filter to unprocessed items (if incremental mode)
            if self.config.import_mode == "incremental":
                unprocessed_items = self._get_unprocessed_items(item_infos)
            else:  # full mode
                unprocessed_items = item_infos

            if not unprocessed_items:
                self.logger.info("No unprocessed items found")
                return []

            # Step 3: Build batches
            batches = self._build_batches_by_strategy(batch_by, unprocessed_items, raw_path)

            # Step 4: Sort batches chronologically
            batches = self._sort_batches(batches)

            self.logger.info(f"Discovered {len(batches)} batch(es) to process")

            return batches

        except Exception as e:
            self.logger.exception(f"Discovery failed: {e}")
            raise FileDiscoveryError(
                message=f"Failed to discover items in: {raw_path}",
                context=ErrorContext(
                    resource_name=self.config.resource_name,
                    source_name=self.config.source_name,
                    file_path=raw_path,
                    operation="discover",
                ),
            ) from e

    def _list_items_by_batch_strategy(self, batch_by: str, base_path: str) -> List[FileInfo]:
        """
        List items based on batching strategy.

        Args:
            batch_by: Batching strategy ('file', 'folder', or 'all')
            base_path: Base path to list from (raw layer)

        Returns:
            List of FileInfo objects
        """
        if batch_by == "all":
            # Read everything in path as one batch - list all files recursively
            all_files = self.lakehouse.list_files_with_metadata(
                directory_path=base_path,
                pattern="*",
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

        elif batch_by == "file":
            # List individual files
            return self.lakehouse.list_files_with_metadata(
                directory_path=base_path,
                pattern="*",
                recursive=True,
            )

        else:  # folder
            # List all subdirectories recursively to find date-partitioned folders
            folder_paths = self.lakehouse.list_directories(base_path, recursive=True)

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

                    # Only include folders that actually contain files
                    # This filters out parent folders like "2024" or "2024/11"
                    # and only includes leaf folders like "2024/11/30"
                    if files_in_folder:
                        latest_modified_ms = max(f.modified_ms for f in files_in_folder)
                        folder_size = sum(f.size for f in files_in_folder)

                        folder_info = FileInfo(
                            path=folder_path,
                            name=os.path.basename(folder_path.rstrip("/")),
                            size=folder_size,
                            modified_ms=latest_modified_ms,
                        )
                        folder_infos.append(folder_info)
                except Exception as e:
                    self.logger.warning(f"Could not get metadata for folder {folder_path}: {e}")
                    continue

            return folder_infos

    def _build_batches_by_strategy(
        self,
        batch_by: str,
        item_infos: List[FileInfo],
        base_path: str
    ) -> List[BatchInfo]:
        """
        Build BatchInfo objects based on batching strategy.

        Args:
            batch_by: Batching strategy ('file', 'folder', or 'all')
            item_infos: List of FileInfo objects
            base_path: Base path for relative path calculation

        Returns:
            List of BatchInfo objects
        """
        batches = []

        if batch_by == "all":
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

        elif batch_by == "file":
            # Build file batches
            for file_info in item_infos:
                # Build full ABFSS path
                full_path = f"{self.lakehouse.lakehouse_files_uri()}{file_info.path.lstrip('/')}"

                batch_info = BatchInfo(
                    batch_id=self._generate_batch_id(),
                    file_paths=[full_path],
                    size_bytes=file_info.size,
                    modified_time=datetime.fromtimestamp(file_info.modified_ms / 1000),
                )

                batches.append(batch_info)

        else:  # folder
            # Build folder batches
            for folder_info in item_infos:
                folder_name = os.path.basename(folder_info.path.rstrip("/"))

                batch_info = BatchInfo(
                    batch_id=self._generate_batch_id(),
                    file_paths=[folder_info.path],  # Folder path as single item
                    size_bytes=folder_info.size,
                    modified_time=datetime.fromtimestamp(folder_info.modified_ms / 1000),
                    folder_name=folder_name,
                )

                batches.append(batch_info)

        return batches

    def _sort_batches(self, batches: List[BatchInfo]) -> List[BatchInfo]:
        """
        Sort batches for sequential processing.

        Sorting strategy:
        - If sort_by is configured in extraction_params: Sort by extracted metadata fields, then modified time
        - If no sort_by: Sort by modified time only

        Args:
            batches: List of BatchInfo objects

        Returns:
            Sorted list of BatchInfo objects
        """
        if not batches:
            return batches

        # Get metadata patterns and sort config from extraction_params
        extraction_params = self.config.extraction_params if isinstance(self.config.extraction_params, dict) else {}
        metadata_patterns = extraction_params.get("filename_metadata", [])
        sort_by = extraction_params.get("sort_by", [])
        sort_order = extraction_params.get("sort_order", "asc")

        # If no sort_by configured, sort by modified time only
        if not sort_by:
            batches.sort(key=lambda x: x.modified_time or datetime.min)
            self.logger.debug(f"Sorted {len(batches)} batch(es) by modified time")
            return batches

        def get_sort_key(batch_info: BatchInfo) -> tuple:
            """Generate sort key from metadata fields plus modified time"""
            # Extract metadata from batch file path
            metadata = self._extract_metadata_from_path(
                batch_info.file_paths[0] if batch_info.file_paths else "",
                metadata_patterns
            )

            # Build sort key tuple from sort_by fields
            sort_values = []
            for field_name in sort_by:
                value = metadata.get(field_name)
                # Use empty string if not found (sorts first for asc, last for desc)
                sort_values.append(value if value is not None else "")

            # Add modified time as final tie-breaker
            sort_values.append(batch_info.modified_time or datetime.min)

            return tuple(sort_values)

        # Sort with reverse flag based on sort_order
        reverse = (sort_order == "desc")
        batches.sort(key=get_sort_key, reverse=reverse)

        # Log sorting method
        sort_fields = ", ".join(sort_by)
        self.logger.debug(
            f"Sorted {len(batches)} batch(es) by [{sort_fields}] ({sort_order}) then modified time"
        )

        return batches

    def _extract_metadata_from_path(self, path: str, metadata_patterns: List[dict]) -> dict:
        """
        Extract metadata values from file path using configured patterns.

        Args:
            path: Full file path
            metadata_patterns: List of metadata extraction patterns

        Returns:
            Dict mapping metadata field names to extracted values
        """
        import re

        metadata = {}

        for pattern in metadata_patterns:
            field_name = pattern["name"]
            regex = pattern["regex"]

            try:
                match = re.search(regex, path)
                if match:
                    groups = match.groups()
                    if groups:
                        # Single group or concatenate multiple groups
                        if len(groups) == 1:
                            value = groups[0]
                        else:
                            value = "".join(groups)
                        metadata[field_name] = value
            except Exception:
                pass  # Silently skip failed extractions

        return metadata

    def _get_unprocessed_items(
        self, item_infos: List[FileInfo]
    ) -> List[FileInfo]:
        """
        Filter to unprocessed items based on log_load_batch table.

        Args:
            item_infos: List of FileInfo objects

        Returns:
            List of unprocessed FileInfo objects
        """
        if not item_infos:
            return []

        # Check if log table exists
        try:
            self.spark.sql("DESCRIBE TABLE log_load_batch")
        except Exception:
            # Log table doesn't exist yet - treat all items as new
            self.logger.info("Log table 'log_load_batch' not found, treating all items as new")
            return item_infos

        # Filter by modified time (only consider items newer than last run)
        try:
            latest_time_result = (
                self.spark.table("log_load_batch")
                .filter(col("source_name") == self.config.source_name)
                .filter(col("resource_name") == self.config.resource_name)
                .filter(col("status") == ExecutionStatus.COMPLETED)
                .filter(col("source_file_modified_time").isNotNull())
                .agg(spark_max("source_file_modified_time").alias("latest_time"))
                .collect()
            )
        except Exception as e:
            self.logger.warning(f"Could not query log table: {e}, treating all items as new")
            return item_infos

        if latest_time_result and latest_time_result[0].latest_time:
            latest_processed_ms = int(latest_time_result[0].latest_time.timestamp() * 1000)
            newer_items = [f for f in item_infos if f.modified_ms > latest_processed_ms]
            self.logger.info(f"Found {len(newer_items)} item(s) newer than last run")
        else:
            self.logger.info("No previous runs found, processing all items")
            newer_items = item_infos

        if not newer_items:
            return []

        # Check which of the newer items have already been processed
        discovered_data = [
            (os.path.basename(f.path), f.path, f.size, f.modified_ms)
            for f in newer_items
        ]

        discovered_df = self.spark.createDataFrame(
            discovered_data, ["item_name", "item_path", "size", "modified_ms"]
        )

        # Query log table for processed items (get latest status per file using window function)
        try:
            window_spec = Window.partitionBy("source_file_path", "source_name", "resource_name").orderBy(col("started_at").desc())

            # Get latest status for each file
            latest_status_df = (
                self.spark.table("log_load_batch")
                .filter(col("source_name") == self.config.source_name)
                .filter(col("resource_name") == self.config.resource_name)
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
            )

            # Filter to completed/no_data statuses and extract item name
            processed_df = (
                latest_status_df
                .filter(col("status").isin([ExecutionStatus.COMPLETED, ExecutionStatus.NO_DATA]))
                .withColumn("item_name", element_at(split(col("source_file_path"), "/"), -1))
                .select("item_name")
                .distinct()
            )
        except Exception as e:
            self.logger.warning(f"Could not query processed items: {e}, treating all items as new")
            return newer_items

        # Find unprocessed items
        unprocessed_df = discovered_df.join(processed_df, on="item_name", how="left_anti")
        unprocessed_names = {row.item_name for row in unprocessed_df.collect()}

        unprocessed_items = [
            f for f in newer_items if os.path.basename(f.path) in unprocessed_names
        ]

        self.logger.info(f"Found {len(unprocessed_items)} unprocessed item(s)")
        return unprocessed_items

    def _generate_batch_id(self) -> str:
        """Generate unique batch ID"""
        import uuid
        return str(uuid.uuid4())
