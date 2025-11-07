# File Loading Framework - FileLoader (Coordinator)
# Coordinates file discovery and batch reading
# Main entry point for the ingestion framework

import logging
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from ingen_fab.python_libs.pyspark.ingestion.batch_reader import BatchReader
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.file_discovery import FileDiscovery
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo, lakehouse_utils

# Configure logging
logger = logging.getLogger(__name__)


class FileLoader:
    """
    Coordinates file discovery and batch reading.

    This is the main entry point for file loading. It composes:
    - FileDiscovery: Finds files/folders and creates BatchInfo objects
    - BatchReader: Reads batches into DataFrames

    This class maintains backward compatibility while delegating
    all work to specialized components.
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
            spark: Spark session for reading files and querying log tables
            config: ResourceConfig with file loading settings
            lakehouse_utils_instance: Optional pre-configured lakehouse_utils instance
                                     (if not provided, will create from config)
        """
        self.spark = spark
        self.config = config

        # Initialize components
        self.discovery = FileDiscovery(spark, config, lakehouse_utils_instance)
        self.reader = BatchReader(spark, config, lakehouse_utils_instance)

        # Configure logging if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s - %(message)s',
                force=True
            )

    @property
    def last_duplicate_items(self) -> List[FileInfo]:
        """
        Get list of duplicate items from last discovery operation.

        Forwards to FileDiscovery component.
        """
        return self.discovery.last_duplicate_items

    def discover_and_read_files(self) -> List[Tuple[BatchInfo, DataFrame, ProcessingMetrics]]:
        """
        Main entry point: discover files from raw layer and read them into DataFrames.

        Returns:
            List of (BatchInfo, DataFrame, ProcessingMetrics) tuples
        """
        # Step 1: Discover files from raw layer
        discovered_batches = self.discovery.discover()

        if not discovered_batches:
            return []

        # Step 2: Read each discovered batch
        results = []
        for batch_info in discovered_batches:
            df, metrics = self.reader.read_batch(batch_info)
            results.append((batch_info, df, metrics))

        return results

    def discover_files(self) -> List[BatchInfo]:
        """
        Discover files based on configuration.

        Delegates to FileDiscovery component.

        Returns:
            List of BatchInfo objects representing discovered batches
        """
        return self.discovery.discover()
