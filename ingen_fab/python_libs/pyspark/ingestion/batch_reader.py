# Batch Reading Framework
# Reads batches into DataFrames with proper schema handling
# Returns DataFrames with processing metrics

import logging
import time
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    ErrorContext,
    FileReadError,
)
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# Configure logging
logger = logging.getLogger(__name__)


class BatchReader:
    """
    Reads batches into DataFrames.

    This class handles:
    - File reading with format-specific options
    - Schema handling (custom or inferred)
    - CSV/JSON/Parquet/etc format support
    - Read metrics tracking

    Returns DataFrames with processing metrics.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ResourceConfig,
        lakehouse_utils_instance: Optional[lakehouse_utils] = None,
    ):
        """
        Initialize BatchReader with configuration.

        Args:
            spark: Spark session for reading files
            config: ResourceConfig with file loading settings
            lakehouse_utils_instance: Optional pre-configured lakehouse_utils instance
                                     (if not provided, will create from config)
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

    def read_batch(self, batch_info: BatchInfo) -> Tuple[DataFrame, ProcessingMetrics]:
        """
        Read a batch into a DataFrame.

        Args:
            batch_info: BatchInfo describing what to read

        Returns:
            (DataFrame, ProcessingMetrics)
        """
        read_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Validate file_format
            if not self.config.file_format:
                raise ValueError("file_format is required")

            file_format = self.config.file_format

            # Get file reading options
            options = self._get_file_read_options()

            # Read batch - always just read from file_paths
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

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            # Note: Read log removed - combined with batch completion log in orchestrator

            return df, metrics

        except Exception as e:
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            self.logger.exception(f"Failed to read batch: {e}")

            # Get file path for context
            file_path = batch_info.file_paths[0] if batch_info.file_paths else "unknown"

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

    def _get_source_identifier(self, batch_info: BatchInfo) -> str:
        """
        Extract the full relative path from Files/ for logging.

        Args:
            batch_info: BatchInfo with source information

        Returns:
            Full relative path (e.g., "exports/incremental/EDL_HANA/2025/01/29")
        """
        if not batch_info.file_paths:
            return "unknown"

        import os
        file_path = batch_info.file_paths[0]

        # Extract path after "Files/"
        if "/Files/" in file_path:
            return file_path.split("/Files/", 1)[1]

        # If no Files/ in path, return basename as fallback
        return os.path.basename(file_path)

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

            # Schema handling
            if self.config.custom_schema_json:
                import json
                schema_dict = json.loads(self.config.custom_schema_json)
                options["schema"] = StructType.fromJson(schema_dict)
            else:
                options["inferSchema"] = True

        return options
