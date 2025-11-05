# Batch Reading Framework
# Reads batches into DataFrames with proper schema handling
# Returns DataFrames with processing metrics

import logging
import time
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from ingen_fab.python_libs.pyspark.ingestion.config import (
    FileSystemLoadingParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.constants import ImportPattern
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
            # Validate source_file_format
            if not self.config.source_file_format:
                raise ValueError("source_file_format is required")

            file_format = self.config.source_file_format

            # Get file reading options
            options = self._get_file_read_options()

            # Read based on import_pattern
            if self.params.import_pattern == ImportPattern.INCREMENTAL_FOLDERS:
                # Read all files in folder
                df = self.lakehouse.read_file(
                    file_path=batch_info.file_paths[0],
                    file_format=file_format,
                    options=options,
                )
            elif len(batch_info.file_paths) == 1:
                # Single file
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

                # Apply CSV options
                if file_format.lower() == "csv":
                    for key, value in options.items():
                        if key != "schema":
                            reader = reader.option(key, value)

                df = reader.load(batch_info.file_paths)

            # Calculate metrics
            read_end = time.time()
            metrics.read_duration_ms = int((read_end - read_start) * 1000)
            metrics.source_row_count = df.count()
            metrics.records_processed = metrics.source_row_count

            self.logger.info(
                f"Read {metrics.source_row_count} records in {metrics.read_duration_ms}ms"
            )

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
                        "file_format": self.config.source_file_format,
                        "import_pattern": self.params.import_pattern,
                    },
                ),
            ) from e

    def _get_file_read_options(self) -> dict:
        """Build file reading options from configuration"""
        options = {}

        if not self.config.source_file_format:
            return options

        if self.config.source_file_format.lower() == "csv":
            options["header"] = str(self.params.has_header).lower()
            options["sep"] = self.params.file_delimiter
            options["encoding"] = self.params.encoding
            options["quote"] = self.params.quote_character
            options["escape"] = self.params.escape_character
            options["multiLine"] = str(self.params.multiline_values).lower()

            # Schema handling
            if self.config.custom_schema_json:
                import json
                schema_dict = json.loads(self.config.custom_schema_json)
                options["schema"] = StructType.fromJson(schema_dict)
            else:
                options["inferSchema"] = "true"

        return options
