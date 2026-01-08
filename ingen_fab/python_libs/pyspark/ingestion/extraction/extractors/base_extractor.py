"""Base interface for all extractors with self-registration."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generator, Generic, List, TypeVar

from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    BaseExtractionParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.common.results import BatchExtractionResult
from ingen_fab.python_libs.pyspark.ingestion.extraction.extraction_logger import (
    ExtractionLogger,
)

# Generic type parameter for extraction params (bound to BaseExtractionParams)
TExtractionParams = TypeVar('TExtractionParams', bound=BaseExtractionParams)


class BaseExtractor(ABC, Generic[TExtractionParams]):
    """
    Abstract base class for all extractors with self-registration.

    Extractors self-register when defined by specifying source_type in class definition:
        class FileSystemExtractor(BaseExtractor, source_type="filesystem"):
            ...

    Use BaseExtractor.create() to instantiate the correct extractor for a config.

    Type Parameters:
        TExtractionParams: The specific extraction params type for this extractor
                          (FileSystemExtractionParams, etc.)
    """

    # Registry of source_type -> extractor class (populated by __init_subclass__)
    _registry: dict[str, type[BaseExtractor]] = {}

    # Common instance variables (defined in __init__ of concrete classes)
    extraction_params: TExtractionParams
    config: ResourceConfig
    extraction_logger: ExtractionLogger
    logger: logging.Logger

    def __init_subclass__(cls, source_type: str | None = None, **kwargs):
        """Auto-register subclasses that specify a source_type."""
        super().__init_subclass__(**kwargs)
        if source_type is not None:
            BaseExtractor._registry[source_type] = cls

    @classmethod
    def create(
        cls,
        config: ResourceConfig,
        extraction_logger: ExtractionLogger,
    ) -> BaseExtractor:
        """
        Factory method - create the correct extractor for the given config.

        Looks up the registered extractor class for config.source_config.source_type
        and delegates to its from_config() method.

        Args:
            config: ResourceConfig with source_type
            extraction_logger: ExtractionLogger instance

        Returns:
            Extractor instance ready to use

        Raises:
            ValueError: If no extractor is registered for the source_type
        """
        source_type = config.source_config.source_type
        extractor_cls = cls._registry.get(source_type)
        if not extractor_cls:
            registered = ", ".join(cls._registry.keys()) or "(none)"
            raise ValueError(
                f"No extractor registered for source_type: '{source_type}'. "
                f"Registered types: {registered}"
            )
        return extractor_cls.from_config(config, extraction_logger)

    @classmethod
    @abstractmethod
    def from_config(
        cls,
        config: ResourceConfig,
        extraction_logger: ExtractionLogger,
    ) -> BaseExtractor:
        """
        Construct extractor from config (production constructor).

        Each subclass implements this to create real dependencies and return
        a fully initialized extractor.

        Args:
            config: ResourceConfig with source configuration
            extraction_logger: ExtractionLogger instance

        Returns:
            Fully initialized extractor instance
        """
        ...

    @abstractmethod
    def extract(self) -> Generator[BatchExtractionResult, None, None]:
        """
        Extract data from source and yield batches.

        Each batch should have:
        - status: ExecutionStatus (SUCCESS, WARNING, or ERROR)
        - error_message: Optional explanation for WARNING/ERROR batches
        - extract_file_paths: Files/folders promoted to raw layer
        - file_count, file_size_bytes: Batch metrics

        Yields:
            BatchExtractionResult: One result per batch extracted
        """
        ...

    def _build_hive_partition_path(self, partition_columns: List[str]) -> str:
        """
        Build Hive partition path from current date and partition columns.

        Args:
            partition_columns: List of partition column names (1, 3, or 4 columns)

        Returns:
            Hive partition path like "date=2025-11-22/" or "year=2025/month=11/day=22/"

        Raises:
            ValueError: If partition_columns has invalid length
        """
        now = datetime.now()
        num_cols = len(partition_columns)

        if num_cols == 1:
            # Single date column: date=2025-11-22/
            return f"{partition_columns[0]}={now.strftime('%Y-%m-%d')}/"

        elif num_cols == 3:
            # Hierarchical date: year=2025/month=11/day=22/
            return (
                f"{partition_columns[0]}={now.year}/"
                f"{partition_columns[1]}={now.month:02d}/"
                f"{partition_columns[2]}={now.day:02d}/"
            )

        elif num_cols == 4:
            # Hierarchical datetime: year=2025/month=11/day=22/hour=14/
            return (
                f"{partition_columns[0]}={now.year}/"
                f"{partition_columns[1]}={now.month:02d}/"
                f"{partition_columns[2]}={now.day:02d}/"
                f"{partition_columns[3]}={now.hour:02d}/"
            )

        else:
            raise ValueError(
                f"partition_columns must have 1, 3, or 4 items. "
                f"Got {num_cols}: {partition_columns}"
            )

    def _build_batch_path(self, batch_id: str) -> tuple[str, str]:
        """
        Build relative and full paths including Hive partitions and batch_id.

        Creates consistent path structure across all extractors:
            {extract_path}/{partition}/{batch_id={uuid}}/

        Args:
            batch_id: UUID string for this batch

        Returns:
            Tuple of (relative_path, full_path) both ending with /
        """
        partition_path = self._build_hive_partition_path(self.config.extract_partition_columns)
        batch_partition = f"batch_id={batch_id}/"

        relative_path = f"{self.config.extract_path.strip('/')}/{partition_path}{batch_partition}"
        full_path = f"{self.config.extract_full_path}/{partition_path}{batch_partition}"

        return relative_path, full_path
