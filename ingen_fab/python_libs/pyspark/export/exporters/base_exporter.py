"""Base interface for all exporters with self-registration."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

from pyspark.sql import DataFrame, SparkSession

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig

# Registry for auto-registration of exporter classes
_EXPORTER_REGISTRY: Dict[str, Type["BaseExporter"]] = {}

logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """
    Abstract base class for all exporters with self-registration.

    Exporters self-register when defined by specifying source_type in class definition:
        class LakehouseExporter(BaseExporter, source_type="lakehouse"):
            ...

    Use BaseExporter.create() to instantiate the correct exporter for a config.
    """

    # Common instance variables
    config: ExportConfig
    spark: SparkSession
    logger: logging.Logger

    def __init_subclass__(cls, source_type: str | None = None, **kwargs):
        """Auto-register subclasses that specify a source_type."""
        super().__init_subclass__(**kwargs)
        if source_type is not None:
            _EXPORTER_REGISTRY[source_type] = cls
            logger.debug(f"Registered exporter for source_type: {source_type}")

    @classmethod
    def create(cls, config: ExportConfig, spark: SparkSession) -> "BaseExporter":
        """
        Factory method - create the correct exporter for the given config.

        Looks up the registered exporter class for config.source_config.source_type
        and delegates to its from_config() method.

        Args:
            config: ExportConfig with source_type
            spark: SparkSession instance

        Returns:
            Exporter instance ready to use

        Raises:
            ValueError: If no exporter is registered for the source_type
        """
        source_type = config.source_config.source_type
        exporter_cls = _EXPORTER_REGISTRY.get(source_type)
        if not exporter_cls:
            registered = ", ".join(_EXPORTER_REGISTRY.keys()) or "(none)"
            raise ValueError(
                f"No exporter registered for source_type: '{source_type}'. "
                f"Registered types: {registered}"
            )
        return exporter_cls.from_config(config, spark)

    @classmethod
    @abstractmethod
    def from_config(cls, config: ExportConfig, spark: SparkSession) -> "BaseExporter":
        """
        Construct exporter from config (production constructor).

        Each subclass implements this to create real dependencies and return
        a fully initialized exporter.

        Args:
            config: ExportConfig with source configuration
            spark: SparkSession instance

        Returns:
            Fully initialized exporter instance
        """
        ...

    @abstractmethod
    def read_source(self, watermark_value: Optional[str] = None) -> DataFrame:
        """
        Read data from source and return DataFrame.

        Args:
            watermark_value: Optional watermark for incremental exports.
                            If provided, only returns rows where incremental_column > watermark_value.

        Returns:
            DataFrame containing the source data
        """
        ...


def get_registered_exporters() -> Dict[str, Type[BaseExporter]]:
    """Get all registered exporters."""
    return dict(_EXPORTER_REGISTRY)
