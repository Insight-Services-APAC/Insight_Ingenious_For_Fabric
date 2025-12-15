"""Base interface for all exporters with self-registration."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
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
    def read_source(
        self,
        run_date: Optional[datetime] = None,
        watermark_value: Optional[str] = None,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None,
    ) -> DataFrame:
        """
        Read data from source and return DataFrame.

        Supports placeholder substitution in source_query:
        - {run_date} - Logical date for export (ISO format)
        - {watermark} - Incremental watermark value
        - {period_start} - Period start date (ISO format)
        - {period_end} - Period end date (ISO format)

        Args:
            run_date: Logical date for the export run (for {run_date} placeholder).
            watermark_value: Optional watermark for incremental exports.
                            If provided, only returns rows where incremental_column > watermark_value.
            period_start: Optional start date for period-based exports.
            period_end: Optional end date for period-based exports.
                       If both provided with period_filter_column, filters rows in date range.

        Returns:
            DataFrame containing the source data
        """
        ...

    def _get_select_columns(self) -> str:
        """Get columns for SELECT clause, auto-including incremental_column if needed."""
        source = self.config.source_config

        if not source.source_columns:
            return "*"

        columns = list(source.source_columns)

        # Auto-include incremental_column for watermark calculation
        if (
            self.config.extract_type == "incremental"
            and self.config.incremental_column
            and self.config.incremental_column not in columns
        ):
            columns.append(self.config.incremental_column)

        return ", ".join(columns)


def get_registered_exporters() -> Dict[str, Type[BaseExporter]]:
    """Get all registered exporters."""
    return dict(_EXPORTER_REGISTRY)
