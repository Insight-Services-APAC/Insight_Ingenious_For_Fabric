"""
Configurable Synthetic Data Generator - Enhanced PySpark Implementation

This module provides a configurable synthetic data generator that supports
runtime parameters, flexible configuration, and enhanced logging capabilities.
"""

# ruff: noqa: I001

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional, Union

try:
    from pyspark.sql import DataFrame  # noqa: F401
    from pyspark.sql.functions import col, concat, expr, lit, rand, randn, when  # noqa: F401

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

# Import enhanced utilities
try:
    from ingen_fab.python_libs.common.file_path_utils import (
        DateBasedFilePathGenerator,
        FilePathManager,
    )
    from ingen_fab.python_libs.common.synthetic_data_config import (
        ConfigurationManager,
        DatasetConfiguration,
        TableGenerationConfig,
    )
    from ingen_fab.python_libs.common.synthetic_data_logger import (
        SyntheticDataLogger,
        TableGenerationMetrics,  # noqa: F401
    )
    from ingen_fab.python_libs.pyspark.incremental_synthetic_data_utils import (
        IncrementalSyntheticDataGenerator,
    )
    from ingen_fab.python_libs.pyspark.synthetic_data_utils import (
        PySparkDatasetBuilder,
        PySparkSyntheticDataGenerator,
    )

    ENHANCED_UTILS_AVAILABLE = True
except ImportError:
    ENHANCED_UTILS_AVAILABLE = False


class ConfigurableSyntheticDataGenerator:
    """
    Enhanced synthetic data generator with runtime configuration support.

    This generator extends the existing functionality with:
    - Runtime parameter overrides
    - Flexible file path patterns
    - Enhanced logging and metrics
    - Configuration-driven generation
    """

    def __init__(
        self,
        lakehouse_utils_instance=None,
        base_config: Union[DatasetConfiguration, Dict[str, Any]] = None,
        enhanced_logging: bool = True,
        seed: Optional[int] = None,
    ):
        self.lakehouse_utils = lakehouse_utils_instance
        self.seed = seed

        # Initialize enhanced utilities
        if ENHANCED_UTILS_AVAILABLE:
            self.config_manager = ConfigurationManager()
            self.file_path_generator = DateBasedFilePathGenerator()
            self.file_path_manager = FilePathManager(self.file_path_generator)

            if enhanced_logging:
                self.logger = SyntheticDataLogger(
                    logger_name=f"{__name__}.{self.__class__.__name__}",
                    enable_console_output=True,
                )
                self.enhanced_logging = True
            else:
                self.logger = logging.getLogger(__name__)
                self.enhanced_logging = False
        else:
            self.config_manager = None
            self.file_path_generator = None
            self.file_path_manager = None
            self.logger = logging.getLogger(__name__)
            self.enhanced_logging = False

        # Set base configuration
        if base_config:
            if isinstance(base_config, dict) and ENHANCED_UTILS_AVAILABLE:
                self.base_config = DatasetConfiguration.from_dict(base_config)
            elif ENHANCED_UTILS_AVAILABLE:
                self.base_config = base_config
            else:
                self.base_config = base_config
        else:
            self.base_config = None

        # Initialize underlying generators
        if lakehouse_utils_instance:
            self.base_generator = PySparkSyntheticDataGenerator(
                lakehouse_utils_instance=lakehouse_utils_instance, seed=seed
            )
            self.incremental_generator = IncrementalSyntheticDataGenerator(
                lakehouse_utils_instance=lakehouse_utils_instance,
                seed=seed,
                enhanced_logging=enhanced_logging,
            )
            self.dataset_builder = PySparkDatasetBuilder(self.base_generator)
        else:
            self.base_generator = None
            self.incremental_generator = None
            self.dataset_builder = None

    def generate_dataset_from_config(
        self,
        dataset_config_id: str = None,
        dataset_config: Union[DatasetConfiguration, Dict[str, Any]] = None,
        generation_date: Union[str, date] = None,
        runtime_overrides: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Generate dataset with flexible configuration options.

        Args:
            dataset_config_id: ID of predefined configuration to use
            dataset_config: Direct configuration object or dict
            generation_date: Date to generate data for (defaults to today)
            runtime_overrides: Runtime parameter overrides

        Returns:
            Generation results with enhanced metrics
        """
        if not ENHANCED_UTILS_AVAILABLE:
            raise RuntimeError(
                "Enhanced utilities not available. Install required dependencies."
            )

        # Determine configuration to use
        if dataset_config_id:
            config = self.config_manager.get_predefined_config(dataset_config_id)
            if not config:
                raise ValueError(
                    f"Predefined configuration '{dataset_config_id}' not found"
                )
        elif dataset_config:
            if isinstance(dataset_config, dict):
                config = DatasetConfiguration.from_dict(dataset_config)
            else:
                config = dataset_config
        elif self.base_config:
            config = self.base_config
        else:
            raise ValueError(
                "No configuration provided. Specify dataset_config_id, dataset_config, or set base_config"
            )

        # Set default generation date
        if generation_date is None:
            generation_date = date.today()
        elif isinstance(generation_date, str):
            generation_date = datetime.strptime(generation_date, "%Y-%m-%d").date()

        # Apply runtime overrides
        if runtime_overrides:
            config.apply_runtime_overrides(runtime_overrides)

        # Validate configuration
        issues = self.config_manager.validate_configuration(config)
        if issues:
            raise ValueError(f"Configuration validation failed: {issues}")

        # Use incremental generator for enhanced generation
        return self.incremental_generator.generate_dataset_from_config(
            dataset_config=config,
            generation_date=generation_date,
            runtime_overrides=runtime_overrides,
        )

    def generate_dataset_with_runtime_scaling(
        self,
        base_config_id: str,
        generation_date: Union[str, date],
        scale_factor: float = 1.0,
        seasonal_adjustments: Dict[str, float] = None,
        table_multipliers: Dict[str, float] = None,
    ) -> Dict[str, Any]:
        """
        Generate dataset with runtime scaling applied to row counts.

        Args:
            base_config_id: Base configuration to scale
            generation_date: Date to generate for
            scale_factor: Global scale factor for all tables
            seasonal_adjustments: Day-of-week seasonal adjustments
            table_multipliers: Per-table multipliers

        Returns:
            Generation results
        """
        if not ENHANCED_UTILS_AVAILABLE:
            raise RuntimeError("Enhanced utilities not available")

        # Build runtime overrides
        runtime_overrides = {}

        # Apply global scale factor
        if scale_factor != 1.0:
            runtime_overrides["global_scale_factor"] = scale_factor

        # Apply seasonal adjustments
        if seasonal_adjustments:
            runtime_overrides.setdefault("incremental_config", {})[
                "seasonal_multipliers"
            ] = seasonal_adjustments

        # Apply table-specific multipliers
        if table_multipliers:
            runtime_overrides["table_configs"] = {}
            for table_name, multiplier in table_multipliers.items():
                runtime_overrides["table_configs"][table_name] = {
                    "row_multiplier": multiplier
                }

        return self.generate_dataset_from_config(
            dataset_config_id=base_config_id,
            generation_date=generation_date,
            runtime_overrides=runtime_overrides,
        )

    def generate_custom_dataset(
        self,
        dataset_id: str,
        table_definitions: List[Dict[str, Any]],
        generation_date: Union[str, date] = None,
        output_settings: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Generate a completely custom dataset from table definitions.

        Args:
            dataset_id: Unique identifier for the dataset
            table_definitions: List of table definition dictionaries
            generation_date: Date to generate for
            output_settings: Output configuration

        Returns:
            Generation results
        """
        if not ENHANCED_UTILS_AVAILABLE:
            raise RuntimeError("Enhanced utilities not available")

        # Create configuration from table definitions
        table_configs = {}
        for table_def in table_definitions:
            table_name = table_def["table_name"]
            table_configs[table_name] = TableGenerationConfig(
                table_name=table_name,
                table_type=table_def.get("table_type", "incremental"),
                frequency=table_def.get("frequency", "daily"),
                base_rows=table_def.get("base_rows", 10000),
                base_rows_per_day=table_def.get("base_rows_per_day", 1000),
                seasonal_enabled=table_def.get("seasonal_enabled", True),
                date_columns=table_def.get("date_columns", ["created_date"]),
                primary_date_column=table_def.get(
                    "primary_date_column", "created_date"
                ),
            )

        # Create dataset configuration
        config = DatasetConfiguration(
            dataset_id=dataset_id,
            dataset_name=f"Custom Dataset - {dataset_id}",
            dataset_type="custom",
            schema_pattern="custom",
            table_configs=table_configs,
        )

        # Apply output settings
        if output_settings:
            config.output_settings.update(output_settings)

        return self.generate_dataset_from_config(
            dataset_config=config, generation_date=generation_date
        )

    def generate_date_series(
        self,
        dataset_config_id: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
        batch_size: int = 30,
        runtime_overrides: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Generate dataset for a series of dates.

        Args:
            dataset_config_id: Configuration to use
            start_date: Start date of series
            end_date: End date of series
            batch_size: Number of days to process at once
            runtime_overrides: Runtime parameter overrides

        Returns:
            Aggregated results for the entire series
        """
        if not ENHANCED_UTILS_AVAILABLE:
            raise RuntimeError("Enhanced utilities not available")

        config = self.config_manager.get_predefined_config(dataset_config_id)
        if not config:
            raise ValueError(f"Configuration '{dataset_config_id}' not found")

        if runtime_overrides:
            config.apply_runtime_overrides(runtime_overrides)

        return self.incremental_generator.generate_incremental_dataset_series(
            dataset_config=config,
            start_date=start_date,
            end_date=end_date,
            path_format=config.output_settings.get("path_format", "nested"),
            output_mode=config.output_settings.get("output_mode", "parquet"),
            batch_size=batch_size,
        )

    def preview_generation_plan(
        self,
        dataset_config_id: str = None,
        dataset_config: Union[DatasetConfiguration, Dict[str, Any]] = None,
        generation_date: Union[str, date] = None,
        runtime_overrides: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Preview what would be generated without actually generating data.

        Args:
            dataset_config_id: ID of predefined configuration
            dataset_config: Direct configuration
            generation_date: Date to generate for
            runtime_overrides: Runtime overrides

        Returns:
            Generation plan with estimated row counts and file paths
        """
        if not ENHANCED_UTILS_AVAILABLE:
            raise RuntimeError("Enhanced utilities not available")

        # Get configuration
        if dataset_config_id:
            config = self.config_manager.get_predefined_config(dataset_config_id)
        elif dataset_config:
            config = (
                DatasetConfiguration.from_dict(dataset_config)
                if isinstance(dataset_config, dict)
                else dataset_config
            )
        else:
            raise ValueError("Must provide dataset_config_id or dataset_config")

        if runtime_overrides:
            config.apply_runtime_overrides(runtime_overrides)

        if generation_date is None:
            generation_date = date.today()
        elif isinstance(generation_date, str):
            generation_date = datetime.strptime(generation_date, "%Y-%m-%d").date()

        # Calculate estimated row counts
        plan = {
            "dataset_id": config.dataset_id,
            "generation_date": generation_date.isoformat(),
            "tables": {},
            "total_estimated_rows": 0,
            "file_paths": {},
            "output_settings": config.output_settings,
        }

        seasonal_multipliers = config.incremental_config.get("seasonal_multipliers", {})

        for table_name, table_config in config.table_configs.items():
            estimated_rows = table_config.calculate_target_rows(
                generation_date, seasonal_multipliers=seasonal_multipliers
            )

            # Generate file path
            file_path = self.file_path_generator.generate_path(
                pattern=config.output_settings.get("path_format", "nested"),
                base_path=f"synthetic_data/{config.dataset_id}",
                table_name=table_name,
                generation_date=generation_date,
                file_extension=config.output_settings.get("file_extension", "parquet"),
            )

            plan["tables"][table_name] = {
                "table_type": table_config.table_type,
                "estimated_rows": estimated_rows,
                "frequency": table_config.frequency,
                "date_columns": table_config.date_columns,
                "primary_date_column": table_config.primary_date_column,
            }
            plan["file_paths"][table_name] = file_path
            plan["total_estimated_rows"] += estimated_rows

        return plan

    def get_available_configurations(self) -> Dict[str, str]:
        """Get all available predefined configurations."""
        if not ENHANCED_UTILS_AVAILABLE or not self.config_manager:
            return {}

        configs = {}
        for config_id in self.config_manager.get_available_configs():
            config = self.config_manager.get_predefined_config(config_id)
            configs[config_id] = config.description if config else "No description"

        return configs

    def get_file_path_patterns(self) -> Dict[str, str]:
        """Get all available file path patterns."""
        if not self.file_path_generator:
            return {}

        return self.file_path_generator.get_available_patterns()

    def create_config_from_template(
        self,
        template_id: str,
        new_dataset_id: str,
        customizations: Dict[str, Any] = None,
    ) -> DatasetConfiguration:
        """
        Create a new configuration based on a template.

        Args:
            template_id: ID of template configuration
            new_dataset_id: ID for the new configuration
            customizations: Custom overrides

        Returns:
            New configuration object
        """
        if not ENHANCED_UTILS_AVAILABLE:
            raise RuntimeError("Enhanced utilities not available")

        return self.config_manager.create_config_from_template(
            template_id=template_id, dataset_id=new_dataset_id, overrides=customizations
        )

    def export_generation_logs(self, output_path: str, format: str = "json"):
        """Export generation logs to file."""
        if self.enhanced_logging and hasattr(self.logger, "export_generation_logs"):
            self.logger.export_generation_logs(output_path, format)
        else:
            self.logger.info("Enhanced logging not available or enabled")

    def get_generation_statistics(self) -> Dict[str, Any]:
        """Get aggregated generation statistics."""
        if self.enhanced_logging and hasattr(self.logger, "get_generation_statistics"):
            return self.logger.get_generation_statistics()
        else:
            return {"message": "Enhanced logging not available or enabled"}


class ConfigurableDatasetBuilder:
    """High-level builder for creating datasets with runtime configuration."""

    def __init__(self, generator: ConfigurableSyntheticDataGenerator):
        self.generator = generator
        self.logger = generator.logger

    def build_retail_oltp_dataset(
        self,
        scale: Literal["small", "medium", "large"] = "medium",
        generation_date: Union[str, date] = None,
        seasonal_adjustments: Dict[str, float] = None,
    ) -> Dict[str, Any]:
        """Build a retail OLTP dataset with configurable scale."""

        scale_configs = {
            "small": {
                "global_scale": 0.1,
                "customers": 1000,
                "products": 100,
                "orders_per_day": 1000,
            },
            "medium": {
                "global_scale": 1.0,
                "customers": 10000,
                "products": 1000,
                "orders_per_day": 10000,
            },
            "large": {
                "global_scale": 10.0,
                "customers": 100000,
                "products": 10000,
                "orders_per_day": 100000,
            },
        }

        scale_config = scale_configs[scale]

        runtime_overrides = {
            "table_configs": {
                "customers": {"base_rows": scale_config["customers"]},
                "products": {"base_rows": scale_config["products"]},
                "orders": {"base_rows_per_day": scale_config["orders_per_day"]},
                "order_items": {
                    "base_rows_per_day": scale_config["orders_per_day"] * 2
                },  # 2 items per order avg
            }
        }

        if seasonal_adjustments:
            runtime_overrides["incremental_config"] = {
                "seasonal_multipliers": seasonal_adjustments
            }

        return self.generator.generate_dataset_from_config(
            dataset_config_id="retail_oltp_enhanced",
            generation_date=generation_date,
            runtime_overrides=runtime_overrides,
        )

    def build_analytics_dataset(
        self,
        fact_rows: int = 1000000,
        customer_count: int = 100000,
        product_count: int = 10000,
        generation_date: Union[str, date] = None,
    ) -> Dict[str, Any]:
        """Build an analytics dataset with configurable dimensions."""

        runtime_overrides = {
            "table_configs": {
                "dim_customer": {"base_rows": customer_count},
                "dim_product": {"base_rows": product_count},
                "fact_sales": {"base_rows_per_day": fact_rows},
            }
        }

        return self.generator.generate_dataset_from_config(
            dataset_config_id="retail_star_enhanced",
            generation_date=generation_date,
            runtime_overrides=runtime_overrides,
        )

    def build_time_series_dataset(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        dataset_config_id: str = "retail_oltp_enhanced",
        include_seasonality: bool = True,
    ) -> Dict[str, Any]:
        """Build a time series dataset across multiple dates."""

        runtime_overrides = {}
        if include_seasonality:
            runtime_overrides["incremental_config"] = {
                "seasonal_multipliers": {
                    "monday": 0.8,
                    "tuesday": 0.9,
                    "wednesday": 1.0,
                    "thursday": 1.1,
                    "friday": 1.3,
                    "saturday": 1.2,
                    "sunday": 0.7,
                }
            }

        return self.generator.generate_date_series(
            dataset_config_id=dataset_config_id,
            start_date=start_date,
            end_date=end_date,
            runtime_overrides=runtime_overrides,
        )
