"""
Incremental Synthetic Data Generation Module

DEPRECATION NOTICE:
This module is deprecated and will be removed in a future version.
The functionality has been moved to the unified synthetic data generation system.
Please use UnifiedSyntheticDataGenerator from unified_commands.py instead.

This module extends the existing synthetic data generation package to support
time-based incremental data generation with configurable snapshot and incremental
table types, date-partitioned folder structures, and state management.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Literal, Union

from ...python_libs.common.synthetic_data_dataset_configs import (
    DatasetConfigurationRepository,
)
from .synthetic_data_generation import SyntheticDataGenerationCompiler


class IncrementalSyntheticDataGenerationCompiler(SyntheticDataGenerationCompiler):
    """
    [DEPRECATED] Compiler for incremental synthetic data generation notebooks and configurations.

    This class is deprecated. Please use UnifiedSyntheticDataGenerator from unified_commands.py instead.
    """

    def __init__(
        self,
        fabric_workspace_repo_dir: str = None,
        fabric_environment: str = None,
        **kwargs,
    ):
        """Initialize the incremental synthetic data generation compiler."""
        import warnings

        warnings.warn(
            "IncrementalSyntheticDataGenerationCompiler is deprecated. Use UnifiedSyntheticDataGenerator instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(fabric_workspace_repo_dir, fabric_environment, **kwargs)

    def compile_incremental_dataset_notebook(
        self,
        dataset_config: Dict[str, Any],
        generation_date: Union[str, date] = None,
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        output_subdir: str = None,
        path_format: Literal["nested", "flat"] = "nested",
        state_management: bool = True,
    ) -> Path:
        """
        Compile an incremental synthetic data generation notebook.

        Args:
            dataset_config: Configuration dictionary for the dataset with incremental settings
            generation_date: Target date for generation (defaults to today)
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            output_subdir: Optional subdirectory for output
            path_format: Path format ("nested" for /YYYY/MM/DD/ or "flat" for YYYYMMDD_)
            state_management: Whether to enable state management for consistency

        Returns:
            Path to the generated notebook
        """
        if generation_date is None:
            generation_date = date.today()
        elif isinstance(generation_date, str):
            generation_date = datetime.strptime(generation_date, "%Y-%m-%d").date()

        # Enhance dataset config with incremental settings
        enhanced_config = self._enhance_config_for_incremental(
            dataset_config, generation_date, path_format
        )

        # Set generation mode based on target environment
        if target_environment == "lakehouse":
            generation_mode = "pyspark"
            language_group = "synapse_pyspark"
        else:  # warehouse
            generation_mode = "python"
            language_group = "python"

        # Use unified template
        template_name = "synthetic_data_base_notebook.py.jinja"

        target_datastore_config_prefix = "config"
        if target_environment == "warehouse":
            target_datastore_config_prefix = "config_wh"

        # Prepare template variables
        template_vars = {
            "target_lakehouse_config_prefix": target_datastore_config_prefix,
            "dataset_config": enhanced_config,
            "generation_mode": generation_mode,
            "target_environment": target_environment,
            "language_group": language_group,
            "dataset_id": enhanced_config.get("dataset_id", "custom_dataset"),
            "generation_date": generation_date.isoformat(),
            "path_format": path_format,
            "state_management": state_management,
            "incremental_config": enhanced_config.get("incremental_config", {}),
            "table_configs": enhanced_config.get("table_configs", {}),
        }

        # Generate notebook name
        dataset_id = enhanced_config.get("dataset_id", "custom")
        date_str = generation_date.strftime("%Y%m%d")
        notebook_name = (
            f"incremental_synthetic_data_{dataset_id}_{date_str}_{generation_mode}"
        )

        # Set default output_subdir if not provided
        if output_subdir is None:
            output_subdir = f"synthetic_data_generation/incremental/{dataset_id}"

        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=f"Incremental Synthetic Data - {enhanced_config.get('dataset_name', dataset_id)} ({generation_date})",
            description=f"Generate incremental synthetic data for {dataset_id} on {generation_date} using {generation_mode}",
            output_subdir=output_subdir,
        )

    def compile_incremental_dataset_series_notebook(
        self,
        dataset_config: Dict[str, Any],
        start_date: Union[str, date],
        end_date: Union[str, date],
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        output_subdir: str = None,
        path_format: Literal["nested", "flat"] = "nested",
        batch_size: int = 30,  # Generate 30 days at a time by default
        output_mode: str = "parquet",
        ignore_state: bool = False,
    ) -> Path:
        """
        Compile a notebook that generates a series of incremental datasets.

        Args:
            dataset_config: Configuration dictionary for the dataset
            start_date: Start date for the series
            end_date: End date for the series
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            output_subdir: Optional subdirectory for output
            path_format: Path format ("nested" for /YYYY/MM/DD/ or "flat" for YYYYMMDD_)
            batch_size: Number of days to process in each batch
            output_mode: Output format ("parquet", "csv", or "table")
            ignore_state: Whether to ignore existing state and regenerate all files

        Returns:
            Path to the generated notebook
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Calculate date range
        date_range = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_days": (end_date - start_date).days + 1,
            "batch_size": batch_size,
        }

        # Enhance dataset config for series generation
        enhanced_config = self._enhance_config_for_incremental(
            dataset_config, start_date, path_format
        )
        enhanced_config["date_range"] = date_range
        enhanced_config["series_generation"] = True

        # Set generation mode based on target environment
        if target_environment == "lakehouse":
            generation_mode = "pyspark"
            language_group = "synapse_pyspark"
        else:  # warehouse
            generation_mode = "python"
            language_group = "python"

        # Use unified template
        template_name = "synthetic_data_base_notebook.py.jinja"

        target_datastore_config_prefix = "config"
        if target_environment == "warehouse":
            target_datastore_config_prefix = "config_wh"

        # Prepare template variables
        template_vars = {
            "target_lakehouse_config_prefix": target_datastore_config_prefix,
            "dataset_config": enhanced_config,
            "generation_mode": generation_mode,
            "target_environment": target_environment,
            "language_group": language_group,
            "dataset_id": enhanced_config.get("dataset_id", "custom_dataset"),
            "batch_size": batch_size,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "date_range": date_range,
            "path_format": path_format,
            "output_mode": output_mode,
            "ignore_state": ignore_state,
            "incremental_config": enhanced_config.get("incremental_config", {}),
            "table_configs": enhanced_config.get("table_configs", {}),
        }

        # Generate notebook name
        dataset_id = enhanced_config.get("dataset_id", "custom")
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        notebook_name = f"incremental_series_synthetic_data_{dataset_id}_{start_str}_{end_str}_{generation_mode}"

        # Set default output_subdir if not provided
        if output_subdir is None:
            output_subdir = f"synthetic_data_generation/incremental_series/{dataset_id}"

        print(
            "💡 [NOTICE] Consider using the new generic template system for better flexibility!"
        )
        print(
            "   Use 'compile-generic-templates' and 'execute-with-parameters' commands."
        )
        print(
            f"   Example: python -m ingen_fab.cli package synthetic-data compile-generic-templates --target-environment {target_environment}"
        )
        print(
            f"   Then: python -m ingen_fab.cli package synthetic-data execute-with-parameters generic_incremental_series_{target_environment} --dataset-id {dataset_id} --start-date {start_date} --end-date {end_date}"
        )
        print()

        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=f"Incremental Series Synthetic Data - {enhanced_config.get('dataset_name', dataset_id)} ({start_date} to {end_date})",
            description=f"Generate incremental synthetic data series for {dataset_id} from {start_date} to {end_date} using {generation_mode}",
            output_subdir=output_subdir,
        )

    def _enhance_config_for_incremental(
        self, dataset_config: Dict[str, Any], generation_date: date, path_format: str
    ) -> Dict[str, Any]:
        """Enhance dataset configuration with incremental settings."""
        enhanced_config = dataset_config.copy()
        dataset_id = enhanced_config.get("dataset_id", "custom")

        # Add incremental configuration if not present
        if "incremental_config" not in enhanced_config:
            # Get from repository with domain-specific adjustments
            enhanced_config["incremental_config"] = (
                DatasetConfigurationRepository.get_incremental_config(
                    dataset_id, enhanced_config.get("incremental_config")
                )
            )

        # Add table configurations if not present
        if "table_configs" not in enhanced_config:
            enhanced_config["table_configs"] = self._get_default_table_configs(
                enhanced_config
            )

        # Add date-specific configuration
        enhanced_config["generation_date"] = generation_date.isoformat()
        enhanced_config["path_format"] = path_format

        return enhanced_config

    def _get_default_incremental_config(self) -> Dict[str, Any]:
        """Get default incremental configuration from repository."""
        return DatasetConfigurationRepository.DEFAULT_INCREMENTAL_CONFIG.copy()

    def _get_default_table_configs(
        self, dataset_config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Get default table configurations based on dataset type."""
        dataset_id = dataset_config.get("dataset_id", "custom")
        try:
            # Try to get table configs from repository
            return DatasetConfigurationRepository.get_table_configs(dataset_id)
        except ValueError:
            # Fall back to generic configs based on schema pattern
            schema_pattern = dataset_config.get("schema_pattern", "oltp")
            return DatasetConfigurationRepository.TABLE_CONFIGS_BY_PATTERN.get(
                schema_pattern,
                {
                    "main_table": {
                        "type": "incremental",
                        "frequency": "daily",
                        "base_rows_per_day": 10000,
                        "seasonal_multipliers_enabled": True,
                    }
                },
            )

    def _calculate_total_incremental_rows(self, dataset_config: Dict[str, Any]) -> int:
        """Calculate total incremental rows for a single day."""
        table_configs = dataset_config.get("table_configs", {})
        total_rows = 0

        for table_name, config in table_configs.items():
            if config.get("type") == "incremental":
                base_rows = config.get("base_rows_per_day", 10000)
                total_rows += base_rows

        return total_rows

    def get_enhanced_predefined_dataset_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get predefined dataset configurations enhanced for incremental generation."""
        base_configs = self._get_predefined_dataset_configs()

        # Enhance each configuration with incremental settings
        enhanced_configs = {}
        for dataset_id, config in base_configs.items():
            enhanced_config = config.copy()
            enhanced_config["incremental_config"] = (
                DatasetConfigurationRepository.get_incremental_config(dataset_id)
            )
            enhanced_config["table_configs"] = (
                DatasetConfigurationRepository.get_table_configs(dataset_id)
            )
            enhanced_configs[dataset_id] = enhanced_config

        return enhanced_configs

    def _get_incremental_dataset_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get predefined incremental dataset configurations."""
        # For incremental-specific datasets, we can enhance base configs
        base_configs = self._get_predefined_dataset_configs()
        incremental_configs = {}

        # Add incremental suffix to existing configs and enhance them
        for dataset_id, config in base_configs.items():
            if "_incremental" not in dataset_id:
                incremental_id = f"{dataset_id}_incremental"
                incremental_config = config.copy()
                incremental_config["dataset_id"] = incremental_id
                incremental_config["dataset_name"] = config["dataset_name"].replace(
                    " -", " Incremental -"
                )
                incremental_config["description"] = (
                    config["description"] + " with incremental generation"
                )

                # Add incremental-specific configurations
                incremental_config["incremental_config"] = (
                    DatasetConfigurationRepository.get_incremental_config(dataset_id)
                )
                incremental_config["table_configs"] = (
                    DatasetConfigurationRepository.get_table_configs(
                        dataset_id, scale_factor=5.0
                    )
                )  # Scale up for incremental

                incremental_configs[incremental_id] = incremental_config

        return incremental_configs


def compile_incremental_synthetic_data_package(
    fabric_workspace_repo_dir: str = None,
    template_vars: Dict[str, Any] = None,
    include_samples: bool = False,
    target_environment: str = "lakehouse",
) -> Dict[str, Any]:
    """Main function to compile the incremental synthetic data package"""

    compiler = IncrementalSyntheticDataGenerationCompiler(fabric_workspace_repo_dir)

    # For now, we'll use the base compile method but could extend this
    # to include incremental-specific DDL scripts and sample notebooks
    return compiler.compile_all_synthetic_data_notebooks(
        target_environment=target_environment
    )
