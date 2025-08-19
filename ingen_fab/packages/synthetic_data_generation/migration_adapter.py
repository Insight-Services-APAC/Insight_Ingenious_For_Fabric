"""
Migration Adapter for Synthetic Data Generation

This module provides backward compatibility for applications using the old
synthetic data generation API while redirecting them to the new unified system.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .unified_commands import GenerationMode, UnifiedSyntheticDataGenerator


class LegacyCompilerAdapter:
    """Adapter that provides backward compatibility for the old compiler interface."""

    def __init__(
        self, fabric_workspace_repo_dir: str = None, fabric_environment: str = None
    ):
        """Initialize the adapter with the same interface as the old compiler."""
        self.fabric_workspace_repo_dir = fabric_workspace_repo_dir
        self.fabric_environment = fabric_environment
        self._generator = UnifiedSyntheticDataGenerator(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            fabric_environment=fabric_environment,
        )

        # Issue deprecation warning
        warnings.warn(
            "LegacyCompilerAdapter is deprecated. Please use UnifiedSyntheticDataGenerator directly.",
            DeprecationWarning,
            stacklevel=2,
        )

    def compile_predefined_dataset_notebook(
        self,
        dataset_id: str,
        target_rows: int = 10000,
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        seed_value: Optional[int] = None,
        output_subdir: Optional[str] = None,
        output_mode: str = "table",
    ) -> Path:
        """Legacy method that redirects to the unified generator."""
        parameters = {
            "target_rows": target_rows,
            "generation_mode": generation_mode,
            "seed_value": seed_value,
            "output_mode": output_mode,
        }

        result = self._generator.generate(
            config=dataset_id,
            mode=GenerationMode.SINGLE,
            parameters=parameters,
            output_path=output_subdir,
            target_environment=target_environment,
        )

        if result["success"]:
            return result["result"]
        else:
            raise RuntimeError(f"Generation failed: {result['errors']}")

    def compile_enhanced_synthetic_data_notebook(
        self,
        dataset_config: Union[Dict[str, Any], Any],
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        output_subdir: Optional[str] = None,
        use_enhanced_template: bool = True,
        runtime_overrides: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Legacy enhanced method that redirects to the unified generator."""
        # Extract dataset ID from config
        if isinstance(dataset_config, dict):
            dataset_id = dataset_config.get("dataset_id", "custom")
            parameters = dataset_config.copy()
        else:
            dataset_id = dataset_config.dataset_id
            parameters = dataset_config.to_dict()

        # Apply runtime overrides
        if runtime_overrides:
            parameters.update(runtime_overrides)

        result = self._generator.generate(
            config=dataset_id,
            mode=GenerationMode.SINGLE,
            parameters=parameters,
            output_path=output_subdir,
            target_environment=target_environment,
        )

        if result["success"]:
            return result["result"]
        else:
            raise RuntimeError(f"Generation failed: {result['errors']}")

    def compile_all_synthetic_data_notebooks(
        self,
        datasets: Optional[list] = None,
        target_environment: str = "lakehouse",
        output_mode: str = "table",
    ) -> Dict[str, Any]:
        """Legacy method to compile all notebooks."""
        if datasets is None:
            # Get default datasets
            available = self._generator.list_items()
            datasets = list(available.get("datasets", {}).keys())[
                :3
            ]  # Limit to first 3

        results = {"success": True, "compiled_items": {}, "errors": []}

        for dataset_id in datasets:
            try:
                result = self._generator.generate(
                    config=dataset_id,
                    mode=GenerationMode.SINGLE,
                    parameters={"output_mode": output_mode},
                    target_environment=target_environment,
                )

                if result["success"]:
                    results["compiled_items"][
                        f"compile_predefined_dataset_notebook_{dataset_id}"
                    ] = result["result"]
                else:
                    results["errors"].extend(result["errors"])
                    results["success"] = False

            except Exception as e:
                results["errors"].append(str(e))
                results["success"] = False

        return results

    def compile_ddl_scripts(
        self, target_environment: str = "warehouse"
    ) -> Dict[str, Any]:
        """Legacy method to compile DDL scripts."""
        result = self._generator.compile(
            template="ddl_scripts",
            output_format="ddl",
            target_environment=target_environment,
        )

        if result["success"]:
            return result["compiled_items"].get("ddl", {})
        else:
            raise RuntimeError(f"DDL compilation failed: {result['errors']}")


class LegacyIncrementalCompilerAdapter:
    """Adapter for the old incremental compiler interface."""

    def __init__(
        self, fabric_workspace_repo_dir: str = None, fabric_environment: str = None
    ):
        """Initialize the incremental adapter."""
        self.fabric_workspace_repo_dir = fabric_workspace_repo_dir
        self.fabric_environment = fabric_environment
        self._generator = UnifiedSyntheticDataGenerator(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            fabric_environment=fabric_environment,
        )

        # Issue deprecation warning
        warnings.warn(
            "LegacyIncrementalCompilerAdapter is deprecated. Please use UnifiedSyntheticDataGenerator directly.",
            DeprecationWarning,
            stacklevel=2,
        )

    def compile_incremental_dataset_notebook(
        self,
        dataset_config: Dict[str, Any],
        generation_date: Union[str, Any] = None,
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        output_subdir: Optional[str] = None,
        path_format: str = "nested",
        state_management: bool = True,
    ) -> Path:
        """Legacy incremental method that redirects to the unified generator."""
        dataset_id = dataset_config.get("dataset_id", "custom")
        parameters = dataset_config.copy()
        parameters.update(
            {
                "generation_date": generation_date,
                "path_format": path_format,
                "state_management": state_management,
                "generation_mode": generation_mode,
            }
        )

        result = self._generator.generate(
            config=dataset_id,
            mode=GenerationMode.INCREMENTAL,
            parameters=parameters,
            output_path=output_subdir,
            target_environment=target_environment,
        )

        if result["success"]:
            return result["result"]
        else:
            raise RuntimeError(f"Generation failed: {result['errors']}")

    def compile_incremental_dataset_series_notebook(
        self,
        dataset_config: Dict[str, Any],
        start_date: Union[str, Any],
        end_date: Union[str, Any],
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        output_subdir: Optional[str] = None,
        path_format: str = "nested",
        batch_size: int = 10,
        output_mode: str = "parquet",
        ignore_state: bool = False,
    ) -> Path:
        """Legacy series method that redirects to the unified generator."""
        dataset_id = dataset_config.get("dataset_id", "custom")
        parameters = dataset_config.copy()
        parameters.update(
            {
                "start_date": start_date,
                "end_date": end_date,
                "batch_size": batch_size,
                "path_format": path_format,
                "output_mode": output_mode,
                "ignore_state": ignore_state,
                "generation_mode": generation_mode,
            }
        )

        result = self._generator.generate(
            config=dataset_id,
            mode=GenerationMode.SERIES,
            parameters=parameters,
            output_path=output_subdir,
            target_environment=target_environment,
        )

        if result["success"]:
            return result["result"]
        else:
            raise RuntimeError(f"Generation failed: {result['errors']}")


def create_legacy_compiler(
    fabric_workspace_repo_dir: str = None, fabric_environment: str = None
):
    """Factory function to create a legacy compiler adapter."""
    return LegacyCompilerAdapter(
        fabric_workspace_repo_dir=fabric_workspace_repo_dir,
        fabric_environment=fabric_environment,
    )


def create_legacy_incremental_compiler(
    fabric_workspace_repo_dir: str = None, fabric_environment: str = None
):
    """Factory function to create a legacy incremental compiler adapter."""
    return LegacyIncrementalCompilerAdapter(
        fabric_workspace_repo_dir=fabric_workspace_repo_dir,
        fabric_environment=fabric_environment,
    )
