"""
Unified Synthetic Data Generation Commands

This module provides the new unified command interface for synthetic data generation,
consolidating the functionality of legacy, enhanced, and generic template systems.
"""

from __future__ import annotations

import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Union, Literal
from enum import Enum

import typer
from rich.console import Console
from rich.table import Table

from .synthetic_data_generation import SyntheticDataGenerationCompiler
from .incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
from ...python_libs.common.synthetic_data_dataset_configs import DatasetConfigurationRepository


console = Console()


class GenerationMode(str, Enum):
    """Generation modes for synthetic data."""
    SINGLE = "single"
    INCREMENTAL = "incremental"
    SERIES = "series"


class OutputFormat(str, Enum):
    """Output formats for synthetic data."""
    TABLE = "table"
    PARQUET = "parquet"
    CSV = "csv"


class ListType(str, Enum):
    """Types of items to list."""
    DATASETS = "datasets"
    TEMPLATES = "templates"
    ALL = "all"


class UnifiedSyntheticDataGenerator:
    """Unified synthetic data generator that consolidates all generation modes."""
    
    def __init__(self, fabric_workspace_repo_dir: str = None, fabric_environment: str = None):
        """Initialize the unified generator."""
        self.base_compiler = SyntheticDataGenerationCompiler(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            fabric_environment=fabric_environment
        )
        self.incremental_compiler = IncrementalSyntheticDataGenerationCompiler(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            fabric_environment=fabric_environment
        )
        self.fabric_workspace_repo_dir = fabric_workspace_repo_dir
        self.fabric_environment = fabric_environment
    
    def generate(
        self,
        config: str,
        mode: GenerationMode = GenerationMode.SINGLE,
        parameters: Optional[Dict[str, Any]] = None,
        output_path: Optional[str] = None,
        dry_run: bool = False,
        target_environment: str = "lakehouse"
    ) -> Dict[str, Any]:
        """
        Unified generation method that handles all modes.
        
        Args:
            config: Dataset ID or template path
            mode: Generation mode (single, incremental, series)
            parameters: Runtime parameters as dictionary
            output_path: Optional output path override
            dry_run: If True, only validate and show what would be generated
            target_environment: Target environment (lakehouse or warehouse)
            
        Returns:
            Dictionary with generation results
        """
        # Merge parameters with defaults
        params = self._merge_parameters(config, mode, parameters)
        
        # Validate parameters
        validation_result = self._validate_parameters(mode, params)
        if not validation_result["valid"]:
            return {
                "success": False,
                "errors": validation_result["errors"],
                "mode": mode,
                "config": config
            }
        
        if dry_run:
            return {
                "success": True,
                "mode": mode,
                "config": config,
                "parameters": params,
                "message": "Dry run successful - no files generated"
            }
        
        # Route to appropriate generation method
        try:
            if mode == GenerationMode.SINGLE:
                result = self._generate_single(config, params, target_environment, output_path)
            elif mode == GenerationMode.INCREMENTAL:
                result = self._generate_incremental(config, params, target_environment, output_path)
            elif mode == GenerationMode.SERIES:
                result = self._generate_series(config, params, target_environment, output_path)
            else:
                raise ValueError(f"Unknown generation mode: {mode}")
            
            return {
                "success": True,
                "mode": mode,
                "config": config,
                "parameters": params,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "errors": [str(e)],
                "mode": mode,
                "config": config
            }
    
    def list_items(self, list_type: ListType = ListType.ALL) -> Dict[str, Any]:
        """
        List available datasets and templates.
        
        Args:
            list_type: Type of items to list
            
        Returns:
            Dictionary with available items
        """
        result = {}
        
        if list_type in [ListType.DATASETS, ListType.ALL]:
            # Get predefined datasets
            datasets = DatasetConfigurationRepository.list_available_datasets()
            result["datasets"] = datasets
            
            # Get incremental datasets
            incremental_configs = self.incremental_compiler._get_incremental_dataset_configs()
            result["incremental_datasets"] = {
                k: v.get("description", "No description") 
                for k, v in incremental_configs.items()
            }
        
        if list_type in [ListType.TEMPLATES, ListType.ALL]:
            # Get configuration templates
            if hasattr(self.base_compiler, 'get_available_configuration_templates'):
                templates = self.base_compiler.get_available_configuration_templates()
                result["templates"] = templates
            
            # Get generic templates
            result["generic_templates"] = {
                "generic_single_dataset_lakehouse": "Generic runtime-parameterized template for single dataset generation in lakehouse",
                "generic_single_dataset_warehouse": "Generic runtime-parameterized template for single dataset generation in warehouse",
                "generic_incremental_series_lakehouse": "Generic runtime-parameterized template for incremental series generation in lakehouse",
                "generic_incremental_series_warehouse": "Generic runtime-parameterized template for incremental series generation in warehouse"
            }
        
        return result
    
    def compile(
        self,
        template: str,
        runtime_config: Optional[Dict[str, Any]] = None,
        output_format: str = "all",
        target_environment: str = "lakehouse"
    ) -> Dict[str, Any]:
        """
        Compile synthetic data generation artifacts.
        
        Args:
            template: Template ID or path
            runtime_config: Runtime configuration
            output_format: Output format (notebook, ddl, all)
            target_environment: Target environment
            
        Returns:
            Dictionary with compilation results
        """
        results = {
            "success": True,
            "compiled_items": {},
            "errors": []
        }
        
        try:
            # Compile notebooks if requested
            if output_format in ["notebook", "all"]:
                if template.startswith("generic_"):
                    # Compile generic template
                    notebook_path = self._compile_generic_template(
                        template, runtime_config, target_environment
                    )
                    results["compiled_items"]["notebook"] = str(notebook_path)
                else:
                    # Use standard compilation
                    config = runtime_config or {}
                    config["dataset_id"] = template
                    notebook_path = self.base_compiler.compile_synthetic_data_generation_notebook(
                        dataset_config=config,
                        target_environment=target_environment
                    )
                    results["compiled_items"]["notebook"] = str(notebook_path)
            
            # Compile DDL if requested
            if output_format in ["ddl", "all"]:
                ddl_results = self.base_compiler.compile_ddl_scripts(target_environment)
                results["compiled_items"]["ddl"] = ddl_results
            
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
        
        return results
    
    def _merge_parameters(
        self, 
        config: str, 
        mode: GenerationMode, 
        parameters: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Merge user parameters with defaults based on config and mode."""
        # Start with defaults based on mode
        defaults = self._get_mode_defaults(mode)
        
        # Try to load config defaults
        if not config.startswith("generic_") and config in DatasetConfigurationRepository._get_all_predefined_datasets():
            config_data = DatasetConfigurationRepository.get_predefined_dataset(config)
            defaults.update(config_data)
        
        # Apply user parameters
        if parameters:
            defaults.update(parameters)
        
        return defaults
    
    def _get_mode_defaults(self, mode: GenerationMode) -> Dict[str, Any]:
        """Get default parameters based on generation mode."""
        if mode == GenerationMode.SINGLE:
            return {
                "target_rows": 10000,
                "output_mode": "table",
                "generation_mode": "auto",
                "chunk_size": 1000000
            }
        elif mode == GenerationMode.INCREMENTAL:
            return {
                "generation_date": date.today().isoformat(),
                "path_format": "nested",
                "state_management": True,
                "output_mode": "parquet"
            }
        elif mode == GenerationMode.SERIES:
            return {
                "start_date": date.today().isoformat(),
                "end_date": (date.today() + timedelta(days=30)).isoformat(),
                "batch_size": 10,
                "path_format": "nested",
                "output_mode": "parquet",
                "ignore_state": False
            }
        return {}
    
    def _validate_parameters(self, mode: GenerationMode, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters based on generation mode."""
        errors = []
        
        if mode == GenerationMode.SINGLE:
            if "target_rows" in params and params["target_rows"] <= 0:
                errors.append("target_rows must be positive")
        
        elif mode == GenerationMode.INCREMENTAL:
            if "generation_date" in params:
                try:
                    datetime.strptime(params["generation_date"], "%Y-%m-%d")
                except ValueError:
                    errors.append("generation_date must be in YYYY-MM-DD format")
        
        elif mode == GenerationMode.SERIES:
            if "start_date" in params and "end_date" in params:
                try:
                    start = datetime.strptime(params["start_date"], "%Y-%m-%d").date()
                    end = datetime.strptime(params["end_date"], "%Y-%m-%d").date()
                    if start > end:
                        errors.append("start_date must be before end_date")
                except ValueError:
                    errors.append("Dates must be in YYYY-MM-DD format")
        
        return {"valid": len(errors) == 0, "errors": errors}
    
    def _generate_single(
        self, 
        config: str, 
        params: Dict[str, Any], 
        target_environment: str,
        output_path: Optional[str]
    ) -> Path:
        """Generate single dataset."""
        return self.base_compiler.compile_predefined_dataset_notebook(
            dataset_id=config,
            target_rows=params.get("target_rows", 10000),
            target_environment=target_environment,
            generation_mode=params.get("generation_mode", "auto"),
            seed_value=params.get("seed_value"),
            output_subdir=output_path,
            output_mode=params.get("output_mode", "table")
        )
    
    def _generate_incremental(
        self, 
        config: str, 
        params: Dict[str, Any], 
        target_environment: str,
        output_path: Optional[str]
    ) -> Path:
        """Generate incremental dataset."""
        # Get dataset configuration
        dataset_config = DatasetConfigurationRepository.get_predefined_dataset(config)
        dataset_config.update(params)
        
        generation_date = params.get("generation_date", date.today().isoformat())
        if isinstance(generation_date, str):
            generation_date = datetime.strptime(generation_date, "%Y-%m-%d").date()
        
        return self.incremental_compiler.compile_incremental_dataset_notebook(
            dataset_config=dataset_config,
            generation_date=generation_date,
            target_environment=target_environment,
            generation_mode=params.get("generation_mode", "auto"),
            output_subdir=output_path,
            path_format=params.get("path_format", "nested"),
            state_management=params.get("state_management", True)
        )
    
    def _generate_series(
        self, 
        config: str, 
        params: Dict[str, Any], 
        target_environment: str,
        output_path: Optional[str]
    ) -> Path:
        """Generate series of incremental datasets."""
        # Get dataset configuration
        dataset_config = DatasetConfigurationRepository.get_predefined_dataset(config)
        dataset_config.update(params)
        
        return self.incremental_compiler.compile_incremental_dataset_series_notebook(
            dataset_config=dataset_config,
            start_date=params.get("start_date"),
            end_date=params.get("end_date"),
            target_environment=target_environment,
            generation_mode=params.get("generation_mode", "auto"),
            output_subdir=output_path,
            path_format=params.get("path_format", "nested"),
            batch_size=params.get("batch_size", 10),
            output_mode=params.get("output_mode", "parquet"),
            ignore_state=params.get("ignore_state", False)
        )
    
    def _compile_generic_template(
        self, 
        template_name: str, 
        runtime_config: Optional[Dict[str, Any]],
        target_environment: str
    ) -> Path:
        """Compile a generic template with runtime configuration."""
        # Map template names to their configurations
        template_configs = {
            "generic_single_dataset_lakehouse": {
                "template_name": "generic_single_dataset_lakehouse.py.jinja",
                "output_name": "generic_single_dataset_lakehouse",
                "language_group": "synapse_pyspark"
            },
            "generic_incremental_series_lakehouse": {
                "template_name": "generic_incremental_series_lakehouse.py.jinja",
                "output_name": "generic_incremental_series_lakehouse",
                "language_group": "synapse_pyspark"
            },
            "generic_incremental_series_warehouse": {
                "template_name": "generic_incremental_series_warehouse.py.jinja",
                "output_name": "generic_incremental_series_warehouse",
                "language_group": "python"
            }
        }
        
        if template_name not in template_configs:
            raise ValueError(f"Unknown generic template: {template_name}")
        
        config = template_configs[template_name]
        template_vars = {
            "target_environment": target_environment,
            "language_group": config["language_group"],
            **(runtime_config or {})
        }
        
        return self.base_compiler.compile_notebook_from_template(
            template_name=config["template_name"],
            output_notebook_name=config["output_name"],
            template_vars=template_vars,
            display_name=f"Generic Template - {template_name}",
            description=f"Runtime-parameterized synthetic data generation",
            output_subdir="synthetic_data_generation/generic"
        )


def format_list_output(items: Dict[str, Any], format_type: str = "table") -> None:
    """Format and display list output."""
    if format_type == "json":
        console.print_json(json.dumps(items, indent=2))
        return
    
    # Table format
    for category, data in items.items():
        console.print(f"\n[bold blue]{category.replace('_', ' ').title()}:[/bold blue]")
        
        if isinstance(data, dict):
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("ID", style="cyan")
            table.add_column("Description")
            
            for key, value in data.items():
                table.add_row(key, value)
            
            console.print(table)
        else:
            console.print(data)