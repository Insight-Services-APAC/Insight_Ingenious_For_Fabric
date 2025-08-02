"""
Synthetic Data Generation Package Compiler

This module provides the main compiler for the synthetic data generation package,
extending BaseNotebookCompiler to generate notebooks for creating synthetic datasets.

MIGRATION NOTICE:
This module has been redesigned with a unified architecture. The old API is maintained
for backward compatibility but will be removed in a future version. Please migrate
to the new UnifiedSyntheticDataGenerator for new development.

Enhanced with support for:
- Runtime configurable parameters
- Flexible file path patterns
- Enhanced logging capabilities
- New template system
- Unified command interface
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Literal

from ...notebook_utils.base_notebook_compiler import BaseNotebookCompiler
from ...python_libs.common.synthetic_data_dataset_configs import DatasetConfigurationRepository

# Import enhanced configuration if available
try:
    from ...python_libs.common.synthetic_data_config import DatasetConfiguration, ConfigurationManager
    from ...python_libs.common.file_path_utils import DateBasedFilePathGenerator
    ENHANCED_CONFIG_AVAILABLE = True
except ImportError:
    ENHANCED_CONFIG_AVAILABLE = False


class SyntheticDataGenerationCompiler(BaseNotebookCompiler):
    """Compiler for synthetic data generation notebooks and configurations."""
    
    def __init__(self, fabric_workspace_repo_dir: str = None, fabric_environment: str = None, **kwargs):
        """Initialize the synthetic data generation compiler."""
        # Set package-specific defaults
        package_templates_dir = Path(__file__).parent / "templates"
        
        # Store fabric_environment for later use but don't pass to parent
        self.fabric_environment = fabric_environment
        
        super().__init__(
            templates_dir=package_templates_dir,
            package_name="synthetic_data_generation",
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            **kwargs
        )
        
        # Initialize enhanced configuration manager if available
        if ENHANCED_CONFIG_AVAILABLE:
            self.config_manager = ConfigurationManager()
            self.file_path_generator = DateBasedFilePathGenerator()
        else:
            self.config_manager = None
            self.file_path_generator = None
    
    def compile_synthetic_data_generation_notebook(
        self,
        dataset_config: Dict[str, Any],
        target_environment: str = "lakehouse",  # "lakehouse" or "warehouse"
        generation_mode: str = "auto",  # "python", "pyspark", or "auto"
        output_subdir: str = None,
        output_mode: str = "table"  # "table" or "parquet"
    ) -> Path:
        """
        Compile a synthetic data generation notebook based on dataset configuration.
        
        Args:
            dataset_config: Configuration dictionary for the dataset
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            output_subdir: Optional subdirectory for output
            output_mode: Output mode ("table" or "parquet")
            
        Returns:
            Path to the generated notebook
        """
        # Auto-select generation mode based on target rows
        if generation_mode == "auto":
            target_rows = dataset_config.get("target_rows", 10000)
            generation_mode = "pyspark" if target_rows > 1000000 else "python"
        
        # Override generation mode for warehouse (always use python)
        if target_environment == "warehouse":
            generation_mode = "python"
        
        # Use unified template for all generation modes
        template_name = "synthetic_data_base_notebook.py.jinja"
        if generation_mode == "pyspark":
            language_group = "synapse_pyspark"
        else:
            language_group = "python"
        
        target_datastore_config_prefix = "config"
        if target_environment == "warehouse":
            target_datastore_config_prefix = "config_wh"

        # Prepare template variables
        template_vars = {
            "target_lakehouse_config_prefix": target_datastore_config_prefix,
            "dataset_config": dataset_config,
            "generation_mode": generation_mode,
            "target_environment": target_environment,
            "language_group": language_group,
            "dataset_id": dataset_config.get("dataset_id", "custom_dataset"),
            "target_rows": dataset_config.get("target_rows", 10000),
            "chunk_size": dataset_config.get("chunk_size", 1000000),
            "seed_value": dataset_config.get("seed_value"),
            "parallel_workers": dataset_config.get("parallel_workers", 1),
            "output_mode": output_mode
        }
        
        # Generate notebook name
        dataset_id = dataset_config.get("dataset_id", "custom")
        scale = "large" if template_vars["target_rows"] > 1000000 else "small"
        notebook_name = f"synthetic_data_{dataset_id}_{scale}_{generation_mode}"
        
        # Compile notebook
        # Set default output_subdir if not provided
        if output_subdir is None:
            output_subdir = "synthetic_data_generation"
            
        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=f"Synthetic Data Generation - {dataset_config.get('dataset_name', dataset_id)}",
            description=f"Generate synthetic data for {dataset_id} using {generation_mode}",
            output_subdir=output_subdir
        )
    
    def compile_enhanced_synthetic_data_notebook(
        self,
        dataset_config: Union[DatasetConfiguration, Dict[str, Any]],
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        output_subdir: str = None,
        use_enhanced_template: bool = True,
        runtime_overrides: Dict[str, Any] = None
    ) -> Path:
        """
        Compile an enhanced synthetic data generation notebook with runtime configuration support.
        
        Args:
            dataset_config: Enhanced dataset configuration or legacy dict
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            output_subdir: Optional subdirectory for output
            use_enhanced_template: Whether to use the enhanced template
            runtime_overrides: Runtime parameter overrides
            
        Returns:
            Path to the generated notebook
        """
        if not ENHANCED_CONFIG_AVAILABLE:
            # Fall back to legacy method
            if isinstance(dataset_config, dict):
                return self.compile_synthetic_data_generation_notebook(
                    dataset_config=dataset_config,
                    target_environment=target_environment,
                    generation_mode=generation_mode,
                    output_subdir=output_subdir
                )
            else:
                return self.compile_synthetic_data_generation_notebook(
                    dataset_config=dataset_config.to_dict(),
                    target_environment=target_environment,
                    generation_mode=generation_mode,
                    output_subdir=output_subdir
                )
        
        # Handle configuration
        if isinstance(dataset_config, dict):
            config = DatasetConfiguration.from_dict(dataset_config)
        else:
            config = dataset_config
        
        # Apply runtime overrides if provided
        if runtime_overrides:
            config.apply_runtime_overrides(runtime_overrides)
        
        # Auto-select generation mode based on configuration
        if generation_mode == "auto":
            from datetime import date
            total_rows = config.calculate_total_rows_for_date(date.today())
            generation_mode = "pyspark" if total_rows > 1000000 else "python"
        
        # Override for warehouse environment
        if target_environment == "warehouse":
            generation_mode = "python"
        
        # Use unified template for all modes
        template_name = "synthetic_data_base_notebook.py.jinja"
        language_group = "synapse_pyspark" if generation_mode == "pyspark" else "python"
        
        # Prepare template variables
        from datetime import date
        template_vars = {
            "target_lakehouse_config_prefix": "config_wh" if target_environment == "warehouse" else "config",
            "dataset_config": config.to_dict(),
            "generation_mode": generation_mode,
            "target_environment": target_environment,
            "language_group": language_group,
            "dataset_id": config.dataset_id,
            "seed_value": config.incremental_config.get("seed_value"),
            "runtime_overrides": runtime_overrides or {},
            "enhanced_features_enabled": True,
            "generation_date": date.today().isoformat()
        }
        
        # Generate notebook name
        from datetime import date
        scale = "large" if config.calculate_total_rows_for_date(date.today()) > 1000000 else "small"
        notebook_name = f"enhanced_synthetic_data_{config.dataset_id}_{scale}_{generation_mode}"
        
        # Set default output_subdir
        if output_subdir is None:
            output_subdir = f"synthetic_data_generation/enhanced/{config.dataset_id}"
        
        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=f"Enhanced Synthetic Data - {config.dataset_name}",
            description=f"Enhanced synthetic data generation for {config.dataset_id} with runtime configuration",
            output_subdir=output_subdir
        )
    
    def compile_configurable_dataset_notebook(
        self,
        config_template_id: str,
        dataset_id: str,
        customizations: Dict[str, Any] = None,
        target_environment: str = "lakehouse",
        output_subdir: str = None
    ) -> Path:
        """
        Compile a notebook using a predefined configuration template.
        
        Args:
            config_template_id: ID of the predefined configuration template
            dataset_id: New dataset ID for the generated notebook
            customizations: Custom overrides for the template
            target_environment: Target environment
            output_subdir: Output subdirectory
            
        Returns:
            Path to the generated notebook
        """
        if not ENHANCED_CONFIG_AVAILABLE:
            raise RuntimeError("Enhanced configuration system not available")
        
        # Create configuration from template
        config = self.config_manager.create_config_from_template(
            template_id=config_template_id,
            dataset_id=dataset_id,
            overrides=customizations
        )
        
        return self.compile_enhanced_synthetic_data_notebook(
            dataset_config=config,
            target_environment=target_environment,
            output_subdir=output_subdir or f"synthetic_data_generation/configured/{dataset_id}"
        )
    
    def get_available_configuration_templates(self) -> Dict[str, str]:
        """Get available configuration templates."""
        if not ENHANCED_CONFIG_AVAILABLE or not self.config_manager:
            # Fall back to repository configurations
            return DatasetConfigurationRepository.list_available_datasets()
        
        templates = {}
        for config_id in self.config_manager.get_available_configs():
            config = self.config_manager.get_predefined_config(config_id)
            templates[config_id] = config.description if config else "No description"
        
        return templates
    
    def get_available_file_path_patterns(self) -> Dict[str, str]:
        """Get available file path patterns."""
        if not self.file_path_generator:
            return {}
        
        return self.file_path_generator.get_available_patterns()
    
    def compile_predefined_dataset_notebook(
        self,
        dataset_id: str,
        target_rows: int,
        target_environment: str = "lakehouse",
        generation_mode: str = "auto",
        seed_value: Optional[int] = None,
        output_subdir: str = None,
        output_mode: str = "table"
    ) -> Path:
        """
        Compile a notebook for a predefined dataset configuration.
        
        Args:
            dataset_id: ID of the predefined dataset
            target_rows: Number of rows to generate
            target_environment: Target environment ("lakehouse" or "warehouse")
            generation_mode: Generation mode ("python", "pyspark", or "auto")
            seed_value: Optional seed for reproducible generation
            output_subdir: Optional subdirectory for output
            output_mode: Output mode ("table" or "parquet")
            
        Returns:
            Path to the generated notebook
        """
        # Load predefined dataset configuration from repository
        base_config = DatasetConfigurationRepository.get_predefined_dataset(dataset_id)
        dataset_config = {
            **base_config,
            "target_rows": target_rows,
            "seed_value": seed_value,
            "chunk_size": min(target_rows, 1000000) if target_rows > 1000000 else target_rows
        }
        
        print("💡 [NOTICE] Consider using the new generic template system for better flexibility!")
        print("   Use 'compile-generic-templates' and 'execute-with-parameters' commands.")
        print(f"   Example: python -m ingen_fab.cli package synthetic-data compile-generic-templates --target-environment {target_environment}")
        print(f"   Then: python -m ingen_fab.cli package synthetic-data execute-with-parameters generic_single_dataset_{target_environment} --dataset-id {dataset_id} --target-rows {target_rows}")
        print()
        
        return self.compile_synthetic_data_generation_notebook(
            dataset_config=dataset_config,
            target_environment=target_environment,
            generation_mode=generation_mode,
            output_subdir=output_subdir,
            output_mode=output_mode
        )
    
    def compile_ddl_scripts(self, target_environment: str = "warehouse") -> Dict[str, List[Path]]:
        """
        Compile DDL scripts for synthetic data generation configuration tables.
        
        Args:
            target_environment: Target environment ("warehouse" or "lakehouse")
            
        Returns:
            Dictionary mapping target directories to lists of copied files
        """
        ddl_source_dir = Path(__file__).parent / "ddl_scripts" / target_environment
        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"
        
        if target_environment == "warehouse":
            # Copy SQL files to Warehouses/Config directory following standard pattern
            script_mappings = {
                "Warehouses/Config/001_Initial_Creation_SyntheticData": [
                    ("001_config_synthetic_data_datasets.sql", "001_config_synthetic_data_datasets.sql"),
                    ("002_config_synthetic_data_generation_jobs.sql", "002_config_synthetic_data_generation_jobs.sql"),
                    ("003_log_synthetic_data_generation.sql", "003_log_synthetic_data_generation.sql"),
                    ("004_sample_dataset_configurations.sql", "004_sample_dataset_configurations.sql")
                ]
            }
        else:
            # Lakehouse DDL scripts (Python files) to Lakehouses/Config directory
            script_mappings = {
                "Lakehouses/Config/001_Initial_Creation_SyntheticData": [
                    ("001_config_synthetic_data_datasets.py", "001_config_synthetic_data_datasets.py"),
                    ("002_config_synthetic_data_generation_jobs.py", "002_config_synthetic_data_generation_jobs.py"),
                    ("003_log_synthetic_data_generation.py", "003_log_synthetic_data_generation.py"),
                    ("004_sample_dataset_configurations.py", "004_sample_dataset_configurations.py")
                ]
            }
        
        return super().compile_ddl_scripts(ddl_source_dir, ddl_output_base, script_mappings)
    
    def compile_all_synthetic_data_notebooks(
        self,
        datasets: List[Dict[str, Any]] = None,
        target_environment: str = "lakehouse",
        output_mode: str = "table"
    ) -> Dict[str, Any]:
        """
        Compile notebooks for multiple synthetic datasets.
        
        Args:
            datasets: List of dataset configurations. If None, uses predefined datasets.
            target_environment: Target environment ("lakehouse" or "warehouse")
            output_mode: Output mode ("table" or "parquet")
            
        Returns:
            Dictionary with compilation results
        """
        if datasets is None:
            # Use sample predefined datasets
            predefined = self._get_predefined_dataset_configs()
            datasets = [
                {**config, "target_rows": 10000 if "small" in config["dataset_id"] else 1000000}
                for config in predefined.values()
                if "small" in config["dataset_id"]  # Only compile small samples by default
            ]
        
        compile_functions = []
        
        # Add DDL compilation
        compile_functions.append((
            self.compile_ddl_scripts,
            [target_environment],
            {}
        ))
        
        # Add notebook compilations
        for dataset_config in datasets:
            compile_functions.append((
                self.compile_synthetic_data_generation_notebook,
                [dataset_config, target_environment],
                {"output_subdir": f"synthetic_data_generation/{dataset_config['dataset_id']}", "output_mode": output_mode}
            ))
        
        return self.compile_all_with_results(
            compile_functions,
            header_title=f"🎲 Compiling Synthetic Data Generation Package for {target_environment.title()}"
        )
    
    def compile_all_enhanced_synthetic_data_notebooks(
        self,
        configuration_templates: List[str] = None,
        target_environment: str = "lakehouse",
        customizations: Dict[str, Dict[str, Any]] = None,
        output_mode: str = "parquet"
    ) -> Dict[str, Any]:
        """
        Compile enhanced synthetic data notebooks using configuration templates.
        
        Args:
            configuration_templates: List of configuration template IDs to use
            target_environment: Target environment ("lakehouse" or "warehouse")
            customizations: Per-template customizations
            output_mode: Output mode ("table" or "parquet")
            
        Returns:
            Dictionary with compilation results
        """
        if not ENHANCED_CONFIG_AVAILABLE:
            # Fall back to standard compilation
            return self.compile_all_synthetic_data_notebooks(
                target_environment=target_environment,
                output_mode=output_mode
            )
        
        # Use default templates if none provided
        if configuration_templates is None:
            configuration_templates = ["retail_oltp_enhanced", "retail_star_enhanced"]
        
        compile_functions = []
        
        # Add DDL compilation
        compile_functions.append((
            self.compile_ddl_scripts,
            [target_environment],
            {}
        ))
        
        # Add enhanced notebook compilations
        for template_id in configuration_templates:
            # Get base configuration
            config = self.config_manager.get_predefined_config(template_id)
            if not config:
                print(f"⚠️ Warning: Configuration template '{template_id}' not found, skipping...")
                continue
            
            # Apply customizations
            template_customizations = customizations.get(template_id, {}) if customizations else {}
            if template_customizations:
                config.apply_runtime_overrides(template_customizations)
            
            # Add output mode to config
            config.output_settings["output_mode"] = output_mode
            
            compile_functions.append((
                self.compile_enhanced_synthetic_data_notebook,
                [config, target_environment],
                {"output_subdir": f"synthetic_data_generation/enhanced/{config.dataset_id}"}
            ))
        
        return self.compile_all_with_results(
            compile_functions,
            header_title=f"🚀 Compiling Enhanced Synthetic Data Generation Package for {target_environment.title()}"
        )
    
    def _get_predefined_dataset_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get predefined dataset configurations from the centralized repository."""
        return DatasetConfigurationRepository._get_all_predefined_datasets()