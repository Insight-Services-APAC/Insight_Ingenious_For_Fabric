"""
Modular Data Profiling Compiler

This module provides a refactored, modular implementation of the data profiling compiler
using separate components for template compilation, DDL management, and configuration.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler
from ingen_fab.python_libs.common.path_configuration import PathConfiguration
from ingen_fab.packages.data_profiling.template_compiler import ProfilingTemplateCompiler
from ingen_fab.packages.data_profiling.ddl_manager import DDLScriptManager
from ingen_fab.packages.data_profiling.configuration_builder import (
    ConfigurationBuilder,
    ProfilingConfiguration,
    create_config,
    create_default_config,
)
from ingen_fab.python_libs.interfaces.profiler_registry import get_registry


class ModularDataProfilingCompiler(BaseNotebookCompiler):
    """
    Modular data profiling compiler using composition over inheritance.
    
    This class coordinates between different specialized components to provide
    a clean, maintainable API for data profiling compilation.
    """
    
    def __init__(self, fabric_workspace_repo_dir: str = None):
        """
        Initialize the modular compiler.
        
        Args:
            fabric_workspace_repo_dir: Path to the Fabric workspace repository
        """
        # Initialize path configuration
        self.path_config = PathConfiguration("data_profiling")
        
        # Initialize specialized components
        self.template_compiler = ProfilingTemplateCompiler(self.path_config)
        self.ddl_manager = DDLScriptManager(self.path_config)
        
        # Initialize base notebook compiler
        template_search_paths = self.path_config.get_template_search_paths()
        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="data_profiling",
        )
        
        # Store component info for debugging
        if self.console:
            self.console.print(f"[bold blue]Package Directory:[/bold blue] {self.path_config.get_package_base_path()}")
            self.console.print(f"[bold blue]Templates Directory:[/bold blue] {self.path_config.get_templates_dir()}")
            self.console.print(f"[bold blue]DDL Scripts Directory:[/bold blue] {self.path_config.get_ddl_scripts_dir()}")
    
    def compile_notebook(
        self,
        config: Optional[Union[ProfilingConfiguration, Dict[str, Any]]] = None,
        target_datastore: str = "lakehouse",
    ) -> Path:
        """
        Compile the data profiling notebook template.
        
        Args:
            config: Profiling configuration or template variables
            target_datastore: Target datastore type
            
        Returns:
            Path to the compiled notebook
        """
        # Convert config to template variables
        if isinstance(config, ProfilingConfiguration):
            template_vars = self._config_to_template_vars(config)
        elif isinstance(config, dict):
            # Apply defaults to provided variables
            default_config = create_default_config(target_datastore)
            template_vars = self._config_to_template_vars(default_config)
            template_vars.update(config)
        else:
            # Use defaults
            default_config = create_default_config(target_datastore)
            template_vars = self._config_to_template_vars(default_config)
        
        # Generate output paths
        output_name = f"data_profiling_processor_{target_datastore}"
        output_path = self._get_notebook_output_path(output_name)
        
        # Compile template
        compiled_path = self.template_compiler.compile_processor_notebook(
            target_datastore=target_datastore,
            template_vars=template_vars,
            output_path=output_path / "notebook-content.py",
        )
        
        # Create notebook metadata
        display_name = f"Data Profiling Processor ({target_datastore.title()})"
        description = f"Profiles datasets in {target_datastore} and generates quality reports"
        
        self._create_notebook_metadata(
            notebook_dir=output_path,
            display_name=display_name,
            description=description,
        )
        
        if self.console:
            self.console.print(f"[green]✓ Notebook created:[/green] {output_path}")
        
        return output_path
    
    def compile_config_notebook(
        self,
        config: Optional[Union[ProfilingConfiguration, Dict[str, Any]]] = None,
        target_datastore: str = "lakehouse",
    ) -> Path:
        """
        Compile the profiling configuration notebook template.
        
        Args:
            config: Profiling configuration or template variables
            target_datastore: Target datastore type
            
        Returns:
            Path to the compiled notebook
        """
        # Convert config to template variables
        if isinstance(config, ProfilingConfiguration):
            template_vars = self._config_to_template_vars(config)
        elif isinstance(config, dict):
            # Apply defaults to provided variables
            default_config = create_default_config(target_datastore)
            template_vars = self._config_to_template_vars(default_config)
            template_vars.update(config)
        else:
            # Use defaults
            default_config = create_default_config(target_datastore)
            template_vars = self._config_to_template_vars(default_config)
        
        # Generate output paths
        output_name = f"data_profiling_config_{target_datastore}"
        output_path = self._get_notebook_output_path(output_name)
        
        # Compile template
        compiled_path = self.template_compiler.compile_config_notebook(
            target_datastore=target_datastore,
            template_vars=template_vars,
            output_path=output_path / "notebook-content.py",
        )
        
        # Create notebook metadata
        display_name = f"Data Profiling Configuration ({target_datastore.title()})"
        description = f"Configures automated data profiling for {target_datastore} tables"
        
        self._create_notebook_metadata(
            notebook_dir=output_path,
            display_name=display_name,
            description=description,
        )
        
        if self.console:
            self.console.print(f"[green]✓ Config notebook created:[/green] {output_path}")
        
        return output_path
    
    def compile_ddl_scripts(
        self,
        include_sample_data: bool = False,
        target_datastore: str = "both",
        template_vars: Optional[Dict[str, Any]] = None,
    ) -> List[Path]:
        """
        Compile DDL scripts to the target directory.
        
        Args:
            include_sample_data: Whether to include sample data scripts
            target_datastore: Target datastore type
            template_vars: Variables for template rendering
            
        Returns:
            List of paths to compiled DDL scripts
        """
        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"
        
        return self.ddl_manager.compile_ddl_scripts(
            output_base_dir=ddl_output_base,
            target_datastore=target_datastore,
            include_samples=include_sample_data,
            template_vars=template_vars or {},
        )
    
    def compile_all(
        self,
        config: Optional[Union[ProfilingConfiguration, Dict[str, Any]]] = None,
        include_samples: bool = False,
        target_datastore: str = "lakehouse",
        include_config: bool = True,
    ) -> Dict[str, Any]:
        """
        Compile all templates and DDL scripts.
        
        Args:
            config: Profiling configuration or template variables
            include_samples: Whether to include sample data scripts
            target_datastore: Target datastore type
            include_config: Whether to include configuration notebook
            
        Returns:
            Dictionary with compilation results
        """
        if target_datastore == "both":
            # Handle both datastores by compiling each separately
            lakehouse_results = self.compile_all(
                config, include_samples, "lakehouse", include_config
            )
            warehouse_results = self.compile_all(
                config, include_samples, "warehouse", include_config
            )
            
            # Combine results
            return self._combine_compilation_results(lakehouse_results, warehouse_results)
        
        # Single datastore compilation
        results = {"success": True, "errors": []}
        
        try:
            # Compile processor notebook
            processor_notebook = self.compile_notebook(config, target_datastore)
            results["notebook_file"] = processor_notebook
            
            # Compile configuration notebook if requested
            if include_config:
                config_notebook = self.compile_config_notebook(config, target_datastore)
                results["config_file"] = config_notebook
            else:
                results["config_file"] = None
            
            # Compile DDL scripts
            template_vars = None
            if isinstance(config, ProfilingConfiguration):
                template_vars = self._config_to_template_vars(config)
            elif isinstance(config, dict):
                template_vars = config
            
            ddl_files = self.compile_ddl_scripts(
                include_sample_data=include_samples,
                target_datastore=target_datastore,
                template_vars=template_vars,
            )
            results["ddl_files"] = ddl_files
            
            # Success message
            self._print_compilation_success(results, target_datastore)
            
        except Exception as e:
            results["success"] = False
            results["errors"] = [str(e)]
            results["notebook_file"] = None
            results["config_file"] = None
            results["ddl_files"] = []
        
        return results
    
    def create_configuration(self) -> ConfigurationBuilder:
        """
        Create a new configuration builder.
        
        Returns:
            ConfigurationBuilder instance for fluent configuration
        """
        return create_config()
    
    def get_available_profilers(self, target_datastore: str) -> List[str]:
        """
        Get list of available profilers for a datastore.
        
        Args:
            target_datastore: Target datastore type
            
        Returns:
            List of available profiler names
        """
        registry = get_registry()
        return registry.find_profilers_for_datastore(target_datastore)
    
    def get_recommended_profiler(
        self,
        target_datastore: str,
        row_count: Optional[int] = None,
        scenario: str = "production",
    ) -> str:
        """
        Get recommended profiler for a scenario.
        
        Args:
            target_datastore: Target datastore type
            row_count: Estimated number of rows
            scenario: Usage scenario (development, testing, production)
            
        Returns:
            Name of recommended profiler
        """
        registry = get_registry()
        
        # Determine preferences based on scenario
        prefer_performance = scenario in ["production", "large_data"]
        
        try:
            return registry.get_best_profiler_for_scenario(
                datastore=target_datastore,
                row_count=row_count,
                prefer_performance=prefer_performance,
            )
        except ValueError:
            # Fallback to any available profiler
            available = self.get_available_profilers(target_datastore)
            return available[0] if available else "standard"
    
    def validate_configuration(
        self, config: Union[ProfilingConfiguration, Dict[str, Any]]
    ) -> tuple[bool, List[str], List[str]]:
        """
        Validate a profiling configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        if isinstance(config, dict):
            # Convert dict to configuration for validation
            builder = create_config()
            builder.custom_variables(**config)
            return builder.validate(), builder.get_errors(), builder.get_warnings()
        else:
            # ProfilingConfiguration - create temporary builder for validation
            builder = create_config()
            builder._config = config
            return builder.validate(), builder.get_errors(), builder.get_warnings()
    
    def _config_to_template_vars(self, config: ProfilingConfiguration) -> Dict[str, Any]:
        """Convert ProfilingConfiguration to template variables."""
        return {
            "profile_type": config.profile_type,
            "save_to_catalog": config.save_to_catalog,
            "generate_report": config.generate_report,
            "generate_yaml_output": config.generate_report,  # Compatibility
            "output_format": config.output_format,
            "sample_size": config.sample_size,
            "auto_sample_large_tables": config.auto_sample_large_tables,
            "max_correlation_columns": config.max_correlation_columns,
            "top_values_limit": config.top_values_limit,
            "enable_performance_mode": config.enable_performance_mode,
            "enable_ultra_fast_mode": config.enable_ultra_fast_mode,
            "auto_discover_tables": config.auto_discover_tables,
            "profile_frequency": config.profile_frequency,
            "quality_thresholds": config.quality_thresholds,
            **config.custom_template_vars,
        }
    
    def _get_notebook_output_path(self, notebook_name: str) -> Path:
        """Get the output path for a notebook."""
        return (
            self.fabric_workspace_repo_dir
            / "fabric_workspace_items"
            / "data_profiling"
            / f"{notebook_name}.Notebook"
        )
    
    def _create_notebook_metadata(
        self, notebook_dir: Path, display_name: str, description: str
    ) -> None:
        """Create notebook metadata files."""
        # Ensure directory exists
        notebook_dir.mkdir(parents=True, exist_ok=True)
        
        # Create .platform file (notebook metadata)
        platform_content = f'''{{"$schema":"https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json","metadata":{{"type":"Notebook","displayName":"{display_name}","description":"{description}"}},"config":{{"version":"2.0","logicalId":"{notebook_dir.name.replace('.Notebook', '')}"}}}}'''
        (notebook_dir / ".platform").write_text(platform_content)
    
    def _combine_compilation_results(
        self, lakehouse_results: Dict[str, Any], warehouse_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Combine results from lakehouse and warehouse compilation."""
        combined_success = lakehouse_results["success"] and warehouse_results["success"]
        combined_errors = lakehouse_results["errors"] + warehouse_results["errors"]
        combined_ddl_files = lakehouse_results["ddl_files"] + warehouse_results["ddl_files"]
        
        return {
            "notebook_file": [lakehouse_results["notebook_file"], warehouse_results["notebook_file"]],
            "config_file": [lakehouse_results.get("config_file"), warehouse_results.get("config_file")],
            "ddl_files": combined_ddl_files,
            "success": combined_success,
            "errors": combined_errors,
        }
    
    def _print_compilation_success(
        self, results: Dict[str, Any], target_datastore: str
    ) -> None:
        """Print success message for compilation."""
        if not self.console:
            return
        
        success_message = (
            f"✓ Successfully compiled data profiling package for {target_datastore}\n"
            f"Notebook: {results['notebook_file']}\n"
        )
        
        if results.get("config_file"):
            success_message += f"Config: {results['config_file']}\n"
        
        success_message += f"DDL Scripts: {len(results['ddl_files'])} files"
        
        self.print_success_panel("Compilation Complete", success_message)


# Backward compatibility function
def compile_data_profiling_package(
    fabric_workspace_repo_dir: str = None,
    template_vars: Dict[str, Any] = None,
    include_samples: bool = False,
    target_datastore: str = "lakehouse",
    add_debug_cells: bool = False,
) -> Dict[str, Any]:
    """
    Main function to compile the data profiling package (backward compatibility).
    
    Args:
        fabric_workspace_repo_dir: Path to workspace repository
        template_vars: Template variables (legacy)
        include_samples: Whether to include sample data
        target_datastore: Target datastore type
        add_debug_cells: Whether to add debug cells (legacy)
        
    Returns:
        Compilation results dictionary
    """
    compiler = ModularDataProfilingCompiler(fabric_workspace_repo_dir)
    
    # Convert legacy template_vars to configuration
    config_vars = template_vars or {}
    if add_debug_cells:
        config_vars["add_debug_cells"] = add_debug_cells
    
    return compiler.compile_all(
        config=config_vars,
        include_samples=include_samples,
        target_datastore=target_datastore,
    )