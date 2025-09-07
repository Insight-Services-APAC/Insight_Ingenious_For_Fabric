"""
DDL Script Management Module for Data Profiling

This module handles the generation and management of DDL scripts for
data profiling tables and configurations.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import jinja2

from ingen_fab.python_libs.common.path_configuration import PathConfiguration


class DDLScriptManager:
    """
    Manages DDL script generation and deployment for data profiling.
    
    This class handles the creation of database schema objects needed
    for data profiling, including configuration tables, result tables,
    and history tracking.
    """
    
    def __init__(
        self,
        path_config: Optional[PathConfiguration] = None,
        console: Optional[object] = None,
    ):
        """
        Initialize the DDL script manager.
        
        Args:
            path_config: Path configuration instance
            console: Rich console for output (optional)
        """
        self.path_config = path_config or PathConfiguration("data_profiling")
        self.console = console
        self._script_registry = self._build_script_registry()
    
    def _build_script_registry(self) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
        """
        Build the registry of DDL scripts for different scenarios.
        
        Returns:
            Nested dictionary of script mappings
        """
        return {
            "lakehouse": {
                "core": [
                    ("lakehouse/config_profiling_create.py.jinja", "001_config_data_profiling_create.py"),
                    ("lakehouse/profile_results_create.py.jinja", "002_profile_results_create.py"),
                    ("lakehouse/profile_history_create.py.jinja", "003_profile_history_create.py"),
                    ("lakehouse/data_quality_rules_create.py.jinja", "004_data_quality_rules_create.py"),
                ],
                "samples": [
                    ("lakehouse/sample_profiling_config.py.jinja", "005_sample_profiling_config.py"),
                    ("lakehouse/sample_quality_rules.py.jinja", "006_sample_quality_rules.py"),
                ],
            },
            "warehouse": {
                "core": [
                    ("warehouse/config_profiling_create.sql.jinja", "001_config_data_profiling_create.sql"),
                    ("warehouse/profile_results_create.sql.jinja", "002_profile_results_create.sql"),
                    ("warehouse/profile_history_create.sql.jinja", "003_profile_history_create.sql"),
                    ("warehouse/data_quality_rules_create.sql.jinja", "004_data_quality_rules_create.sql"),
                ],
                "samples": [
                    ("warehouse/sample_profiling_config.sql.jinja", "005_sample_profiling_config.sql"),
                    ("warehouse/sample_quality_rules.sql.jinja", "006_sample_quality_rules.sql"),
                ],
            },
        }
    
    def get_script_mappings(
        self,
        target_datastore: str,
        include_samples: bool = False,
    ) -> Dict[str, List[Tuple[str, str]]]:
        """
        Get script mappings for a specific datastore and configuration.
        
        Args:
            target_datastore: Target datastore (lakehouse, warehouse, both)
            include_samples: Whether to include sample data scripts
            
        Returns:
            Dictionary mapping folder paths to script tuples
        """
        mappings = {}
        
        # Determine which datastores to include
        if target_datastore == "both":
            datastores = ["lakehouse", "warehouse"]
        elif target_datastore in self._script_registry:
            datastores = [target_datastore]
        else:
            raise ValueError(
                f"Unsupported target datastore: {target_datastore}. "
                f"Must be 'lakehouse', 'warehouse', or 'both'"
            )
        
        # Build mappings for each datastore
        for datastore in datastores:
            folder_prefix = "Lakehouses" if datastore == "lakehouse" else "Warehouses"
            
            # Add core scripts
            core_scripts = self._script_registry[datastore]["core"]
            core_folder = f"{folder_prefix}/Config/001_Initial_Creation_Profiling"
            mappings[core_folder] = core_scripts
            
            # Add sample scripts if requested
            if include_samples:
                sample_scripts = self._script_registry[datastore]["samples"]
                sample_folder = f"{folder_prefix}/Config/002_Sample_Data_Profiling"
                mappings[sample_folder] = sample_scripts
        
        return mappings
    
    def compile_ddl_scripts(
        self,
        output_base_dir: Path,
        target_datastore: str = "both",
        include_samples: bool = False,
        template_vars: Optional[Dict] = None,
    ) -> List[Path]:
        """
        Compile DDL scripts to the target directory.
        
        Args:
            output_base_dir: Base directory for DDL output
            target_datastore: Target datastore type
            include_samples: Whether to include sample data scripts
            template_vars: Variables for template rendering
            
        Returns:
            List of paths to compiled DDL scripts
        """
        script_mappings = self.get_script_mappings(target_datastore, include_samples)
        compiled_files = []
        
        for folder_path, file_mappings in script_mappings.items():
            target_dir = output_base_dir / folder_path
            target_dir.mkdir(parents=True, exist_ok=True)
            
            for source_file, target_file in file_mappings:
                target_path = target_dir / target_file
                
                if source_file.endswith(".jinja"):
                    # Process as template
                    compiled_path = self._compile_ddl_template(
                        source_file, target_path, template_vars or {}
                    )
                else:
                    # Copy directly
                    compiled_path = self._copy_ddl_script(source_file, target_path)
                
                if compiled_path:
                    compiled_files.append(compiled_path)
        
        return compiled_files
    
    def _compile_ddl_template(
        self,
        source_file: str,
        target_path: Path,
        template_vars: Dict,
    ) -> Optional[Path]:
        """
        Compile a single DDL template file.
        
        Args:
            source_file: Relative path to source template
            target_path: Target path for compiled script
            template_vars: Variables for template rendering
            
        Returns:
            Path to compiled file, or None if failed
        """
        try:
            ddl_scripts_dir = self.path_config.get_ddl_scripts_dir()
            source_path = ddl_scripts_dir / source_file
            
            if not source_path.exists():
                self._log_error(f"Template file not found: {source_path}")
                return None
            
            # Read and render template
            template_content = source_path.read_text()
            
            # Create Jinja environment
            template_paths = [ddl_scripts_dir]
            unified_dir = self.path_config.get_unified_templates_dir()
            if unified_dir:
                template_paths.append(unified_dir)
            
            env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(template_paths),
                autoescape=False,
            )
            
            template = env.from_string(template_content)
            rendered_content = template.render(**template_vars)
            
            # Write rendered content
            target_path.write_text(rendered_content)
            self._log_success(f"Template rendered: {target_path}")
            
            return target_path
            
        except Exception as e:
            self._log_error(f"Template error: {source_file} - {e}")
            return None
    
    def _copy_ddl_script(self, source_file: str, target_path: Path) -> Optional[Path]:
        """
        Copy a DDL script file directly.
        
        Args:
            source_file: Relative path to source script
            target_path: Target path for script
            
        Returns:
            Path to copied file, or None if failed
        """
        try:
            ddl_scripts_dir = self.path_config.get_ddl_scripts_dir()
            source_path = ddl_scripts_dir / source_file
            
            if not source_path.exists():
                self._log_error(f"Source file not found: {source_path}")
                return None
            
            target_path.write_bytes(source_path.read_bytes())
            self._log_success(f"File copied: {target_path}")
            
            return target_path
            
        except Exception as e:
            self._log_error(f"Copy error: {source_file} - {e}")
            return None
    
    def validate_scripts(self, target_datastore: str) -> Dict[str, bool]:
        """
        Validate that all required DDL scripts exist.
        
        Args:
            target_datastore: Target datastore to validate
            
        Returns:
            Dictionary mapping script names to validation status
        """
        validation_results = {}
        ddl_scripts_dir = self.path_config.get_ddl_scripts_dir()
        
        if target_datastore not in self._script_registry:
            raise ValueError(f"Unknown datastore: {target_datastore}")
        
        datastore_scripts = self._script_registry[target_datastore]
        
        for category, scripts in datastore_scripts.items():
            for source_file, _ in scripts:
                source_path = ddl_scripts_dir / source_file
                validation_results[source_file] = source_path.exists()
        
        return validation_results
    
    def get_ddl_dependencies(self, target_datastore: str) -> List[str]:
        """
        Get the ordered list of DDL dependencies for a datastore.
        
        Args:
            target_datastore: Target datastore
            
        Returns:
            Ordered list of table/object names that will be created
        """
        dependencies = {
            "lakehouse": [
                "config_data_profiling",
                "profile_results",
                "profile_history",
                "data_quality_rules",
            ],
            "warehouse": [
                "config.config_data_profiling",
                "config.profile_results",
                "config.profile_history",
                "config.data_quality_rules",
            ],
        }
        
        return dependencies.get(target_datastore, [])
    
    def _log_success(self, message: str) -> None:
        """Log a success message."""
        if self.console:
            self.console.print(f"[green]✓ {message}[/green]")
    
    def _log_error(self, message: str) -> None:
        """Log an error message."""
        if self.console:
            self.console.print(f"[red]✗ {message}[/red]")