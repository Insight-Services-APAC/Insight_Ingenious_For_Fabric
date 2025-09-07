"""
Template Compilation Module for Data Profiling

This module handles the compilation of Jinja2 templates for data profiling notebooks,
separating template concerns from the main compiler logic.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import jinja2

from ingen_fab.python_libs.common.path_configuration import PathConfiguration


class ProfilingTemplateCompiler:
    """
    Handles template compilation for data profiling notebooks.
    
    This class is responsible for loading, rendering, and managing
    Jinja2 templates for different profiling scenarios.
    """
    
    def __init__(self, path_config: Optional[PathConfiguration] = None):
        """
        Initialize the template compiler.
        
        Args:
            path_config: Path configuration instance (creates default if not provided)
        """
        self.path_config = path_config or PathConfiguration("data_profiling")
        self._template_cache = {}
        self._env = None
    
    def get_template_environment(self) -> jinja2.Environment:
        """
        Get or create the Jinja2 template environment.
        
        Returns:
            Configured Jinja2 Environment
        """
        if self._env is None:
            template_paths = self.path_config.get_template_search_paths()
            self._env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(template_paths),
                autoescape=False,
                trim_blocks=True,
                lstrip_blocks=True,
            )
        return self._env
    
    def get_template_mapping(self, target_datastore: str) -> Dict[str, str]:
        """
        Get the mapping of template types to template files for a datastore.
        
        Args:
            target_datastore: Target datastore type (lakehouse, warehouse)
            
        Returns:
            Dictionary mapping template types to template filenames
        """
        mappings = {
            "lakehouse": {
                "processor": "data_profiling_lakehouse.py.jinja",
                "config": "data_profiling_config_lakehouse.py.jinja",
            },
            "warehouse": {
                "processor": "data_profiling_warehouse.py.jinja",
                "config": "data_profiling_config_warehouse.py.jinja",
            },
        }
        
        if target_datastore not in mappings:
            raise ValueError(
                f"Unsupported target datastore: {target_datastore}. "
                f"Must be one of: {list(mappings.keys())}"
            )
        
        return mappings[target_datastore]
    
    def compile_template(
        self,
        template_name: str,
        template_vars: Dict[str, Any],
        output_path: Path,
    ) -> Path:
        """
        Compile a single template to an output file.
        
        Args:
            template_name: Name of the template file
            template_vars: Variables to pass to the template
            output_path: Path where the compiled template should be written
            
        Returns:
            Path to the compiled file
            
        Raises:
            jinja2.TemplateNotFound: If template doesn't exist
            jinja2.TemplateSyntaxError: If template has syntax errors
        """
        env = self.get_template_environment()
        
        # Load and render template
        template = env.get_template(template_name)
        rendered_content = template.render(**template_vars)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write rendered content
        output_path.write_text(rendered_content)
        
        return output_path
    
    def compile_processor_notebook(
        self,
        target_datastore: str,
        template_vars: Dict[str, Any],
        output_path: Path,
    ) -> Path:
        """
        Compile the processor notebook for a specific datastore.
        
        Args:
            target_datastore: Target datastore (lakehouse, warehouse)
            template_vars: Variables for template rendering
            output_path: Output path for the notebook
            
        Returns:
            Path to the compiled notebook
        """
        template_mapping = self.get_template_mapping(target_datastore)
        template_name = template_mapping["processor"]
        
        # Add datastore-specific variables
        enhanced_vars = self._enhance_template_vars(template_vars, target_datastore)
        
        return self.compile_template(template_name, enhanced_vars, output_path)
    
    def compile_config_notebook(
        self,
        target_datastore: str,
        template_vars: Dict[str, Any],
        output_path: Path,
    ) -> Path:
        """
        Compile the configuration notebook for a specific datastore.
        
        Args:
            target_datastore: Target datastore (lakehouse, warehouse)
            template_vars: Variables for template rendering
            output_path: Output path for the notebook
            
        Returns:
            Path to the compiled notebook
        """
        template_mapping = self.get_template_mapping(target_datastore)
        template_name = template_mapping["config"]
        
        # Add datastore-specific variables
        enhanced_vars = self._enhance_template_vars(template_vars, target_datastore)
        
        return self.compile_template(template_name, enhanced_vars, output_path)
    
    def _enhance_template_vars(
        self, base_vars: Dict[str, Any], target_datastore: str
    ) -> Dict[str, Any]:
        """
        Enhance template variables with datastore-specific settings.
        
        Args:
            base_vars: Base template variables
            target_datastore: Target datastore type
            
        Returns:
            Enhanced template variables
        """
        enhanced = base_vars.copy()
        
        # Add datastore-specific defaults
        enhanced["datastore_type"] = target_datastore
        enhanced["kernel_name"] = self._get_kernel_name(target_datastore)
        enhanced["language_group"] = self._get_language_group(target_datastore)
        enhanced["runtime_type"] = self._get_runtime_type(target_datastore)
        
        return enhanced
    
    def _get_kernel_name(self, target_datastore: str) -> str:
        """Get the kernel name for a datastore."""
        kernel_mapping = {
            "lakehouse": "synapse_pyspark",
            "warehouse": "synapse_python",
        }
        return kernel_mapping.get(target_datastore, "python3")
    
    def _get_language_group(self, target_datastore: str) -> str:
        """Get the language group for a datastore."""
        language_mapping = {
            "lakehouse": "synapse_pyspark",
            "warehouse": "synapse_python",
        }
        return language_mapping.get(target_datastore, "python")
    
    def _get_runtime_type(self, target_datastore: str) -> str:
        """Get the runtime type for a datastore."""
        runtime_mapping = {
            "lakehouse": "pyspark",
            "warehouse": "python",
        }
        return runtime_mapping.get(target_datastore, "python")
    
    def validate_template(self, template_name: str) -> bool:
        """
        Validate that a template exists and has valid syntax.
        
        Args:
            template_name: Name of the template to validate
            
        Returns:
            True if template is valid, False otherwise
        """
        try:
            env = self.get_template_environment()
            env.get_template(template_name)
            return True
        except (jinja2.TemplateNotFound, jinja2.TemplateSyntaxError):
            return False
    
    def list_available_templates(self) -> List[str]:
        """
        List all available template files.
        
        Returns:
            List of template filenames
        """
        templates = []
        for search_path in self.path_config.get_template_search_paths():
            if search_path.exists():
                templates.extend(
                    [f.name for f in search_path.glob("*.jinja")]
                )
        return list(set(templates))  # Remove duplicates