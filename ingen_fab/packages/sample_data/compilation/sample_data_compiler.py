from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler

logger = logging.getLogger(__name__)


class SampleDataCompiler(BaseNotebookCompiler):
    """Compiler for generating sample data loading notebooks."""
    
    def __init__(
        self,
        fabric_workspace_repo_dir: Union[str, Path] = None,
        output_dir: Union[str, Path] = None,
        **kwargs
    ):
        """Initialize the SampleDataCompiler.
        
        Args:
            fabric_workspace_repo_dir: Root directory of the fabric workspace repository
            output_dir: Output directory for generated notebooks
            **kwargs: Additional arguments passed to BaseNotebookCompiler
        """
        # Set templates directory to our package templates
        templates_dir = Path(__file__).parent.parent / "templates"
        
        super().__init__(
            templates_dir=templates_dir,
            output_dir=output_dir,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="sample_data",
            **kwargs
        )
        
    def compile_sample_data_notebook(
        self,
        notebook_name: str = "load_sample_datasets",
        datasets: Optional[List[str]] = None,
        auto_load: bool = True,
        include_custom_loader: bool = True,
        workspace_id: Optional[str] = None,
        lakehouse_id: Optional[str] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Path:
        """Compile a notebook for loading sample datasets.
        
        Args:
            notebook_name: Name for the output notebook
            datasets: List of dataset names to include (None for all)
            auto_load: Whether to automatically load datasets on notebook run
            include_custom_loader: Include custom dataset loading capability
            workspace_id: Target workspace ID
            lakehouse_id: Target lakehouse ID
            display_name: Display name for the notebook
            description: Description for the notebook
            
        Returns:
            Path to the created notebook
        """
        if display_name is None:
            display_name = "Load Sample Datasets"
            
        if description is None:
            description = "Notebook for loading sample datasets into the lakehouse"
            
        # Prepare template variables
        template_vars = {
            "datasets": datasets or [],
            "auto_load": auto_load,
            "include_custom_loader": include_custom_loader,
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "package_name": self.package_name,
        }
        
        # Compile the notebook
        logger.info(f"Compiling sample data notebook: {notebook_name}")
        
        return self.compile_notebook_from_template(
            template_name="sample_data_loader.jinja2",
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=display_name,
            description=description,
            output_subdir="sample_data"
        )
        
    def compile_data_explorer_notebook(
        self,
        notebook_name: str = "explore_sample_data",
        include_profiling: bool = True,
        include_visualization: bool = True,
        workspace_id: Optional[str] = None,
        lakehouse_id: Optional[str] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Path:
        """Compile a notebook for exploring loaded sample datasets.
        
        Args:
            notebook_name: Name for the output notebook
            include_profiling: Include data profiling capabilities
            include_visualization: Include visualization capabilities
            workspace_id: Target workspace ID
            lakehouse_id: Target lakehouse ID
            display_name: Display name for the notebook
            description: Description for the notebook
            
        Returns:
            Path to the created notebook
        """
        if display_name is None:
            display_name = "Explore Sample Data"
            
        if description is None:
            description = "Notebook for exploring and analyzing sample datasets"
            
        # Prepare template variables
        template_vars = {
            "include_profiling": include_profiling,
            "include_visualization": include_visualization,
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "package_name": self.package_name,
        }
        
        logger.info(f"Compiling data explorer notebook: {notebook_name}")
        
        return self.compile_notebook_from_template(
            template_name="sample_data_explorer.jinja2",
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=display_name,
            description=description,
            output_subdir="sample_data"
        )
        
    def compile_dataset_registry_notebook(
        self,
        notebook_name: str = "dataset_registry",
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Path:
        """Compile a notebook showing available datasets in the registry.
        
        Args:
            notebook_name: Name for the output notebook
            display_name: Display name for the notebook
            description: Description for the notebook
            
        Returns:
            Path to the created notebook
        """
        if display_name is None:
            display_name = "Dataset Registry"
            
        if description is None:
            description = "View all available sample datasets and their information"
            
        template_vars = {
            "package_name": self.package_name,
        }
        
        logger.info(f"Compiling dataset registry notebook: {notebook_name}")
        
        return self.compile_notebook_from_template(
            template_name="dataset_registry.jinja2",
            output_notebook_name=notebook_name,
            template_vars=template_vars,
            display_name=display_name,
            description=description,
            output_subdir="sample_data"
        )