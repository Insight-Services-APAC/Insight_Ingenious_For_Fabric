"""
Flat File Ingestion Package

This module provides functionality to compile and generate flat file ingestion
notebooks and DDL scripts based on templates.
"""

import os
import uuid
from pathlib import Path
from typing import Dict, Any, List
from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.panel import Panel

from ingen_fab.python_libs.common.config_utils import get_configs_as_object


class FlatFileIngestionCompiler:
    """Compiler for flat file ingestion templates"""
    
    def __init__(self, fabric_workspace_repo_dir: str = None):
        self.console = Console()
        self.package_dir = Path(__file__).parent
        self.templates_dir = self.package_dir / "templates"
        self.ddl_scripts_dir = self.package_dir / "ddl_scripts"
        
        if fabric_workspace_repo_dir is None:
            fabric_workspace_repo_dir = Path.cwd() / "sample_project"
        
        self.fabric_workspace_repo_dir = Path(fabric_workspace_repo_dir)
        self.output_dir = self.fabric_workspace_repo_dir / "fabric_workspace_items"
        
        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            autoescape=False
        )
        
        self.console.print(f"[bold blue]Package Directory:[/bold blue] {self.package_dir}")
        self.console.print(f"[bold blue]Templates Directory:[/bold blue] {self.templates_dir}")
        self.console.print(f"[bold blue]Output Directory:[/bold blue] {self.output_dir}")
    
    def compile_notebook(self, template_vars: Dict[str, Any] = None) -> Path:
        """Compile the flat file ingestion notebook template"""
        
        if template_vars is None:
            template_vars = {}
        
        # Load configuration
        try:
            configs = get_configs_as_object()
            template_vars.update({
                "varlib": {
                    "config_workspace_id": getattr(configs, "config_workspace_id", ""),
                    "config_workspace_name": getattr(configs, "config_workspace_name", ""),
                    "config_lakehouse_workspace_id": getattr(configs, "config_lakehouse_workspace_id", ""),
                    "config_lakehouse_id": getattr(configs, "config_lakehouse_id", "")
                }
            })
        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not load config variables: {e}[/yellow]")
            template_vars.setdefault("varlib", {})
        
        # Load template
        template = self.jinja_env.get_template("flat_file_ingestion_notebook.py.jinja")
        
        # Render template
        rendered_content = template.render(**template_vars)
        
        # Create output directory
        notebook_dir = self.output_dir / "flat_file_ingestion" / "flat_file_ingestion_processor.Notebook"
        notebook_dir.mkdir(parents=True, exist_ok=True)
        
        # Write notebook content
        notebook_file = notebook_dir / "notebook-content.py"
        with notebook_file.open("w", encoding="utf-8") as f:
            f.write(rendered_content)
        
        # Create .platform file
        platform_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "Notebook",
                "displayName": "Flat File Ingestion Processor",
                "description": "Processes flat files and loads them into delta tables based on configuration metadata"
            },
            "config": {
                "version": "2.0",
                "logicalId": str(uuid.uuid4())
            }
        }
        
        platform_file = notebook_dir / ".platform"
        import json
        with platform_file.open("w", encoding="utf-8") as f:
            json.dump(platform_content, f, indent=2)
        
        self.console.print(f"[green]✓ Notebook compiled to: {notebook_file}[/green]")
        return notebook_file
    
    def compile_ddl_scripts(self) -> List[Path]:
        """Compile DDL scripts to the target directory"""
        
        compiled_files = []
        
        # Create DDL scripts directory structure
        ddl_output_dir = self.output_dir / "ddl_scripts" / "flat_file_ingestion"
        ddl_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy lakehouse DDL scripts
        lakehouse_dir = ddl_output_dir / "Lakehouses" / "Config" / "001_Initial_Creation"
        lakehouse_dir.mkdir(parents=True, exist_ok=True)
        
        lakehouse_files = [
            ("lakehouse_config_create.py", "001_config_flat_file_ingestion_create.py"),
            ("lakehouse_log_create.py", "002_log_flat_file_ingestion_create.py")
        ]
        
        for source_file, target_file in lakehouse_files:
            source_path = self.ddl_scripts_dir / source_file
            target_path = lakehouse_dir / target_file
            
            if source_path.exists():
                with source_path.open("r", encoding="utf-8") as f:
                    content = f.read()
                with target_path.open("w", encoding="utf-8") as f:
                    f.write(content)
                compiled_files.append(target_path)
                self.console.print(f"[green]✓ DDL script compiled to: {target_path}[/green]")
        
        # Copy warehouse DDL scripts
        warehouse_dir = ddl_output_dir / "Warehouses" / "Config" / "001_Initial_Creation"
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        
        warehouse_files = [
            ("warehouse_config_create.sql", "001_config_flat_file_ingestion_create.sql"),
            ("warehouse_log_create.sql", "002_log_flat_file_ingestion_create.sql")
        ]
        
        for source_file, target_file in warehouse_files:
            source_path = self.ddl_scripts_dir / source_file
            target_path = warehouse_dir / target_file
            
            if source_path.exists():
                with source_path.open("r", encoding="utf-8") as f:
                    content = f.read()
                with target_path.open("w", encoding="utf-8") as f:
                    f.write(content)
                compiled_files.append(target_path)
                self.console.print(f"[green]✓ DDL script compiled to: {target_path}[/green]")
        
        return compiled_files
    
    def compile_all(self, template_vars: Dict[str, Any] = None) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""
        
        self.console.print(
            Panel.fit(
                "[bold cyan]Flat File Ingestion Package Compiler[/bold cyan]",
                border_style="cyan"
            )
        )
        
        results = {
            "notebook_file": None,
            "ddl_files": [],
            "success": True,
            "errors": []
        }
        
        try:
            # Compile notebook
            notebook_file = self.compile_notebook(template_vars)
            results["notebook_file"] = notebook_file
            
            # Compile DDL scripts
            ddl_files = self.compile_ddl_scripts()
            results["ddl_files"] = ddl_files
            
            self.console.print(
                Panel.fit(
                    f"[bold green]✓ Successfully compiled flat file ingestion package[/bold green]\n"
                    f"[green]Notebook: {notebook_file}[/green]\n"
                    f"[green]DDL Scripts: {len(ddl_files)} files[/green]",
                    title="[bold]Compilation Complete[/bold]",
                    border_style="green"
                )
            )
            
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            self.console.print(f"[red]Error compiling package: {e}[/red]")
            raise
        
        return results


def compile_flat_file_ingestion_package(fabric_workspace_repo_dir: str = None, 
                                       template_vars: Dict[str, Any] = None) -> Dict[str, Any]:
    """Main function to compile the flat file ingestion package"""
    
    compiler = FlatFileIngestionCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars)