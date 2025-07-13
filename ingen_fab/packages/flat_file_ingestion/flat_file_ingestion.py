"""
Flat File Ingestion Package

This module provides functionality to compile and generate flat file ingestion
notebooks and DDL scripts based on templates.
"""

from pathlib import Path
from typing import Any, Dict, List

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler


class FlatFileIngestionCompiler(BaseNotebookCompiler):
    """Compiler for flat file ingestion templates"""
    
    def __init__(self, fabric_workspace_repo_dir: str = None):
        self.package_dir = Path(__file__).parent
        self.templates_dir = self.package_dir / "templates"
        self.ddl_scripts_dir = self.package_dir / "ddl_scripts"
        
        # Set up template directories - include package templates, common templates, and unified templates
        root_dir = Path.cwd()
        common_templates_dir = root_dir / "ingen_fab" / "notebook_utils" / "templates"
        unified_templates_dir = root_dir / "ingen_fab" / "templates"
        template_search_paths = [self.templates_dir, common_templates_dir, unified_templates_dir]
        
        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="flat_file_ingestion"
        )
        
        if self.console:
            self.console.print(f"[bold blue]Package Directory:[/bold blue] {self.package_dir}")
            self.console.print(f"[bold blue]Templates Directory:[/bold blue] {self.templates_dir}")
            self.console.print(f"[bold blue]DDL Scripts Directory:[/bold blue] {self.ddl_scripts_dir}")
    
    def compile_notebook(self, template_vars: Dict[str, Any] = None) -> Path:
        """Compile the flat file ingestion notebook template"""
        
        return self.compile_notebook_from_template(
            template_name="flat_file_ingestion_notebook.py.jinja",
            output_notebook_name="flat_file_ingestion_processor",
            template_vars=template_vars,
            display_name="Flat File Ingestion Processor",
            description="Processes flat files and loads them into delta tables based on configuration metadata",
            output_subdir="flat_file_ingestion"
        )
    
    def compile_ddl_scripts(self) -> List[Path]:
        """Compile DDL scripts to the target directory"""
        
        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"
        
        # Define script mappings for different targets
        script_mappings = {
            "Lakehouses/Config/001_Initial_Creation_Ingestion": [
                ("lakehouse_config_create.py", "001_config_flat_file_ingestion_create.py"),
                ("lakehouse_log_create.py", "002_log_flat_file_ingestion_create.py")
            ],
            "Warehouses/Config/001_Initial_Creation_Ingestion": [
                ("warehouse_config_create.sql", "001_config_flat_file_ingestion_create.sql"),
                ("warehouse_log_create.sql", "002_log_flat_file_ingestion_create.sql")
            ]
        }
        
        # Use base class method to copy files
        results = super().compile_ddl_scripts(self.ddl_scripts_dir, ddl_output_base, script_mappings)
        
        # Flatten results into a single list for backward compatibility
        compiled_files = []
        for file_list in results.values():
            compiled_files.extend(file_list)
        
        return compiled_files
    
    def compile_all(self, template_vars: Dict[str, Any] = None) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""
        
        compile_functions = [
            (self.compile_notebook, [template_vars], {}),
            (self.compile_ddl_scripts, [], {})
        ]
        
        results = self.compile_all_with_results(
            compile_functions, 
            "Flat File Ingestion Package Compiler"
        )
        
        # Transform results for backward compatibility
        if results["success"]:
            notebook_file = results["compiled_items"].get("compile_notebook")
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])
            
            self.print_success_panel(
                "Compilation Complete",
                f"✓ Successfully compiled flat file ingestion package\n"
                f"Notebook: {notebook_file}\n"
                f"DDL Scripts: {len(ddl_files)} files"
            )
            
            # Return in expected format
            return {
                "notebook_file": notebook_file,
                "ddl_files": ddl_files,
                "success": True,
                "errors": []
            }
        else:
            return {
                "notebook_file": None,
                "ddl_files": [],
                "success": False,
                "errors": results["errors"]
            }


def compile_flat_file_ingestion_package(fabric_workspace_repo_dir: str = None, 
                                       template_vars: Dict[str, Any] = None) -> Dict[str, Any]:
    """Main function to compile the flat file ingestion package"""
    
    compiler = FlatFileIngestionCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars)