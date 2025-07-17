"""
Synapse Sync Package

This module provides functionality to compile and generate synapse sync
notebooks and DDL scripts based on templates.
"""

from pathlib import Path
from typing import Any, Dict, List

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler


class SynapseSyncCompiler(BaseNotebookCompiler):
    """Compiler for synapse sync templates"""
    
    def __init__(self, fabric_workspace_repo_dir: str = None):
        self.package_dir = Path(__file__).parent
        self.templates_dir = self.package_dir / "templates"
        self.ddl_scripts_dir = self.package_dir / "ddl_scripts"
        
        # Set up template directories - include package templates and unified templates
        root_dir = Path.cwd()
        unified_templates_dir = root_dir / "ingen_fab" / "templates"
        template_search_paths = [self.templates_dir, unified_templates_dir]
        
        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="synapse_sync"
        )
        
        if self.console:
            self.console.print(f"[bold blue]Package Directory:[/bold blue] {self.package_dir}")
            self.console.print(f"[bold blue]Templates Directory:[/bold blue] {self.templates_dir}")
            self.console.print(f"[bold blue]DDL Scripts Directory:[/bold blue] {self.ddl_scripts_dir}")
    
    def compile_notebook(self, template_vars: Dict[str, Any] = None) -> Path:
        """Compile the synapse sync notebook template"""
        
        return self.compile_notebook_from_template(
            template_name="synapse_sync_notebook.py.jinja",
            output_notebook_name="synapse_sync_processor",
            template_vars=template_vars,
            display_name="Synapse Sync Processor",
            description="Orchestrates extraction of data from Azure Synapse Analytics to Parquet files stored in ADLS",
            output_subdir="synapse_sync"
        )
    
    def compile_ddl_scripts(self, include_sample_data: bool = False) -> List[Path]:
        """Compile DDL scripts to the target directory"""
        
        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"
        
        # Define script mappings for different targets
        script_mappings = {
            "Lakehouses/Config/001_Initial_Creation_Synapse_Sync": [
                ("lakehouse/config_synapse_extract_objects_create.py.jinja", "001_config_synapse_extract_objects_create.py"),
                ("lakehouse/log_synapse_extract_run_log_create.py.jinja", "002_log_synapse_extract_run_log_create.py")
            ],
            "Warehouses/Config/001_Initial_Creation_Synapse_Sync": [
                ("warehouse/config_synapse_extract_objects_create.sql.jinja", "001_config_synapse_extract_objects_create.sql"),
                ("warehouse/log_synapse_extract_run_log_create.sql.jinja", "002_log_synapse_extract_run_log_create.sql")
            ]
        }
        
        # Add sample data scripts if requested
        if include_sample_data:
            script_mappings["Lakehouses/Config/002_Sample_Data_Synapse_Sync"] = [
                ("lakehouse/config_synapse_extract_objects_insert.py.jinja", "003_config_synapse_extract_objects_insert.py")
            ]
            script_mappings["Warehouses/Config/002_Sample_Data_Synapse_Sync"] = [
                ("warehouse/config_synapse_extract_objects_insert.sql.jinja", "003_config_synapse_extract_objects_insert.sql")
            ]
        
        # Process DDL scripts with template rendering
        results = self._compile_ddl_scripts_with_templates(self.ddl_scripts_dir, ddl_output_base, script_mappings)
        
        # Flatten results into a single list for backward compatibility
        compiled_files = []
        for file_list in results.values():
            compiled_files.extend(file_list)
        
        return compiled_files
    
    def _compile_ddl_scripts_with_templates(self, ddl_source_dir: Path, ddl_output_base: Path, script_mappings: Dict[str, List[tuple]]) -> Dict[str, List[Path]]:
        """Compile DDL scripts with Jinja template processing"""
        from pathlib import Path

        import jinja2
        
        results = {}
        
        for folder_path, file_mappings in script_mappings.items():
            folder_results = []
            
            # Create target directory
            target_dir = ddl_output_base / folder_path
            target_dir.mkdir(parents=True, exist_ok=True)
            
            for source_file, target_file in file_mappings:
                source_path = ddl_source_dir / source_file
                target_path = target_dir / target_file
                
                if source_file.endswith('.jinja'):
                    # Process as Jinja template
                    try:
                        if source_path.exists():
                            template_content = source_path.read_text()
                            
                            # Create a template environment that includes our DDL scripts directory and unified templates
                            template_paths = [ddl_source_dir]
                            # Add the unified templates directory
                            unified_templates_dir = Path.cwd() / "ingen_fab" / "templates"
                            template_paths.append(unified_templates_dir)
                            
                            env = jinja2.Environment(
                                loader=jinja2.FileSystemLoader(template_paths),
                                autoescape=False
                            )
                            
                            template = env.from_string(template_content)
                            rendered_content = template.render()
                            
                            target_path.write_text(rendered_content)
                            if self.console:
                                self.console.print(f"[green]✓ Template rendered:[/green] {target_path}")
                        else:
                            if self.console:
                                self.console.print(f"[red]✗ Template file not found:[/red] {source_path}")
                            continue
                    except Exception as e:
                        if self.console:
                            self.console.print(f"[red]✗ Template error:[/red] {source_file} - {e}")
                        continue
                else:
                    # Copy file directly
                    if source_path.exists():
                        target_path.write_bytes(source_path.read_bytes())
                        if self.console:
                            self.console.print(f"[green]✓ File copied:[/green] {target_path}")
                    else:
                        if self.console:
                            self.console.print(f"[red]✗ Source file not found:[/red] {source_path}")
                        continue
                
                folder_results.append(target_path)
            
            results[folder_path] = folder_results
        
        return results
    
    def compile_all(self, template_vars: Dict[str, Any] = None, include_samples: bool = False) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""
        
        compile_functions = [
            (self.compile_notebook, [template_vars], {}),
            (self.compile_ddl_scripts, [], {"include_sample_data": include_samples})
        ]
        
        results = self.compile_all_with_results(
            compile_functions, 
            "Synapse Sync Package Compiler"
        )
        
        # Transform results for backward compatibility
        if results["success"]:
            notebook_file = results["compiled_items"].get("compile_notebook")
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])
            
            success_message = (
                f"✓ Successfully compiled synapse sync package\n"
                f"Notebook: {notebook_file}\n"
                f"DDL Scripts: {len(ddl_files)} files\n"
                f"📈 Improvements: Uses Pipeline_Utils for better error handling and modularity"
            )
            
            self.print_success_panel("Compilation Complete", success_message)
            
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


def compile_synapse_sync_package(fabric_workspace_repo_dir: str = None, 
                                template_vars: Dict[str, Any] = None,
                                include_samples: bool = False) -> Dict[str, Any]:
    """Main function to compile the synapse sync package"""
    
    compiler = SynapseSyncCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars, include_samples=include_samples)