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
        
        # Set up template directories - include package templates and unified templates
        root_dir = Path.cwd()
        unified_templates_dir = root_dir / "ingen_fab" / "templates"
        template_search_paths = [self.templates_dir, unified_templates_dir]
        
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
    
    def compile_ddl_scripts(self, include_sample_data: bool = False) -> List[Path]:
        """Compile DDL scripts to the target directory"""
        
        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"
        
        # Define script mappings for different targets
        script_mappings = {
            "Lakehouses/Config/001_Initial_Creation_Ingestion": [
                ("lakehouse/config_create.py.jinja", "001_config_flat_file_ingestion_create.py"),
                ("lakehouse/log_create.py", "002_log_flat_file_ingestion_create.py")
            ],
            "Warehouses/Config/001_Initial_Creation_Ingestion": [
                ("warehouse/config_create.sql", "001_config_flat_file_ingestion_create.sql"),
                ("warehouse/log_create.sql", "002_log_flat_file_ingestion_create.sql")
            ]
        }
        
        # Add sample data scripts if requested
        if include_sample_data:
            script_mappings["Lakehouses/Config/002_Sample_Data_Ingestion"] = [
                ("lakehouse/sample_data_insert.py.jinja", "003_sample_data_insert.py")
            ]
            script_mappings["Warehouses/Config/002_Sample_Data_Ingestion"] = [
                ("warehouse/sample_data_insert.sql", "003_sample_data_insert.sql")
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
    
    def compile_sample_files(self) -> List[Path]:
        """Upload sample data files to the lakehouse using lakehouse_utils"""
        
        sample_source_dir = self.package_dir / "sample_project" / "sample_data"
        
        if not sample_source_dir.exists():
            self.print_error("Sample data directory not found", f"Expected: {sample_source_dir}")
            return []
        
        # Import lakehouse_utils for file upload in local mode
        try:
            from pyspark.sql import SparkSession

            from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
            
            # Get or create Spark session
            spark = SparkSession.builder.appName("FlatFileIngestionCompiler").getOrCreate()
            
            # Initialize lakehouse_utils for the raw lakehouse (where sample files go)
            raw_lakehouse = lakehouse_utils(
                spark=spark,
                lakehouse_id=None,  # Use default from environment
                workspace_id=None,  # Use default from environment
                lakehouse_name="raw"
            )
            
            # Upload all files from sample_data directory
            uploaded_files = []
            for file_path in sample_source_dir.iterdir():
                if file_path.is_file():
                    # Read file content based on type
                    relative_path = f"Files/sample_data/{file_path.name}"
                    
                    if file_path.suffix == '.csv':
                        # Read CSV and write using lakehouse_utils
                        df = spark.read.option("header", "true").csv(str(file_path))
                        raw_lakehouse.write_file(
                            df=df,
                            file_path=relative_path,
                            file_format="csv",
                            options={"header": True}
                        )
                    elif file_path.suffix == '.json':
                        # Read JSON and write using lakehouse_utils
                        df = spark.read.json(str(file_path))
                        raw_lakehouse.write_file(
                            df=df,
                            file_path=relative_path,
                            file_format="json"
                        )
                    elif file_path.suffix == '.parquet':
                        # Read Parquet and write using lakehouse_utils
                        df = spark.read.parquet(str(file_path))
                        raw_lakehouse.write_file(
                            df=df,
                            file_path=relative_path,
                            file_format="parquet"
                        )
                    else:
                        # For other file types, copy as-is (not supported in this implementation)
                        if self.console:
                            self.console.print(f"[yellow]⚠ Skipping unsupported file type:[/yellow] {file_path.name}")
                        continue
                    
                    uploaded_files.append(Path(relative_path))
                    if self.console:
                        self.console.print(f"[green]✓ File uploaded:[/green] {relative_path}")
                        
        except Exception as e:
            # Fallback to file copy if lakehouse_utils is not available
            if self.console:
                self.console.print(f"[yellow]⚠ Lakehouse upload not available, falling back to file copy:[/yellow] {e}")
            
            sample_output_dir = self.fabric_workspace_repo_dir / "Files" / "sample_data"
            sample_output_dir.mkdir(parents=True, exist_ok=True)
            
            uploaded_files = []
            for file_path in sample_source_dir.iterdir():
                if file_path.is_file():
                    target_path = sample_output_dir / file_path.name
                    target_path.write_bytes(file_path.read_bytes())
                    uploaded_files.append(target_path)
                    if self.console:
                        self.console.print(f"[green]✓ File copied:[/green] {target_path}")
        
        return uploaded_files
    
    def compile_all(self, template_vars: Dict[str, Any] = None, include_samples: bool = False) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""
        
        compile_functions = [
            (self.compile_notebook, [template_vars], {}),
            (self.compile_ddl_scripts, [], {"include_sample_data": include_samples})
        ]
        
        # Add sample files compilation if requested
        if include_samples:
            compile_functions.append((self.compile_sample_files, [], {}))
        
        results = self.compile_all_with_results(
            compile_functions, 
            "Flat File Ingestion Package Compiler"
        )
        
        # Transform results for backward compatibility
        if results["success"]:
            notebook_file = results["compiled_items"].get("compile_notebook")
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])
            sample_files = results["compiled_items"].get("compile_sample_files", [])
            
            success_message = (
                f"✓ Successfully compiled flat file ingestion package\n"
                f"Notebook: {notebook_file}\n"
                f"DDL Scripts: {len(ddl_files)} files"
            )
            
            if sample_files:
                success_message += f"\nSample Files: {len(sample_files)} files"
            
            self.print_success_panel("Compilation Complete", success_message)
            
            # Return in expected format
            return {
                "notebook_file": notebook_file,
                "ddl_files": ddl_files,
                "sample_files": sample_files,
                "success": True,
                "errors": []
            }
        else:
            return {
                "notebook_file": None,
                "ddl_files": [],
                "sample_files": [],
                "success": False,
                "errors": results["errors"]
            }


def compile_flat_file_ingestion_package(fabric_workspace_repo_dir: str = None, 
                                       template_vars: Dict[str, Any] = None,
                                       include_samples: bool = False) -> Dict[str, Any]:
    """Main function to compile the flat file ingestion package"""
    
    compiler = FlatFileIngestionCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars, include_samples=include_samples)