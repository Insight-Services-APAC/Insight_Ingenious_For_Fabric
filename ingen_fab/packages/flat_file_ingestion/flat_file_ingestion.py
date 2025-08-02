"""
Flat File Ingestion Package

This module provides functionality to compile and generate flat file ingestion
notebooks and DDL scripts based on templates.
"""

from pathlib import Path
from typing import Any, Dict, List

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler
from ingen_fab.python_libs.common.utils.path_utils import PathUtils


class FlatFileIngestionCompiler(BaseNotebookCompiler):
    """Compiler for flat file ingestion templates"""

    def __init__(self, fabric_workspace_repo_dir: str = None):
        try:
            # Use path utilities for package resource discovery
            package_base = PathUtils.get_package_resource_path(
                "packages/flat_file_ingestion"
            )
            self.package_dir = package_base
            self.templates_dir = package_base / "templates"
            self.ddl_scripts_dir = package_base / "ddl_scripts"
        except FileNotFoundError:
            # Fallback for development environment
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
            package_name="flat_file_ingestion",
        )

        if self.console:
            self.console.print(
                f"[bold blue]Package Directory:[/bold blue] {self.package_dir}"
            )
            self.console.print(
                f"[bold blue]Templates Directory:[/bold blue] {self.templates_dir}"
            )
            self.console.print(
                f"[bold blue]DDL Scripts Directory:[/bold blue] {self.ddl_scripts_dir}"
            )

    def compile_notebook(
        self, template_vars: Dict[str, Any] = None, target_datastore: str = "lakehouse"
    ) -> Path:
        """Compile the flat file ingestion notebook template"""

        # Select template based on target datastore
        template_mapping = {
            "lakehouse": "flat_file_ingestion_lakehouse.py.jinja",
            "warehouse": "flat_file_ingestion_warehouse.py.jinja",
        }

        template_name = template_mapping.get(target_datastore)
        if not template_name:
            raise ValueError(
                f"Unsupported target datastore: {target_datastore}. Must be 'lakehouse' or 'warehouse'"
            )

        # Customize output name and description based on target
        output_name = f"flat_file_ingestion_processor_{target_datastore}"
        display_name = f"Flat File Ingestion Processor ({target_datastore.title()})"
        description = f"Processes flat files and loads them into {target_datastore} tables based on configuration metadata"

        if target_datastore == "warehouse":
            description += " using COPY INTO operations"

        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=output_name,
            template_vars=template_vars,
            display_name=display_name,
            description=description,
            output_subdir="flat_file_ingestion",
        )

    def compile_ddl_scripts(
        self, include_sample_data: bool = False, target_datastore: str = "both"
    ) -> List[Path]:
        """Compile DDL scripts to the target directory"""

        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"

        # Define script mappings for different targets
        all_script_mappings = {
            "Lakehouses/Config/001_Initial_Creation_Ingestion": [
                (
                    "lakehouse/config_create_universal.py.jinja",
                    "001_config_flat_file_ingestion_create.py",
                ),
                ("lakehouse/log_create.py", "002_log_flat_file_ingestion_create.py"),
            ],
            "Warehouses/Config/000_Initial_Schema_Creation": [
                ("warehouse/000_schema_creation.py.jinja", "000_schema_creation.py")
            ],
            "Warehouses/Config/001_Initial_Creation_Ingestion": [
                (
                    "warehouse/config_create_universal.sql.jinja",
                    "001_config_flat_file_ingestion_create.sql",
                ),
                ("warehouse/log_create.sql", "002_log_flat_file_ingestion_create.sql"),
            ],
        }

        # Add sample data scripts if requested
        if include_sample_data:
            all_script_mappings["Lakehouses/Config/002_Sample_Data_Ingestion"] = [
                (
                    "lakehouse/sample_data_insert_universal.py.jinja",
                    "003_sample_data_insert.py",
                )
            ]
            all_script_mappings["Warehouses/Config/002_Sample_Data_Ingestion"] = [
                (
                    "warehouse/sample_data_insert_universal.sql.jinja",
                    "003_sample_data_insert.sql",
                )
            ]

        # Filter script mappings based on target datastore
        script_mappings = {}
        if target_datastore == "lakehouse":
            script_mappings = {
                k: v for k, v in all_script_mappings.items() if "Lakehouses" in k
            }
        elif target_datastore == "warehouse":
            script_mappings = {
                k: v for k, v in all_script_mappings.items() if "Warehouses" in k
            }
        elif target_datastore == "both":
            script_mappings = all_script_mappings
        else:
            raise ValueError(
                f"Unsupported target datastore: {target_datastore}. Must be 'lakehouse', 'warehouse', or 'both'"
            )

        # Process DDL scripts with template rendering
        results = self._compile_ddl_scripts_with_templates(
            self.ddl_scripts_dir, ddl_output_base, script_mappings
        )

        # Flatten results into a single list for backward compatibility
        compiled_files = []
        for file_list in results.values():
            compiled_files.extend(file_list)

        return compiled_files

    def _compile_ddl_scripts_with_templates(
        self,
        ddl_source_dir: Path,
        ddl_output_base: Path,
        script_mappings: Dict[str, List[tuple]],
    ) -> Dict[str, List[Path]]:
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

                if source_file.endswith(".jinja"):
                    # Process as Jinja template
                    try:
                        if source_path.exists():
                            template_content = source_path.read_text()

                            # Create a template environment that includes our DDL scripts directory and
                            # unified templates
                            template_paths = [ddl_source_dir]
                            # Add the unified templates directory
                            unified_templates_dir = (
                                Path.cwd() / "ingen_fab" / "templates"
                            )
                            template_paths.append(unified_templates_dir)

                            env = jinja2.Environment(
                                loader=jinja2.FileSystemLoader(template_paths),
                                autoescape=False,
                            )

                            template = env.from_string(template_content)
                            rendered_content = template.render()

                            target_path.write_text(rendered_content)
                            if self.console:
                                self.console.print(
                                    f"[green]✓ Template rendered:[/green] {target_path}"
                                )
                        else:
                            if self.console:
                                self.console.print(
                                    f"[red]✗ Template file not found:[/red] {source_path}"
                                )
                            continue
                    except Exception as e:
                        if self.console:
                            self.console.print(
                                f"[red]✗ Template error:[/red] {source_file} - {e}"
                            )
                        continue
                else:
                    # Copy file directly
                    if source_path.exists():
                        target_path.write_bytes(source_path.read_bytes())
                        if self.console:
                            self.console.print(
                                f"[green]✓ File copied:[/green] {target_path}"
                            )
                    else:
                        if self.console:
                            self.console.print(
                                f"[red]✗ Source file not found:[/red] {source_path}"
                            )
                        continue

                folder_results.append(target_path)

            results[folder_path] = folder_results

        return results

    def compile_sample_files(self) -> List[Path]:
        """Upload sample data files to the lakehouse using lakehouse_utils"""

        # Get workspace repo directory using the path utilities
        workspace_repo_dir = PathUtils.get_workspace_repo_dir()
        sample_source_dir = workspace_repo_dir / "Files" / "sample_data"

        if not sample_source_dir.exists():
            if self.console:
                self.console.print("[red]Error: Sample data directory not found[/red]")
                self.console.print(f"[red]Expected: {sample_source_dir}[/red]")
            return []

        # Import lakehouse_utils for file upload in local mode
        try:
            import ingen_fab.python_libs.common.config_utils as cu
            from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

            # Get config values
            configs = cu.get_configs_as_object()

            # Initialize lakehouse_utils for the raw lakehouse (where sample files go)
            # Note: Using config lakehouse as the target for sample files
            raw_lakehouse = lakehouse_utils(
                target_workspace_id=configs.config_workspace_id,
                target_lakehouse_id=configs.config_lakehouse_id,
                spark=None,  # Will create/reuse session automatically
            )

            # Upload all files from sample_data directory
            uploaded_files = []
            for file_path in sample_source_dir.iterdir():
                if file_path.is_file():
                    # Read file content based on type
                    relative_path = f"Files/sample_data/{file_path.name}"

                    if file_path.suffix == ".csv":
                        # Read CSV and write using lakehouse_utils
                        # For local files, use file:// prefix to indicate absolute path
                        df = raw_lakehouse.read_file(
                            file_path=f"file://{file_path.absolute()}",
                            file_format="csv",
                            options={"header": True},
                        )
                        raw_lakehouse.write_file(
                            df=df,
                            file_path=relative_path,
                            file_format="csv",
                            options={"header": True},
                        )
                    elif file_path.suffix == ".json":
                        # Read JSON and write using lakehouse_utils
                        # For local files, use file:// prefix to indicate absolute path
                        df = raw_lakehouse.read_file(
                            file_path=f"file://{file_path.absolute()}",
                            file_format="json",
                        )
                        raw_lakehouse.write_file(
                            df=df, file_path=relative_path, file_format="json"
                        )
                    elif file_path.suffix == ".parquet":
                        # Read Parquet and write using lakehouse_utils
                        # For local files, use file:// prefix to indicate absolute path
                        df = raw_lakehouse.read_file(
                            file_path=f"file://{file_path.absolute()}",
                            file_format="parquet",
                        )
                        raw_lakehouse.write_file(
                            df=df, file_path=relative_path, file_format="parquet"
                        )
                    else:
                        # For other file types, copy as-is (not supported in this implementation)
                        if self.console:
                            self.console.print(
                                f"[yellow]⚠ Skipping unsupported file type:[/yellow] {file_path.name}"
                            )
                        continue

                    uploaded_files.append(Path(relative_path))
                    if self.console:
                        self.console.print(
                            f"[green]✓ File uploaded:[/green] {relative_path}"
                        )

        except Exception as e:
            # Fallback to file copy if lakehouse_utils is not available
            if self.console:
                self.console.print(
                    f"[yellow]⚠ Lakehouse upload not available, falling back to file copy:[/yellow] {e}"
                )

            sample_output_dir = self.fabric_workspace_repo_dir / "Files" / "sample_data"
            sample_output_dir.mkdir(parents=True, exist_ok=True)

            uploaded_files = []
            for file_path in sample_source_dir.iterdir():
                if file_path.is_file():
                    target_path = sample_output_dir / file_path.name
                    target_path.write_bytes(file_path.read_bytes())
                    uploaded_files.append(target_path)
                    if self.console:
                        self.console.print(
                            f"[green]✓ File copied:[/green] {target_path}"
                        )

        return uploaded_files

    def compile_all(
        self,
        template_vars: Dict[str, Any] = None,
        include_samples: bool = False,
        target_datastore: str = "lakehouse",
    ) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""

        # Handle "both" option by compiling both variants
        if target_datastore == "both":
            lakehouse_results = self.compile_all(
                template_vars, include_samples, "lakehouse"
            )
            warehouse_results = self.compile_all(
                template_vars, include_samples, "warehouse"
            )

            # Combine results
            combined_success = (
                lakehouse_results["success"] and warehouse_results["success"]
            )
            combined_errors = lakehouse_results["errors"] + warehouse_results["errors"]
            combined_ddl_files = (
                lakehouse_results["ddl_files"] + warehouse_results["ddl_files"]
            )

            success_message = (
                f"✓ Successfully compiled flat file ingestion package for both lakehouse and warehouse\n"
                f"Lakehouse Notebook: {lakehouse_results['notebook_file']}\n"
                f"Warehouse Notebook: {warehouse_results['notebook_file']}\n"
                f"DDL Scripts: {len(combined_ddl_files)} files"
            )

            if lakehouse_results.get("sample_files"):
                success_message += (
                    f"\nSample Files: {len(lakehouse_results['sample_files'])} files"
                )

            if combined_success:
                self.print_success_panel("Compilation Complete", success_message)

            return {
                "notebook_file": [
                    lakehouse_results["notebook_file"],
                    warehouse_results["notebook_file"],
                ],
                "ddl_files": combined_ddl_files,
                "sample_files": lakehouse_results.get("sample_files", []),
                "success": combined_success,
                "errors": combined_errors,
            }

        compile_functions = [
            (
                self.compile_notebook,
                [template_vars],
                {"target_datastore": target_datastore},
            ),
            (
                self.compile_ddl_scripts,
                [],
                {
                    "include_sample_data": include_samples,
                    "target_datastore": target_datastore,
                },
            ),
        ]

        # Add sample files compilation if requested
        if include_samples:
            compile_functions.append((self.compile_sample_files, [], {}))

        results = self.compile_all_with_results(
            compile_functions,
            f"Flat File Ingestion Package Compiler ({target_datastore.title()})",
        )

        # Transform results for backward compatibility
        if results["success"]:
            notebook_file = results["compiled_items"].get("compile_notebook")
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])
            sample_files = results["compiled_items"].get("compile_sample_files", [])

            success_message = (
                f"✓ Successfully compiled flat file ingestion package for {target_datastore}\n"
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
                "errors": [],
            }
        else:
            return {
                "notebook_file": None,
                "ddl_files": [],
                "sample_files": [],
                "success": False,
                "errors": results["errors"],
            }


def compile_flat_file_ingestion_package(
    fabric_workspace_repo_dir: str = None,
    template_vars: Dict[str, Any] = None,
    include_samples: bool = False,
    target_datastore: str = "lakehouse",
    add_debug_cells: bool = False,
) -> Dict[str, Any]:
    """Main function to compile the flat file ingestion package"""

    compiler = FlatFileIngestionCompiler(fabric_workspace_repo_dir)
    # Pass template vars with add_debug_cells flag
    if template_vars is None:
        template_vars = {}
    template_vars["add_debug_cells"] = add_debug_cells
    return compiler.compile_all(
        template_vars,
        include_samples=include_samples,
        target_datastore=target_datastore,
    )
