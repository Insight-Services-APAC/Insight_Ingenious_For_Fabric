"""
Data Profiling Package

This module provides functionality to compile and generate data profiling
notebooks and DDL scripts based on templates.
"""

from pathlib import Path
from typing import Any, Dict, List

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler
from ingen_fab.python_libs.common.utils.path_utils import PathUtils


class DataProfilingCompiler(BaseNotebookCompiler):
    """Compiler for data profiling templates"""

    def __init__(self, fabric_workspace_repo_dir: str = None):
        try:
            # Use path utilities for package resource discovery
            package_base = PathUtils.get_package_resource_path(
                "packages/data_profiling"
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
        try:
            unified_templates_dir = PathUtils.get_package_resource_path("templates")
        except FileNotFoundError:
            # Fallback for compatibility
            unified_templates_dir = Path(__file__).parent.parent.parent / "templates"

        template_search_paths = [self.templates_dir, unified_templates_dir]

        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="data_profiling",
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
        """Compile the data profiling notebook template"""

        # Select template based on target datastore
        template_mapping = {
            "lakehouse": "data_profiling_lakehouse.py.jinja",
            "warehouse": "data_profiling_warehouse.py.jinja",
        }

        template_name = template_mapping.get(target_datastore)
        if not template_name:
            raise ValueError(
                f"Unsupported target datastore: {target_datastore}. Must be 'lakehouse' or 'warehouse'"
            )

        # Default template variables
        default_vars = {
            "profile_type": "full",
            "save_to_catalog": True,
            "generate_report": True,
            "output_format": "yaml",
            "sample_size": None,  # Use full dataset by default
            # Performance tuning defaults
            "auto_sample_large_tables": True,
            "max_correlation_columns": 20,
            "top_values_limit": 100,
            "enable_performance_mode": False,
            "enable_ultra_fast_mode": True,  # Enable by default for better performance
        }

        # Merge with provided template vars
        if template_vars:
            default_vars.update(template_vars)

        # Customize output name and description based on target
        output_name = f"data_profiling_processor_{target_datastore}"
        display_name = f"Data Profiling Processor ({target_datastore.title()})"
        description = f"Profiles datasets in {target_datastore} and generates quality reports"

        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=output_name,
            template_vars=default_vars,
            display_name=display_name,
            description=description,
            output_subdir="data_profiling",
        )

    def compile_config_notebook(
        self, template_vars: Dict[str, Any] = None, target_datastore: str = "lakehouse"
    ) -> Path:
        """Compile the profiling configuration notebook template"""

        # Select template based on target datastore
        template_mapping = {
            "lakehouse": "data_profiling_config_lakehouse.py.jinja",
            "warehouse": "data_profiling_config_warehouse.py.jinja",
        }

        template_name = template_mapping.get(target_datastore)
        if not template_name:
            raise ValueError(
                f"Unsupported target datastore: {target_datastore}. Must be 'lakehouse' or 'warehouse'"
            )

        # Default template variables for config generation
        default_vars = {
            "auto_discover_tables": True,
            "profile_frequency": "daily",
            "quality_thresholds": {
                "completeness": 0.95,
                "uniqueness": 0.99,
                "validity": 0.98,
            },
        }

        # Merge with provided template vars
        if template_vars:
            default_vars.update(template_vars)

        # Customize output name and description based on target
        output_name = f"data_profiling_config_{target_datastore}"
        display_name = f"Data Profiling Configuration ({target_datastore.title()})"
        description = f"Configures automated data profiling for {target_datastore} tables"

        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name=output_name,
            template_vars=default_vars,
            display_name=display_name,
            description=description,
            output_subdir="data_profiling",
        )

    def compile_ddl_scripts(
        self, include_sample_data: bool = False, target_datastore: str = "both"
    ) -> List[Path]:
        """Compile DDL scripts to the target directory"""

        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"

        # Define script mappings for different targets
        all_script_mappings = {
            "Lakehouses/Config/001_Initial_Creation_Profiling": [
                (
                    "lakehouse/config_profiling_create.py.jinja",
                    "001_config_data_profiling_create.py",
                ),
                (
                    "lakehouse/profile_results_create.py.jinja",
                    "002_profile_results_create.py",
                ),
                (
                    "lakehouse/profile_history_create.py.jinja",
                    "003_profile_history_create.py",
                ),
                (
                    "lakehouse/data_quality_rules_create.py.jinja",
                    "004_data_quality_rules_create.py",
                ),
            ],
            "Warehouses/Config/001_Initial_Creation_Profiling": [
                (
                    "warehouse/config_profiling_create.sql.jinja",
                    "001_config_data_profiling_create.sql",
                ),
                (
                    "warehouse/profile_results_create.sql.jinja",
                    "002_profile_results_create.sql",
                ),
                (
                    "warehouse/profile_history_create.sql.jinja",
                    "003_profile_history_create.sql",
                ),
                (
                    "warehouse/data_quality_rules_create.sql.jinja",
                    "004_data_quality_rules_create.sql",
                ),
            ],
        }

        # Add sample data scripts if requested
        if include_sample_data:
            all_script_mappings["Lakehouses/Config/002_Sample_Data_Profiling"] = [
                (
                    "lakehouse/sample_profiling_config.py.jinja",
                    "005_sample_profiling_config.py",
                ),
                (
                    "lakehouse/sample_quality_rules.py.jinja",
                    "006_sample_quality_rules.py",
                ),
            ]
            all_script_mappings["Warehouses/Config/002_Sample_Data_Profiling"] = [
                (
                    "warehouse/sample_profiling_config.sql.jinja",
                    "005_sample_profiling_config.sql",
                ),
                (
                    "warehouse/sample_quality_rules.sql.jinja",
                    "006_sample_quality_rules.sql",
                ),
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

                            # Create a template environment
                            template_paths = [ddl_source_dir]
                            try:
                                unified_templates_dir = (
                                    PathUtils.get_package_resource_path("templates")
                                )
                            except FileNotFoundError:
                                unified_templates_dir = (
                                    Path(__file__).parent.parent.parent / "templates"
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

    def compile_all(
        self,
        template_vars: Dict[str, Any] = None,
        include_samples: bool = False,
        target_datastore: str = "lakehouse",
        include_config: bool = True,
    ) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""

        # Handle "both" option by compiling both variants
        if target_datastore == "both":
            lakehouse_results = self.compile_all(
                template_vars, include_samples, "lakehouse", include_config
            )
            warehouse_results = self.compile_all(
                template_vars, include_samples, "warehouse", include_config
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
                f"✓ Successfully compiled data profiling package for both lakehouse and warehouse\n"
                f"Lakehouse Notebook: {lakehouse_results['notebook_file']}\n"
                f"Warehouse Notebook: {warehouse_results['notebook_file']}\n"
            )

            if lakehouse_results.get("config_file"):
                success_message += (
                    f"Lakehouse Config: {lakehouse_results['config_file']}\n"
                )
            if warehouse_results.get("config_file"):
                success_message += (
                    f"Warehouse Config: {warehouse_results['config_file']}\n"
                )

            success_message += f"DDL Scripts: {len(combined_ddl_files)} files"

            if combined_success:
                self.print_success_panel("Compilation Complete", success_message)

            return {
                "notebook_file": [
                    lakehouse_results["notebook_file"],
                    warehouse_results["notebook_file"],
                ],
                "config_file": [
                    lakehouse_results.get("config_file"),
                    warehouse_results.get("config_file"),
                ],
                "ddl_files": combined_ddl_files,
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

        # Add config notebook if requested
        if include_config:
            compile_functions.append(
                (
                    self.compile_config_notebook,
                    [template_vars],
                    {"target_datastore": target_datastore},
                )
            )

        results = self.compile_all_with_results(
            compile_functions,
            f"Data Profiling Package Compiler ({target_datastore.title()})",
        )

        # Transform results for backward compatibility
        if results["success"]:
            notebook_file = results["compiled_items"].get("compile_notebook")
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])
            config_file = results["compiled_items"].get("compile_config_notebook")

            success_message = (
                f"✓ Successfully compiled data profiling package for {target_datastore}\n"
                f"Notebook: {notebook_file}\n"
            )

            if config_file:
                success_message += f"Config: {config_file}\n"

            success_message += f"DDL Scripts: {len(ddl_files)} files"

            self.print_success_panel("Compilation Complete", success_message)

            # Return in expected format
            return {
                "notebook_file": notebook_file,
                "config_file": config_file,
                "ddl_files": ddl_files,
                "success": True,
                "errors": [],
            }
        else:
            return {
                "notebook_file": None,
                "config_file": None,
                "ddl_files": [],
                "success": False,
                "errors": results["errors"],
            }


def compile_data_profiling_package(
    fabric_workspace_repo_dir: str = None,
    template_vars: Dict[str, Any] = None,
    include_samples: bool = False,
    target_datastore: str = "lakehouse",
    add_debug_cells: bool = False,
) -> Dict[str, Any]:
    """Main function to compile the data profiling package"""

    compiler = DataProfilingCompiler(fabric_workspace_repo_dir)
    # Pass template vars with add_debug_cells flag
    if template_vars is None:
        template_vars = {}
    template_vars["add_debug_cells"] = add_debug_cells
    return compiler.compile_all(
        template_vars,
        include_samples=include_samples,
        target_datastore=target_datastore,
    )