"""
Export Package

This module provides functionality to compile and generate Export
notebooks and DDL scripts based on templates.
"""

import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler


class ExportCompiler(BaseNotebookCompiler):
    """Compiler for Export framework templates"""

    def __init__(self, fabric_workspace_repo_dir: str = None):
        self.package_dir = Path(__file__).parent
        self.templates_dir = self.package_dir / "templates"
        self.ddl_scripts_dir = self.package_dir / "ddl_scripts"

        # Set up template directories - include package templates and unified templates
        # Use package location instead of cwd() so it works when installed as a package
        ingen_fab_dir = Path(__file__).parent.parent.parent
        unified_templates_dir = ingen_fab_dir / "templates"
        template_search_paths = [self.templates_dir, unified_templates_dir]

        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="export",
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

    def compile_notebook(self, template_vars: Dict[str, Any] = None) -> Path:
        """Compile Export notebook template."""

        return self.compile_notebook_from_template(
            template_name="export.py.jinja",
            output_notebook_name="nb_export_extracts",
            template_vars=template_vars,
            display_name="nb_export_extracts",
            description="Orchestrates data exports from tables to files in Lakehouse Files.",
            output_subdir="notebooks",
        )

    def compile_all_notebooks(self, template_vars: Dict[str, Any] = None) -> Dict[str, Path]:
        """Compile all Export notebook templates"""
        notebooks = {}

        try:
            notebook_path = self.compile_notebook(template_vars)
            notebooks["nb_export_extracts"] = notebook_path

            if self.console:
                self.console.print(f"[green]✓ Compiled nb_export_extracts notebook:[/green] {notebook_path}")
        except Exception as e:
            if self.console:
                self.console.print(f"[red]✗ Failed to compile nb_export_extracts notebook:[/red] {e}")
            notebooks["nb_export_extracts"] = None

        return notebooks

    def compile_ddl_scripts(self, include_sample_data: bool = False) -> List[Path]:
        """Compile DDL scripts to the target directory"""

        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"

        # Define script mappings for Lakehouse only
        script_mappings = {
            "Lakehouses/Config/002_Initial_Creation_Export": [
                (
                    "lakehouse/config_export_resource_create.py.jinja",
                    "001_config_export_resource_create.py",
                ),
                (
                    "lakehouse/log_resource_export_create.py.jinja",
                    "002_log_resource_export_create.py",
                ),
                (
                    "lakehouse/log_resource_export_watermark_create.py.jinja",
                    "003_log_resource_export_watermark_create.py",
                ),
            ],
        }

        # Process DDL scripts with template rendering
        results = self._compile_ddl_scripts_with_templates(
            self.ddl_scripts_dir, ddl_output_base, script_mappings
        )

        # Flatten results into a single list
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
                            # Use package location instead of cwd() so it works when installed
                            template_paths = [ddl_source_dir]
                            ingen_fab_dir = Path(__file__).parent.parent.parent
                            unified_templates_dir = ingen_fab_dir / "templates"
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
        self, template_vars: Dict[str, Any] = None, include_samples: bool = False
    ) -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""

        compile_functions = [
            (self.compile_all_notebooks, [template_vars], {}),
            (self.compile_ddl_scripts, [], {"include_sample_data": include_samples}),
        ]

        results = self.compile_all_with_results(
            compile_functions, "Export Package Compiler"
        )

        if results["success"]:
            notebook_files = results["compiled_items"].get("compile_all_notebooks", {})
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])

            # Count successful notebook compilations
            successful_notebooks = sum(1 for path in notebook_files.values() if path is not None)

            success_message = (
                f"✓ Successfully compiled Export package\n"
                f"Notebooks: {successful_notebooks} compiled ({', '.join(notebook_files.keys())})\n"
                f"DDL Scripts: {len(ddl_files)} files"
            )

            self.print_success_panel("Compilation Complete", success_message)

            return {
                "notebook_files": notebook_files,
                "ddl_files": ddl_files,
                "success": True,
                "errors": [],
            }
        else:
            return {
                "notebook_files": {},
                "ddl_files": [],
                "success": False,
                "errors": results["errors"],
            }


def compile_export_package(
    fabric_workspace_repo_dir: str = None,
    template_vars: Optional[Dict[str, Any]] = None,
    include_samples: bool = False,
) -> Dict[str, Any]:
    """Main function to compile the Export package

    Args:
        fabric_workspace_repo_dir: Target directory for compilation
        template_vars: Variables to inject into templates
        include_samples: Whether to include sample data scripts

    Returns:
        Dict containing compilation results
    """

    compiler = ExportCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars, include_samples=include_samples)
