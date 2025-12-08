"""
Ingestion Package

This module provides functionality to compile and generate Ingestion
notebooks, DDL scripts, and pipelines based on templates.
"""

import json
import uuid
from pathlib import Path
from typing import Any, Dict, List

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler


class IngestionCompiler(BaseNotebookCompiler):
    """Compiler for Ingestion framework templates"""

    def __init__(self, fabric_workspace_repo_dir: str = None):
        self.package_dir = Path(__file__).parent
        self.templates_dir = self.package_dir / "templates"
        self.ddl_scripts_dir = self.package_dir / "ddl_scripts"
        self.pipelines_dir = self.package_dir / "pipelines"

        # Set up template directories - include package templates and unified templates
        # Use package location instead of cwd() so it works when installed as a package
        ingen_fab_dir = Path(__file__).parent.parent.parent
        unified_templates_dir = ingen_fab_dir / "templates"
        template_search_paths = [self.templates_dir, unified_templates_dir]

        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="ingestion",
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
            self.console.print(
                f"[bold blue]Pipelines Directory:[/bold blue] {self.pipelines_dir}"
            )

    def compile_notebook(self, template_vars: Dict[str, Any] = None, notebook_type: str = "extract") -> Path:
        """Compile Ingestion notebook templates (extract, load)."""

        # Define template mappings for supported notebook types
        notebook_templates = {
            "extract": {
                "template_name": "ingestion_extract.py.jinja",
                "output_name": "ingestion_extract",
                "display_name": "ingestion_extract",
                "description": "Orchestrates data extraction from external sources to raw layer."
            },
            "load": {
                "template_name": "ingestion_load.py.jinja",
                "output_name": "ingestion_load",
                "display_name": "ingestion_load",
                "description": "Orchestrates data loading from raw layer to Delta tables."
            }
        }

        # Get template configuration
        template_config = notebook_templates.get(notebook_type, notebook_templates["extract"])

        return self.compile_notebook_from_template(
            template_name=template_config["template_name"],
            output_notebook_name=template_config["output_name"],
            template_vars=template_vars,
            display_name=template_config["display_name"],
            description=template_config["description"],
            output_subdir="ingestion",
        )

    def compile_all_notebooks(self, template_vars: Dict[str, Any] = None) -> Dict[str, Path]:
        """Compile all Ingestion notebook templates"""
        notebooks = {}

        for notebook_type in ["extract", "load"]:
            try:
                notebook_path = self.compile_notebook(template_vars, notebook_type)
                notebooks[notebook_type] = notebook_path

                if self.console:
                    self.console.print(f"[green]✓ Compiled {notebook_type} notebook:[/green] {notebook_path}")
            except Exception as e:
                if self.console:
                    self.console.print(f"[red]✗ Failed to compile {notebook_type} notebook:[/red] {e}")
                notebooks[notebook_type] = None

        return notebooks

    def compile_ddl_scripts(self, include_sample_data: bool = False) -> List[Path]:
        """Compile DDL scripts to the target directory"""

        ddl_output_base = self.fabric_workspace_repo_dir / "ddl_scripts"

        # Define script mappings for Lakehouse only
        script_mappings = {
            "Lakehouses/Config/001_Initial_Creation_Ingestion": [
                (
                    "lakehouse/config_ingestion_resource_create.py.jinja",
                    "001_config_ingestion_resource_create.py",
                ),
                (
                    "lakehouse/log_resource_extract_create.py.jinja",
                    "002_log_resource_extract_create.py",
                ),
                (
                    "lakehouse/log_resource_extract_batch_create.py.jinja",
                    "003_log_resource_extract_batch_create.py",
                ),
                (
                    "lakehouse/log_resource_extract_watermark_create.py.jinja",
                    "004_log_resource_extract_watermark_create.py",
                ),
                (
                    "lakehouse/log_resource_load_create.py.jinja",
                    "005_log_resource_load_create.py",
                ),
                (
                    "lakehouse/log_resource_load_batch_create.py.jinja",
                    "006_log_resource_load_batch_create.py",
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

    def create_pipeline_platform_metadata(
        self,
        pipeline_name: str,
        display_name: str = None,
        description: str = None,
        logical_id: str = None,
    ) -> Dict[str, Any]:
        """
        Create platform metadata dictionary for a pipeline.

        Args:
            pipeline_name: The pipeline name
            display_name: Display name (defaults to pipeline_name)
            description: Optional description
            logical_id: Logical ID (generates UUID if not provided)

        Returns:
            Platform metadata dictionary
        """
        return {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "DataPipeline",
                "displayName": display_name or pipeline_name,
                **({"description": description} if description else {}),
            },
            "config": {"version": "2.0", "logicalId": logical_id or str(uuid.uuid4())},
        }

    def create_pipeline_with_platform(
        self,
        pipeline_name: str,
        pipeline_content: Dict[str, Any],
        output_dir: Path = None,
        display_name: str = None,
        description: str = None,
    ) -> Path:
        """
        Create a pipeline with its .platform file.

        Args:
            pipeline_name: The name of the pipeline (without extension)
            pipeline_content: The pipeline content as a dictionary
            output_dir: The directory where the pipeline should be created
            display_name: Display name for the pipeline
            description: Optional description for the pipeline

        Returns:
            Path to the created pipeline directory
        """
        if output_dir is None:
            output_dir = self.output_dir / "pipelines"

        # Create output path
        output_path = output_dir / f"{pipeline_name}.DataPipeline"
        output_path.mkdir(parents=True, exist_ok=True)

        # Write pipeline content
        pipeline_file = output_path / "pipeline-content.json"
        with pipeline_file.open("w", encoding="utf-8") as f:
            json.dump(pipeline_content, f, indent=2)

        # Create .platform file
        platform_path = output_path / ".platform"
        if not platform_path.exists():
            platform_metadata = json.dumps(
                self.create_pipeline_platform_metadata(
                    pipeline_name, display_name, description
                ),
                indent=2,
            )
            with platform_path.open("w", encoding="utf-8") as f:
                f.write(platform_metadata)

        if self.console:
            self.console.print(f"[green]✓ Pipeline created: {pipeline_file}[/green]")

        return output_path

    def compile_pipelines(self) -> List[Path]:
        """Compile pipelines to the target directory"""

        pipelines_output = self.fabric_workspace_repo_dir / "fabric_workspace_items" / "pipelines"
        compiled_pipelines = []

        # Define pipelines to compile
        pipeline_configs = [
            {
                "source_file": "pl_extract_sql_server.json",
                "pipeline_name": "pl_extract_sql_server",
                "display_name": "pl_extract_sql_server",
                "description": "Extracts data from SQL Server to raw lakehouse as Parquet files.",
            },
            {
                "source_file": "pl_extract_synapse_cetas.json",
                "pipeline_name": "pl_extract_synapse_cetas",
                "display_name": "pl_extract_synapse_cetas",
                "description": "Extracts data from Synapse using CETAS writes directly to ADLS.",
            },
        ]

        for config in pipeline_configs:
            source_path = self.pipelines_dir / config["source_file"]

            if source_path.exists():
                try:
                    with source_path.open("r", encoding="utf-8") as f:
                        pipeline_content = json.load(f)

                    pipeline_path = self.create_pipeline_with_platform(
                        pipeline_name=config["pipeline_name"],
                        pipeline_content=pipeline_content,
                        output_dir=pipelines_output,
                        display_name=config["display_name"],
                        description=config["description"],
                    )
                    compiled_pipelines.append(pipeline_path)

                except Exception as e:
                    if self.console:
                        self.console.print(
                            f"[red]✗ Failed to compile pipeline {config['pipeline_name']}:[/red] {e}"
                        )
            else:
                if self.console:
                    self.console.print(
                        f"[red]✗ Pipeline source not found:[/red] {source_path}"
                    )

        return compiled_pipelines

    def compile_all(
        self, template_vars: Dict[str, Any] = None, include_samples: bool = False
    ) -> Dict[str, Any]:
        """Compile all templates, DDL scripts, and pipelines"""

        compile_functions = [
            (self.compile_all_notebooks, [template_vars], {}),
            (self.compile_ddl_scripts, [], {"include_sample_data": include_samples}),
            (self.compile_pipelines, [], {}),
        ]

        results = self.compile_all_with_results(
            compile_functions, "Ingestion Package Compiler"
        )

        if results["success"]:
            notebook_files = results["compiled_items"].get("compile_all_notebooks", {})
            ddl_files = results["compiled_items"].get("compile_ddl_scripts", [])
            pipeline_files = results["compiled_items"].get("compile_pipelines", [])

            # Count successful notebook compilations
            successful_notebooks = sum(1 for path in notebook_files.values() if path is not None)

            success_message = (
                f"✓ Successfully compiled Ingestion package\n"
                f"Notebooks: {successful_notebooks} compiled ({', '.join(notebook_files.keys())})\n"
                f"DDL Scripts: {len(ddl_files)} files\n"
                f"Pipelines: {len(pipeline_files)} compiled"
            )

            self.print_success_panel("Compilation Complete", success_message)

            return {
                "notebook_files": notebook_files,
                "ddl_files": ddl_files,
                "pipeline_files": pipeline_files,
                "success": True,
                "errors": [],
            }
        else:
            return {
                "notebook_files": {},
                "ddl_files": [],
                "pipeline_files": [],
                "success": False,
                "errors": results["errors"],
            }


def compile_ingestion_package(
    fabric_workspace_repo_dir: str = None,
    template_vars: Dict[str, Any] = None,
    include_samples: bool = False,
) -> Dict[str, Any]:
    """Main function to compile the Ingestion package

    Args:
        fabric_workspace_repo_dir: Target directory for compilation
        template_vars: Variables to inject into templates
        include_samples: Whether to include sample data scripts

    Returns:
        Dict containing compilation results
    """

    compiler = IngestionCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars, include_samples=include_samples)
