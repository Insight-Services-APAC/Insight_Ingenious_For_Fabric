import hashlib
import time
import uuid
from enum import Enum
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, exceptions
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from ingen_fab.python_libs.gather_python_libs import GatherPythonLibs


def required_filter(value, var_name=""):
    """Jinja2 filter: raises an error if value is not provided or is falsy."""
    if value is None or (hasattr(value, '__len__') and len(value) == 0):
        raise exceptions.TemplateRuntimeError(
            f"Required parameter '{var_name or 'unknown'}' was not provided!"
        )
    return value

class NotebookGenerator:
    """Class to generate notebooks and orchestrators for Lakehouses or Warehouses."""

    class GenerationMode(str, Enum):
        warehouse = "Warehouse"
        lakehouse = "Lakehouse"

    class OutputMode(str, Enum):
        fabric_workspace_repo = "fabric_workspace_repo"
        local = "local"

    def __init__(
        self,
        generation_mode: GenerationMode = GenerationMode.lakehouse,
        output_mode: OutputMode = OutputMode.fabric_workspace_repo,
        templates_dir: str | None = None,
        fabric_workspace_repo_dir: str | None = None,
    ):
        self.generation_mode = generation_mode
        self.language_group = "synapse_pyspark"  # Default language group
        self.output_mode = output_mode
        self.console = Console()
        self.base_dir = Path.cwd()

        if self.generation_mode == NotebookGenerator.GenerationMode.warehouse:            
            self.language_group = "jupyter_python"
        else: 
            self.language_group = "synapse_pyspark"

        if templates_dir is None:
            templates_dir = (
                Path(__file__).resolve().parent
                / "_templates"
                / generation_mode.value.lower()
            )
        if fabric_workspace_repo_dir is None:
            fabric_workspace_repo_dir = (
                Path(__file__).resolve().parents[2] / "sample_project"
            )

        self.fabric_workspace_repo_dir = Path(fabric_workspace_repo_dir).resolve()
        self.templates_dir = Path(templates_dir).resolve()

        # Determine folder names based on generation mode
        self.entity_type = self.generation_mode.value
        self.entities_folder = f"{self.entity_type}s"

        self.entities_dir = (
            self.fabric_workspace_repo_dir / "ddl_scripts" / self.entities_folder
        )

        if self.output_mode == NotebookGenerator.OutputMode.fabric_workspace_repo:
            self.output_dir = (
                self.fabric_workspace_repo_dir
                / "fabric_workspace_items"
                / "ddl_scripts"
                / self.entities_folder
            ).resolve()
        elif self.output_mode == NotebookGenerator.OutputMode.local:
            self.output_dir = (
                self.base_dir / "output" / "ddl_scripts" / self.entities_folder
            )
        else:
            raise ValueError(
                "Invalid output mode. Choose 'fabric_workspace_repo' or 'local'."
            )

        # Inject python libs into lib.py.jinja
        #self.inject_python_libs_into_template()

        # Initialize Rich console
        self.console = Console()

        # Paths (platform agnostic)
        self.base_dir = Path(__file__).resolve().parent
        self.console.print(
            f"[bold blue]Fabric Workspace Dir:[/bold blue] {self.fabric_workspace_repo_dir}"
        )
        self.console.print(
            f"[bold blue]Templates Directory:[/bold blue] {self.templates_dir}"
        )
        self.console.print(
            f"[bold blue]Lakehouses or Warehouses Directory:[/bold blue] {self.entities_dir}"
        )
        self.console.print(
            f"[bold blue]Output Directory:[/bold blue] {self.output_dir}"
        )

        # Jinja2 Environment
        self.env = Environment(loader=FileSystemLoader(str(self.templates_dir.parent)))
        self.env.filters["required"] = lambda value, var_name="": required_filter(value, var_name)

    def load_template(self, template_name):
        """Load a Jinja template."""
        return self.env.get_template(template_name)

    def generate_guid(self, relative_path):
        """Generate a GUID by hashing the relative path."""
        return hashlib.sha256(str(relative_path).encode("utf-8")).hexdigest()[:12]

    def get_sorted_directories(self, parent_dir):
        """
        Get sorted directories based on numeric prefix convention (e.g., '1_name', '2_name').
        Alerts if any directory names don't match the expected convention.
        """
        directories = []
        errors = []

        for path in parent_dir.iterdir():
            if path.is_dir():
                try:
                    # Check if name matches pattern: number-name
                    parts = path.name.split("_", 1)
                    if len(parts) < 2:
                        errors.append(
                            f"Directory '{path.name}' doesn't match expected pattern 'number_name'"
                        )
                        continue

                    # Try to parse the numeric prefix
                    numeric_prefix = int(parts[0])
                    directories.append((numeric_prefix, path))
                except ValueError:
                    errors.append(
                        f"Directory '{path.name}' doesn't have a valid numeric prefix"
                    )
                except Exception as e:
                    errors.append(
                        f"Error processing directory '{path.name}': {str(e)}"
                    )  # Alert about any errors
        if errors:
            self.console.print(
                "\n[bold yellow]⚠️  WARNING: The following directories don't "
                "match the expected naming convention:[/bold yellow]"
            )
            for error in errors:
                self.console.print(f"  [red]✗[/red] {error}")
            self.console.print()

        # Sort by numeric prefix and return just the paths
        sorted_dirs = sorted(directories, key=lambda x: x[0])
        return [path for _, path in sorted_dirs]

    def get_sorted_files(self, parent_dir, extensions=None):
        """
        Get sorted files based on numeric prefix convention (e.g., '1_name.py', '2_name.sql').
        Alerts if any file names don't match the expected convention.

        Args:
            parent_dir: The directory to scan for files
            extensions: List of file extensions to include (e.g., ['.py', '.sql']). If None, includes all files.
        """
        files = []
        errors = []

        for path in parent_dir.iterdir():
            if path.is_file():
                # Check if we should filter by extension
                if extensions and path.suffix not in extensions:
                    continue

                try:
                    # Check if name matches pattern: number-name.extension
                    name_without_ext = path.stem
                    parts = name_without_ext.split("_", 1)
                    if len(parts) < 2:
                        errors.append(
                            f"File '{path.name}' doesn't match expected pattern 'number_name.extension'"
                        )
                        continue

                    # Try to parse the numeric prefix
                    numeric_prefix = int(parts[0])
                    files.append((numeric_prefix, path))
                except ValueError:
                    errors.append(
                        f"File '{path.name}' doesn't have a valid numeric prefix"
                    )
                except Exception as e:
                    errors.append(
                        f"Error processing file '{path.name}': {str(e)}"
                    )  # Alert about any errors
        if errors:
            self.console.print(
                "\n[bold yellow]⚠️  WARNING: The following files don't "
                "match the expected naming convention:[/bold yellow]"
            )
            for error in errors:
                self.console.print(f"  [red]✗[/red] {error}")
            self.console.print()

        # Sort by numeric prefix and return just the paths
        sorted_files = sorted(files, key=lambda x: x[0])
        return [path for _, path in sorted_files]

    def generate_config_notebook(self, output_dir):
        """Generate a configuration notebook for the current entity."""
        # Load the configuration template
        config_template = self.load_template(
            "config.py.jinja"
        )  # Render the configuration notebook
        rendered_config = config_template.render()

        notebook_name = f"00_all_{self.entities_folder.lower()}_config_notebook"
        return self.create_notebook_with_platform(
            notebook_name=notebook_name,
            rendered_content=rendered_config,
            output_dir=output_dir,
        )

    def generate_notebook(
        self,
        config_folder,
        output_folder,
        notebook_display_name,
        target_lakehouse_config_prefix,
        progress=None,
        parent_task=None,
    ):
        """Generate a notebook and its .platform file for a specific entity configuration."""
        notebook_template = self.load_template(f"{self.generation_mode.lower()}/notebook_content.py.jinja")
        cells = []

        config_folder = Path(config_folder)
        output_folder = Path(output_folder)

        # Get sorted files with .py and .sql extensions
        sorted_files = self.get_sorted_files(config_folder, extensions=[".py", ".sql"])

        # Create subtask for processing cells if progress tracking is enabled
        if progress and parent_task is not None:
            cell_task = progress.add_task(
                "[dim]Processing cells...[/dim]",
                total=len(sorted_files),
                parent=parent_task,
            )

        # Iterate through sorted files
        for file_path in sorted_files:
            if progress and parent_task is not None:
                progress.update(
                    cell_task, description=f"[dim]Processing {file_path.name}[/dim]"
                )

            # Determine the template to use based on file extension
            if file_path.suffix == ".py":
                cell_template = self.load_template("./common/ddl_script_execution_cells/pyspark.py.jinja")
            elif file_path.suffix == ".sql":
                cell_template = self.load_template("./common/ddl_script_execution_cells/spark_sql.py.jinja")
            else:
                continue  # Skip unsupported file types

            # Read file content
            with file_path.open("r", encoding="utf-8") as f:
                content = f.read()

            # Generate GUID based on relative path
            relative_path = file_path.relative_to(self.entities_dir)
            script_guid = self.generate_guid(str(relative_path))

            # Render cell template
            rendered_cell = cell_template.render(
                heading_level="#",
                heading_name=f"Cell for {file_path.name}",
                content=content,
                script_guid=script_guid,
                script_name=file_path.stem,
                target_workspace_id="example-workspace-id",
                target_lakehouse_id=(
                    "example-lakehouse-id"
                    if self.generation_mode
                    == NotebookGenerator.GenerationMode.lakehouse
                    else None
                ),
                target_warehouse_id=(
                    "example-warehouse-id"
                    if self.generation_mode
                    == NotebookGenerator.GenerationMode.warehouse
                    else None              
                ),            
                language_group=self.language_group  
            )

            # Add cell to the list
            cells.append(rendered_cell)

            if progress and parent_task is not None:
                progress.advance(cell_task)

            #time.sleep(0.5)  # Simulate processing time for each cell

        # Render the notebook template with the cells
        rendered_notebook = notebook_template.render(cells=cells, target_lakehouse_config_prefix=target_lakehouse_config_prefix, language_group=self.language_group)

        # Create notebook with platform file
        relative_path = config_folder.relative_to(self.entities_dir)
        self.create_notebook_with_platform(
            notebook_name=notebook_display_name,
            rendered_content=rendered_notebook,
            output_dir=output_folder.parent,
        )

        # Mark cell task as complete
        if progress and parent_task is not None:
            progress.update(
                cell_task,
                description=f"[dim green]✓ Processed {len(cells)} cells[/dim green]",
            )

    def generate_orchestrator_notebook(
        self, entity_name, notebook_names, output_folder, target_lakehouse_config_prefix
    ):
        """Generate an orchestrator notebook that runs all notebooks for an entity in sequence."""

        # Load the orchestrator template
        orchestrator_template = self.load_template(f"{self.generation_mode.lower()}/orchestrator_notebook.py.jinja")

        # Prepare notebook execution data
        notebooks = []
        for i, notebook_name in enumerate(notebook_names, 1):
            notebooks.append(
                {"index": i, "name": notebook_name, "total": len(notebook_names)}
            )  # Render the orchestrator notebook
        orchestrator_content = orchestrator_template.render(
            lakehouse_name=entity_name,
            notebooks=notebooks,
            total_notebooks=len(notebook_names),
            target_lakehouse_config_prefix=target_lakehouse_config_prefix,
            language_group=self.language_group
        )

        # Create notebook with platform file
        notebook_name = f"00_orchestrator_{entity_name}_{self.generation_mode.lower()}"
        return self.create_notebook_with_platform(
            notebook_name=notebook_name,
            rendered_content=orchestrator_content,
            output_dir=output_folder,
        )

    def generate_all_entities_orchestrator(self, entity_names, output_dir):
        """Generate a master orchestrator notebook that runs all entity orchestrators in parallel."""

        # Load the template
        all_lakehouses_template = self.load_template(
            f"{self.generation_mode.lower()}/orchestrator_notebook_all_lakehouses.py.jinja"
        )

        # Prepare lakehouse data
        lakehouses = []
        for name in entity_names:
            lakehouses.append(
                {"name": name, "orchestrator_name": f"0_orchestrator_{name}_{self.generation_mode.lower()}_ddl_scripts"}
            )  # Render the all lakehouses orchestrator notebook
        orchestrator_content = all_lakehouses_template.render(
            lakehouses=lakehouses, total_lakehouses=len(lakehouses), language_group=self.language_group
        )

        # Create notebook with platform file
        notebook_name = f"00_all_{self.entities_folder.lower()}_orchestrator"
        return self.create_notebook_with_platform(
            notebook_name=notebook_name,
            rendered_content=orchestrator_content,
            output_dir=output_dir,
        )

    def inject_python_libs_into_template(self):
        """
        Analyze python_libs files, sort by dependencies, and inject into lib.py.jinja.
        """

        # Gather Python libraries and their dependencies
        if self.generation_mode == NotebookGenerator.GenerationMode.lakehouse:
            lib_path = "pyspark"
            libs_to_include = ["lakehouse_utils", "ddl_utils", "config_utils"]
        else:
            lib_path = "python"
            libs_to_include = [
                "sql_templates",
                "warehouse_utils",
                "lakehouse_utils",
                "ddl_utils",
                "config_utils",
            ]

        gpl = GatherPythonLibs(
            console=self.console
        )  # Replace with actual console instance
        combined_content = gpl.gather_files(
            lib_path, libs_to_include
        )  # Call the method to gather and process files
        # Write to lib.py.jinja template
        lib_template_path = self.templates_dir / "lib.py.jinja"

        try:
            with lib_template_path.open("w", encoding="utf-8") as f:
                f.write("\n".join(combined_content))

            self.console.print(
                f"\n[green]✓ Successfully injected libs into {lib_template_path}[/green]"
            )

        except Exception as e:
            self.console.print(f"[red]Error writing to {lib_template_path}: {e}[/red]")

    def run_all(self):
        """Main function to generate notebooks and .platform files."""
        # Print header
        self.console.print(
            Panel.fit(
                "[bold cyan]DDL Notebook Generator[/bold cyan]", border_style="cyan"
            )
        )
        self.console.print()

        # Get sorted entity directories
        entity_dirs = [path for path in self.entities_dir.iterdir() if path.is_dir()]

        # Count total tasks and cells
        total_tasks = 0
        total_cells = 0
        entity_configs = {}  # Store config info for orchestrator generation
        entity_names = []  # Track entity names for master orchestrator

        for entity_path in entity_dirs:
            config_dirs = self.get_sorted_directories(entity_path)
            total_tasks += len(config_dirs)
            entity_configs[entity_path] = config_dirs
            entity_names.append(entity_path.name)

            for config_path in config_dirs:
                cell_files = self.get_sorted_files(
                    config_path, extensions=[".py", ".sql"]
                )
                total_cells += len(cell_files)

        # Show summary (updated to include all_lakehouses_orchestrator)
        summary_table = Table(title="[bold]Generation Summary[/bold]")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        summary_table.add_row(f"{self.entities_folder} Found", str(len(entity_dirs)))
        summary_table.add_row("Total Notebooks to Generate", str(total_tasks))
        summary_table.add_row(
            f"{self.entity_type} Orchestrators to Generate", str(len(entity_dirs))
        )
        summary_table.add_row("Master Orchestrator", "1")
        summary_table.add_row("Total Cells to Process", str(total_cells))
        summary_table.add_row("Output Directory", str(self.output_dir))
        self.console.print(summary_table)
        self.console.print()

        # Progress tracking
        success_count = 0
        error_count = 0
        orchestrator_count = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console,
            expand=True,
        ) as progress:
            # Add extra tasks for orchestrators + 1 for all_lakehouses_orchestrator
            main_task = progress.add_task(
                f"[cyan]Processing {self.entities_folder.lower()}...",
                total=total_tasks + len(entity_dirs) + 1,
            )

            for entity_path, config_dirs in entity_configs.items():
                notebook_names = []  # Collect notebook names for orchestrator

                # Generate individual notebooks
                for config_path in config_dirs:
                    task_description = (
                        f"[yellow]{entity_path.name}/{config_path.name}[/yellow]"
                    )
                    progress.update(
                        main_task, description=f"Processing {task_description}"
                    )

                    output_path = (
                        self.output_dir / entity_path.name / f"{config_path.name}"
                    )
                    notebook_display_name = (
                        f"{config_path.name}_{entity_path.name}_{self.entities_folder}"
                    )
                    notebook_names.append(f"{notebook_display_name}")

                    try:
                        # Ensure directory structure exists before generating
                        # output_path.mkdir(parents=True, exist_ok=True)
                        self.generate_notebook(
                            config_folder=config_path,
                            output_folder=output_path,
                            notebook_display_name=notebook_display_name,
                            target_lakehouse_config_prefix=entity_path.name,
                            progress=progress,
                            parent_task=main_task,
                        )
                        success_count += 1
                        progress.console.print(
                            f"[green]✓[/green] Generated notebook for {task_description}"
                        )
                    except Exception as e:
                        error_count += 1
                        progress.console.print_exception()
                        progress.console.print(
                            f"[red]✗[/red] Error generating notebook for {task_description}: {str(e)}"
                        )

                    progress.advance(main_task)

                # Generate orchestrator notebook for this entity
                try:
                    progress.update(
                        main_task,
                        description=f"Generating orchestrator for [yellow]{entity_path.name}[/yellow]",
                    )
                    self.generate_orchestrator_notebook(
                        entity_path.name,
                        notebook_names,
                        self.output_dir / entity_path.name,
                        target_lakehouse_config_prefix=entity_path.name,
                    )
                    orchestrator_count += 1
                    progress.console.print(
                        f"[green]✓[/green] Generated orchestrator for {entity_path.name}"
                    )
                    progress.advance(main_task)
                except Exception as e:
                    progress.console.print_exception()
                    progress.console.print(
                        f"[red]✗[/red] Error generating orchestrator for {entity_path.name}: {str(e)}"
                    )
                    progress.advance(main_task)

            # Generate the all_lakehouses_orchestrator
            try:
                progress.update(
                    main_task,
                    description=f"Generating master orchestrator for all "
                    f"{self.entities_folder.lower()}",
                )
                self.generate_all_entities_orchestrator(entity_names, self.output_dir)
                #self.generate_config_notebook(self.output_dir)
                progress.console.print(
                    f"[green]✓[/green] Generated master orchestrator for all {self.entities_folder.lower()}"
                )
                progress.advance(main_task)
            except Exception as e:
                progress.console.print_exception()
                progress.console.print(
                    f"[red]✗[/red] Error generating master orchestrator: {str(e)}"
                )
                progress.advance(main_task)

        # Print final summary
        self.console.print()
        self.console.print(
            Panel.fit(
                f"[bold green]✓ Successfully generated {success_count} notebooks[/bold green]\n"
                f"[bold green]✓ Successfully generated {orchestrator_count} "
                f"{self.entity_type.lower()} orchestrators[/bold green]\n"
                f"[bold green]✓ Generated 1 master orchestrator[/bold green]\n"
                f"[bold red]✗ Failed to generate {error_count} notebooks[/bold red]",
                title="[bold]Generation Complete[/bold]",
                border_style="green" if error_count == 0 else "yellow",
            )
        )

    def create_notebook_with_platform(
        self,
        notebook_name: str,
        rendered_content: str,
        output_dir: Path,
    ) -> Path:
        """
        Create a notebook with its .platform file.

        Args:
            notebook_name: The name of the notebook (without extension)
            rendered_content: The rendered notebook content
            output_dir: The directory where the notebook should be created

        Returns:
            Path to the created notebook directory
        """
        # Create output path
        output_path = output_dir / f"{notebook_name}.Notebook"
        output_path.mkdir(parents=True, exist_ok=True)

        # Write notebook content
        notebook_file = output_path / "notebook-content.py"
        with notebook_file.open("w", encoding="utf-8") as f:
            f.write(rendered_content)

        # Create .platform file
        platform_template = self.load_template("./common/platform.json.jinja")
        platform_metadata = platform_template.render(
            notebook_name=f"{notebook_name}_ddl_scripts", guid=uuid.uuid4()
        )

        platform_path = output_path / ".platform"
        # Only create .platform if it does not exist
        if not platform_path.exists():
            with platform_path.open("w", encoding="utf-8") as f:
                f.write(platform_metadata)

        return output_path
