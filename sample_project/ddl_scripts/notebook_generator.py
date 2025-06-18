
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import argparse
from rich.console import Console
from rich.panel import Panel
from rich.table import Table


class NotebookGenerator:
    def __init__(self, mode: str, output_mode: str):
        """
        Initialize the DDL Notebook Generator.
        
        Args:
            mode: Either "lakehouse" or "warehouse"
                - "lakehouse": Generate notebooks for Lakehouses
                - "warehouse": Generate notebooks for Warehouses
            output_mode: Either "fabric_workspace_repo" or "local"
                - "fabric_workspace_repo": Write to ../fabric_workspace_items/ddl_scripts/Lakehouses
                - "local": Write to ./target/ddl_scripts/Lakehouses
        """
        assert mode in ["lakehouse", "warehouse"], "Mode must be 'lakehouse' or 'warehouse'"
        self.mode = mode
        assert output_mode in ["fabric_workspace_repo", "local"], "Output mode must be 'fabric_workspace_repo' or 'local'"
        self.output_mode = output_mode
        self.base_dir = Path(__file__).resolve().parent
        self.templates_dir = self.base_dir / "_templates" / (mode if mode == "warehouse" else "")
        self.input_dir = self.base_dir / ("Warehouses" if mode == "warehouse" else "Lakehouses")
        
        if output_mode == "local":
            self.output_dir = self.base_dir / "target" / "ddl_scripts" / (mode.capitalize() + "s")
        else: 
            self.output_dir = (
                self.base_dir / ".." / "fabric_workspace_items" / "ddl_scripts" /
                ("Warehouses" if mode == "warehouse" else "Lakehouses")
            ).resolve()

        self.env = Environment(loader=FileSystemLoader(str(self.templates_dir)))

    def load_template(self, name: str):
        return self.env.get_template(name)

    def get_dirs(self):
        return sorted([d for d in self.input_dir.iterdir() if d.is_dir()])

    def get_sorted_files(self, dir_path, extensions=None):
        files = []
        for path in dir_path.iterdir():
            if path.is_file() and (not extensions or path.suffix in extensions):
                try:
                    num = int(path.stem.split("_", 1)[0])
                    files.append((num, path))
                except Exception:
                    continue
        return [path for _, path in sorted(files)]

    def generate_guid(self, relative_path):
        import hashlib
        return hashlib.sha256(str(relative_path).encode("utf-8")).hexdigest()[:12]

    def render_notebook(self, config_folder, output_folder, display_name):
        notebook_template = self.load_template("notebook_content.py.jinja")
        platform_template = self.load_template("platform.json.jinja")

        cells = []
        sorted_files = self.get_sorted_files(config_folder, extensions=[".py", ".sql"])
        for file_path in sorted_files:
            relative_path = file_path.relative_to(self.input_dir)
            guid = self.generate_guid(str(relative_path))
            content = file_path.read_text(encoding="utf-8")

            if file_path.suffix == ".py":
                template = self.load_template("script_cells/pyspark.py.jinja")
            elif file_path.suffix == ".sql":
                template = self.load_template("script_cells/spark_sql.py.jinja")
            else:
                continue

            rendered = template.render(
                heading_level="#",
                heading_name=f"Cell for {file_path.name}",
                content=content,
                script_guid=guid,
                script_name=file_path.stem,
                target_workspace_id="example-workspace-id",
                target_lakehouse_id="example-id" if self.mode == "lakehouse" else None,
                target_warehouse_id="example-id" if self.mode == "warehouse" else None
            )
            cells.append(rendered)

        output_folder.mkdir(parents=True, exist_ok=True)
        with (output_folder / "notebook-content.py").open("w", encoding="utf-8") as f:
            f.write(notebook_template.render(cells=cells))

        import uuid
        rel_path = config_folder.relative_to(self.input_dir)
        metadata = platform_template.render(
            notebook_name=display_name,
            guid=uuid.uuid4(),
            relative_path=rel_path
        )
        platform_file = output_folder / ".platform"
        if not platform_file.exists():
            with platform_file.open("w", encoding="utf-8") as f:
                f.write(metadata)

    def generate_orchestrator(self, name, notebook_names):
        orchestrator_template = self.load_template("orchestrator_notebook.py.jinja")
        platform_template = self.load_template("platform.json.jinja")

        orchestrator_path = self.output_dir / name / f"0_orchestrator_{name}.Notebook"
        orchestrator_path.mkdir(parents=True, exist_ok=True)

        with (orchestrator_path / "notebook-content.py").open("w", encoding="utf-8") as f:
            f.write(orchestrator_template.render(
                lakehouse_name=name,
                notebooks=[{"index": i + 1, "name": nb, "total": len(notebook_names)}
                           for i, nb in enumerate(notebook_names)],
                total_notebooks=len(notebook_names)
            ))

        import uuid
        with (orchestrator_path / ".platform").open("w", encoding="utf-8") as f:
            f.write(platform_template.render(
                notebook_name=f"0_orchestrator_{name}_{self.mode.capitalize()}s_ddl_scripts",
                guid=uuid.uuid4(),
                relative_path=f"{name}/0_orchestrator_{name}"
            ))

    def generate_master_orchestrator(self, names):
        template = self.load_template(f"orchestrator_notebook_all_{self.mode}s.py.jinja")
        platform_template = self.load_template("platform.json.jinja")

        output = self.output_dir / f"00_all_{self.mode}s_orchestrator.Notebook"
        output.mkdir(parents=True, exist_ok=True)

        with (output / "notebook-content.py").open("w", encoding="utf-8") as f:
            f.write(template.render(
                lakehouses=[{
                    "name": name,
                    "orchestrator_name": f"0_orchestrator_{name}"
                } for name in names],
                total_lakehouses=len(names)
            ))

        import uuid
        with (output / ".platform").open("w", encoding="utf-8") as f:
            f.write(platform_template.render(
                notebook_name=f"00_all_{self.mode}s_orchestrator_ddl_scripts",
                guid=uuid.uuid4(),
                relative_path=f"00_all_{self.mode}s_orchestrator"
            ))

    def generate_ddl_notebooks(self):
        console = Console()
        console.print(Panel.fit(f"[bold cyan]{self.mode.capitalize()} Notebook Generator[/bold cyan]", border_style="cyan"))
        console.print()

        generator = NotebookGenerator(mode=self.mode)
        entity_dirs = generator.get_dirs()
        total_cells = 0
        total_notebooks = 0
        error_count = 0
        entity_names = []
        entity_configs = {}

        for entity_path in entity_dirs:
            config_dirs = [d for d in generator.get_dirs() if d.parent == entity_path]
            entity_configs[entity_path] = config_dirs
            entity_names.append(entity_path.name)
            total_notebooks += len(config_dirs)

            for cfg in config_dirs:
                total_cells += len(generator.get_sorted_files(cfg, extensions=[".py", ".sql"]))

        summary = Table(title=f"{self.mode.capitalize()} Generation Summary")
        summary.add_column("Metric", style="cyan")
        summary.add_column("Value", style="green")
        summary.add_row(f"{self.mode.capitalize()}s Found", str(len(entity_dirs)))
        summary.add_row("Notebooks to Generate", str(total_notebooks))
        summary.add_row("Total Cells", str(total_cells))
        summary.add_row("Output Folder", str(generator.output_dir))
        console.print(summary)
        console.print()

        for entity_path, config_dirs in entity_configs.items():
            notebook_names = []
            for config_path in config_dirs:
                try:
                    notebook_name = f"{config_path.name}_{entity_path.name}_{self.mode.capitalize()}s_ddl_scripts"
                    output_path = generator.output_dir / entity_path.name / f"{config_path.name}.Notebook"
                    generator.render_notebook(config_path, output_path, notebook_name)
                    notebook_names.append(notebook_name)
                    console.print(f"[green]✓ Generated:[/green] {notebook_name}")
                except Exception as e:
                    error_count += 1
                    console.print(f"[red]✗ Failed to generate for {config_path}: {e}")

            try:
                generator.generate_orchestrator(entity_path.name, notebook_names)
                console.print(f"[green]✓ Orchestrator created for {entity_path.name}[/green]")
            except Exception as e:
                error_count += 1
                console.print(f"[red]✗ Failed to create orchestrator for {entity_path.name}: {e}")

        try:
            generator.generate_master_orchestrator(entity_names)
            console.print(f"[green]✓ Master orchestrator created[/green]")
        except Exception as e:
            error_count += 1
            console.print(f"[red]✗ Failed to create master orchestrator: {e}")

        console.print()
        console.print(Panel.fit(
            f"[bold green]✓ Total Successful Notebooks: {total_notebooks - error_count}[/bold green]\\n"
            f"[bold green]✓ Total Orchestrators: {len(entity_dirs)}[/bold green]\\n"
            f"[bold red]✗ Total Errors: {error_count}[/bold red]",
            title=f"[bold]{self.mode.capitalize()} Generation Complete[/bold]",
            border_style="green" if error_count == 0 else "red"
        ))

