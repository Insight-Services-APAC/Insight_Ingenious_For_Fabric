from pathlib import Path

from rich.console import Console
from rich.theme import Theme

from ingen_fab.cli_utils.console_styles import ConsoleStyles


def init_solution(project_name: str, path: Path):
    project_path = Path(path)
    directories = [
        "ddl_scripts/Lakehouses",
        "ddl_scripts/Warehouses",
        "fabric_workspace_items",
        "var_lib",
        "diagrams",
    ]
    for dir_path in directories:
        (project_path / dir_path).mkdir(parents=True, exist_ok=True)
    templates_dir = Path(__file__).parent.parent / "project_templates"
    dev_vars_template = (templates_dir / "development.yml").read_text()
    prod_vars_template = (templates_dir / "production.yml").read_text()
    gitignore_template = (templates_dir / ".gitignore").read_text()
    readme_template = (templates_dir / "README.md").read_text()
    with open(project_path / "var_lib" / "development.yml", "w") as f:
        f.write(dev_vars_template.format(project_name=project_name))
    with open(project_path / "var_lib" / "production.yml", "w") as f:
        f.write(prod_vars_template.format(project_name=project_name))
    with open(project_path / ".gitignore", "w") as f:
        f.write(gitignore_template)
    with open(project_path / "README.md", "w") as f:
        f.write(readme_template.format(project_name=project_name))
    console = Console()
    ConsoleStyles.print_success(
        console, f"✓ Initialized Fabric solution '{project_name}' at {project_path}"
    )
    ConsoleStyles.print_info(console, "\nNext steps:")
    ConsoleStyles.print_info(console, "1. Update var_lib/*.yml files with your workspace IDs")
    ConsoleStyles.print_info(console, "2. Create DDL scripts in ddl_scripts/")
    ConsoleStyles.print_info(console, "3. Run: ingen_fab compile-ddl-notebooks --output-mode fabric --generation-mode warehouse")
