import shutil
import uuid
from pathlib import Path

from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles


def init_solution(project_name: str, path: Path):
    project_path = Path(path) / project_name
    
    # Get the templates directory using proper path resolution
    # Always use Path.cwd() and full path for pip package compatibility
    templates_dir = Path.cwd() / "project_templates"
    
    # If not found in current directory, try the development path
    if not templates_dir.exists():
        templates_dir = Path.cwd() / "ingen_fab" / "project_templates"
    
    if not templates_dir.exists():
        console = Console()
        ConsoleStyles.print_error(console, f"❌ Templates directory not found: {templates_dir}")
        return
    
    # Create the project directory
    project_path.mkdir(parents=True, exist_ok=True)
    
    console = Console()
    ConsoleStyles.print_info(console, f"Creating new Fabric project '{project_name}' at {project_path}")
    
    # Copy all template files and directories
    for item in templates_dir.iterdir():
        dest = project_path / item.name
        if item.is_dir():
            shutil.copytree(item, dest, dirs_exist_ok=True)
            ConsoleStyles.print_info(console, f"  ✓ Copied directory: {item.name}")
        else:
            shutil.copy2(item, dest)
            ConsoleStyles.print_info(console, f"  ✓ Copied file: {item.name}")
    
    # Process template files that need variable substitution
    _process_template_files(project_path, project_name, console)
    
    ConsoleStyles.print_success(
        console, f"✓ Initialized Fabric solution '{project_name}' at {project_path}"
    )
    ConsoleStyles.print_info(console, "\nNext steps:")
    ConsoleStyles.print_info(console, "1. Update variable values in fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/")
    ConsoleStyles.print_info(console, "   - Replace placeholder GUIDs with your actual workspace and lakehouse IDs")
    ConsoleStyles.print_info(console, "2. Create additional DDL scripts in ddl_scripts/ as needed")
    ConsoleStyles.print_info(console, "3. Generate DDL notebooks:")
    ConsoleStyles.print_info(console, f"   export FABRIC_WORKSPACE_REPO_DIR='./{project_name}'")
    ConsoleStyles.print_info(console, "   export FABRIC_ENVIRONMENT='development'")
    ConsoleStyles.print_info(console, "   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse")
    ConsoleStyles.print_info(console, "4. Deploy to Fabric:")
    ConsoleStyles.print_info(console, "   ingen_fab deploy deploy --environment development")


def _process_template_files(project_path: Path, project_name: str, console: Console):
    """Process template files that need variable substitution"""
    
    # Generate unique GUIDs for platform files
    sample_lakehouse_guid = str(uuid.uuid4())
    sample_warehouse_guid = str(uuid.uuid4())
    
    # Files that need template processing
    template_files = [
        "README.md",
        "fabric_workspace_items/lakehouses/sample.Lakehouse/.platform",
        "fabric_workspace_items/warehouses/sample.Warehouse/.platform",
    ]
    
    for template_file in template_files:
        file_path = project_path / template_file
        if file_path.exists():
            content = file_path.read_text()
            
            # Replace template variables
            content = content.replace("{project_name}", project_name)
            content = content.replace("REPLACE_WITH_UNIQUE_GUID", 
                                    sample_lakehouse_guid if "lakehouse" in template_file.lower() 
                                    else sample_warehouse_guid)
            
            file_path.write_text(content)
            ConsoleStyles.print_info(console, f"  ✓ Processed template: {template_file}")
