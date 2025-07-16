import json
import shutil
import uuid
from pathlib import Path

from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.utils.path_utils import PathUtils


def init_solution(project_name: str, path: Path):
    project_path = Path(path) / project_name
    
    console = Console()
    
    # Get the templates directory using the new path utilities
    try:
        templates_dir = PathUtils.get_template_path('project')
    except FileNotFoundError as e:
        ConsoleStyles.print_error(console, f"❌ Project templates not found: {e}")
        return
    
    if not templates_dir.exists():
        ConsoleStyles.print_error(console, f"❌ Templates directory does not exist: {templates_dir}")
        return
    
    # Create the project directory
    project_path.mkdir(parents=True, exist_ok=True)
    
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
    
    # Process README.md for project name substitution
    readme_path = project_path / "README.md"
    if readme_path.exists():
        content = readme_path.read_text()
        content = content.replace("{project_name}", project_name)
        readme_path.write_text(content)
        ConsoleStyles.print_info(console, "  ✓ Processed template: README.md")
    
    # Find and process all .platform files
    platform_files = list(project_path.rglob("*.platform"))
    
    if platform_files:
        ConsoleStyles.print_info(console, f"  Found {len(platform_files)} .platform files to process")
        
        for platform_file in platform_files:
            try:
                # Generate a unique GUID for each .platform file
                unique_guid = str(uuid.uuid4())
                
                # Read and parse the JSON content
                content = platform_file.read_text(encoding='utf-8')
                
                # Replace GUID placeholders and any project name references
                content = content.replace("REPLACE_WITH_UNIQUE_GUID", unique_guid)
                content = content.replace("{project_name}", project_name)
                
                # For files that might have hardcoded GUIDs, replace them with new ones
                # This handles cases where template files already have GUIDs that should be unique per project
                try:
                    json_data = json.loads(content)
                    if "config" in json_data and "logicalId" in json_data["config"]:
                        # If logicalId exists and is not already "REPLACE_WITH_UNIQUE_GUID", 
                        # it might be a hardcoded GUID that should be replaced
                        existing_logical_id = json_data["config"]["logicalId"]
                        if existing_logical_id != "REPLACE_WITH_UNIQUE_GUID" and len(existing_logical_id) > 30:
                            # This looks like a GUID, replace it with a new one
                            json_data["config"]["logicalId"] = unique_guid
                            content = json.dumps(json_data, indent=2)
                except json.JSONDecodeError:
                    # If it's not valid JSON, just do string replacement
                    pass
                
                platform_file.write_text(content, encoding='utf-8')
                
                # Get relative path for display
                relative_path = platform_file.relative_to(project_path)
                ConsoleStyles.print_info(console, f"  ✓ Processed .platform file: {relative_path} (GUID: {unique_guid[:8]}...)")
                
            except Exception as e:
                relative_path = platform_file.relative_to(project_path)
                ConsoleStyles.print_error(console, f"  ❌ Failed to process .platform file: {relative_path} - {str(e)}")
    else:
        ConsoleStyles.print_info(console, "  No .platform files found to process")
