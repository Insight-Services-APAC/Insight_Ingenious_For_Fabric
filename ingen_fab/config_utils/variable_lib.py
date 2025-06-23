"""
Script to populate Fabric Configurations in config.jinja template from variable library JSON files.
"""
from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Any


class VariableLibraryUtils:
    """Utility class for handling variable library operations."""

    def __init__(
            self,
            project_path: Path = Path('sample_project'),
            environment: str = 'development',
            template_path: Path = Path('ingen_fab/ddl_scripts/_templates/warehouse/config.jinja'),
            output: Path | None = None,
            in_place: bool = False
            ):
        """Initialize the VariableLibraryUtils class."""
        self.project_path = project_path
        self.environment = environment
        self.template_path = template_path
        self.output = output
        self.in_place = in_place

    def load_variable_library(self, project_path: Path, environment: str = "development") -> dict[str, Any]:
        """Load variable library JSON file for the specified environment."""
        varlib_path = project_path / Path("fabric_workspace_items") / \
            Path("config") / Path("var_lib.VariableLibrary") / Path("valueSets") / Path(f"{environment}.json")
        
        if not varlib_path.exists():
            raise FileNotFoundError(f"Variable library file not found: {varlib_path}")
        
        with open(varlib_path, 'r') as f:
            return json.load(f)

    def extract_variables(self, varlib_data: dict[str, Any]) -> dict[str, str]:
        """Extract variables from the variable library data."""
        variables = {}
        
        for override in varlib_data.get("variableOverrides", []):
            name = override.get("name")
            value = override.get("value")
            if name and value:
                variables[name] = value
        
        return variables

    def get_config_block(self, template_path: Path, variables: dict[str, str]) -> str:
        """Update the config.jinja template with values from the variable library."""
        with open(template_path, 'r', encoding="utf-8") as f:
            content = f.read()
        
        # Define the variable mappings (template variable name -> varlib variable name)
        # Create variable mappings dynamically from all variables in varlib
        variable_mappings = {var_name: var_name for var_name in variables.keys()}
        
        return content

    def inject_variables_into_template(self):
        """Main function to inject variables into the template."""
        #try:
        # Load variable library
        varlib_data = self.load_variable_library(self.project_path, self.environment)
        
        # Extract variables
        variables = self.extract_variables(varlib_data)
        
        # Output results
        # Find all notebook-content files and update them
        notebook_files = []
        workspace_items_path = self.project_path / Path("fabric_workspace_items")
        
        if workspace_items_path.exists():
            for notebook_file in workspace_items_path.rglob("notebook-content.py"):
                notebook_files.append(notebook_file)
        
        if not notebook_files:
            print(f"No notebook-content files found in {workspace_items_path}")
            return
        
        # Process each notebook file
        updated_files = []
        for notebook_file in notebook_files:
            with open(notebook_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Pattern to find the injection blocks
            pattern = r'(# variableLibraryInjectionStart: var_lib\n)(.*?)(# variableLibraryInjectionEnd: var_lib)'
            
            # Replace the content between injection markers
            def replace_block(match):
                start_marker = match.group(1)
                end_marker = match.group(3)
            
                # Build the new content with variables
                new_lines = []
                for var_name, var_value in variables.items():
                    # Convert value to appropriate Python literal
                    if isinstance(var_value, str):
                        new_lines.append(f'{var_name} = "{var_value}"')
                    else:
                        new_lines.append(f'{var_name} = {var_value}')
                
                # Also inject the entire variables dict
                new_lines.append('')  # Add blank line for readability
                new_lines.append('# All variables as a dictionary')
                new_lines.append(f'configs_dict = {repr(variables)}')
                
                # Join with newlines and add markers
                new_content = start_marker + '\n'.join(new_lines) + '\n' + end_marker
                return new_content
            
            # Check if the pattern exists in the file
            if re.search(pattern, content, re.DOTALL):
                updated_content = re.sub(
                    pattern, 
                    replace_block, 
                    content, 
                    flags=re.DOTALL)
            
            # Write the updated content back to the file
            if re.search(pattern, content, re.DOTALL):
                with open(notebook_file, 'w', encoding='utf-8') as f:
                    f.write(updated_content)
                updated_files.append(notebook_file)
        
        # Report results
        if updated_files:
            print(
                f"Updated {len(updated_files)} notebook-content file(s) with values from {self.environment}"
                " environment:"
                )
            for file in updated_files:
                print(f"  - {file.relative_to(self.project_path)}")
        else:
            print(f"No notebook-content files with injection markers found in {workspace_items_path}")
                
        