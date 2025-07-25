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
        project_path: Path = None,
        environment: str = None
    ):
        """Initialize the VariableLibraryUtils class."""

        self.project_path = project_path
        self.environment = environment                        
        self.variables = self._extract_variables(
            self._load_variable_library(project_path, environment)
        )

    def _load_variable_library(
        self, project_path: Path, environment: str = "development"
    ) -> dict[str, Any]:
        """Load variable library JSON file for the specified environment."""
        varlib_path = (
            project_path
            / Path("fabric_workspace_items")
            / Path("config")
            / Path("var_lib.VariableLibrary")
            / Path("valueSets")
            / Path(f"{environment}.json")
        )

        if not varlib_path.exists():
            raise FileNotFoundError(f"Variable library file not found: {varlib_path}")

        with open(varlib_path, "r") as f:
            return json.load(f)

    def _extract_variables(self, varlib_data: dict[str, Any]) -> dict[str, str]:
        """Extract variables from the variable library data."""
        variables = {}

        for override in varlib_data.get("variableOverrides", []):
            name = override.get("name")
            value = override.get("value")
            if name and value:
                variables[name] = value

        return variables

    def _get_config_block(self, template_path: Path, variables: dict[str, str]) -> str:
        """Update the config.jinja template with values from the variable library."""
        with open(template_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Define the variable mappings (template variable name -> varlib variable name)
        # Create variable mappings dynamically from all variables in varlib
        variable_mappings = {var_name: var_name for var_name in variables.keys()}

        return content
      
    def inject_variables_into_template(self, target_file: Path = None) -> None:
        """Main function to inject variables into the template and replace variable placeholders.
        
        Args:
            target_file: Optional specific file to update. If None, updates all notebook-content files.
        """
        variables = self.variables

        # If a specific file is provided, update only that file
        if target_file:
            files_to_update = [target_file]
        else:
            # Find all notebook-content files and update them
            files_to_update = []
            workspace_items_path = self.project_path / Path("fabric_workspace_items")

            if workspace_items_path.exists():
                for notebook_file in workspace_items_path.rglob("notebook-content.py"):
                    files_to_update.append(notebook_file)

            if not files_to_update:
                print(f"No notebook-content files found in {workspace_items_path}")
                return

        updated_files = []
        for file_path in files_to_update:
            # Skip if file doesn't exist
            if not file_path.exists():
                print(f"Warning: File not found: {file_path}")
                continue
                
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            updated_content = self.perform_code_replacements(content)

            # Write the updated content back to the file if changed
            if updated_content != content:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                updated_files.append(file_path)

        # Report results
        if updated_files:
            if target_file:
                print(f"Updated specific file with values from {self.environment} environment:")
            else:
                print(f"Updated {len(updated_files)} file(s) with values from {self.environment} environment:")
            for file in updated_files:
                print(f"  - {file.relative_to(self.project_path) if self.project_path else file}")
        else:
            if target_file:
                print(f"No changes needed for target file: {target_file}")
            else:
                print(f"No files with injection markers or placeholders found")
            
    def get_workspace_id(self) -> str:
        """Get the target workspace ID from the variable library."""
        ret_val = None
        ret_val = self.variables.get("fabric_deployment_workspace_id", None)
        if ret_val is None:
            raise ValueError(
                "fabric_deployment_workspace_id not found in variable library"
            )
        return ret_val

    def get_variable_value(self, variable_name: str) -> str:
        """Get the value of a specific variable from the variable library."""
        ret_val = None
        ret_val = self.variables.get(variable_name, None)
        if ret_val is None:
            raise ValueError(
                f"{variable_name} not found in variable library"
            )
        return ret_val

    def perform_code_replacements(self, content: str) -> str:
        """Replace variable placeholders in the content with actual values."""
        # 1. Replace variable placeholders throughout the file
        def replace_placeholder(match):
            var_name = match.group(1)
            if var_name in self.variables:
                return str(self.variables[var_name])
            return match.group(0)  # leave unchanged if not found

        # Regex to match placeholders like {{varlib:variable_name}}
        placeholder_pattern = re.compile(r"\{\{varlib:([a-zA-Z0-9_]+)\}\}")
        content_with_vars = placeholder_pattern.sub(replace_placeholder, content)

        # 2. Replace the content between injection markers (if present)
        pattern = r"(# variableLibraryInjectionStart: var_lib\n)(.*?)(# variableLibraryInjectionEnd: var_lib)"

        def replace_block(match):
            start_marker = match.group(1)
            end_marker = match.group(3)

            new_lines = []
            class_definition_lines = []
            class_definition_lines.append("from dataclasses import dataclass")
            class_definition_lines.append("@dataclass")
            class_definition_lines.append("class ConfigsObject:")
            for var_name, var_value in self.variables.items():
                if isinstance(var_value, str):
                    class_definition_lines.append(f"    {var_name}: str ")
                else:
                    class_definition_lines.append(f"    {var_name}: Any ")

            new_lines.append("")
            new_lines.append("# All variables as a dictionary")
            new_lines.append(f"configs_dict = {repr(self.variables)}")
            new_lines.append("# All variables as an object")
            new_lines.append("\n".join(class_definition_lines))
            new_lines.append("configs_object: ConfigsObject = ConfigsObject(**configs_dict)")

            new_content = start_marker + "\n".join(new_lines) + "\n" + end_marker
            return new_content

        if re.search(pattern, content_with_vars, re.DOTALL):
            updated_content = re.sub(pattern, replace_block, content_with_vars, flags=re.DOTALL)
        else:
            updated_content = content_with_vars

        return updated_content


def inject_variables_into_file(
    project_path: Path,
    target_file: Path,
    environment: str = "local"
) -> None:
    """Convenience function to inject variables into a specific file.
    
    Args:
        project_path: Path to the project root
        target_file: Path to the specific file to update
        environment: Environment name for variable library lookup
    """
    varlib_utils = VariableLibraryUtils(project_path, environment)
    varlib_utils.inject_variables_into_template(target_file)