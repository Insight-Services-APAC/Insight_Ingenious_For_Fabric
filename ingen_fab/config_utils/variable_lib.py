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
      
    def inject_variables_into_template(self, target_file: Path = None, output_dir: Path = None, preserve_structure: bool = True, replace_placeholders: bool = True, inject_code: bool = True) -> None:
        """Main function to inject variables into the template and replace variable placeholders.
        
        Args:
            target_file: Optional specific file to update. If None, updates all notebook-content files.
            output_dir: Optional directory to save updated files instead of modifying in place.
                       If None, files are updated in place (default behavior).
            preserve_structure: When using output_dir, whether to preserve the directory structure
                              relative to project_path (default: True).
            replace_placeholders: Whether to replace {{varlib:...}} placeholders (default: True).
            inject_code: Whether to inject code between markers (default: True).
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

        # Create output directory if specified
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        updated_files = []
        for file_path in files_to_update:
            # Skip if file doesn't exist
            if not file_path.exists():
                print(f"Warning: File not found: {file_path}")
                continue
                
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            updated_content = self.perform_code_replacements(content, replace_placeholders=replace_placeholders, inject_code=inject_code)

            # Only process if content changed
            if updated_content != content:
                if output_dir:
                    # Determine output file path
                    if preserve_structure and self.project_path:
                        # Calculate relative path from project root
                        try:
                            rel_path = file_path.relative_to(self.project_path)
                            output_file_path = output_dir / rel_path
                        except ValueError:
                            # If file is not under project path, just use filename
                            output_file_path = output_dir / file_path.name
                    else:
                        # Just use the filename without preserving structure
                        output_file_path = output_dir / file_path.name
                    
                    # Create parent directories if needed
                    output_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Write to output directory
                    with open(output_file_path, "w", encoding="utf-8") as f:
                        f.write(updated_content)
                    updated_files.append((file_path, output_file_path))
                else:
                    # Write back to original file (in-place update)
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(updated_content)
                    updated_files.append((file_path, file_path))

        # Report results
        if updated_files:
            if output_dir:
                print(f"Saved {len(updated_files)} updated file(s) to {output_dir} with values from {self.environment} environment:")
                for orig_file, output_file in updated_files:
                    if self.project_path:
                        try:
                            orig_rel = orig_file.relative_to(self.project_path)
                            print(f"  - {orig_rel} -> {output_file.relative_to(output_dir)}")
                        except ValueError:
                            print(f"  - {orig_file.name} -> {output_file.relative_to(output_dir)}")
                    else:
                        print(f"  - {orig_file.name} -> {output_file.relative_to(output_dir)}")
            else:
                if target_file:
                    print(f"Updated specific file with values from {self.environment} environment:")
                else:
                    print(f"Updated {len(updated_files)} file(s) with values from {self.environment} environment:")
                for file_tuple in updated_files:
                    file_path = file_tuple[0]  # Get the original file path
                    print(f"  - {file_path.relative_to(self.project_path) if self.project_path else file_path}")
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

    def replace_variable_placeholders(self, content: str) -> str:
        """Replace variable placeholders like {{varlib:variable_name}} with actual values.
        
        Args:
            content: The content containing variable placeholders
            
        Returns:
            Content with placeholders replaced by actual values
        """
        def replace_placeholder(match):
            var_name = match.group(1)
            if var_name in self.variables:
                return str(self.variables[var_name])
            return match.group(0)  # leave unchanged if not found

        # Regex to match placeholders like {{varlib:variable_name}}
        placeholder_pattern = re.compile(r"\{\{varlib:([a-zA-Z0-9_]+)\}\}")
        return placeholder_pattern.sub(replace_placeholder, content)

    def inject_code_between_markers(self, content: str) -> str:
        """Replace content between injection markers with generated code.
        
        Args:
            content: The content containing injection markers
            
        Returns:
            Content with code between markers replaced
        """
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

        if re.search(pattern, content, re.DOTALL):
            return re.sub(pattern, replace_block, content, flags=re.DOTALL)
        else:
            return content

    def perform_code_replacements(self, content: str, replace_placeholders: bool = True, inject_code: bool = True) -> str:
        """Replace variable placeholders and/or inject code between markers.
        
        This method maintains backward compatibility while allowing separate control
        over placeholder replacement and code injection.
        
        Args:
            content: The content to process
            replace_placeholders: Whether to replace {{varlib:...}} placeholders (default: True)
            inject_code: Whether to inject code between markers (default: True)
            
        Returns:
            Processed content with requested replacements
        """
        result = content
        
        # 1. Replace variable placeholders if requested
        if replace_placeholders:
            result = self.replace_variable_placeholders(result)
        
        # 2. Inject code between markers if requested
        if inject_code:
            result = self.inject_code_between_markers(result)
        
        return result


def inject_variables_into_file(
    project_path: Path,
    target_file: Path,
    environment: str = "local",
    output_dir: Path = None,
    preserve_structure: bool = True,
    replace_placeholders: bool = True,
    inject_code: bool = True
) -> None:
    """Convenience function to inject variables into a specific file.
    
    Args:
        project_path: Path to the project root
        target_file: Path to the specific file to update
        environment: Environment name for variable library lookup
        output_dir: Optional directory to save updated files instead of modifying in place
        preserve_structure: When using output_dir, whether to preserve the directory structure
        replace_placeholders: Whether to replace {{varlib:...}} placeholders
        inject_code: Whether to inject code between markers
    """
    varlib_utils = VariableLibraryUtils(project_path, environment)
    varlib_utils.inject_variables_into_template(target_file, output_dir, preserve_structure, replace_placeholders, inject_code)