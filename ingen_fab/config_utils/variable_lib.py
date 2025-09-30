"""
Script to populate Fabric Configurations in config.jinja template from variable library JSON files.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

try:
    from rich.console import Console

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from ingen_fab.cli_utils.console_styles import MessageHelpers
from ingen_fab.cli_utils.display_utils import PanelBuilder, TableBuilder
from ingen_fab.cli_utils.progress_utils import ProgressTracker


class VariableLibraryUtils:
    """Utility class for handling variable library operations."""

    def __init__(self, project_path: Path = None, environment: str = None):
        """Initialize the VariableLibraryUtils class."""

        self.project_path = project_path
        self.environment = environment
        self.console = Console() if RICH_AVAILABLE else None

        # Initialize utility classes
        self.message_helper = MessageHelpers(self.console)
        self.table_builder = TableBuilder(self.console)
        self.panel_builder = PanelBuilder(self.console)
        self.progress_tracker = ProgressTracker(self.console)

        # Load variables with progress indication
        if self.console and RICH_AVAILABLE:
            with self.console.status(
                f"[blue]Loading variable library for environment: {environment}[/blue]",
                spinner="dots",
            ):
                varlib_data = self._load_variable_library(project_path, environment)
                self.variables = self._extract_variables(varlib_data)
        else:
            varlib_data = self._load_variable_library(project_path, environment)
            self.variables = self._extract_variables(varlib_data)

    def _load_variable_library(self, project_path: Path, environment: str = "development") -> dict[str, Any]:
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

    def _display_variables_summary(self) -> None:
        """Display a summary of loaded variables in a nice table format."""
        self.table_builder.create_summary_table(
            self.variables,
            title=f"Variable Library Summary - Environment: {self.environment}",
            title_style="bold blue",
            border_style="blue",
        )

    def _display_processing_stats(self, stats: dict, total_files: int, updated_files_count: int) -> None:
        """Display processing statistics in a nice table format."""
        self.table_builder.create_statistics_table(stats, total_files, updated_files_count)

    def _create_operation_setup(
        self,
        title: str,
        replace_placeholders: bool,
        inject_code: bool,
        border_style: str = "blue",
        target_directory: str = None,
        files_count: int = None,
        files_label: str = "Total files",
        mode: str = None,
        output_dir: str = None,
    ) -> None:
        """Create standardized operation setup with panel display.

        Args:
            title: Title for the operation panel
            replace_placeholders: Whether placeholder replacement is enabled
            inject_code: Whether code injection is enabled
            border_style: Panel border style
            target_directory: Target directory path (optional)
            files_count: Number of files to process (optional)
            files_label: Label for file count (default: "Total files")
            mode: Operation mode (e.g., "In-place modification")
            output_dir: Output directory path (optional)
        """
        # Show operation rule
        self.message_helper.print_rule(title, "bold blue")

        # Create operation details using helper methods
        operations = []
        if replace_placeholders:
            operations.append(self.message_helper.format_check_item("Replace {{varlib:...}} placeholders"))
        if inject_code:
            operations.append(self.message_helper.format_check_item("Inject code between markers"))

        operation_details = self.message_helper.create_operation_details(
            environment=self.environment,
            target_directory=target_directory,
            total_files=files_count,
            operations=operations,
            output_dir=output_dir,
            mode=mode,
            files_found_label=files_label,
        )

        self.panel_builder.create_operation_panel(
            operation_details,
            title=f"{title.split()[0]} Configuration",  # Extract first word for panel title
            border_style=border_style,
        )

    def _get_config_block(self, template_path: Path) -> str:
        """Update the config.jinja template with values from the variable library."""
        with open(template_path, "r", encoding="utf-8") as f:
            content = f.read()

        return content

    def _process_file(
        self,
        file_path: Path,
        replace_placeholders: bool,
        inject_code: bool,
        output_dir: Path = None,
        preserve_structure: bool = True,
        track_stats: bool = False,
        stats: dict = None,
        python_libs_mode: bool = False,
    ):
        """Unified file processing method for variable injection.

        Args:
            file_path: Path to the file to process
            replace_placeholders: Whether to replace {{varlib:...}} placeholders
            inject_code: Whether to inject code between markers
            output_dir: Optional directory to save updated files (None for in-place)
            preserve_structure: Whether to preserve directory structure in output
            track_stats: Whether to track detailed statistics
            stats: Statistics dictionary to update (required if track_stats=True)
            python_libs_mode: Whether this is processing python_libs (affects return format)

        Returns:
            - None if no changes made
            - Path if python_libs_mode=True and file was updated
            - Tuple[Path, Path] if output_dir specified (original, output)
            - Tuple[Path, Path] if in-place update (original, original)
        """

        # Validate stats parameter
        if track_stats and stats is None:
            raise ValueError("stats parameter required when track_stats=True")

        # Skip if file doesn't exist
        if not file_path.exists():
            self.message_helper.print_warning(f"File not found: {file_path}")
            if track_stats:
                stats["errors"] += 1
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Process content
            original_content = content
            result_content = content

            # Track placeholder replacements
            if replace_placeholders:
                result_content = self.replace_variable_placeholders(result_content)
                if track_stats and result_content != content:
                    stats["placeholders_replaced"] += 1

            # Track code injections
            if inject_code:
                old_content = result_content
                result_content = self.inject_code_between_markers(result_content)
                if track_stats and result_content != old_content:
                    stats["code_injected"] += 1

            # Only process if content changed
            if result_content != original_content:
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
                        f.write(result_content)
                    return (file_path, output_file_path)
                else:
                    # Write back to original file (in-place update)
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(result_content)

                    # Return format depends on mode
                    if python_libs_mode:
                        return file_path
                    else:
                        return (file_path, file_path)
            else:
                if track_stats:
                    stats["files_skipped"] += 1
                return None
        except Exception as e:
            self.message_helper.print_error(f"Error processing {file_path}: {e}")
            if track_stats:
                stats["errors"] += 1
            return None

    def _process_single_file(
        self,
        file_path: Path,
        output_dir: Path,
        preserve_structure: bool,
        replace_placeholders: bool,
        inject_code: bool,
    ):
        """Process a single file for variable injection."""
        return self._process_file(
            file_path=file_path,
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
            output_dir=output_dir,
            preserve_structure=preserve_structure,
            track_stats=False,
        )

    def _process_python_lib_file_with_stats(
        self, py_file: Path, replace_placeholders: bool, inject_code: bool, stats: dict
    ):
        """Process a single python_libs file and update statistics."""
        return self._process_file(
            file_path=py_file,
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
            output_dir=None,  # python_libs always in-place
            preserve_structure=True,
            track_stats=True,
            stats=stats,
            python_libs_mode=True,
        )

    def get_workspace_id(self) -> str:
        """Get the target workspace ID from the variable library."""
        ret_val = None
        ret_val = self.variables.get("fabric_deployment_workspace_id", None)
        if ret_val is None:
            raise ValueError("fabric_deployment_workspace_id not found in variable library")
        return ret_val

    def get_variable_value(self, variable_name: str) -> str:
        """Get the value of a specific variable from the variable library."""
        ret_val = None
        ret_val = self.variables.get(variable_name, None)
        if ret_val is None:
            raise ValueError(f"{variable_name} not found in variable library")
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
                replacement_value = str(self.variables[var_name])
                # Debug output when replacement occurs
                self.message_helper.print_info(f"Replacing {{{{varlib:{var_name}}}}} with '{replacement_value}'")
                return replacement_value
            else:
                # Debug output when variable not found
                self.message_helper.print_warning(
                    f"Variable '{var_name}' not found in library, keeping placeholder {{{{varlib:{var_name}}}}}"
                )
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

        # Check if markers exist in content
        # matches = list(re.finditer(pattern, content, re.DOTALL))

        # if matches:
        # self.message_helper.print_info(
        #    f"Found {len(matches)} code injection marker(s) to process"
        # )

        def replace_block(match):
            start_marker = match.group(1)
            end_marker = match.group(3)
            # existing_content = match.group(2)

            # Debug: show what's being replaced
            # if existing_content.strip():
            #    self.message_helper.print_info(
            #        f"Replacing existing code between markers (was {len(existing_content.splitlines())} lines)"
            #    )
            #    self._display_variables_summary()
            # else:
            #    self.message_helper.print_info(
            #        "Injecting code between empty markers"
            #    )

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

    def perform_code_replacements(
        self, content: str, replace_placeholders: bool = True, inject_code: bool = True
    ) -> str:
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

    def inject_for_development(self, target_file: Path = None) -> None:
        """Development workflow: inject code only (no placeholder replacement).

        This is optimized for the most common development workflow where you want
        to inject variable code between markers but keep placeholders for deployment.

        Args:
            target_file: Optional specific file to update. If None, updates all files.
        """
        return self.inject_variables_into_template(
            target_file=target_file, replace_placeholders=False, inject_code=True
        )

    def inject_for_deployment(
        self,
        output_dir: Path,
        preserve_structure: bool = True,
        target_file: Path = None,
    ) -> None:
        """Deployment workflow: full variable substitution with output directory.

        This performs both placeholder replacement and code injection, saving
        results to an output directory for deployment.

        Args:
            output_dir: Directory to save processed files
            preserve_structure: Whether to preserve directory structure (default: True)
            target_file: Optional specific file to update. If None, updates all files.
        """
        return self.inject_variables_into_template(
            target_file=target_file,
            output_dir=output_dir,
            preserve_structure=preserve_structure,
            replace_placeholders=True,
            inject_code=True,
        )

    def inject_python_libs_for_development(self) -> None:
        """Development workflow for python_libs: inject code only.

        Convenience method for the common python_libs development workflow.
        """
        return self.inject_variables_into_python_libs(replace_placeholders=False, inject_code=True)

    def inject_variables_into_template(
        self,
        target_file: Path = None,
        output_dir: Path = None,
        preserve_structure: bool = True,
        replace_placeholders: bool = False,  # Changed default to match common usage
        inject_code: bool = True,
    ) -> None:
        """Main function to inject variables into the template and replace variable placeholders.

        Args:
            target_file: Optional specific file to update. If None, updates all notebook-content files.
            output_dir: Optional directory to save updated files instead of modifying in place.
                       If None, files are updated in place for inject_code operations only.
                       replace_placeholders operations always require output_dir to avoid in-place changes.
            preserve_structure: When using output_dir, whether to preserve the directory structure
                              relative to project_path (default: True).
            replace_placeholders: Whether to replace {{varlib:...}} placeholders (default: False).
                                This default matches the most common development workflow.
            inject_code: Whether to inject code between markers (default: True).
        """

        # Validate that replace_placeholders operations have an output_dir
        if replace_placeholders and output_dir is None:
            raise ValueError(
                "replace_placeholders=True requires output_dir to be specified. In-place replacement of placeholders is not allowed."
            )

        # For inject_code only operations, in-place updates are allowed
        if inject_code and not replace_placeholders and output_dir is None:
            # This is allowed - inject_code can be done in-place
            pass

        # If a specific file is provided, update only that file
        if target_file:
            files_to_update = [target_file]
        else:
            # Find all notebook-content files and update them
            files_to_update = []
            workspace_items_path = self.project_path / Path("fabric_workspace_items")
            python_libs_path = Path("./ingen_fab") / Path("python_libs")

            if workspace_items_path.exists():
                for notebook_file in workspace_items_path.rglob("notebook-content.py"):
                    files_to_update.append(notebook_file)

            if python_libs_path.exists():
                for python_file in python_libs_path.rglob("*.py"):
                    files_to_update.append(python_file)

            if not files_to_update:
                self.message_helper.print_warning(f"No files found in {workspace_items_path}")
                return

        # Create output directory if specified
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            self.message_helper.print_info(f"Created output directory: {output_dir}")

        # Show operation summary with rich formatting
        mode = "In-place modification" if not output_dir else None
        self._create_operation_setup(
            title="Variable Library Injection",
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
            border_style="blue",
            files_count=len(files_to_update),
            mode=mode,
            output_dir=str(output_dir) if output_dir else None,
        )

        # Process files with progress bar
        updated_files = []
        stats = {
            "placeholders_replaced": 0,
            "code_injected": 0,
            "files_skipped": 0,
            "errors": 0,
        }

        # Use progress tracker utility
        for file_path in self.progress_tracker.simple_progress_tracker(
            files_to_update, "Processing files...", self.project_path
        ):
            # Process file and track result
            result = self._process_file(
                file_path=file_path,
                replace_placeholders=replace_placeholders,
                inject_code=inject_code,
                output_dir=output_dir,
                preserve_structure=preserve_structure,
                track_stats=True,
                stats=stats,
                python_libs_mode=False,
            )

            if result:
                updated_files.append(result)

        # Show final statistics
        self._display_processing_stats(stats, len(files_to_update), len(updated_files))

        # Report results with rich formatting
        if updated_files:
            if output_dir:
                self.message_helper.print_success(f"Saved {len(updated_files)} updated file(s) to {output_dir}")

                self.panel_builder.create_results_panel(
                    updated_files,
                    self.environment,
                    title_prefix="Variable Injection Results",
                    border_style="green",
                )
            else:
                if target_file:
                    self.message_helper.print_success(
                        f"Updated specific file with values from {self.environment} environment"
                    )
                else:
                    self.message_helper.print_success(
                        f"Updated {len(updated_files)} file(s) with values from {self.environment} environment"
                    )

                # Extract file paths from tuples
                file_paths = [file_tuple[0] for file_tuple in updated_files]
                self.panel_builder.create_file_list_panel(
                    file_paths,
                    self.environment,
                    title_prefix="Updated Files",
                    border_style="green",
                )
        else:
            if target_file:
                self.message_helper.print_info(f"No changes needed for target file: {target_file}")
            else:
                self.message_helper.print_info("No files with injection markers or placeholders found")

    def inject_variables_into_python_libs(self, replace_placeholders: bool = False, inject_code: bool = True) -> None:
        """Inject variables into python_libs files only (not notebooks).

        Args:
            replace_placeholders: Whether to replace {{varlib:...}} placeholders (default: False).
                                This default matches the most common development workflow.
            inject_code: Whether to inject code between markers (default: True).
        """
        # Display loaded variables summary
        if self.console and RICH_AVAILABLE:
            self._display_variables_summary()

        # Find python_libs directory
        python_libs_path = Path("ingen_fab") / Path("python_libs")

        if not python_libs_path.exists():
            self.message_helper.print_warning(f"Python libs directory not found: {python_libs_path}")
            return

        # Find all Python files in python_libs
        python_files = list(python_libs_path.rglob("*.py"))

        if not python_files:
            self.message_helper.print_warning("No Python files found in python_libs directory")
            return

        # Show operation summary with rich formatting
        self._create_operation_setup(
            title="Python Libraries Variable Injection",
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
            border_style="magenta",
            target_directory=str(python_libs_path),
            files_count=len(python_files),
            files_label="Python files found",
            mode="In-place modification",
        )

        # Process files with enhanced progress tracking
        updated_files = []
        stats = {
            "placeholders_replaced": 0,
            "code_injected": 0,
            "files_skipped": 0,
            "errors": 0,
        }

        # Use progress tracker utility
        for py_file in self.progress_tracker.simple_progress_tracker(
            python_files,
            "Processing Python libs...",
            None,  # No project path needed for python files
        ):
            # Process file
            result = self._process_python_lib_file_with_stats(py_file, replace_placeholders, inject_code, stats)
            if result:
                updated_files.append(result)

        # Show final statistics
        self._display_processing_stats(stats, len(python_files), len(updated_files))

        # Report results with rich formatting
        if updated_files:
            self.message_helper.print_success(
                f"Updated {len(updated_files)} python_libs file(s) with values from {self.environment} environment"
            )

            self.panel_builder.create_file_list_panel(
                updated_files,
                self.environment,
                title_prefix="Updated Python Libs Files",
                border_style="green",
            )
        else:
            self.message_helper.print_info("No python_libs files with injection markers or placeholders found")
