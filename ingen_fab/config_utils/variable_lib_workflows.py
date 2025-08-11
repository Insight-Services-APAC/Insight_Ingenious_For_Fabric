"""
Specialized workflow classes for VariableLibraryUtils.

This module provides workflow-specific classes that pre-configure VariableLibraryUtils
for common usage patterns, making the intent clearer and reducing parameter complexity.
"""

from __future__ import annotations

from pathlib import Path

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils


class DevelopmentVariableInjector(VariableLibraryUtils):
    """Pre-configured for development workflow (inject code only, no placeholder replacement).

    This class is optimized for the most common development workflow where you want
    to inject variable code between markers but keep placeholders for deployment.

    Usage:
        injector = DevelopmentVariableInjector(project_path, environment)
        injector.inject_variables()  # Uses development defaults
    """

    def inject_variables(self, target_file: Path = None) -> None:
        """Inject variables using development workflow defaults.

        This method is pre-configured with:
        - replace_placeholders=False (keep placeholders for deployment)
        - inject_code=True (inject variable code between markers)
        - In-place modification (no output directory)

        Args:
            target_file: Optional specific file to update. If None, updates all files.
        """
        return self.inject_variables_into_template(
            target_file=target_file, replace_placeholders=False, inject_code=True
        )

    def inject_python_libs(self) -> None:
        """Inject variables into python_libs using development workflow defaults."""
        return self.inject_variables_into_python_libs(
            replace_placeholders=False, inject_code=True
        )


class DeploymentVariableInjector(VariableLibraryUtils):
    """Pre-configured for deployment workflow (full substitution with output directory).

    This class is optimized for deployment workflows where you want both
    placeholder replacement and code injection, with results saved to an output directory.

    Usage:
        injector = DeploymentVariableInjector(project_path, environment)
        injector.inject_variables(output_dir=Path("dist"))
    """

    def inject_variables(
        self,
        output_dir: Path,
        preserve_structure: bool = True,
        target_file: Path = None,
    ) -> None:
        """Inject variables using deployment workflow defaults.

        This method is pre-configured with:
        - replace_placeholders=True (substitute all placeholders)
        - inject_code=True (inject variable code between markers)
        - Requires output directory (deployment-ready files)

        Args:
            output_dir: Directory to save processed files (required)
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

    def inject_python_libs(self, output_dir: Path) -> None:
        """Inject variables into python_libs using deployment workflow defaults.

        Note: This creates a copy of python_libs in the output directory.
        For deployment, you may want to copy python_libs separately.

        Args:
            output_dir: Directory to save processed files
        """
        # For python_libs in deployment, we typically don't process them
        # as they are packaged separately. But if needed, this method provides the option.
        raise NotImplementedError(
            "Python libs injection for deployment is not typically needed. "
            "Python libs are usually packaged separately for deployment."
        )


class TestingVariableInjector(VariableLibraryUtils):
    """Pre-configured for testing workflow (code injection only, specific targeting).

    This class is optimized for testing workflows where you want to inject
    variables into specific files without modifying the entire project.

    Usage:
        injector = TestingVariableInjector(project_path, environment)
        injector.inject_into_file(test_file)
    """

    def inject_into_file(
        self,
        target_file: Path,
        replace_placeholders: bool = False,
        inject_code: bool = True,
    ) -> None:
        """Inject variables into a specific file for testing.

        Args:
            target_file: Specific file to update (required)
            replace_placeholders: Whether to replace placeholders (default: False)
            inject_code: Whether to inject code (default: True)
        """
        return self.inject_variables_into_template(
            target_file=target_file,
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
        )

    def inject_with_output(
        self,
        target_file: Path,
        output_dir: Path,
        replace_placeholders: bool = False,
        inject_code: bool = True,
    ) -> None:
        """Inject variables into a specific file with output directory for testing.

        Args:
            target_file: Specific file to update (required)
            output_dir: Directory to save processed file
            replace_placeholders: Whether to replace placeholders (default: False)
            inject_code: Whether to inject code (default: True)
        """
        return self.inject_variables_into_template(
            target_file=target_file,
            output_dir=output_dir,
            preserve_structure=False,  # Simpler structure for testing
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
        )


class ReadOnlyVariableLibrary(VariableLibraryUtils):
    """Read-only access to variable library (no injection capabilities).

    This class is optimized for cases where you only need to read variables
    without performing any injection operations. It provides a cleaner API
    for variable lookup scenarios.

    Usage:
        reader = ReadOnlyVariableLibrary(project_path, environment)
        workspace_id = reader.workspace_id
        value = reader.get_variable("config_lakehouse_id")
    """

    @property
    def workspace_id(self) -> str:
        """Get the workspace ID as a property for convenience."""
        return self.get_workspace_id()

    def get_variable(self, variable_name: str) -> str:
        """Shortened method name for variable lookup."""
        return self.get_variable_value(variable_name)

    def get_variables(self) -> dict[str, str]:
        """Get all variables as a dictionary."""
        return self.variables.copy()

    def has_variable(self, variable_name: str) -> bool:
        """Check if a variable exists in the library."""
        return variable_name in self.variables

    # Disable injection methods to prevent misuse
    def inject_variables_into_template(self, *args, **kwargs) -> None:
        """Disabled in read-only mode."""
        raise NotImplementedError(
            "Variable injection is disabled in ReadOnlyVariableLibrary. "
            "Use DevelopmentVariableInjector or DeploymentVariableInjector instead."
        )

    def inject_variables_into_python_libs(self, *args, **kwargs) -> None:
        """Disabled in read-only mode."""
        raise NotImplementedError(
            "Variable injection is disabled in ReadOnlyVariableLibrary. "
            "Use DevelopmentVariableInjector or DeploymentVariableInjector instead."
        )

    def inject_for_development(self, *args, **kwargs) -> None:
        """Disabled in read-only mode."""
        raise NotImplementedError(
            "Variable injection is disabled in ReadOnlyVariableLibrary. "
            "Use DevelopmentVariableInjector instead."
        )

    def inject_for_deployment(self, *args, **kwargs) -> None:
        """Disabled in read-only mode."""
        raise NotImplementedError(
            "Variable injection is disabled in ReadOnlyVariableLibrary. "
            "Use DeploymentVariableInjector instead."
        )

    def inject_python_libs_for_development(self, *args, **kwargs) -> None:
        """Disabled in read-only mode."""
        raise NotImplementedError(
            "Variable injection is disabled in ReadOnlyVariableLibrary. "
            "Use DevelopmentVariableInjector instead."
        )
