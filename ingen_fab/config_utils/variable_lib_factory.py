"""
Factory functions and utilities for VariableLibraryUtils.

This module provides convenient factory functions for common instantiation patterns
and utilities to reduce boilerplate code across the codebase.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
from ingen_fab.config_utils.variable_lib_workflows import (
    DevelopmentVariableInjector,
    DeploymentVariableInjector,
    ReadOnlyVariableLibrary,
    TestingVariableInjector,
)


class VariableLibraryCache:
    """Cache for VariableLibraryUtils instances to reduce repeated instantiations."""

    _instances: Dict[Tuple[str, str], VariableLibraryUtils] = {}

    @classmethod
    def get_instance(cls, project_path: Path, environment: str) -> VariableLibraryUtils:
        """Get cached instance or create new one.

        Args:
            project_path: Path to the project root
            environment: Environment name

        Returns:
            Cached or new VariableLibraryUtils instance
        """
        key = (str(project_path), environment)
        if key not in cls._instances:
            cls._instances[key] = VariableLibraryUtils(
                project_path=project_path, environment=environment
            )
        return cls._instances[key]

    @classmethod
    def clear_cache(cls) -> None:
        """Clear all cached instances."""
        cls._instances.clear()

    @classmethod
    def cache_size(cls) -> int:
        """Get the number of cached instances."""
        return len(cls._instances)


class VariableLibraryFactory:
    """Factory class for creating VariableLibraryUtils instances with common patterns."""

    @staticmethod
    def from_cli_context(ctx, use_cache: bool = True) -> VariableLibraryUtils:
        """Create VariableLibraryUtils from CLI context.

        Args:
            ctx: Click context object with fabric_environment and fabric_workspace_repo_dir
            use_cache: Whether to use cached instances (default: True)

        Returns:
            Configured VariableLibraryUtils instance
        """
        environment = ctx.obj.get("fabric_environment")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir"))

        if use_cache:
            return VariableLibraryCache.get_instance(project_path, environment)
        else:
            return VariableLibraryUtils(
                environment=environment, project_path=project_path
            )

    @staticmethod
    def from_environment_and_path(
        environment: str, project_path: Path, use_cache: bool = True
    ) -> VariableLibraryUtils:
        """Create VariableLibraryUtils with explicit environment and path.

        Args:
            environment: Environment name (e.g., 'development', 'production')
            project_path: Path to the project root
            use_cache: Whether to use cached instances (default: True)

        Returns:
            Configured VariableLibraryUtils instance
        """
        if use_cache:
            return VariableLibraryCache.get_instance(project_path, environment)
        else:
            return VariableLibraryUtils(
                environment=environment, project_path=project_path
            )

    @staticmethod
    def from_instance_vars(instance, use_cache: bool = True) -> VariableLibraryUtils:
        """Create VariableLibraryUtils from an object with environment and project_path attributes.

        Args:
            instance: Object with .environment and .project_path attributes
            use_cache: Whether to use cached instances (default: True)

        Returns:
            Configured VariableLibraryUtils instance
        """
        if use_cache:
            return VariableLibraryCache.get_instance(
                instance.project_path, instance.environment
            )
        else:
            return VariableLibraryUtils(
                environment=instance.environment, project_path=instance.project_path
            )

    # Workflow-specific factory methods

    @staticmethod
    def for_development(
        environment: str, project_path: Path, use_cache: bool = True
    ) -> DevelopmentVariableInjector:
        """Create a DevelopmentVariableInjector instance.

        Args:
            environment: Environment name
            project_path: Path to the project root
            use_cache: Whether to use cached instances (default: True)

        Returns:
            DevelopmentVariableInjector instance
        """
        if use_cache:
            # Check if we have a cached base instance we can reuse
            base_instance = VariableLibraryCache.get_instance(project_path, environment)
            # Create development injector with same variables
            dev_injector = DevelopmentVariableInjector(project_path, environment)
            dev_injector.variables = base_instance.variables
            return dev_injector
        else:
            return DevelopmentVariableInjector(project_path, environment)

    @staticmethod
    def for_deployment(
        environment: str, project_path: Path, use_cache: bool = True
    ) -> DeploymentVariableInjector:
        """Create a DeploymentVariableInjector instance.

        Args:
            environment: Environment name
            project_path: Path to the project root
            use_cache: Whether to use cached instances (default: True)

        Returns:
            DeploymentVariableInjector instance
        """
        if use_cache:
            # Check if we have a cached base instance we can reuse
            base_instance = VariableLibraryCache.get_instance(project_path, environment)
            # Create deployment injector with same variables
            deploy_injector = DeploymentVariableInjector(project_path, environment)
            deploy_injector.variables = base_instance.variables
            return deploy_injector
        else:
            return DeploymentVariableInjector(project_path, environment)

    @staticmethod
    def for_testing(
        environment: str, project_path: Path, use_cache: bool = True
    ) -> TestingVariableInjector:
        """Create a TestingVariableInjector instance.

        Args:
            environment: Environment name
            project_path: Path to the project root
            use_cache: Whether to use cached instances (default: True)

        Returns:
            TestingVariableInjector instance
        """
        if use_cache:
            # Check if we have a cached base instance we can reuse
            base_instance = VariableLibraryCache.get_instance(project_path, environment)
            # Create testing injector with same variables
            test_injector = TestingVariableInjector(project_path, environment)
            test_injector.variables = base_instance.variables
            return test_injector
        else:
            return TestingVariableInjector(project_path, environment)

    @staticmethod
    def for_readonly(
        environment: str, project_path: Path, use_cache: bool = True
    ) -> ReadOnlyVariableLibrary:
        """Create a ReadOnlyVariableLibrary instance.

        Args:
            environment: Environment name
            project_path: Path to the project root
            use_cache: Whether to use cached instances (default: True)

        Returns:
            ReadOnlyVariableLibrary instance
        """
        if use_cache:
            # Check if we have a cached base instance we can reuse
            base_instance = VariableLibraryCache.get_instance(project_path, environment)
            # Create readonly library with same variables
            readonly_lib = ReadOnlyVariableLibrary(project_path, environment)
            readonly_lib.variables = base_instance.variables
            return readonly_lib
        else:
            return ReadOnlyVariableLibrary(project_path, environment)

    @staticmethod
    def for_development_from_cli(
        ctx, use_cache: bool = True
    ) -> DevelopmentVariableInjector:
        """Create DevelopmentVariableInjector from CLI context."""
        environment = ctx.obj.get("fabric_environment")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir"))
        return VariableLibraryFactory.for_development(
            environment, project_path, use_cache
        )

    @staticmethod
    def for_deployment_from_cli(
        ctx, use_cache: bool = True
    ) -> DeploymentVariableInjector:
        """Create DeploymentVariableInjector from CLI context."""
        environment = ctx.obj.get("fabric_environment")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir"))
        return VariableLibraryFactory.for_deployment(
            environment, project_path, use_cache
        )

    @staticmethod
    def for_readonly_from_cli(ctx, use_cache: bool = True) -> ReadOnlyVariableLibrary:
        """Create ReadOnlyVariableLibrary from CLI context."""
        environment = ctx.obj.get("fabric_environment")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir"))
        return VariableLibraryFactory.for_readonly(environment, project_path, use_cache)


# Convenience functions for common single-operation patterns
def get_workspace_id_from_cli(ctx) -> str:
    """Quick workspace ID lookup from CLI context.

    Args:
        ctx: Click context object

    Returns:
        Workspace ID from variable library
    """
    return VariableLibraryFactory.from_cli_context(ctx).get_workspace_id()


def get_variable_from_cli(ctx, variable_name: str) -> str:
    """Quick variable lookup from CLI context.

    Args:
        ctx: Click context object
        variable_name: Name of the variable to retrieve

    Returns:
        Variable value from variable library
    """
    return VariableLibraryFactory.from_cli_context(ctx).get_variable_value(
        variable_name
    )


def get_workspace_id_from_environment(environment: str, project_path: Path) -> str:
    """Quick workspace ID lookup from environment and path.

    Args:
        environment: Environment name
        project_path: Path to the project root

    Returns:
        Workspace ID from variable library
    """
    return VariableLibraryFactory.from_environment_and_path(
        environment, project_path
    ).get_workspace_id()


def get_variable_from_environment(
    environment: str, project_path: Path, variable_name: str
) -> str:
    """Quick variable lookup from environment and path.

    Args:
        environment: Environment name
        project_path: Path to the project root
        variable_name: Name of the variable to retrieve

    Returns:
        Variable value from variable library
    """
    return VariableLibraryFactory.from_environment_and_path(
        environment, project_path
    ).get_variable_value(variable_name)


def process_file_content(
    file_path: Path,
    varlib_utils: VariableLibraryUtils,
    replace_placeholders: bool = False,
    inject_code: bool = True,
) -> bool:
    """Process a single file with variable injection.

    This utility function encapsulates the common read/process/write pattern
    found throughout the codebase.

    Args:
        file_path: Path to the file to process
        varlib_utils: VariableLibraryUtils instance to use
        replace_placeholders: Whether to replace {{varlib:...}} placeholders
        inject_code: Whether to inject code between markers

    Returns:
        True if file was modified, False otherwise

    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading or writing the file
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        # Read file content
        with open(file_path, "r", encoding="utf-8") as f:
            original_content = f.read()

        # Process content
        updated_content = varlib_utils.perform_code_replacements(
            original_content,
            replace_placeholders=replace_placeholders,
            inject_code=inject_code,
        )

        # Write back if changed
        if updated_content != original_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(updated_content)
            return True

        return False

    except Exception as e:
        raise IOError(f"Error processing file {file_path}: {e}") from e


def process_file_content_from_cli(
    ctx, file_path: Path, replace_placeholders: bool = False, inject_code: bool = True
) -> bool:
    """Process a single file with variable injection using CLI context.

    Args:
        ctx: Click context object
        file_path: Path to the file to process
        replace_placeholders: Whether to replace {{varlib:...}} placeholders
        inject_code: Whether to inject code between markers

    Returns:
        True if file was modified, False otherwise
    """
    varlib_utils = VariableLibraryFactory.from_cli_context(ctx)
    return process_file_content(
        file_path, varlib_utils, replace_placeholders, inject_code
    )
