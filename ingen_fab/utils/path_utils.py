"""
Path utility functions for handling portability across development and pip-installed environments.

This module provides centralized path resolution that works correctly whether the package
is installed via pip or running from source code in development.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Fallback for Python < 3.9
    import importlib_resources as pkg_resources


@dataclass
class WorkspaceContext:
    """Context object containing workspace configuration."""
    repo_dir: Path
    environment: str
    
    @classmethod
    def from_cli_or_env(cls, cli_repo_dir: Optional[str] = None, 
                       cli_env: Optional[str] = None) -> 'WorkspaceContext':
        """Create workspace context from CLI arguments or environment variables.
        
        Args:
            cli_repo_dir: Repository directory from CLI argument
            cli_env: Environment from CLI argument
            
        Returns:
            WorkspaceContext with resolved paths and environment
        """
        # Resolve repo directory
        if cli_repo_dir:
            repo_dir = Path(cli_repo_dir)
        else:
            env_repo_dir = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
            if env_repo_dir:
                repo_dir = Path(env_repo_dir)
            else:
                # Use current working directory as default
                repo_dir = Path.cwd()
        
        # Resolve environment
        environment = cli_env or os.environ.get("FABRIC_ENVIRONMENT", "development")
        
        return cls(repo_dir=repo_dir.resolve(), environment=environment)


class PathUtils:
    """Centralized path resolution utilities."""
    
    @staticmethod
    def get_package_resource_path(resource_path: str) -> Path:
        """Get path to package resources (templates, etc.) that works in both 
        development and pip-installed environments.
        
        Args:
            resource_path: Relative path within the ingen_fab package
            
        Returns:
            Absolute path to the resource
            
        Raises:
            FileNotFoundError: If resource cannot be found, with detailed search paths
            
        Example:
            get_package_resource_path("project_templates")
            get_package_resource_path("templates/ddl")
        """
        searched_paths = []
        is_dev = PathUtils.is_development_environment()
        
        if is_dev:
            # Development environment: prioritize current working directory
            dev_paths = [
                Path.cwd() / resource_path,  # Root level (for project_templates)
                Path.cwd() / "ingen_fab" / resource_path,  # Package level
            ]
            
            for dev_path in dev_paths:
                searched_paths.append(f"Development path: {dev_path}")
                if dev_path.exists():
                    return dev_path
                    
        else:
            # Pip-installed environment: prioritize package resources
            try:
                import ingen_fab
                package_root = pkg_resources.files(ingen_fab)
                resource_full_path = package_root / resource_path
                pip_path = Path(str(resource_full_path))
                searched_paths.append(f"Package resource: {pip_path}")
                
                # Check if the resource exists
                if hasattr(resource_full_path, 'exists') and resource_full_path.exists():
                    return pip_path
                elif hasattr(resource_full_path, 'is_file') and (
                    resource_full_path.is_file() or resource_full_path.is_dir()
                ):
                    return pip_path
                    
            except (ImportError, AttributeError, FileNotFoundError) as e:
                searched_paths.append(f"Package resource error: {e}")
        
        # Fallback: try alternate paths regardless of environment
        fallback_paths = [
            Path.cwd() / resource_path,
            Path.cwd() / "ingen_fab" / resource_path,
        ]
        
        # Add namespace extension paths (similar to ingenious pattern)
        namespace_paths = PathUtils._get_namespace_resource_paths(resource_path)
        fallback_paths.extend(namespace_paths)
        
        for fallback_path in fallback_paths:
            searched_paths.append(f"Fallback path: {fallback_path}")
            if fallback_path.exists():
                return fallback_path
            
        # Create detailed error message with all search paths
        search_details = "\n".join(f"  - {path}" for path in searched_paths)
        raise FileNotFoundError(
            f"Could not locate package resource: {resource_path}\n"
            f"Searched the following paths:\n{search_details}\n"
            f"Current working directory: {Path.cwd()}\n"
            f"Development environment detected: {is_dev}"
        )
    
    @staticmethod
    def get_workspace_repo_dir() -> Path:
        """Get the user workspace repository directory from environment or current directory.
        
        Returns:
            Path to the workspace repository directory
        """
        env_val = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
        if env_val:
            return Path(env_val).resolve()
        
        # Default to current working directory
        return Path.cwd()
    
    @staticmethod
    def get_template_path(template_type: str, template_name: str = "") -> Path:
        """Get path to templates with unified discovery across all template types.
        
        Args:
            template_type: Type of template ('ddl', 'notebooks', 'sql_fabric', 'sql_server', 'project')
            template_name: Optional specific template name
            
        Returns:
            Path to template directory or specific template file
            
        Raises:
            ValueError: If template_type is unknown
            FileNotFoundError: If template path cannot be found, with search details
        """
        template_mappings = {
            'ddl': 'templates/ddl',
            'notebooks': 'templates/notebooks',
            'sql_fabric': 'python_libs/python/sql_template_factory/fabric',
            'sql_server': 'python_libs/python/sql_template_factory/sql_server',
            'project': 'project_templates'
        }
        
        if template_type not in template_mappings:
            raise ValueError(f"Unknown template type: {template_type}. "
                           f"Valid types: {list(template_mappings.keys())}")
        
        try:
            base_path = PathUtils.get_package_resource_path(template_mappings[template_type])
        except FileNotFoundError as e:
            # Re-raise with template type context
            raise FileNotFoundError(
                f"Could not locate {template_type} templates.\n"
                f"Template type '{template_type}' maps to: {template_mappings[template_type]}\n"
                f"Original error:\n{str(e)}"
            )
        
        if template_name:
            template_file = base_path / template_name
            if not template_file.exists():
                # List available templates for better error message
                try:
                    available = [f.name for f in base_path.iterdir() if f.is_file()]
                    available_msg = f"Available templates: {', '.join(available[:10])}"
                    if len(available) > 10:
                        available_msg += f" ... and {len(available) - 10} more"
                except Exception:
                    available_msg = "Could not list available templates"
                
                raise FileNotFoundError(
                    f"Template file not found: {template_name}\n"
                    f"Searched in: {base_path}\n"
                    f"{available_msg}"
                )
            return template_file
        
        return base_path
    
    @staticmethod
    def ensure_workspace_structure(workspace_dir: Path) -> None:
        """Ensure the workspace directory has the expected structure.
        
        Args:
            workspace_dir: Path to workspace directory to validate/create
        """
        required_dirs = [
            "fabric_workspace_items",
            "ddl_scripts",
            "ddl_scripts/Lakehouses", 
            "ddl_scripts/Warehouses"
        ]
        
        for dir_name in required_dirs:
            (workspace_dir / dir_name).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def is_development_environment() -> bool:
        """Check if we're running in a development environment (source code checkout).
        
        Uses similar logic to ingenious imports.py for consistent environment detection.
        
        Returns:
            True if running from development environment, False if pip-installed
        """
        try:
            cwd = Path.cwd()
            
            # Primary development indicators (similar to ingenious pattern)
            dev_indicators = [
                cwd / "ingen_fab" / "cli.py",  # Main CLI entry point
                cwd / "pyproject.toml",  # Python project file
                cwd / "ingen_fab" / "project_templates",  # Our templates directory
                cwd / ".git",  # Git repository
                cwd / "README.md",  # Project documentation
            ]
            
            # Check if we're in the package source structure
            source_structure = [
                cwd / "ingen_fab" / "__init__.py",
                cwd / "ingen_fab" / "utils",
                cwd / "ingen_fab" / "python_libs",
            ]
            
            # Development environment if we find key indicators
            has_dev_indicators = any(indicator.exists() for indicator in dev_indicators)
            has_source_structure = any(path.exists() for path in source_structure)
            
            return has_dev_indicators or has_source_structure
            
        except Exception:
            return False
    
    @staticmethod
    def _get_namespace_resource_paths(resource_path: str) -> List[Path]:
        """Get potential namespace-based resource paths (similar to ingenious extensions pattern).
        
        Args:
            resource_path: Resource path to search for
            
        Returns:
            List of potential paths in namespace hierarchy
        """
        paths = []
        cwd = Path.cwd()
        
        # Define namespaces in priority order (similar to ingenious pattern)
        namespaces = [
            "ingen_fab_extensions",  # Extension namespace
            "ingen_fab",  # Base namespace
        ]
        
        for namespace in namespaces:
            # Try direct namespace paths
            paths.append(cwd / namespace / resource_path)
            
            # Try with subdirectory structure
            if "/" not in resource_path:
                paths.append(cwd / namespace / "templates" / resource_path)
                paths.append(cwd / namespace / "resources" / resource_path)
        
        return paths


# Legacy compatibility - will be deprecated
def get_fabric_workspace_repo_dir() -> Path:
    """DEPRECATED: Use PathUtils.get_workspace_repo_dir() instead."""
    import warnings
    warnings.warn(
        "get_fabric_workspace_repo_dir() is deprecated. Use PathUtils.get_workspace_repo_dir()",
        DeprecationWarning,
        stacklevel=2
    )
    return PathUtils.get_workspace_repo_dir()