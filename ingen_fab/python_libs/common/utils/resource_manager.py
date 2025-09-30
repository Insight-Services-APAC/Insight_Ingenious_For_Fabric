"""
Resource manager for template discovery and caching.

This module provides centralized resource management for templates, SQL files,
and other package resources with caching for performance.
"""

from pathlib import Path
from typing import Dict, List

from jinja2 import Environment, FileSystemLoader, Template

from ingen_fab.python_libs.common.utils.path_utils import PathUtils


class ResourceManager:
    """Manages package resources with caching and unified template discovery."""

    def __init__(self):
        """Initialize the resource manager with empty caches."""
        self._template_cache: Dict[str, Template] = {}
        self._resource_paths: Dict[str, Path] = {}
        self._jinja_envs: Dict[str, Environment] = {}

    def get_ddl_templates(self) -> Dict[str, Path]:
        """Get all DDL template files.

        Returns:
            Dictionary mapping template names to file paths
        """
        cache_key = "ddl_templates"
        if cache_key not in self._resource_paths:
            try:
                ddl_template_dir = PathUtils.get_template_path("ddl")
                templates = {}

                if ddl_template_dir.exists():
                    for template_file in ddl_template_dir.rglob("*.jinja*"):
                        template_name = template_file.stem
                        templates[template_name] = template_file

                self._resource_paths[cache_key] = templates
            except Exception:
                self._resource_paths[cache_key] = {}

        return self._resource_paths[cache_key]

    def get_sql_templates(self, db_type: str) -> Dict[str, Path]:
        """Get SQL template files for a specific database type.

        Args:
            db_type: Database type ('fabric' or 'sql_server')

        Returns:
            Dictionary mapping template names to file paths
        """
        if db_type not in ["fabric", "sql_server"]:
            raise ValueError(f"Unsupported database type: {db_type}")

        cache_key = f"sql_templates_{db_type}"
        if cache_key not in self._resource_paths:
            try:
                template_type = f"sql_{db_type}"
                sql_template_dir = PathUtils.get_template_path(template_type)
                templates = {}

                if sql_template_dir.exists():
                    for template_file in sql_template_dir.glob("*.sql.jinja"):
                        # Remove .sql.jinja extension for template name
                        template_name = template_file.name.replace(".sql.jinja", "")
                        templates[template_name] = template_file

                self._resource_paths[cache_key] = templates
            except Exception:
                self._resource_paths[cache_key] = {}

        return self._resource_paths[cache_key]

    def get_project_templates_dir(self) -> Path:
        """Get the project templates directory.

        Returns:
            Path to project templates directory
        """
        cache_key = "project_templates_dir"
        if cache_key not in self._resource_paths:
            try:
                templates_dir = PathUtils.get_template_path("project")
                self._resource_paths[cache_key] = templates_dir
            except Exception:
                # Fallback to current directory for development
                fallback_path = Path.cwd() / "project_templates"
                self._resource_paths[cache_key] = fallback_path

        return self._resource_paths[cache_key]

    def get_jinja_environment(self, template_type: str) -> Environment:
        """Get a Jinja2 environment for a specific template type.

        Args:
            template_type: Type of templates ('ddl', 'notebooks', 'sql_fabric', 'sql_server')

        Returns:
            Configured Jinja2 Environment
        """
        if template_type not in self._jinja_envs:
            try:
                template_dir = PathUtils.get_template_path(template_type)
                loader = FileSystemLoader(str(template_dir))
                env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)

                # Add custom filters that templates might need
                def required_filter(value, var_name=""):
                    """Jinja2 filter: raises an error if value is not provided or is falsy."""
                    if value is None or (hasattr(value, "__len__") and len(value) == 0):
                        raise ValueError(f"Required variable '{var_name}' is missing or empty")
                    return value

                env.filters["required"] = required_filter
                self._jinja_envs[template_type] = env
            except Exception:
                # Fallback to empty environment
                env = Environment()
                self._jinja_envs[template_type] = env

        return self._jinja_envs[template_type]

    def load_jinja_template(self, template_type: str, template_name: str) -> Template:
        """Load a Jinja2 template with caching.

        Args:
            template_type: Type of template ('ddl', 'notebooks', 'sql_fabric', 'sql_server')
            template_name: Name of the template file (without extension)

        Returns:
            Loaded Jinja2 Template object
        """
        cache_key = f"{template_type}:{template_name}"

        if cache_key not in self._template_cache:
            env = self.get_jinja_environment(template_type)

            # Try different extensions
            extensions = [".jinja", ".jinja2", ".sql.jinja", ".py.jinja"]
            template = None

            for ext in extensions:
                try:
                    template_filename = f"{template_name}{ext}"
                    template = env.get_template(template_filename)
                    break
                except Exception:
                    continue

            if template is None:
                # Get template directory for better error message
                try:
                    template_dir = PathUtils.get_template_path(template_type)
                    search_info = f"Searched in: {template_dir}"

                    # List available templates
                    try:
                        available = []
                        for f in template_dir.rglob("*.jinja*"):
                            # For .sql.jinja files, remove .sql.jinja to get template name
                            if f.name.endswith(".sql.jinja"):
                                template_name = f.name.replace(".sql.jinja", "")
                            else:
                                # For other .jinja files, just remove .jinja
                                template_name = f.stem
                            available.append(template_name)

                        if available:
                            available_msg = f"Available templates: {', '.join(available[:10])}"
                            if len(available) > 10:
                                available_msg += f" ... and {len(available) - 10} more"
                        else:
                            available_msg = "No templates found in directory"
                    except Exception:
                        available_msg = "Could not list available templates"

                except Exception as e:
                    search_info = f"Template directory lookup failed: {e}"
                    available_msg = ""

                error_msg = f"Template not found: {template_name} in {template_type} templates\n{search_info}"
                if available_msg:
                    error_msg += f"\n{available_msg}"

                raise FileNotFoundError(error_msg)

            self._template_cache[cache_key] = template

        return self._template_cache[cache_key]

    def list_templates(self, template_type: str) -> List[str]:
        """List all available templates of a specific type.

        Args:
            template_type: Type of templates to list

        Returns:
            List of template names (without extensions)
        """
        try:
            if template_type.startswith("sql_"):
                db_type = template_type.replace("sql_", "")
                templates = self.get_sql_templates(db_type)
            elif template_type == "ddl":
                templates = self.get_ddl_templates()
            else:
                # For other types, scan directory
                template_dir = PathUtils.get_template_path(template_type)
                templates = {}

                if template_dir.exists():
                    for template_file in template_dir.rglob("*.jinja*"):
                        template_name = template_file.stem
                        templates[template_name] = template_file

            return list(templates.keys())

        except Exception:
            return []

    def template_exists(self, template_type: str, template_name: str) -> bool:
        """Check if a template exists.

        Args:
            template_type: Type of template
            template_name: Name of the template

        Returns:
            True if template exists, False otherwise
        """
        try:
            self.load_jinja_template(template_type, template_name)
            return True
        except Exception:
            return False

    def clear_cache(self) -> None:
        """Clear all cached templates and paths."""
        self._template_cache.clear()
        self._resource_paths.clear()
        self._jinja_envs.clear()


# Global instance for convenience
_resource_manager = ResourceManager()


# Convenience functions
def get_ddl_templates() -> Dict[str, Path]:
    """Get all DDL template files."""
    return _resource_manager.get_ddl_templates()


def get_sql_templates(db_type: str) -> Dict[str, Path]:
    """Get SQL template files for a specific database type."""
    return _resource_manager.get_sql_templates(db_type)


def load_template(template_type: str, template_name: str) -> Template:
    """Load a Jinja2 template."""
    return _resource_manager.load_jinja_template(template_type, template_name)


def list_templates(template_type: str) -> List[str]:
    """List all available templates of a specific type."""
    return _resource_manager.list_templates(template_type)
