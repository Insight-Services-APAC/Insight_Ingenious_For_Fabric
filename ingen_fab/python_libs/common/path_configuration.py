"""
Centralized Path Configuration for Data Profiling Package

This module provides centralized path resolution to avoid hardcoded fallbacks
and improve maintainability across different environments.
"""

from pathlib import Path
from typing import Optional, List
import os


class PathConfiguration:
    """
    Centralized configuration for path resolution across the data profiling package.
    
    This class handles path discovery for templates, DDL scripts, and other resources
    in a consistent manner across development and production environments.
    """
    
    def __init__(self, package_name: str = "data_profiling"):
        """
        Initialize path configuration for a specific package.
        
        Args:
            package_name: Name of the package to configure paths for
        """
        self.package_name = package_name
        self._cache = {}
    
    def get_package_base_path(self) -> Path:
        """
        Get the base path for the package.
        
        Returns:
            Path to the package base directory
        """
        if 'package_base' in self._cache:
            return self._cache['package_base']
        
        # Try multiple strategies to find the package base
        strategies = [
            self._get_path_from_utils,
            self._get_path_from_environment,
            self._get_path_from_file_location,
        ]
        
        for strategy in strategies:
            path = strategy()
            if path and path.exists():
                self._cache['package_base'] = path
                return path
        
        raise FileNotFoundError(
            f"Could not locate package base directory for {self.package_name}"
        )
    
    def get_templates_dir(self) -> Path:
        """
        Get the templates directory for the package.
        
        Returns:
            Path to the templates directory
        """
        if 'templates_dir' in self._cache:
            return self._cache['templates_dir']
        
        package_base = self.get_package_base_path()
        templates_dir = package_base / "templates"
        
        if not templates_dir.exists():
            raise FileNotFoundError(
                f"Templates directory not found at {templates_dir}"
            )
        
        self._cache['templates_dir'] = templates_dir
        return templates_dir
    
    def get_ddl_scripts_dir(self) -> Path:
        """
        Get the DDL scripts directory for the package.
        
        Returns:
            Path to the DDL scripts directory
        """
        if 'ddl_scripts_dir' in self._cache:
            return self._cache['ddl_scripts_dir']
        
        package_base = self.get_package_base_path()
        ddl_scripts_dir = package_base / "ddl_scripts"
        
        if not ddl_scripts_dir.exists():
            raise FileNotFoundError(
                f"DDL scripts directory not found at {ddl_scripts_dir}"
            )
        
        self._cache['ddl_scripts_dir'] = ddl_scripts_dir
        return ddl_scripts_dir
    
    def get_unified_templates_dir(self) -> Optional[Path]:
        """
        Get the unified templates directory (shared across packages).
        
        Returns:
            Path to unified templates directory, or None if not found
        """
        if 'unified_templates_dir' in self._cache:
            return self._cache['unified_templates_dir']
        
        # Try to find unified templates
        try:
            from ingen_fab.python_libs.common.utils.path_utils import PathUtils
            unified_dir = PathUtils.get_package_resource_path("templates")
            if unified_dir.exists():
                self._cache['unified_templates_dir'] = unified_dir
                return unified_dir
        except (ImportError, FileNotFoundError):
            pass
        
        # Fallback to relative path
        package_base = self.get_package_base_path()
        unified_dir = package_base.parent.parent.parent / "templates"
        if unified_dir.exists():
            self._cache['unified_templates_dir'] = unified_dir
            return unified_dir
        
        return None
    
    def get_template_search_paths(self) -> List[Path]:
        """
        Get all template search paths in priority order.
        
        Returns:
            List of paths to search for templates
        """
        paths = [self.get_templates_dir()]
        
        unified_dir = self.get_unified_templates_dir()
        if unified_dir:
            paths.append(unified_dir)
        
        return paths
    
    def _get_path_from_utils(self) -> Optional[Path]:
        """Try to get path using PathUtils."""
        try:
            from ingen_fab.python_libs.common.utils.path_utils import PathUtils
            return PathUtils.get_package_resource_path(f"packages/{self.package_name}")
        except (ImportError, FileNotFoundError):
            return None
    
    def _get_path_from_environment(self) -> Optional[Path]:
        """Try to get path from environment variable."""
        env_var = f"INGEN_FAB_{self.package_name.upper()}_PATH"
        path_str = os.environ.get(env_var)
        if path_str:
            return Path(path_str)
        return None
    
    def _get_path_from_file_location(self) -> Optional[Path]:
        """Try to get path relative to this file's location."""
        # Navigate from python_libs/common to packages/data_profiling
        current_file = Path(__file__)
        packages_dir = current_file.parent.parent.parent / "packages"
        package_path = packages_dir / self.package_name
        
        if package_path.exists():
            return package_path
        return None
    
    def clear_cache(self) -> None:
        """Clear the internal path cache."""
        self._cache.clear()


# Singleton instance for default data_profiling package
_default_config = None


def get_default_path_config() -> PathConfiguration:
    """
    Get the default path configuration instance for data_profiling.
    
    Returns:
        Singleton PathConfiguration instance
    """
    global _default_config
    if _default_config is None:
        _default_config = PathConfiguration("data_profiling")
    return _default_config