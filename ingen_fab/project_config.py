from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
from dataclasses import dataclass


@dataclass
class Workspace:
    workspace_name: str
    environment_name: str


@dataclass
class FunctionalTests:
    target_workspace_name: str
    target_warehouse_name: str


@dataclass
class ProjectConfig:
    workspaces: List[Workspace]
    functional_tests: FunctionalTests

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProjectConfig':
        """Create ProjectConfig from dictionary loaded from YAML."""
        workspaces = [
            Workspace(
                workspace_name=w['workspace_name'],
                environment_name=w['environment_name']
            )
            for w in data.get('workspaces', [])
        ]

        functional_tests_data = data.get('functional_tests', {})
        functional_tests = FunctionalTests(
            target_workspace_name=functional_tests_data.get('target_workspace_name', ''),
            target_warehouse_name=functional_tests_data.get('target_warehouse_name', '')
        )

        return cls(workspaces=workspaces, functional_tests=functional_tests)


def load_project_config(config_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Load project configuration from YAML.

    The search order is:
    1. The ``config_dir`` parameter if provided.
    2. The path specified in the ``IGEN_FAB_CONFIG`` environment variable.
    3. ``project.yml`` in the current working directory.
    Returns an empty dictionary if no configuration file is found.
    """
    search_paths = []
    if config_dir:
        search_paths.append(Path(config_dir))
    env_path = os.getenv("IGEN_FAB_CONFIG")
    if env_path:
        search_paths.append(Path(env_path))
    search_paths.append(Path.cwd())

    for path in search_paths:
        config_file = path
        if config_file.is_dir():
            config_file = config_file / "project.yml"
        if config_file.is_file():
            with config_file.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    return {}


def load_project_config_object(config_dir: Optional[Path] = None) -> ProjectConfig:
    """Load project configuration as a typed Python object."""
    config_dict = load_project_config(config_dir)
    return ProjectConfig.from_dict(config_dict)
