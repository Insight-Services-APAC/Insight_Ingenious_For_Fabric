from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml


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
