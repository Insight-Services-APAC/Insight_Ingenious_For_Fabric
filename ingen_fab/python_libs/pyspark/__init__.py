"""PySpark utilities package for Fabric data processing and lakehouse operations."""

from __future__ import annotations

from .ddl_utils import ddl_utils
from .lakehouse_utils import lakehouse_utils
from .notebook_utils_abstraction import (
    FabricNotebookUtils,
    LocalNotebookUtils,
    NotebookUtilsFactory,
    NotebookUtilsInterface,
    connect_to_artifact,
    display,
    exit_notebook,
    get_notebook_utils,
    get_secret,
)
from .parquet_load_utils import testing_code_replacement

__all__ = [
    # Main utility classes
    "ddl_utils",
    "lakehouse_utils",
    # Notebook utilities
    "NotebookUtilsInterface",
    "FabricNotebookUtils", 
    "LocalNotebookUtils",
    "NotebookUtilsFactory",
    "get_notebook_utils",
    "connect_to_artifact",
    "display",
    "exit_notebook",
    "get_secret",
    # Parquet utilities
    "testing_code_replacement",
]
