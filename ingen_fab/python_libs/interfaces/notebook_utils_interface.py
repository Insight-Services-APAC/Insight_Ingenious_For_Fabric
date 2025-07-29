"""
Abstract interface for notebook utilities.

This module provides a unified interface for notebook utilities that can work in both
local development environments and Fabric notebook execution environments.
"""

from abc import ABC, abstractmethod
from typing import Any


class NotebookExit(Exception):
    """Exception raised when a notebook calls exit_notebook() in local environment."""

    def __init__(self, exit_value: Any = "success"):
        self.exit_value = exit_value
        super().__init__(f"Notebook exit with value: {exit_value}")


class NotebookUtilsInterface(ABC):
    """Abstract interface for notebook utilities."""

    @abstractmethod
    def connect_to_artifact(self, artifact_id: str, workspace_id: str) -> Any:
        """Connect to a Fabric artifact (warehouse/lakehouse)."""
        pass

    @abstractmethod
    def display(self, obj: Any) -> None:
        """Display an object in the notebook."""
        pass

    @abstractmethod
    def get_secret(self, secret_name: str, key_vault_name: str) -> str:
        """Get a secret from Azure Key Vault."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the notebook utils implementation is available."""
        pass

