"""
Abstraction layer for notebookutils to support both local and Fabric execution environments.

This module provides a unified interface for notebook utilities that can work in both
local development environments and Fabric notebook execution environments.
"""

import logging
from typing import Any, Optional

from ingen_fab.python_libs.common.config_utils import get_configs_as_object
from ingen_fab.python_libs.common.notebook_utils_base import (
    LocalNotebookUtilsBase,
    NotebookUtilsFactoryBase,
)
from ingen_fab.python_libs.interfaces.notebook_utils_interface import (
    NotebookUtilsInterface,
)

logger = logging.getLogger(__name__)


class FabricNotebookUtils(NotebookUtilsInterface):
    """Fabric notebook utilities implementation."""

    def __init__(self, notebookutils: Optional[Any] = None):
        self._notebookutils = notebookutils
        self._mssparkutils = None  # Not used in python implementation, but can be added if needed
        self._available = self._check_availability()

    def _check_availability(self) -> bool:
        """Check if notebookutils is available."""
        if get_configs_as_object().fabric_environment == "local":
            logger.debug("Running in local environment - notebookutils not available")
            return False
        else:
            return True

    def connect_to_artifact(self, artifact_id: str, workspace_id: str) -> Any:
        """Connect to a Fabric artifact."""
        if not self._available:
            raise RuntimeError("notebookutils not available - cannot connect to artifact")
        logging.info(
            f"Connecting to artifact (artifact_id: {artifact_id}, workspace_id: {workspace_id}) using Fabric notebookutils"
        )
        return self._notebookutils.data.connect_to_artifact(artifact_id, workspace_id)

    def display(self, obj: Any) -> None:
        """Display an object in the notebook."""
        if not self._available:
            # Fallback to print for local development
            print(obj)
            return

        # Use the built-in display function in Fabric
        self._fabric_display(obj)

    def _fabric_display(self, obj: Any) -> None:
        """Display function that calls Fabric's native display."""
        display(obj)  # type: ignore # noqa: F821

    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook with an optional return value."""
        if not self._available:
            logger.warning("Notebook exit requested but not in Fabric environment")
            return

        pass  # Not available in Python implementation, but can be added if needed

    def get_secret(self, secret_name: str, key_vault_name: str) -> str:
        """Get a secret from Azure Key Vault."""
        if not self._available:
            raise RuntimeError("notebookutils not available - cannot access Key Vault")

        return self._mssparkutils.credentials.getSecret(key_vault_name, secret_name)

    def is_available(self) -> bool:
        """Check if Fabric notebook utils are available."""
        return self._available

    def run_notebook(self, notebook_name: str, timeout: int = 60, params: dict = None) -> str:
        """Run a notebook using notebookutils.notebook.run."""
        if not self._available:
            raise RuntimeError("notebookutils not available - cannot run notebook")

        return self._notebookutils.notebook.run(notebook_name, timeout, params)


class LocalNotebookUtils(LocalNotebookUtilsBase):
    """Local development notebook utilities implementation for Python."""

    def connect_to_artifact(self, artifact_id: str, workspace_id: str) -> Any:
        """Connect to a local SQL Server instead of Fabric artifact."""
        try:
            return self._connect_to_local_sql_server()
        except ImportError:
            raise RuntimeError("pyodbc not available - cannot connect to local SQL Server")

    def display(self, obj: Any) -> None:
        """Display an object using print."""
        if hasattr(obj, "to_string"):
            print(obj.to_string())
        else:
            print(obj)

    @property
    def mssparkutils(self):
        """Return a dummy mssparkutils for local development."""

        class DummyMSSparkUtils:
            notebook = self._create_dummy_notebook_runner()

        return DummyMSSparkUtils()


class NotebookUtilsFactory(NotebookUtilsFactoryBase):
    """Factory for creating notebook utils instances."""

    @classmethod
    def get_instance(cls, notebookutils: Optional[Any] = None, force_local: bool = False) -> NotebookUtilsInterface:
        """Get a singleton instance of notebook utils."""
        if cls._instance is None:
            cls._instance = cls.create_instance(notebookutils=notebookutils, force_local=force_local)
        return cls._instance

    @classmethod
    def create_instance(cls, notebookutils: Optional[Any] = None, force_local: bool = False) -> NotebookUtilsInterface:
        """Create a new instance of notebook utils."""
        if force_local:
            logger.info("Creating local notebook utils instance (forced)")
            return LocalNotebookUtils()

        if notebookutils:
            logger.info("Creating Fabric notebook utils instance")
            return FabricNotebookUtils(notebookutils=notebookutils)
        else:
            # Fallback to local
            logger.info("Creating local notebook utils instance (fallback)")
            return LocalNotebookUtils()
