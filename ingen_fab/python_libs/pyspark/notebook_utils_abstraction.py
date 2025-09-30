"""
Abstraction layer for notebookutils to support both local and Fabric execution environments.

This module provides a unified interface for notebook utilities that can work in both
local development environments and Fabric notebook execution environments.
"""

import logging
from typing import Any, Optional

from ingen_fab.python_libs.common.notebook_utils_base import (
    LocalNotebookUtilsBase,
    NotebookUtilsFactoryBase,
)
from ingen_fab.python_libs.interfaces.notebook_utils_interface import (
    NotebookExit,
    NotebookUtilsInterface,
)

logger = logging.getLogger(__name__)


class FabricNotebookUtils(NotebookUtilsInterface):
    """Fabric notebook utilities implementation."""

    def __init__(self, notebookutils: Optional[Any] = None, mssparkutils: Optional[Any] = None):
        self._notebookutils = notebookutils
        self._mssparkutils = mssparkutils
        self._available = self._check_availability()

    def _check_availability(self) -> bool:
        """Check if notebookutils is available."""
        # If provided in constructor, use those
        if self._notebookutils is not None and self._mssparkutils is not None:
            self._notebook = self._mssparkutils.notebook
            return True

        # Otherwise try to import them
        try:
            import notebookutils  # type: ignore  # noqa: I001
            from notebookutils import mssparkutils  # type: ignore

            self._notebookutils = notebookutils
            self._mssparkutils = mssparkutils
            self._notebook = notebookutils.mssparkutils.notebook

            return True
        except ImportError:
            logger.debug("notebookutils not available - not running in Fabric environment")
            return False

    def connect_to_artifact(self, artifact_id: str, workspace_id: str) -> Any:
        """Connect to a Fabric artifact."""
        if not self._available:
            raise RuntimeError("notebookutils not available - cannot connect to artifact")

        return self._notebookutils.lakehouse.get(artifact_id)

    # def display(self, obj: Any) -> None:
    #    """Display an object in the notebook."""
    #    if not self._available:
    #        # Fallback to print for local development
    #        print(obj)
    #        return

    # Use the built-in display function in Fabric
    #    display(obj)  # type: ignore # noqa: F821

    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook with an optional return value."""
        if not self._available:
            print(f"Notebook would exit with value: {value}")
            raise NotebookExit(value or "success")

        self._mssparkutils.notebook.exit(value)

    def get_secret(self, secret_name: str, key_vault_name: str) -> str:
        """Get a secret from Azure Key Vault."""
        if not self._available:
            raise RuntimeError("mssparkutils not available - cannot access Key Vault")

        return self._mssparkutils.credentials.getSecret(key_vault_name, secret_name)

    def is_available(self) -> bool:
        """Check if Fabric notebook utils are available."""
        return self._available

    def run_notebook(self, notebook_name: str, timeout: int = 60, params: dict = None) -> str:
        """Run a notebook using notebookutils.mssparkutils.notebook.run."""
        if not self._available:
            raise RuntimeError("mssparkutils not available - cannot run notebook")

        return self._mssparkutils.notebook.run(notebook_name, timeout, params)


class LocalNotebookUtils(LocalNotebookUtilsBase):
    """Local development notebook utilities implementation for PySpark."""

    def __init__(self, connection_string: Optional[str] = None, spark_session_name: str = "spark"):
        super().__init__(connection_string)
        self.spark_session_name = spark_session_name
        self.spark_session = None

    def _get_spark_session(self):
        """Get the spark session from globals if available."""
        return globals().get(self.spark_session_name)

    def _create_lakehouse_path(self, lakehouse_name: str):
        """Create a lakehouse path for local development."""
        from pathlib import Path

        path = Path(f"/tmp/{lakehouse_name}")
        try:
            path.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            pass  # Directory already exists, which is fine
        return path

    def connect_to_artifact(self, artifact_id: str, workspace_id: str) -> Any:
        """Connect to a spark session instead of Fabric artifact."""
        spark_session = self._get_spark_session()
        if spark_session is None:
            raise RuntimeError("Spark session not available")
        logger.info(f"Connecting to spark session (artifact_id: {artifact_id}, workspace_id: {workspace_id})")
        return spark_session

    def display(self, obj: Any) -> None:
        """Display an object using appropriate method."""
        if hasattr(obj, "show") and hasattr(obj, "count"):
            # Spark DataFrame (has both show and count methods)
            obj.show()
        elif hasattr(obj, "head"):
            # Pandas DataFrame (has head method)
            print(obj.head())
        else:
            # Regular object
            print(obj)

    @property
    def mssparkutils(self) -> Any:
        """Return a dummy mssparkutils for local development."""

        class DummyMSSparkUtils:
            notebook = self._create_dummy_notebook_runner()

        return DummyMSSparkUtils()


class NotebookUtilsFactory(NotebookUtilsFactoryBase):
    """Factory for creating notebook utils instances."""

    @classmethod
    def get_instance(cls, force_local: bool = False) -> NotebookUtilsInterface:
        """Get a singleton instance of notebook utils."""
        if cls._instance is None:
            cls._instance = cls.create_instance(force_local=force_local)
        return cls._instance

    @classmethod
    def create_instance(
        cls,
        force_local: bool = False,
        notebookutils: Optional[Any] = None,
        mssparkutils: Optional[Any] = None,
    ) -> NotebookUtilsInterface:
        """Create a new instance of notebook utils."""
        if force_local:
            logger.info("Creating local notebook utils instance (forced)")
            return LocalNotebookUtils()

        # Check if fabric_environment is set to local
        try:
            from ingen_fab.python_libs.common.config_utils import get_configs_as_object

            config = get_configs_as_object()
            if hasattr(config, "fabric_environment") and config.fabric_environment == "local":
                logger.info("Creating local notebook utils instance (fabric_environment is local)")
                return LocalNotebookUtils()
        except ImportError:
            pass  # Continue with normal logic if config not available

        # Try Fabric first
        fabric_utils = FabricNotebookUtils(notebookutils, mssparkutils)
        if fabric_utils.is_available():
            logger.info("Creating Fabric notebook utils instance")
            return fabric_utils

        # Fallback to local
        logger.info("Creating local notebook utils instance (fallback)")
        return LocalNotebookUtils()
