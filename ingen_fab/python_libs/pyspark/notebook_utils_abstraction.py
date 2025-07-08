"""
Abstraction layer for notebookutils to support both local and Fabric execution environments.

This module provides a unified interface for notebook utilities that can work in both
local development environments and Fabric notebook execution environments.
"""

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


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
    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook with an optional return value."""
        pass

    @abstractmethod
    def get_secret(self, secret_name: str, key_vault_name: str) -> str:
        """Get a secret from Azure Key Vault."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the notebook utils implementation is available."""
        pass

    class mssparkutils:
        """Abstraction layer for mssparkutils."""

        class notebook:
            """Abstraction for notebook exit functionality."""

            @staticmethod
            def exit(value: Any = None) -> None:
                """Exit the notebook with an optional return value."""
                raise NotImplementedError("This method should be implemented in the concrete class.")


class FabricNotebookUtils(NotebookUtilsInterface):
    """Fabric notebook utilities implementation."""

    def __init__(self):
        self._notebookutils = None
        self._mssparkutils = None
        self._available = self._check_availability()

    def _check_availability(self) -> bool:
        """Check if notebookutils is available."""
        try:
            import notebookutils # type: ignore  # noqa: I001
            from notebookutils import mssparkutils # type: ignore
            
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
        
        return self._notebookutils.data.connect_to_artifact(artifact_id, workspace_id)

    def display(self, obj: Any) -> None:
        """Display an object in the notebook."""
        if not self._available:
            # Fallback to print for local development
            print(obj)
            return
        
        # Use the built-in display function in Fabric
        display(obj)

    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook with an optional return value."""
        if not self._available:
            logger.warning("Notebook exit requested but not in Fabric environment")
            return
        
        self._mssparkutils.notebook.exit(value)

    def get_secret(self, secret_name: str, key_vault_name: str) -> str:
        """Get a secret from Azure Key Vault."""
        if not self._available:
            raise RuntimeError("notebookutils not available - cannot access Key Vault")
        
        return self._mssparkutils.credentials.getSecret(key_vault_name, secret_name)

    def is_available(self) -> bool:
        """Check if Fabric notebook utils are available."""
        return self._available
    
    


class LocalNotebookUtils(NotebookUtilsInterface):
    """Local development notebook utilities implementation."""

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or self._get_default_connection_string()
        self._secrets = self._load_local_secrets()

    def _get_default_connection_string(self) -> str:
        """Get default local SQL Server connection string."""
        password = os.getenv("SQL_SERVER_SA_PASSWORD", "YourStrong!Passw0rd")
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost,1433;"
            f"UID=sa;PWD={password};TrustServerCertificate=yes;"
        )

    def _load_local_secrets(self) -> Dict[str, str]:
        """Load secrets from environment variables for local development."""
        secrets = {}
        for key, value in os.environ.items():
            if key.startswith("SECRET_"):
                secret_name = key[7:]  # Remove "SECRET_" prefix
                secrets[secret_name] = value
        return secrets

    def connect_to_artifact(self, artifact_id: str, workspace_id: str) -> Any:
        """Connect to a local SQL Server instead of Fabric artifact."""
        try:
            import pyodbc
            logger.info(f"Connecting to local SQL Server (artifact_id: {artifact_id}, workspace_id: {workspace_id})")
            return pyodbc.connect(self.connection_string)
        except ImportError:
            raise RuntimeError("pyodbc not available - cannot connect to local SQL Server")

    def display(self, obj: Any) -> None:
        """Display an object using print."""
        if hasattr(obj, 'to_string'):
            print(obj.to_string())
        else:
            print(obj)

    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook (no-op in local environment)."""
        logger.info(f"Notebook exit requested with value: {value}")
        if value is not None:
            print(f"Notebook would exit with value: {value}")

    def get_secret(self, secret_name: str, key_vault_name: str) -> str:
        """Get a secret from environment variables."""
        secret_value = self._secrets.get(secret_name)
        if secret_value is None:
            # Try with key vault prefix
            env_key = f"SECRET_{secret_name.upper()}"
            secret_value = os.getenv(env_key)
        
        if secret_value is None:
            raise ValueError(f"Secret '{secret_name}' not found in environment variables")
        
        return secret_value

    def is_available(self) -> bool:
        """Local utils are always available."""
        return True
    
    @property
    def mssparkutils(self) -> NotebookUtilsInterface.mssparkutils:
        """Return a dummy mssparkutils for local development."""
        class DummyMSSparkUtils:
            class notebook:
                @staticmethod
                def exit(value: Any = None) -> None:
                    """Exit method for local development."""
                    return value
                
                @staticmethod
                def run(name: Any = None, timeout: int = 60) -> None:
                    """Run the notebook in value."""
                    # Search sample_project/fabric_workspace_items Find the directory of the notebook
                    from ingen_fab.fabric_cicd.promotion_utils import (
                        SyncToFabricEnvironment,
                    )
                    pu = SyncToFabricEnvironment("sample_project/fabric_workspace_items")
                    folders = pu.find_platform_folders(Path("sample_project/fabric_workspace_items"))
                    # print each folder                    
                    for folder in folders:
                        if folder.name == name + ".Notebook":
                            # Run the notebook-content.py file
                            notebook_content_path = Path(folder.path) / "notebook-content.py"
                            import importlib.util
                            spec = importlib.util.spec_from_file_location("notebook_content", notebook_content_path)
                            notebook_content = importlib.util.module_from_spec(spec)
                            result = spec.loader.exec_module(notebook_content)
                            return result                          
                            
                        

        return DummyMSSparkUtils()


class NotebookUtilsFactory:
    """Factory for creating notebook utils instances."""
    
    _instance: Optional[NotebookUtilsInterface] = None
    
    @classmethod
    def get_instance(cls, force_local: bool = False) -> NotebookUtilsInterface:
        """Get a singleton instance of notebook utils."""
        if cls._instance is None:
            cls._instance = cls.create_instance(force_local=force_local)
        return cls._instance
    
    @classmethod
    def create_instance(cls, force_local: bool = False) -> NotebookUtilsInterface:
        """Create a new instance of notebook utils."""
        if force_local:
            logger.info("Creating local notebook utils instance (forced)")
            return LocalNotebookUtils()
        
        # Try Fabric first
        fabric_utils = FabricNotebookUtils()
        if fabric_utils.is_available():
            logger.info("Creating Fabric notebook utils instance")
            return fabric_utils
        
        # Fallback to local
        logger.info("Creating local notebook utils instance (fallback)")
        return LocalNotebookUtils()
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None


# Convenience functions for backward compatibility
def get_notebook_utils(force_local: bool = False) -> NotebookUtilsInterface:
    """Get notebook utils instance."""
    return NotebookUtilsFactory.get_instance(force_local=force_local)


def connect_to_artifact(artifact_id: str, workspace_id: str) -> Any:
    """Connect to a Fabric artifact or local equivalent."""
    return get_notebook_utils().connect_to_artifact(artifact_id, workspace_id)


def display(obj: Any) -> None:
    """Display an object in the notebook."""
    return get_notebook_utils().display(obj)


def exit_notebook(value: Any = None) -> None:
    """Exit the notebook with an optional return value."""
    return get_notebook_utils().exit_notebook(value)


def get_secret(secret_name: str, key_vault_name: str) -> str:
    """Get a secret from Azure Key Vault or local environment."""
    return get_notebook_utils().get_secret(secret_name, key_vault_name)