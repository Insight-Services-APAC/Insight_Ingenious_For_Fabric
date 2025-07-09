"""
Abstraction layer for notebookutils to support both local and Fabric execution environments.

This module provides a unified interface for notebook utilities that can work in both
local development environments and Fabric notebook execution environments.
"""

import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ingen_fab.python_libs.common.config_utils import get_configs_as_object

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


class FabricNotebookUtils(NotebookUtilsInterface):
    """Fabric notebook utilities implementation."""

    def __init__(self, notebookutils: Optional[Any] = None):
        self._notebookutils = notebookutils
        self._mssparkutils = None # Not used in python implementation, but can be added if needed
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
        logging.info(f"Connecting to artifact (artifact_id: {artifact_id}, workspace_id: {workspace_id}) using Fabric notebookutils")
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
        
        pass # Not available in Python implementation, but can be added if needed

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
            # Try SQL_SERVER_PASSWORD first, then fall back to SQL_SERVER_SA_PASSWORD, then default
            password = os.getenv("SQL_SERVER_PASSWORD") or os.getenv("SQL_SERVER_SA_PASSWORD") or "YourStrong!Passw0rd"
            connection_string = (
                "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;"
                + f"PWD={password};TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(connection_string)
            logger.debug("Connected to local SQL Server instance.")
            return conn
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


class NotebookUtilsFactory:
    """Factory for creating notebook utils instances."""
    
    _instance: Optional[NotebookUtilsInterface] = None
    
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
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None
