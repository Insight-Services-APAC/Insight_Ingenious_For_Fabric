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

from ingen_fab.python_libs.common.config_utils import get_configs_as_object

logger = logging.getLogger(__name__)


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

    @abstractmethod
    def run_notebook(self, notebook_name: str, timeout: int = 60, params: dict = None) -> str:
        """Run a notebook and return the result."""
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

    def run_notebook(self, notebook_name: str, timeout: int = 60, params: dict = None) -> str:
        """Run a notebook using notebookutils.notebook.run."""
        if not self._available:
            raise RuntimeError("notebookutils not available - cannot run notebook")
        
        return self._notebookutils.notebook.run(notebook_name, timeout, params)


class LocalNotebookUtils(NotebookUtilsInterface):
    """Local development notebook utilities implementation."""

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or self._get_default_connection_string()
        self._secrets = self._load_local_secrets()

    def _get_default_connection_string(self) -> str:
        """Get default local SQL Server connection string."""
        password = os.getenv("SQL_SERVER_PASSWORD", "YourStrong!Passw0rd")
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
            return self._connect_to_local_sql_server()
        except ImportError:
            raise RuntimeError("pyodbc not available - cannot connect to local SQL Server")
        
    def display(self, obj: Any) -> None:
        """Display an object using print."""
        if hasattr(obj, 'to_string'):
            print(obj.to_string())
        else:
            print(obj)

    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook by raising a special exception with the exit value."""
        logger.info(f"Notebook exit requested with value: {value}")
        if value is not None:
            print(f"Notebook would exit with value: {value}")
        
        # Raise a special exception that will be caught by the notebook runner
        # to return the proper exit value
        raise NotebookExit(value or "success")

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

    def run_notebook(self, notebook_name: str, timeout: int = 60, params: dict = None) -> str:
        """Run a notebook by executing its Python file directly."""
        return self.mssparkutils.notebook.run(notebook_name, timeout, params)
    
    @property
    def mssparkutils(self):
        """Return a dummy mssparkutils for local development."""
        class DummyMSSparkUtils:
            class notebook:
                @staticmethod
                def exit(value: Any = None) -> None:
                    """Exit method for local development."""
                    return value
                
                @staticmethod
                def run(name: Any = None, timeout: int = 60, params: dict = None) -> None:
                    """Run the notebook in value."""
                    # Search sample_project/fabric_workspace_items Find the directory of the notebook
                    from ingen_fab.fabric_cicd.promotion_utils import (
                        SyncToFabricEnvironment,
                    )
                    # Find the current working directory and navigate to the fabric_workspace_items root
                    import os
                    current_dir = Path(os.getcwd())
                    
                    # Navigate up to find fabric_workspace_items
                    workspace_items_path = current_dir
                    while workspace_items_path.name != "fabric_workspace_items" and workspace_items_path.parent != workspace_items_path:
                        workspace_items_path = workspace_items_path.parent
                    
                    if workspace_items_path.name != "fabric_workspace_items":
                        # Fallback using the path utilities
                        from ingen_fab.utils.path_utils import PathUtils
                        workspace_repo_dir = PathUtils.get_workspace_repo_dir()
                        workspace_items_path = workspace_repo_dir / "fabric_workspace_items"
                    
                    print(f"DEBUG: Searching for notebooks in: {workspace_items_path}")
                    pu = SyncToFabricEnvironment(str(workspace_items_path))
                    folders = pu.find_platform_folders(workspace_items_path)
                    # print each folder                    
                    for folder in folders:
                        # Check for exact match first, then with suffix
                        if folder.name == name + ".Notebook" or folder.name.startswith(name + "_") and folder.name.endswith(".Notebook"):
                            # Run the notebook-content.py file
                            notebook_content_path = Path(folder.path) / "notebook-content.py"
                            import importlib.util
                            spec = importlib.util.spec_from_file_location("notebook_content", notebook_content_path)
                            notebook_content = importlib.util.module_from_spec(spec)
                            try:
                                result = spec.loader.exec_module(notebook_content)
                                return "success"  # Return success if notebook executes without error
                            except NotebookExit as e:
                                # Notebook called exit_notebook() - return the exit value
                                return e.exit_value
                            except Exception as e:
                                logger.error(f"Error executing notebook {name}: {e}")
                                return f"failed: {str(e)}"                          
                            
                        

        return DummyMSSparkUtils()


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
