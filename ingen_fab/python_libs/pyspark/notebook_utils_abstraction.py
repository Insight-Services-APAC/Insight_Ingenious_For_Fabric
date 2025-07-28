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
        
        return self._notebookutils.lakehouse.get(artifact_id)

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
    

class LocalNotebookUtils(NotebookUtilsInterface):
    """Local development notebook utilities implementation."""

    def __init__(self, connection_string: Optional[str] = None, spark_session_name: str = "spark"):
        self.connection_string = connection_string or self._get_default_connection_string()
        self.spark_session_name = spark_session_name
        self.spark_session = None
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
        if hasattr(obj, 'show') and hasattr(obj, 'count'):
            # Spark DataFrame (has both show and count methods)
            obj.show()
        elif hasattr(obj, 'head'):
            # Pandas DataFrame (has head method)
            print(obj.head())
        else:
            # Regular object
            print(obj)

    def exit_notebook(self, value: Any = None) -> None:
        """Exit the notebook (no-op in local environment)."""
        logger.info(f"Notebook exit requested with value: {value}")
        if value is not None:
            print(f"Notebook would exit with value: {value}")
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
    def mssparkutils(self) -> NotebookUtilsInterface.mssparkutils:
        """Return a dummy mssparkutils for local development."""
        class DummyMSSparkUtils:
            class notebook:
                @staticmethod
                def exit(value: Any = None) -> None:
                    """Exit method for local development."""
                    return value
                
                @staticmethod
                def run(name: Any = None, timeout: int = 60, params: dict = None) -> str:
                    """Run the notebook in value."""
                    print(f"DEBUG: Running notebook '{name}' with timeout {timeout} and params {params}")
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
                        # If workspace_repo_dir is a relative path and we're already inside it, resolve correctly
                        if not workspace_repo_dir.is_absolute():
                            workspace_repo_dir = Path(workspace_repo_dir).resolve()
                        workspace_items_path = workspace_repo_dir / "fabric_workspace_items"
                    
                    print(f"DEBUG: Searching for notebooks in: {workspace_items_path}")
                    pu = SyncToFabricEnvironment(str(workspace_items_path))
                    folders = pu.find_platform_folders(workspace_items_path)
                    
                    print(f"DEBUG: All folders found:")
                    for folder in folders:
                        print(f"DEBUG:   - {folder.name} at {folder.path}")
                    
                    # print each folder                    
                    for folder in folders:
                        # Check for exact match first, then with suffix
                        if folder.name == name + ".Notebook" or folder.name.startswith(name + "_") and folder.name.endswith(".Notebook"):
                            print(f"DEBUG: Found notebook folder: {folder.path}")
                            # Run the notebook-content.py file
                            notebook_content_path = Path(folder.path) / "notebook-content.py"
                            import importlib.util
                            spec = importlib.util.spec_from_file_location("notebook_content", notebook_content_path)
                            notebook_content = importlib.util.module_from_spec(spec)
                            try:
                                print(f"DEBUG: Executing notebook at: {notebook_content_path}")
                                result = spec.loader.exec_module(notebook_content)
                                print(f"DEBUG: Notebook executed successfully, returning 'success'")
                                return "success"  # Return success if notebook executes without error
                            except NotebookExit as e:
                                # Notebook called exit_notebook() - return the exit value
                                return e.exit_value
                            except Exception as e:
                                logger.error(f"Error executing notebook {name}: {e}")
                                return f"failed: {str(e)}"
                    
                    print(f"DEBUG: Notebook '{name}' not found")
                    return f"failed: notebook '{name}' not found"                          
                            
                        

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
    def create_instance(cls, force_local: bool = False, notebookutils: Optional[Any] = None, mssparkutils: Optional[Any] = None) -> NotebookUtilsInterface:
        """Create a new instance of notebook utils."""
        if force_local:
            logger.info("Creating local notebook utils instance (forced)")
            return LocalNotebookUtils()
        
        # Check if fabric_environment is set to local
        try:
            from ingen_fab.python_libs.common.config_utils import get_configs_as_object
            config = get_configs_as_object()
            if hasattr(config, 'fabric_environment') and config.fabric_environment == "local":
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