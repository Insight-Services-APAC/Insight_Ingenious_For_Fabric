"""
Base classes and common functionality for notebook utilities.

This module provides shared implementations used by both python and pyspark notebook utilities.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from ingen_fab.python_libs.common.utils.path_utils import PathUtils
from ingen_fab.python_libs.interfaces.notebook_utils_interface import (
    NotebookExit,
    NotebookUtilsInterface,
)

logger = logging.getLogger(__name__)


class LocalNotebookUtilsBase(NotebookUtilsInterface):
    """Base class for local development notebook utilities implementation."""

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

    def _create_dummy_notebook_runner(self) -> Any:
        """Create the notebook runner implementation shared between python and pyspark."""

        class DummyNotebookRunner:
            @staticmethod
            def exit(value: Any = None) -> Any:
                """Exit method for local development."""
                print(f"Notebook would exit with value: {value}")
                raise NotebookExit(value or "success")

            @staticmethod
            def run(name: str, timeoutSeconds: int = 60, arguements: dict = None) -> str:
                """Run the notebook in value."""
                # Search sample_project/fabric_workspace_items Find the directory of the notebook
                # Find the current working directory and navigate to the fabric_workspace_items root

                from ingen_fab.fabric_cicd.promotion_utils import (
                    SyncToFabricEnvironment,
                )

                workspace_repo_dir = PathUtils.get_workspace_repo_dir()
                workspace_items_path = workspace_repo_dir / "fabric_workspace_items"

                print(f"DEBUG: Searching for notebooks in: {workspace_items_path}")
                pu = SyncToFabricEnvironment(str(workspace_items_path))
                folders = pu.find_platform_folders(workspace_items_path, adjust_paths=False)

                # print each folder
                for folder in folders:
                    # Check for exact match first, then with suffix
                    if (
                        folder.name == name + ".Notebook"
                        or folder.name.startswith(name + "_")
                        and folder.name.endswith(".Notebook")
                    ):
                        # Run the notebook-content.py file
                        notebook_content_path = Path(folder.path) / "notebook-content.py"
                        import importlib.util

                        spec = importlib.util.spec_from_file_location("notebook_content", notebook_content_path)
                        notebook_content = importlib.util.module_from_spec(spec)
                        try:
                            result = spec.loader.exec_module(notebook_content)
                            if result == "success":
                                return "success"  # Return success if notebook executes without error
                            else:
                                return f"failed: {result}"
                        except NotebookExit as e:
                            # Notebook called exit_notebook() - return the exit value
                            return e.exit_value
                        except Exception as e:
                            logger.error(f"Error executing notebook {name}: {e}")
                            return f"failed: {str(e)}"

                print(f"DEBUG: Notebook '{name}' not found")
                return f"failed: notebook '{name}' not found"

        return DummyNotebookRunner


class NotebookUtilsFactoryBase:
    """Base factory for creating notebook utils instances."""

    _instance: Optional[NotebookUtilsInterface] = None

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None
