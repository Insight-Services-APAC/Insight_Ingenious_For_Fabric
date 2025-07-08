"""
OneLake utilities for interacting with Microsoft Fabric OneLake storage using Azure Storage SDK.
"""

from pathlib import Path
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
from ingen_fab.fabric_api.utils import FabricApiUtils


class OneLakeUtils:
    """
    Utility class for interacting with Microsoft Fabric OneLake storage using Azure Storage SDK.
    """

    def __init__(
        self, 
        environment: str, 
        project_path: Path, 
        *, 
        credential: Optional[DefaultAzureCredential] = None
    ) -> None:
        """
        Initialize OneLakeUtils.

        Args:
            environment: Environment name (e.g., 'development', 'production')
            project_path: Path to the project directory
            credential: Azure credential (defaults to DefaultAzureCredential)
        """
        self.environment = environment
        self.project_path = project_path
        self.credential = credential or DefaultAzureCredential()
        self.onelake_base_url = "https://onelake.dfs.fabric.microsoft.com"
        
        # Get workspace ID from variable library
        self.vlu = VariableLibraryUtils(environment=self.environment, project_path=self.project_path)
        self.workspace_id = self.vlu.get_workspace_id()
        
        # Initialize Fabric API utils for name resolution
        self.fabric_api = FabricApiUtils(environment=self.environment, project_path=self.project_path, credential=self.credential)

    def get_config_lakehouse_id(self) -> str:
        """
        Get the config lakehouse ID from the variable library.

        Returns:
            The config lakehouse ID
        """
        vlu = VariableLibraryUtils(environment=self.environment, project_path=self.project_path)
        return vlu.get_variable_value("config_lakehouse_id")

    def _get_datalake_service_client(self) -> DataLakeServiceClient:
        """
        Get a DataLakeServiceClient for OneLake.

        Returns:
            DataLakeServiceClient instance
        """
        return DataLakeServiceClient(account_url=self.onelake_base_url, credential=self.credential)

    def _get_workspace_name(self) -> str:
        """
        Get the workspace name from its ID.

        Returns:
            The workspace name
        """
        workspace_name = self.fabric_api.get_workspace_name_from_id(self.workspace_id)
        if workspace_name is None:
            raise ValueError(f"Could not find workspace name for ID: {self.workspace_id}")
        return workspace_name

    def _get_lakehouse_name(self, lakehouse_id: str) -> str:
        """
        Get the lakehouse name from its ID.

        Args:
            lakehouse_id: The lakehouse ID

        Returns:
            The lakehouse name
        """
        lakehouse_name = self.fabric_api.get_lakehouse_name_from_id(self.workspace_id, lakehouse_id)
        if lakehouse_name is None:
            raise ValueError(f"Could not find lakehouse name for ID: {lakehouse_id} in workspace: {self.workspace_id}")
        return lakehouse_name

    def upload_file_to_lakehouse(
        self, 
        lakehouse_id: str, 
        file_path: str, 
        target_path: str = None,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None
    ) -> dict:
        """
        Upload a file to a lakehouse's Files section using Azure Storage SDK.

        Args:
            lakehouse_id: ID of the target lakehouse
            file_path: Local path to the file to upload
            target_path: Path in the lakehouse (defaults to filename)

        Returns:
            Dictionary with upload result information
        """
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if target_path is None:
            target_path = file_path_obj.name

        try:
            # Use provided clients or create new ones
            if service_client is None:
                service_client = self._get_datalake_service_client()
            
            if file_system_client is None:
                workspace_name = self._get_workspace_name()
                file_system_client = service_client.get_file_system_client(workspace_name)
            
            # Get lakehouse name and construct the full path: {lakehouse_name}.Lakehouse/Files/{target_path}
            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            full_target_path = f"{lakehouse_name}.Lakehouse/Files/{target_path}".replace("\\", "/")
            
            # Get the file client
            file_client = file_system_client.get_file_client(full_target_path)
            
            print(f"Uploading {file_path} to OneLake://{self.workspace_id}/{full_target_path}")
            
            # Upload the file
            file_data = ""
            with open(file_path_obj, 'r', encoding='utf-8') as f:
               file_data = f.read()

            file_data = self.vlu.perform_code_replacements(file_data)

            # Convert to bytes and get correct byte length
            file_data_bytes = file_data.encode('utf-8')
            
            file_client.upload_data(
                data=file_data_bytes,
                overwrite=True,
                length=len(file_data_bytes)
            )
            
            print(f"Successfully uploaded {file_path} to OneLake")
            return {
                "success": True,
                "local_path": str(file_path),
                "remote_path": full_target_path,
                "lakehouse_id": lakehouse_id
            }
            
        except Exception as e:
            error_msg = f"Failed to upload {file_path}: {str(e)}"
            print(error_msg)
            return {
                "success": False,
                "local_path": str(file_path),
                "remote_path": target_path,
                "error": error_msg
            }

    def upload_directory_to_lakehouse(
        self,
        lakehouse_id: str,
        directory_path: str,
        target_prefix: str = "",
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
        max_workers: int = 8,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        include_extensions: Optional[list[str]] = None
    ) -> dict:
        """
        Upload all files in a directory to a lakehouse's Files section using parallel uploads.

        Args:
            lakehouse_id: ID of the target lakehouse
            directory_path: Local directory path to upload
            target_prefix: Prefix for remote paths (optional)
            max_workers: Number of parallel uploads (default: 8)

        Returns:
            Dictionary with upload results
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        dir_path = Path(directory_path)
        if not dir_path.exists() or not dir_path.is_dir():
            raise ValueError(f"Directory not found: {directory_path}")

        upload_results = {
            "successful": [],
            "failed": [],
            "total_files": 0
        }

        # Find all files recursively, filtering out __ directories and by extension if specified
        all_files = []
        for file_path in dir_path.rglob("*"):
            if file_path.is_file():
                path_parts = file_path.relative_to(dir_path).parts
                if any(part.startswith("__") for part in path_parts):
                    continue
                if include_extensions is not None:
                    if not any(str(file_path).lower().endswith(ext.lower()) for ext in include_extensions):
                        continue
                all_files.append(file_path)

        upload_results["total_files"] = len(all_files)
        print(f"Found {len(all_files)} files to upload from {directory_path}")

        # Create clients once for efficiency if not provided
        if service_client is None:
            service_client = self._get_datalake_service_client()
        if file_system_client is None:
            workspace_name = self._get_workspace_name()
            file_system_client = service_client.get_file_system_client(workspace_name)

        import time

        def upload_one(file_path: Path) -> dict:
            relative_path = file_path.relative_to(dir_path)
            if target_prefix:
                target_path = f"{target_prefix}/{relative_path}".replace("\\", "/")
            else:
                target_path = str(relative_path).replace("\\", "/")

            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return self.upload_file_to_lakehouse(
                        lakehouse_id,
                        str(file_path),
                        target_path,
                        service_client=service_client,
                        file_system_client=file_system_client
                    )
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        sleep_time = backoff_factor * (2 ** (attempt - 1))
                        print(f"Retry {attempt} for {file_path} after error: {e}. Backing off {sleep_time:.2f}s...")
                        time.sleep(sleep_time)
            return {
                "success": False,
                "local_path": str(file_path),
                "remote_path": str(relative_path),
                "error": str(last_exception)
            }

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(upload_one, file_path): file_path for file_path in all_files}
            for future in as_completed(future_to_file):
                result = future.result()
                if result["success"]:
                    upload_results["successful"].append(result)
                else:
                    upload_results["failed"].append(result)
                    print(f"Failed to upload {result['local_path']}: {result.get('error')}")

        print(f"Upload completed: {len(upload_results['successful'])} successful, {len(upload_results['failed'])} failed")
        return upload_results

    def upload_python_libs_to_config_lakehouse(self, python_libs_path: str = None) -> dict:
        """
        Upload the python_libs directory to the config lakehouse's Files section.

        Args:
            python_libs_path: Path to the python_libs directory (defaults to standard location)

        Returns:
            Dictionary with upload results
        """
        if python_libs_path is None:
            # Default to the standard python_libs location
            current_dir = Path(__file__).parent
            python_libs_path = current_dir.parent / "python_libs"

        python_libs_path = Path(python_libs_path)
        
        if not python_libs_path.exists():
            raise ValueError(f"Python libs directory not found: {python_libs_path}")

        # Get config lakehouse ID
        config_lakehouse_id = self.get_config_lakehouse_id()
        
        print(f"Uploading python_libs from: {python_libs_path}")
        print(f"Target config lakehouse ID: {config_lakehouse_id}")
        
        # Upload all files with "python_libs" prefix
        return self.upload_directory_to_lakehouse(
            lakehouse_id=config_lakehouse_id,
            directory_path=str(python_libs_path), 
            target_prefix="ingen_fab/python_libs",
            service_client=self._get_datalake_service_client(),
            include_extensions=[".py"]
        )

    def list_lakehouse_files(
        self, 
        lakehouse_id: str, 
        path: str = "",
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None
    ) -> list:
        """
        List files in a lakehouse's Files section using Azure Storage SDK.

        Args:
            lakehouse_id: ID of the lakehouse
            path: Path within the Files section (optional)

        Returns:
            List of file paths
        """
        try:
            # Use provided clients or create new ones
            if service_client is None:
                service_client = self._get_datalake_service_client()
            
            if file_system_client is None:
                workspace_name = self._get_workspace_name()
                file_system_client = service_client.get_file_system_client(workspace_name)
            
            # Get lakehouse name and construct the base path: {lakehouse_name}.Lakehouse/Files/{path}
            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            base_path = f"{lakehouse_name}.Lakehouse/Files"
            if path:
                base_path = f"{base_path}/{path}"
            
            # List files in the specified path
            file_paths = []
            paths = file_system_client.get_paths(path=base_path, recursive=True)
            
            for path_item in paths:
                if not path_item.is_directory:  # Only include files, not directories
                    file_paths.append(path_item.name)
            
            return file_paths
            
        except Exception as e:
            print(f"Failed to list lakehouse files: {str(e)}")
            return []

    def delete_lakehouse_file(
        self, 
        lakehouse_id: str, 
        file_path: str,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None
    ) -> bool:
        """
        Delete a file from a lakehouse's Files section using Azure Storage SDK.

        Args:
            lakehouse_id: ID of the lakehouse
            file_path: Path to the file within the Files section

        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            # Use provided clients or create new ones
            if service_client is None:
                service_client = self._get_datalake_service_client()
            
            if file_system_client is None:
                workspace_name = self._get_workspace_name()
                file_system_client = service_client.get_file_system_client(workspace_name)
            
            # Get lakehouse name and construct the full path: {lakehouse_name}.Lakehouse/Files/{file_path}
            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            full_file_path = f"{lakehouse_name}.Lakehouse/Files/{file_path}".replace("\\", "/")
            
            # Get the file client and delete
            file_client = file_system_client.get_file_client(full_file_path)
            file_client.delete_file()
            
            print(f"Successfully deleted {file_path} from lakehouse")
            return True
            
        except Exception as e:
            print(f"Failed to delete {file_path}: {str(e)}")
            return False

    def clean_python_libs_files_from_config_lakehouse(
        self,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None
    ) -> dict:
        """
        Delete all python_libs files from the config lakehouse's Files section.

        Returns:
            Dictionary with deletion results
        """
        config_lakehouse_id = self.get_config_lakehouse_id()
        
        try:
            # Create clients once for efficiency if not provided
            if service_client is None:
                service_client = self._get_datalake_service_client()
            if file_system_client is None:
                workspace_name = self._get_workspace_name()
                file_system_client = service_client.get_file_system_client(workspace_name)
            
            # List files in python_libs directory
            file_paths = self.list_lakehouse_files(
                config_lakehouse_id, 
                "python_libs",
                service_client=service_client,
                file_system_client=file_system_client
            )
            
            deleted_count = 0
            errors = []
            
            print(f"Found {len(file_paths)} files in python_libs directory to delete")
            
            # Delete each file
            for file_path in file_paths:
                # Extract the relative path from the full OneLake path
                if "/Files/" in file_path:
                    relative_path = file_path.split("/Files/", 1)[1]
                else:
                    relative_path = file_path
                    
                if self.delete_lakehouse_file(
                    config_lakehouse_id, 
                    relative_path,
                    service_client=service_client,
                    file_system_client=file_system_client
                ):
                    deleted_count += 1
                else:
                    errors.append(f"Failed to delete {relative_path}")
            
            return {
                "deleted_count": deleted_count,
                "errors": errors,
                "lakehouse_id": config_lakehouse_id,
                "total_found": len(file_paths)
            }
            
        except Exception as e:
            return {
                "deleted_count": 0,
                "errors": [str(e)],
                "lakehouse_id": config_lakehouse_id,
                "total_found": 0
            }