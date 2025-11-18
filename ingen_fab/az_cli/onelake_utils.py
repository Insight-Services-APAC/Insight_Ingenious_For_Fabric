"""
OneLake utilities for interacting with Microsoft Fabric OneLake storage using Azure Storage SDK.
"""

import logging
from pathlib import Path
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from ingen_fab.cli_utils.console_styles import MessageHelpers
from ingen_fab.cli_utils.progress_utils import ProgressTracker
from ingen_fab.config_utils.variable_lib_factory import (
    VariableLibraryFactory,
    get_variable_from_environment,
    get_workspace_id_from_environment,
)
from ingen_fab.fabric_api.utils import FabricApiUtils

import os

try:
    from rich.console import Console

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Suppress verbose Azure SDK logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.WARNING
)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("azure.storage").setLevel(logging.WARNING)


class OneLakeUtils:
    """
    Utility class for interacting with Microsoft Fabric OneLake storage using Azure Storage SDK.
    """

    def __init__(
        self,
        environment: str,
        project_path: Path,
        *,
        credential: Optional[DefaultAzureCredential] = None,
        console: Optional[Console] = None,
    ) -> None:
        """
        Initialize OneLakeUtils.

        Args:
            environment: Environment name (e.g., 'development', 'production')
            project_path: Path to the project directory
            credential: Azure credential (defaults to DefaultAzureCredential)
            console: Rich console instance for formatted output
        """
        self.environment = environment
        self.project_path = project_path
        self.credential = credential or DefaultAzureCredential()
        self.onelake_base_url = "https://onelake.dfs.fabric.microsoft.com"

        # Initialize console utilities
        self.console = (
            console
            if console and RICH_AVAILABLE
            else (Console() if RICH_AVAILABLE else None)
        )
        self.msg_helper = MessageHelpers(self.console)
        self.progress_tracker = ProgressTracker(self.console)

        # Get workspace ID from variable library
        self.vlu = VariableLibraryFactory.from_environment_and_path(
            self.environment, self.project_path
        )
        self.workspace_id = get_workspace_id_from_environment(
            self.environment, self.project_path
        )

        # Initialize Fabric API utils for name resolution
        self.fabric_api = FabricApiUtils(
            environment=self.environment,
            project_path=self.project_path,
            credential=self.credential,
        )

        self.workspace_name = self._get_workspace_name()
        self.lakehouses = dict[str, str]()

    def get_config_lakehouse_id(self) -> Optional[str]:
        """
        Get the config lakehouse ID from the variable library.

        Returns:
            The config lakehouse ID
        """
        config_lakehouse_name = get_variable_from_environment(
            self.environment, self.project_path, "config_lakehouse_name"
        )

        return self.fabric_api.get_lakehouse_id_from_name(
            self.workspace_id, config_lakehouse_name
        )

    def _get_datalake_service_client(self) -> DataLakeServiceClient:
        """
        Get a DataLakeServiceClient for OneLake.

        Returns:
            DataLakeServiceClient instance
        """
        return DataLakeServiceClient(
            account_url=self.onelake_base_url, credential=self.credential
        )

    def _get_workspace_name(self) -> str:
        """
        Get the workspace name from its ID.

        Returns:
            The workspace name
        """
        workspace_name = self.fabric_api.get_workspace_name_from_id(self.workspace_id)
        if workspace_name is None:
            raise ValueError(
                f"Could not find workspace name for ID: {self.workspace_id}"
            )
        return workspace_name

    def _get_lakehouse_name(self, lakehouse_id: str) -> str:
        """
        Get the lakehouse name from its ID.

        Args:
            lakehouse_id: The lakehouse ID

        Returns:
            The lakehouse name
        """
        lakehouse_name = None
        for lakehouse_name, lakehouse_id_ in self.lakehouses.items():
            if lakehouse_id_ == lakehouse_id:
                return lakehouse_name

        if lakehouse_name is None:
            # If not found in cache, fetch from Fabric API
            lakehouse_name = self.fabric_api.get_lakehouse_name_from_id(
                self.workspace_id, lakehouse_id
            )

            if lakehouse_name is not None:
                self.lakehouses[lakehouse_name] = lakehouse_id
                return lakehouse_name

        # If still not found, raise an error
        if lakehouse_name is None:
            raise ValueError(
                f"Could not find lakehouse name for ID: {lakehouse_id} in workspace: {self.workspace_id}"
            )
        return lakehouse_name

    def _list_remote_directory_files(
        self,
        lakehouse_id: str,
        target_prefix: str,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
    ) -> set[str]:
        """
        List all files in a remote directory and return their relative paths.

        Args:
            lakehouse_id: ID of the lakehouse
            target_prefix: Directory path within the Files section
            service_client: Optional service client to reuse
            file_system_client: Optional file system client to reuse

        Returns:
            Set of relative file paths (relative to target_prefix)
        """
        try:
            if service_client is None:
                service_client = self._get_datalake_service_client()

            if file_system_client is None:
                file_system_client = service_client.get_file_system_client(
                    self.workspace_name
                )

            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            base_path = f"{lakehouse_name}.Lakehouse/Files/{target_prefix}".replace(
                "\\", "/"
            )

            remote_files = set()
            paths = file_system_client.get_paths(path=base_path, recursive=True)

            for path_item in paths:
                if not path_item.is_directory:
                    # Extract relative path from full OneLake path
                    if "/Files/" in path_item.name:
                        files_part = path_item.name.split("/Files/", 1)[1]
                        if target_prefix and files_part.startswith(target_prefix + "/"):
                            relative_path = files_part[len(target_prefix) + 1 :]
                            remote_files.add(relative_path)

            return remote_files

        except Exception as e:
            # If directory doesn't exist, return empty set (expected for first upload)
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg:
                return set()
            # For other errors, log warning but continue
            if self.console:
                self.msg_helper.print_warning(
                    f"Could not list remote directory '{target_prefix}': {str(e)}"
                )
            return set()

    def _compute_sync_deletions(
        self, local_files: set[str], remote_files: set[str]
    ) -> set[str]:
        """
        Compute which remote files should be deleted to match local directory.

        Args:
            local_files: Set of local file paths (relative to source directory)
            remote_files: Set of remote file paths (relative to target prefix)

        Returns:
            Set of file paths that exist remotely but not locally
        """
        return remote_files - local_files

    def _delete_remote_files_parallel(
        self,
        lakehouse_id: str,
        files_to_delete: set[str],
        target_prefix: str,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
        max_workers: int = 8,
    ) -> dict:
        """
        Delete multiple files from lakehouse in parallel with progress tracking.

        Args:
            lakehouse_id: ID of the lakehouse
            files_to_delete: Set of relative file paths to delete
            target_prefix: Directory prefix for the files
            service_client: Optional service client to reuse
            file_system_client: Optional file system client to reuse
            max_workers: Number of parallel deletions

        Returns:
            Dictionary with deletion results (deleted count, failed count, errors)
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not files_to_delete:
            return {"deleted": 0, "failed": 0, "errors": []}

        # Create clients once for efficiency
        if service_client is None:
            service_client = self._get_datalake_service_client()
        if file_system_client is None:
            file_system_client = service_client.get_file_system_client(
                self.workspace_name
            )

        deletion_results = {"deleted": 0, "failed": 0, "errors": []}

        self.msg_helper.print_info(
            f"Removing {len(files_to_delete)} files from remote that don't exist locally"
        )

        def delete_one(relative_path: str) -> dict:
            full_path = f"{target_prefix}/{relative_path}".replace("\\", "/")
            try:
                if self.delete_lakehouse_file(
                    lakehouse_id,
                    full_path,
                    service_client=service_client,
                    file_system_client=file_system_client,
                ):
                    return {"success": True, "path": relative_path}
                else:
                    return {
                        "success": False,
                        "path": relative_path,
                        "error": "Delete returned False",
                    }
            except Exception as e:
                return {"success": False, "path": relative_path, "error": str(e)}

        with self.progress_tracker.create_progress_bar(
            len(files_to_delete), description="Deleting remote files..."
        ) as progress:
            task_id = self.progress_tracker.add_task(
                "Deleting remote files...", len(files_to_delete)
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(delete_one, file_path): file_path
                    for file_path in files_to_delete
                }

                for future in as_completed(future_to_file):
                    result = future.result()
                    file_name = Path(result["path"]).name

                    if result["success"]:
                        deletion_results["deleted"] += 1
                        if progress:
                            self.progress_tracker.update_task(
                                task_id,
                                description=f"[green]✓[/green] {file_name}",
                            )
                    else:
                        deletion_results["failed"] += 1
                        deletion_results["errors"].append(
                            result.get("error", "Unknown error")
                        )
                        if progress:
                            self.progress_tracker.update_task(
                                task_id, description=f"[red]✗[/red] {file_name}"
                            )

                    if progress:
                        self.progress_tracker.advance_task(task_id)

        # Only show failures; success will be in the final summary
        if deletion_results["failed"] > 0:
            self.msg_helper.print_warning(
                f"{deletion_results['failed']} files failed to delete"
            )

        return deletion_results

    def download_file_from_lakehouse(
        self,
        lakehouse_id: str,
        file_path: str,
        source_path: str = None,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
        verbose: bool = True,
    ) -> dict:
        """
        Download a file from a lakehouse's Files section using Azure Storage SDK.

        Args:
            lakehouse_id: ID of the source lakehouse
            file_path: Local path to the file to download
            source_path: Path in the lakehouse (defaults to filename)
            verbose: Whether to show individual file upload messages (default: True)

        Returns:
            Dictionary with upload result information
        """
        file_path_obj = Path(file_path)

        #if not file_path_obj.exists():
        #    raise FileNotFoundError(f"File not found: {file_path}")

        if source_path is None:
            source_path = file_path_obj.name

        try:
            # Use provided clients or create new ones
            if service_client is None:
                service_client = self._get_datalake_service_client()

            if file_system_client is None:
                workspace_name = self.workspace_name
                file_system_client = service_client.get_file_system_client(
                    workspace_name
                )

            # Get lakehouse name and construct the full path: {lakehouse_name}.Lakehouse/Files/{target_path}
            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            full_source_path = (
                f"{lakehouse_name}.Lakehouse/Files/{source_path}".replace("\\", "/")
            )

            file_client = file_system_client.get_file_client(full_source_path)

            if verbose and self.console:
                self.console.print(
                    f"[cyan]Uploading:[/cyan] {Path(file_path).name} → OneLake"
                )

            download = file_client.download_file()
            
            with open(file_path_obj, "wb") as my_file:
                downloaded_bytes = download.readinto(my_file)

            if verbose:
                self.msg_helper.print_success(f"Downloaded {full_source_path}")
            return {
                "success": True,
                "local_path": str(file_path),
                "remote_path": full_source_path,
                "lakehouse_id": lakehouse_id,
            }

        except Exception as e:
            error_msg = f"Failed to download {full_source_path}: {str(e)}"
            if verbose:
                self.msg_helper.print_error(error_msg)
            return {
                "success": False,
                "local_path": str(file_path),
                "remote_path": source_path,
                "error": error_msg,
            }

    def upload_file_to_lakehouse(
        self,
        lakehouse_id: str,
        file_path: str,
        target_path: str = None,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
        verbose: bool = True,
    ) -> dict:
        """
        Upload a file to a lakehouse's Files section using Azure Storage SDK.

        Args:
            lakehouse_id: ID of the target lakehouse
            file_path: Local path to the file to upload
            target_path: Path in the lakehouse (defaults to filename)
            verbose: Whether to show individual file upload messages (default: True)

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
                workspace_name = self.workspace_name
                file_system_client = service_client.get_file_system_client(
                    workspace_name
                )

            # Get lakehouse name and construct the full path: {lakehouse_name}.Lakehouse/Files/{target_path}
            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            full_target_path = (
                f"{lakehouse_name}.Lakehouse/Files/{target_path}".replace("\\", "/")
            )

            # Get the file client
            file_client = file_system_client.get_file_client(full_target_path)

            if verbose and self.console:
                self.console.print(
                    f"[cyan]Uploading:[/cyan] {Path(file_path).name} → OneLake"
                )

            # Upload the file
            file_data_raw = ""
            with open(file_path_obj, "r", encoding="utf-8") as f:
                file_data_raw = f.read()

            file_data = self.vlu.perform_code_replacements(file_data_raw)
            # if file_data_raw != file_data:
            #    print(
            #        f"[yellow]Injected variables into:[/yellow] {Path(file_path).name}"
            #    )
            # else:
            #    print(f"[green]No variable replacements needed for:[/green] {Path(file_path).name}")

            # Convert to bytes and get correct byte length
            file_data_bytes = file_data.encode("utf-8")

            file_client.upload_data(
                data=file_data_bytes, overwrite=True, length=len(file_data_bytes)
            )

            if verbose:
                self.msg_helper.print_success(f"Uploaded {Path(file_path).name}")
            return {
                "success": True,
                "local_path": str(file_path),
                "remote_path": full_target_path,
                "lakehouse_id": lakehouse_id,
            }

        except Exception as e:
            error_msg = f"Failed to upload {Path(file_path).name}: {str(e)}"
            if verbose:
                self.msg_helper.print_error(error_msg)
            return {
                "success": False,
                "local_path": str(file_path),
                "remote_path": target_path,
                "error": error_msg,
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
        include_extensions: Optional[list[str]] = None,
        sync_enabled: bool = True,
    ) -> dict:
        """
        Upload all files in a directory to a lakehouse's Files section using parallel uploads.

        Args:
            lakehouse_id: ID of the target lakehouse
            directory_path: Local directory path to upload
            target_prefix: Prefix for remote paths (optional)
            max_workers: Number of parallel uploads (default: 8)
            max_retries: Maximum number of retry attempts for failed uploads (default: 3)
            backoff_factor: Exponential backoff factor for retries (default: 0.5)
            include_extensions: Optional list of file extensions to include (e.g., [".py", ".sql"])
            sync_enabled: If True, delete remote files that don't exist locally (default: True)

        Returns:
            Dictionary with upload and deletion results
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        dir_path = Path(directory_path)
        if not dir_path.exists() or not dir_path.is_dir():
            raise ValueError(f"Directory not found: {directory_path}")

        upload_results = {
            "successful": [],
            "failed": [],
            "total_files": 0,
            "deleted": 0,
            "deletion_failed": 0,
        }

        # Find all files recursively, filtering out __ directories and by extension if specified
        all_files = []
        for file_path in dir_path.rglob("*"):
            if file_path.is_file():
                path_parts = file_path.relative_to(dir_path).parts
                if any(part.startswith("__") for part in path_parts):
                    continue
                if include_extensions is not None:
                    if not any(
                        str(file_path).lower().endswith(ext.lower())
                        for ext in include_extensions
                    ):
                        continue
                all_files.append(file_path)

        upload_results["total_files"] = len(all_files)

        if len(all_files) == 0:
            self.msg_helper.print_warning("No files found to upload")
            return upload_results

        self.msg_helper.print_info(
            f"Found {len(all_files)} files to upload from {Path(directory_path).name}"
        )

        # Create clients once for efficiency if not provided
        if service_client is None:
            service_client = self._get_datalake_service_client()
        if file_system_client is None:
            workspace_name = self.workspace_name
            file_system_client = service_client.get_file_system_client(workspace_name)

        # Sync logic: delete remote files that don't exist locally
        deletion_results = {"deleted": 0, "failed": 0, "errors": []}
        if sync_enabled and target_prefix:
            # Build set of local file relative paths
            local_file_paths = set()
            for file_path in all_files:
                relative_path = str(file_path.relative_to(dir_path)).replace("\\", "/")
                local_file_paths.add(relative_path)

            # List remote files
            remote_file_paths = self._list_remote_directory_files(
                lakehouse_id,
                target_prefix,
                service_client=service_client,
                file_system_client=file_system_client,
            )

            # Compute and execute deletions
            files_to_delete = self._compute_sync_deletions(
                local_file_paths, remote_file_paths
            )
            if files_to_delete:
                deletion_results = self._delete_remote_files_parallel(
                    lakehouse_id,
                    files_to_delete,
                    target_prefix,
                    service_client=service_client,
                    file_system_client=file_system_client,
                    max_workers=max_workers,
                )

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
                        file_system_client=file_system_client,
                        verbose=False,  # Don't show individual messages when using progress bar
                    )
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        sleep_time = backoff_factor * (2 ** (attempt - 1))
                        if self.console:
                            self.console.print(
                                f"[yellow]Retry {attempt}[/yellow] for {Path(file_path).name} after error. Backing off {sleep_time:.1f}s..."
                            )
                        time.sleep(sleep_time)
            return {
                "success": False,
                "local_path": str(file_path),
                "remote_path": str(relative_path),
                "error": str(last_exception),
            }

        # Use rich progress bar for tracking uploads
        with self.progress_tracker.create_progress_bar(
            len(all_files), description="Uploading files..."
        ) as progress:
            task_id = self.progress_tracker.add_task(
                "Uploading files...", len(all_files)
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(upload_one, file_path): file_path
                    for file_path in all_files
                }

                for future in as_completed(future_to_file):
                    result = future.result()
                    file_name = Path(result["local_path"]).name

                    if result["success"]:
                        upload_results["successful"].append(result)
                        if progress:
                            self.progress_tracker.update_task(
                                task_id, description=f"[green]✓[/green] {file_name}"
                            )
                    else:
                        upload_results["failed"].append(result)
                        if progress:
                            self.progress_tracker.update_task(
                                task_id, description=f"[red]✗[/red] {file_name}"
                            )

                    if progress:
                        self.progress_tracker.advance_task(task_id)

        # Merge deletion results into upload_results
        upload_results["deleted"] = deletion_results["deleted"]
        upload_results["deletion_failed"] = deletion_results["failed"]

        # Show final results with rich formatting
        successful_count = len(upload_results["successful"])
        failed_count = len(upload_results["failed"])
        deleted_count = upload_results["deleted"]
        deletion_failed_count = upload_results["deletion_failed"]

        if failed_count == 0 and deletion_failed_count == 0:
            # Build success message based on what operations occurred
            if deleted_count > 0:
                self.msg_helper.print_success(
                    f"Sync completed: {successful_count} uploaded, {deleted_count} deleted"
                )
            else:
                self.msg_helper.print_success(
                    f"Upload completed: {successful_count} files uploaded successfully"
                )
        else:
            self.msg_helper.print_warning(
                f"Sync completed with errors: {successful_count} uploaded, {failed_count} upload failed"
            )

        # Show summary panel
        if self.console:
            from rich.panel import Panel

            summary_content = f"[green]✓ Uploaded:[/green] {successful_count}\n"
            if failed_count > 0:
                summary_content += f"[red]✗ Upload failed:[/red] {failed_count}\n"
            if deleted_count > 0:
                summary_content += f"[yellow]🗑 Deleted:[/yellow] {deleted_count}\n"
            if deletion_failed_count > 0:
                summary_content += f"[red]✗ Deletion failed:[/red] {deletion_failed_count}"

            # Remove trailing newline if present
            summary_content = summary_content.rstrip("\n")

            panel = Panel(
                summary_content,
                title="[bold]Directory Sync Summary[/bold]",
                border_style="blue",
            )
            self.console.print(panel)
        return upload_results

    def upload_manifest_file_to_config_lakehouse(
        self, manifest_file_path: str = None
    ) -> dict:
        path_and_file = os.path.split(manifest_file_path)

        directory_path = path_and_file[0]
        file_name = path_and_file[1]

        root_path = str(Path.cwd())

        manifest_file_path_full = os.path.join(root_path, directory_path, file_name)
        
        target_path = "ingen_fab/manifest/"+file_name
        
        service_client=self._get_datalake_service_client()
        file_system_client = service_client.get_file_system_client(self.workspace_name)
        lakehouse_id = self.get_config_lakehouse_id()

        return self.upload_file_to_lakehouse(
            lakehouse_id,
            manifest_file_path_full,
            target_path,
            service_client=service_client,
            file_system_client=file_system_client,
            verbose=False, 
        )

    def download_manifest_file_from_config_lakehouse(
        self, manifest_file_path: str = None
    ) -> dict:
        manifest_path_obj = Path(manifest_file_path)
        file_name = manifest_path_obj.name

        manifest_file_path_full = str(manifest_path_obj)

        target_path = f"ingen_fab/manifest/{file_name}"
        
        service_client=self._get_datalake_service_client()
        file_system_client = service_client.get_file_system_client(self.workspace_name)
        lakehouse_id = self.get_config_lakehouse_id()

        return self.download_file_from_lakehouse(
            lakehouse_id,
            manifest_file_path_full,
            target_path,
            service_client=service_client,
            file_system_client=file_system_client,
            verbose=False, 
        )

    def upload_python_libs_to_config_lakehouse(
        self, python_libs_path: str = None
    ) -> dict:
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

        # Show upload info with rich formatting
        self.msg_helper.print_info(
            f"Uploading python_libs from: {python_libs_path.name}"
        )

        if self.console:
            from rich.panel import Panel

            info_content = (
                f"[cyan]Source:[/cyan] {python_libs_path}\n"
                f"[cyan]Target lakehouse ID:[/cyan] {config_lakehouse_id}\n"
                f"[cyan]Target path:[/cyan] ingen_fab/python_libs"
            )
            panel = Panel(
                info_content,
                title="[bold]Upload Configuration[/bold]",
                border_style="cyan",
            )
            self.console.print(panel)

        # Upload all files with "python_libs" prefix
        return self.upload_directory_to_lakehouse(
            lakehouse_id=config_lakehouse_id,
            directory_path=str(python_libs_path),
            target_prefix="ingen_fab/python_libs",
            service_client=self._get_datalake_service_client(),
            include_extensions=[".py"],
        )
    
    def upload_dbt_project_to_config_lakehouse(
        self, dbt_project_name: str, dbt_project_path: str = None
    ) -> dict:
        """
        Upload the dbt project directory to the config lakehouse's Files section.

        Args:
            dbt_project_path: Path to the dbt project directory (defaults to standard location)

        Returns:
            Dictionary with upload results
        """
        if dbt_project_path is None:
            # Default to the standard dbt project location
            current_dir = Path(__file__).parent
            dbt_project_path = current_dir.parent

        dbt_project_path = Path(dbt_project_path) / dbt_project_name

        if not dbt_project_path.exists():
            raise ValueError(f"dbt project directory not found: {dbt_project_path}")

        # Get config lakehouse ID
        config_lakehouse_id = self.get_config_lakehouse_id()

        # Show upload info with rich formatting
        self.msg_helper.print_info(
            f"Uploading dbt project from: {dbt_project_path.name}"
        )

        if self.console:
            from rich.panel import Panel

            info_content = (
                f"[cyan]Source:[/cyan] {dbt_project_path}\n"
                f"[cyan]Target lakehouse ID:[/cyan] {config_lakehouse_id}\n"
                f"[cyan]Target path:[/cyan] {dbt_project_name}"
            )
            panel = Panel(
                info_content,
                title="[bold]Upload Configuration[/bold]",
                border_style="cyan",
            )
            self.console.print(panel)

        # Upload all dbt project files
        return self.upload_directory_to_lakehouse(
            lakehouse_id=config_lakehouse_id,
            directory_path=str(dbt_project_path),
            target_prefix=f"{dbt_project_name}",
            service_client=self._get_datalake_service_client()
        )

    def list_lakehouse_files(
        self,
        lakehouse_id: str,
        path: str = "",
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
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
                workspace_name = self.workspace_name
                file_system_client = service_client.get_file_system_client(
                    workspace_name
                )

            # Get lakehouse name and construct the base path: {lakehouse_name}.Lakehouse/Files/{path}
            lakehouse_name = self.lakehouse_name
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
            error_msg = f"Failed to list lakehouse files: {str(e)}"
            self.msg_helper.print_error(error_msg)
            return []

    def delete_lakehouse_file(
        self,
        lakehouse_id: str,
        file_path: str,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
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
                workspace_name = self.workspace_name
                file_system_client = service_client.get_file_system_client(
                    workspace_name
                )

            # Get lakehouse name and construct the full path: {lakehouse_name}.Lakehouse/Files/{file_path}
            lakehouse_name = self._get_lakehouse_name(lakehouse_id)
            full_file_path = f"{lakehouse_name}.Lakehouse/Files/{file_path}".replace(
                "\\", "/"
            )

            # Get the file client and delete
            file_client = file_system_client.get_file_client(full_file_path)
            file_client.delete_file()

            # Only show verbose success if console is not available (fallback)
            if not self.console:
                print(f"Successfully deleted {Path(file_path).name} from lakehouse")
            return True

        except Exception as e:
            error_msg = f"Failed to delete {Path(file_path).name}: {str(e)}"
            if not self.console:
                print(error_msg)
            # Errors are handled at the caller level when using progress tracking
            return False

    def clean_python_libs_files_from_config_lakehouse(
        self,
        *,
        service_client: Optional[DataLakeServiceClient] = None,
        file_system_client: Optional[FileSystemClient] = None,
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
                workspace_name = self.workspace_name
                file_system_client = service_client.get_file_system_client(
                    workspace_name
                )

            # List files in python_libs directory
            file_paths = self.list_lakehouse_files(
                config_lakehouse_id,
                "python_libs",
                service_client=service_client,
                file_system_client=file_system_client,
            )

            deleted_count = 0
            errors = []

            if len(file_paths) == 0:
                self.msg_helper.print_info("No python_libs files found to delete")
                return {
                    "deleted_count": 0,
                    "errors": [],
                    "lakehouse_id": config_lakehouse_id,
                    "total_found": 0,
                }

            self.msg_helper.print_info(
                f"Found {len(file_paths)} files in python_libs directory to delete"
            )

            # Delete each file with progress tracking
            with self.progress_tracker.create_progress_bar(
                len(file_paths), description="Deleting files..."
            ) as progress:
                task_id = self.progress_tracker.add_task(
                    "Deleting files...", len(file_paths)
                )

                for file_path in file_paths:
                    # Extract the relative path from the full OneLake path
                    if "/Files/" in file_path:
                        relative_path = file_path.split("/Files/", 1)[1]
                    else:
                        relative_path = file_path

                    file_name = Path(relative_path).name

                    if self.delete_lakehouse_file(
                        config_lakehouse_id,
                        relative_path,
                        service_client=service_client,
                        file_system_client=file_system_client,
                    ):
                        deleted_count += 1
                        if progress:
                            self.progress_tracker.update_task(
                                task_id,
                                description=f"[green]✓[/green] Deleted {file_name}",
                            )
                    else:
                        errors.append(f"Failed to delete {relative_path}")
                        if progress:
                            self.progress_tracker.update_task(
                                task_id, description=f"[red]✗[/red] Failed {file_name}"
                            )

                    if progress:
                        self.progress_tracker.advance_task(task_id)

            # Show final results
            if deleted_count > 0:
                self.msg_helper.print_success(
                    f"Deleted {deleted_count} python_libs files from lakehouse"
                )
            if errors:
                self.msg_helper.print_warning(f"{len(errors)} files failed to delete")

            return {
                "deleted_count": deleted_count,
                "errors": errors,
                "lakehouse_id": config_lakehouse_id,
                "total_found": len(file_paths),
            }

        except Exception as e:
            return {
                "deleted_count": 0,
                "errors": [str(e)],
                "lakehouse_id": config_lakehouse_id,
                "total_found": 0,
            }
