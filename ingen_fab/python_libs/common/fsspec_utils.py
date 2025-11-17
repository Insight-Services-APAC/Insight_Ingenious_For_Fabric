import os
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime

# Suppress Fabric fsspec wrapper and related logging (BEFORE imports)
logging.getLogger("fsspec_wrapper.trident.core").setLevel(logging.WARNING)
logging.getLogger("fsspec").setLevel(logging.WARNING)
logging.getLogger("adlfs").setLevel(logging.WARNING)

from fsspec import AbstractFileSystem
from fsspec.core import url_to_fs

from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo

logger = logging.getLogger(__name__)

def build_onelake_url(workspace_name: str, lakehouse_name: str, path: str = "") -> str:
    """
    Build OneLake ABFSS URL from workspace and lakehouse names.

    Args:
        workspace_name: Fabric workspace name
        lakehouse_name: Lakehouse name (with or without .Lakehouse suffix)
        path: Optional path within lakehouse (e.g., "Files/inbound")

    Returns:
        Full ABFSS URL like: abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/path
    """
    # Ensure lakehouse has .Lakehouse suffix
    if not lakehouse_name.endswith(".Lakehouse"):
        lakehouse_name = f"{lakehouse_name}.Lakehouse"

    # Build base URL
    base_url = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}"

    # Add path if provided
    if path:
        path = path.lstrip("/")
        return f"{base_url}/{path}"

    return base_url


def get_filesystem_client(connection_params: Dict[str, Any]) -> Tuple[AbstractFileSystem, str]:
    """
    Create fsspec filesystem client for OneLake using notebook user credentials.

    Uses notebookutils.credentials.getToken() to authenticate with the credentials
    of the person running the notebook (passthrough authentication).

    Supports two configuration patterns:
    1. workspace_name + lakehouse_name → builds OneLake URL
    2. bucket_url → uses provided ABFSS URL (for flexibility)

    Args:
        connection_params: Either:
            - {"workspace_name": "...", "lakehouse_name": "..."}  # OneLake simple
            - {"bucket_url": "abfss://..."}  # Full ABFSS URL

    Returns:
        (filesystem_client, base_url)

    Raises:
        ValueError: If connection_params doesn't match either pattern

    Note:
        The account_name is automatically extracted from the ABFSS URL by fsspec,
        so it should NOT be included in connection_params (would cause duplicate key error).
    """
    # Get OAuth token from notebook context (user's credentials)
    try:
        from notebookutils import credentials
        token = credentials.getToken("storage")
    except Exception as e:
        logger.warning(f"Could not get token from notebookutils: {e}")
        token = None

    # Pattern 1: OneLake simple (workspace/lakehouse names)
    if "workspace_name" in connection_params and "lakehouse_name" in connection_params:
        workspace = connection_params["workspace_name"]
        lakehouse = connection_params["lakehouse_name"]

        # Build OneLake URL
        bucket_url = build_onelake_url(workspace, lakehouse)

    # Pattern 2: Full ABFSS URL (for flexibility)
    elif "bucket_url" in connection_params:
        bucket_url = connection_params["bucket_url"]

    else:
        raise ValueError(
            "connection_params must have either:\n"
            "  1. workspace_name + lakehouse_name (OneLake simple mode)\n"
            "  2. bucket_url (full ABFSS URL mode)"
        )

    # Build credentials dict
    # NOTE: url_to_fs automatically extracts account_name from the ABFSS URL,
    # so we don't need to pass it explicitly (would cause duplicate key error)
    credentials_dict = {}

    if token:
        credentials_dict["token"] = token

    # Azure best practice - disable concurrent operations for stability
    credentials_dict["max_concurrency"] = 1

    # Create fsspec filesystem
    fs, _ = url_to_fs(bucket_url, **credentials_dict)

    logger.debug(f"Created fsspec client for: {bucket_url}")
    # Return the original bucket_url (full ABFSS path), not the normalized path
    # The normalized path from fsspec strips the protocol and is not suitable for batch table storage
    return fs, bucket_url


def move_file(
    source_fs: AbstractFileSystem,
    source_path: str,
    dest_fs: AbstractFileSystem,
    dest_path: str,
) -> bool:
    """
    Move file from source to destination using fs.cp() + delete.

    Primary use case: OneLake → OneLake
    Can also work with any ABFSS-compatible storage if needed in the future.

    Args:
        source_fs: Source filesystem client
        source_path: Full path to source file
        dest_fs: Destination filesystem client
        dest_path: Full path to destination file

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create destination directory if needed
        dest_dir = os.path.dirname(dest_path)
        if dest_dir and not dest_fs.exists(dest_dir):
            dest_fs.makedirs(dest_dir, exist_ok=True)

        # Copy file
        if source_fs == dest_fs:
            # Same filesystem - use cp
            source_fs.cp(source_path, dest_path)
        else:
            # Different filesystems - use get/put pattern
            with source_fs.open(source_path, 'rb') as f_src:
                with dest_fs.open(dest_path, 'wb') as f_dest:
                    f_dest.write(f_src.read())

        # Delete source after successful copy
        source_fs.rm(source_path)

        return True

    except Exception as e:
        logger.error(f"✗ Failed to move {source_path} → {dest_path}: {e}")
        return False


def glob(
    fs: AbstractFileSystem,
    path: str,
    pattern: str = "*",
    recursive: bool = True,
    files_only: bool = True,
    directories_only: bool = False,
) -> List[FileInfo]:
    """
    Glob files and/or directories using fsspec.

    Args:
        fs: Filesystem client
        path: Directory path to search in (should be full ABFSS path)
        pattern: Glob pattern for matching (default: "*")
        recursive: Whether to search recursively (default: True)
        files_only: Return only files (default: True)
        directories_only: Return only directories (default: False)
            Note: If both files_only and directories_only are True, returns both

    Returns:
        List of FileInfo objects with path, name, size, modified_ms
    """
    path = path.rstrip("/")

    # Build glob pattern
    if recursive:
        glob_pattern = f"{path}/**/{pattern}" if files_only and not directories_only else f"{path}/**"
    else:
        glob_pattern = f"{path}/{pattern}" if files_only and not directories_only else f"{path}/*"

    # Get items with metadata
    try:
        glob_result = fs.glob(glob_pattern, detail=True)
    except Exception as e:
        logger.error(f"Failed to glob {glob_pattern}: {e}")
        return []

    if isinstance(glob_result, list):
        logger.error("fsspec glob returned list instead of dict - version too old?")
        return []

    # Extract URL prefix from input path (protocol + host + container)
    # fsspec.glob() returns paths like "CONTAINER/path/to/file", we need to reconstruct full ABFSS URL
    url_prefix = ""
    container = ""
    if "://" in path:
        # Parse ABFSS URL: abfss://CONTAINER@HOST/PATH
        protocol = path.split("://")[0]  # e.g., "abfss"
        rest = path.split("://", 1)[1]  # e.g., "CONTAINER@HOST/PATH"

        if "@" in rest:
            # Extract container and host (e.g., "CONTAINER@onelake.dfs.fabric.microsoft.com")
            container_and_host = rest.split("/", 1)[0]
            container = container_and_host.split("@")[0]
            url_prefix = f"{protocol}://{container_and_host}/"
        else:
            # No host in URL (shouldn't happen with ABFSS, but handle it)
            container = rest.split("/", 1)[0]
            url_prefix = f"{protocol}://{container}/"

    # Filter and convert to FileInfo
    items = []
    for item_path, metadata in glob_result.items():
        item_type = metadata.get("type")

        # Filter based on type
        if files_only and not directories_only:
            if item_type != "file":
                continue
        elif directories_only and not files_only:
            if item_type != "directory":
                continue
        # If both True, return everything (files and dirs)

        # Reconstruct full ABFSS path
        if url_prefix:
            # fsspec returns paths like "CONTAINER/path/to/file"
            # We need to rebuild as "abfss://CONTAINER@HOST/path/to/file"
            if container and item_path.startswith(f"{container}/"):
                # Strip container prefix and add URL prefix
                path_without_container = item_path[len(container) + 1:]
                full_path = f"{url_prefix}{path_without_container}"
            else:
                # Path doesn't start with container - just prepend URL prefix
                full_path = f"{url_prefix}{item_path}"
        else:
            full_path = item_path

        # Extract modified time (Azure-specific fields)
        modified_time = metadata.get("last_modified") or metadata.get("LastModified")
        if modified_time:
            if isinstance(modified_time, datetime):
                modified_ms = int(modified_time.timestamp() * 1000)
            else:
                # Parse timestamp string
                from dateutil import parser
                dt = parser.parse(str(modified_time))
                modified_ms = int(dt.timestamp() * 1000)
        else:
            modified_ms = 0

        file_info = FileInfo(
            path=full_path,
            name=os.path.basename(item_path),
            size=metadata.get("size", 0),
            modified_ms=modified_ms,
        )
        items.append(file_info)

    return items


def file_exists(fs: AbstractFileSystem, file_path: str) -> bool:
    """
    Check if file exists using fsspec.

    Args:
        fs: Filesystem client
        file_path: Path to file

    Returns:
        True if file exists, False otherwise
    """
    try:
        return fs.exists(file_path)
    except Exception:
        return False


def is_directory_empty(fs: AbstractFileSystem, directory_path: str) -> bool:
    """
    Check if directory is empty using fsspec.

    Args:
        fs: Filesystem client
        directory_path: Path to directory

    Returns:
        True if directory is empty or doesn't exist, False otherwise
    """
    try:
        items = fs.ls(directory_path)
        return len(items) == 0
    except (FileNotFoundError, Exception):
        return True  # Directory doesn't exist = empty


def cleanup_empty_directories(
    fs: AbstractFileSystem,
    start_path: str,
    stop_path: str,
) -> None:
    """
    Recursively delete empty directories from start_path up to stop_path.

    Walks up the directory tree from start_path, deleting empty directories
    until it reaches stop_path or finds a non-empty directory.

    Args:
        fs: Filesystem client
        start_path: Starting path (usually a file that was just moved)
        stop_path: Stop path (usually the base inbound directory)
    """
    try:
        current = os.path.dirname(start_path)

        while current and current != stop_path and current != "/":
            if is_directory_empty(fs, current):
                fs.rmdir(current)
                logger.debug(f"Deleted empty directory: {current}")
                current = os.path.dirname(current)
            else:
                # Directory not empty - stop climbing
                break
    except Exception as e:
        logger.debug(f"Could not cleanup directories: {e}")
