import os
import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
from fnmatch import fnmatch

from notebookutils import mssparkutils # type: ignore

from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo

logger = logging.getLogger(__name__)


@dataclass
class FilesystemConnection:
    """Filesystem client and base URL bundle for dependency injection."""
    fs: mssparkutils.fs
    base_url: str

    @classmethod
    def from_params(cls, connection_params: Dict[str, Any]) -> "FilesystemConnection":
        """
        Create connection from connection params.

        Supports:
        - {"workspace_name": "...", "lakehouse_name": "..."} (OneLake)
        - {"bucket_url": "abfss://..."} (full ABFSS URL)

        Args:
            connection_params: Connection parameters dict

        Returns:
            FilesystemConnection with fs client and base_url
        """
        fs, base_url = _get_filesystem_client(connection_params)
        return cls(fs=fs, base_url=base_url)

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

def _get_filesystem_client(connection_params: Dict[str, Any]) -> Tuple[mssparkutils.fs, str]:
    """
    Return mounted mssparkutils filesystem for OneLake using.

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

    #mssparkutils.fs.mount(bucket_url, "/mnt/temp_mount")  # Mount to a temp location to get fs client

    return mssparkutils.fs, bucket_url

def copy_file(
    source_fs: mssparkutils.fs,
    source_path: str,
    dest_fs: mssparkutils.fs,
    dest_path: str,
    quietly: bool = False,
) -> bool:
    """
    Copy file from source to destination without deleting source.

    Same as move_file() but leaves the source file in place.
    Used for watermark-based extraction where files stay at source.

    Args:
        source_fs: Source filesystem client
        source_path: Full path to source file
        dest_fs: Destination filesystem client
        dest_path: Full path to destination file

    Returns:
        True if successful, False otherwise
    """
    # Create destination directory if needed
    try:
        dest_dir = os.path.dirname(dest_path)
    except Exception as e:
        logger.error(f"✗ Invalid destination path {dest_path}: {e}")
        return False
    
    # Only attempt to create directory if dest_dir is not empty (i.e., dest_path is not at root)
    try:
        if dest_dir and not dest_fs.exists(dest_dir):
            dest_fs.mkdirs(dest_dir)
    except Exception as e:
        logger.error(f"✗ Failed to create destination directory {dest_dir}: {e}")
        return False
    
    # Copy file
    try:
        source_fs.cp(source_path, dest_path)
    except Exception as e:
        logger.error(f"✗ Failed to copy {source_path} → {dest_path}: {e}")
        return False

    if not quietly:
        logger.info(f"✓ Successfully copied {source_path} → {dest_path}")
    return True

def move_file(
    source_fs: mssparkutils.fs,
    source_path: str,
    dest_fs: mssparkutils.fs,
    dest_path: str
) -> bool:
    """
    Move file from source to destination using fs.cp() + delete.

    Args:
        source_fs: Source filesystem client
        source_path: Full path to source file
        dest_fs: Destination filesystem client
        dest_path: Full path to destination file

    Returns:
        True if successful, False otherwise
    """
    # Copy file from source to destination
    if not copy_file(source_fs, source_path, dest_fs, dest_path, quietly=True):
        return False
    
    try:
        # Delete source after successful copy
        source_fs.rm(source_path)
    except Exception as e:
        logger.error(f"✗ Failed to delete source file {source_path} after moving: {e}")
        return False

    logger.info(f"✓ Successfully moved {source_path} → {dest_path}")
    return True

def glob(
    fs: mssparkutils.fs,
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
    try:
        files = fs.ls(path)
    except Exception as e:
        logger.error(f"Path not found: {path}")
        return []

    if recursive:
        all_files = []
        for file in files:
            if file.isDir:
                try:
                    all_files.extend(fs.ls(file.path))
                except Exception as e:
                    logger.error(f"Unable to find sub-path: {path} {e}")
                all_files.append(file)
            else:
                all_files.append(file)
        files = all_files

    if files_only and not directories_only:
        files = [f for f in files if f.isFile]
    elif directories_only and not files_only:
        files = [f for f in files if f.isDir]
    else:
        # If both are True, return all (files and directories)
        pass

    # Apply glob pattern matching to file names
    if pattern != "*":
        files = [f for f in files if fnmatch(os.path.basename(f.name), pattern)]

    return [FileInfo(path=f.path, name=os.path.basename(f.name), size=f.size, modified_ms=f.modifyTime) for f in files]

def file_exists(fs: mssparkutils.fs, file_path: str) -> bool:
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
        logger.warning(f"Error checking existence of {file_path}")
        return False

def is_directory_empty(fs: mssparkutils.fs, directory_path: str) -> bool:
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
    fs: mssparkutils.fs,
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
                fs.rm(current)
                logger.debug(f"Deleted empty directory: {current}")
                current = os.path.dirname(current)
            else:
                # Directory not empty - stop climbing
                break
    except Exception as e:
        logger.debug(f"Could not cleanup directories: {e}")