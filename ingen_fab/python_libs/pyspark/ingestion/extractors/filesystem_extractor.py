# Filesystem Extractor
# Validates and extracts external files from inbound → raw layer

import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Generator, List, Optional

from ingen_fab.python_libs.pyspark.ingestion.config import (
    FileSystemExtractionParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.constants import DuplicateHandling, ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    DuplicateDataError,
    DuplicateFilesError,
    ErrorContext,
    ExtractionError,
)
from ingen_fab.python_libs.pyspark.ingestion.results import BatchExtractionResult
from ingen_fab.python_libs.common.fsspec_utils import (
    get_filesystem_client,
    move_file,
    glob,
    file_exists,
    is_directory_empty,
    cleanup_empty_directories,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo

logger = logging.getLogger(__name__)

# Suppress verbose Azure SDK and adlfs logging (must be at module level before any clients are created)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("azure.identity._credentials").setLevel(logging.WARNING)
logging.getLogger("adlfs").setLevel(logging.WARNING)
logging.getLogger("adlfs.spec").setLevel(logging.WARNING)
logging.getLogger("fsspec").setLevel(logging.WARNING)
logging.getLogger("fsspec.spec").setLevel(logging.WARNING)
logging.getLogger("adlfs.utils").setLevel(logging.WARNING)


class FolderInfo:
    """Information about a folder batch"""

    def __init__(self, path: str, files: List[FileInfo]):
        self.path = path
        self.files = files
        self.name = os.path.basename(path.rstrip("/"))
        self.file_count = len(files)
        self.total_size = sum(f.size for f in files)
        self.latest_modified_ms = max(f.modified_ms for f in files) if files else 0

    def __repr__(self) -> str:
        return f"FolderInfo(path={self.path}, file_count={self.file_count})"


class ValidationResult:
    """Result of file/folder validation"""

    def __init__(self):
        self.valid_files: List[FileInfo] = []
        self.failed_files: List[tuple[FileInfo, str]] = []  # (file, error_message)
        self.duplicate_files: List[FileInfo] = []

        # For folder-batch validation
        self.valid_folders: List[FolderInfo] = []
        self.failed_folders: List[tuple[FolderInfo, str]] = []  # (folder, error_message)
        self.duplicate_folders: List[FolderInfo] = []

    def add_valid(self, file_info: FileInfo):
        self.valid_files.append(file_info)

    def add_failed(self, file_info: FileInfo, error: str):
        self.failed_files.append((file_info, error))

    def add_duplicate(self, file_info: FileInfo):
        self.duplicate_files.append(file_info)

    def add_valid_folder(self, folder_info: FolderInfo):
        self.valid_folders.append(folder_info)

    def add_failed_folder(self, folder_info: FolderInfo, error: str):
        self.failed_folders.append((folder_info, error))

    def add_duplicate_folder(self, folder_info: FolderInfo):
        self.duplicate_folders.append(folder_info)

    @property
    def total_files(self) -> int:
        return len(self.valid_files) + len(self.failed_files) + len(self.duplicate_files)


class FileSystemExtractor:
    """
    Extracts (validates and moves) files from inbound to raw layer.

    This extractor:
    1. Discovers files in inbound folder
    2. Validates control files, duplicates, format
    3. Moves validated files to raw layer (organized by date)
    4. Leaves failed files in inbound with error details
    """

    def __init__(
        self,
        resource_config: ResourceConfig,
        logger_instance=None,  # Optional context-aware logger
        extraction_logger=None,  # REQUIRED extraction logger for duplicate checking
    ):
        """
        Initialize FileSystemExtractor.

        Args:
            resource_config: Resource configuration
            logger_instance: Optional logger instance with resource context (ConfigLoggerAdapter)
            extraction_logger: Extraction logger for querying log tables (REQUIRED)
        """
        self.config = resource_config
        self.extraction_logger = extraction_logger

        # Validate required dependencies
        if self.extraction_logger is None:
            raise ValueError("extraction_logger is required for duplicate detection")

        # Parse extraction params
        if isinstance(resource_config.extraction_params, dict):
            self.params = FileSystemExtractionParams.from_dict(
                resource_config.extraction_params
            )
        elif isinstance(resource_config.extraction_params, FileSystemExtractionParams):
            self.params = resource_config.extraction_params
        else:
            raise ValueError(
                "extraction_params must be dict or FileSystemExtractionParams"
            )

        # Get SOURCE filesystem client
        source_params = resource_config.source_config.connection_params
        self.source_fs, self.source_base_url = get_filesystem_client(source_params)

        # Get DESTINATION filesystem client (target lakehouse)
        dest_params = {
            "workspace_name": resource_config.target_workspace,
            "lakehouse_name": resource_config.target_lakehouse,
        }
        self.dest_fs, self.dest_base_url = get_filesystem_client(dest_params)

        # Build inbound path (relative to source filesystem)
        if "workspace_name" in source_params:
            # OneLake: paths are relative to Files/
            inbound_subpath = self.params.inbound_path.lstrip("/") if self.params.inbound_path else ""
            # Avoid double slashes in path construction, but preserve abfss:// protocol
            path_part = f"/Files/{inbound_subpath}".replace("//", "/")
            self.inbound_full_path = f"{self.source_base_url}{path_part}".rstrip("/")
        else:
            # Full ABFSS URL: paths are relative to bucket root
            inbound_subpath = self.params.inbound_path.lstrip("/") if self.params.inbound_path else ""
            # Avoid double slashes in path construction, but preserve abfss:// protocol
            path_part = f"/{inbound_subpath}".replace("//", "/") if inbound_subpath else ""
            self.inbound_full_path = f"{self.source_base_url}{path_part}".rstrip("/")

        # Build raw landing path (in destination lakehouse)
        raw_subpath = resource_config.raw_landing_path.lstrip("/")
        # Avoid double slashes in path construction, but preserve abfss:// protocol
        path_part = f"/Files/{raw_subpath}".replace("//", "/")
        self.raw_landing_full = f"{self.dest_base_url}{path_part}".rstrip("/")

        # Use provided logger or fallback to module logger
        self.logger = logger_instance if logger_instance is not None else logger

    def extract(self) -> Generator[BatchExtractionResult, None, None]:
        """
        Extract files/folders from inbound to raw layer.

        Routes to folder-batch or file-level extraction based on batch_by parameter.

        Yields:
            BatchExtractionResult for each batch extracted (real-time streaming)

        Raises:
            ExtractionError: On validation or extraction failures
            DuplicateFilesError: If duplicates detected and duplicate_handling=FAIL
        """
        try:
            self.logger.debug(f"Starting extraction: {self.config.resource_name}")
            self.logger.debug(f"Batch mode: {self.params.batch_by}")
            self.logger.debug(f"Inbound path: {self.inbound_full_path}")
            self.logger.debug(f"Raw landing path: {self.raw_landing_full}")

            # Route based on batch_by parameter
            if self.params.batch_by == "folder":
                # FOLDER-BATCH EXTRACTION
                yield from self._extract_folders()
            elif self.params.batch_by == "all":
                # ALL-FILES-AS-ONE-BATCH EXTRACTION
                yield from self._extract_all()
            else:
                # FILE-LEVEL EXTRACTION (default)
                yield from self._extract_files()

        except (ExtractionError, DuplicateFilesError):
            # Re-raise known extraction errors without wrapping
            raise
        except Exception as e:
            # Wrap unexpected errors
            self.logger.exception(f"Unexpected extraction error: {e}")
            raise ExtractionError(
                message=f"Failed to extract from inbound to raw",
                context=ErrorContext(
                    resource_name=self.config.resource_name,
                    source_name=self.config.source_name,
                    operation="extract",
                ),
            ) from e

    def _extract_files(self) -> Generator[BatchExtractionResult, None, None]:
        """Extract individual files (batch_by='file')"""
        # Step 1: Discover files in inbound
        files = self._discover_files_in_inbound()

        if not files:
            # Check require_files when no files discovered
            if self.params.require_files:
                raise ExtractionError(
                    message=f"No files found in {self.params.inbound_path} (require_files=True)",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        operation="discover_files",
                    ),
                )
            # No files but not required - log and return
            self.logger.info("No files found in inbound")
            return  # No files, empty generator

        self.logger.info(f"Discovered {len(files)} files in inbound")

        # Step 2: Validate files
        validation = self._validate_files(files)

        # Step 3: Handle validation results
        if validation.failed_files:
            self.logger.warning(
                f"{len(validation.failed_files)} files failed validation"
            )
            for file_info, error in validation.failed_files:
                self._write_error_file(file_info, error)

        if validation.duplicate_files:
            dup_names = [os.path.basename(f.path) for f in validation.duplicate_files]
            self.logger.warning(f"Skipping {len(validation.duplicate_files)} duplicate(s): {', '.join(dup_names)}")

        # Check require_files BEFORE extraction
        if self.params.require_files and len(validation.valid_files) == 0:
            raise ExtractionError(
                message=(
                    f"No valid files in {self.params.inbound_path} (require_files=True): "
                    f"{len(files)} discovered, {len(validation.failed_files)} failed validation, "
                    f"{len(validation.duplicate_files)} duplicates"
                ),
                context=ErrorContext(
                    resource_name=self.config.resource_name,
                    source_name=self.config.source_name,
                    operation="validate_files",
                ),
            )

        # Step 4: Extract valid files to raw (yields batches)
        if validation.valid_files:
            yield from self._extract_files_to_raw(validation.valid_files)

    def _extract_folders(self) -> Generator[BatchExtractionResult, None, None]:
        """Extract folder batches (batch_by='folder')"""
        # Step 1: Discover folders in inbound
        folders = self._discover_folders_in_inbound()

        if not folders:
            # Check require_files when no folders discovered
            if self.params.require_files:
                raise ExtractionError(
                    message=f"No folders found in {self.params.inbound_path} (require_files=True)",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        operation="discover_folders",
                    ),
                )
            # No folders but not required - log and return
            self.logger.info("No folders found in inbound")
            return  # No folders, empty generator

        self.logger.info(f"Discovered {len(folders)} folders in inbound")
        total_files = sum(f.file_count for f in folders)
        self.logger.info(f"Total files across folders: {total_files}")

        # Step 2: Validate folders
        validation = self._validate_folders(folders)

        # Step 3: Handle validation results
        if validation.failed_folders:
            self.logger.warning(
                f"{len(validation.failed_folders)} folders failed validation"
            )

        if validation.duplicate_folders:
            # Show full paths for better clarity
            dup_paths = [f.path for f in validation.duplicate_folders]
            self.logger.warning(
                f"Skipping {len(validation.duplicate_folders)} duplicate folders:\n  " +
                "\n  ".join(dup_paths)
            )

        # Check require_files BEFORE extraction
        if self.params.require_files and len(validation.valid_folders) == 0:
            raise ExtractionError(
                message=(
                    f"No valid folders in {self.params.inbound_path} (require_files=True): "
                    f"{len(folders)} discovered, {len(validation.failed_folders)} failed validation, "
                    f"{len(validation.duplicate_folders)} duplicates"
                ),
                context=ErrorContext(
                    resource_name=self.config.resource_name,
                    source_name=self.config.source_name,
                    operation="validate_folders",
                ),
            )

        # Step 4: Extract valid folders to raw (yields batches)
        if validation.valid_folders:
            yield from self._extract_folders_to_raw(validation.valid_folders)

    def _extract_all(self) -> Generator[BatchExtractionResult, None, None]:
        """Extract all files as one batch (batch_by='all')"""
        # Step 1: Discover all files in inbound directory
        files = self._discover_files_in_inbound()

        if not files:
            # Check require_files when no files discovered
            if self.params.require_files:
                raise ExtractionError(
                    message=f"No files found in {self.params.inbound_path} (require_files=True)",
                    context=ErrorContext(
                        resource_name=self.config.resource_name,
                        source_name=self.config.source_name,
                        operation="discover_files",
                    ),
                )
            # No files but not required - log and return
            self.logger.info("No files found in inbound")
            return  # No files, empty generator

        self.logger.info(f"Discovered {len(files)} files in inbound (treating as ONE batch)")

        # Step 2: Check for duplicate batch
        if self.params.duplicate_handling != DuplicateHandling.ALLOW:
            # Build expected raw folder path for this batch
            raw_folder_path = self._build_raw_folder_path_for_all_batch()

            # Check if folder already exists and has files
            try:
                if not is_directory_empty(self.dest_fs, raw_folder_path):
                    # Folder exists and has files - this is a duplicate
                    self.logger.warning(f"Duplicate batch detected: {raw_folder_path} already exists")

                    # Fail if configured
                    if self.params.duplicate_handling == DuplicateHandling.FAIL:
                        raise DuplicateDataError(
                            f"Duplicate batch detected: folder {raw_folder_path} already processed"
                        )

                    # Skip mode - return empty generator
                    return
            except Exception:
                # Folder doesn't exist - not a duplicate, proceed
                pass

        # Step 3: Move all files to raw as one batch (yields single batch)
        yield from self._extract_all_to_raw(files)

    def _discover_files_in_inbound(self) -> List[FileInfo]:
        """Discover files in inbound folder matching discovery pattern"""
        pattern = self.params.discovery_pattern or "*"

        files = glob(
            fs=self.source_fs,
            path=self.inbound_full_path,
            pattern=pattern,
            recursive=self.params.recursive,
            files_only=True,
            directories_only=False,
        )

        # Sort files to ensure chronological processing
        if files:
            files = self._sort_files(files)

        return files

    def _discover_folders_in_inbound(self) -> List[FolderInfo]:
        """
        Discover folder batches in inbound.

        If partition_depth is specified, only checks folders at that specific depth.
        Otherwise, recursively searches all folders.

        Configuration examples:
        - partition_depth=3, date_regex=r"(\d{4})/(\d{2})/(\d{2})" → Day-level (YYYY/MM/DD)
        - partition_depth=4, date_regex=r"(\d{4})/(\d{2})/(\d{2})/(\d{2})" → Hour-level (YYYY/MM/DD/HH)
        - partition_depth=5, date_regex=r"(\d{4})/(\d{2})/(\d{2})/(\d{2})/(\d{2})" → Minute-level

        Returns list of FolderInfo objects, one per folder batch.
        Used when batch_by="folder" to treat each folder as a batch.
        """
        folder_infos = []
        pattern = self.params.discovery_pattern or "*"

        # Get candidate folders based on partition_depth
        if self.params.partition_depth is not None:
            # User specified exact depth - only check folders at that level
            candidate_folders = self._list_folders_at_depth(self.params.partition_depth)
            self.logger.info(
                f"Checking {len(candidate_folders)} folders at depth {self.params.partition_depth}"
            )
        else:
            # No depth specified - check all folders recursively (old behavior)
            folder_items = glob(
                fs=self.source_fs,
                path=self.inbound_full_path,
                recursive=True,
                files_only=False,
                directories_only=True,
            )
            candidate_folders = [item.path for item in folder_items]
            self.logger.info(
                f"Checking {len(candidate_folders)} folders (recursive, no depth limit)"
            )

        # Process each candidate folder
        for folder_path in candidate_folders:
            try:
                # Check if folder has files
                files_in_folder = glob(
                    fs=self.source_fs,
                    path=folder_path,
                    pattern=pattern,
                    recursive=False,  # Only files directly in this folder
                    files_only=True,
                    directories_only=False,
                )

                if not files_in_folder:
                    # No files in this folder - skip it
                    continue

                # If date_regex is configured, validate folder matches regex
                if self.params.date_regex:
                    date_str = self._extract_date_with_regex(folder_path)
                    if not date_str:
                        # Regex didn't match - skip this folder
                        self.logger.debug(
                            f"Skipping folder (regex mismatch): {folder_path}"
                        )
                        continue

                # Valid folder with files and matching regex
                folder_info = FolderInfo(path=folder_path, files=files_in_folder)
                folder_infos.append(folder_info)
                self.logger.debug(
                    f"Discovered batch folder: {folder_path} ({folder_info.file_count} files)"
                )

            except Exception as e:
                self.logger.warning(f"Could not process folder {folder_path}: {e}")
                continue

        # Sort folders to ensure chronological processing
        if folder_infos:
            folder_infos = self._sort_folders(folder_infos)

        return folder_infos

    def _list_folders_at_depth(self, depth: int) -> List[str]:
        """
        List all folders at a specific depth from inbound_path.

        Args:
            depth: Number of levels deep to look (e.g., 3 for YYYY/MM/DD)

        Returns:
            List of folder paths at the specified depth
        """
        if depth <= 0:
            return [self.inbound_full_path]

        # Start with inbound path
        current_level_folders = [self.inbound_full_path]

        # Traverse depth levels
        for level in range(depth):
            next_level_folders = []
            for folder in current_level_folders:
                try:
                    # Get immediate subdirectories (non-recursive)
                    subdir_items = glob(
                        fs=self.source_fs,
                        path=folder,
                        recursive=False,
                        files_only=False,
                        directories_only=True,
                    )
                    subdirs = [item.path for item in subdir_items]
                    next_level_folders.extend(subdirs)
                except Exception as e:
                    self.logger.debug(f"Could not list subdirectories in {folder}: {e}")
                    continue

            current_level_folders = next_level_folders

            if not current_level_folders:
                # No folders at this depth
                self.logger.warning(f"No folders found at depth level {level + 1}")
                break

        return current_level_folders

    def _validate_files(self, files: List[FileInfo]) -> ValidationResult:
        """
        Validate files for control files, duplicates, etc.

        Returns:
            ValidationResult with valid, failed, and duplicate files categorized
        """
        result = ValidationResult()

        # Check for duplicates first (check if files already exist in raw)
        if self.params.duplicate_handling != DuplicateHandling.ALLOW:
            duplicates = self._check_for_duplicates(files)
            for dup in duplicates:
                result.add_duplicate(dup)

            # Remove duplicates from further processing
            duplicate_names = {os.path.basename(f.path) for f in duplicates}
            files = [f for f in files if os.path.basename(f.path) not in duplicate_names]

            # Fail on duplicates if configured
            if duplicates and self.params.duplicate_handling == DuplicateHandling.FAIL:
                dup_names = [os.path.basename(f.path) for f in duplicates]
                raise DuplicateDataError(
                    f"Duplicate files detected: {', '.join(dup_names)}"
                )

        # Validate control files
        if self.params.require_control_file and self.params.control_file_pattern:
            for file_info in files:
                if self._has_control_file(file_info):
                    result.add_valid(file_info)
                else:
                    result.add_failed(
                        file_info, f"Missing control file: {self._get_control_file_name(file_info)}"
                    )
        else:
            # No control file validation - all are valid
            for file_info in files:
                result.add_valid(file_info)

        return result

    def _check_for_duplicates(self, files: List[FileInfo]) -> List[FileInfo]:
        """
        Check if any files have already been extracted (across all partitions).

        Queries extraction logs to find files previously extracted on ANY date.

        Returns:
            List of files that are duplicates (already extracted)
        """
        duplicates = []

        for file_info in files:
            filename = os.path.basename(file_info.path)

            # Query extraction logs (works across all partitions)
            if self.extraction_logger.check_file_already_extracted(
                config=self.config,
                filename=filename
            ):
                duplicates.append(file_info)
                self.logger.debug(
                    f"Duplicate: {filename} already extracted (found in extraction logs)"
                )

        return duplicates

    def _has_control_file(self, file_info: FileInfo) -> bool:
        """Check if control file exists for this file"""
        control_file_path = self._get_control_file_path(file_info)
        return file_exists(self.source_fs, control_file_path)

    def _get_control_file_path(self, file_info: FileInfo) -> str:
        """Build control file path"""
        directory = os.path.dirname(file_info.path.rstrip("/"))
        control_file_name = self._get_control_file_name(file_info)
        return f"{directory}/{control_file_name}".replace("//", "/")

    def _get_control_file_name(self, file_info: FileInfo) -> str:
        """Get control file name for a file"""
        if not self.params.control_file_pattern:
            return ""

        if "{basename}" in self.params.control_file_pattern:
            # Per-file mode
            filename = os.path.basename(file_info.path.rstrip("/"))
            basename = os.path.splitext(filename)[0]
            return self.params.control_file_pattern.replace("{basename}", basename)
        else:
            # Per-folder mode (fixed name)
            return self.params.control_file_pattern

    def _build_hive_partition_path(self) -> str:
        """
        Build Hive partition path from process date and raw_partition_columns.

        Returns:
            Hive partition path like "date=2025-11-09/" or "year=2025/month=11/day=09/"
        """
        from datetime import datetime

        now = datetime.now()
        num_cols = len(self.params.raw_partition_columns)

        if num_cols == 1:
            # Single date column: date=2025-11-09/
            col_name = self.params.raw_partition_columns[0]
            return f"{col_name}={now.strftime('%Y-%m-%d')}/"

        elif num_cols == 3:
            # Hierarchical date: year=2025/month=11/day=09/
            return (f"{self.params.raw_partition_columns[0]}={now.year}/"
                    f"{self.params.raw_partition_columns[1]}={now.month:02d}/"
                    f"{self.params.raw_partition_columns[2]}={now.day:02d}/")

        elif num_cols == 4:
            # Hierarchical datetime: year=2025/month=11/day=09/hour=14/
            return (f"{self.params.raw_partition_columns[0]}={now.year}/"
                    f"{self.params.raw_partition_columns[1]}={now.month:02d}/"
                    f"{self.params.raw_partition_columns[2]}={now.day:02d}/"
                    f"{self.params.raw_partition_columns[3]}={now.hour:02d}/")

        else:
            raise ValueError(
                f"raw_partition_columns must have 1, 3, or 4 items. "
                f"Got {num_cols}: {self.params.raw_partition_columns}"
            )

    def _build_raw_path(self, file_info: FileInfo) -> str:
        """
        Build the raw layer path for a file.

        Raw layer is ALWAYS partitioned by process date (receipt date) using Hive partitioning.
        Business dates are extracted as columns in bronze layer.

        Returns the FULL ABFSS path for both fsspec operations and batch table storage.
        """
        filename = os.path.basename(file_info.path)

        # Build Hive partition path
        partition_path = self._build_hive_partition_path()

        # Return full path with Hive partitions
        return f"{self.raw_landing_full.rstrip('/')}/{partition_path}{filename}"

    def _extract_date_with_regex(self, path: str) -> Optional[str]:
        """
        Extract date from path using regex pattern.

        Supports:
        - Single capture group: r"daily_sales_(\d{8})" → "20250107"
        - Multiple capture groups: r"(\d{4})/(\d{2})/(\d{2})" → "20250107"

        Args:
            path: Full file path to extract date from

        Returns:
            Date string in YYYYMMDD format, or None if no match
        """
        if not self.params.date_regex:
            return None

        try:
            match = re.search(self.params.date_regex, path)
            if not match:
                self.logger.warning(f"Date regex did not match path: {path}")
                return None

            # Get all capture groups
            groups = match.groups()

            if not groups:
                self.logger.warning(f"Date regex matched but has no capture groups: {self.params.date_regex}")
                return None

            if len(groups) == 1:
                # Single group - should be YYYYMMDD format
                date_str = groups[0]
            elif len(groups) == 3:
                # Three groups - assume YYYY, MM, DD
                date_str = f"{groups[0]}{groups[1]}{groups[2]}"
            else:
                # Concatenate all groups
                date_str = "".join(groups)

            self.logger.debug(f"Extracted date '{date_str}' from path: {path}")
            return date_str

        except Exception as e:
            self.logger.warning(f"Date extraction failed for {path}: {e}")
            return None

    def _sort_files(self, files: List[FileInfo]) -> List[FileInfo]:
        """
        Sort files for sequential processing.

        Sorting strategy:
        - If sort_by is configured: Sort by extracted metadata fields, then modified time (tie-breaker)
        - If no sort_by: Sort by modified time only

        Args:
            files: List of FileInfo objects to sort

        Returns:
            Sorted list of FileInfo objects
        """
        if not files:
            return files

        # If no sort_by configured, sort by modified time only
        if not self.params.sort_by:
            files.sort(key=lambda x: x.modified_ms)
            self.logger.debug(f"Sorted {len(files)} files by modified time")
            return files

        def get_sort_key(file_info: FileInfo) -> tuple:
            """Generate sort key from metadata fields plus modified time"""
            # Extract all metadata for this file
            metadata = self._extract_metadata_from_path(file_info.path)

            # Build sort key tuple from sort_by fields
            sort_values = []
            for field_name in self.params.sort_by:
                value = metadata.get(field_name)
                # Use empty string if not found (sorts first for asc, last for desc)
                sort_values.append(value if value is not None else "")

            # Add modified time as final tie-breaker
            sort_values.append(file_info.modified_ms)

            return tuple(sort_values)

        # Sort with reverse flag based on sort_order
        reverse = (self.params.sort_order == "desc")
        sorted_files = sorted(files, key=get_sort_key, reverse=reverse)

        # Log sorting method
        sort_fields = ", ".join(self.params.sort_by)
        self.logger.debug(
            f"Sorted {len(files)} files by [{sort_fields}] ({self.params.sort_order}) then modified time"
        )

        return sorted_files

    def _sort_folders(self, folders: List[FolderInfo]) -> List[FolderInfo]:
        """
        Sort folders for sequential processing.

        Sorting strategy:
        - If sort_by is configured: Sort by extracted metadata fields, then latest modified time (tie-breaker)
        - If no sort_by: Sort by latest modified time only

        Args:
            folders: List of FolderInfo objects to sort

        Returns:
            Sorted list of FolderInfo objects
        """
        if not folders:
            return folders

        # If no sort_by configured, sort by modified time only
        if not self.params.sort_by:
            folders.sort(key=lambda x: x.latest_modified_ms)
            self.logger.debug(f"Sorted {len(folders)} folders by modified time")
            return folders

        def get_sort_key(folder_info: FolderInfo) -> tuple:
            """Generate sort key from metadata fields plus modified time"""
            # Extract all metadata for this folder
            metadata = self._extract_metadata_from_path(folder_info.path)

            # Build sort key tuple from sort_by fields
            sort_values = []
            for field_name in self.params.sort_by:
                value = metadata.get(field_name)
                # Use empty string if not found (sorts first for asc, last for desc)
                sort_values.append(value if value is not None else "")

            # Add latest modified time as final tie-breaker
            sort_values.append(folder_info.latest_modified_ms)

            return tuple(sort_values)

        # Sort with reverse flag based on sort_order
        reverse = (self.params.sort_order == "desc")
        sorted_folders = sorted(folders, key=get_sort_key, reverse=reverse)

        # Log sorting method
        sort_fields = ", ".join(self.params.sort_by)
        self.logger.debug(
            f"Sorted {len(folders)} folders by [{sort_fields}] ({self.params.sort_order}) then modified time"
        )

        return sorted_folders

    def _extract_metadata_from_path(self, path: str) -> dict:
        """
        Extract metadata values from file/folder path using configured patterns.

        Args:
            path: Full file or folder path

        Returns:
            Dict mapping metadata field names to extracted values
        """
        metadata = {}

        if not self.params.filename_metadata:
            return metadata

        for pattern in self.params.filename_metadata:
            field_name = pattern["name"]
            regex = pattern["regex"]

            try:
                match = re.search(regex, path)
                if match:
                    groups = match.groups()
                    if groups:
                        # Single group or concatenate multiple groups
                        if len(groups) == 1:
                            value = groups[0]
                        else:
                            value = "".join(groups)

                        # Store raw string value (type conversion happens in loader)
                        metadata[field_name] = value
                        self.logger.debug(f"Extracted {field_name}='{value}' from: {path}")
                    else:
                        self.logger.warning(f"Regex matched but no capture groups for {field_name}: {regex}")
                else:
                    self.logger.debug(f"Regex did not match for {field_name}: {path}")
            except Exception as e:
                self.logger.warning(f"Metadata extraction failed for {field_name} on {path}: {e}")

        return metadata

    def _extract_files_to_raw(
        self, files: List[FileInfo]
    ) -> Generator[BatchExtractionResult, None, None]:
        """Move validated files from inbound to raw, yielding batch for each file"""
        first_file_path = None
        extracted_count = 0

        for file_info in files:
            # Track first file for cleanup
            if first_file_path is None:
                first_file_path = file_info.path

            # Build destination path in raw
            raw_path = self._build_raw_path(file_info)

            # Track extraction timing
            start_time = time.time()

            try:
                # Move file
                success = move_file(
                    source_fs=self.source_fs,
                    source_path=file_info.path,
                    dest_fs=self.dest_fs,
                    dest_path=raw_path,
                )

                end_time = time.time()
                duration_ms = int((end_time - start_time) * 1000)

                if success:
                    # Also move control file if it exists
                    if self.params.require_control_file and self._has_control_file(file_info):
                        self._move_control_file(file_info, raw_path)

                    # Yield success batch
                    yield BatchExtractionResult(
                        extraction_id=str(uuid.uuid4()),
                        source_path=file_info.path,
                        destination_path=raw_path,
                        status=ExecutionStatus.COMPLETED,
                        file_count=1,
                        file_size_bytes=file_info.size,
                        promoted_count=1,
                        duration_ms=duration_ms,
                    )
                    extracted_count += 1
                    self.logger.info(f"Extracted: {os.path.basename(file_info.path)}")
                else:
                    # Yield failed batch
                    error = "Move operation failed"
                    yield BatchExtractionResult(
                        extraction_id=str(uuid.uuid4()),
                        source_path=file_info.path,
                        destination_path=raw_path,
                        status=ExecutionStatus.FAILED,
                        file_count=1,
                        file_size_bytes=file_info.size,
                        failed_count=1,
                        duration_ms=duration_ms,
                        error_message=error,
                    )
                    self.logger.error(f"Failed to move {file_info.path}: {error}")

            except Exception as e:
                end_time = time.time()
                duration_ms = int((end_time - start_time) * 1000)
                error = str(e)

                # Yield failed batch
                yield BatchExtractionResult(
                    extraction_id=str(uuid.uuid4()),
                    source_path=file_info.path,
                    destination_path=raw_path,
                    status=ExecutionStatus.FAILED,
                    file_count=1,
                    file_size_bytes=file_info.size,
                    failed_count=1,
                    duration_ms=duration_ms,
                    error_message=error,
                )
                self.logger.error(f"Failed to extract {file_info.path}: {error}")

        # Clean up empty directories in inbound ONCE after all files moved
        if first_file_path and extracted_count > 0:
            cleanup_empty_directories(
                fs=self.source_fs,
                start_path=first_file_path,
                stop_path=self.inbound_full_path,
            )

    def _move_control_file(self, file_info: FileInfo, raw_file_path: str) -> None:
        """Move control file along with data file"""
        try:
            control_file_path = self._get_control_file_path(file_info)
            raw_dir = os.path.dirname(raw_file_path)
            control_filename = os.path.basename(control_file_path)
            raw_control_path = f"{raw_dir}/{control_filename}"

            move_file(
                source_fs=self.source_fs,
                source_path=control_file_path,
                dest_fs=self.dest_fs,
                dest_path=raw_control_path,
            )
            self.logger.debug(f"Moved control file: {raw_control_path}")
        except Exception as e:
            self.logger.warning(f"Failed to move control file: {e}")

    def _write_error_file(self, file_info: FileInfo, error_message: str) -> None:
        """Write error file next to failed file in inbound"""
        try:
            error_file_path = f"{file_info.path}.error"
            error_content = f"""File Validation Error
Time: {datetime.now().isoformat()}
File: {os.path.basename(file_info.path)}
Error: {error_message}
Resource: {self.config.resource_name}
"""
            # Write error file using lakehouse utils
            # Note: lakehouse_utils may not have write_text method, so we log instead
            self.logger.error(
                f"Validation error for {os.path.basename(file_info.path)}: {error_message}"
            )
        except Exception as e:
            self.logger.warning(f"Could not write error file: {e}")

    # =========================================================================
    # FOLDER-BATCH METHODS (for batch_by="folder")
    # =========================================================================

    def _validate_folders(self, folders: List[FolderInfo]) -> ValidationResult:
        """
        Validate folder batches.

        Args:
            folders: List of FolderInfo objects

        Returns:
            ValidationResult with valid, failed, and duplicate folders categorized
        """
        result = ValidationResult()

        # Check for duplicates (check if folder already exists in raw)
        if self.params.duplicate_handling != DuplicateHandling.ALLOW:
            duplicates = self._check_for_duplicate_folders(folders)
            for dup in duplicates:
                result.add_duplicate_folder(dup)

            # Remove duplicates from further processing
            duplicate_names = {f.name for f in duplicates}
            folders = [f for f in folders if f.name not in duplicate_names]

            # Fail on duplicates if configured
            if duplicates and self.params.duplicate_handling == DuplicateHandling.FAIL:
                dup_names = [f.name for f in duplicates]
                raise DuplicateDataError(
                    f"Duplicate folders detected: {', '.join(dup_names)}"
                )

        # For folder batches, control file validation is typically not needed
        # (part files don't have individual control files)
        # All folders are valid
        for folder_info in folders:
            result.add_valid_folder(folder_info)

        return result

    def _check_for_duplicate_folders(self, folders: List[FolderInfo]) -> List[FolderInfo]:
        """
        Check if any folders have already been extracted (across all partitions).

        For folder batches, checks if the FOLDER NAME was previously extracted.

        Args:
            folders: List of FolderInfo objects

        Returns:
            List of folders that are duplicates (already extracted)
        """
        duplicates = []

        for folder_info in folders:
            # Use folder name as the duplicate key (not individual files)
            folder_name = folder_info.name

            # Query extraction logs for this folder name
            if self.extraction_logger.check_file_already_extracted(
                config=self.config,
                filename=folder_name
            ):
                duplicates.append(folder_info)
                self.logger.debug(
                    f"Duplicate: Folder {folder_name} already extracted (found in logs)"
                )

        return duplicates

    def _build_raw_folder_path(self, folder_info: FolderInfo) -> str:
        """
        Build the raw layer path for a folder batch.

        Raw layer is ALWAYS partitioned by process date (receipt date) using Hive partitioning.

        Returns the FULL ABFSS path for both fsspec operations and batch table storage.
        """
        # Build Hive partition path
        partition_path = self._build_hive_partition_path()

        # Return full path with Hive partitions (ends with /)
        return f"{self.raw_landing_full.rstrip('/')}/{partition_path}"

    def _extract_folders_to_raw(
        self, folders: List[FolderInfo]
    ) -> Generator[BatchExtractionResult, None, None]:
        """
        Move validated folder batches from inbound to raw, yielding batch for each folder.

        Args:
            folders: List of FolderInfo objects to extract
        """
        for folder_info in folders:
            # Track extraction timing for this folder batch
            folder_start_time = time.time()

            # Build destination folder path in raw
            raw_folder_path = self._build_raw_folder_path(folder_info)

            self.logger.debug(
                f"Extracting folder: {folder_info.name} ({folder_info.file_count} files)"
            )

            # Move all files in the folder
            files_moved = 0
            files_failed = 0

            for file_info in folder_info.files:
                try:
                    # Build destination file path
                    filename = os.path.basename(file_info.path)
                    dest_file_path = f"{raw_folder_path}{filename}"

                    # Move file
                    success = move_file(
                        source_fs=self.source_fs,
                        source_path=file_info.path,
                        dest_fs=self.dest_fs,
                        dest_path=dest_file_path,
                    )

                    if success:
                        files_moved += 1
                    else:
                        files_failed += 1
                        self.logger.warning(f"Failed to move file: {file_info.path}")

                except Exception as e:
                    files_failed += 1
                    self.logger.warning(f"Failed to move {file_info.path}: {e}")

            # Calculate folder extraction duration
            folder_end_time = time.time()
            folder_duration_ms = int((folder_end_time - folder_start_time) * 1000)

            # Yield batch record
            if files_failed == 0:
                # Success batch
                yield BatchExtractionResult(
                    extraction_id=str(uuid.uuid4()),
                    source_path=folder_info.path,
                    destination_path=raw_folder_path,
                    status=ExecutionStatus.COMPLETED,
                    file_count=files_moved,
                    file_size_bytes=folder_info.total_size,
                    promoted_count=files_moved,
                    duration_ms=folder_duration_ms,
                )
                self.logger.debug(f"✓ Extracted folder: {folder_info.name} ({files_moved} files)")

                # Clean up empty directories in inbound after successful folder extraction
                if folder_info.files:
                    cleanup_empty_directories(
                        fs=self.source_fs,
                        start_path=folder_info.files[0].path,
                        stop_path=self.inbound_full_path,
                    )
            else:
                # Failed/partial batch
                error = f"Partial failure: {files_moved} succeeded, {files_failed} failed"
                yield BatchExtractionResult(
                    extraction_id=str(uuid.uuid4()),
                    source_path=folder_info.path,
                    destination_path=raw_folder_path,
                    status=ExecutionStatus.FAILED,
                    file_count=folder_info.file_count,
                    file_size_bytes=folder_info.total_size,
                    promoted_count=files_moved,
                    failed_count=files_failed,
                    duration_ms=folder_duration_ms,
                    error_message=error,
                )
                self.logger.error(f"✗ Folder {folder_info.name}: {error}")

    # =========================================================================
    # ALL-BATCH METHODS (for batch_by="all")
    # =========================================================================

    def _build_raw_folder_path_for_all_batch(self) -> str:
        """
        Build the raw layer path for an "all" batch.

        Raw layer is ALWAYS partitioned by process date (receipt date) using Hive partitioning.

        Returns the FULL ABFSS path for both fsspec operations and batch table storage.
        """
        # Build Hive partition path
        partition_path = self._build_hive_partition_path()

        # Return full path with Hive partitions (ends with /)
        return f"{self.raw_landing_full.rstrip('/')}/{partition_path}"

    def _extract_all_to_raw(
        self, files: List[FileInfo]
    ) -> Generator[BatchExtractionResult, None, None]:
        """
        Move all files to raw as one batch, yielding single batch record.

        Args:
            files: List of FileInfo objects to move
        """
        # Track extraction timing for this batch
        batch_start_time = time.time()

        # Build destination folder path
        raw_folder_path = self._build_raw_folder_path_for_all_batch()

        self.logger.info(
            f"Extracting all files as ONE batch: {len(files)} files → {raw_folder_path}"
        )

        # Move all files to the destination folder
        files_moved = 0
        files_failed = 0
        first_file_path = None
        total_bytes = sum(f.size for f in files)

        for file_info in files:
            try:
                # Track first file for cleanup
                if first_file_path is None:
                    first_file_path = file_info.path

                # Build destination file path
                filename = os.path.basename(file_info.path)
                dest_file_path = f"{raw_folder_path}{filename}"

                # Move file
                success = move_file(
                    source_fs=self.source_fs,
                    source_path=file_info.path,
                    dest_fs=self.dest_fs,
                    dest_path=dest_file_path,
                )

                if success:
                    files_moved += 1
                    self.logger.debug(f"Moved: {filename}")
                else:
                    files_failed += 1
                    self.logger.warning(f"Failed to move file: {file_info.path}")

            except Exception as e:
                files_failed += 1
                self.logger.warning(f"Failed to move {file_info.path}: {e}")

        # Calculate batch extraction duration
        batch_end_time = time.time()
        batch_duration_ms = int((batch_end_time - batch_start_time) * 1000)

        # Yield single batch record
        if files_failed == 0:
            yield BatchExtractionResult(
                extraction_id=str(uuid.uuid4()),
                source_path=self.inbound_full_path,
                destination_path=raw_folder_path,
                status=ExecutionStatus.COMPLETED,
                file_count=files_moved,
                file_size_bytes=total_bytes,
                promoted_count=files_moved,
                duration_ms=batch_duration_ms,
            )
            self.logger.info(f"✓ Extracted batch ({files_moved} files)")
        else:
            error = f"Partial failure: {files_moved} succeeded, {files_failed} failed"
            yield BatchExtractionResult(
                extraction_id=str(uuid.uuid4()),
                source_path=self.inbound_full_path,
                destination_path=raw_folder_path,
                status=ExecutionStatus.FAILED,
                file_count=len(files),
                file_size_bytes=total_bytes,
                promoted_count=files_moved,
                failed_count=files_failed,
                duration_ms=batch_duration_ms,
                error_message=error,
            )
            self.logger.error(f"✗ Batch extraction failed: {error}")

        # Clean up empty directories in inbound ONCE after all files moved
        if first_file_path and files_moved > 0:
            cleanup_empty_directories(
                fs=self.source_fs,
                start_path=first_file_path,
                stop_path=self.inbound_full_path,
            )
