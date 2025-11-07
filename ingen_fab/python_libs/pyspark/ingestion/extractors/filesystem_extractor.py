# Filesystem Extractor
# Validates and promotes external files from inbound → raw layer

import logging
import os
import re
from datetime import datetime
from typing import List, Optional

from pyspark.sql import SparkSession

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
    DatePartitionUtils,
    FilePatternUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.config import (
    FileSystemExtractionParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.constants import DuplicateHandling
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    DuplicateFilesError,
    ErrorContext,
    ExtractionError,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo, lakehouse_utils

logger = logging.getLogger(__name__)


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


class PromotionResult:
    """Result of promoting files from inbound to raw"""

    def __init__(self):
        self.promoted_count: int = 0
        self.failed_count: int = 0
        self.duplicate_count: int = 0
        self.promoted_files: List[str] = []
        self.failed_files: List[tuple[str, str]] = []  # (file_path, error)
        self.total_files_count: int = 0  # Total number of files promoted
        self.error_message: Optional[str] = None  # Validation failure message (not an exception)

    def summary(self) -> str:
        if self.total_files_count > 0:
            return (
                f"Extraction: {self.promoted_count} folders ({self.total_files_count} files) → {self.promoted_count} batches"
            )
        else:
            return (
                f"Promoted: {self.promoted_count}, "
                f"Failed: {self.failed_count}, "
                f"Duplicates: {self.duplicate_count}"
            )


class FileSystemExtractor:
    """
    Extracts (validates and promotes) files from inbound to raw layer.

    This extractor:
    1. Discovers files in inbound folder
    2. Validates control files, duplicates, format
    3. Moves validated files to raw layer (organized by date)
    4. Leaves failed files in inbound with error details
    """

    def __init__(
        self,
        resource_config: ResourceConfig,
        spark: SparkSession,
        location_resolver=None,  # Not used for filesystem
        logger_instance=None,  # Optional context-aware logger
    ):
        """
        Initialize FileSystemExtractor.

        Args:
            resource_config: Resource configuration
            spark: Spark session
            location_resolver: Not used (kept for interface compatibility)
            logger_instance: Optional logger instance with resource context (ConfigLoggerAdapter)
        """
        self.config = resource_config
        self.spark = spark

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

        # Get lakehouse utils
        workspace = resource_config.source_config.connection_params.get("workspace_name")
        lakehouse = resource_config.source_config.connection_params.get("lakehouse_name")

        if not workspace or not lakehouse:
            raise ValueError(
                "source_config.connection_params must have workspace_name and lakehouse_name"
            )

        self.lakehouse = lakehouse_utils(
            target_workspace_name=workspace,
            target_lakehouse_name=lakehouse,
            spark=spark,
        )

        # Use provided logger or fallback to module logger
        self.logger = logger_instance if logger_instance is not None else logger

    def extract(self) -> PromotionResult:
        """
        Extract files/folders from inbound to raw layer.

        Routes to folder-batch or file-level extraction based on batch_by parameter.

        Returns:
            PromotionResult with summary of promoted/failed items
        """
        result = PromotionResult()

        try:
            self.logger.debug(f"Starting extraction: {self.config.resource_name}")
            self.logger.debug(f"Batch mode: {self.params.batch_by}")
            self.logger.debug(f"Inbound path: {self.params.inbound_path}")
            self.logger.debug(f"Raw path: {self.config.raw_file_path}")

            # Route based on batch_by parameter
            if self.params.batch_by == "folder":
                # FOLDER-BATCH EXTRACTION
                return self._extract_folders(result)
            elif self.params.batch_by == "all":
                # ALL-FILES-AS-ONE-BATCH EXTRACTION
                return self._extract_all(result)
            else:
                # FILE-LEVEL EXTRACTION (default)
                return self._extract_files(result)

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

    def _extract_files(self, result: PromotionResult) -> PromotionResult:
        """Extract individual files (batch_by='file')"""
        # Step 1: Discover files in inbound
        files = self._discover_files_in_inbound()

        if not files:
            self.logger.info("No files found in inbound")
            # Check require_files when no files discovered
            if self.params.require_files:
                result.error_message = f"No files found in {self.params.inbound_path} (require_files=True)"
            return result

        self.logger.info(f"Discovered {len(files)} file(s) in inbound")

        # Step 2: Validate files
        validation = self._validate_files(files)

        # Step 3: Handle validation results
        if validation.failed_files:
            self.logger.warning(
                f"{len(validation.failed_files)} file(s) failed validation"
            )
            for file_info, error in validation.failed_files:
                self._write_error_file(file_info, error)
                result.failed_files.append((file_info.path, error))
                result.failed_count += 1

        if validation.duplicate_files:
            result.duplicate_count = len(validation.duplicate_files)
            dup_names = [os.path.basename(f.path) for f in validation.duplicate_files]
            self.logger.warning(f"Skipping {result.duplicate_count} duplicate(s): {', '.join(dup_names)}")

        # Check require_files BEFORE promotion
        if self.params.require_files and len(validation.valid_files) == 0:
            result.error_message = (
                f"No valid files in {self.params.inbound_path} (require_files=True): "
                f"{len(files)} discovered, {len(validation.failed_files)} failed validation, "
                f"{len(validation.duplicate_files)} duplicates"
            )
            return result

        # Step 4: Promote valid files to raw
        if validation.valid_files:
            self._promote_files_to_raw(validation.valid_files, result)
            result.total_files_count = result.promoted_count  # For file-level, count = promoted_count

        self.logger.info(f"Extraction complete: {result.summary()}")
        return result

    def _extract_folders(self, result: PromotionResult) -> PromotionResult:
        """Extract folder batches (batch_by='folder')"""
        # Step 1: Discover folders in inbound
        folders = self._discover_folders_in_inbound()

        if not folders:
            self.logger.info("No folders found in inbound")
            # Check require_files when no folders discovered
            if self.params.require_files:
                result.error_message = f"No folders found in {self.params.inbound_path} (require_files=True)"
            return result

        self.logger.info(f"Discovered {len(folders)} folder(s) in inbound")
        total_files = sum(f.file_count for f in folders)
        self.logger.info(f"Total files across folders: {total_files}")

        # Step 2: Validate folders
        validation = self._validate_folders(folders)

        # Step 3: Handle validation results
        if validation.failed_folders:
            self.logger.warning(
                f"{len(validation.failed_folders)} folder(s) failed validation"
            )
            for folder_info, error in validation.failed_folders:
                result.failed_files.append((folder_info.path, error))
                result.failed_count += 1

        if validation.duplicate_folders:
            result.duplicate_count = len(validation.duplicate_folders)
            # Show full paths for better clarity
            dup_paths = [f.path for f in validation.duplicate_folders]
            self.logger.warning(
                f"Skipping {result.duplicate_count} duplicate folder(s):\n  " +
                "\n  ".join(dup_paths)
            )

        # Check require_files BEFORE promotion
        if self.params.require_files and len(validation.valid_folders) == 0:
            result.error_message = (
                f"No valid folders in {self.params.inbound_path} (require_files=True): "
                f"{len(folders)} discovered, {len(validation.failed_folders)} failed validation, "
                f"{len(validation.duplicate_folders)} duplicates"
            )
            return result

        # Step 4: Promote valid folders to raw
        if validation.valid_folders:
            self._promote_folders_to_raw(validation.valid_folders, result)

        self.logger.info(f"Extraction complete: {result.summary()}")
        return result

    def _extract_all(self, result: PromotionResult) -> PromotionResult:
        """Extract all files as one batch (batch_by='all')"""
        # Step 1: Discover all files in inbound directory
        files = self._discover_files_in_inbound()

        if not files:
            self.logger.info("No files found in inbound")
            # Check require_files when no files discovered
            if self.params.require_files:
                result.error_message = f"No files found in {self.params.inbound_path} (require_files=True)"
            return result

        self.logger.info(f"Discovered {len(files)} file(s) in inbound (treating as ONE batch)")

        # Step 2: Check for duplicate batch
        is_duplicate = False
        if self.params.duplicate_handling != DuplicateHandling.ALLOW:
            # Build expected raw folder path for this batch
            raw_folder_path = self._build_raw_folder_path_for_all_batch()

            # Check if folder already exists and has files
            try:
                if not self.lakehouse.is_directory_empty(raw_folder_path):
                    # Folder exists and has files - this is a duplicate
                    self.logger.warning(f"Duplicate batch detected: {raw_folder_path} already exists")
                    result.duplicate_count = len(files)
                    is_duplicate = True

                    # Fail if configured
                    if self.params.duplicate_handling == DuplicateHandling.FAIL:
                        raise DuplicateFilesError(
                            f"Duplicate batch detected: folder {raw_folder_path} already processed"
                        )
            except Exception:
                # Folder doesn't exist - not a duplicate, proceed
                pass

        # Check require_files BEFORE promotion
        if self.params.require_files and is_duplicate:
            result.error_message = (
                f"No valid batches in {self.params.inbound_path} (require_files=True): "
                f"duplicate batch detected"
            )
            return result

        # If duplicate and skip mode, return early
        if is_duplicate:
            return result

        # Step 3: Move all files to raw as one batch
        self._promote_all_to_raw(files, result)
        result.total_files_count = len(files)  # For all-batch, total is all files

        self.logger.info(f"Extraction complete: {result.summary()}")
        return result

    def _discover_files_in_inbound(self) -> List[FileInfo]:
        """Discover files in inbound folder matching discovery pattern"""
        pattern = self.params.discovery_pattern or "*"

        files = self.lakehouse.list_files_with_metadata(
            directory_path=self.params.inbound_path,
            pattern=pattern,
            recursive=self.params.recursive,
        )

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
            candidate_folders = self.lakehouse.list_directories(
                directory_path=self.params.inbound_path,
                recursive=True
            )
            self.logger.info(
                f"Checking {len(candidate_folders)} folders (recursive, no depth limit)"
            )

        # Process each candidate folder
        for folder_path in candidate_folders:
            try:
                # Check if folder has files
                files_in_folder = self.lakehouse.list_files_with_metadata(
                    directory_path=folder_path,
                    pattern=pattern,
                    recursive=False  # Only files directly in this folder
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
            return [self.params.inbound_path]

        # Start with inbound path
        current_level_folders = [self.params.inbound_path]

        # Traverse depth levels
        for level in range(depth):
            next_level_folders = []
            for folder in current_level_folders:
                try:
                    # Get immediate subdirectories (non-recursive)
                    subdirs = self.lakehouse.list_directories(
                        directory_path=folder,
                        recursive=False
                    )
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
                raise DuplicateFilesError(
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
        Check if any files already exist in raw layer.

        Returns:
            List of files that are duplicates (already exist in raw)
        """
        duplicates = []

        for file_info in files:
            # Build expected raw path
            raw_file_path = self._build_raw_path(file_info)

            # Check if file already exists in raw
            if self.lakehouse.file_exists(raw_file_path):
                duplicates.append(file_info)
                self.logger.debug(
                    f"Duplicate: {os.path.basename(file_info.path)} already exists in raw"
                )

        return duplicates

    def _has_control_file(self, file_info: FileInfo) -> bool:
        """Check if control file exists for this file"""
        control_file_path = self._get_control_file_path(file_info)
        return self.lakehouse.file_exists(control_file_path)

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

    def _build_raw_path(self, file_info: FileInfo) -> str:
        """
        Build the raw layer path for a file.

        Uses output_structure template with date extraction via regex or process date.
        """
        filename = os.path.basename(file_info.path)

        # Get date string: either process date or extracted from path via regex
        date_str = None
        if self.params.use_process_date:
            # Use current execution date (for snapshots without dates)
            date_str = datetime.now().strftime("%Y%m%d")
            self.logger.debug(f"Using process date for partitioning: {date_str}")
        elif self.params.date_regex:
            # Extract date from full file path using regex
            date_str = self._extract_date_with_regex(file_info.path)

        # Build output path using template
        if date_str and self.params.output_structure:
            # Parse date and build folder structure
            folder_path = self._resolve_output_structure(date_str)
            return f"{self.config.raw_file_path.rstrip('/')}/{folder_path}/{filename}"
        else:
            # No date - put directly in raw
            return f"{self.config.raw_file_path.rstrip('/')}/{filename}"

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

    def _resolve_output_structure(self, date_str: str) -> str:
        """
        Resolve output_structure template with date components.

        Args:
            date_str: Date string in format YYYYMMDD or YYYY-MM-DD

        Returns:
            Resolved path like "2025/01/07"
        """
        # Normalize date string to YYYYMMDD
        normalized = date_str.replace("-", "").replace("/", "")

        if len(normalized) != 8:
            self.logger.warning(f"Invalid date format: {date_str}, expected YYYYMMDD")
            return ""

        year = normalized[0:4]
        month = normalized[4:6]
        day = normalized[6:8]

        # Replace placeholders
        output = self.params.output_structure
        output = output.replace("{YYYY}", year)
        output = output.replace("{MM}", month)
        output = output.replace("{DD}", day)

        return output.strip("/")

    def _promote_files_to_raw(
        self, files: List[FileInfo], result: PromotionResult
    ) -> None:
        """Move validated files from inbound to raw"""
        for file_info in files:
            try:
                # Build destination path in raw
                raw_path = self._build_raw_path(file_info)

                # Move file
                success = self.lakehouse.move_file(file_info.path, raw_path)

                if success:
                    result.promoted_count += 1
                    result.promoted_files.append(raw_path)
                    self.logger.info(
                        f"Promoted: {os.path.basename(file_info.path)} → {raw_path}"
                    )

                    # Also move control file if it exists
                    if self.params.require_control_file and self._has_control_file(file_info):
                        self._move_control_file(file_info, raw_path)
                else:
                    error = "Move operation failed"
                    result.failed_count += 1
                    result.failed_files.append((file_info.path, error))
                    self.logger.error(f"Failed to move {file_info.path}: {error}")

            except Exception as e:
                error = str(e)
                result.failed_count += 1
                result.failed_files.append((file_info.path, error))
                self.logger.error(f"Failed to promote {file_info.path}: {error}")

    def _move_control_file(self, file_info: FileInfo, raw_file_path: str) -> None:
        """Move control file along with data file"""
        try:
            control_file_path = self._get_control_file_path(file_info)
            raw_dir = os.path.dirname(raw_file_path)
            control_filename = os.path.basename(control_file_path)
            raw_control_path = f"{raw_dir}/{control_filename}"

            self.lakehouse.move_file(control_file_path, raw_control_path)
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
                raise DuplicateFilesError(
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
        Check if any folders already exist in raw layer.

        Args:
            folders: List of FolderInfo objects

        Returns:
            List of folders that are duplicates (already exist in raw)
        """
        duplicates = []

        for folder_info in folders:
            # Build expected raw folder path
            raw_folder_path = self._build_raw_folder_path(folder_info)

            # Check if folder already exists in raw (check if non-empty)
            try:
                if not self.lakehouse.is_directory_empty(raw_folder_path):
                    duplicates.append(folder_info)
                    self.logger.debug(
                        f"Duplicate: Folder {folder_info.name} already exists in raw"
                    )
            except Exception:
                # Folder doesn't exist - not a duplicate
                pass

        return duplicates

    def _build_raw_folder_path(self, folder_info: FolderInfo) -> str:
        """
        Build the raw layer path for a folder batch.

        Uses output_structure template with date extraction via regex or process date.
        """
        # Get date string: either process date or extracted from path via regex
        date_str = None
        if self.params.use_process_date:
            # Use current execution date (for snapshots)
            date_str = datetime.now().strftime("%Y%m%d")
            self.logger.debug(f"Using process date for partitioning: {date_str}")
        elif self.params.date_regex:
            # Extract from folder path using regex
            date_str = self._extract_date_with_regex(folder_info.path)

        # Build output path using template
        if date_str and self.params.output_structure:
            # Parse date and build folder structure
            folder_path = self._resolve_output_structure(date_str)
            return f"{self.config.raw_file_path.rstrip('/')}/{folder_path}/"
        else:
            # No date - use folder name
            return f"{self.config.raw_file_path.rstrip('/')}/{folder_info.name}/"

    def _promote_folders_to_raw(
        self, folders: List[FolderInfo], result: PromotionResult
    ) -> None:
        """
        Move validated folder batches from inbound to raw.

        Args:
            folders: List of FolderInfo objects to promote
            result: PromotionResult to update with progress
        """
        for folder_info in folders:
            try:
                # Build destination folder path in raw
                raw_folder_path = self._build_raw_folder_path(folder_info)

                self.logger.debug(
                    f"Promoting folder: {folder_info.name} ({folder_info.file_count} files)"
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
                        success = self.lakehouse.move_file(file_info.path, dest_file_path)

                        if success:
                            files_moved += 1
                        else:
                            files_failed += 1
                            self.logger.warning(f"Failed to move file: {file_info.path}")

                    except Exception as e:
                        files_failed += 1
                        self.logger.warning(f"Failed to move {file_info.path}: {e}")

                # Update result
                if files_failed == 0:
                    result.promoted_count += 1
                    result.promoted_files.append(raw_folder_path)
                    result.total_files_count += files_moved  # Track total files
                    self.logger.debug(
                        f"✓ Promoted folder: {folder_info.name} → {raw_folder_path} ({files_moved} files)"
                    )
                else:
                    error = f"Partial failure: {files_moved} succeeded, {files_failed} failed"
                    result.failed_count += 1
                    result.failed_files.append((folder_info.path, error))
                    self.logger.error(
                        f"✗ Folder {folder_info.name}: {error}"
                    )

            except Exception as e:
                error = str(e)
                result.failed_count += 1
                result.failed_files.append((folder_info.path, error))
                self.logger.error(f"Failed to promote folder {folder_info.name}: {error}")

    # =========================================================================
    # ALL-BATCH METHODS (for batch_by="all")
    # =========================================================================

    def _build_raw_folder_path_for_all_batch(self) -> str:
        """
        Build the raw layer path for an "all" batch.

        Uses process date or regex extraction from inbound path.
        """
        # Get date string using process date or regex
        date_str = None
        if self.params.use_process_date:
            date_str = datetime.now().strftime("%Y%m%d")
            self.logger.debug(f"Using process date for batch folder: {date_str}")
        elif self.params.date_regex:
            # Try to extract from inbound path itself using regex
            date_str = self._extract_date_with_regex(self.params.inbound_path)

        # Build output path using template
        if date_str and self.params.output_structure:
            folder_path = self._resolve_output_structure(date_str)
            return f"{self.config.raw_file_path.rstrip('/')}/{folder_path}/"
        else:
            # No date - use a generic folder name
            return f"{self.config.raw_file_path.rstrip('/')}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}/"

    def _promote_all_to_raw(
        self, files: List[FileInfo], result: PromotionResult
    ) -> None:
        """
        Move all files to raw as one batch.

        Args:
            files: List of FileInfo objects to move
            result: PromotionResult to update with progress
        """
        # Build destination folder path
        raw_folder_path = self._build_raw_folder_path_for_all_batch()

        self.logger.info(
            f"Promoting all files as ONE batch: {len(files)} file(s) → {raw_folder_path}"
        )

        # Move all files to the destination folder
        files_moved = 0
        files_failed = 0

        for file_info in files:
            try:
                # Build destination file path
                filename = os.path.basename(file_info.path)
                dest_file_path = f"{raw_folder_path}{filename}"

                # Move file
                success = self.lakehouse.move_file(file_info.path, dest_file_path)

                if success:
                    files_moved += 1
                    self.logger.debug(f"Moved: {filename}")
                else:
                    files_failed += 1
                    self.logger.warning(f"Failed to move file: {file_info.path}")

            except Exception as e:
                files_failed += 1
                self.logger.warning(f"Failed to move {file_info.path}: {e}")

        # Update result
        if files_failed == 0:
            result.promoted_count = 1  # ONE batch
            result.promoted_files.append(raw_folder_path)
            self.logger.info(
                f"✓ Promoted batch → {raw_folder_path} ({files_moved} files)"
            )
        else:
            error = f"Partial failure: {files_moved} succeeded, {files_failed} failed"
            result.failed_count = 1
            result.failed_files.append((self.params.inbound_path, error))
            self.logger.error(f"✗ Batch promotion failed: {error}")
