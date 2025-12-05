"""Unit tests for FileSystemExtractor watermark functionality.

Tests the new watermark-based incremental extraction feature including:
- Config parameters (move_source_file, incremental_column)
- Watermark filtering (files and folders)
- Copy vs move behavior
- Duplicate check bypass in watermark mode
- Watermark updates after extraction
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor import (
    FileSystemExtractor,
    FolderInfo,
)
from ingen_fab.python_libs.pyspark.ingestion.common.config import (
    ResourceConfig,
    SourceConfig,
    FileFormatParams,
    FileSystemExtractionParams,
)
from ingen_fab.python_libs.common.fsspec_utils import FilesystemConnection
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo


# Module path for patching (correct path with 'extraction.extractors')
MODULE_PATH = "ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.filesystem_extractor"


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_extraction_logger():
    """Mock extraction logger with watermark methods."""
    logger = Mock()
    logger.get_watermark.return_value = None  # First run by default
    logger.update_watermark.return_value = None
    logger.check_file_already_extracted.return_value = False
    return logger


@pytest.fixture
def mock_source_conn():
    """Mock source filesystem connection."""
    mock_fs = Mock()
    mock_fs.info.return_value = {"last_modified": datetime(2025, 1, 15, 10, 0, 0)}
    return FilesystemConnection(
        fs=mock_fs,
        base_url="abfss://source_ws@onelake.dfs.fabric.microsoft.com/source_lh.Lakehouse"
    )


@pytest.fixture
def mock_dest_conn():
    """Mock destination filesystem connection."""
    return FilesystemConnection(
        fs=Mock(),
        base_url="abfss://dest_ws@onelake.dfs.fabric.microsoft.com/dest_lh.Lakehouse"
    )


@pytest.fixture
def traditional_config():
    """Traditional config (move files, no watermark)."""
    return ResourceConfig(
        resource_name="test_resource",
        source_name="test_source",
        source_config=SourceConfig(
            source_type="filesystem",
            source_connection_params={
                "workspace_name": "test_workspace",
                "lakehouse_name": "test_lakehouse",
            },
        ),
        source_extraction_params=FileSystemExtractionParams(
            inbound_path="Files/inbound/test/",
            discovery_pattern="*.csv",
            move_source_file=True,  # Default
            incremental_column=None,  # Default - no watermark
        ),
        extract_path="Files/raw/test/",
        extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
        extract_storage_workspace="test_storage_ws",
        extract_storage_lakehouse="test_storage_lh",
        extract_error_path="Files/error/test/",
        target_write_mode="append",
    )


@pytest.fixture
def watermark_config():
    """Watermark mode config (copy files, use watermark)."""
    return ResourceConfig(
        resource_name="test_resource",
        source_name="test_source",
        source_config=SourceConfig(
            source_type="filesystem",
            source_connection_params={
                "workspace_name": "test_workspace",
                "lakehouse_name": "test_lakehouse",
            },
        ),
        source_extraction_params=FileSystemExtractionParams(
            inbound_path="Files/inbound/test/",
            discovery_pattern="*.csv",
            move_source_file=False,  # Copy only
            incremental_column="modified_time",  # Watermark mode
        ),
        extract_path="Files/raw/test/",
        extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
        extract_storage_workspace="test_storage_ws",
        extract_storage_lakehouse="test_storage_lh",
        extract_error_path="Files/error/test/",
        target_write_mode="append",
    )


@pytest.fixture
def sample_files():
    """Sample FileInfo objects for testing."""
    return [
        FileInfo(
            path="abfss://test/Files/inbound/old_file.csv",
            name="old_file.csv",
            size=100,
            modified_ms=1704067200000,  # 2024-01-01 00:00:00
        ),
        FileInfo(
            path="abfss://test/Files/inbound/new_file.csv",
            name="new_file.csv",
            size=200,
            modified_ms=1736899200000,  # 2025-01-15 00:00:00
        ),
    ]


# ============================================================================
# CONFIG PARAMETER TESTS
# ============================================================================


class TestFileSystemExtractionParamsWatermark:
    """Tests for new watermark config parameters."""

    def test_default_move_source_file_is_true(self):
        """move_source_file defaults to True (traditional behavior)."""
        params = FileSystemExtractionParams(inbound_path="/test/")
        assert params.move_source_file is True

    def test_default_incremental_column_is_none(self):
        """incremental_column defaults to None (no watermark)."""
        params = FileSystemExtractionParams(inbound_path="/test/")
        assert params.incremental_column is None

    def test_incremental_column_accepts_modified_time(self):
        """incremental_column='modified_time' is valid."""
        params = FileSystemExtractionParams(
            inbound_path="/test/",
            incremental_column="modified_time"
        )
        assert params.incremental_column == "modified_time"

    def test_incremental_column_rejects_invalid_values(self):
        """incremental_column rejects invalid column names."""
        with pytest.raises(ValueError) as exc_info:
            FileSystemExtractionParams(
                inbound_path="/test/",
                incremental_column="invalid_column"
            )
        assert "must be 'modified_time' or a filename_metadata field name" in str(exc_info.value)

    def test_move_source_file_false_is_valid(self):
        """move_source_file=False is valid (copy mode)."""
        params = FileSystemExtractionParams(
            inbound_path="/test/",
            move_source_file=False
        )
        assert params.move_source_file is False


# ============================================================================
# WATERMARK FILTERING TESTS (FILES)
# ============================================================================


class TestFileWatermarkFiltering:
    """Tests for _filter_files_by_watermark method."""

    def test_filter_returns_all_files_when_no_incremental_column(
        self, traditional_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_files
    ):
        """When incremental_column is None, return all files."""
        extractor = FileSystemExtractor(
            resource_config=traditional_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_files_by_watermark(sample_files)

        assert len(result) == 2
        mock_extraction_logger.get_watermark.assert_not_called()

    def test_filter_returns_all_files_on_first_run(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_files
    ):
        """When no watermark exists, return all files (first run)."""
        mock_extraction_logger.get_watermark.return_value = None  # No watermark yet

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_files_by_watermark(sample_files)

        assert len(result) == 2
        mock_extraction_logger.get_watermark.assert_called_once()

    def test_filter_excludes_files_older_than_watermark(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_files
    ):
        """Files with modified_time <= watermark are filtered out."""
        # Set watermark to 2025-01-10 - old_file should be excluded
        mock_extraction_logger.get_watermark.return_value = datetime(2025, 1, 10, 0, 0, 0)

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_files_by_watermark(sample_files)

        # Only new_file (2025-01-15) should pass the filter
        assert len(result) == 1
        assert result[0].name == "new_file.csv"

    def test_filter_includes_files_newer_than_watermark(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Files with modified_time > watermark are included."""
        # Set watermark to 2024-12-01 - both files should be included
        mock_extraction_logger.get_watermark.return_value = datetime(2024, 12, 1, 0, 0, 0)

        files = [
            FileInfo(
                path="abfss://test/Files/inbound/file1.csv",
                name="file1.csv",
                size=100,
                modified_ms=1704067200000,  # 2024-01-01 (older than watermark)
            ),
            FileInfo(
                path="abfss://test/Files/inbound/file2.csv",
                name="file2.csv",
                size=200,
                modified_ms=1736899200000,  # 2025-01-15 (newer than watermark)
            ),
        ]

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_files_by_watermark(files)

        # Only file2 (2025-01-15) should pass
        assert len(result) == 1
        assert result[0].name == "file2.csv"


# ============================================================================
# COPY VS MOVE TESTS
# ============================================================================


class TestCopyVsMoveExtraction:
    """Tests for copy vs move behavior."""

    @patch(f"{MODULE_PATH}.cleanup_empty_directories")
    @patch(f"{MODULE_PATH}.move_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_uses_move_file_when_move_source_file_true(
        self, mock_glob, mock_move, mock_cleanup,
        traditional_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Default behavior uses move_file (copy + delete)."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1736899200000,
            )
        ]
        mock_move.return_value = True

        extractor = FileSystemExtractor(
            resource_config=traditional_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        list(extractor.extract())

        mock_move.assert_called_once()

    @patch(f"{MODULE_PATH}.cleanup_empty_directories")
    @patch(f"{MODULE_PATH}.copy_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_uses_copy_file_when_move_source_file_false(
        self, mock_glob, mock_copy, mock_cleanup,
        watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """When move_source_file=False, uses copy_file (no delete)."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1736899200000,
            )
        ]
        mock_copy.return_value = True

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        list(extractor.extract())

        mock_copy.assert_called_once()

    @patch(f"{MODULE_PATH}.cleanup_empty_directories")
    @patch(f"{MODULE_PATH}.copy_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_no_cleanup_when_copy_mode(
        self, mock_glob, mock_copy, mock_cleanup,
        watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Skips cleanup_empty_directories when using copy mode."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1736899200000,
            )
        ]
        mock_copy.return_value = True

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        list(extractor.extract())

        mock_cleanup.assert_not_called()

    @patch(f"{MODULE_PATH}.cleanup_empty_directories")
    @patch(f"{MODULE_PATH}.move_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_cleanup_runs_when_move_mode(
        self, mock_glob, mock_move, mock_cleanup,
        traditional_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """cleanup_empty_directories runs when using move mode."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1736899200000,
            )
        ]
        mock_move.return_value = True

        extractor = FileSystemExtractor(
            resource_config=traditional_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        list(extractor.extract())

        mock_cleanup.assert_called_once()


# ============================================================================
# DUPLICATE CHECK BYPASS TESTS
# ============================================================================


class TestWatermarkDuplicateBypass:
    """Tests for skipping duplicate check in watermark mode."""

    @patch(f"{MODULE_PATH}.glob")
    def test_skips_duplicate_check_when_incremental_column_set(
        self, mock_glob, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """When incremental_column is set, skip batch log duplicate check."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1736899200000,
            )
        ]
        # Even if check_file_already_extracted returns True, it shouldn't be called
        mock_extraction_logger.check_file_already_extracted.return_value = True

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        # Call _validate_files directly to test duplicate checking
        files = mock_glob.return_value
        result = extractor._validate_files(files)

        # In watermark mode, duplicate check is skipped
        mock_extraction_logger.check_file_already_extracted.assert_not_called()
        assert len(result.valid_files) == 1

    @patch(f"{MODULE_PATH}.glob")
    def test_duplicate_check_still_runs_without_watermark(
        self, mock_glob, traditional_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Traditional mode still checks for duplicates."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/data.csv",
                name="data.csv",
                size=100,
                modified_ms=1736899200000,
            )
        ]
        mock_extraction_logger.check_file_already_extracted.return_value = True  # File is duplicate

        extractor = FileSystemExtractor(
            resource_config=traditional_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        files = mock_glob.return_value
        result = extractor._validate_files(files)

        # In traditional mode, duplicate check runs
        mock_extraction_logger.check_file_already_extracted.assert_called()
        # File should be marked as duplicate
        assert len(result.duplicate_files) == 1
        assert len(result.valid_files) == 0


# ============================================================================
# WATERMARK UPDATE TESTS
# ============================================================================


class TestWatermarkUpdate:
    """Tests for _update_watermark_for_files method."""

    def test_no_update_when_incremental_column_not_set(
        self, traditional_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_files
    ):
        """Skip watermark update when not in watermark mode."""
        extractor = FileSystemExtractor(
            resource_config=traditional_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        extractor._update_watermark_for_files(sample_files, extract_batch_id="test-batch-123")

        mock_extraction_logger.update_watermark.assert_not_called()

    def test_updates_watermark_with_max_modified_time(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_files
    ):
        """Updates watermark with max modified_time from extracted files."""
        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        extractor._update_watermark_for_files(sample_files, extract_batch_id="test-batch-123")

        mock_extraction_logger.update_watermark.assert_called_once()
        call_args = mock_extraction_logger.update_watermark.call_args
        # Verify max modified time is used (new_file's timestamp)
        assert call_args.kwargs["incremental_column"] == "modified_time"

    def test_no_update_when_empty_file_list(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Skip watermark update when no files extracted."""
        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        extractor._update_watermark_for_files([], extract_batch_id="test-batch-123")

        mock_extraction_logger.update_watermark.assert_not_called()


# ============================================================================
# FOLDER WATERMARK TESTS
# ============================================================================


class TestFolderWatermarkFiltering:
    """Tests for folder-level watermark filtering."""

    @pytest.fixture
    def sample_folders(self):
        """Sample FolderInfo objects for testing."""
        return [
            FolderInfo(
                path="abfss://test/Files/inbound/2024/01/01/",
                files=[
                    FileInfo(
                        path="abfss://test/Files/inbound/2024/01/01/file1.csv",
                        name="file1.csv",
                        size=100,
                        modified_ms=1704067200000,  # 2024-01-01
                    )
                ]
            ),
            FolderInfo(
                path="abfss://test/Files/inbound/2025/01/15/",
                files=[
                    FileInfo(
                        path="abfss://test/Files/inbound/2025/01/15/file2.csv",
                        name="file2.csv",
                        size=200,
                        modified_ms=1736899200000,  # 2025-01-15
                    )
                ]
            ),
        ]

    def test_filter_folders_returns_all_when_no_incremental_column(
        self, traditional_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_folders
    ):
        """When incremental_column is None, return all folders."""
        extractor = FileSystemExtractor(
            resource_config=traditional_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_folders_by_watermark(sample_folders)

        assert len(result) == 2
        mock_extraction_logger.get_watermark.assert_not_called()

    def test_filter_folders_returns_all_on_first_run(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_folders
    ):
        """When no watermark exists, return all folders (first run)."""
        mock_extraction_logger.get_watermark.return_value = None

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_folders_by_watermark(sample_folders)

        assert len(result) == 2

    def test_filter_folders_by_latest_file_timestamp(
        self, watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn, sample_folders
    ):
        """Filter folders by latest_modified_ms when no control file."""
        # Set watermark to 2025-01-10 - old folder should be excluded
        mock_extraction_logger.get_watermark.return_value = datetime(2025, 1, 10, 0, 0, 0)

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_folders_by_watermark(sample_folders)

        # Only 2025/01/15 folder should pass
        assert len(result) == 1
        assert "2025/01/15" in result[0].path


class TestFolderControlFileReadiness:
    """Tests for folder control file readiness check."""

    @pytest.fixture
    def folder_config_with_control(self):
        """Config with folder batching and control file."""
        return ResourceConfig(
            resource_name="test_resource",
            source_name="test_source",
            source_config=SourceConfig(
                source_type="filesystem",
                source_connection_params={
                    "workspace_name": "test_workspace",
                    "lakehouse_name": "test_lakehouse",
                },
            ),
            source_extraction_params=FileSystemExtractionParams(
                inbound_path="Files/inbound/test/",
                discovery_pattern="*.csv",
                batch_by="folder",
                control_file_pattern="folder.done",
                move_source_file=False,
                incremental_column="modified_time",
            ),
            extract_path="Files/raw/test/",
            extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
            extract_storage_workspace="test_storage_ws",
            extract_storage_lakehouse="test_storage_lh",
            extract_error_path="Files/error/test/",
            target_write_mode="append",
        )

    @patch(f"{MODULE_PATH}.file_exists")
    def test_filter_ready_folders_includes_folders_with_control_file(
        self, mock_file_exists, folder_config_with_control, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """_filter_ready_folders includes folders with control files."""
        mock_file_exists.return_value = True  # Control file exists

        folders = [
            FolderInfo(
                path="abfss://test/Files/inbound/2025/01/15/",
                files=[
                    FileInfo(
                        path="abfss://test/Files/inbound/2025/01/15/file.csv",
                        name="file.csv",
                        size=100,
                        modified_ms=1736899200000,
                    )
                ]
            ),
        ]

        extractor = FileSystemExtractor(
            resource_config=folder_config_with_control,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_ready_folders(folders)

        assert len(result) == 1
        mock_file_exists.assert_called()

    @patch(f"{MODULE_PATH}.file_exists")
    def test_filter_ready_folders_excludes_folders_without_control_file(
        self, mock_file_exists, folder_config_with_control, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """_filter_ready_folders excludes folders without control files."""
        mock_file_exists.return_value = False  # Control file doesn't exist

        folders = [
            FolderInfo(
                path="abfss://test/Files/inbound/2025/01/15/",
                files=[
                    FileInfo(
                        path="abfss://test/Files/inbound/2025/01/15/file.csv",
                        name="file.csv",
                        size=100,
                        modified_ms=1736899200000,
                    )
                ]
            ),
        ]

        extractor = FileSystemExtractor(
            resource_config=folder_config_with_control,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_ready_folders(folders)

        assert len(result) == 0


# ============================================================================
# INTEGRATION-STYLE TESTS
# ============================================================================


class TestWatermarkExtractionFlow:
    """End-to-end tests for watermark extraction flow."""

    @patch(f"{MODULE_PATH}.copy_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_full_watermark_extraction_flow(
        self, mock_glob, mock_copy,
        watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Full flow: discover files -> filter by watermark -> copy -> update watermark."""
        # Setup: one old file, one new file
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/old.csv",
                name="old.csv",
                size=100,
                modified_ms=1704067200000,  # 2024-01-01
            ),
            FileInfo(
                path="abfss://test/Files/inbound/new.csv",
                name="new.csv",
                size=200,
                modified_ms=1736899200000,  # 2025-01-15
            ),
        ]
        # Watermark at 2025-01-10 - only new.csv should be extracted
        mock_extraction_logger.get_watermark.return_value = datetime(2025, 1, 10, 0, 0, 0)
        mock_copy.return_value = True

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        # Only one file should be extracted
        assert len(results) == 1
        assert results[0].file_count == 1
        # copy_file should be called (not move_file)
        mock_copy.assert_called_once()
        # Watermark should be updated
        mock_extraction_logger.update_watermark.assert_called_once()

    @patch(f"{MODULE_PATH}.copy_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_first_run_extracts_all_files(
        self, mock_glob, mock_copy,
        watermark_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """First run (no watermark) extracts all files."""
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/file1.csv",
                name="file1.csv",
                size=100,
                modified_ms=1704067200000,
            ),
            FileInfo(
                path="abfss://test/Files/inbound/file2.csv",
                name="file2.csv",
                size=200,
                modified_ms=1736899200000,
            ),
        ]
        mock_extraction_logger.get_watermark.return_value = None  # First run
        mock_copy.return_value = True

        extractor = FileSystemExtractor(
            resource_config=watermark_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        # Both files should be extracted
        assert len(results) == 2
        assert mock_copy.call_count == 2


# ============================================================================
# FILENAME-BASED WATERMARK TESTS
# ============================================================================


class TestFilenameWatermarkConfig:
    """Tests for filename-based incremental_column configuration."""

    def test_incremental_column_accepts_filename_metadata_field(self):
        """incremental_column can reference a filename_metadata field name."""
        params = FileSystemExtractionParams(
            inbound_path="/test/",
            filename_metadata=[
                {"name": "file_date", "regex": r"(\d{8})", "type": "date", "format": "yyyyMMdd"}
            ],
            incremental_column="file_date",  # References filename_metadata field
        )
        assert params.incremental_column == "file_date"

    def test_incremental_column_rejects_nonexistent_metadata_field(self):
        """incremental_column rejects field names not in filename_metadata."""
        with pytest.raises(ValueError) as exc_info:
            FileSystemExtractionParams(
                inbound_path="/test/",
                filename_metadata=[
                    {"name": "other_field", "regex": r"(\d+)", "type": "int"}
                ],
                incremental_column="file_date",  # Not in filename_metadata
            )
        assert "must be 'modified_time' or a filename_metadata field name" in str(exc_info.value)

    def test_incremental_column_accepts_integer_metadata_field(self):
        """incremental_column can reference an integer type metadata field."""
        params = FileSystemExtractionParams(
            inbound_path="/test/",
            filename_metadata=[
                {"name": "batch_id", "regex": r"batch_(\d+)", "type": "int"}
            ],
            incremental_column="batch_id",
        )
        assert params.incremental_column == "batch_id"


class TestFilenameIncrementalValueExtraction:
    """Tests for _get_incremental_value_from_filename method."""

    @pytest.fixture
    def filename_date_config(self):
        """Config with date-based filename metadata."""
        return ResourceConfig(
            resource_name="test_resource",
            source_name="test_source",
            source_config=SourceConfig(
                source_type="filesystem",
                source_connection_params={
                    "workspace_name": "test_workspace",
                    "lakehouse_name": "test_lakehouse",
                },
            ),
            source_extraction_params=FileSystemExtractionParams(
                inbound_path="Files/inbound/test/",
                discovery_pattern="*.csv",
                filename_metadata=[
                    {"name": "file_date", "regex": r"sales_(\d{8})\.csv", "type": "date", "format": "yyyyMMdd"}
                ],
                incremental_column="file_date",
                move_source_file=False,
            ),
            extract_path="Files/raw/test/",
            extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
            extract_storage_workspace="test_storage_ws",
            extract_storage_lakehouse="test_storage_lh",
            extract_error_path="Files/error/test/",
            target_write_mode="append",
        )

    @pytest.fixture
    def filename_int_config(self):
        """Config with integer-based filename metadata."""
        return ResourceConfig(
            resource_name="test_resource",
            source_name="test_source",
            source_config=SourceConfig(
                source_type="filesystem",
                source_connection_params={
                    "workspace_name": "test_workspace",
                    "lakehouse_name": "test_lakehouse",
                },
            ),
            source_extraction_params=FileSystemExtractionParams(
                inbound_path="Files/inbound/test/",
                discovery_pattern="*.csv",
                filename_metadata=[
                    {"name": "batch_id", "regex": r"batch_(\d+)\.csv", "type": "int"}
                ],
                incremental_column="batch_id",
                move_source_file=False,
            ),
            extract_path="Files/raw/test/",
            extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
            extract_storage_workspace="test_storage_ws",
            extract_storage_lakehouse="test_storage_lh",
            extract_error_path="Files/error/test/",
            target_write_mode="append",
        )

    def test_extracts_date_from_filename(
        self, filename_date_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """_get_incremental_value_from_filename extracts date correctly."""
        from datetime import date

        extractor = FileSystemExtractor(
            resource_config=filename_date_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        file_info = FileInfo(
            path="abfss://test/Files/inbound/sales_20250115.csv",
            name="sales_20250115.csv",
            size=100,
            modified_ms=1736899200000,
        )

        result = extractor._get_incremental_value_from_filename(file_info)

        assert result == date(2025, 1, 15)

    def test_extracts_integer_from_filename(
        self, filename_int_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """_get_incremental_value_from_filename extracts integer correctly."""
        extractor = FileSystemExtractor(
            resource_config=filename_int_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        file_info = FileInfo(
            path="abfss://test/Files/inbound/batch_12345.csv",
            name="batch_12345.csv",
            size=100,
            modified_ms=1736899200000,
        )

        result = extractor._get_incremental_value_from_filename(file_info)

        assert result == 12345
        assert isinstance(result, int)

    def test_returns_none_when_regex_doesnt_match(
        self, filename_date_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """_get_incremental_value_from_filename returns None when regex doesn't match."""
        extractor = FileSystemExtractor(
            resource_config=filename_date_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        file_info = FileInfo(
            path="abfss://test/Files/inbound/other_file.csv",  # Doesn't match sales_(\d{8})\.csv
            name="other_file.csv",
            size=100,
            modified_ms=1736899200000,
        )

        result = extractor._get_incremental_value_from_filename(file_info)

        assert result is None


class TestFilenameWatermarkFiltering:
    """Tests for watermark filtering using filename metadata."""

    @pytest.fixture
    def filename_date_config(self):
        """Config with date-based filename metadata."""
        return ResourceConfig(
            resource_name="test_resource",
            source_name="test_source",
            source_config=SourceConfig(
                source_type="filesystem",
                source_connection_params={
                    "workspace_name": "test_workspace",
                    "lakehouse_name": "test_lakehouse",
                },
            ),
            source_extraction_params=FileSystemExtractionParams(
                inbound_path="Files/inbound/test/",
                discovery_pattern="*.csv",
                filename_metadata=[
                    {"name": "file_date", "regex": r"sales_(\d{8})\.csv", "type": "date", "format": "yyyyMMdd"}
                ],
                incremental_column="file_date",
                move_source_file=False,
            ),
            extract_path="Files/raw/test/",
            extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
            extract_storage_workspace="test_storage_ws",
            extract_storage_lakehouse="test_storage_lh",
            extract_error_path="Files/error/test/",
            target_write_mode="append",
        )

    def test_filters_files_by_filename_date(
        self, filename_date_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Files are filtered by date extracted from filename."""
        from datetime import date

        # Set watermark to 2025-01-10
        mock_extraction_logger.get_watermark.return_value = date(2025, 1, 10)

        files = [
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250108.csv",  # Before watermark
                name="sales_20250108.csv",
                size=100,
                modified_ms=1736899200000,
            ),
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250115.csv",  # After watermark
                name="sales_20250115.csv",
                size=200,
                modified_ms=1736899200000,
            ),
        ]

        extractor = FileSystemExtractor(
            resource_config=filename_date_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_files_by_watermark(files)

        # Only sales_20250115.csv should pass (date > 2025-01-10)
        assert len(result) == 1
        assert result[0].name == "sales_20250115.csv"

    def test_includes_all_files_when_no_watermark(
        self, filename_date_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """All files included on first run (no watermark)."""
        mock_extraction_logger.get_watermark.return_value = None

        files = [
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250108.csv",
                name="sales_20250108.csv",
                size=100,
                modified_ms=1736899200000,
            ),
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250115.csv",
                name="sales_20250115.csv",
                size=200,
                modified_ms=1736899200000,
            ),
        ]

        extractor = FileSystemExtractor(
            resource_config=filename_date_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        result = extractor._filter_files_by_watermark(files)

        assert len(result) == 2


class TestFilenameWatermarkUpdate:
    """Tests for watermark updates using filename metadata."""

    @pytest.fixture
    def filename_date_config(self):
        """Config with date-based filename metadata."""
        return ResourceConfig(
            resource_name="test_resource",
            source_name="test_source",
            source_config=SourceConfig(
                source_type="filesystem",
                source_connection_params={
                    "workspace_name": "test_workspace",
                    "lakehouse_name": "test_lakehouse",
                },
            ),
            source_extraction_params=FileSystemExtractionParams(
                inbound_path="Files/inbound/test/",
                discovery_pattern="*.csv",
                filename_metadata=[
                    {"name": "file_date", "regex": r"sales_(\d{8})\.csv", "type": "date", "format": "yyyyMMdd"}
                ],
                incremental_column="file_date",
                move_source_file=False,
            ),
            extract_path="Files/raw/test/",
            extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
            extract_storage_workspace="test_storage_ws",
            extract_storage_lakehouse="test_storage_lh",
            extract_error_path="Files/error/test/",
            target_write_mode="append",
        )

    def test_updates_watermark_with_max_filename_date(
        self, filename_date_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Watermark is updated with max date from extracted files."""
        from datetime import date

        files = [
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250108.csv",
                name="sales_20250108.csv",
                size=100,
                modified_ms=1736899200000,
            ),
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250115.csv",
                name="sales_20250115.csv",
                size=200,
                modified_ms=1736899200000,
            ),
        ]

        extractor = FileSystemExtractor(
            resource_config=filename_date_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        extractor._update_watermark_for_files(files, extract_batch_id="test-batch-123")

        mock_extraction_logger.update_watermark.assert_called_once()
        call_args = mock_extraction_logger.update_watermark.call_args
        assert call_args.kwargs["incremental_column"] == "file_date"
        assert call_args.kwargs["new_value"] == date(2025, 1, 15)  # Max date


class TestFilenameWatermarkExtractionFlow:
    """End-to-end tests for filename-based watermark extraction."""

    @pytest.fixture
    def filename_date_config(self):
        """Config with date-based filename metadata."""
        return ResourceConfig(
            resource_name="test_resource",
            source_name="test_source",
            source_config=SourceConfig(
                source_type="filesystem",
                source_connection_params={
                    "workspace_name": "test_workspace",
                    "lakehouse_name": "test_lakehouse",
                },
            ),
            source_extraction_params=FileSystemExtractionParams(
                inbound_path="Files/inbound/test/",
                discovery_pattern="*.csv",
                filename_metadata=[
                    {"name": "file_date", "regex": r"sales_(\d{8})\.csv", "type": "date", "format": "yyyyMMdd"}
                ],
                incremental_column="file_date",
                move_source_file=False,
            ),
            extract_path="Files/raw/test/",
            extract_file_format_params=FileFormatParams(file_format="csv", format_options={"file_delimiter": ","}),
            extract_storage_workspace="test_storage_ws",
            extract_storage_lakehouse="test_storage_lh",
            extract_error_path="Files/error/test/",
            target_write_mode="append",
        )

    @patch(f"{MODULE_PATH}.copy_file")
    @patch(f"{MODULE_PATH}.glob")
    def test_full_filename_watermark_flow(
        self, mock_glob, mock_copy,
        filename_date_config, mock_extraction_logger, mock_source_conn, mock_dest_conn
    ):
        """Full flow: discover -> filter by filename date -> copy -> update watermark."""
        from datetime import date

        # Setup: one old file, one new file (by filename date)
        mock_glob.return_value = [
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250108.csv",
                name="sales_20250108.csv",
                size=100,
                modified_ms=1736899200000,
            ),
            FileInfo(
                path="abfss://test/Files/inbound/sales_20250115.csv",
                name="sales_20250115.csv",
                size=200,
                modified_ms=1736899200000,
            ),
        ]
        # Watermark at 2025-01-10 - only sales_20250115 should be extracted
        mock_extraction_logger.get_watermark.return_value = date(2025, 1, 10)
        mock_copy.return_value = True

        extractor = FileSystemExtractor(
            resource_config=filename_date_config,
            extraction_logger=mock_extraction_logger,
            source_conn=mock_source_conn,
            dest_conn=mock_dest_conn,
        )

        results = list(extractor.extract())

        # Only one file should be extracted
        assert len(results) == 1
        assert results[0].file_count == 1
        # copy_file should be called once
        mock_copy.assert_called_once()
        # Watermark should be updated with the max filename date
        mock_extraction_logger.update_watermark.assert_called_once()
        call_args = mock_extraction_logger.update_watermark.call_args
        assert call_args.kwargs["new_value"] == date(2025, 1, 15)
